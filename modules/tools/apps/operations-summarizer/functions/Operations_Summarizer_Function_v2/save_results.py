"""
Save podcast script and audio to CDF.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import requests as _requests
from cognite.client import CogniteClient

from cdf_helpers import ensure_space_exists
from podcast_config import runtime
from tts_engine import get_audio_mime_type, get_audio_extension


def _upload_with_retry(client: CogniteClient, content: bytes, space: str, external_id: str,
                       max_retries: int = 4, initial_delay: float = 2.0):
    """Upload file content with retries to handle eventual consistency after space/node creation."""
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return client.files.upload_content_bytes(
                content=content,
                instance_id=(space, external_id),
            )
        except Exception as e:
            if attempt < max_retries - 1 and "not found" in str(e).lower():
                print(f"  Upload attempt {attempt + 1} failed (instance not yet visible), retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2
            else:
                raise


def fetch_summary_text(
    client: CogniteClient,
    external_id: str | None,
    space: str,
) -> tuple[str | None, str | None]:
    """Fetch a summary's text content from CDF.

    If ``external_id`` is None, fetches the most recent summary in the space.

    Returns ``(markdown_text, resolved_external_id)``. Either value may be
    ``None`` when the summary cannot be found or has no readable text.

    The Operations Summarizer saves summaries as CogniteFile nodes where the
    *description* property holds the full markdown text and the uploaded file
    content is a PDF.
    """
    from cognite.client.data_classes.data_modeling import NodeId
    from cognite.client.data_classes import filters

    file_view = runtime.file_view
    if file_view is None:
        raise RuntimeError("Podcast config has not been loaded")
    project = client.config.project

    if external_id is None:
        external_id = _find_latest_summary(client, space, file_view)
        if not external_id:
            print("No summaries found in CDF")
            return None, None

    resolved_external_id = external_id
    print(f"  Attempting to fetch summary: space={space}, externalId={external_id}")

    # Strategy 1: retrieve the node directly and read its description.
    # The Operations Summarizer always stores full markdown in description.
    try:
        print(f"  Retrieving node via SDK instances.retrieve...")
        res = client.data_modeling.instances.retrieve(
            nodes=NodeId(space, external_id),
            sources=file_view,
        )
        if res.nodes:
            node = res.nodes[0]
            # SDK properties structure: {ViewId: {prop_name: value}}
            for view_id, view_props in node.properties.items():
                desc = view_props.get("description")
                if desc and str(desc).strip():
                    print(f"  Got summary from description ({len(str(desc))} chars)")
                    return str(desc), resolved_external_id
            print(f"  Node retrieved but description is empty")
        else:
            print(f"  instances.retrieve returned no nodes")
    except Exception as e:
        print(f"  instances.retrieve failed: {e}")
        import traceback
        traceback.print_exc()

    # Strategy 2: download the file content directly
    try:
        print("  Trying file downloadlink...")
        resp = client.post(
            f"/api/v1/projects/{project}/files/downloadlink",
            json={
                "items": [
                    {"instanceId": {"space": space, "externalId": external_id}}
                ]
            },
        )
        data = resp.json() if hasattr(resp, "json") else resp
        items = data.get("items", [])
        if not items:
            print(f"  Summary file not found via downloadlink: {external_id}")
            return None, resolved_external_id

        download_url = items[0].get("downloadUrl")
        if not download_url:
            print("  No downloadUrl in response")
            return None, resolved_external_id

        content = _requests.get(download_url, timeout=60)
        if content.status_code != 200:
            print(f"  Failed to download summary: HTTP {content.status_code}")
            return None, resolved_external_id

        content_type = content.headers.get("content-type", "")
        if "pdf" in content_type or content.content[:5] == b"%PDF-":
            print("  File content is PDF — cannot extract text from binary")
            return None, resolved_external_id

        text = content.text
        if text and text.strip():
            print(f"  Got summary from file download ({len(text)} chars)")
            return text, resolved_external_id

    except Exception as e:
        print(f"  Error downloading summary file: {e}")
        import traceback
        traceback.print_exc()

    return None, resolved_external_id


def _find_latest_summary(client: CogniteClient, space: str, file_view) -> str | None:
    """Find the most recently created operation summary in the given space.

    The filter is applied in CDF. A shared data space also holds test reports,
    podcast files, and the function zip, so there is no fallback to other nodes.
    """
    from cognite.client.data_classes import filters
    from config import runtime as summary_runtime

    prefix = summary_runtime.operation_summary_prefix
    if not prefix:
        print("  operation summary prefix is empty; not listing files")
        return None

    try:
        summary_filter = filters.And(
            filters.Equals(property=["node", "space"], value=space),
            filters.Prefix(property=["node", "externalId"], value=prefix),
        )

        print(f"  Listing latest '{prefix}' node in space '{space}'...")
        nodes = client.data_modeling.instances.list(
            instance_type="node",
            sources=file_view,
            filter=summary_filter,
            sort=[{"property": ["node", "createdTime"], "direction": "descending"}],
            limit=1,
        )
        print(f"  Found {len(nodes)} matching file nodes")

        for node in nodes:
            eid = node.external_id or ""
            if eid.startswith(prefix):
                print(f"  Found most recent summary: {eid}")
                return eid

    except Exception as e:
        print(f"  Error finding latest summary: {e}")
        import traceback
        traceback.print_exc()

    return None


def podcast_external_ids(
    *,
    source_summary_id: str | None,
    start_date: str,
    end_date: str,
    prefix: str,
    now: datetime | None = None,
) -> tuple[str, str]:
    """Deterministic script and audio external IDs. Same literals as the app."""
    if source_summary_id:
        base = _sanitize(source_summary_id)
    else:
        moment = now or datetime.now(timezone.utc)
        now_str = moment.strftime("%Y%m%d_%H%M%S")
        base = _sanitize(f"ops_summary_{start_date}_{end_date}_{now_str}")
    return f"{prefix}{base}_script", f"{prefix}{base}_audio"


def save_podcast_to_cdf(
    client: CogniteClient,
    script: str,
    audio_bytes: bytes | None = None,
    start_date: str = "",
    end_date: str = "",
    fusion_domain: str | None = None,
    source_summary_id: str | None = None,
) -> dict:
    """Save podcast script (and optional audio) to CDF.

    Uses deterministic external IDs derived from ``source_summary_id`` so that
    the same summary always maps to the same podcast nodes (upsert semantics).

    Returns dict with keys: script_external_id, audio_external_id (if audio),
    fusion_url (if audio).
    """
    try:
        from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply
    except ImportError:
        from cognite.client.data_classes.data_modeling import CogniteFileApply

    ensure_space_exists(client, runtime.summary_space)

    result: dict = {}
    script_eid, audio_eid = podcast_external_ids(
        source_summary_id=source_summary_id,
        start_date=start_date,
        end_date=end_date,
        prefix=runtime.podcast_prefix,
    )
    print(f"  script_eid={script_eid!r}, audio_eid={audio_eid!r}")

    # ── Save script as text file ──────────────────────────────────
    client.data_modeling.instances.apply(
        CogniteFileApply(
            name=f"{script_eid}.txt",
            space=runtime.summary_space,
            external_id=script_eid,
            mime_type="text/plain",
            description=script,
        ),
    )

    _upload_with_retry(client, script.encode("utf-8"), runtime.summary_space, script_eid)
    result["script_external_id"] = script_eid
    print(f"  Saved script: {script_eid}")

    # ── Save audio file (if available) ────────────────────────────
    if audio_bytes:
        mime = get_audio_mime_type()
        ext = get_audio_extension()

        client.data_modeling.instances.apply(
            CogniteFileApply(
                name=f"{audio_eid}.{ext}",
                space=runtime.summary_space,
                external_id=audio_eid,
                mime_type=mime,
                description=f"Podcast audio for summary {source_summary_id or audio_eid}",
            ),
        )

        uploaded = _upload_with_retry(client, audio_bytes, runtime.summary_space, audio_eid)
        file_id = uploaded.id if hasattr(uploaded, "id") else uploaded
        result["audio_external_id"] = audio_eid
        result["audio_file_id"] = file_id
        print(f"  Saved audio: {audio_eid}")

        domain = _resolve_fusion_domain(client, fusion_domain)
        if domain and file_id:
            project = client.config.project
            cluster = client.config.base_url.replace("https://", "").replace("http://", "").rstrip("/")
            result["fusion_url"] = (
                f"https://{domain}/{project}/search/file/{file_id}"
                f"?cluster={cluster}&workspace=industrial-tools"
            )

    return result


def _resolve_fusion_domain(client: CogniteClient, override: str | None) -> str | None:
    """Determine the Fusion domain for URLs."""
    if override:
        return override

    env_val = os.environ.get("COGNITE_FUSION_DOMAIN") or os.environ.get("FUSION_DOMAIN")
    if env_val:
        return env_val

    project = client.config.project
    if "-" in project:
        prefix = project.split("-")[0]
        if prefix and len(prefix) <= 20:
            return f"{prefix}.fusion.cognite.com"

    return None


def _sanitize(name: str) -> str:
    return "".join(c for c in name if c.isalnum() or c in "_-.")
