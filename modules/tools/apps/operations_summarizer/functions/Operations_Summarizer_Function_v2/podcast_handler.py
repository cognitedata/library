"""Podcast generation path for the unified operations function."""

from __future__ import annotations

from datetime import datetime, timezone

from cognite.client import CogniteClient

from cdf_helpers import CDFHelper
from podcast_config import get_podcast_config
from podcast_script import ensure_podcast_agent, generate_podcast_script
from save_results import fetch_summary_text, save_podcast_to_cdf
from tts_engine import generate_audio, get_audio_mime_type


def handle_podcast(
    data: dict | None = None,
    client: CogniteClient | None = None,
    secrets: dict | None = None,
    function_call_info: dict | None = None,
) -> dict:
    """Generate a podcast script and optional audio from a saved summary."""
    data = data or {}
    print("=" * 60)
    print("Podcast generation — start")
    print(f"  Timestamp : {datetime.now(timezone.utc).isoformat()}")
    print(f"  Data keys : {list(data.keys())}")
    print("=" * 60)

    try:
        cfg = get_podcast_config(data, client)
        summary_space = cfg["summary_space"]
        podcast_agent_id = cfg["podcast_agent_id"]
        podcast_instructions = cfg["podcast_instructions"]
        voice = cfg["voice"]
        speed = cfg["speed"]
        do_audio = cfg["generate_audio"]
        do_save = cfg["save_to_cdf"]
        fusion_domain = cfg["fusion_domain"]

        start_date = data.get("start_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        end_date = data.get("end_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

        source_summary_id = data.get("source_summary_id") or data.get("summary_external_id")
        print(f"  source_summary_id: {source_summary_id!r}")

        summary_markdown = data.get("summary_markdown")
        summary_external_id = data.get("summary_external_id")
        resolved_summary_id: str | None = None

        if not summary_markdown:
            if summary_external_id:
                print(f"[2/6] Fetching summary from CDF: {summary_external_id} (space={summary_space})")
                summary_markdown, resolved_summary_id = fetch_summary_text(
                    client, summary_external_id, summary_space,
                )
            else:
                print("[2/6] No summary provided — fetching most recent summary from CDF...")
                summary_markdown, resolved_summary_id = fetch_summary_text(
                    client, None, summary_space,
                )
        else:
            print("[2/6] Using summary_markdown from data parameter")

        if not source_summary_id and resolved_summary_id:
            source_summary_id = resolved_summary_id
            print(f"  Resolved source_summary_id from fetched summary: {source_summary_id!r}")

        if not summary_markdown or not summary_markdown.strip():
            return {
                "success": False,
                "error": "No summary text available. Provide summary_markdown or summary_external_id.",
            }

        print(f"  Summary length: {len(summary_markdown)} chars")

        print("[3/6] Ensuring podcast agent exists...")
        cdf_helper = CDFHelper(client)
        agent_id = ensure_podcast_agent(client, cdf_helper, podcast_agent_id, podcast_instructions)
        print(f"  Using agent: {agent_id}")

        print("[4/6] Generating podcast script...")
        script = generate_podcast_script(cdf_helper, agent_id, summary_markdown)
        print(f"  Script length: {len(script)} chars")

        result: dict = {
            "success": True,
            "script": script,
            "source_summary_id": source_summary_id,
        }

        audio_bytes: bytes | None = None
        if do_audio:
            print(f"[5/6] Generating audio (voice={voice}, speed={speed})...")
            try:
                audio_bytes = generate_audio(script, voice=voice, speed=speed)
                if audio_bytes:
                    print(f"  Audio size: {len(audio_bytes)} bytes ({get_audio_mime_type()})")
                    result["audio_generated"] = True
                    result["audio_size_bytes"] = len(audio_bytes)
                else:
                    print("  Audio generation returned no data")
                    result["audio_generated"] = False
                    result["audio_error"] = "TTS engine returned no audio"
            except Exception as e:
                print(f"  Audio generation failed: {e}")
                result["audio_generated"] = False
                result["audio_error"] = str(e)
        else:
            print("[5/6] Audio generation skipped (generate_audio=False)")
            result["audio_generated"] = False

        if do_save:
            print("[6/6] Saving podcast to CDF...")
            try:
                cdf_result = save_podcast_to_cdf(
                    client,
                    script=script,
                    audio_bytes=audio_bytes,
                    start_date=start_date,
                    end_date=end_date,
                    fusion_domain=fusion_domain,
                    source_summary_id=source_summary_id,
                )
                result["cdf_result"] = cdf_result
                print(f"  Saved: {cdf_result}")
            except Exception as e:
                print(f"  Error saving to CDF: {e}")
                result["cdf_save_error"] = str(e)
        else:
            print("[6/6] CDF save skipped (save_to_cdf=False)")

        print("=" * 60)
        print("Podcast generation — complete")
        print("=" * 60)
        return result

    except Exception as e:
        print(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}
