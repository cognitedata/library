"""Cognite Function entry point for P&ID annotation.

The function is invokable two ways:

1. **Function Apps API**, ``POST /function-apps/{functionExternalId}/calls``.
   The caller sends a request envelope:

       {"data": {"path": "/annotate", "method": "POST", "body": {...}},
        "nonce": "..."}

   The handler routes by ``(method, path)`` using a tiny in-file dispatcher and
   returns a typed response envelope:

       {"status": <int>, "data": {...}}                # success
       {"status": <int>, "error": {"message": "..."}}  # failure

   Built-in system routes:

       GET  /__health__   liveness probe.
       GET  /__routes__   introspection of registered routes.

   User routes:

       POST /annotate     run the full P&ID annotation pipeline.

2. **Legacy Functions API / schedules**. The caller sends the bare payload:

       {"logLevel": "INFO",
        "ExtractionPipelineExtId": "ep_ctx_files_pandid_annotation"}

   The handler synthesises a ``POST /annotate`` call internally and returns
   the legacy ``{"status": "succeeded"|"failure", ...}`` shape so existing
   schedules and workflow tasks keep working unchanged.

Notes
-----
* The Function Apps API is in **Private Beta** at the time of writing
  (`20230101-alpha` spec). Customers need access enabled by Cognite Support
  before the ``/function-apps/...`` paths can actually be called.
* Cognite has not published an official handler-side framework for Function
  Apps (the OpenAPI ``Function`` schema still expects a ``handle(data, client)``
  in ``handler.py``), so the dispatcher below is intentionally minimal — no
  routing-by-pattern, no middleware, just method+path equality lookup. If/when
  Cognite ships an SDK, this can be swapped out without touching ``pipeline.py``
  or ``config.py``.
* The runtime ``handle(data, client)`` signature is fixed by the Functions
  runtime; ``data`` may legitimately be ``None`` for schedule invocations
  with no payload, which the dispatcher handles gracefully.
"""

from __future__ import annotations

import os
import sys
import threading
from pathlib import Path
from typing import Any, Callable

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

sys.path.append(str(Path(__file__).parent))

from config import load_config_parameters
from logger import CogniteFunctionLogger
from pipeline import annotate_p_and_id

# ---------------------------------------------------------------------------
# Usage tracking (best effort; must not affect the handler)
# ---------------------------------------------------------------------------

_SOURCE = "dp:cdf_p_and_id_annotation"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"


def _report_usage(client: CogniteClient) -> None:
    try:
        from mixpanel import Consumer, Mixpanel
        mp = Mixpanel(
            "8f28374a6614237dd49877a0d27daa78",
            consumer=Consumer(api_host="api-eu.mixpanel.com"),
        )
        distinct_id = f"{client.config.project}:{client.config.cdf_cluster}"

        def _send() -> None:
            # Inner guard so a Mixpanel JSON-encode / network error in the
            # daemon thread can't surface as an unhandled thread exception
            # in production stderr or as a noisy pytest warning in tests.
            try:
                mp.track(distinct_id, "fn-handle", {
                    "source": _SOURCE,
                    "tracker_version": _TRACKER_VERSION,
                    "dp_version": _DP_VERSION,
                    "type": "py-function",
                    "cdf_cluster": client.config.cdf_cluster,
                    "cdf_project": client.config.project,
                })
            except Exception:
                pass

        threading.Thread(target=_send, daemon=True).start()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Function App route registry + dispatcher
# ---------------------------------------------------------------------------

class _Route:
    __slots__ = ("method", "path", "handler")

    def __init__(self, method: str, path: str, handler: Callable[..., dict]) -> None:
        self.method = method.upper()
        self.path = path
        self.handler = handler


_ROUTES: list[_Route] = []


def _route(method: str, path: str) -> Callable[[Callable[..., dict]], Callable[..., dict]]:
    """Register a route handler with the in-file dispatcher."""

    def _wrap(fn: Callable[..., dict]) -> Callable[..., dict]:
        _ROUTES.append(_Route(method, path, fn))
        return fn

    return _wrap


def _ok(body: Any, status: int = 200) -> dict:
    """Wrap a successful response in the Function App typed-envelope shape."""
    return {"status": status, "data": body}


def _err(status: int, message: str, **extra: Any) -> dict:
    """Wrap an error response in the Function App typed-envelope shape."""
    return {"status": status, "error": {"message": message, **extra}}


def _looks_like_function_app_envelope(payload: dict | None) -> bool:
    """Detect a Function App request envelope vs a legacy schedule payload.

    A Function App envelope has ``data`` as a dict carrying ``path`` and
    ``method`` strings. A legacy schedule payload has neither.
    """
    if not isinstance(payload, dict):
        return False
    inner = payload.get("data")
    return (
        isinstance(inner, dict)
        and isinstance(inner.get("path"), str)
        and isinstance(inner.get("method"), str)
    )


def _dispatch(
    client: CogniteClient,
    envelope: dict,
    logger: CogniteFunctionLogger,
) -> dict:
    request = envelope.get("data") or {}
    method = (request.get("method") or "").upper()
    path = request.get("path") or ""
    raw_body = request.get("body")
    body: dict = raw_body if isinstance(raw_body, dict) else {}

    for route in _ROUTES:
        if route.method == method and route.path == path:
            try:
                return route.handler(client=client, body=body, logger=logger)
            except Exception as exc:
                msg = f"{method} {path} failed: {exc!s}"
                logger.error(msg)
                return _err(500, msg)

    return _err(
        404,
        f"No route registered for {method!s} {path!s}",
        routes=[f"{r.method} {r.path}" for r in _ROUTES],
    )


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@_route("GET", "/__health__")
def _health(client: CogniteClient, body: dict, logger: CogniteFunctionLogger) -> dict:
    return _ok({
        "ok": True,
        "source": _SOURCE,
        "dp_version": _DP_VERSION,
        "tracker_version": _TRACKER_VERSION,
    })


@_route("GET", "/__routes__")
def _routes(client: CogniteClient, body: dict, logger: CogniteFunctionLogger) -> dict:
    return _ok({"routes": [{"method": r.method, "path": r.path} for r in _ROUTES]})


@_route("POST", "/annotate")
def _annotate(client: CogniteClient, body: dict, logger: CogniteFunctionLogger) -> dict:
    """Run the full P&ID annotation pipeline.

    Expected body (same shape as the legacy schedule payload):
        {"logLevel": "INFO",
         "ExtractionPipelineExtId": "ep_ctx_files_pandid_annotation"}
    """
    pipeline_ext_id = body.get("ExtractionPipelineExtId")
    if not isinstance(pipeline_ext_id, str):
        return _err(400, "Missing or invalid 'ExtractionPipelineExtId' in body")

    logger.info(
        "Starting diagram parsing annotation, reading parameters from extraction "
        f"pipeline config: {pipeline_ext_id}"
    )
    config = load_config_parameters(client, body)
    logger.debug("Loaded config successfully")
    annotate_p_and_id(client, logger, body, config)
    return _ok({"status": "succeeded", "data": body})


# ---------------------------------------------------------------------------
# CDF Functions runtime entry point
# ---------------------------------------------------------------------------

def handle(data: dict | None, client: CogniteClient) -> dict:
    """Cognite Functions runtime entry point.

    Accepts both shapes:
      1. Function Apps request envelope: dispatches by path+method, returns
         the typed envelope ``{"status": int, "data"|"error": ...}``.
      2. Legacy schedule payload: runs the annotation pipeline and returns
         the legacy ``{"status": "succeeded"|"failure", ...}`` shape so
         existing schedules and workflow tasks aren't disrupted.
    """
    _report_usage(client)

    payload: dict = data if isinstance(data, dict) else {}

    # Resolve log level from either the envelope body or the legacy payload.
    inner = payload.get("data") if isinstance(payload.get("data"), dict) else None
    inner_body = inner.get("body") if isinstance(inner, dict) else None
    if isinstance(inner_body, dict) and isinstance(inner_body.get("logLevel"), str):
        loglevel = inner_body["logLevel"]
    elif isinstance(payload.get("logLevel"), str):
        loglevel = payload["logLevel"]
    else:
        loglevel = "INFO"

    logger = CogniteFunctionLogger(loglevel)

    if _looks_like_function_app_envelope(payload):
        # Function Apps mode: route, return typed envelope.
        return _dispatch(client, payload, logger)

    # Legacy mode: preserve the original succeeded/failure response shape.
    try:
        logger.info(
            f"Starting diagram parsing annotation with loglevel = {loglevel},  "
            f"reading parameters from extraction pipeline config: "
            f"{payload.get('ExtractionPipelineExtId')}"
        )
        config = load_config_parameters(client, payload)
        logger.debug("Loaded config successfully")
        annotate_p_and_id(client, logger, payload, config)
        return {"status": "succeeded", "data": payload}
    except Exception as e:
        message = f"failed, Message: {e!s}"
        logger.error(message)
        return {"status": "failure", "message": message}


# ---------------------------------------------------------------------------
# Local-runner: lets you exercise the handler against a real CDF project
# ---------------------------------------------------------------------------

def run_locally() -> None:
    """Authenticate against a real CDF project and invoke ``handle()`` once.

    Set the toggle ``USE_FUNCTION_APP_ENVELOPE`` below to choose between the
    two payload shapes the deployed function accepts. The Function Apps
    envelope is the preferred path going forward; the legacy shape is
    preserved for compatibility with existing schedules.
    """
    required_envvars = (
        "CDF_PROJECT", "CDF_CLUSTER", "IDP_CLIENT_ID", "IDP_CLIENT_SECRET", "IDP_TOKEN_URL",
    )
    if missing := [envvar for envvar in required_envvars if envvar not in os.environ]:
        raise ValueError(f"Missing one or more env.vars: {missing}")

    cdf_project_name = os.environ["CDF_PROJECT"]
    cdf_cluster = os.environ["CDF_CLUSTER"]
    client_id = os.environ["IDP_CLIENT_ID"]
    client_secret = os.environ["IDP_CLIENT_SECRET"]
    token_uri = os.environ["IDP_TOKEN_URL"]
    base_url = f"https://{cdf_cluster}.cognitedata.com"

    client = CogniteClient(
        ClientConfig(
            client_name="Toolkit P&ID pipeline",
            base_url=base_url,
            project=cdf_project_name,
            credentials=OAuthClientCredentials(
                token_url=token_uri,
                client_id=client_id,
                client_secret=client_secret,
                scopes=[f"{base_url}/.default"],
            ),
        )
    )

    USE_FUNCTION_APP_ENVELOPE = True
    extraction_pipeline_ext_id = "ep_ctx_files_pandid_annotation"

    if USE_FUNCTION_APP_ENVELOPE:
        payload: dict = {
            "data": {
                "method": "POST",
                "path": "/annotate",
                "body": {
                    "logLevel": "INFO",
                    "ExtractionPipelineExtId": extraction_pipeline_ext_id,
                },
            },
            "nonce": "local",
        }
    else:
        payload = {
            "logLevel": "INFO",
            "ExtractionPipelineExtId": extraction_pipeline_ext_id,
        }

    result = handle(payload, client)
    print(result)


if __name__ == "__main__":
    run_locally()
