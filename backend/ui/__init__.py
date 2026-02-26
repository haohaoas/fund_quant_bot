# backend/ui/__init__.py
"""UI router (redirect-only).

This package previously aggregated server-rendered UI pages implemented in:
- dashboard.py
- portfolio.py
- strategy.py
- record.py

Now we keep only a thin router that redirects all `/ui` traffic to an external
frontend (static site or separate service).

Configure the target base via env var `UI_FORWARD_BASE`.
Examples:
  UI_FORWARD_BASE="/static"            -> /ui -> /static/ui
  UI_FORWARD_BASE="https://xx.com"     -> /ui -> https://xx.com/ui

Notes:
- Preserves path and query string.
- Endpoints are excluded from OpenAPI schema.
"""

from __future__ import annotations

import os
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

router = APIRouter()

UI_FORWARD_BASE = os.getenv("UI_FORWARD_BASE", "").rstrip("/")


def _join_forward_url(base: str, path: str, query: str) -> str:
    base = (base or "").rstrip("/")
    path = ("/" + (path or "").lstrip("/")) if path else "/"
    url = f"{base}{path}" if base else path
    return f"{url}?{query}" if query else url


@router.api_route("/ui", methods=["GET", "HEAD"], include_in_schema=False)
@router.api_route("/ui/", methods=["GET", "HEAD"], include_in_schema=False)
async def ui_root(request: Request) -> RedirectResponse:
    target = _join_forward_url(UI_FORWARD_BASE, "/ui", request.url.query)
    return RedirectResponse(url=target)


@router.api_route("/ui/{path:path}", methods=["GET", "HEAD"], include_in_schema=False)
async def ui_forward(path: str, request: Request) -> RedirectResponse:
    target_path = f"/ui/{path}" if path else "/ui"
    target = _join_forward_url(UI_FORWARD_BASE, target_path, request.url.query)
    return RedirectResponse(url=target)