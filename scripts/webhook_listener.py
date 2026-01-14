"""Webhook listener scaffold (aiohttp).

Receives POST webhooks (EDGAR, CI, or other) and enqueues lightweight
handlers. Safe by default: validates an optional `FOIP_WEBHOOK_SECRET` header
and only logs received events. To act on events, wire handler hooks (ingest,
health gate, etc.) and secure with a reverse proxy or token validation.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Optional

from aiohttp import web

try:
    # optional local handlers
    from foip import finscan_sec_ingestor as ingestor
except Exception:  # pragma: no cover - optional import
    ingestor = None

logger = logging.getLogger("foip.webhook_listener")


async def handle_webhook(request: web.Request) -> web.Response:
    secret = os.environ.get("FOIP_WEBHOOK_SECRET")
    if secret:
        header = request.headers.get("X-FOIP-SIGN")
        if header != secret:
            logger.warning("webhook secret mismatch")
            return web.Response(status=403, text="forbidden")

    try:
        data = await request.json()
    except Exception:
        data = await request.text()

    logger.info("received webhook: %s", data)

    # Example: if payload contains {'action': 'ingest', 'symbols': [...]}
    if isinstance(data, dict) and data.get("action") == "ingest":
        # schedule ingestion in background to keep webhook fast
        asyncio.create_task(_schedule_ingest(data))

    return web.Response(status=202, text="accepted")


async def _schedule_ingest(payload: dict) -> None:
    # If ingestor provides a compatible API, call it; otherwise log intent.
    if ingestor and hasattr(ingestor, "fetch_company_filings"):
        try:
            # Best-effort: use default session creation inside ingestor
            await ingestor.fetch_company_filings(symbols=payload.get("symbols"))
            logger.info("background ingest completed")
            return
        except Exception:
            logger.exception("background ingest failed")

    logger.info("would run ingest for: %s", payload.get("symbols"))


def make_app() -> web.Application:
    app = web.Application()
    app.add_routes([web.post("/webhook", handle_webhook)])
    return app


def run(host: str = "127.0.0.1", port: int = 8080) -> None:
    app = make_app()
    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    run(host=os.environ.get("FOIP_WEBHOOK_HOST", "127.0.0.1"), port=int(os.environ.get("FOIP_WEBHOOK_PORT", "8080")))
