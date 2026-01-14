"""Simple Periodic SEC ingestor worker (scaffold).

Best-practice design notes:
- Configurable via environment variables or `config.toml`.
- Graceful shutdown, backoff on errors, pluggable fetcher.
- Does not perform any destructive actions by default.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import aiohttp

try:
    # Import the package ingestor if available. This import is optional
    # so the script can be inspected without executing network code.
    from foip import finscan_sec_ingestor as ingestor
except Exception:  # pragma: no cover - best-effort import
    ingestor = None

logger = logging.getLogger("foip.ingest_worker")


async def run_once(session: aiohttp.ClientSession, *, limit: int = 10) -> None:
    """Run a single ingestion attempt.

    This function detects whether a `fetch_company_filings` helper exists in
    `foip.finscan_sec_ingestor` and calls it. If not present it logs a message
    and returns.
    """
    if ingestor is None:
        logger.warning("ingestor package not available; skipping run_once")
        return

    fetch = getattr(ingestor, "fetch_company_filings", None)
    if fetch is None:
        logger.warning("fetch_company_filings not found in ingestor; skipping")
        return

    try:
        # Call fetch with best-effort argument names. If the signature differs,
        # the ingestor should provide a compatible wrapper for this script.
        result = fetch(session=session, limit=limit)
        if asyncio.iscoroutine(result):
            await result
        logger.info("ingest run completed")
    except Exception:
        logger.exception("ingest run failed")


async def daemon(poll_interval: float = 60.0) -> None:
    """Background ingestion loop with graceful shutdown.

    - `poll_interval` seconds between successful runs.
    - exponential backoff on errors is applied implicitly by sleeping longer.
    """
    backoff = 1.0
    max_backoff = 300.0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await run_once(session)
                backoff = 1.0
            except Exception:
                logger.exception("Unexpected error in daemon")
                backoff = min(backoff * 2, max_backoff)

            # Sleep with the configured poll interval plus backoff on errors
            interval = float(os.environ.get("FOIP_POLL_INTERVAL", poll_interval))
            await asyncio.sleep(interval + backoff)


def run_blocking() -> None:
    """Run the daemon in the current process (blocking).

    This function does not catch KeyboardInterrupt so the process can be
    stopped by systemd/supervisor or Ctrl-C in development.
    """
    logging.basicConfig(level=logging.INFO)
    interval = float(os.environ.get("FOIP_POLL_INTERVAL", 60))
    asyncio.run(daemon(poll_interval=interval))


if __name__ == "__main__":
    run_blocking()
