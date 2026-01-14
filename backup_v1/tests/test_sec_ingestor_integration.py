import asyncio
import json
from datetime import datetime, timezone

import pytest
from aiohttp import web

from finscan_sec_ingestor import SECIngestor


class FakeRegistry:
    def __init__(self):
        self.registered = []
        self.relationships = []

    async def register_entity(self, entity_id, payload):
        self.registered.append((entity_id, payload))
        return True

    async def get_entity(self, entity_id):
        return None

    async def add_relationship(self, relationship):
        self.relationships.append(relationship)
        return True


class StubParser:
    def parse_filing(self, text, form_type):
        return {"sections": {"intro": "ok"}, "risk_clauses": []}


@pytest.mark.asyncio
async def test_sec_ingestor_integration(aiohttp_server):
    # Create a tiny aiohttp app that mimics the SEC API and a text endpoint
    async def query_handler(request):
        response = {
            "filings": [
                {
                    "accessionNo": "0001",
                    "formType": "10-K",
                    "filedAt": datetime.now(timezone.utc).isoformat(),
                    "linkToTxt": str(request.url.with_path("/text/0001")),
                    "periodOfReport": datetime.now(timezone.utc).isoformat(),
                    "cik": "0000001",
                    "companyName": "ACME Corp",
                }
            ]
        }
        return web.json_response(response)

    async def text_handler(request):
        return web.Response(text="<html>filing text</html>")

    app = web.Application()
    app.router.add_get("/query", query_handler)
    app.router.add_get("/text/0001", text_handler)

    server = await aiohttp_server(app)
    base_url = f"http://{server.host}:{server.port}"

    # Create SECIngestor with stub parser and fake registry
    registry = FakeRegistry()
    ingestor = SECIngestor(api_key="key", entity_registry=registry, base_url=base_url)
    ingestor.parser = StubParser()

    # Initialize session explicitly
    await ingestor.initialize()
    try:
        filings = await ingestor.fetch_company_filings(ticker="ACME")
    finally:
        await ingestor.close()

    assert len(filings) == 1
    # Ensure registry was called to register company and filing
    # Two registrations: company and filing
    assert len(registry.registered) >= 1
    assert any(r[0].entity_type == r[0].entity_type for r in registry.registered)
    assert len(registry.relationships) >= 1


@pytest.mark.asyncio
async def test_sec_ingestor_no_filings(aiohttp_server):
    async def query_handler(request):
        return web.json_response({"filings": []})

    app = web.Application()
    app.router.add_get("/query", query_handler)

    server = await aiohttp_server(app)
    base_url = f"http://{server.host}:{server.port}"

    registry = FakeRegistry()
    ingestor = SECIngestor(api_key="key", entity_registry=registry, base_url=base_url)

    await ingestor.initialize()
    try:
        filings = await ingestor.fetch_company_filings(ticker="NIL")
    finally:
        await ingestor.close()

    assert filings == []
    assert registry.registered == []


@pytest.mark.asyncio
async def test_sec_ingestor_text_fetch_failure(aiohttp_server):
    async def query_handler(request):
        response = {
            "filings": [
                {
                    "accessionNo": "0002",
                    "formType": "10-Q",
                    "filedAt": datetime.now(timezone.utc).isoformat(),
                    "linkToTxt": str(request.url.with_path("/text/0002")),
                    "periodOfReport": datetime.now(timezone.utc).isoformat(),
                    "cik": "0000002",
                    "companyName": "BETA Corp",
                }
            ]
        }
        return web.json_response(response)

    async def text_handler(request):
        return web.Response(status=500, text="error")

    app = web.Application()
    app.router.add_get("/query", query_handler)
    app.router.add_get("/text/0002", text_handler)

    server = await aiohttp_server(app)
    base_url = f"http://{server.host}:{server.port}"

    registry = FakeRegistry()
    ingestor = SECIngestor(api_key="key", entity_registry=registry, base_url=base_url)

    await ingestor.initialize()
    try:
        filings = await ingestor.fetch_company_filings(ticker="BETA")
    finally:
        await ingestor.close()

    assert filings == []
    assert registry.registered == []
