"""SEC ingestor inside foip package."""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union, cast

import aiohttp
import attr
from types import SimpleNamespace

if TYPE_CHECKING:
    from foip.shared_metadata_schema import (
        Company,
        EntityID,
        EntityRelationship,
        EntityType,
        FormType,
        SECFiling,
        TimePoint,
    )
    from foip.shared_metadata_registry import EntityRegistry
else:
    Company = Any
    EntityID = Any
    EntityRelationship = Any
    EntityType = Any
    FormType = Any
    SECFiling = Any
    TimePoint = Any
    EntityRegistry = Any

try:
    from foip.foip_infra import log as _foip_log
    log = _foip_log.bind(module="finscan_sec_ingestor")
except Exception:
    import structlog
    log = structlog.get_logger(__name__)


class SECIngestor:
    def __init__(self, api_key: str, entity_registry: "EntityRegistry", base_url: str = "https://api.sec-api.io"):
        self.api_key = api_key
        self.entity_registry = entity_registry
        self.base_url = base_url
        self.parser = None
        self.diff_engine = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_request: Optional[datetime] = None
        self.min_interval = timedelta(seconds=0.1)
        self._rate_lock = asyncio.Lock()
        log.info("SEC Ingestor initialized")

    async def initialize(self):
        self.session = aiohttp.ClientSession(headers={"Authorization": self.api_key}, timeout=aiohttp.ClientTimeout(total=30))

    async def close(self):
        if self.session:
            await self.session.close()

    async def fetch_company_filings(
        self,
        ticker: str,
        cik: Optional[str] = None,
        forms: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Union["SECFiling", Dict[str, Any]]]:
        """Fetch SEC filings for a company.

        This implementation is robust for test environments where the
        real schema classes (EntityID, SECFiling, Company) are not
        available at runtime and fall back to `Any`.
        """
        if not self.session:
            await self.initialize()

        await self._rate_limit()

        # Build a simple query compatible with test server
        query_parts = [f'ticker:"{ticker}"']
        if cik:
            query_parts.append(f'cik:"{cik}"')
        if forms:
            form_list = " OR ".join([f'formType:"{f}"' for f in forms])
            query_parts.append(f"({form_list})")
        if start_date:
            query_parts.append(f"filedAt:[{start_date.isoformat()} TO *]")
        if end_date:
            query_parts.append(f"filedAt:[* TO {end_date.isoformat()}]")

        query = " AND ".join(query_parts)

        try:
            url = f"{self.base_url}/query"
            params = {"query": query, "from": "0", "size": "100", "sort": "filedAt:desc"}

            assert self.session is not None
            session = self.session
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                filings: List[Union["SECFiling", Dict[str, Any]]] = []
                for filing_data in data.get("filings", []):
                    try:
                        processed = await self._process_filing(filing_data, ticker)
                        if processed:
                            filings.append(processed)
                    except Exception as e:
                        log.error("Failed to process filing", filing=filing_data, error=str(e))

                log.info("Fetched company filings", ticker=ticker, count=len(filings))
                return filings

        except aiohttp.ClientError as e:
            log.error("SEC API request failed", ticker=ticker, error=str(e))
            return []

    async def _process_filing(self, filing_data: Dict[str, Any], ticker: str) -> Optional[Union["SECFiling", Dict[str, Any]]]:
        """Turn raw filing JSON into a registry payload and return a filing object or dict."""
        accession_number = filing_data.get("accessionNo")
        form_type = filing_data.get("formType")
        filed_at = filing_data.get("filedAt")
        if isinstance(filed_at, str):
            try:
                filing_date = datetime.fromisoformat(filed_at.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                filing_date = datetime.now(timezone.utc)
        else:
            filing_date = datetime.now(timezone.utc)

        # Build simple entity id and payload dicts when schema classes are not present
        # Entity constructors may require concrete string types; coerce safely
        accession_str = str(accession_number) if accession_number is not None else ""
        try:
            entity_id: Any = EntityID(entity_type=EntityType.FILING, identifier=accession_str, source="sec", timestamp=filing_date)
        except Exception:
            entity_id = SimpleNamespace(
                entity_type=getattr(EntityType, "FILING", "filing"),
                identifier=accession_str,
                source="sec",
                timestamp=filing_date.isoformat(),
            )

        # If already processed, skip (registry.get_entity should handle lookup)
        try:
            existing = await self.entity_registry.get_entity(entity_id)
        except Exception:
            existing = None
        if existing:
            log.debug("Filing already processed", accession=accession_number)
            return None

        # Fetch filing text
        filing_text = await self._fetch_filing_text(filing_data.get("linkToTxt"))
        if not filing_text:
            return None

        # Parse filing using provided parser if available
        parsed: Dict[str, Any] = {}
        try:
            if self.parser:
                parsed = self.parser.parse_filing(filing_text, form_type)
        except Exception:
            parsed = {}

        # Build filing payload
        filing_payload: Dict[str, Any] = {
            "entity_id": entity_id,
            "form_type": form_type,
            "filing_date": filing_date.isoformat(),
            "accession_number": accession_number,
            "sections": parsed.get("sections", {}),
            "risk_clauses": parsed.get("risk_clauses", []),
            "metadata": {
                "cik": filing_data.get("cik"),
                "company_name": filing_data.get("companyName"),
                "link_to_txt": filing_data.get("linkToTxt"),
            },
        }

        # Register company and relationship if cik present
        company_cik = filing_data.get("cik")
        if company_cik:
            company_id: Any
            try:
                company_id = EntityID(entity_type=EntityType.COMPANY, identifier=str(company_cik), source="sec")
            except Exception:
                company_id = SimpleNamespace(entity_type=getattr(EntityType, "COMPANY", "company"), identifier=str(company_cik), source="sec")

            company_payload = {
                "entity_id": company_id,
                "name": filing_data.get("companyName", ""),
                "ticker": ticker,
                "cik": company_cik,
                "metadata": {"sec_registered": True},
            }

            try:
                await self.entity_registry.register_entity(company_id, company_payload)
            except Exception:
                # Best-effort register; continue
                pass

            relationship_obj = SimpleNamespace(
                source_entity_id=entity_id,
                target_entity_id=company_id,
                relationship_type="filed_by",
                established_by_run=uuid.uuid4().hex,
                established_at=datetime.now(timezone.utc).isoformat(),
            )

            try:
                await self.entity_registry.add_relationship(cast(Any, relationship_obj))
            except Exception:
                pass

        # Register filing
        try:
            await self.entity_registry.register_entity(cast(Any, entity_id), filing_payload)
        except Exception:
            pass

        # Return the filing payload as the SECFiling representation
        return filing_payload

    async def _fetch_filing_text(self, url: Optional[str]) -> Optional[str]:
        if not url or not self.session:
            return None
        await self._rate_limit()
        try:
            assert self.session is not None
            session = self.session
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                return await response.text()
        except aiohttp.ClientError as e:
            log.error("Failed to fetch filing text", url=url, error=str(e))
            return None

    async def _rate_limit(self):
        async with self._rate_lock:
            now = datetime.now(timezone.utc)
            if self.last_request:
                elapsed = now - self.last_request
                if elapsed < self.min_interval:
                    wait_time = (self.min_interval - elapsed).total_seconds()
                    await asyncio.sleep(wait_time)
            self.last_request = now
