"""
SEC Filing Ingestor - Primary truth source for FinScan
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp
import attr

# Schema imports with fallbacks to the canonical shared_metadata_schema
try:
    from shared_metadata_schema import (
        Company,
        EntityID,
        EntityRelationship,
        EntityType,
        FormType,
        SECFiling,
        TimePoint,
    )
except Exception:
    # Minimal placeholders for test environments
    from enum import Enum as _Enum

    class EntityType(_Enum):
        FILING = "filing"
        COMPANY = "company"

    class FormType(_Enum):
        TEN_K = "10-K"

    class EntityRelationship:
        pass


try:
    from shared_metadata_registry import EntityRegistry
except Exception:
    EntityRegistry = object

try:
    from .parser import SECParser
except Exception:
    try:
        # Support running as top-level module in tests
        from parser import SECParser
    except Exception:
        # Minimal fallback for test environments
        class SECParser:
            def parse_filing(self, text, form_type):
                return {"sections": {}, "risk_clauses": []}


try:
    from .diff_engine import DiffEngine
except Exception:
    try:
        from diff_engine import DiffEngine
    except Exception:

        class DiffEngine:
            def create_diff(self, a, b):
                return {"summary": {}}

            def compare_filings(self, old, new):
                return {}

            def identify_material_changes(self, diff):
                return []


try:
    from foip_infra import log as _foip_log

    log = _foip_log.bind(module="finscan_sec_ingestor")
except Exception:
    import structlog

    log = structlog.get_logger(__name__)


class SECIngestor:
    """Primary SEC data ingestion engine"""

    def __init__(
        self,
        api_key: str,
        entity_registry: EntityRegistry,
        base_url: str = "https://api.sec-api.io",
    ):
        self.api_key = api_key
        self.entity_registry = entity_registry
        self.base_url = base_url
        self.parser = SECParser()
        self.diff_engine = DiffEngine()
        self.session: Optional[aiohttp.ClientSession] = None

        # Rate limiting
        self.last_request: Optional[datetime] = None
        self.min_interval = timedelta(seconds=0.1)  # 10 requests per second
        self._rate_lock = asyncio.Lock()

        log.info("SEC Ingestor initialized")

    async def initialize(self):
        """Initialize HTTP session"""
        self.session = aiohttp.ClientSession(
            headers={"Authorization": self.api_key},
            timeout=aiohttp.ClientTimeout(total=30),
        )

    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()

    async def fetch_company_filings(
        self,
        ticker: str,
        cik: Optional[str] = None,
        forms: List[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[SECFiling]:
        """Fetch SEC filings for a company"""
        if not self.session:
            await self.initialize()

        # Respect rate limits
        await self._rate_limit()

        # Build query
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
            # Fetch from SEC API
            url = f"{self.base_url}/query"
            params = {
                "query": query,
                "from": "0",
                "size": "100",  # Maximum per request
                "sort": "filedAt:desc",
            }

            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                filings = []
                for filing_data in data.get("filings", []):
                    try:
                        filing = await self._process_filing(filing_data, ticker)
                        if filing:
                            filings.append(filing)
                    except Exception as e:
                        log.error(
                            "Failed to process filing",
                            filing_id=filing_data.get("id"),
                            error=str(e),
                        )

                log.info("Fetched company filings", ticker=ticker, count=len(filings))

                return filings

        except aiohttp.ClientError as e:
            log.error("SEC API request failed", ticker=ticker, error=str(e))
            return []

    async def _process_filing(
        self, filing_data: Dict[str, Any], ticker: str
    ) -> Optional[SECFiling]:
        """Process raw filing data into structured format"""
        # Extract basic info
        accession_number = filing_data.get("accessionNo")
        form_type = filing_data.get("formType")
        filing_date = datetime.fromisoformat(
            filing_data.get("filedAt").replace("Z", "+00:00")
        ).astimezone(timezone.utc)

        # Create entity ID
        entity_id = EntityID(
            entity_type=EntityType.FILING,
            identifier=accession_number,
            source="sec",
            timestamp=filing_date,
        )

        # Check if already processed
        existing = await self.entity_registry.get_entity(entity_id)
        if existing:
            log.debug("Filing already processed", accession=accession_number)
            # Return existing but check for updates
            return await self._check_for_updates(filing_data, existing[0])

        # Fetch filing text
        filing_text = await self._fetch_filing_text(filing_data.get("linkToTxt"))
        if not filing_text:
            return None

        # Parse filing
        parsed = self.parser.parse_filing(filing_text, form_type)

        # Create timepoint
        timepoint = TimePoint(
            effective_date=filing_date,
            source_timestamp=filing_date,
            recorded_date=datetime.now(timezone.utc),
        )

        # Coerce form_type to FormType when available and create SECFiling
        try:
            form_enum = FormType(form_type)
        except Exception:
            form_enum = form_type

        filing = SECFiling(
            entity_id=entity_id,
            timepoint=timepoint,
            form_type=form_enum,
            filing_date=filing_date,
            period_end_date=datetime.fromisoformat(
                filing_data.get("periodOfReport", filing_date.isoformat())
            ).astimezone(timezone.utc),
            accession_number=accession_number,
            file_number=filing_data.get("fileNumber"),
            amount=None,  # SEC filings don't have single amounts
            description=filing_data.get("description", ""),
            sections=parsed.get("sections", {}),
            risk_clauses=parsed.get("risk_clauses", []),
            metadata={
                "cik": filing_data.get("cik"),
                "company_name": filing_data.get("companyName"),
                "link_to_html": filing_data.get("linkToHtml"),
                "link_to_xbrl": filing_data.get("linkToXbrl"),
                "items": filing_data.get("items", ""),
                "size": len(filing_text),
            },
        )

        # Register company if not exists
        company_cik = filing_data.get("cik")
        if company_cik:
            company_id = EntityID(
                entity_type=EntityType.COMPANY, identifier=company_cik, source="sec"
            )

            company = Company(
                entity_id=company_id,
                name=filing_data.get("companyName", ""),
                ticker=ticker,
                cik=company_cik,
            )
            # Some Company models do not include a `metadata` field; attach metadata
            # to the payload that will be registered in the registry.
            company_metadata = {"sec_registered": True}

            try:
                company_payload = attr.asdict(company)
            except Exception:
                company_payload = getattr(company, "__dict__", {})
            # Ensure metadata exists on the payload
            if isinstance(company_payload, dict):
                company_payload.setdefault("metadata", {}).update(company_metadata)

            await self.entity_registry.register_entity(company_id, company_payload)

            relationship = EntityRelationship(
                source_entity_id=entity_id,
                target_entity_id=company_id,
                relationship_type="filed_by",
                established_by_run=uuid.uuid4(),  # Would be actual run ID
                established_at=datetime.now(timezone.utc),
            )

            await self.entity_registry.add_relationship(relationship)

        # Register filing
        try:
            filing_payload = attr.asdict(filing)
        except Exception:
            filing_payload = getattr(filing, "__dict__", {})

        await self.entity_registry.register_entity(entity_id, filing_payload)

        log.info("Processed SEC filing", form=form_type, accession=accession_number)

        return filing

    async def _fetch_filing_text(self, url: str) -> Optional[str]:
        """Fetch full filing text"""
        if not url or not self.session:
            return None

        await self._rate_limit()

        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError as e:
            log.error("Failed to fetch filing text", url=url, error=str(e))
            return None

    async def _check_for_updates(
        self, new_data: Dict[str, Any], existing_id: EntityID
    ) -> Optional[SECFiling]:
        """Check if filing has been amended/updated"""
        # Check for amendments
        if new_data.get("amendmentNo"):
            # This is an amendment, process diff
            amendment_text = await self._fetch_filing_text(new_data.get("linkToTxt"))

            if amendment_text:
                # Get original filing
                existing_data = await self.entity_registry.get_entity(existing_id)
                if existing_data:
                    # Create diff
                    diff = self.diff_engine.create_diff(
                        existing_data[1].get("full_text", ""), amendment_text
                    )

                    # Update entity with amendment info
                    updated_metadata = existing_data[1].copy()
                    updated_metadata["amendments"] = updated_metadata.get(
                        "amendments", []
                    ) + [
                        {
                            "amendment_no": new_data.get("amendmentNo"),
                            "filed_at": new_data.get("filedAt"),
                            "diff_summary": diff.get("summary", {}),
                        }
                    ]

                    await self.entity_registry.register_entity(
                        existing_id, updated_metadata
                    )

                    log.info(
                        "Processed filing amendment",
                        accession=existing_id.identifier,
                        amendment=new_data.get("amendmentNo"),
                    )

        return None  # Return None since we already have this filing

    async def _rate_limit(self):
        """Implement rate limiting"""
        # Rate limiter uses UTC-aware timestamps and a lock for concurrency
        async with self._rate_lock:
            now = datetime.now(timezone.utc)
            if self.last_request:
                elapsed = now - self.last_request
                if elapsed < self.min_interval:
                    wait_time = (self.min_interval - elapsed).total_seconds()
                    await asyncio.sleep(wait_time)
            self.last_request = now

    async def fetch_and_diff(
        self, ticker: str, form_type: str, compare_with: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Fetch latest filing and diff with previous"""
        # Fetch latest filing
        filings = await self.fetch_company_filings(
            ticker=ticker, forms=[form_type], start_date=compare_with
        )

        if not filings:
            return {"error": "No filings found"}

        latest_filing = filings[0]

        # Find previous filing for comparison
        if compare_with:
            previous_filings = await self.fetch_company_filings(
                ticker=ticker,
                forms=[form_type],
                end_date=compare_with,
                start_date=compare_with - timedelta(days=365),  # Previous year
            )

            if previous_filings:
                previous_filing = previous_filings[0]

                # Create diff
                diff = self.diff_engine.compare_filings(previous_filing, latest_filing)

                try:
                    latest_payload = attr.asdict(latest_filing)
                except Exception:
                    latest_payload = getattr(latest_filing, "__dict__", {})
                try:
                    previous_payload = attr.asdict(previous_filing)
                except Exception:
                    previous_payload = getattr(previous_filing, "__dict__", {})

                return {
                    "latest": latest_payload,
                    "previous": previous_payload,
                    "diff": diff,
                    "material_changes": self.diff_engine.identify_material_changes(
                        diff
                    ),
                }

        try:
            latest_payload = attr.asdict(latest_filing)
        except Exception:
            latest_payload = getattr(latest_filing, "__dict__", {})

        return {
            "latest": latest_payload,
            "previous": None,
            "diff": None,
            "material_changes": [],
        }


class SECIngestionAgent:
    """Agent wrapper for MAKER orchestrator"""

    def __init__(self, ingestor: SECIngestor):
        self.name = "sec_ingestor"
        self.agent_type = "finscan"
        self.ingestor = ingestor

    async def execute(self, context) -> Dict[str, Any]:
        """Execute SEC ingestion for specified companies"""
        companies = context.parameters.get("companies", [])
        forms = context.parameters.get("forms", ["10-K", "10-Q", "8-K"])

        results = {}

        for company in companies:
            try:
                filings = await self.ingestor.fetch_company_filings(
                    ticker=company.get("ticker"),
                    cik=company.get("cik"),
                    forms=forms,
                    start_date=company.get("start_date"),
                )

                results[company["ticker"]] = {
                    "count": len(filings),
                    "filings": [f.dict() for f in filings[:10]],  # Limit for response
                }

                # Add entity IDs to context
                for filing in filings:
                    context.output_entity_ids.append(filing.entity_id)

            except Exception as e:
                results[company["ticker"]] = {"error": str(e)}
                context.errors.append(
                    f"SEC ingestion failed for {company['ticker']}: {str(e)}"
                )

        return results

    def estimate_cost(self, context) -> float:
        """Estimate cost based on number of companies and forms"""
        companies = context.parameters.get("companies", [])

        # SEC API costs: ~$0.001 per filing
        base_cost = 0.001 * len(companies) * 3  # Assume 3 filings per company

        # Add overhead
        return base_cost + 0.01
