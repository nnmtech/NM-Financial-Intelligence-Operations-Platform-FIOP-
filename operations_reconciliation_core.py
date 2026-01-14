"""
Bank Reconciliation MVP
Integrates with FinScan for counterparty intelligence
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import attr

try:
    from foip_infra import log as _foip_log

    log = _foip_log.bind(module="operations_reconciliation_core")
except Exception:
    import structlog

    log = structlog.get_logger(__name__)

try:
    from shared_metadata_schema import (
        BankTransaction,
        Company,
        EntityID,
        EntityRelationship,
        EntityType,
        TimePoint,
    )
except Exception:
    # Minimal placeholders for environments without the canonical schema
    from enum import Enum as _Enum

    class EntityType(_Enum):
        TRANSACTION = "transaction"
        ARTIFACT = "artifact"
        COMPANY = "company"

    EntityID = object
    BankTransaction = object
    TimePoint = object
    Company = object
    EntityRelationship = object

try:
    from shared_metadata_registry import EntityRegistry
except Exception:
    EntityRegistry = object
from .connectors import BankAPIConnector
from .matchers import TransactionMatcher

log = structlog.get_logger(__name__)


class BankReconciliationEngine:
    """Core bank reconciliation engine"""

    def __init__(
        self,
        entity_registry: EntityRegistry,
        bank_connector: BankAPIConnector,
        transaction_matcher: TransactionMatcher,
    ):
        self.entity_registry = entity_registry
        self.bank_connector = bank_connector
        self.matcher = transaction_matcher
        # lifecycle tracking for orchestrator heuristics
        self.initialized = False

        # Reconciliation rules
        self.matching_rules = [
            {"type": "exact", "fields": ["amount", "date", "counterparty"]},
            {"type": "fuzzy", "fields": ["amount", "counterparty"], "tolerance": 0.01},
            {"type": "date_range", "fields": ["amount"], "days": 3},
        ]

        log.info("Bank Reconciliation Engine initialized")

    async def initialize(self):
        """Initialize any underlying connectors or matchers"""
        if self.initialized:
            return
        try:
            init_tasks = []
            if hasattr(self.bank_connector, "initialize") and callable(
                self.bank_connector.initialize
            ):
                init_tasks.append(self.bank_connector.initialize())
            if hasattr(self.matcher, "initialize") and callable(
                self.matcher.initialize
            ):
                init_tasks.append(self.matcher.initialize())

            if init_tasks:
                await asyncio.gather(*init_tasks)
            self.initialized = True
            log.info("BankReconciliationEngine initialized resources")
        except Exception:
            log.exception("Failed initializing BankReconciliationEngine resources")

    async def close(self):
        """Close any underlying connectors or matchers"""
        try:
            close_tasks = []
            if hasattr(self.bank_connector, "close") and callable(
                self.bank_connector.close
            ):
                close_tasks.append(self.bank_connector.close())
            if hasattr(self.matcher, "close") and callable(self.matcher.close):
                close_tasks.append(self.matcher.close())

            if close_tasks:
                await asyncio.gather(*close_tasks)
            self.initialized = False
            log.info("BankReconciliationEngine closed resources")
        except Exception:
            log.exception("Failed closing BankReconciliationEngine resources")

    async def reconcile_account(
        self,
        account_id: str,
        start_date: datetime,
        end_date: datetime,
        ledger_entries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Reconcile bank transactions with ledger entries"""
        log.info(
            "Starting reconciliation",
            account=account_id,
            date_range=f"{start_date} to {end_date}",
        )

        # Fetch bank transactions
        bank_transactions = await self.bank_connector.fetch_transactions(
            account_id=account_id, start_date=start_date, end_date=end_date
        )

        # Process and enrich transactions
        processed_transactions = []
        for tx in bank_transactions:
            processed = await self._process_transaction(tx, account_id)
            if processed:
                processed_transactions.append(processed)

        # Match transactions
        reconciliation_result = await self.matcher.match_transactions(
            bank_transactions=processed_transactions,
            ledger_entries=ledger_entries,
            rules=self.matching_rules,
        )

        # Generate reconciliation report
        report = await self._generate_reconciliation_report(
            reconciliation_result, account_id, start_date, end_date
        )

        # Store reconciliation artifacts
        await self._store_reconciliation_artifacts(
            account_id, report, processed_transactions
        )

        log.info(
            "Reconciliation completed",
            account=account_id,
            matched=report["summary"]["matched_count"],
            unmatched=report["summary"]["unmatched_count"],
        )

        return report

    async def _process_transaction(
        self, raw_transaction: Dict[str, Any], account_id: str
    ) -> Optional[BankTransaction]:
        """Process raw bank transaction into enriched entity"""
        # Create entity ID
        transaction_id = raw_transaction.get("transaction_id")
        if not transaction_id:
            return None

        # Normalize timestamps to UTC
        tx_date = datetime.fromisoformat(raw_transaction.get("date"))
        try:
            tx_date = tx_date.astimezone(timezone.utc)
        except Exception:
            tx_date = datetime.now(timezone.utc)

        entity_id = EntityID(
            entity_type=EntityType.TRANSACTION,
            identifier=f"{account_id}:{transaction_id}",
            source="bank_api",
            timestamp=tx_date,
        )

        # Check if already processed
        existing = await self.entity_registry.get_entity(entity_id)
        if existing:
            log.debug("Transaction already processed", transaction_id=transaction_id)
            # Could check for updates/status changes
            return None

        # Create timepoint
        # Timepoint values normalized to UTC
        posted = None
        if raw_transaction.get("posted_date"):
            try:
                posted = datetime.fromisoformat(
                    raw_transaction.get("posted_date")
                ).astimezone(timezone.utc)
            except Exception:
                posted = None

        timepoint = TimePoint(
            effective_date=tx_date,
            source_timestamp=posted,
            recorded_date=datetime.now(timezone.utc),
        )

        # Enrich with counterparty intelligence
        counterparty = raw_transaction.get("counterparty")
        sec_intelligence = await self._enrich_with_sec_intelligence(counterparty)

        # Create BankTransaction
        transaction = BankTransaction(
            entity_id=entity_id,
            timepoint=timepoint,
            amount=float(Decimal(raw_transaction.get("amount", "0"))),
            currency=raw_transaction.get("currency", "USD"),
            description=raw_transaction.get("description", ""),
            transaction_id=transaction_id,
            bank_account=account_id,
            counterparty=counterparty,
            transaction_type=raw_transaction.get("type", "unknown"),
            status="pending",
            sec_risk_score=sec_intelligence.get("risk_score"),
            metadata={
                "raw_transaction": raw_transaction,
                "enrichment_source": "sec_api",
                "category": raw_transaction.get("category"),
            },
        )

        # Register transaction (serialize attrs models)
        try:
            payload = attr.asdict(transaction)
        except Exception:
            payload = getattr(transaction, "__dict__", {})

        await self.entity_registry.register_entity(entity_id, payload)

        # Register counterparty company if exists in SEC data
        if sec_intelligence.get("company_entity_id"):
            company_relationship = EntityRelationship(
                source_entity_id=entity_id,
                target_entity_id=sec_intelligence["company_entity_id"],
                relationship_type="counterparty",
                confidence=sec_intelligence.get("confidence", 0.8),
                established_by_run=uuid.uuid4(),  # Would be actual run ID
                established_at=datetime.now(timezone.utc),
                metadata={"enrichment_method": "name_matching"},
            )

            await self.entity_registry.add_relationship(company_relationship)

            transaction.company_entity_id = sec_intelligence["company_entity_id"]

        log.debug(
            "Processed bank transaction",
            transaction_id=transaction_id,
            counterparty=counterparty,
        )

        return transaction

    async def _enrich_with_sec_intelligence(
        self, counterparty_name: str
    ) -> Dict[str, Any]:
        """Enrich transaction with SEC data about counterparty"""
        # This would integrate with FinScan's company search
        # For MVP, we'll implement basic name matching

        # Search for company in registry by name
        # In production, this would use FinScan's company search API
        return {
            "risk_score": 0.5,  # Placeholder
            "confidence": 0.7,
            "company_entity_id": None,  # Would be populated if match found
        }

    async def _generate_reconciliation_report(
        self,
        reconciliation_result: Dict[str, Any],
        account_id: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Dict[str, Any]:
        """Generate comprehensive reconciliation report"""
        matched = reconciliation_result.get("matched", [])
        unmatched_bank = reconciliation_result.get("unmatched_bank", [])
        unmatched_ledger = reconciliation_result.get("unmatched_ledger", [])

        # Calculate metrics
        total_bank = len(matched) + len(unmatched_bank)
        total_ledger = len(matched) + len(unmatched_ledger)

        # Identify exceptions
        exceptions = await self._identify_exceptions(
            matched, unmatched_bank, unmatched_ledger
        )

        # Calculate amounts
        bank_total = sum(tx.get("amount", 0) for tx in matched + unmatched_bank)
        ledger_total = sum(
            entry.get("amount", 0) for entry in matched + unmatched_ledger
        )
        variance = abs(bank_total - ledger_total)

        report = {
            "account_id": account_id,
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
            "summary": {
                "total_bank_transactions": total_bank,
                "total_ledger_entries": total_ledger,
                "matched_count": len(matched),
                "unmatched_bank_count": len(unmatched_bank),
                "unmatched_ledger_count": len(unmatched_ledger),
                "match_rate": len(matched) / max(total_bank, 1),
                "bank_total": bank_total,
                "ledger_total": ledger_total,
                "variance": variance,
                "reconciled": variance
                < 0.01,  # Consider reconciled if variance < 1 cent
            },
            "matched_transactions": matched,
            "unmatched_bank_transactions": unmatched_bank,
            "unmatched_ledger_entries": unmatched_ledger,
            "exceptions": exceptions,
            "recommendations": await self._generate_recommendations(
                matched, unmatched_bank, unmatched_ledger, exceptions
            ),
        }

        return report

    async def _identify_exceptions(
        self,
        matched: List[Dict],
        unmatched_bank: List[Dict],
        unmatched_ledger: List[Dict],
    ) -> List[Dict[str, Any]]:
        """Identify exceptional items needing review"""
        exceptions = []

        # Check matched transactions for anomalies
        for match in matched:
            tx = match.get("bank_transaction", {})
            ledger = match.get("ledger_entry", {})

            # Large amount check
            if abs(tx.get("amount", 0)) > 10000:  # $10k threshold
                exceptions.append(
                    {
                        "type": "large_transaction",
                        "transaction_id": tx.get("transaction_id"),
                        "amount": tx.get("amount"),
                        "description": f"Large transaction: ${tx.get('amount'):,.2f}",
                    }
                )

            # Date mismatch check
            tx_date = datetime.fromisoformat(tx.get("date"))
            ledger_date = datetime.fromisoformat(ledger.get("date"))
            date_diff = abs((tx_date - ledger_date).days)

            if date_diff > 7:
                exceptions.append(
                    {
                        "type": "date_mismatch",
                        "transaction_id": tx.get("transaction_id"),
                        "date_difference": date_diff,
                        "description": f"Date mismatch: {date_diff} days",
                    }
                )

        # Check unmatched transactions
        for tx in unmatched_bank:
            # Check for duplicates
            if await self._check_duplicate_transaction(tx):
                exceptions.append(
                    {
                        "type": "possible_duplicate",
                        "transaction_id": tx.get("transaction_id"),
                        "description": "Possible duplicate transaction",
                    }
                )

            # Check for unusual counterparties
            if self._is_unusual_counterparty(tx.get("counterparty")):
                exceptions.append(
                    {
                        "type": "unusual_counterparty",
                        "transaction_id": tx.get("transaction_id"),
                        "counterparty": tx.get("counterparty"),
                        "description": f"Unusual counterparty: {tx.get('counterparty')}",
                    }
                )

        return exceptions

    async def _generate_recommendations(
        self,
        matched: List[Dict],
        unmatched_bank: List[Dict],
        unmatched_ledger: List[Dict],
        exceptions: List[Dict],
    ) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []

        # High unmatched count
        if len(unmatched_bank) > 10:
            recommendations.append(
                f"High number of unmatched bank transactions ({len(unmatched_bank)}). "
                "Consider reviewing matching rules or checking for data quality issues."
            )

        # Large variance
        total_bank = sum(tx.get("amount", 0) for tx in matched + unmatched_bank)
        total_ledger = sum(
            entry.get("amount", 0) for entry in matched + unmatched_ledger
        )
        variance = abs(total_bank - total_ledger)

        if variance > 1000:  # $1k variance threshold
            recommendations.append(
                f"Significant variance detected (${variance:,.2f}). "
                "Consider manual investigation of large transactions."
            )

        # Exception-based recommendations
        exception_types = {e["type"] for e in exceptions}

        if "large_transaction" in exception_types:
            recommendations.append(
                "Large transactions detected. Ensure proper authorization and documentation."
            )

        if "unusual_counterparty" in exception_types:
            recommendations.append(
                "Unusual counterparties detected. Consider updating vendor master list."
            )

        return recommendations

    async def _store_reconciliation_artifacts(
        self,
        account_id: str,
        report: Dict[str, Any],
        transactions: List[BankTransaction],
    ):
        """Store reconciliation results in entity registry"""
        # Create reconciliation entity
        recon_id = EntityID(
            entity_type=EntityType.ARTIFACT,
            identifier=f"reconciliation_{account_id}_{int(datetime.now(timezone.utc).timestamp())}",
            source="reconciliation_engine",
            timestamp=datetime.now(timezone.utc),
        )

        # Register reconciliation
        payload = {
            "type": "reconciliation_report",
            "account_id": account_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": report["summary"],
            "exception_count": len(report["exceptions"]),
        }
        await self.entity_registry.register_entity(recon_id, payload)

        # Link transactions to reconciliation
        for tx in transactions:
            relationship = EntityRelationship(
                source_entity_id=recon_id,
                target_entity_id=tx.entity_id,
                relationship_type="included_in_reconciliation",
                established_by_run=uuid.uuid4(),
                established_at=datetime.now(timezone.utc),
            )

            await self.entity_registry.add_relationship(relationship)


class BankReconciliationAgent:
    """Agent wrapper for MAKER orchestrator"""

    def __init__(self, engine: BankReconciliationEngine):
        self.name = "bank_reconciliation"
        self.agent_type = "operations"
        self.engine = engine

    async def execute(self, context) -> Dict[str, Any]:
        """Execute bank reconciliation for specified accounts"""
        accounts = context.parameters.get("accounts", [])

        results = {}

        for account in accounts:
            try:
                # Get ledger entries (from ERP/accounting system)
                ledger_entries = await self._fetch_ledger_entries(account)

                # Run reconciliation
                report = await self.engine.reconcile_account(
                    account_id=account["id"],
                    start_date=datetime.fromisoformat(account["start_date"]),
                    end_date=datetime.fromisoformat(account["end_date"]),
                    ledger_entries=ledger_entries,
                )

                results[account["id"]] = report

                # Add exceptions to context warnings
                for exception in report.get("exceptions", []):
                    context.warnings.append(
                        f"Account {account['id']}: {exception['description']}"
                    )

            except Exception as e:
                results[account["id"]] = {"error": str(e)}
                context.errors.append(
                    f"Bank reconciliation failed for {account['id']}: {str(e)}"
                )

        return results

    async def _fetch_ledger_entries(self, account: Dict) -> List[Dict]:
        """Fetch ledger entries from accounting system"""
        # In production, this would integrate with QuickBooks, Xero, etc.
        # For MVP, return mock data
        return [
            {
                "id": f"ledger_{i}",
                "date": account["start_date"],
                "amount": 1000.0 + (i * 100),
                "counterparty": f"Vendor_{i}",
                "description": f"Payment {i}",
                "account_code": "AP",
            }
            for i in range(5)
        ]

    def estimate_cost(self, context) -> float:
        """Estimate cost based on number of accounts and transactions"""
        accounts = context.parameters.get("accounts", [])

        # Bank API costs: ~$0.0001 per transaction
        # Assume 100 transactions per account
        estimated_transactions = len(accounts) * 100

        base_cost = 0.0001 * estimated_transactions

        # Add processing overhead
        return base_cost + 0.05
