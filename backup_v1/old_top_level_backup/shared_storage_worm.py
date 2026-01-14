"""
Write-Once Read-Many (WORM) Storage Implementation
Immutable storage for all platform artifacts
"""

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
import aiofiles.os
import structlog

try:
    from foip_infra import log as _foip_log

    log = _foip_log.bind(module="shared_storage_worm")
except Exception:
    log = structlog.get_logger(__name__)

try:
    from shared_metadata_schema import Artifact, EntityID, EntityType
except Exception:
    # Minimal fallbacks for testing environments
    class EntityType:
        ARTIFACT = "artifact"

    EntityID = None
    Artifact = None


class WORMStorage:
    """Immutable storage for all platform artifacts"""

    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Create directory structure
        (self.base_path / "artifacts").mkdir(exist_ok=True)
        (self.base_path / "index").mkdir(exist_ok=True)
        (self.base_path / "metadata").mkdir(exist_ok=True)

        log.info("WORM Storage initialized", base_path=str(base_path))

    async def store_artifact(
        self,
        artifact_type: str,
        content: Dict[str, Any],
        run_id: uuid.UUID,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> EntityID:
        """Store an immutable artifact"""
        # Generate content hash
        content_bytes = json.dumps(content, sort_keys=True).encode("utf-8")
        content_hash = hashlib.sha256(content_bytes).hexdigest()

        # Create artifact ID (prefer canonical EntityID/EntityType)
        if EntityID is not None:
            try:
                et = getattr(EntityType, "ARTIFACT", "artifact")
            except Exception:
                et = "artifact"

            artifact_id = EntityID(
                entity_type=et,
                identifier=f"{artifact_type}_{content_hash[:16]}",
                source="worm_storage",
                timestamp=datetime.now(timezone.utc),
            )
        else:
            # Minimal fallback object for tests
            artifact_id = type("E", (), {})()
            artifact_id.identifier = f"{artifact_type}_{content_hash[:16]}"

        # Create storage path
        timestamp = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        filename = f"{content_hash}.json"
        storage_path = self.base_path / "artifacts" / timestamp / filename

        # Ensure directory exists
        storage_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if already exists (idempotent)
        if storage_path.exists():
            log.debug("Artifact already exists", hash=content_hash)
            return artifact_id

        # Store artifact
        artifact_data = {
            "artifact_id": artifact_id.identifier,
            "artifact_type": artifact_type,
            "run_id": str(run_id),
            "content_hash": content_hash,
            "content": content,
            "metadata": metadata or {},
            "stored_at": datetime.now(timezone.utc).isoformat(),
            "storage_path": str(storage_path.relative_to(self.base_path)),
        }

        async with aiofiles.open(storage_path, "w") as f:
            await f.write(json.dumps(artifact_data, indent=2))

        # Store metadata index
        await self._index_artifact(artifact_id, artifact_data)

        log.info(
            "Artifact stored", type=artifact_type, hash=content_hash[:8], run_id=run_id
        )

        return artifact_id

    async def retrieve_artifact(
        self,
        run_id: Optional[uuid.UUID] = None,
        artifact_type: Optional[str] = None,
        content_hash: Optional[str] = None,
        artifact_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Retrieve artifact by various criteria"""
        # Find artifact path
        artifact_path = await self._find_artifact_path(
            run_id, artifact_type, content_hash, artifact_id
        )

        if not artifact_path or not artifact_path.exists():
            log.warning(
                "Artifact not found",
                run_id=run_id,
                type=artifact_type,
                hash=content_hash,
            )
            return None

        # Read artifact
        async with aiofiles.open(artifact_path, "r") as f:
            data = json.loads(await f.read())

        # Verify hash
        if content_hash and data.get("content_hash") != content_hash:
            log.error(
                "Artifact hash mismatch",
                expected=content_hash,
                actual=data.get("content_hash"),
            )
            return None

        return data

    async def retrieve_run_artifacts(self, run_id: uuid.UUID) -> List[Dict[str, Any]]:
        """Retrieve all artifacts for a run"""
        index_path = self.base_path / "index" / "runs" / f"{run_id}.json"

        if not index_path.exists():
            return []

        async with aiofiles.open(index_path, "r") as f:
            run_index = json.loads(await f.read())

        artifacts = []
        for artifact_info in run_index.get("artifacts", []):
            artifact_path = self.base_path / artifact_info["storage_path"]

            if artifact_path.exists():
                async with aiofiles.open(artifact_path, "r") as f:
                    artifact_data = json.loads(await f.read())
                    artifacts.append(artifact_data)

        return artifacts

    async def _index_artifact(
        self, artifact_id: EntityID, artifact_data: Dict[str, Any]
    ):
        """Index artifact for efficient retrieval"""
        run_id = artifact_data["run_id"]

        # Index by run
        run_index_path = self.base_path / "index" / "runs" / f"{run_id}.json"
        run_index_path.parent.mkdir(parents=True, exist_ok=True)

        run_index: Dict[str, Any] = {"artifacts": []}
        if run_index_path.exists():
            async with aiofiles.open(run_index_path, "r") as f:
                run_index = json.loads(await f.read())

        run_index["artifacts"].append(
            {
                "artifact_id": artifact_id.identifier,
                "artifact_type": artifact_data["artifact_type"],
                "content_hash": artifact_data["content_hash"],
                "storage_path": artifact_data["storage_path"],
                "stored_at": artifact_data["stored_at"],
            }
        )

        async with aiofiles.open(run_index_path, "w") as f:
            await f.write(json.dumps(run_index, indent=2))

        # Index by type
        artifact_type = artifact_data["artifact_type"]
        type_index_path = self.base_path / "index" / "types" / f"{artifact_type}.json"
        type_index_path.parent.mkdir(parents=True, exist_ok=True)

        type_index: Dict[str, Any] = {"artifacts": []}
        if type_index_path.exists():
            async with aiofiles.open(type_index_path, "r") as f:
                type_index = json.loads(await f.read())

        type_index["artifacts"].append(
            {
                "artifact_id": artifact_id.identifier,
                "run_id": run_id,
                "content_hash": artifact_data["content_hash"],
                "stored_at": artifact_data["stored_at"],
            }
        )

        async with aiofiles.open(type_index_path, "w") as f:
            await f.write(json.dumps(type_index, indent=2))

        # Index by hash (for deduplication)
        content_hash = artifact_data["content_hash"]
        hash_index_path = (
            self.base_path
            / "index"
            / "hashes"
            / f"{content_hash[:2]}"
            / f"{content_hash}.json"
        )
        hash_index_path.parent.mkdir(parents=True, exist_ok=True)

        hash_index = {
            "artifact_id": artifact_id.identifier,
            "run_id": run_id,
            "artifact_type": artifact_type,
            "storage_path": artifact_data["storage_path"],
            "stored_at": artifact_data["stored_at"],
        }

        async with aiofiles.open(hash_index_path, "w") as f:
            await f.write(json.dumps(hash_index, indent=2))

    async def _find_artifact_path(
        self,
        run_id: Optional[uuid.UUID] = None,
        artifact_type: Optional[str] = None,
        content_hash: Optional[str] = None,
        artifact_id: Optional[str] = None,
    ) -> Optional[Path]:
        """Find artifact path using available criteria"""
        if content_hash:
            # Direct hash lookup
            hash_index_path = (
                self.base_path
                / "index"
                / "hashes"
                / f"{content_hash[:2]}"
                / f"{content_hash}.json"
            )
            if hash_index_path.exists():
                async with aiofiles.open(hash_index_path, "r") as f:
                    index_data = json.loads(await f.read())
                    return self.base_path / index_data["storage_path"]

        elif run_id and artifact_type:
            # Run + type lookup
            run_index_path = self.base_path / "index" / "runs" / f"{run_id}.json"
            if run_index_path.exists():
                async with aiofiles.open(run_index_path, "r") as f:
                    run_index = json.loads(await f.read())

                for artifact in run_index.get("artifacts", []):
                    if artifact["artifact_type"] == artifact_type:
                        return self.base_path / artifact["storage_path"]

        elif artifact_id:
            # Full scan (inefficient, should use better indexing in production)
            for artifact_file in (self.base_path / "artifacts").rglob("*.json"):
                async with aiofiles.open(artifact_file, "r") as f:
                    data = json.loads(await f.read())
                    if data.get("artifact_id") == artifact_id:
                        return artifact_file

        return None

    async def verify_integrity(self, artifact_id: str) -> bool:
        """Verify artifact integrity using hash"""
        artifact_path = await self._find_artifact_path(artifact_id=artifact_id)

        if not artifact_path:
            return False

        async with aiofiles.open(artifact_path, "r") as f:
            data = json.loads(await f.read())

        # Recalculate hash
        content_bytes = json.dumps(data["content"], sort_keys=True).encode("utf-8")
        calculated_hash = hashlib.sha256(content_bytes).hexdigest()

        return calculated_hash == data["content_hash"]

    async def get_artifact_metadata(self, artifact_id: str) -> Optional[Dict[str, Any]]:
        """Get metadata for an artifact without loading content"""
        artifact_path = await self._find_artifact_path(artifact_id=artifact_id)

        if not artifact_path:
            return None

        async with aiofiles.open(artifact_path, "r") as f:
            data = json.loads(await f.read())

        # Return metadata only (exclude content for efficiency)
        return {
            "artifact_id": data["artifact_id"],
            "artifact_type": data["artifact_type"],
            "run_id": data["run_id"],
            "content_hash": data["content_hash"],
            "stored_at": data["stored_at"],
            "metadata": data["metadata"],
        }
