"""Staging publisher scaffold.

Builds `foip-staging.tar.gz`, computes SHA256, and optionally uploads to a
remote target. Safe by default: upload only occurs when `FOIP_UPLOAD_TARGET`
is set and `FOIP_DO_UPLOAD=yes`.

Supported remote targets (best-effort):
- `s3://bucket/path` via `aws` CLI if available.
- `file:///local/path` will copy the artifact locally.

This scaffold is intentionally minimal and avoids adding remote SDK
dependencies; it is suitable for CI wiring or local manual runs.
"""
from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger("foip.staging_publish")


def make_staging_tar(src_root: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "foip-staging.tar.gz"
    # create tar.gz of the package dir
    archive_path = shutil.make_archive(str(target.with_suffix("").with_suffix("")), "gztar", root_dir=src_root, base_dir="foip")
    return Path(archive_path)


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def upload_target(local: Path, target: str) -> bool:
    """Attempt to upload `local` to `target`.

    Returns True on success, False otherwise. This function uses best-effort
    CLI helpers (e.g., `aws`) if present.
    """
    if target.startswith("s3://"):
        # prefer aws cli
        cmd = ["aws", "s3", "cp", str(local), target]
        try:
            subprocess.run(cmd, check=True)
            return True
        except Exception:
            logger.exception("aws s3 upload failed")
            return False
    if target.startswith("file://"):
        dest = Path(target[len("file://"):])
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local, dest)
        return True

    logger.warning("unknown upload target scheme; skipping: %s", target)
    return False


def publish(src_root: Path, out_dir: Path, *, do_upload: bool = False, upload_target_str: Optional[str] = None) -> dict:
    tar = make_staging_tar(src_root, out_dir)
    tar_sha = sha256_file(tar)
    sha_file = out_dir / "foip-staging.tar.gz.sha256"
    sha_file.write_text(f"{tar_sha}  {tar.name}\n")

    uploaded = False
    if do_upload and upload_target_str:
        uploaded = upload_target(tar, upload_target_str)

    return {"tar": str(tar), "sha": tar_sha, "uploaded": uploaded}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    src_root = Path("src")
    out_dir = Path("dist")
    do_upload = os.environ.get("FOIP_DO_UPLOAD", "no").lower() in ("1", "yes", "true")
    upload_target_str = os.environ.get("FOIP_UPLOAD_TARGET")
    meta = publish(src_root, out_dir, do_upload=do_upload, upload_target_str=upload_target_str)
    logger.info("staging publish meta: %s", meta)
