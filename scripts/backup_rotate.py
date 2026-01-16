"""Backup and rotation agent scaffold.

Design goals / safety:
- Non-destructive by default: actual encryption/upload only runs when
  `FOIP_DO_ENCRYPT=yes` and `FOIP_UPLOAD_TARGET` (or other env) are set.
- Keeps a rotation of encrypted archives, with configurable `KEEP` count.
- Uses client-side encryption via OpenSSL when enabled (mirrors project
  practices). Secrets (passphrase) must come from env or secret manager.
"""
from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

logger = logging.getLogger("foip.backup_rotate")


def make_workspace_tar(src_dir: Path, dest: Path, *, exclude: Iterable[str] | None = None) -> Path:
    """Create a gzipped tarball of `src_dir` at `dest` (dest is file path).

    This function returns the path to the created tar file.
    """
    exclude = set(exclude or [])
    with tempfile.TemporaryDirectory() as td:
        tmproot = Path(td) / "workspace-for-backup"
        shutil.copytree(src_dir, tmproot, ignore=shutil.ignore_patterns(*exclude))
        archive_base = str(dest.with_suffix("").with_suffix("").absolute())
        # shutil.make_archive expects base_name without final compression suffix
        archive_path = shutil.make_archive(archive_base, "gztar", root_dir=tmproot)
        return Path(archive_path)


def sha256_of_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def encrypt_with_openssl(in_file: Path, out_file: Path, passphrase: str) -> None:
    """Encrypt `in_file` to `out_file` using openssl AES-256-CBC with PBKDF2.

    This uses a blocking subprocess call. Caller must ensure passphrase is kept
    secret (not printed) and removed when no longer needed.
    """
    cmd = [
        "openssl",
        "enc",
        "-aes-256-cbc",
        "-pbkdf2",
        "-iter",
        "100000",
        "-salt",
        "-in",
        str(in_file),
        "-out",
        str(out_file),
        "-pass",
        f"pass:{passphrase}",
    ]
    subprocess.run(cmd, check=True)


def rotate_backups(directory: Path, pattern: str = "FOIP_backup_*.tar.gz.enc", keep: int = 7) -> List[Path]:
    """Delete older backup files matching `pattern` leaving `keep` newest.

    Returns list of deleted paths for audit.
    """
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    to_delete = files[keep:]
    deleted: List[Path] = []
    for p in to_delete:
        try:
            p.unlink()
            deleted.append(p)
            logger.info("rotated out %s", p)
        except Exception:
            logger.exception("failed to remove %s", p)
    return deleted


def create_encrypted_backup(root: Path, out_dir: Path, *, keep: int = 7, do_encrypt: bool = False) -> dict:
    """High-level helper: create tar, checksum, optionally encrypt, rotate.

    Returns metadata dict describing outcome.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    # Use timezone-aware UTC timestamp for filenames
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tar_name = f"FOIP_backup_{timestamp}.tar.gz"
    tar_path = out_dir / tar_name

    tar_path_created = make_workspace_tar(root, tar_path, exclude=[".git", "backup_v1", "dist"])  # type: ignore[arg-type]
    tar_sha = sha256_of_file(tar_path_created)

    enc_path = out_dir / f"{tar_path_created.stem}.enc"
    enc_sha = None
    passphrase = os.environ.get("FOIP_BACKUP_PASSPHRASE")

    if do_encrypt:
        if not passphrase:
            raise RuntimeError("FOIP_BACKUP_PASSPHRASE must be set to perform encryption")
        encrypt_with_openssl(tar_path_created, enc_path, passphrase)
        enc_sha = sha256_of_file(enc_path)
        # remove plaintext tar after encryption
        try:
            tar_path_created.unlink()
        except Exception:
            logger.exception("failed to remove plaintext tar %s", tar_path_created)

    # rotation
    rotate_backups(out_dir, keep=keep)

    meta = {"tar": str(tar_path_created), "tar_sha": tar_sha, "enc": str(enc_path) if enc_sha else None, "enc_sha": enc_sha}
    return meta


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    root = Path(os.getcwd())
    out = Path(os.environ.get("FOIP_BACKUP_OUT", os.getcwd()))
    do_encrypt = os.environ.get("FOIP_DO_ENCRYPT", "no").lower() in ("1", "yes", "true")
    keep = int(os.environ.get("FOIP_BACKUP_KEEP", "7"))
    logger.info("creating backup (encrypt=%s) to %s", do_encrypt, out)
    meta = create_encrypted_backup(root, out, keep=keep, do_encrypt=do_encrypt)
    logger.info("backup meta: %s", meta)
