# FOIP Backup v1 — Human-readable Details

This file documents the workspace backup labeled `v1` created from this repository.

## Artifacts (repo root)
- `FOIP_backup_v1.tar.gz.enc` — AES-256-CBC encrypted tarball (PBKDF2, 100000 iterations). (~37M)
- `FOIP_backup_v1.tar.gz.sha256` — SHA256 checksum for the plaintext tarball.
- `backup_manifest.txt` — metadata produced at backup time.

## Manifest (values recorded)
```
tag: v1
created_utc: 2026-01-14T03:10:24.787282Z
commit: ab038be
sha256: 152b0dd3101ac40c3e2104146b0ccabcfdebaa6564d73fd3d487fdc22d0e1d7f  FOIP_backup_v1.tar.gz
size_bytes: 38651016
```

## Verification and decryption
1. If you have the passphrase file (not stored in this repo), decrypt to a temporary location:

```bash
openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -salt \
  -in FOIP_backup_v1.tar.gz.enc \
  -out FOIP_backup_v1.tar.gz \
  -pass file:FOIP_backup_v1.passphrase
```

2. Verify the decrypted tarball matches the recorded checksum:

```bash
sha256sum -c FOIP_backup_v1.tar.gz.sha256
```

3. Inspect or extract the tarball:

```bash
tar -tzf FOIP_backup_v1.tar.gz    # list
tar -xzf FOIP_backup_v1.tar.gz    # extract
```

Security note: the plaintext tarball and passphrase file were removed from the repository after encryption. If you regenerate a passphrase or store it offsite, keep it protected (password manager or secure vault).

## Recommended next steps
- Store `FOIP_backup_v1.tar.gz.enc` and `backup_manifest.txt` in offsite durable storage (S3/GCS/other). Consider adding a small lifecycle/retention policy.
- Keep the passphrase in a secure secret manager. Without the passphrase, the encrypted archive cannot be recovered.
- Optionally add an entry to your project README or backups index noting where the encrypted archive is stored.

## Contact
If you want, I can: upload the encrypted archive to a configured bucket, create a git commit referencing the backup, or regenerate a new encrypted backup with a fresh passphrase.
