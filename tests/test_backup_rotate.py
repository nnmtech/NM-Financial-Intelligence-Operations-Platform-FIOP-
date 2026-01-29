from pathlib import Path
import tempfile


def test_sha256_and_rotation(tmp_path: Path):
    from scripts import backup_rotate

    # create a small file and compute sha
    p = tmp_path / "small.txt"
    p.write_text("hello")
    sha = backup_rotate.sha256_of_file(p)
    assert len(sha) == 64

    # create dummy enc files to test rotation
    out = tmp_path / "out"
    out.mkdir()
    for i in range(5):
        (out / f"FOIP_backup_200{i}.tar.gz.enc").write_text("x")

    deleted = backup_rotate.rotate_backups(out, keep=3)
    # two removed
    assert len(deleted) == 2
