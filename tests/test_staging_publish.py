from pathlib import Path


def test_publish_creates_artifact(tmp_path: Path):
    from scripts import staging_publish

    src = tmp_path / "src"
    pkg = src / "foip"
    pkg.mkdir(parents=True)
    # create a dummy file in package
    (pkg / "__init__.py").write_text("# dummy")

    out = tmp_path / "dist"
    meta = staging_publish.publish(src, out, do_upload=False)
    assert Path(meta["tar"]).exists()
    assert len(meta["sha"]) == 64
