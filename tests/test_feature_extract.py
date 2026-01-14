from pathlib import Path


def test_extract_features_basic():
    from scripts import feature_extract

    rec = feature_extract.extract_features_from_text("E1", "F1", "hello world")
    assert rec.word_count == 2

    # test process_new_filings no-op when storage missing
    out = Path("tmp_out")
    written = feature_extract.process_new_filings(out, do_write=False)
    assert written == []
