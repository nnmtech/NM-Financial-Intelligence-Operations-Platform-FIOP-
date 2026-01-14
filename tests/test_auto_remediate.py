def test_auto_remediate_imports():
    import importlib

    mod = importlib.import_module("scripts.auto_remediate")
    assert hasattr(mod, "detect_failing_tests")
