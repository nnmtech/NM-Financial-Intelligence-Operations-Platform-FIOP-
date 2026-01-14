def test_registry_reconciler_loads():
    import importlib

    mod = importlib.import_module("scripts.registry_reconciler")
    assert hasattr(mod, "reconcile_once")
