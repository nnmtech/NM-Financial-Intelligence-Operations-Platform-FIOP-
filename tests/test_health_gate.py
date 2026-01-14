def test_health_gate_module_loads():
    import importlib

    mod = importlib.import_module("scripts.health_gate")
    assert hasattr(mod, "run_checks")
