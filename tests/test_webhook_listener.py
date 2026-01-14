def test_webhook_listener_imports():
    import importlib

    mod = importlib.import_module("scripts.webhook_listener")
    assert hasattr(mod, "make_app")
