import inspect


def test_ingest_worker_has_api():
    """Sanity test: the ingest worker exposes expected callables."""
    import importlib

    mod = importlib.import_module("scripts.ingest_worker")
    assert hasattr(mod, "run_once")
    assert hasattr(mod, "daemon")
    assert inspect.iscoroutinefunction(mod.daemon) or callable(mod.daemon)
