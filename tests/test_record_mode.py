import asyncio
import json
import sys

import jobserver
import seamless.transformer as seamless_transformer
from seamless_config import select
from seamless_transformer.probe_index import RecordBucketError
from seamless_transformer.record_runtime import get_record_mode


class _FakeRequest:
    def __init__(self, payload=None, *, match_info=None):
        self.match_info = match_info or {}
        self._payload = payload

    async def json(self):
        return self._payload


def test_record_mode_missing_probe_blocks_before_worker_dispatch(monkeypatch):
    calls = []
    server = jobserver.JobServer("127.0.0.1", 0)

    async def _missing_probe(*args, **kwargs):
        calls.append(("probe", args, kwargs))
        raise RecordBucketError("missing bucket probe")

    async def _unexpected_dispatch(*args, **kwargs):
        calls.append(("dispatch", args, kwargs))
        raise AssertionError("jobserver dispatched before record probes existed")

    monkeypatch.setattr(jobserver, "get_record_mode", lambda: True)
    monkeypatch.setattr(jobserver, "_STARTUP_RECORD_MODE", True)
    monkeypatch.setattr(jobserver, "is_record_probe", lambda *args, **kwargs: False)
    monkeypatch.setattr(jobserver, "ensure_record_bucket_preconditions", _missing_probe)
    monkeypatch.setattr(jobserver.worker, "dispatch_to_workers", _unexpected_dispatch)

    response = asyncio.run(
        server._run_transformation(
            _FakeRequest(
                {
                    "transformation_dict": {
                        "__language__": "python",
                        "__output__": ("result", "mixed", None),
                    },
                    "tf_checksum": "1" * 64,
                    "tf_dunder": {},
                    "scratch": False,
                    "record": True,
                }
            )
        )
    )

    assert response.status == 500
    assert "missing bucket probe" in response.text
    assert [call[0] for call in calls] == ["probe"]


def test_record_mode_mismatch_blocks_before_worker_dispatch(monkeypatch):
    calls = []
    server = jobserver.JobServer("127.0.0.1", 0)
    monkeypatch.setattr(jobserver, "_STARTUP_RECORD_MODE", True)
    monkeypatch.setattr(jobserver.worker, "dispatch_to_workers", lambda *args, **kwargs: calls.append("dispatch"))

    response = asyncio.run(
        server._run_transformation(
            _FakeRequest(
                {
                    "transformation_dict": {
                        "__language__": "python",
                        "__output__": ("result", "mixed", None),
                    },
                    "tf_checksum": "1" * 64,
                    "tf_dunder": {},
                    "scratch": False,
                    "record": False,
                }
            )
        )
    )

    assert response.status == 409
    assert "Jobserver record mode mismatch" in response.text
    assert calls == []


def test_transformation_status_endpoint_reports_dask_status(monkeypatch):
    server = jobserver.JobServer("127.0.0.1", 0)
    monkeypatch.setattr(server, "_dask_transformation_status", lambda _checksum: "running")

    response = asyncio.run(
        server._transformation_status(
            _FakeRequest(match_info={"tf_checksum": "1" * 64})
        )
    )

    assert response.status == 200
    assert json.loads(response.text) == {"status": "running"}


def test_transformation_status_endpoint_reports_local_status():
    server = jobserver.JobServer("127.0.0.1", 0)

    class _Task:
        def cancelled(self):
            return False

        def done(self):
            return False

    server._active_transformations["3" * 64] = {
        "task": _Task(),
        "status": "running",
    }

    response = asyncio.run(
        server._transformation_status(
            _FakeRequest(match_info={"tf_checksum": "3" * 64})
        )
    )

    assert response.status == 200
    assert json.loads(response.text) == {"status": "running"}


def test_cancel_transformation_endpoint_cancels_local_task():
    server = jobserver.JobServer("127.0.0.1", 0)
    calls = []

    class _Task:
        def cancelled(self):
            return False

        def done(self):
            return False

        def cancel(self):
            calls.append("cancel")

    server._active_transformations["4" * 64] = {
        "task": _Task(),
        "status": "running",
    }

    response = asyncio.run(
        server._cancel_transformation(
            _FakeRequest(match_info={"tf_checksum": "4" * 64})
        )
    )

    assert response.status == 200
    assert json.loads(response.text) == {"canceled": True, "status": "canceled"}
    assert calls == ["cancel"]
    assert server._active_transformations["4" * 64]["status"] == "canceled"


def test_cancel_transformation_endpoint_uses_dask_client(monkeypatch):
    server = jobserver.JobServer("127.0.0.1", 0)
    calls = []

    class _FakeDaskClient:
        def cancel_by_checksum(self, checksum):
            calls.append(checksum.hex())
            return True

    monkeypatch.setitem(
        sys.modules,
        "seamless_dask.transformer_client",
        type(
            "_FakeTransformerClient",
            (),
            {"get_seamless_dask_client": staticmethod(lambda: _FakeDaskClient())},
        )(),
    )

    response = asyncio.run(
        server._cancel_transformation(
            _FakeRequest(match_info={"tf_checksum": "2" * 64})
        )
    )

    assert response.status == 200
    assert json.loads(response.text) == {"canceled": True, "status": "canceled"}
    assert calls == ["2" * 64]


def test_main_reasserts_record_after_startup_setup(monkeypatch, tmp_path):
    observed = []
    status_file = tmp_path / "jobserver.json"
    status_file.write_text(
        json.dumps(
            {
                "parameters": {
                    "record": True,
                    "database": [],
                    "buffer": [],
                }
            }
        ),
        encoding="utf-8",
    )

    class _FakeServer:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            observed.append(("start", select.get_record(), get_record_mode()))

        async def stop(self):
            observed.append(("stop", select.get_record(), get_record_mode()))

    class _FakeLoop:
        def run_forever(self):
            raise KeyboardInterrupt

        def run_until_complete(self, awaitable):
            return asyncio.run(awaitable)

    def _reset_record(*args, **kwargs):
        select.select_record(False)

    fake_seamless_config = type(
        "_FakeSeamlessConfig", (), {"set_remote_clients": _reset_record}
    )()

    monkeypatch.setattr(
        sys, "argv", ["seamless-jobserver", "--status-file", str(status_file)]
    )
    monkeypatch.setattr(seamless_transformer, "spawn", _reset_record)
    monkeypatch.setattr(jobserver, "JobServer", _FakeServer)
    monkeypatch.setattr(jobserver, "get_event_loop", lambda: _FakeLoop())
    monkeypatch.setattr(jobserver.seamless, "close", lambda: None)
    monkeypatch.setattr(jobserver.worker, "shutdown_workers", lambda: None)
    monkeypatch.setitem(sys.modules, "seamless.config", fake_seamless_config)

    select.select_record(False)
    jobserver.main()

    assert observed[0] == ("start", True, True)


def test_main_sets_remote_clients_in_remote_mode(monkeypatch, tmp_path):
    calls = []
    status_file = tmp_path / "jobserver.json"
    parameters = {
        "record": False,
        "database": [
            {
                "readonly": False,
                "url": "http://localhost:39389",
                "remote_url": "http://mbi-frontend.loria.fr:60941",
            }
        ],
        "buffer": [
            {
                "readonly": False,
                "url": "http://localhost:33093",
                "remote_url": "http://mbi-frontend.loria.fr:60085",
            }
        ],
    }
    status_file.write_text(json.dumps({"parameters": parameters}), encoding="utf-8")

    class _FakeServer:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            calls.append(("start",))

        async def stop(self):
            calls.append(("stop",))

    class _FakeLoop:
        def run_forever(self):
            raise KeyboardInterrupt

        def run_until_complete(self, awaitable):
            return asyncio.run(awaitable)

    def _set_remote_clients(clients, **kwargs):
        calls.append(("set_remote_clients", clients, kwargs))

    fake_seamless_config = type(
        "_FakeSeamlessConfig",
        (),
        {"set_remote_clients": staticmethod(_set_remote_clients)},
    )()

    monkeypatch.setattr(sys, "argv", ["seamless-jobserver", "--status-file", str(status_file)])
    monkeypatch.setattr(seamless_transformer, "spawn", lambda *args, **kwargs: None)
    monkeypatch.setattr(jobserver, "JobServer", _FakeServer)
    monkeypatch.setattr(jobserver, "get_event_loop", lambda: _FakeLoop())
    monkeypatch.setattr(jobserver.seamless, "close", lambda: None)
    monkeypatch.setattr(jobserver.worker, "shutdown_workers", lambda: None)
    monkeypatch.setitem(sys.modules, "seamless.config", fake_seamless_config)

    jobserver.main()

    assert calls[0] == ("set_remote_clients", parameters, {"in_remote": True})
    assert calls[1] == ("start",)
