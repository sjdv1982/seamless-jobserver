import asyncio
import json
import sys

import jobserver
import seamless.transformer as seamless_transformer
from seamless_config import select
from seamless_transformer.probe_index import RecordBucketError
from seamless_transformer.record_runtime import get_record_mode


class _FakeRequest:
    def __init__(self, payload):
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
