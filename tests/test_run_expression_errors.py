import asyncio
import json

import pytest

from seamless import Buffer, Checksum, CacheMissError
from seamless.checksum import expression as expression_mod

import jobserver


class _FakeRequest:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


def _payload(checksum, path="a"):
    return {
        "input_checksum": Checksum(checksum).hex(),
        "path": path,
        "input_celltype": "plain",
        "celltype": "str",
    }


def test_missing_input_returns_structured_cache_miss():
    server = jobserver.JobServer("127.0.0.1", 0)
    missing = Checksum("f" * 64)

    response = asyncio.run(server._run_expression(_FakeRequest(_payload(missing))))

    assert response.status == 200
    body = json.loads(response.text)
    assert body["error"]["kind"] == "cache_miss"
    assert body["error"]["checksum"] == missing.hex()
    assert missing.hex() in body["error"]["message"]


def test_invalid_expression_returns_structured_evaluation_error():
    server = jobserver.JobServer("127.0.0.1", 0)
    source = Buffer({"a": "hello"}, "plain")
    source_checksum = source.get_checksum()
    source_ref = source.tempref()

    try:
        response = asyncio.run(
            server._run_expression(_FakeRequest(_payload(source_checksum, path="[")))
        )
    finally:
        source_ref.clear()

    assert response.status == 200
    body = json.loads(response.text)
    assert body["error"]["kind"] == "expression_evaluation"
    assert "Unclosed path bracket" in body["error"]["message"]
    assert body["error"].get("checksum") is None


def test_jobserver_forwards_the_requesters_scratch_decision(monkeypatch):
    server = jobserver.JobServer("127.0.0.1", 0)
    source_checksum = Checksum("d" * 64)
    observed = []

    async def dispatch(*args, **kwargs):
        observed.append(kwargs)
        return Checksum("e" * 64)

    monkeypatch.setattr(jobserver.worker, "dispatch_expression", dispatch)
    payload = {**_payload(source_checksum), "scratch": True}

    response = asyncio.run(server._run_expression(_FakeRequest(payload)))

    assert response.status == 200
    assert observed == [
        {"validator": None, "validator_language": None, "scratch": True}
    ]


def test_jobserver_merges_duplicate_requests(monkeypatch):
    server = jobserver.JobServer("127.0.0.1", 0)
    source = Buffer({"a": "one evaluation"}, "plain")
    source_checksum = source.get_checksum()
    source_ref = source.tempref()
    original_evaluate = expression_mod.evaluate_expression_async
    original_finish = expression_mod._evaluate_expression_after_validation
    both_entered = asyncio.Event()
    entered = 0
    evaluations = 0

    async def synchronized_evaluate(*args, **kwargs):
        nonlocal entered
        entered += 1
        if entered == 2:
            both_entered.set()
        await both_entered.wait()
        return await original_evaluate(*args, **kwargs)

    def count_evaluation(*args, **kwargs):
        nonlocal evaluations
        evaluations += 1
        return original_finish(*args, **kwargs)

    monkeypatch.setattr(
        expression_mod, "evaluate_expression_async", synchronized_evaluate
    )
    monkeypatch.setattr(
        expression_mod, "_evaluate_expression_after_validation", count_evaluation
    )

    async def main():
        request = _FakeRequest(_payload(source_checksum))
        return await asyncio.gather(
            server._run_expression(request),
            server._run_expression(request),
        )

    try:
        responses = asyncio.run(main())
    finally:
        source_ref.clear()

    expected = Buffer("one evaluation", "str").get_checksum().hex()
    assert [json.loads(response.text)["result_checksum"] for response in responses] == [
        expected,
        expected,
    ]
    assert evaluations == 1


def test_expression_and_transformation_errors_share_one_envelope(monkeypatch):
    from seamless.error_envelope import encode_error
    missing = Checksum("e" * 64)
    server = jobserver.JobServer("127.0.0.1", 0)

    async def fail(*args, **kwargs):
        raise CacheMissError(missing)

    monkeypatch.setattr(server, "_run_transformation_task", fail)
    monkeypatch.setattr(jobserver.worker, "dispatch_expression", fail)

    async def main():
        expression = await server._run_expression(_FakeRequest(_payload(missing)))
        transformation = await server._run_transformation(_FakeRequest({
            "transformation_dict": {}, "tf_checksum": missing.hex(),
            "record": jobserver._STARTUP_RECORD_MODE,
        }))
        return expression, transformation

    for response in asyncio.run(main()):
        assert response.status == 200
        assert json.loads(response.text) == encode_error(CacheMissError(missing))
