"""Contract: the jobserver's own membership set (contracts/cancellation.md).

Service-free: the jobserver's request handlers are driven directly with fake
requests (same pattern as ``test_record_mode.py``), and worker dispatch is
replaced by a gate, so "running", "left" and "killed" are all observable
without starting a jobserver process.

Rules pinned here (cancellation.md):
- "The three sites" — the jobserver has an independent set; a member is a
  client identified by the member id it sends.
- "The two operations" — softcancel removes one member; above zero the run
  continues; at zero the underlying run is cancelled; on a completed or
  forgotten checksum softcancel is a no-op.
- "The two operations" — hard cancel kills the run for every member.
- Constraint 1 — the leaf kill happens when *this* set empties (the jobserver
  is co-located with its worker slots, so it is the leaf).
- Constraint 2 — a leaked member (one that never softcancels) causes
  under-cancellation: the run completes; no peer is killed.
"""

import asyncio
import json

import pytest

import jobserver
from seamless import Checksum


TF_DICT = {"__language__": "python", "__output__": ("result", "mixed", None)}
RESULT = "e" * 64


class _FakeRequest:
    def __init__(self, payload=None, *, match_info=None):
        self.match_info = match_info or {}
        self._payload = payload

    async def json(self):
        return self._payload


class _Gate:
    """Stand-in for worker dispatch: blocks until released, counts calls."""

    def __init__(self):
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = False

    async def __call__(self, *args, **kwargs):
        self.calls += 1
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return Checksum(RESULT)


@pytest.fixture
def server(monkeypatch):
    srv = jobserver.JobServer("127.0.0.1", 0)
    gate = _Gate()
    worker_kills = []
    monkeypatch.setattr(jobserver.worker, "dispatch_to_workers", gate)
    monkeypatch.setattr(
        jobserver.worker,
        "cancel_by_checksum",
        lambda tf_checksum: worker_kills.append(Checksum(tf_checksum).hex()) or True,
    )
    # No Dask client: the jobserver's hard cancel must not reach a real one.
    import seamless_dask.transformer_client as tc

    monkeypatch.setattr(tc, "get_seamless_dask_client", lambda: None)
    srv._gate = gate
    srv._worker_kills = worker_kills
    return srv


def _run(srv, tf_checksum, member_id):
    payload = {
        "transformation_dict": TF_DICT,
        "tf_checksum": tf_checksum,
        "tf_dunder": {},
        "scratch": True,  # skip result-buffer resolution: no hashserver needed
        "record": False,
        "member_id": member_id,
    }
    return asyncio.ensure_future(srv._run_transformation(_FakeRequest(payload)))


async def _softcancel(srv, tf_checksum, member_id):
    payload = {} if member_id is None else {"member_id": member_id}
    return await srv._softcancel_transformation(
        _FakeRequest(payload, match_info={"tf_checksum": tf_checksum})
    )


async def _hardcancel(srv, tf_checksum):
    return await srv._cancel_transformation(
        _FakeRequest(None, match_info={"tf_checksum": tf_checksum})
    )


def _body(response):
    return json.loads(response.text)


async def _both_attached(srv, tf_checksum, n=2):
    await asyncio.wait_for(srv._gate.started.wait(), 5)
    for _ in range(200):
        entry = srv._active_transformations.get(tf_checksum)
        if entry is not None and len(entry["members"]) >= n:
            return entry
        await asyncio.sleep(0.005)
    raise AssertionError("members did not attach")


def test_jobserver_dedups_members_into_one_run(server):
    tfc = "1" * 64

    async def main():
        a = _run(server, tfc, "member-a")
        b = _run(server, tfc, "member-b")
        await _both_attached(server, tfc)
        server._gate.release.set()
        ra, rb = await a, await b
        assert ra.status == rb.status == 200
        assert _body(ra)["result_checksum"] == _body(rb)["result_checksum"] == RESULT

    asyncio.run(main())
    assert server._gate.calls == 1


def test_jobserver_softcancel_one_member_peer_survives(server):
    """softcancel above zero: pure deregistration, the run continues."""
    tfc = "2" * 64

    async def main():
        a = _run(server, tfc, "member-a")
        b = _run(server, tfc, "member-b")
        await _both_attached(server, tfc)
        resp = await _softcancel(server, tfc, "member-a")
        assert resp.status == 200
        assert _body(resp)["canceled"] is False  # set not empty: nothing killed
        assert server._active_transformations[tfc]["members"] == {"member-b"}
        assert not server._gate.cancelled
        assert server._worker_kills == []
        server._gate.release.set()
        rb = await b
        assert rb.status == 200
        assert _body(rb)["result_checksum"] == RESULT
        await a

    asyncio.run(main())
    assert server._gate.calls == 1


def test_jobserver_all_softcancel_kills_at_the_leaf(server):
    """Constraint 1: when the jobserver's own set empties, the run is killed
    there (the jobserver is the leaf, co-located with its worker slots)."""
    tfc = "3" * 64

    async def main():
        a = _run(server, tfc, "member-a")
        b = _run(server, tfc, "member-b")
        await _both_attached(server, tfc)
        assert _body(await _softcancel(server, tfc, "member-a"))["canceled"] is False
        resp = await _softcancel(server, tfc, "member-b")
        assert _body(resp)["canceled"] is True
        for fut in (a, b):
            r = await asyncio.wait_for(fut, 5)
            assert "result_checksum" not in r.text
        assert server._gate.cancelled
        assert server._worker_kills == [tfc]

    asyncio.run(main())


def test_jobserver_leaked_member_is_benign_under_cancellation(server):
    """Constraint 2: a member that never deregisters (crashed client) keeps the
    run alive for itself; the other member's softcancel kills nothing."""
    tfc = "4" * 64

    async def main():
        leaked = _run(server, tfc, "crashed-client")
        b = _run(server, tfc, "member-b")
        await _both_attached(server, tfc)
        assert _body(await _softcancel(server, tfc, "member-b"))["canceled"] is False
        assert not server._gate.cancelled
        server._gate.release.set()
        r = await leaked
        assert r.status == 200
        assert _body(r)["result_checksum"] == RESULT
        await b

    asyncio.run(main())
    assert server._worker_kills == []


def test_jobserver_hard_cancel_kills_every_member(server):
    tfc = "5" * 64

    async def main():
        a = _run(server, tfc, "member-a")
        b = _run(server, tfc, "member-b")
        await _both_attached(server, tfc)
        resp = await _hardcancel(server, tfc)
        assert _body(resp)["canceled"] is True
        for fut in (a, b):
            r = await asyncio.wait_for(fut, 5)
            assert "result_checksum" not in r.text
            assert "cancel" in r.text.lower()
        assert server._gate.cancelled
        assert server._active_transformations[tfc]["members"] == set()
        assert server._worker_kills == [tfc]  # the jobserver is the leaf

    asyncio.run(main())


def test_jobserver_softcancel_noop_on_unknown_member_and_completed_run(server):
    tfc = "6" * 64

    async def main():
        # Forgotten / never-seen checksum.
        assert _body(await _softcancel(server, "7" * 64, "nobody"))["canceled"] is False
        a = _run(server, tfc, "member-a")
        await _both_attached(server, tfc, n=1)
        # A member id that is not in the set is not a member.
        assert _body(await _softcancel(server, tfc, "stranger"))["canceled"] is False
        assert not server._gate.cancelled
        server._gate.release.set()
        assert (await a).status == 200
        # Completed run: softcancel of the former member is a no-op.
        assert _body(await _softcancel(server, tfc, "member-a"))["canceled"] is False

    asyncio.run(main())
    assert server._worker_kills == []


def test_jobserver_softcancel_requires_a_member_id(server):
    """A caller only ever softcancels its *own* participation: a softcancel
    without a member id is rejected, not treated as a kill."""
    tfc = "8" * 64

    async def main():
        a = _run(server, tfc, "member-a")
        await _both_attached(server, tfc, n=1)
        resp = await _softcancel(server, tfc, None)
        assert resp.status == 400
        assert not server._gate.cancelled
        server._gate.release.set()
        assert (await a).status == 200

    asyncio.run(main())


def test_jobserver_resubmission_after_leaf_kill_is_a_fresh_run(server):
    """Cancellation is not a state to clear: after the set emptied and the run
    was killed, the same tf_checksum simply runs again."""
    tfc = "9" * 64

    async def main():
        a = _run(server, tfc, "member-a")
        await _both_attached(server, tfc, n=1)
        assert _body(await _softcancel(server, tfc, "member-a"))["canceled"] is True
        await asyncio.wait_for(a, 5)
        server._gate.started.clear()
        server._gate.release.set()
        b = _run(server, tfc, "member-b")
        r = await asyncio.wait_for(b, 5)
        assert r.status == 200
        assert _body(r)["result_checksum"] == RESULT

    asyncio.run(main())
    assert server._gate.calls == 2
