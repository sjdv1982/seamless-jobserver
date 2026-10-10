"""Celljoin HTTP handling and direct worker evaluation without live services."""
import asyncio
import json
import pytest
from seamless import Buffer, Checksum, CacheMissError
from seamless.checksum import celljoin as joins, expression as expressions
from seamless_dask import transformer_client
from seamless_remote import buffer_remote
import jobserver


class Request:
    def __init__(self, payload):
        self.payload = payload
    async def json(self):
        return self.payload


@pytest.fixture(autouse=True)
def local_worker(monkeypatch):
    expressions.get_expression_cache().clear()
    monkeypatch.setattr(transformer_client, 'get_seamless_dask_client', lambda: None)
    yield
    expressions.get_expression_cache().clear()


def request(checksum, celltype='plain', scratch=True):
    return Request({'celljoin_checksum': Checksum(checksum).hex(), 'celltype': celltype, 'scratch': scratch})


def call(payload):
    server = jobserver.JobServer('127.0.0.1', 0)
    return asyncio.run(server._run_celljoin(payload))


@pytest.mark.parametrize('scratch', [True, False])
def test_handler_forwards_identity_and_scratch(monkeypatch, scratch):
    observed = []
    async def dispatch(*args, **kwargs):
        observed.append((args, kwargs))
        return Checksum('ab' * 32)
    monkeypatch.setattr(jobserver.worker, 'dispatch_celljoin', dispatch)
    checksum = Checksum('12' * 32)
    response = call(request(checksum, scratch=scratch))
    assert response.status == 200
    assert json.loads(response.text) == {'result_checksum': 'ab' * 32}
    assert observed == [((checksum, 'plain'), {'scratch': scratch})]


@pytest.mark.parametrize('payload', [{}, {'celljoin_checksum': 'invalid', 'celltype': 'plain'},
    {'celljoin_checksum': '12' * 32, 'celltype': 4}])
def test_malformed_request_is_http_400(payload):
    assert call(Request(payload)).status == 400


def test_missing_definition_is_structured_cache_miss():
    checksum = Checksum('f1' * 32)
    response = call(request(checksum))
    assert response.status == 200
    error = json.loads(response.text)['error']
    assert error['kind'] == 'cache_miss'
    assert error['checksum'] == checksum.hex()


def make_definition(member):
    definition = joins.celljoin_buffer(joins.build_celljoin(None, {'a': member}))
    definition.tempref()
    return definition


def test_missing_member_is_structured_cache_miss():
    member = Checksum('f2' * 32)
    held = make_definition(member)
    response = call(request(held.get_checksum()))
    assert response.status == 200
    error = json.loads(response.text)['error']
    assert error['kind'] == 'cache_miss'
    assert error['checksum'] == member.hex()


@pytest.mark.parametrize('celltype', ['deepcell', 'deepfolder'])
def test_worker_refuses_deep_dispatch(celltype):
    held = make_definition(Checksum('f3' * 32))
    response = call(request(held.get_checksum(), celltype))
    assert response.status == 200
    assert json.loads(response.text)['error']['kind'] == 'expression_evaluation'


@pytest.mark.parametrize('scratch', [True, False])
def test_direct_worker_evaluates_and_publishes_only_nonscratch(monkeypatch, scratch):
    member = Buffer(42, 'plain')
    member.tempref()
    definition = make_definition(member.get_checksum())
    writes = []
    async def write(checksum, buffer):
        writes.append((checksum, buffer.get_value('plain')))
        return True
    monkeypatch.setattr(buffer_remote, 'write_buffer', write)
    response = call(request(definition.get_checksum(), scratch=scratch))
    assert response.status == 200
    result = Buffer({'a': 42}, 'plain').get_checksum()
    assert json.loads(response.text) == {'result_checksum': result.hex()}
    assert writes == ([] if scratch else [(result, {'a': 42})])
