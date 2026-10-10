import asyncio
import json

import jobserver
from seamless import Checksum


class Request:
    async def json(self):
        return {'transformation_dict': {'__language__': 'python'}, 'tf_checksum': '8' * 64, 'tf_dunder': {}, 'scratch': True, 'record': False}


def test_http_success_returns_separate_logs_and_dispatcher_runtime(monkeypatch):
    logs = {'stdout': 'output\n', 'stderr': 'error\n'}
    async def dispatch(*args, **kwargs):
        assert kwargs['with_logs'] is True
        return Checksum('9' * 64), logs
    monkeypatch.setattr(jobserver, '_STARTUP_RECORD_MODE', False)
    monkeypatch.setattr(jobserver, 'get_record_mode', lambda: False)
    monkeypatch.setattr(jobserver.worker, 'dispatch_to_workers', dispatch)
    response = asyncio.run(jobserver.JobServer('127.0.0.1', 0)._run_transformation(Request()))
    assert response.status == 200
    body = json.loads(response.text)
    assert body['result_checksum'] == '9' * 64
    assert body['logs'] == logs
    assert isinstance(body['record_runtime']['wall_time_seconds'], (int, float))
    assert body['record_runtime']['wall_time_seconds'] >= 0


def test_dask_status_completed_thin_tuple_with_logs_reports_done(monkeypatch):
    import concurrent.futures
    from types import SimpleNamespace
    import seamless_dask.transformer_client as transformer_client
    base = concurrent.futures.Future()
    base.set_result(None)
    thin = concurrent.futures.Future()
    thin.set_result(('a' * 64, 'b' * 64, None, {'stdout': 'out', 'stderr': '', 'wall_time_seconds': 1}))
    client = SimpleNamespace(get_transformation_futures=lambda checksum: SimpleNamespace(base=base, thin=thin))
    monkeypatch.setattr(transformer_client, 'get_seamless_dask_client', lambda: client)
    assert jobserver.JobServer('127.0.0.1', 0)._dask_transformation_status('a' * 64) == 'done'
