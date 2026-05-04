from aiohttp import web
import asyncio
import contextlib
from datetime import datetime, timezone
import json
import os
import random
import resource
import signal
import socket
import sys
import time

STATUS_FILE_WAIT_TIMEOUT = 20.0
INACTIVITY_CHECK_INTERVAL = 1.0
_RESTARTABLE_REMOTE_CLIENT_ERROR_MARKERS = (
    "device or resource busy",
    "clientrestartrequirederror",
    "[errno 16]",
)

status_tracker = None
_PROCESS_STARTED_AT = datetime.now(timezone.utc)
_EXECUTION_RECORD_COUNTER = 0
_STARTUP_RECORD_MODE = False


class _LazyModuleProxy:
    def __init__(self, module_name: str):
        self._module_name = module_name

    def _load(self):
        import importlib

        return importlib.import_module(self._module_name)

    def __getattr__(self, name: str):
        return getattr(self._load(), name)


seamless = _LazyModuleProxy("seamless")
worker = _LazyModuleProxy("seamless_transformer.worker")


def get_event_loop():
    from seamless.util.get_event_loop import get_event_loop as _get_event_loop

    return _get_event_loop()


def ensure_record_bucket_preconditions(*args, **kwargs):
    from seamless_transformer.probe_index import ensure_record_bucket_preconditions

    return ensure_record_bucket_preconditions(*args, **kwargs)


def is_record_probe(*args, **kwargs):
    from seamless_transformer.probe_index import is_record_probe

    return is_record_probe(*args, **kwargs)


def get_record_mode():
    from seamless_transformer.record_runtime import get_record_mode

    return get_record_mode()


def _memory_peak_bytes():
    from seamless_transformer.record_utils import _memory_peak_bytes

    return _memory_peak_bytes()


def _process_create_time_epoch():
    from seamless_transformer.record_utils import _process_create_time_epoch

    return _process_create_time_epoch()


def _utcnow_iso():
    from seamless_transformer.record_utils import _utcnow_iso

    return _utcnow_iso()


def parse_remote_job_written(*args, **kwargs):
    from seamless_transformer.remote_job import parse_remote_job_written

    return parse_remote_job_written(*args, **kwargs)


def _process_started_at_iso() -> str:
    return _PROCESS_STARTED_AT.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _next_execution_record_index() -> int:
    global _EXECUTION_RECORD_COUNTER
    _EXECUTION_RECORD_COUNTER += 1
    return _EXECUTION_RECORD_COUNTER


def is_port_in_use(address, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((address, port)) == 0


def _is_restartable_remote_client_error_text(value) -> bool:
    if not isinstance(value, str):
        return False
    if parse_remote_job_written(value) is not None:
        return False
    lower = value.lower()
    return any(marker in lower for marker in _RESTARTABLE_REMOTE_CLIENT_ERROR_MARKERS)


def wait_for_status_file(path: str, timeout: float = STATUS_FILE_WAIT_TIMEOUT):
    deadline = time.monotonic() + timeout
    while True:
        try:
            with open(path, "r", encoding="utf-8") as status_stream:
                contents = json.load(status_stream)
                break
        except FileNotFoundError:
            if time.monotonic() >= deadline:
                print(
                    f"Status file '{path}' not found after {int(timeout)} seconds",
                    file=sys.stderr,
                )
                sys.exit(1)
            time.sleep(0.1)
            continue
        except json.JSONDecodeError as exc:
            print(
                f"Status file '{path}' is not valid JSON: {exc}",
                file=sys.stderr,
            )
            sys.exit(1)

    if not isinstance(contents, dict):
        print(
            f"Status file '{path}' must contain a JSON object",
            file=sys.stderr,
        )
        sys.exit(1)

    return contents


class StatusFileTracker:
    def __init__(self, path: str, base_contents: dict, port: int):
        self.path = path
        self._base_contents = dict(base_contents)
        self.port = port
        self.running_written = False

    def _write(self, payload: dict):
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as status_stream:
            json.dump(payload, status_stream)
            status_stream.write("\n")
        os.replace(tmp_path, self.path)

    def write_running(self):
        payload = dict(self._base_contents)
        payload["port"] = self.port
        payload["status"] = "running"
        self._write(payload)
        self._base_contents = payload
        self.running_written = True

    def write_failed(self):
        payload = dict(self._base_contents)
        payload["status"] = "failed"
        self._write(payload)


def raise_startup_error(exc: BaseException):
    if status_tracker and not status_tracker.running_written:
        status_tracker.write_failed()
    raise exc


def pick_random_free_port(host: str, start: int, end: int) -> int:
    if start < 0 or end > 65535:
        raise RuntimeError("--port-range values must be between 0 and 65535")
    if start > end:
        raise RuntimeError("--port-range START must be less than or equal to END")

    span = end - start + 1
    attempted = set()
    while len(attempted) < span:
        port = random.randint(start, end)
        if port in attempted:
            continue
        attempted.add(port)
        try:
            with socket.create_server((host, port), reuse_port=False):
                pass
        except OSError:
            continue
        return port

    raise RuntimeError(f"No free port available in range {start}-{end}")


class JobServer:
    future = None

    def __init__(
        self,
        host,
        port,
        *,
        timeout_seconds=None,
        status_tracker=None,
    ):
        self.host = host
        self.port = port
        self._timeout_seconds = timeout_seconds
        self._status_tracker = status_tracker
        self._timeout_task = None
        self._last_request = None
        self._runner = None
        self._site = None

    async def _start(self):
        if is_port_in_use(self.host, self.port):
            print("ERROR: %s port %d already in use" % (self.host, self.port))
            raise Exception

        app = web.Application(client_max_size=10e9)
        app.add_routes(
            [
                web.get("/", self._welcome),
                web.get("/healthcheck", self._healthcheck),
                web.get("/run-transformation", self._run_transformation),
            ]
        )
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        self._runner = runner
        self._site = site
        if self._status_tracker and not self._status_tracker.running_written:
            self._status_tracker.write_running()
        print(f"Welcome to the Seamless jobserver on {self.host}:{self.port}")
        sys.stdout.flush()
        if self._timeout_seconds is not None:
            self._last_request = time.monotonic()
            loop = asyncio.get_running_loop()
            self._timeout_task = loop.create_task(self._monitor_inactivity())

    def start(self):
        if self.future is not None:
            return
        loop = get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        coro = self._start()
        self.future = loop.create_task(coro)

    async def stop(self):
        if self._timeout_task:
            self._timeout_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._timeout_task
            self._timeout_task = None
        if self._site is not None:
            await self._site.stop()
            self._site = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def _monitor_inactivity(self):
        try:
            while True:
                await asyncio.sleep(INACTIVITY_CHECK_INTERVAL)
                if self._last_request is None:
                    continue
                if time.monotonic() - self._last_request >= self._timeout_seconds:
                    loop = asyncio.get_running_loop()
                    loop.call_soon(loop.stop)
                    break
        except asyncio.CancelledError:
            raise

    def _register_activity(self):
        if self._timeout_seconds is not None:
            self._last_request = time.monotonic()

    async def _healthcheck(self, _):
        self._register_activity()
        return web.Response(status=200, body="OK")

    async def _welcome(self, _):
        self._register_activity()
        return web.Response(status=200, body="Seamless jobserver is running")

    async def _run_transformation(self, request):
        from seamless import Buffer, Checksum

        self._register_activity()
        try:
            payload = await request.json()
        except Exception as exc:
            return web.Response(status=400, text=f"Invalid JSON: {exc}")

        try:
            transformation_dict = payload["transformation_dict"]
            tf_checksum = Checksum(payload["tf_checksum"])
            tf_dunder = payload.get("tf_dunder", {})
            scratch = bool(payload.get("scratch", False))
            request_record_mode = bool(payload.get("record", False))
        except Exception as exc:
            return web.Response(status=400, text=f"Invalid payload: {exc}")

        if request_record_mode != _STARTUP_RECORD_MODE:
            return web.Response(
                status=409,
                text=(
                    "Jobserver record mode mismatch: "
                    f"client requested record={request_record_mode}, "
                    f"but jobserver started with record={_STARTUP_RECORD_MODE}. "
                    "Restart the jobserver after changing record mode."
                ),
            )

        tf_checksum_hex = tf_checksum.hex()
        print(f"[jobserver] Received transformation {tf_checksum_hex}", flush=True)
        started_at = _utcnow_iso()
        wall_start = time.perf_counter()
        cpu_start = os.times()
        from seamless_transformer.transformation_cache import (
            start_gpu_memory_sampler,
            stop_gpu_memory_sampler,
        )

        gpu_sampler = start_gpu_memory_sampler()
        gpu_memory_peak_bytes = None
        result_checksum = None
        retry_count = 0
        try:
            record_mode = get_record_mode()
            record_probe = is_record_probe(transformation_dict, tf_dunder)
            if record_mode and not record_probe:
                try:
                    await ensure_record_bucket_preconditions(
                        transformation_dict,
                        tf_dunder,
                        execution="remote",
                    )
                except Exception as exc:
                    return web.Response(status=500, text=str(exc))
            for attempt in range(2):
                try:
                    result_checksum = await worker.dispatch_to_workers(
                        transformation_dict,
                        tf_checksum=tf_checksum,
                        tf_dunder=tf_dunder,
                        scratch=scratch,
                    )
                except Exception as exc:
                    error_text = str(exc)
                    if attempt == 0 and _is_restartable_remote_client_error_text(
                        error_text
                    ):
                        retry_count += 1
                        print(
                            f"[jobserver] Retrying transformation {tf_checksum_hex} after restartable remote-client failure",
                            flush=True,
                        )
                        continue
                    return web.Response(status=500, text=error_text)

                if isinstance(result_checksum, str):
                    remote_job_dir = parse_remote_job_written(result_checksum)
                    if remote_job_dir is not None:
                        print(
                            f"[jobserver] Prepared transformation {tf_checksum_hex} in {remote_job_dir}",
                            flush=True,
                        )
                        return web.Response(
                            status=200,
                            text=json.dumps({"remote_job_written": result_checksum}),
                        )
                    if attempt == 0 and _is_restartable_remote_client_error_text(
                        result_checksum
                    ):
                        retry_count += 1
                        print(
                            f"[jobserver] Retrying transformation {tf_checksum_hex} after restartable remote-client failure",
                            flush=True,
                        )
                        continue
                    return web.Response(status=500, text=result_checksum)
                break
        finally:
            gpu_memory_peak_bytes = stop_gpu_memory_sampler(gpu_sampler)

        assert result_checksum is not None

        result_checksum = Checksum(result_checksum)
        if not scratch:
            result_buf = await result_checksum.resolution()
            assert isinstance(result_buf, Buffer)
        finished_at = _utcnow_iso()
        wall_time_seconds = round(time.perf_counter() - wall_start, 6)
        cpu_end = os.times()
        cpu_user_seconds = round(cpu_end.user - cpu_start.user, 6)
        cpu_system_seconds = round(cpu_end.system - cpu_start.system, 6)
        record_runtime = {
            "started_at": started_at,
            "finished_at": finished_at,
            "wall_time_seconds": wall_time_seconds,
            "cpu_user_seconds": cpu_user_seconds,
            "cpu_system_seconds": cpu_system_seconds,
            "memory_peak_bytes": _memory_peak_bytes(),
            "gpu_memory_peak_bytes": gpu_memory_peak_bytes,
        }
        response_payload = {
            "result_checksum": result_checksum.hex(),
            "record_runtime": record_runtime,
        }
        if record_mode and not record_probe:
            from seamless_transformer.transformation_cache import (
                build_compilation_context_checksum,
                collect_compilation_runtime_metadata,
                collect_job_validation,
            )

            probe_context = await ensure_record_bucket_preconditions(
                transformation_dict,
                tf_dunder,
                execution="remote",
            )
            compilation_context = await build_compilation_context_checksum(
                transformation_dict,
                tf_dunder,
            )
            job_validation = await collect_job_validation(
                transformation_dict,
                tf_dunder,
                compilation_context=compilation_context,
                probe_context=probe_context,
            )
            record_runtime.update(
                {
                    "hostname": socket.gethostname(),
                    "pid": os.getpid(),
                    "process_started_at": _process_started_at_iso(),
                    "process_create_time_epoch": _process_create_time_epoch(),
                    "worker_execution_index": _next_execution_record_index(),
                    "retry_count": retry_count,
                }
            )
            record_runtime.update(
                await collect_compilation_runtime_metadata(
                    transformation_dict,
                    tf_dunder,
                )
            )
            response_payload.update(
                {
                    "probe_context": probe_context,
                    "compilation_context": compilation_context,
                    "job_validation": job_validation,
                }
            )
        print(
            f"[jobserver] Completed transformation {tf_checksum_hex} -> {result_checksum.hex()}",
            flush=True,
        )
        return web.Response(status=200, text=json.dumps(response_payload))


def main():
    import argparse

    p = argparse.ArgumentParser()
    port_group = p.add_mutually_exclusive_group()
    port_group.add_argument("--port", type=int, help="Network port")
    port_group.add_argument(
        "--port-range",
        type=int,
        nargs=2,
        metavar=("START", "END"),
        help="Inclusive port range to select a random free port from",
    )
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument(
        "--status-file",
        type=str,
        help="JSON file used to report server status",
    )
    p.add_argument(
        "--timeout",
        type=float,
        help="Stop the server after this many seconds of inactivity",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes to spawn",
    )
    args = p.parse_args()

    global status_tracker, _STARTUP_RECORD_MODE

    selected_port = args.port if args.port is not None else 5533
    status_file_path = args.status_file
    status_tracker = None
    parameters = {}
    if status_file_path:
        status_file_contents = wait_for_status_file(status_file_path)
        parameters = status_file_contents.get("parameters", {}) or {}
        status_tracker = StatusFileTracker(
            status_file_path, status_file_contents, args.port
        )

    if args.port_range:
        start, end = args.port_range
        try:
            selected_port = pick_random_free_port(args.host, start, end)
        except BaseException as exc:
            raise_startup_error(exc)
    if status_tracker:
        status_tracker.port = selected_port

    timeout_seconds = args.timeout
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise_startup_error(RuntimeError("--timeout must be a positive number"))
    if args.workers is not None and args.workers <= 0:
        raise_startup_error(RuntimeError("--workers must be a positive integer"))

    def raise_system_exit(*args, **kwargs):
        raise SystemExit

    signal.signal(signal.SIGTERM, raise_system_exit)
    signal.signal(signal.SIGHUP, raise_system_exit)
    signal.signal(signal.SIGINT, raise_system_exit)

    record_requested = bool(parameters.get("record"))
    _STARTUP_RECORD_MODE = record_requested
    try:
        from seamless.transformer import spawn

        spawn(args.workers)
        if parameters:
            from seamless.config import set_remote_clients

            set_remote_clients(parameters)
        if record_requested:
            # Worker/client setup may import or initialize configuration modules.
            # Re-assert record mode last so the request handler sees the launch
            # contract passed in the remote-http-launcher status file.
            from seamless_config.select import select_record

            select_record(True)
    except BaseException as exc:
        raise_startup_error(exc)

    job_server = JobServer(
        args.host,
        selected_port,
        timeout_seconds=timeout_seconds,
        status_tracker=status_tracker,
    )
    job_server.start()

    loop = get_event_loop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    except BaseException:
        if status_tracker and not status_tracker.running_written:
            status_tracker.write_failed()
        raise
    finally:
        loop.run_until_complete(job_server.stop())
        closed_ok = False
        try:
            seamless.close()
            closed_ok = True
        except Exception:
            pass
        if not closed_ok:
            try:
                worker.shutdown_workers()
            except Exception:
                pass
            try:
                from seamless_remote.client import close_all_clients

                close_all_clients()
            except Exception:
                pass


if __name__ == "__main__":
    main()
