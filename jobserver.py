from aiohttp import web
import asyncio
import contextlib
import json
import os
import random
import signal
import socket
import sys
import time

from seamless import Checksum, Buffer
from seamless.transformer import spawn
from seamless_transformer import worker
import seamless
from seamless.util.get_event_loop import get_event_loop

try:
    from seamless_remote.client import close_all_clients as _close_all_clients
except ImportError:
    _close_all_clients = None


STATUS_FILE_WAIT_TIMEOUT = 20.0
INACTIVITY_CHECK_INTERVAL = 1.0

status_tracker = None


def is_port_in_use(address, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((address, port)) == 0


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
        except Exception as exc:
            return web.Response(status=400, text=f"Invalid payload: {exc}")

        tf_checksum_hex = tf_checksum.hex()
        print(f"[jobserver] Received transformation {tf_checksum_hex}", flush=True)
        try:
            result_checksum = await worker.dispatch_to_workers(
                transformation_dict,
                tf_checksum=tf_checksum,
                tf_dunder=tf_dunder,
                scratch=scratch,
            )
        except Exception as exc:
            return web.Response(status=500, text=str(exc))

        if isinstance(result_checksum, str):
            return web.Response(status=500, text=result_checksum)

        result_checksum = Checksum(result_checksum)
        if not scratch:
            result_buf = await result_checksum.resolution()
            assert isinstance(result_buf, Buffer)
        print(
            f"[jobserver] Completed transformation {tf_checksum_hex} -> {result_checksum.hex()}",
            flush=True,
        )
        return web.Response(status=200, text=result_checksum.hex())


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

    global status_tracker

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

    try:
        spawn(args.workers)
        if parameters:
            from seamless.config import set_remote_clients

            set_remote_clients(parameters)
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
            if _close_all_clients is not None:
                try:
                    _close_all_clients()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
