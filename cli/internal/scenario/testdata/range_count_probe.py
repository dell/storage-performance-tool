"""Bounded packaged-engine count gate; uses only an isolated local HTTP fixture.

Invoked by range_count_integration_test.go. The unmodified generated scenario runs
against two eight-byte objects. Keep logs and canonical artifacts for diagnosis;
remove all Docker containers/networks, even when the engine times out.
"""

import http.server
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid

image, scenario, mode, seed, offset = sys.argv[1:]
root = pathlib.Path(
    tempfile.mkdtemp(
        prefix="range-count-", dir=os.environ.get("SPT_RANGE_TEST_ARTIFACTS")
    )
)
root.chmod(0o755)
for part in ["home", "log", "worker-log"]:
    (root / part).mkdir()
    (root / part).chmod(0o777)
(root / "items.csv").write_text("/bucket/key0,0,8,0/0\n/bucket/key1,1,8,0/0\n")
(root / "scenario.js").write_text(pathlib.Path(scenario).read_text())
objects = (
    {}
    if seed == "seeded"
    else {"/bucket/key0": b"abcdefgh", "/bucket/key1": b"abcdefgh"}
)
requests = []
network = "spt-range-net-" + uuid.uuid4().hex[:12]
worker = "spt-range-worker-" + uuid.uuid4().hex[:12]


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_PUT(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        assert not self.headers.get("Range")
        objects[self.path] = body
        requests.append({"method": "PUT", "path": self.path, "bytes": len(body)})
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        value = self.headers.get("Range", "")
        import re

        match = re.fullmatch(r"bytes=(\d+)-(\d+)", value)
        data = objects.get(self.path)
        good = match is not None and data is not None
        start, end = map(int, match.groups()) if match else (0, 0)
        good = (
            good
            and end - start == 2
            and 0 <= start <= end < len(data)
            and (offset == "random" or start == 2)
        )
        requests.append(
            {"method": "GET", "path": self.path, "range": value, "valid": good}
        )
        body = data[start : end + 1] if good else b"error"
        self.send_response(206 if good else 400)
        if good:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_DELETE(self):
        assert not self.headers.get("Range")
        requests.append({"method": "DELETE", "path": self.path})
        objects.pop(self.path, None)
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        assert not self.headers.get("Range")
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


gateway = subprocess.check_output(
    [
        "docker",
        "network",
        "inspect",
        "bridge",
        "--format",
        "{{(index .IPAM.Config 0).Gateway}}",
    ],
    text=True,
).strip()
server = http.server.ThreadingHTTPServer((gateway, 0), Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
name = "spt-range-probe-" + uuid.uuid4().hex[:12]
args = [
    "docker",
    "run",
    "--rm",
    "--pull=never",
    "--name",
    name,
    "--network",
    network,
    "--hostname",
    name,
    "--add-host",
    "range-loopback:host-gateway",
    "-e",
    "SPT_JAVA_OPTS=-Xms64m -Xmx512m -XX:-AlwaysPreTouch -XX:MaxDirectMemorySize=128m",
    "-v",
    str(root) + ":/work",
    "-v",
    str(root / "home") + ":/home/spt",
    "-v",
    str(root / "log") + ":/opt/spt/log",
    image,
    "--storage-driver-type=s3",
    "--storage-net-node-addrs=range-loopback",
    "--storage-net-node-port=" + str(server.server_port),
    "--storage-net-timeoutMilliSec=2000",
    "--storage-driver-limit-concurrency=1",
    "--storage-driver-limit-queue-input=16",
    "--storage-driver-threads=1",
    "--load-op-type=read",
    "--run-scenario=/work/scenario.js",
    "--load-op-limit-rate=20",
    "--load-step-limit-time=30s",
    "--load-batch-size=1",
    "--item-output-path=/bucket",
    "--item-data-ranges-threshold=0",
    "--api-linger-sec=0",
]
if mode == "distributed":
    args.append("--load-step-node-addrs=" + worker)
result = {
    "image": image,
    "image_id": subprocess.check_output(
        ["docker", "image", "inspect", image, "--format", "{{.Id}}"], text=True
    ).strip(),
}
(root / "command.json").write_text(json.dumps(args, indent=2) + "\n")
try:
    subprocess.run(
        ["docker", "network", "create", network], check=True, stdout=subprocess.DEVNULL
    )
    if mode == "distributed":
        worker_args = [
            "docker",
            "run",
            "-d",
            "--rm",
            "--pull=never",
            "--name",
            worker,
            "--hostname",
            worker,
            "--network",
            network,
            "-p",
            "127.0.0.1::9999",
            "--add-host",
            "range-loopback:host-gateway",
            "-e",
            "SPT_JAVA_OPTS=-Xms64m -Xmx512m -XX:-AlwaysPreTouch "
            "-XX:MaxDirectMemorySize=128m -Djava.rmi.server.hostname=" + worker,
            "-v",
            str(root / "worker-log") + ":/opt/spt/log",
            image,
            "--run-node=true",
        ]
        (root / "worker-command.json").write_text(
            json.dumps(worker_args, indent=2) + "\n"
        )
        subprocess.run(worker_args, check=True, stdout=subprocess.DEVNULL)
        result["worker_image_id"] = subprocess.check_output(
            ["docker", "inspect", worker, "--format", "{{.Image}}"], text=True
        ).strip()
        assert result["worker_image_id"] == result["image_id"]
        port = (
            subprocess.check_output(["docker", "port", worker, "9999/tcp"], text=True)
            .strip()
            .rsplit(":", 1)[1]
        )
        for _ in range(60):
            try:
                with urllib.request.urlopen(
                    "http://127.0.0.1:" + port + "/status", timeout=1
                ) as response:
                    response.read()
                break
            except Exception:
                time.sleep(0.5)
        else:
            raise RuntimeError("worker API unavailable")
    with (root / "engine.log").open("w") as log:
        run = subprocess.run(args, stdout=log, stderr=subprocess.STDOUT, timeout=90)
    result["exit_code"] = run.returncode
except subprocess.TimeoutExpired:
    result["timeout"] = True
finally:
    subprocess.run(
        ["docker", "rm", "-f", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    )
    worker_logs = subprocess.run(
        ["docker", "logs", worker], capture_output=True, text=True
    )
    (root / "worker-engine.log").write_text(worker_logs.stdout + worker_logs.stderr)
    subprocess.run(
        ["docker", "rm", "-f", worker],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    )
    subprocess.run(
        ["docker", "network", "rm", network],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    )
    server.shutdown()
    server.server_close()
    thread.join(5)
    result["requests"] = requests
    (root / "receipt.json").write_text(json.dumps(result, indent=2) + "\n")

    result["remaining_objects"] = list(objects)
    import csv

    rows = []
    for path in (root / "log").glob("*read/range.read.csv"):
        with path.open() as stream:
            rows.extend(csv.DictReader(stream))
    result["range_rows"] = rows
    (root / "receipt.json").write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                "artifact_root": str(root),
                "exit_code": result.get("exit_code"),
                "get_count": sum(r["method"] == "GET" for r in requests),
            }
        )
    )
assert result.get("exit_code") == 0, result
gets = [r for r in requests if r["method"] == "GET"]
assert len(gets) == 12 and all(r["valid"] for r in gets), gets
assert not objects, objects
assert rows, "missing canonical range artifact"
assert sum(int(r["accepted"]) for r in rows) == 12, rows
assert sum(int(r["successful_bytes"]) for r in rows) == 36, rows
assert sum(int(r["requests_sent"]) for r in rows) == len(gets), rows
assert len({(r["engine_run_id"], r["step_id"]) for r in rows}) == 1, rows
for row in rows:
    assert row["terminal"] == "true" and row["overflow"] == "false", row
    accepted, failed, unresolved, unattempted = (
        int(row[field]) for field in ("accepted", "failed", "unresolved", "unattempted")
    )
    assert int(row["selected"]) == accepted + failed + unresolved + unattempted, row
    assert int(row["attempted"]) == accepted + failed + unresolved, row
    assert int(row["successful_bytes"]) == accepted * 3, row
    for field in [
        "failed",
        "unresolved",
        "generator_buffered",
        "driver_queued",
        "in_flight",
    ]:
        assert int(row[field]) == 0, (field, row)
assert len({r["worker_id"] for r in rows}) == (2 if mode == "distributed" else 1), rows
for container in [name, worker]:
    assert (
        subprocess.run(
            ["docker", "inspect", container],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        != 0
    )

assert not thread.is_alive(), "HTTP fixture failed to stop"
assert (
    subprocess.run(
        ["docker", "network", "inspect", network],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    ).returncode
    != 0
), "test network leaked"
