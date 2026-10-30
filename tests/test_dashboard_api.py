import json
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from taskqueue import metrics
from taskqueue.core.queue import Queue
from taskqueue.history import HistoryWriter
from taskqueue.worker import Worker


@pytest.fixture
def server(r):
    metrics.Handler.redis_client = r
    srv = ThreadingHTTPServer(("127.0.0.1", 0), metrics.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def post(url, body=None, demo_header=True):
    req = urllib.request.Request(url, method="POST", data=json.dumps(body or {}).encode(),
                                 headers={"X-Demo": "1"} if demo_header else {})
    try:
        with urllib.request.urlopen(req) as res:
            return res.status, json.loads(res.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_timeline_lists_every_attempt(r, pg):
    q = Queue(r, retry_base_seconds=0)
    job_id = q.enqueue({"task": "echo"}, priority=2)
    _, t1 = q.lease()
    q.fail(job_id, t1, "boom")
    _, t2 = q.lease()
    q.ack(job_id, t2, "ok")
    writer = HistoryWriter(r, pg)
    while writer.process_once():
        pass

    t = metrics.job_timeline(pg, job_id)
    assert t["status"] == "done" and t["priority"] == 2 and t["result"] == "ok"
    assert [(a["number"], a["outcome"]) for a in t["attempts"]] == [(1, "error"), (2, "done")]
    assert metrics.job_timeline(pg, "not-a-uuid") is None


def test_demo_controls_off_by_default(server, monkeypatch):
    monkeypatch.delenv("DEMO_CONTROLS", raising=False)
    assert post(server + "/demo/submit")[0] == 403


def test_demo_controls_need_the_header(server, monkeypatch):
    """A page on another site can't add custom headers, so it can't trigger these."""
    monkeypatch.setenv("DEMO_CONTROLS", "1")
    assert post(server + "/demo/submit", demo_header=False)[0] == 403


def test_demo_submit(server, r, monkeypatch):
    monkeypatch.setenv("DEMO_CONTROLS", "1")
    status, body = post(server + "/demo/submit", {"kind": "urgent", "count": 3})
    assert status == 200 and body["submitted"] == 3
    assert Queue(r).count_waiting() == 3


def test_demo_kill_crashes_the_chosen_worker(server, r, monkeypatch):
    monkeypatch.setenv("DEMO_CONTROLS", "1")
    q = Queue(r)
    w = Worker(q)
    w._beat()
    status, body = post(server + "/demo/kill")
    assert status == 200 and body["killed"] == w.worker_id

    exits = []
    monkeypatch.setattr(metrics.os, "_exit", exits.append)  # same os module the worker uses
    w._check_demo_kill()
    assert exits == [9]
    assert r.exists(metrics.kill_flag_key("default", w.worker_id)) == 0  # flag used up


def test_worker_ignores_kill_flag_when_demo_is_off(r, monkeypatch):
    monkeypatch.delenv("DEMO_CONTROLS", raising=False)
    q = Queue(r)
    w = Worker(q)
    r.set(metrics.kill_flag_key("default", w.worker_id), 1)
    exits = []
    monkeypatch.setattr(metrics.os, "_exit", exits.append)
    w._check_demo_kill()
    assert exits == []


def test_kill_with_no_live_workers(server, monkeypatch):
    monkeypatch.setenv("DEMO_CONTROLS", "1")
    assert post(server + "/demo/kill")[0] == 409
