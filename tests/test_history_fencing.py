import threading
import time

import pytest

from taskqueue.core.queue import Queue
from taskqueue.history import FencedOut, HistoryWriter


def job_count(pg):
    return pg.execute("SELECT count(*) FROM jobs").fetchone()[0]


def test_only_one_writer_holds_the_lock(r, pg):
    a, b = HistoryWriter(r, pg), HistoryWriter(r, pg)
    assert a.try_acquire() is True
    assert b.try_acquire() is False


def test_standby_takes_over_with_a_higher_token_when_the_lock_expires(r, pg):
    a = HistoryWriter(r, pg, lock_ms=100)
    b = HistoryWriter(r, pg, lock_ms=100)
    a.try_acquire()
    time.sleep(0.15)  # a froze; its lock expired
    assert b.try_acquire() is True
    assert b.token > a.token
    assert a.renew() is False  # a finds out when it next tries to renew


def test_frozen_writer_cannot_write_a_stale_batch(r, pg):
    """Failure test: writer A freezes, loses the lock, B takes over and writes.
    When A wakes up and tries to write, PostgreSQL must reject it."""
    q = Queue(r)
    a = HistoryWriter(r, pg, lock_ms=100)
    b = HistoryWriter(r, pg, lock_ms=100)
    a.try_acquire()
    time.sleep(0.15)
    b.try_acquire()
    q.enqueue({})
    assert b.process_once() == 1  # B is the active writer now

    q.enqueue({})
    with pytest.raises(FencedOut):
        a.process_once()  # A wakes up, still thinking it's the writer
    pg.rollback()
    assert job_count(pg) == 1  # A's batch was rolled back

    assert b.process_once() == 1  # B picks up the event A couldn't write
    assert job_count(pg) == 2


def test_clean_release_lets_standby_take_over_immediately(r, pg):
    a, b = HistoryWriter(r, pg), HistoryWriter(r, pg)
    a.try_acquire()
    a.release()
    assert b.try_acquire() is True


def test_two_running_writers_record_every_event_once(r, pg):
    """Two writers run side by side; one is active, one waits. Nothing is lost
    or doubled, even when the active one stops and the standby takes over."""
    from taskqueue import db
    q = Queue(r)
    conn_b = db.connect("taskqueue_test")
    a = HistoryWriter(r, pg, lock_ms=500)
    b = HistoryWriter(r, conn_b, lock_ms=500)
    threads = [threading.Thread(target=w.run) for w in (a, b)]
    for t in threads:
        t.start()

    ids = [q.enqueue({}) for _ in range(20)]
    time.sleep(1.5)
    active = a if a.token is not None else b
    active._stopping = True  # active writer shuts down cleanly...
    ids += [q.enqueue({}) for _ in range(20)]
    time.sleep(3)  # ...and the standby takes over

    a._stopping = b._stopping = True
    for t in threads:
        t.join()
    conn_b.close()
    assert job_count(pg) == 40
    assert r.xlen(q.events_key) == 0
