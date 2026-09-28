"""Durable outbox: events are stored before delivery and removed from the
queue only once a sink confirms them, so nothing is lost across restarts."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any

PENDING, DELIVERED, UNDELIVERABLE = "pending", "delivered", "undeliverable"

# How long a sender owns a delivery it picked up; stops two processes sharing
# the outbox (agent server and relay) from sending the same event at once
LEASE_SECONDS = 120


@dataclass
class Delivery:
    event_id: str
    sink: str
    target: str
    session_key: str
    seq: int
    attempts: int
    created_at: float
    payload: str

    @property
    def event(self) -> dict[str, Any]:
        return json.loads(self.payload)


class Outbox:
    def __init__(self, path: str):
        if os.path.dirname(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    session_key TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    UNIQUE (session_key, seq)
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    event_id TEXT NOT NULL REFERENCES events(id),
                    sink TEXT NOT NULL,
                    target TEXT NOT NULL,
                    session_key TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    finished_at REAL,
                    lease_until REAL,
                    PRIMARY KEY (event_id, sink)
                );
                CREATE INDEX IF NOT EXISTS deliveries_pending
                    ON deliveries (status, session_key, sink, seq);
                """
            )

    def append(self, session_key: str, event: dict[str, Any], targets: list[tuple[str, str]]) -> dict[str, Any]:
        """Assign the next per-session seq, store the event and one delivery per target."""
        now = time.time()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                seq = self._conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE session_key = ?",
                    (session_key,),
                ).fetchone()[0]
                event = {**event, "seq": seq}
                payload = json.dumps(event, default=str)
                self._conn.execute(
                    "INSERT INTO events (id, session_key, seq, payload, created_at) VALUES (?, ?, ?, ?, ?)",
                    (event["id"], session_key, seq, payload, now),
                )
                self._conn.executemany(
                    "INSERT INTO deliveries (event_id, sink, target, session_key, seq, status, next_attempt_at, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [(event["id"], sink, target, session_key, seq, PENDING, now, now) for sink, target in targets],
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return event

    def due(self, limit: int = 100) -> list[Delivery]:
        """Claim pending deliveries that are due, head-of-line only: a session's
        next event for a sink waits until the earlier one is delivered or given up on."""
        now = time.time()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT d.event_id, d.sink, d.target, d.session_key, d.seq, d.attempts, d.created_at, e.payload
                FROM deliveries d JOIN events e ON e.id = d.event_id
                WHERE d.status = 'pending' AND d.next_attempt_at <= ?
                  AND (d.lease_until IS NULL OR d.lease_until < ?)
                  AND d.seq = (SELECT MIN(seq) FROM deliveries d2
                               WHERE d2.session_key = d.session_key AND d2.sink = d.sink
                                 AND d2.status = 'pending')
                ORDER BY d.created_at LIMIT ?
                """,
                (now, now, limit),
            ).fetchall()
            claimed = []
            for row in rows:
                # Conditional update: only one process can win the claim
                won = self._conn.execute(
                    "UPDATE deliveries SET lease_until = ? WHERE event_id = ? AND sink = ?"
                    " AND status = 'pending' AND (lease_until IS NULL OR lease_until < ?)",
                    (now + LEASE_SECONDS, row["event_id"], row["sink"], now),
                ).rowcount
                if won:
                    claimed.append(Delivery(**dict(row)))
        return claimed

    def pending_count(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM deliveries WHERE status = 'pending'").fetchone()[0]

    def next_due_at(self) -> float | None:
        with self._lock:
            return self._conn.execute(
                "SELECT MIN(next_attempt_at) FROM deliveries WHERE status = 'pending'"
            ).fetchone()[0]

    def mark_delivered(self, d: Delivery) -> None:
        self._finish(d, DELIVERED, None)

    def mark_undeliverable(self, d: Delivery, error: str) -> None:
        self._finish(d, UNDELIVERABLE, error)

    def mark_retry(self, d: Delivery, next_attempt_at: float, error: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE deliveries SET attempts = attempts + 1, next_attempt_at = ?, last_error = ?, lease_until = NULL"
                " WHERE event_id = ? AND sink = ?",
                (next_attempt_at, error, d.event_id, d.sink),
            )

    def _finish(self, d: Delivery, status: str, error: str | None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE deliveries SET status = ?, attempts = attempts + 1, last_error = ?, finished_at = ?, lease_until = NULL"
                " WHERE event_id = ? AND sink = ?",
                (status, error, time.time(), d.event_id, d.sink),
            )
