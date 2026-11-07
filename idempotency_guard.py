"""idempotency_guard: stop agent retries from repeating side effects.

Agents retry. Networks flake, models re-issue the same tool call, loops
re-run after a crash. When the call has a side effect - an email sent, a
booking made, a charge created - a naive retry does it twice.

This library gives each effectful call an idempotency key (Stripe-style):

  result, record = guard.call("welcome-email:jane", send_email,
                              to="jane@example.com", template="welcome")

  - First call: executes, stores the result, returns it.
  - Same key + same arguments later: returns the stored result WITHOUT
    re-executing, marked replayed=True.
  - Same key + DIFFERENT arguments: raises ConflictError - the key is
    being reused for a different effect, which is almost always a bug.
  - Key already in flight: raises InFlightError rather than racing a
    duplicate.

Storage is SQLite (stdlib), so the record survives the process that made
the call - which is the whole point when retries happen after a crash.

Dependency-free, deterministic, no network access.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple


class ConflictError(Exception):
    """Same idempotency key, different arguments."""


class InFlightError(Exception):
    """Key is currently executing elsewhere (or crashed mid-call)."""


class FailedCallError(Exception):
    """The original call raised; its exception is stored, not replayed."""


def fingerprint(payload: Any) -> str:
    """Stable SHA-256 over a canonical JSON encoding of the arguments.

    Key order, whitespace, and unicode escaping are normalized so two
    dicts with the same content hash identically.
    """
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Record:
    key: str
    fingerprint: str
    status: str          # 'in_flight' | 'completed' | 'failed'
    result_json: Optional[str]
    error: Optional[str]
    created_at: float
    expires_at: Optional[float]


@dataclass(frozen=True)
class Event:
    key: str
    kind: str            # 'executed' | 'replayed' | 'conflict' | 'in_flight' | 'failed' | 'expired'
    detail: str


class IdempotencyGuard:
    """SQLite-backed idempotency store for effectful tool calls.

    ttl_seconds: how long a completed record authorizes replay. After
    expiry the key may be reused (the old record is purged lazily).
    Pass ttl_seconds=None to keep records forever.
    """

    def __init__(self, db_path: str = ":memory:",
                 ttl_seconds: Optional[float] = 24 * 3600,
                 clock: Callable[[], float] = time.time) -> None:
        self.ttl_seconds = ttl_seconds
        self.clock = clock
        self.db = sqlite3.connect(db_path)
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS calls(
                 key TEXT PRIMARY KEY,
                 fingerprint TEXT NOT NULL,
                 status TEXT NOT NULL,
                 result_json TEXT,
                 error TEXT,
                 created_at REAL NOT NULL,
                 expires_at REAL)"""
        )
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS events(
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 key TEXT NOT NULL, kind TEXT NOT NULL,
                 detail TEXT NOT NULL, ts REAL NOT NULL)"""
        )
        self.db.commit()

    # -- main entry point -------------------------------------------------

    def call(self, key: str, fn: Callable[..., Any], **kwargs: Any
             ) -> Tuple[Any, Event]:
        """Run `fn(**kwargs)` at most once per `key` + arguments."""
        fp = fingerprint(kwargs)
        existing = self._load(key)

        if existing is not None:
            if existing.fingerprint != fp:
                ev = self._event(key, "conflict",
                                 "same key, different arguments")
                raise ConflictError(
                    f"key {key!r} already used with different arguments")
            if existing.status == "in_flight":
                self._event(key, "in_flight", "call already executing")
                raise InFlightError(f"key {key!r} is in flight")
            if existing.status == "failed":
                self._event(key, "failed", existing.error or "unknown")
                raise FailedCallError(
                    f"key {key!r} previously failed: {existing.error}")
            # completed: replay the stored result, do not re-execute
            ev = self._event(key, "replayed", "stored result returned")
            return json.loads(existing.result_json), ev

        # first time we see this key
        expires = None if self.ttl_seconds is None else self.clock() + self.ttl_seconds
        self._insert(key, fp, "in_flight", None, None, expires)
        try:
            result = fn(**kwargs)
        except Exception as exc:  # store the failure so a retry sees it
            self._complete(key, "failed", None, f"{type(exc).__name__}: {exc}")
            self._event(key, "failed", f"{type(exc).__name__}: {exc}")
            raise
        self._complete(key, "completed", json.dumps(result, sort_keys=True,
                                                    default=repr), None)
        ev = self._event(key, "executed", "call ran and result stored")
        return result, ev

    # -- internals --------------------------------------------------------

    def _load(self, key: str) -> Optional[Record]:
        row = self.db.execute(
            "SELECT key, fingerprint, status, result_json, error,"
            " created_at, expires_at FROM calls WHERE key=?", (key,)
        ).fetchone()
        if row is None:
            return None
        rec = Record(*row)
        if rec.expires_at is not None and self.clock() >= rec.expires_at:
            self.db.execute("DELETE FROM calls WHERE key=?", (key,))
            self.db.commit()
            self._event(key, "expired", "record past ttl, key reusable")
            return None
        return rec

    def _insert(self, key: str, fp: str, status: str,
                result_json: Optional[str], error: Optional[str],
                expires_at: Optional[float]) -> None:
        self.db.execute(
            "INSERT INTO calls VALUES(?,?,?,?,?,?,?)",
            (key, fp, status, result_json, error, self.clock(), expires_at),
        )
        self.db.commit()

    def _complete(self, key: str, status: str,
                  result_json: Optional[str], error: Optional[str]) -> None:
        self.db.execute(
            "UPDATE calls SET status=?, result_json=?, error=? WHERE key=?",
            (status, result_json, error, key),
        )
        self.db.commit()

    def _event(self, key: str, kind: str, detail: str) -> Event:
        self.db.execute(
            "INSERT INTO events(key, kind, detail, ts) VALUES(?,?,?,?)",
            (key, kind, detail, self.clock()),
        )
        self.db.commit()
        return Event(key=key, kind=kind, detail=detail)

    def events(self) -> List[Event]:
        rows = self.db.execute(
            "SELECT key, kind, detail FROM events ORDER BY id").fetchall()
        return [Event(*r) for r in rows]

    def close(self) -> None:
        self.db.close()
