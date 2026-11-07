import os
import tempfile
import unittest

from idempotency_guard import (
    ConflictError, FailedCallError, IdempotencyGuard, InFlightError,
    fingerprint,
)


def fake_send(to, template):
    return {"sent_to": to, "template": template}


class TestFingerprint(unittest.TestCase):
    def test_key_order_irrelevant(self):
        self.assertEqual(fingerprint({"a": 1, "b": 2}),
                         fingerprint({"b": 2, "a": 1}))

    def test_nested_order_irrelevant(self):
        self.assertEqual(fingerprint({"x": {"p": 1, "q": [1, 2]}}),
                         fingerprint({"x": {"q": [1, 2], "p": 1}}))

    def test_different_content_differs(self):
        self.assertNotEqual(fingerprint({"a": 1}), fingerprint({"a": 2}))

    def test_list_order_matters(self):
        self.assertNotEqual(fingerprint({"v": [1, 2]}),
                            fingerprint({"v": [2, 1]}))

    def test_unicode_stable(self):
        self.assertEqual(fingerprint({"city": "Sao Paulo"}),
                         fingerprint({"city": "Sao Paulo"}))

    def test_nan_rejected(self):
        with self.assertRaises(ValueError):
            fingerprint({"x": float("nan")})


class GuardBase(unittest.TestCase):
    def setUp(self):
        self.now = [1000.0]
        self.guard = IdempotencyGuard(clock=lambda: self.now[0])

    def tearDown(self):
        self.guard.close()


class TestCall(GuardBase):
    def test_first_call_executes(self):
        result, ev = self.guard.call("k1", fake_send,
                                     to="a@example.com", template="t")
        self.assertEqual(result["sent_to"], "a@example.com")
        self.assertEqual(ev.kind, "executed")

    def test_replay_does_not_reexecute(self):
        calls = []
        def counting(to, template):
            calls.append(to)
            return {"n": len(calls)}
        r1, _ = self.guard.call("k1", counting, to="a", template="t")
        r2, ev2 = self.guard.call("k1", counting, to="a", template="t")
        self.assertEqual(r1, r2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(ev2.kind, "replayed")

    def test_same_key_different_args_conflicts(self):
        self.guard.call("k1", fake_send, to="a@example.com", template="t")
        with self.assertRaises(ConflictError):
            self.guard.call("k1", fake_send, to="b@example.com", template="t")

    def test_conflict_logs_event(self):
        self.guard.call("k1", fake_send, to="a@example.com", template="t")
        try:
            self.guard.call("k1", fake_send, to="b@example.com", template="t")
        except ConflictError:
            pass
        kinds = [e.kind for e in self.guard.events()]
        self.assertIn("conflict", kinds)

    def test_different_keys_independent(self):
        self.guard.call("k1", fake_send, to="a@example.com", template="t")
        r, ev = self.guard.call("k2", fake_send, to="b@example.com", template="t")
        self.assertEqual(ev.kind, "executed")
        self.assertEqual(r["sent_to"], "b@example.com")

    def test_failed_call_stored_and_reraised(self):
        def boom(to, template):
            raise RuntimeError("smtp down")
        with self.assertRaises(RuntimeError):
            self.guard.call("k1", boom, to="a", template="t")
        # retry with same args sees the recorded failure
        with self.assertRaises(FailedCallError) as ctx:
            self.guard.call("k1", boom, to="a", template="t")
        self.assertIn("smtp down", str(ctx.exception))

    def test_in_flight_rejected(self):
        # simulate a crashed process: insert an in_flight record directly
        self.guard._insert("k1", fingerprint({"to": "a", "template": "t"}),
                           "in_flight", None, None, None)
        with self.assertRaises(InFlightError):
            self.guard.call("k1", fake_send, to="a", template="t")

    def test_result_roundtrip_json(self):
        def complex_fn(to, template):
            return {"list": [1, 2, 3], "nested": {"ok": True}, "none": None}
        r1, _ = self.guard.call("k1", complex_fn, to="a", template="t")
        r2, _ = self.guard.call("k1", complex_fn, to="a", template="t")
        self.assertEqual(r1, r2)


class TestTtl(GuardBase):
    def test_expired_key_reusable(self):
        self.guard.ttl_seconds = 60
        self.guard.call("k1", fake_send, to="a@example.com", template="t")
        self.now[0] += 61
        r, ev = self.guard.call("k1", fake_send, to="a@example.com", template="t")
        self.assertEqual(ev.kind, "executed")

    def test_within_ttl_replays(self):
        self.guard.ttl_seconds = 60
        self.guard.call("k1", fake_send, to="a@example.com", template="t")
        self.now[0] += 59
        _, ev = self.guard.call("k1", fake_send, to="a@example.com", template="t")
        self.assertEqual(ev.kind, "replayed")

    def test_expiry_logged(self):
        self.guard.ttl_seconds = 60
        self.guard.call("k1", fake_send, to="a@example.com", template="t")
        self.now[0] += 61
        self.guard.call("k1", fake_send, to="a@example.com", template="t")
        kinds = [e.kind for e in self.guard.events()]
        self.assertIn("expired", kinds)

    def test_no_ttl_keeps_forever(self):
        self.guard.ttl_seconds = None
        self.guard.call("k1", fake_send, to="a@example.com", template="t")
        self.now[0] += 10_000_000
        _, ev = self.guard.call("k1", fake_send, to="a@example.com", template="t")
        self.assertEqual(ev.kind, "replayed")


class TestPersistence(unittest.TestCase):
    def test_survives_reopen(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "guard.db")
            g1 = IdempotencyGuard(db_path=path)
            g1.call("k1", fake_send, to="a@example.com", template="t")
            g1.close()
            # new process, same db: the send must not happen again
            g2 = IdempotencyGuard(db_path=path)
            calls = []
            def counting(to, template):
                calls.append(to)
                return {}
            _, ev = g2.call("k1", counting, to="a@example.com", template="t")
            g2.close()
            self.assertEqual(ev.kind, "replayed")
            self.assertEqual(calls, [])


class TestEvents(GuardBase):
    def test_event_sequence(self):
        self.guard.call("k1", fake_send, to="a@example.com", template="t")
        self.guard.call("k1", fake_send, to="a@example.com", template="t")
        kinds = [e.kind for e in self.guard.events()]
        self.assertEqual(kinds, ["executed", "replayed"])


if __name__ == "__main__":
    unittest.main()
