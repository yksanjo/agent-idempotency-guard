# agent-idempotency-guard

Stop agent retries from repeating side effects, in dependency-free Python.

Agents retry. Networks flake, models re-issue the same tool call, loops
re-run after a crash. When the call has a side effect - an email sent, a
booking made, a charge created - a naive retry does it twice. This library
gives each effectful call an idempotency key, the same pattern Stripe uses
for API requests, packaged for agent tool calls.

## How it works

```python
guard.call(key, fn, **kwargs)
```

- **First call** with a key: runs `fn(**kwargs)`, stores the result in
  SQLite, returns it.
- **Same key, same arguments**: returns the stored result marked
  `replayed` - `fn` is never run again.
- **Same key, different arguments**: raises `ConflictError`. A key reused
  for a different effect is almost always a bug, and silence here is how
  the wrong person gets an email.
- **Key in flight**: raises `InFlightError` rather than racing a duplicate.
- **Original call raised**: the failure is stored; a same-args retry raises
  `FailedCallError` with the original error instead of blindly re-firing.

Arguments are matched by a canonical-JSON SHA-256 fingerprint, so dict key
order and whitespace never matter but content always does. Storage is
stdlib SQLite: pass a file path and the record survives the crash that
caused the retry, which is the whole point. Completed records support a
TTL (default 24h, `None` keeps forever); expired keys are purged lazily
and can be reused.

## Demo and tests

```
python3 demo.py                          # scripted scenario, no network
python3 -m unittest test_idempotency_guard -v   # 20 tests
```

The demo sends one synthetic welcome email, then shows a crash-retry being
replayed (the mail server still received exactly one message) and a
key-reuse bug being refused.

## Honest limits

- **The caller must pick good keys.** The guard dedupes by the key you
  give it. Two calls with different keys are different calls, even if they
  do the same thing. A natural key ("welcome:jane") dedupes intent; a
  random UUID per attempt dedupes nothing.
- **Single process at a time.** SQLite serializes writers on one machine,
  but this is not a distributed lock. Two agents on two machines sharing
  one network filesystem can still race; use a real remote store for that.
- **Crash mid-call leaves `in_flight`.** If the process dies after the
  effect happened but before the result was stored, the key stays
  `in_flight` and later calls refuse rather than guess. That is the safe
  direction, but it needs a human (or a `DELETE`) to resolve.
- **Replay returns the stored result only.** It cannot know whether the
  world still matches it; the email was sent once, but the guard cannot
  tell you whether it bounced.
- **Results must be JSON-shaped.** Non-serializable return values are
  stored via `repr` and replay as strings.
- **TTL expiry is lazy.** Expired records are removed when their key is
  next used, not on a schedule.

## License

MIT. See [LICENSE](LICENSE).
