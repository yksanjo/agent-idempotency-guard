"""Synthetic demo: an agent sending a welcome email through a flaky runner.

All addresses are fictional; nothing here touches a network.
"""

from idempotency_guard import ConflictError, IdempotencyGuard

sent_log = []  # what the "mail server" actually got


def send_email(to, template):
    sent_log.append(to)
    return {"sent_to": to, "template": template, "id": f"msg-{len(sent_log)}"}


guard = IdempotencyGuard()  # in-memory for the demo; pass a path to persist

# Attempt 1: the call goes through.
r1, ev1 = guard.call("welcome:jane", send_email,
                     to="jane@example-user.com", template="welcome")
print(f"attempt 1: {ev1.kind:9} -> {r1}")

# Attempt 2: the agent crashed after sending and lost its place, so it
# issues the exact same call again. The guard replays the stored result.
r2, ev2 = guard.call("welcome:jane", send_email,
                     to="jane@example-user.com", template="welcome")
print(f"attempt 2: {ev2.kind:9} -> {r2}   (no second send)")

# Attempt 3: a bug reuses the key for a DIFFERENT recipient. Refused.
try:
    guard.call("welcome:jane", send_email,
               to="mark@example-user.com", template="welcome")
except ConflictError as e:
    print(f"attempt 3: conflict  -> {e}")

print(f"\nmail server received {len(sent_log)} message(s): {sent_log}")
print("event log:", [e.kind for e in guard.events()])
