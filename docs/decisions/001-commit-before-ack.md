\# ADR 001: Commit PostgreSQL Before Redis XACK



\*\*Status:\*\* Accepted



Redis `XACK` must occur only after the PostgreSQL transaction containing the bid has committed successfully. A Stream entry delivered with `XREADGROUP` remains in the consumer group's Pending Entries List until it is acknowledged. If PostgreSQL fails before commit, the consumer must return without calling `XACK`; the pending entry can then be claimed and retried by another consumer. Therefore, the permanent ledger is always written before Redis is told that processing has completed.



Acknowledging first is unsafe because a crash between `XACK` and the PostgreSQL commit would remove the entry from the Pending Entries List while leaving no durable bid in the ledger. Redis would no longer offer the entry for recovery, producing permanent bid loss. With commit-first ordering, a crash between commit and `XACK` creates only a duplicate delivery: the entry remains pending, `XAUTOCLAIM` recovers it, and the `(auction\_id, idempotency\_key)` uniqueness rule makes replay safe.

