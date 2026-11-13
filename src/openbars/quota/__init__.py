"""Shared, persistent quota gate. Every outbound request takes a token first."""

from openbars.quota.defaults import DEFAULT_POLICIES
from openbars.quota.gate import Lease, QuotaGate, QuotaUsage
from openbars.quota.pacer import PersistentRequestPacer, RequestPacer, store_slot_reserver
from openbars.quota.persistent import PersistentQuotaGate, SqliteQuotaGate
from openbars.quota.policy import BackoffPolicy, QuotaPolicy, RateLimit, tighten
from openbars.quota.retry import RetryDecision, RetryState, compute_backoff

__all__ = [
    "BackoffPolicy",
    "DEFAULT_POLICIES",
    "Lease",
    "PersistentQuotaGate",
    "PersistentRequestPacer",
    "QuotaGate",
    "QuotaPolicy",
    "QuotaUsage",
    "RateLimit",
    "RequestPacer",
    "RetryDecision",
    "RetryState",
    "SqliteQuotaGate",
    "compute_backoff",
    "store_slot_reserver",
    "tighten",
]
