"""Canonical enums for channels, decision types, and entity types."""
from enum import Enum


class Channel(str, Enum):
    """Marketing channels."""

    META = "meta"
    GOOGLE = "google"
    OTHER = "other"


class DecisionType(str, Enum):
    """Decision / reason codes produced by rules engine."""

    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    PAUSE_CAMPAIGN = "pause_campaign"
    PAUSE_PRODUCT = "pause_product"
    REALLOCATE_BUDGET = "reallocate_budget"


class EntityType(str, Enum):
    """Entity targeted by a decision."""

    CHANNEL = "channel"
    CAMPAIGN = "campaign"
    PRODUCT = "product"


class DecisionStatus(str, Enum):
    """Status of a decision in the store."""

    PENDING = "pending"
    APPLIED = "applied"
    DISMISSED = "dismissed"
    # Decision lifecycle (v2)
    GENERATED = "generated"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    EXECUTED = "executed"
    VERIFIED = "verified"
