"""Operator ceilings and immutable, portable Discussion room budgets."""

from dataclasses import asdict, dataclass
from collections.abc import Mapping
import json

HARD_CAP_MEMBERS = 32


@dataclass(frozen=True)
class DiscussionPolicy:
    max_members: int = 6
    max_rounds: int = 3
    max_turns_per_round: int = 6
    max_messages_total: int = 10
    max_delta_lines: int = 24

    def __post_init__(self):
        bounds = {
            "max_members": (2, HARD_CAP_MEMBERS),
            "max_rounds": (1, 32),
            "max_turns_per_round": (1, HARD_CAP_MEMBERS),
            "max_messages_total": (1, 1024),
            "max_delta_lines": (1, 1024),
        }
        for key, (minimum, maximum) in bounds.items():
            value = getattr(self, key)
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError(
                    f"discussion_policy.{key} must be an integer between {minimum} and {maximum}"
                )

    def to_dict(self):
        return asdict(self)

    def canonical_json(self):
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, value=None):
        if isinstance(value, cls):
            return value
        if value is None:
            return cls()
        if not isinstance(value, Mapping) or set(value) - set(cls.__dataclass_fields__):
            raise ValueError(
                "discussion_policy must contain only supported budget fields"
            )
        return cls(**value)

    def reduce(self, overrides=None):
        if overrides is None:
            return self
        if not isinstance(overrides, Mapping) or set(overrides) - set(self.to_dict()):
            raise ValueError(
                "discussion_policy override must contain only supported budget fields"
            )
        effective = self.from_dict({**self.to_dict(), **overrides})
        for key, value in effective.to_dict().items():
            if value > getattr(self, key):
                raise ValueError(
                    f"discussion_policy.{key} cannot exceed the operator ceiling"
                )
        return effective
