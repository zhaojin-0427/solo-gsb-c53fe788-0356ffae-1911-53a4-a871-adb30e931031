"""Precondition evaluation per RFC 9110, Section 13.2.2.

The five conditional header fields are evaluated in exactly the order
mandated by the specification:

  1. If-Match
  2. If-Unmodified-Since   (only when If-Match is absent)
  3. If-None-Match
  4. If-Modified-Since     (only for GET/HEAD and when If-None-Match
                            is absent)

Steps 1 and 2 short-circuit the remainder on failure (412 Precondition
Failed).  Step 3 yields 304 for GET/HEAD and 412 for state-changing
methods.  Malformed header fields are ignored, again per the spec.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from .etags import ETag, parse_etag_list, strong_equal, weak_equal

WILDCARD = "*"


class Outcome(Enum):
    PASS = "pass"
    NOT_MODIFIED = "not_modified"       # 304
    PRECONDITION_FAILED = "failed"      # 412


@dataclass(frozen=True, slots=True)
class SelectedRepresentation:
    """Validator state of the representation currently selected by the path."""

    exists: bool
    etag: ETag | None = None
    last_modified: datetime | None = None


@dataclass(frozen=True, slots=True)
class RequestConditions:
    method: str
    if_match_raw: str | None = None
    if_none_match_raw: str | None = None
    if_unmodified_since: datetime | None = None
    if_modified_since: datetime | None = None

    @property
    def is_safe_method(self) -> bool:
        return self.method in ("GET", "HEAD")


def evaluate(
    conditions: RequestConditions, selected: SelectedRepresentation
) -> Outcome:
    """Evaluate all applicable preconditions, returning the final outcome."""
    method_get_head = conditions.is_safe_method

    # Step 1: If-Match.
    if conditions.if_match_raw is not None:
        tags = parse_etag_list(conditions.if_match_raw)
        if tags is not None:
            matched = False
            if tags and tags[0] == WILDCARD:
                matched = selected.exists
            elif selected.exists and selected.etag is not None:
                # If-Match uses the strong comparison function.
                matched = any(
                    isinstance(t, ETag) and strong_equal(selected.etag, t)
                    for t in tags
                )
            if not matched:
                return Outcome.PRECONDITION_FAILED
            # When If-Match succeeds, steps 2 and 3 are skipped... but the
            # RFC only skips step 2; If-None-Match (step 3) is still
            # evaluated below.
    elif conditions.if_unmodified_since is not None:
        # Step 2: If-Unmodified-Since (only without If-Match).
        if (
            selected.exists
            and selected.last_modified is not None
            and selected.last_modified > conditions.if_unmodified_since
        ):
            return Outcome.PRECONDITION_FAILED

    # Step 3: If-None-Match.
    if conditions.if_none_match_raw is not None:
        tags = parse_etag_list(conditions.if_none_match_raw)
        if tags is not None:
            matched = False
            if tags and tags[0] == WILDCARD:
                matched = selected.exists
            elif selected.exists and selected.etag is not None:
                # If-None-Match uses the weak comparison function.
                matched = any(
                    isinstance(t, ETag) and weak_equal(selected.etag, t)
                    for t in tags
                )
            if matched:
                return (
                    Outcome.NOT_MODIFIED
                    if method_get_head
                    else Outcome.PRECONDITION_FAILED
                )

    # Step 4: If-Modified-Since (GET/HEAD only, ignored alongside If-None-Match).
    if (
        method_get_head
        and conditions.if_none_match_raw is None
        and conditions.if_modified_since is not None
        and selected.exists
        and selected.last_modified is not None
        and selected.last_modified <= conditions.if_modified_since
    ):
        return Outcome.NOT_MODIFIED

    return Outcome.PASS
