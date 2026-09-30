"""Unit tests for RFC 9110 13.2.2 precondition evaluation order."""
from datetime import datetime, timezone

from app.etags import make_etag
from app.preconditions import (
    Outcome,
    RequestConditions,
    SelectedRepresentation,
    evaluate,
)

ETAG = make_etag("a" * 64)
T0 = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
BEFORE = datetime(2026, 9, 1, 11, 0, 0, tzinfo=timezone.utc)
AFTER = datetime(2026, 9, 1, 13, 0, 0, tzinfo=timezone.utc)

CURRENT = SelectedRepresentation(exists=True, etag=ETAG, last_modified=T0)
MISSING = SelectedRepresentation(exists=False)


def cond(method="GET", **kw):
    return RequestConditions(method=method, **kw)


# ---- If-Match (step 1) ----------------------------------------------------

def test_if_match_strong_hit_passes():
    assert evaluate(cond(if_match_raw=f'"{ETAG.opaque}"'), CURRENT) is Outcome.PASS


def test_if_match_weak_form_fails_strong_comparison():
    assert (
        evaluate(cond(if_match_raw=f'W/"{ETAG.opaque}"'), CURRENT)
        is Outcome.PRECONDITION_FAILED
    )


def test_if_match_wildcard_requires_existence():
    assert evaluate(cond(if_match_raw="*"), CURRENT) is Outcome.PASS
    assert evaluate(cond(if_match_raw="*"), MISSING) is Outcome.PRECONDITION_FAILED


def test_if_match_mismatch_fails():
    assert (
        evaluate(cond(if_match_raw='"deadbeef"'), CURRENT)
        is Outcome.PRECONDITION_FAILED
    )


def test_if_match_malformed_is_ignored():
    assert evaluate(cond(if_match_raw="not-an-etag"), CURRENT) is Outcome.PASS


# ---- If-Unmodified-Since (step 2, only without If-Match) ------------------

def test_if_unmodified_since_fails_when_newer():
    assert (
        evaluate(cond(if_unmodified_since=BEFORE), CURRENT)
        is Outcome.PRECONDITION_FAILED
    )


def test_if_unmodified_since_passes_when_not_newer():
    assert evaluate(cond(if_unmodified_since=T0), CURRENT) is Outcome.PASS
    assert evaluate(cond(if_unmodified_since=AFTER), CURRENT) is Outcome.PASS


def test_if_match_present_disables_if_unmodified_since():
    # If-Match matches, so step 2 must be skipped even though I-U-S would fail.
    c = cond(if_match_raw=f'"{ETAG.opaque}"', if_unmodified_since=BEFORE)
    assert evaluate(c, CURRENT) is Outcome.PASS


# ---- If-None-Match (step 3) ------------------------------------------------

def test_if_none_match_hit_get_gives_304():
    assert (
        evaluate(cond(if_none_match_raw=f'"{ETAG.opaque}"'), CURRENT)
        is Outcome.NOT_MODIFIED
    )


def test_if_none_match_hit_put_gives_412():
    assert (
        evaluate(cond("PUT", if_none_match_raw=f'"{ETAG.opaque}"'), CURRENT)
        is Outcome.PRECONDITION_FAILED
    )


def test_if_none_match_uses_weak_comparison():
    assert (
        evaluate(cond(if_none_match_raw=f'W/"{ETAG.opaque}"'), CURRENT)
        is Outcome.NOT_MODIFIED
    )


def test_if_none_match_wildcard():
    assert evaluate(cond(if_none_match_raw="*"), CURRENT) is Outcome.NOT_MODIFIED
    assert evaluate(cond(if_none_match_raw="*"), MISSING) is Outcome.PASS


def test_if_none_match_mismatch_passes():
    assert evaluate(cond(if_none_match_raw='"other"'), CURRENT) is Outcome.PASS


# ---- If-Modified-Since (step 4) --------------------------------------------

def test_if_modified_since_not_modified():
    assert evaluate(cond(if_modified_since=T0), CURRENT) is Outcome.NOT_MODIFIED
    assert evaluate(cond(if_modified_since=AFTER), CURRENT) is Outcome.NOT_MODIFIED


def test_if_modified_since_modified():
    assert evaluate(cond(if_modified_since=BEFORE), CURRENT) is Outcome.PASS


def test_if_modified_since_ignored_for_put():
    assert (
        evaluate(cond("PUT", if_modified_since=AFTER), CURRENT) is Outcome.PASS
    )


def test_if_modified_since_ignored_when_if_none_match_present():
    c = cond(if_none_match_raw='"nomatch"', if_modified_since=AFTER)
    assert evaluate(c, CURRENT) is Outcome.PASS


# ---- Ordering / short-circuiting -------------------------------------------

def test_if_match_failure_short_circuits_if_none_match():
    # If-Match fails -> 412 even though If-None-Match would give 304.
    c = cond(if_match_raw='"nope"', if_none_match_raw=f'"{ETAG.opaque}"')
    assert evaluate(c, CURRENT) is Outcome.PRECONDITION_FAILED


def test_if_unmodified_since_failure_short_circuits_if_none_match():
    c = cond(if_unmodified_since=BEFORE, if_none_match_raw=f'"{ETAG.opaque}"')
    assert evaluate(c, CURRENT) is Outcome.PRECONDITION_FAILED


def test_if_none_match_beats_if_modified_since():
    # Both present and both "match": step 3 wins (304 either way here,
    # but with PUT the distinction is 412 vs pass).
    c = cond("PUT", if_none_match_raw=f'"{ETAG.opaque}"', if_modified_since=AFTER)
    assert evaluate(c, CURRENT) is Outcome.PRECONDITION_FAILED


def test_no_conditions_passes():
    assert evaluate(cond(), CURRENT) is Outcome.PASS
    assert evaluate(cond(), MISSING) is Outcome.PASS
