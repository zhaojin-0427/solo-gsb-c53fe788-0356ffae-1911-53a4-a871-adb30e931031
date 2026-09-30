"""Unit tests for byte-range parsing, coalescing and multipart helpers."""
from datetime import datetime, timezone

from app.etags import make_etag
from app.ranges import (
    ByteRange,
    closing_delimiter,
    if_range_matches,
    multipart_boundary,
    multipart_content_length,
    parse_byte_ranges,
    MultipartPart,
)

LEN = 1000
ETAG = make_etag("b" * 64)
T0 = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_single_range():
    assert parse_byte_ranges("bytes=0-99", LEN) == [ByteRange(0, 99)]


def test_open_ended_range():
    assert parse_byte_ranges("bytes=100-", LEN) == [ByteRange(100, 999)]


def test_suffix_range():
    assert parse_byte_ranges("bytes=-100", LEN) == [ByteRange(900, 999)]


def test_suffix_larger_than_entity_clips():
    assert parse_byte_ranges("bytes=-5000", LEN) == [ByteRange(0, 999)]


def test_last_byte_beyond_length_clips():
    assert parse_byte_ranges("bytes=0-9999", LEN) == [ByteRange(0, 999)]


def test_unsatisfiable_range_returns_empty():
    assert parse_byte_ranges("bytes=1000-2000", LEN) == []
    assert parse_byte_ranges("bytes=-0", LEN) == []
    assert parse_byte_ranges("bytes=5-2", LEN) == []


def test_malformed_range_returns_none():
    assert parse_byte_ranges("items=0-10", LEN) is None
    assert parse_byte_ranges("bytes=", LEN) is None
    assert parse_byte_ranges("bytes=a-b", LEN) is None
    assert parse_byte_ranges("bytes=1-2,x", LEN) is None
    assert parse_byte_ranges("bytes=-", LEN) is None


def test_overlapping_ranges_merge():
    assert parse_byte_ranges("bytes=0-100,50-200", LEN) == [ByteRange(0, 200)]


def test_adjacent_ranges_merge():
    assert parse_byte_ranges("bytes=0-99,100-199", LEN) == [ByteRange(0, 199)]


def test_disjoint_ranges_stay_separate_and_sorted():
    assert parse_byte_ranges("bytes=500-599,0-99", LEN) == [
        ByteRange(0, 99),
        ByteRange(500, 599),
    ]


def test_unsatisfiable_members_dropped_valid_kept():
    assert parse_byte_ranges("bytes=0-99,5000-6000", LEN) == [ByteRange(0, 99)]


def test_empty_representation():
    assert parse_byte_ranges("bytes=0-0", 0) == []
    assert parse_byte_ranges("bytes=-5", 0) == []


def test_boundary_is_deterministic():
    ranges = [ByteRange(0, 9), ByteRange(20, 29)]
    b1 = multipart_boundary(ETAG, ranges, "text/plain")
    b2 = multipart_boundary(ETAG, ranges, "text/plain")
    assert b1 == b2
    # Different revision or ranges -> different boundary.
    assert b1 != multipart_boundary(make_etag("c" * 64), ranges, "text/plain")
    assert b1 != multipart_boundary(ETAG, [ByteRange(0, 9)], "text/plain")


def test_multipart_length_matches_rendered_parts():
    ranges = [ByteRange(0, 9), ByteRange(20, 29)]
    boundary = multipart_boundary(ETAG, ranges, "text/plain")
    expected = multipart_content_length(ranges, LEN, "text/plain", boundary)
    rendered = b""
    for r in ranges:
        part = MultipartPart(r.first, r.last, LEN, "text/plain")
        rendered += part.header_block(boundary)
        rendered += b"x" * r.length + b"\r\n"
    rendered += closing_delimiter(boundary)
    assert expected == len(rendered)


def test_if_range_strong_etag_matches():
    assert if_range_matches(f'"{ETAG.opaque}"', ETAG, T0)


def test_if_range_weak_etag_never_matches():
    assert not if_range_matches(f'W/"{ETAG.opaque}"', ETAG, T0)


def test_if_range_other_etag_fails():
    assert not if_range_matches('"zzz"', ETAG, T0)


def test_if_range_date_equal_matches():
    assert if_range_matches("Tue, 01 Sep 2026 12:00:00 GMT", ETAG, T0)


def test_if_range_date_older_fails():
    assert not if_range_matches("Tue, 01 Sep 2026 11:00:00 GMT", ETAG, T0)


def test_if_range_garbage_fails_closed():
    assert not if_range_matches("???", ETAG, T0)


def test_empty_range_list_elements_ignored():
    assert parse_byte_ranges("bytes=0-9,", LEN) == [ByteRange(0, 9)]
    assert parse_byte_ranges("bytes=,0-9,,20-29", LEN) == [ByteRange(0, 9), ByteRange(20, 29)]
    assert parse_byte_ranges("bytes=,,", LEN) is None
