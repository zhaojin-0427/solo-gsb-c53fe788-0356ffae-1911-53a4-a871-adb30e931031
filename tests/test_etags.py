"""Unit tests for entity-tag parsing and comparison."""
from app.etags import (
    ETag,
    make_etag,
    parse_etag,
    parse_etag_list,
    strong_equal,
    weak_equal,
)


def test_parse_strong_etag():
    tag = parse_etag('"abc123"')
    assert tag == ETag("abc123", weak=False)
    assert str(tag) == '"abc123"'


def test_parse_weak_etag():
    tag = parse_etag('W/"abc123"')
    assert tag == ETag("abc123", weak=True)
    assert str(tag) == 'W/"abc123"'


def test_parse_etag_rejects_garbage():
    assert parse_etag("abc123") is None
    assert parse_etag('"unterminated') is None
    assert parse_etag('W/"a"extra') is None


def test_parse_etag_list_multiple():
    tags = parse_etag_list('"a", W/"b" ,"c"')
    assert tags == [ETag("a"), ETag("b", weak=True), ETag("c")]


def test_parse_etag_list_wildcard():
    assert parse_etag_list("*") == ["*"]
    assert parse_etag_list("  *  ") == ["*"]


def test_parse_etag_list_invalid_returns_none():
    assert parse_etag_list('"a", garbage') is None
    assert parse_etag_list('*, "a"') is None  # * must stand alone
    assert parse_etag_list("") is None


def test_strong_comparison_requires_both_strong():
    a = ETag("x")
    w = ETag("x", weak=True)
    assert strong_equal(a, ETag("x"))
    assert not strong_equal(a, w)
    assert not strong_equal(w, w)
    assert not strong_equal(a, ETag("y"))


def test_weak_comparison_ignores_weakness():
    assert weak_equal(ETag("x"), ETag("x", weak=True))
    assert weak_equal(ETag("x", weak=True), ETag("x", weak=True))
    assert not weak_equal(ETag("x"), ETag("y"))


def test_make_etag_is_strong():
    tag = make_etag("0" * 64)
    assert not tag.weak and tag.opaque == "0" * 64


def test_empty_list_elements_ignored():
    assert parse_etag_list('"a", , "b"') == [ETag("a"), ETag("b")]
    assert parse_etag_list(' , ') is None
