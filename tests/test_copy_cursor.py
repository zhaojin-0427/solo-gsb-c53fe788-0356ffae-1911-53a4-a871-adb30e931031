"""Unit tests for copy-session cursors and manifest digests."""
from dataclasses import replace
from types import SimpleNamespace

from app.copy_sessions import (
    CopyCursor,
    decode_cursor,
    encode_cursor,
    manifest_hash,
    segment_digest,
    source_set_hash,
)

SECRET = "00" * 32
CURSOR = CopyCursor(
    session_id="s" * 32,
    segment=3,
    manifest_sha256="a" * 64,
    last_path="dir/b.txt",
    target_prefix="backup/",
    source_hash="b" * 64,
)


def test_cursor_roundtrip():
    token = encode_cursor(SECRET, CURSOR)
    assert decode_cursor(SECRET, token) == CURSOR


def test_cursor_is_deterministic():
    assert encode_cursor(SECRET, CURSOR) == encode_cursor(SECRET, CURSOR)


def test_cursor_segments_differ():
    other = encode_cursor(SECRET, replace(CURSOR, segment=4))
    assert other != encode_cursor(SECRET, CURSOR)


def test_cursor_rejects_wrong_secret():
    token = encode_cursor(SECRET, CURSOR)
    assert decode_cursor("ff" * 32, token) is None


def test_cursor_rejects_tampering():
    token = encode_cursor(SECRET, CURSOR)
    payload, mac = token.rsplit(".", 1)
    # Flip a character in the payload and in the MAC.
    flip = "A" if payload[5] != "A" else "B"
    assert decode_cursor(SECRET, payload[:5] + flip + payload[6:] + "." + mac) is None
    flip = "A" if mac[2] != "A" else "B"
    assert decode_cursor(SECRET, payload + "." + mac[:2] + flip + mac[3:]) is None


def test_cursor_rejects_garbage():
    assert decode_cursor(SECRET, "") is None
    assert decode_cursor(SECRET, "not-a-cursor") is None
    assert decode_cursor(SECRET, "!!!.@@@") is None
    assert decode_cursor(SECRET, "aGVsbG8.d29ybGQ") is None  # valid b64, bad MAC


def test_source_set_hash_order_independent_and_deduped():
    h1 = source_set_hash(["a/1", "b/2", "a/1"])
    h2 = source_set_hash(["b/2", "a/1"])
    assert h1 == h2
    assert h1 != source_set_hash(["a/1", "b/3"])


def _entry(path, rev_id=1, rev_no=1, sha="c" * 64, target=None):
    return SimpleNamespace(
        source_path=path,
        revision_id=rev_id,
        revision_no=rev_no,
        sha256=sha,
        target_path=target if target is not None else "backup/" + path,
    )


def test_manifest_hash_binds_order_and_revisions():
    e1, e2 = _entry("a", rev_id=1), _entry("b", rev_id=2)
    assert manifest_hash([e1, e2]) == manifest_hash([e1, e2])
    assert manifest_hash([e1, e2]) != manifest_hash([e2, e1])
    changed = _entry("b", rev_id=9)
    assert manifest_hash([e1, changed]) != manifest_hash([e1, e2])


def test_segment_digest_binds_content_digests():
    e1 = _entry("a", sha="1" * 64)
    e2 = _entry("a", sha="2" * 64)
    assert segment_digest([e1]) == segment_digest([_entry("a", sha="1" * 64)])
    assert segment_digest([e1]) != segment_digest([e2])
