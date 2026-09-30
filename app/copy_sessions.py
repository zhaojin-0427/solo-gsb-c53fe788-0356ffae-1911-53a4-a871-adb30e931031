"""Deterministic cursors, digests and segmentation for copy sessions.

A copy session pins a manifest of (source path, revision) pairs captured
in one database snapshot.  Every segment of the manifest is addressed by
an opaque *cursor*: a tamper-evident token derived deterministically
from the session id, the segment number, the manifest hash and the
resume boundary (the segment's last source path).

Because a cursor is a pure function of the session state:

* re-issuing a cursor for the same segment always yields the same token
  (repeated pulls return identical cursors);
* a cursor minted for one session / manifest is rejected by any other
  (that is what "cursor expired" means on pull and acknowledge);
* a cursor from a finished or aborted session can be presented when
  creating a *new* session with the same source set and target prefix,
  resuming the copy right after the cursor's ``last_path``.

Integrity is provided by HMAC-SHA-256 keyed with a server-side secret
stored in the ``service_meta`` table, so tokens survive restarts.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Iterable, Protocol

CURSOR_VERSION = 1
_MAC_LEN = 18  # bytes of HMAC-SHA-256 kept in the token


@dataclass(frozen=True, slots=True)
class CopyCursor:
    """Decoded cursor payload.

    ``last_path`` is the last source path of the segment the cursor
    identifies; it is the resume boundary used when a new session is
    created with this cursor.
    """

    session_id: str
    segment: int
    manifest_sha256: str
    last_path: str
    target_prefix: str
    source_hash: str


class _EntryLike(Protocol):
    source_path: str
    revision_id: int
    revision_no: int
    sha256: str
    target_path: str


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(token: str) -> bytes | None:
    try:
        padded = token + "=" * (-len(token) % 4)
        return base64.urlsafe_b64decode(padded.encode("ascii"))
    except Exception:
        return None


def encode_cursor(secret: str, cursor: CopyCursor) -> str:
    """Serialise and sign a cursor; deterministic for the same input."""
    payload = json.dumps(
        {
            "v": CURSOR_VERSION,
            "sid": cursor.session_id,
            "seg": cursor.segment,
            "mhash": cursor.manifest_sha256,
            "path": cursor.last_path,
            "tp": cursor.target_prefix,
            "shash": cursor.source_hash,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    mac = hmac.new(secret.encode("ascii"), payload, hashlib.sha256).digest()
    return _b64encode(payload) + "." + _b64encode(mac[:_MAC_LEN])


def decode_cursor(secret: str, token: str) -> CopyCursor | None:
    """Verify and decode a cursor token; ``None`` when invalid."""
    if not token or "." not in token:
        return None
    payload_b64, mac_b64 = token.rsplit(".", 1)
    payload = _b64decode(payload_b64)
    mac = _b64decode(mac_b64)
    if payload is None or mac is None:
        return None
    expected = hmac.new(
        secret.encode("ascii"), payload, hashlib.sha256
    ).digest()[:_MAC_LEN]
    if not hmac.compare_digest(mac, expected):
        return None
    try:
        data = json.loads(payload)
    except ValueError:
        return None
    if not isinstance(data, dict) or data.get("v") != CURSOR_VERSION:
        return None
    try:
        segment = data["seg"]
        if (
            not isinstance(segment, int)
            or isinstance(segment, bool)
            or segment < 0
        ):
            return None
        return CopyCursor(
            session_id=_checked_str(data["sid"]),
            segment=segment,
            manifest_sha256=_checked_str(data["mhash"]),
            last_path=_checked_str(data["path"]),
            target_prefix=_checked_str(data["tp"]),
            source_hash=_checked_str(data["shash"]),
        )
    except (KeyError, TypeError):
        return None


def _checked_str(value) -> str:
    if not isinstance(value, str):
        raise TypeError("cursor field must be a string")
    return value


def source_set_hash(paths: Iterable[str]) -> str:
    """Digest of the de-duplicated source path set (order independent)."""
    h = hashlib.sha256()
    for path in sorted(set(paths)):
        h.update(path.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()


def manifest_hash(entries: Iterable[_EntryLike]) -> str:
    """Digest of the ordered manifest: paths, pinned revisions, digests."""
    h = hashlib.sha256()
    for e in entries:
        h.update(
            f"{e.source_path}\x00{e.revision_id}\x00{e.revision_no}\x00"
            f"{e.sha256}\x00{e.target_path}\x00".encode("utf-8")
        )
    return h.hexdigest()


def segment_digest(entries: Iterable[_EntryLike]) -> str:
    """Digest of one segment's entries (path, revision, content digest)."""
    h = hashlib.sha256()
    for e in entries:
        h.update(
            f"{e.source_path}\x00{e.revision_no}\x00{e.sha256}\x00".encode(
                "utf-8"
            )
        )
    return h.hexdigest()
