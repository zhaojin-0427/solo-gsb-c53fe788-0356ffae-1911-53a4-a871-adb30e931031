"""Entity-tag parsing and comparison (RFC 9110, Section 8.8).

ETags produced by this origin are always *strong* SHA-256 digests, but
requests may carry weak validators (``W/"..."``), so both weak and strong
comparison functions are implemented per RFC 9110 Section 8.8.3.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# 1*etagc, where etagc excludes obs-text and the DQUOTE (0x22).
_ETAGC = r'[\x21\x23-\x7E\x80-\xFF]'
_ONE_ETAG = re.compile(rf'\s*(W/)?"({_ETAGC}*)"\s*')
_IF_MATCH_ANY = re.compile(r'^\s*\*\s*$')


@dataclass(frozen=True, slots=True)
class ETag:
    opaque: str
    weak: bool = False

    def __str__(self) -> str:
        return f'W/"{self.opaque}"' if self.weak else f'"{self.opaque}"'


def make_etag(digest_hex: str) -> ETag:
    """Wrap a hex digest in a strong validator."""
    return ETag(opaque=digest_hex, weak=False)


def parse_etag(value: str) -> ETag | None:
    """Parse a single entity-tag; return None when malformed."""
    m = _ONE_ETAG.fullmatch(value)
    if not m:
        return None
    return ETag(opaque=m.group(2), weak=bool(m.group(1)))


def parse_etag_list(value: str) -> list[ETag] | None:
    """Parse an If-Match / If-None-Match list.

    ``['*']`` is returned for the wildcard.  Empty list elements are
    ignored per RFC 9110 Section 5.6.1.1.  A malformed (non-empty)
    element invalidates the whole field; per RFC 9110 recipients MUST
    then ignore the header field.
    """
    if _IF_MATCH_ANY.match(value):
        return ["*"]  # type: ignore[list-item]
    tags: list[ETag] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        tag = parse_etag(item)
        if tag is None:
            return None
        tags.append(tag)
    return tags or None


def strong_equal(a: ETag, b: ETag) -> bool:
    """Strong comparison (RFC 9110 8.8.3.2): both strong and opaque equal."""
    return not a.weak and not b.weak and a.opaque == b.opaque


def weak_equal(a: ETag, b: ETag) -> bool:
    """Weak comparison (RFC 9110 8.8.3.3): opaque equal regardless of weakness."""
    return a.opaque == b.opaque
