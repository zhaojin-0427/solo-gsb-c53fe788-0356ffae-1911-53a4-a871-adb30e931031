"""Byte-range parsing, coalescing and multipart rendering.

Implements the ``bytes`` range unit of RFC 9110 (Section 14) with:

* single ranges: ``bytes=0-99``, open-ended ``bytes=0-``
* suffix ranges: ``bytes=-500``
* multiple ranges: ``bytes=0-49,100-149``
* merging of overlapping or adjacent intervals, always served ascending
* deterministic multipart/byteranges boundaries (identical requests
  produce byte-identical bodies)
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .etags import ETag, parse_etag
from .dates import parse_http_date


@dataclass(frozen=True, slots=True)
class ByteRange:
    first: int
    last: int  # inclusive

    @property
    def length(self) -> int:
        return self.last - self.first + 1

    def content_range(self, complete_length: int) -> str:
        return f"bytes {self.first}-{self.last}/{complete_length}"


def parse_byte_ranges(header_value: str, complete_length: int) -> list[ByteRange] | None:
    """Parse a Range header value against a representation length.

    Returns ``None`` when the range-unit is not ``bytes`` or the spec is
    syntactically invalid (the range is then ignored).  Returns an empty
    list when the spec parsed but every range is unsatisfiable (416).
    """
    value = header_value.strip()
    if not value.lower().startswith("bytes="):
        return None
    spec = value[len("bytes="):]
    if not spec or spec == ",":
        return None

    ranges: list[ByteRange] = []
    saw_valid = False
    for raw in spec.split(","):
        item = raw.strip()
        if not item:
            # RFC 9110 5.6.1.1: empty list elements are ignored.
            continue
        if "-" not in item:
            # Invalid sub-range syntax => the whole Range header is invalid.
            return None
        start_s, end_s = item.split("-", 1)
        start_s, end_s = start_s.strip(), end_s.strip()

        if start_s == "":
            # Suffix range: bytes=-N (last N bytes).
            if end_s == "" or not end_s.isascii() or not end_s.isdigit():
                return None
            suffix_length = int(end_s)
            saw_valid = True
            if suffix_length == 0:
                # bytes=-0 is syntactically valid but unsatisfiable.
                continue
            if complete_length == 0:
                continue
            first = max(0, complete_length - suffix_length)
            ranges.append(ByteRange(first, complete_length - 1))
            continue

        if not start_s.isascii() or not start_s.isdigit():
            return None
        first = int(start_s)
        if end_s == "":
            last = complete_length - 1
        elif end_s.isascii() and end_s.isdigit():
            last = int(end_s)
        else:
            return None
        saw_valid = True
        if complete_length == 0 or first >= complete_length or first > last:
            # Unsatisfiable: start past the end, or last before first.
            continue
        ranges.append(ByteRange(first, min(last, complete_length - 1)))

    if not saw_valid:
        return None
    return coalesce_ranges(ranges)


def coalesce_ranges(ranges: list[ByteRange]) -> list[ByteRange]:
    """Merge overlapping or adjacent ranges; output is ascending, unique."""
    if not ranges:
        return []
    ordered = sorted(ranges, key=lambda r: (r.first, r.last))
    merged = [ordered[0]]
    for cur in ordered[1:]:
        prev = merged[-1]
        # Adjacent (cur.first == prev.last + 1) intervals merge too.
        if cur.first <= prev.last + 1:
            if cur.last > prev.last:
                merged[-1] = ByteRange(prev.first, cur.last)
        else:
            merged.append(cur)
    return merged


def if_range_matches(
    if_range_raw: str, etag: ETag, last_modified
) -> bool:
    """Evaluate an If-Range validator.

    If-Range takes either an entity-tag or an HTTP-date.  A weak ETag
    never matches (the strong comparison function is required), and an
    unparseable value causes a full-entity fallback.
    """
    value = if_range_raw.strip()
    if value.startswith(("W/", '"', "w/")):
        tag = parse_etag(value)
        if tag is None:
            return False
        return not tag.weak and not etag.weak and tag.opaque == etag.opaque
    dt = parse_http_date(value)
    if dt is None or last_modified is None:
        return False
    # HTTP-date comparison is at one-second resolution; equality means
    # the representation has not changed since the validator was issued.
    return int(last_modified.timestamp()) == int(dt.timestamp())


def multipart_boundary(
    etag: ETag, ranges: list[ByteRange], media_type: str
) -> str:
    """Derive a stable boundary from the revision and requested ranges.

    The boundary depends only on the entity-tag (revision identity), the
    coalesced range set in ascending order, and the media type, so a
    repeated byte-identical request yields the same boundary and body.
    """
    h = hashlib.sha256()
    h.update(etag.opaque.encode("ascii"))
    h.update(b"|")
    h.update(media_type.encode("ascii"))
    for r in ranges:
        h.update(f"|{r.first}-{r.last}".encode("ascii"))
    return "byteranges_" + h.hexdigest()


def multipart_content_type(boundary: str) -> str:
    return f"multipart/byteranges; boundary={boundary}"


@dataclass(slots=True)
class MultipartPart:
    first: int
    last: int
    complete_length: int
    media_type: str

    def header_block(self, boundary: str) -> bytes:
        return (
            f"--{boundary}\r\n"
            f"Content-Type: {self.media_type}\r\n"
            f"Content-Range: bytes {self.first}-{self.last}/{self.complete_length}\r\n\r\n"
        ).encode("ascii")


def closing_delimiter(boundary: str) -> bytes:
    return f"--{boundary}--\r\n".encode("ascii")


def multipart_content_length(
    ranges: list[ByteRange], complete_length: int, media_type: str, boundary: str
) -> int:
    total = 0
    for r in ranges:
        part = MultipartPart(r.first, r.last, complete_length, media_type)
        total += len(part.header_block(boundary)) + r.length + len(b"\r\n")
    total += len(closing_delimiter(boundary))
    return total
