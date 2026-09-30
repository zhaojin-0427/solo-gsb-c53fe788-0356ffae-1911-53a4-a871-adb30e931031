"""End-to-end tests against a real PostgreSQL (pgserver) and a live
uvicorn server running in a background thread."""
from __future__ import annotations

import hashlib
import threading
import time
import uuid

CONTENT = b"The quick brown fox jumps over the lazy dog. " * 8  # 360 bytes
ETAG_OF = lambda body: '"' + hashlib.sha256(body).hexdigest() + '"'


def path(name: str) -> str:
    return f"/objects/itest/{name}-{uuid.uuid4().hex[:12]}"


# ---- basic publish / read --------------------------------------------------

def test_put_get_head_roundtrip(client):
    p = path("roundtrip")
    r = client.put(p, content=CONTENT, headers={"Content-Type": "text/plain"})
    assert r.status_code == 204
    assert r.headers["etag"] == ETAG_OF(CONTENT)
    assert "last-modified" in r.headers
    assert r.headers["x-revision-no"] == "1"

    g = client.get(p)
    assert g.status_code == 200
    assert g.content == CONTENT
    assert g.headers["etag"] == ETAG_OF(CONTENT)
    assert g.headers["accept-ranges"] == "bytes"
    assert g.headers["content-length"] == str(len(CONTENT))
    assert g.headers["content-type"].startswith("text/plain")
    assert g.headers["last-modified"] == r.headers["last-modified"]

    h = client.head(p)
    assert h.status_code == 200
    assert h.headers["etag"] == ETAG_OF(CONTENT)
    assert h.headers["content-length"] == str(len(CONTENT))
    assert h.content == b""


def test_get_missing_is_404(client):
    assert client.get(path("nope")).status_code == 404


def test_republish_creates_new_revision(client):
    p = path("rev")
    r1 = client.put(p, content=b"v1")
    r2 = client.put(p, content=b"v2-content")
    assert r2.headers["x-revision-no"] == "2"
    assert r2.headers["etag"] != r1.headers["etag"]
    g = client.get(p)
    assert g.content == b"v2-content"
    assert g.headers["etag"] == r2.headers["etag"]


# ---- conditional GET (304 / 412) -------------------------------------------

def test_if_none_match_strong_and_weak(client):
    p = path("inm")
    etag = client.put(p, content=CONTENT).headers["etag"]
    assert client.get(p, headers={"If-None-Match": etag}).status_code == 304
    weak = "W/" + etag
    assert client.get(p, headers={"If-None-Match": weak}).status_code == 304
    assert client.get(p, headers={"If-None-Match": '"other"'}).status_code == 200


def test_if_none_match_wildcard(client):
    p = path("inm-star")
    client.put(p, content=CONTENT)
    assert client.get(p, headers={"If-None-Match": "*"}).status_code == 304
    assert client.get(path("missing"), headers={"If-None-Match": "*"}).status_code == 404


def test_304_carries_validators(client):
    p = path("304-hdrs")
    put = client.put(p, content=CONTENT)
    r = client.get(p, headers={"If-None-Match": put.headers["etag"]})
    assert r.status_code == 304
    assert r.headers["etag"] == put.headers["etag"]
    assert r.headers["last-modified"] == put.headers["last-modified"]


def test_if_modified_since(client):
    p = path("ims")
    lm = client.put(p, content=CONTENT).headers["last-modified"]
    assert client.get(p, headers={"If-Modified-Since": lm}).status_code == 304
    assert (
        client.get(p, headers={"If-Modified-Since": "Wed, 01 Jan 2020 00:00:00 GMT"}).status_code
        == 200
    )


def test_if_match_on_get(client):
    p = path("im-get")
    etag = client.put(p, content=CONTENT).headers["etag"]
    assert client.get(p, headers={"If-Match": etag}).status_code == 200
    assert client.get(p, headers={"If-Match": "*"}).status_code == 200
    assert client.get(p, headers={"If-Match": '"stale"'}).status_code == 412
    # Weak form must fail the strong comparison If-Match requires.
    assert client.get(p, headers={"If-Match": "W/" + etag}).status_code == 412


def test_if_unmodified_since_on_get(client):
    p = path("ius-get")
    lm = client.put(p, content=CONTENT).headers["last-modified"]
    assert client.get(p, headers={"If-Unmodified-Since": lm}).status_code == 200
    assert (
        client.get(p, headers={"If-Unmodified-Since": "Wed, 01 Jan 2020 00:00:00 GMT"}).status_code
        == 412
    )


def test_precondition_ordering_if_match_beats_if_none_match(client):
    p = path("order")
    etag = client.put(p, content=CONTENT).headers["etag"]
    # If-Match fails -> 412, even though If-None-Match alone would give 304.
    r = client.get(p, headers={"If-Match": '"nope"', "If-None-Match": etag})
    assert r.status_code == 412


# ---- conditional PUT --------------------------------------------------------

def test_put_if_none_match_star(client):
    p = path("create-only")
    assert client.put(p, content=b"first", headers={"If-None-Match": "*"}).status_code == 204
    assert client.put(p, content=b"second", headers={"If-None-Match": "*"}).status_code == 412
    assert client.get(p).content == b"first"


def test_put_if_match(client):
    p = path("optimistic")
    r1 = client.put(p, content=b"v1")
    etag1 = r1.headers["etag"]
    # Stale validator -> 412, object untouched.
    assert client.put(p, content=b"v2", headers={"If-Match": '"stale"'}).status_code == 412
    assert client.get(p).content == b"v1"
    # Correct validator -> 204.
    r2 = client.put(p, content=b"v2", headers={"If-Match": etag1})
    assert r2.status_code == 204
    assert client.get(p).content == b"v2"
    # If-Match: * requires existence.
    assert (
        client.put(path("fresh"), content=b"x", headers={"If-Match": "*"}).status_code == 412
    )


def test_put_if_unmodified_since(client):
    p = path("put-ius")
    lm = client.put(p, content=b"v1").headers["last-modified"]
    assert (
        client.put(p, content=b"v2", headers={"If-Unmodified-Since": "Wed, 01 Jan 2020 00:00:00 GMT"}).status_code
        == 412
    )
    assert client.put(p, content=b"v2", headers={"If-Unmodified-Since": lm}).status_code == 204


# ---- ranges ------------------------------------------------------------------

def test_single_range(client):
    p = path("r1")
    client.put(p, content=CONTENT)
    r = client.get(p, headers={"Range": "bytes=0-9"})
    assert r.status_code == 206
    assert r.headers["content-range"] == f"bytes 0-9/{len(CONTENT)}"
    assert r.headers["content-length"] == "10"
    assert r.content == CONTENT[:10]
    assert r.headers["etag"] == ETAG_OF(CONTENT)


def test_suffix_and_open_ranges(client):
    p = path("r2")
    client.put(p, content=CONTENT)
    r = client.get(p, headers={"Range": "bytes=-10"})
    assert r.status_code == 206
    assert r.content == CONTENT[-10:]
    assert r.headers["content-range"] == f"bytes {len(CONTENT)-10}-{len(CONTENT)-1}/{len(CONTENT)}"
    r = client.get(p, headers={"Range": f"bytes={len(CONTENT)-5}-"})
    assert r.status_code == 206
    assert r.content == CONTENT[-5:]


def test_range_beyond_end_clips(client):
    p = path("r3")
    client.put(p, content=CONTENT)
    r = client.get(p, headers={"Range": "bytes=0-999999"})
    assert r.status_code == 206
    assert r.content == CONTENT


def test_unsatisfiable_range_is_416(client):
    p = path("r4")
    client.put(p, content=CONTENT)
    r = client.get(p, headers={"Range": "bytes=99999-100000"})
    assert r.status_code == 416
    assert r.headers["content-range"] == f"bytes */{len(CONTENT)}"


def test_malformed_range_ignored(client):
    p = path("r5")
    client.put(p, content=CONTENT)
    r = client.get(p, headers={"Range": "bytes=abc-def"})
    assert r.status_code == 200
    assert r.content == CONTENT


def test_adjacent_and_overlapping_ranges_merge(client):
    p = path("r6")
    client.put(p, content=CONTENT)
    r = client.get(p, headers={"Range": "bytes=0-9,10-19"})  # adjacent
    assert r.status_code == 206
    assert r.headers["content-range"] == f"bytes 0-19/{len(CONTENT)}"
    assert r.content == CONTENT[:20]
    r = client.get(p, headers={"Range": "bytes=0-15,10-25"})  # overlapping
    assert r.headers["content-range"] == f"bytes 0-25/{len(CONTENT)}"
    assert r.content == CONTENT[:26]


def _parse_multipart(body: bytes, boundary: str):
    parts = {}
    for chunk in body.split(f"--{boundary}\r\n".encode())[1:]:
        if chunk.startswith(b"--"):
            break
        head, data = chunk.split(b"\r\n\r\n", 1)
        headers = head.decode().split("\r\n")
        cr = [h for h in headers if h.lower().startswith("content-range:")][0]
        spec = cr.split(":", 1)[1].strip()  # bytes a-b/n
        rng = spec.split()[1].split("/")[0]
        first, last = (int(x) for x in rng.split("-"))
        parts[(first, last)] = data[: last - first + 1]
    return parts


def test_multipart_ranges_deterministic(client):
    p = path("r7")
    client.put(p, content=CONTENT)
    hdr = {"Range": "bytes=100-109,0-9,50-59"}  # unordered on the wire
    r1 = client.get(p, headers=hdr)
    r2 = client.get(p, headers=hdr)
    assert r1.status_code == 206
    ct = r1.headers["content-type"]
    assert ct.startswith("multipart/byteranges; boundary=")
    # Repeated request: identical boundary and identical body.
    assert r2.headers["content-type"] == ct
    assert r2.content == r1.content
    assert r1.headers["content-length"] == str(len(r1.content))

    boundary = ct.split("boundary=")[1]
    parts = _parse_multipart(r1.content, boundary)
    # Ascending order after coalescing.
    assert list(parts.keys()) == [(0, 9), (50, 59), (100, 109)]
    for (first, last), data in parts.items():
        assert data == CONTENT[first : last + 1]


def test_head_with_range(client):
    p = path("r8")
    client.put(p, content=CONTENT)
    r = client.head(p, headers={"Range": "bytes=0-9"})
    assert r.status_code == 206
    assert r.headers["content-range"] == f"bytes 0-9/{len(CONTENT)}"
    assert r.headers["content-length"] == "10"
    assert r.content == b""


def test_if_range_etag(client):
    p = path("r9")
    etag = client.put(p, content=CONTENT).headers["etag"]
    r = client.get(p, headers={"Range": "bytes=0-9", "If-Range": etag})
    assert r.status_code == 206
    r = client.get(p, headers={"Range": "bytes=0-9", "If-Range": '"stale"'})
    assert r.status_code == 200
    assert r.content == CONTENT
    # Weak etag never matches If-Range.
    r = client.get(p, headers={"Range": "bytes=0-9", "If-Range": "W/" + etag})
    assert r.status_code == 200


def test_if_range_date(client):
    p = path("r10")
    lm = client.put(p, content=CONTENT).headers["last-modified"]
    r = client.get(p, headers={"Range": "bytes=0-9", "If-Range": lm})
    assert r.status_code == 206
    r = client.get(
        p, headers={"Range": "bytes=0-9", "If-Range": "Wed, 01 Jan 2020 00:00:00 GMT"}
    )
    assert r.status_code == 200
    assert r.content == CONTENT


def test_range_after_update_uses_new_revision(client):
    p = path("r11")
    client.put(p, content=b"A" * 100)
    etag2 = client.put(p, content=b"B" * 100).headers["etag"]
    r = client.get(p, headers={"Range": "bytes=0-9"})
    assert r.content == b"B" * 10
    assert r.headers["etag"] == etag2


# ---- concurrency --------------------------------------------------------------

def test_concurrent_create_only_one_wins(client):
    p = path("race")
    results = []

    def publish(i):
        results.append(
            client.put(p, content=f"body-{i}".encode(),
                       headers={"If-None-Match": "*"}).status_code
        )

    threads = [threading.Thread(target=publish, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results).count(204) == 1
    assert results.count(412) == 5


def test_stream_not_mixed_with_concurrent_publish(client):
    p = path("stream")
    v1 = bytes((i * 7 + 13) % 256 for i in range(3 * 1024 * 1024))  # 3 MiB
    v2 = b"X" * (3 * 1024 * 1024)
    etag1 = client.put(p, content=v1).headers["etag"]

    put_result = {}

    def republish():
        put_result["resp"] = client.put(p, content=v2)

    with client.stream("GET", p) as resp:
        assert resp.status_code == 200
        assert resp.headers["etag"] == etag1
        it = resp.iter_bytes()
        first = next(it)
        t = threading.Thread(target=republish)
        t.start()
        time.sleep(0.3)  # let the PUT reach its lock wait
        rest = b"".join(it)
    t.join(timeout=30)
    assert not t.is_alive()
    # The in-flight response stayed bound to revision 1.
    assert first + rest == v1
    assert put_result["resp"].status_code == 204
    # And the next read sees the new revision.
    g = client.get(p)
    assert g.content == v2
    assert g.headers["etag"] != etag1
