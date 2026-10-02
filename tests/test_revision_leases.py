"""End-to-end tests for historical revision leases.

Covers:

* POST /revision-leases validation (path, positive revision_no, TTL
  window), 404 for revisions that are not in storage any more, and the
  201 response shape;
* GC interaction: active leases pin revisions in preview and execute
  (``revision_lease`` reason), expiry/release makes them collectable
  again;
* GET/HEAD .../content: the bound OLD revision is served with the
  existing precondition and Range rules; path migration and re-PUT do
  not change it; replays are identical;
* 410 after expiry/release (and after the revision was subsequently
  collected), 404 for unknown ids;
* DELETE idempotence;
* serialisation: lease creation vs GC execute never ends with a lease
  on a lost revision, and a stream that started while active finishes
  intact even if the lease is released or expires mid-read.
"""
from __future__ import annotations

import asyncio
import os
import threading
import time
import uuid
from datetime import datetime, timezone

MIN_TTL = 60
MAX_TTL = 86400
FUTURE = "2099-01-01T00:00:00Z"


def _path(name: str) -> str:
    return f"lease/{name}-{uuid.uuid4().hex[:12]}"


def _put(client, path: str, body: bytes, ctype="application/octet-stream") -> str:
    r = client.put(f"/objects/{path}", content=body, headers={"Content-Type": ctype})
    assert r.status_code == 204
    return r.headers["x-revision-no"]


def _lease(client, path, revision_no, ttl=3600):
    return client.post(
        "/revision-leases",
        json={"path": path, "revision_no": revision_no, "ttl_seconds": ttl},
    )


def _gc(client, paths, before=FUTURE, dry_run=True):
    return client.post(
        "/gc",
        json={"paths": paths, "before": before, "dry_run": dry_run},
    )


def _expire_lease(lease_id: str) -> None:
    """Force a lease past its expiry directly in the database (the API
    minimum TTL is 60s, so the expiry path is tested this way)."""
    import asyncpg

    async def run():
        conn = await asyncpg.connect(os.environ["DATABASE_URL"])
        try:
            await conn.execute(
                "UPDATE revision_leases SET expires_at = now() - interval '1 s'"
                " WHERE id = $1",
                lease_id,
            )
        finally:
            await conn.close()

    asyncio.run(run())


# ---- creation / validation ----------------------------------------------------


def test_create_lease_validation(client):
    p = _path("validation")
    _put(client, p, b"v1")

    assert client.post("/revision-leases", content=b"not json").status_code == 400
    assert client.post("/revision-leases", json=["x"]).status_code == 400
    assert client.post("/revision-leases", json={}).status_code == 400
    assert _lease(client, "", 1).status_code == 400
    assert _lease(client, 7, 1).status_code == 400
    assert _lease(client, p, 0).status_code == 400
    assert _lease(client, p, -3).status_code == 400
    assert _lease(client, p, "1").status_code == 400
    assert _lease(client, p, True).status_code == 400
    assert _lease(client, p, 1, ttl=MIN_TTL - 1).status_code == 400
    assert _lease(client, p, 1, ttl=MAX_TTL + 1).status_code == 400
    assert _lease(client, p, 1, ttl="60").status_code == 400
    assert _lease(client, p, 1, ttl=60.0).status_code == 400
    assert _lease(client, p, 1, ttl=True).status_code == 400
    # Boundary values of the window are accepted.
    assert _lease(client, p, 1, ttl=MIN_TTL).status_code == 201
    assert _lease(client, p, 1, ttl=MAX_TTL).status_code == 201


def test_create_lease_201_shape_and_etag(client):
    p = _path("shape")
    _put(client, p, b"v1")
    _put(client, p, b"v2 content")
    etag2 = client.head(f"/objects/{p}").headers["etag"]

    r = _lease(client, p, 2, ttl=120)
    assert r.status_code == 201
    data = r.json()
    lid = data["lease"]
    assert isinstance(lid, str) and len(lid) >= 32
    assert data["path"] == p
    assert data["revision_no"] == 2
    assert data["etag"] == etag2
    assert data["ttl_seconds"] == 120
    # Expiry is a parseable ISO instant roughly ttl seconds in the future.
    expires = datetime.fromisoformat(data["expires_at"])
    delta = (expires - datetime.now(timezone.utc)).total_seconds()
    assert 115 <= delta <= 125
    # Validator headers + a content-location-style Location are present.
    assert r.headers["etag"] == etag2
    assert r.headers["location"] == f"/revision-leases/{lid}/content"
    assert "last-modified" in r.headers


def test_create_lease_on_historical_revision(client):
    p = _path("historical")
    _put(client, p, b"old-bytes")
    _put(client, p, b"new-bytes")
    r = _lease(client, p, 1)
    assert r.status_code == 201
    assert r.json()["revision_no"] == 1


def test_create_lease_unknown_revision_is_404(client):
    p = _path("missing-rev")
    _put(client, p, b"v1")

    # Revision number never published on an existing path.
    r = _lease(client, p, 2)
    assert r.status_code == 404
    assert r.json() == {"detail": "revision_not_found", "path": p,
                        "revision_no": 2}

    # Path never used at all.
    q = _path("never-existed")
    r = _lease(client, q, 1)
    assert r.status_code == 404
    assert r.json()["path"] == q

    # Failed creation leaves no placeholder object behind: GET and GC on
    # the never-used path still see it as absent.
    assert client.get(f"/objects/{q}").status_code == 404
    assert _gc(client, [q]).status_code == 404


def test_lease_on_revision_collected_beforehand_is_404(client):
    p = _path("already-gone")
    _put(client, p, b"v1")
    _put(client, p, b"v2")
    assert _gc(client, [p], dry_run=False).status_code == 200
    # Revision 1 was collected; leasing it is a 404 even though the
    # number lies within the path's high-water range.
    assert _lease(client, p, 1).status_code == 404
    assert _lease(client, p, 2).status_code == 201


# ---- GC pinning ---------------------------------------------------------------


def test_gc_preview_reports_revision_lease_reason(client):
    p = _path("gc-preview")
    _put(client, p, b"v1")
    _put(client, p, b"v2")
    _put(client, p, b"v3")
    lid = _lease(client, p, 1).json()["lease"]

    data = _gc(client, [p]).json()
    (obj,) = data["objects"]
    assert obj["candidates"] == [{"revision_no": 2, "bytes": 2}]
    assert obj["retained"] == [
        {"revision_no": 1, "reasons": ["revision_lease"]},
        {"revision_no": 3, "reasons": ["current"]},
    ]
    # And the execute response reports the same retained list.
    exec_data = _gc(client, [p], dry_run=False).json()
    assert exec_data["deleted"] == [{"path": p, "revision_no": 2, "bytes": 2}]
    retained = [o for o in exec_data["retained"] if o["path"] == p][0]
    assert retained["retained"] == [
        {"revision_no": 1, "reasons": ["revision_lease"]},
        {"revision_no": 3, "reasons": ["current"]},
    ]
    assert lid  # lease stays usable


def test_gc_execute_does_not_delete_leased_revision(client):
    p = _path("gc-execute-pin")
    _put(client, p, b"v1-aaaa")
    _put(client, p, b"v2-bb")
    lid = _lease(client, p, 1).json()["lease"]

    data = _gc(client, [p], dry_run=False).json()
    assert data["deleted"] == []
    # The pinned revision still serves its original bytes via the lease.
    r = client.get(f"/revision-leases/{lid}/content")
    assert r.status_code == 200
    assert r.content == b"v1-aaaa"


def test_lease_reason_combines_with_others(client):
    prefix = _path("combine")
    src = f"{prefix}/a.txt"
    _put(client, src, b"alpha")  # rev 1: current + manifest + lease
    client.post(
        "/copy-sessions",
        json={"source_paths": [src], "target_prefix": "backup/"},
    )
    _lease(client, src, 1)
    (obj,) = _gc(client, [src]).json()["objects"]
    assert obj["retained"] == [
        {"revision_no": 1, "reasons": ["current", "copy_manifest",
                                       "revision_lease"]}
    ]


def test_release_allows_collection_and_content_410(client):
    p = _path("release-collect")
    _put(client, p, b"v1")
    _put(client, p, b"v2")
    lid = _lease(client, p, 1).json()["lease"]

    # While active: pinned.
    assert _gc(client, [p], dry_run=False).json()["deleted"] == []

    assert client.delete(f"/revision-leases/{lid}").status_code == 204
    # Read is 410 immediately after release.
    r = client.get(f"/revision-leases/{lid}/content")
    assert r.status_code == 410
    assert r.json()["state"] == "released"

    # GC collects the now-unpinned revision.
    data = _gc(client, [p], dry_run=False).json()
    assert [d["revision_no"] for d in data["deleted"]] == [1]
    # The released lease now points at a consumed revision -- still 410.
    r = client.get(f"/revision-leases/{lid}/content")
    assert r.status_code == 410
    assert r.json()["state"] == "consumed"


def test_expired_lease_does_not_pin_and_content_is_410(client):
    p = _path("expired-collect")
    _put(client, p, b"v1")
    _put(client, p, b"v2")
    lid = _lease(client, p, 1).json()["lease"]

    _expire_lease(lid)
    r = client.get(f"/revision-leases/{lid}/content")
    assert r.status_code == 410
    assert r.json()["state"] == "expired"

    (obj,) = _gc(client, [p]).json()["objects"]
    assert [c["revision_no"] for c in obj["candidates"]] == [1]
    data = _gc(client, [p], dry_run=False).json()
    assert [d["revision_no"] for d in data["deleted"]] == [1]
    assert client.get(f"/revision-leases/{lid}/content").status_code == 410


# ---- content: preconditions & ranges on the bound OLD revision -----------------


def test_content_serves_bound_old_revision(client):
    p = _path("content-old")
    _put(client, p, b"old revision content")
    _put(client, p, b"new revision content")
    lid = _lease(client, p, 1).json()["lease"]

    r = client.get(f"/revision-leases/{lid}/content")
    assert r.status_code == 200
    assert r.content == b"old revision content"
    assert r.headers["etag"] == client.head(
        f"/revision-leases/{lid}/content"
    ).headers["etag"]


def test_content_head_matches_get_headers(client):
    p = _path("content-head")
    body = b"head me please"
    _put(client, p, body)
    lid = _lease(client, p, 1).json()["lease"]
    g = client.get(f"/revision-leases/{lid}/content")
    h = client.head(f"/revision-leases/{lid}/content")
    assert h.status_code == 200
    for key in ("etag", "last-modified", "content-type", "content-length",
                "accept-ranges"):
        assert h.headers[key] == g.headers[key]
    assert h.content == b""


def test_content_conditional_requests(client):
    p = _path("content-cond")
    _put(client, p, b"old")
    _put(client, p, b"new")
    lid = _lease(client, p, 1).json()["lease"]
    etag = _lease(client, p, 1).json()["etag"]
    url = f"/revision-leases/{lid}/content"

    # If-None-Match on the BOUND revision -> 304, even though /objects
    # currently serves a different revision.
    r = client.get(url, headers={"If-None-Match": etag})
    assert r.status_code == 304
    assert r.headers["etag"] == etag
    # If-Match with the current revision's etag fails against the old one.
    cur_etag = client.head(f"/objects/{p}").headers["etag"]
    r = client.get(url, headers={"If-Match": cur_etag})
    assert r.status_code == 412
    assert r.headers["etag"] == etag
    # If-Match with the bound etag passes.
    assert client.get(url, headers={"If-Match": etag}).status_code == 200
    # If-Modified-Since at/after the revision time -> 304.
    lm = client.head(url).headers["last-modified"]
    assert client.get(url, headers={"If-Modified-Since": lm}).status_code == 304


def test_content_ranges_and_if_range(client):
    p = _path("content-range")
    body = b"0123456789abcdef"
    _put(client, p, body)
    lid = _lease(client, p, 1).json()["lease"]
    url = f"/revision-leases/{lid}/content"

    r = client.get(url, headers={"Range": "bytes=2-5"})
    assert r.status_code == 206
    assert r.content == b"2345"
    assert r.headers["content-range"] == f"bytes 2-5/{len(body)}"

    r = client.get(url, headers={"Range": "bytes=-4"})
    assert r.status_code == 206
    assert r.content == b"cdef"

    r = client.get(url, headers={"Range": "bytes=99-"})
    assert r.status_code == 416
    assert r.headers["content-range"] == f"bytes */{len(body)}"

    etag = client.head(url).headers["etag"]
    # If-Range mismatch -> full representation, ignoring the range.
    r = client.get(url, headers={"Range": "bytes=0-3", "If-Range": '"stale"'})
    assert r.status_code == 200
    assert r.content == body
    # If-Range match -> the part.
    r = client.get(url, headers={"Range": "bytes=0-3", "If-Range": etag})
    assert r.status_code == 206
    assert r.content == b"0123"

    # Multipart ranges work and are deterministic across replays.
    r1 = client.get(url, headers={"Range": "bytes=0-1,5-9"})
    r2 = client.get(url, headers={"Range": "bytes=0-1,5-9"})
    assert r1.status_code == r2.status_code == 206
    assert r1.content == r2.content
    assert "multipart/byteranges" in r1.headers["content-type"]


def test_content_replays_byte_identically(client):
    p = _path("content-replay")
    _put(client, p, b"replay me bytes")
    lid = _lease(client, p, 1).json()["lease"]
    url = f"/revision-leases/{lid}/content"
    first = client.get(url)
    second = client.get(url, headers={"Range": "bytes=0-5"})
    third = client.get(url, headers={"Range": "bytes=0-5"})
    assert first.content == b"replay me bytes"
    assert second.content == third.content == b"replay"
    assert second.headers["content-range"] == third.headers["content-range"]


def test_content_ignores_path_migration_and_republish(client):
    """After the path moves away, /objects on the source is 404 but the
    lease still serves the exact pinned revision."""
    src = _path("moves/src.txt")
    dst = _path("moves/dst.txt")
    body = b"moving content"
    _put(client, src, b"first")
    r2 = client.put(f"/objects/{src}", content=body)
    etag2 = r2.headers["etag"]
    rev2 = int(r2.headers["x-revision-no"])
    lid = _lease(client, src, rev2).json()["lease"]

    mr = client.post(
        "/moves",
        json={
            "source_path": src,
            "target_path": dst,
            "source_etag": etag2,
            "source_revision_no": rev2,
        },
    )
    assert mr.status_code == 201

    assert client.get(f"/objects/{src}").status_code == 404
    r = client.get(f"/revision-leases/{lid}/content")
    assert r.status_code == 200
    assert r.content == body
    assert r.headers["etag"] == etag2

    # Re-PUT at the old path, then re-PUT at the target: the lease stays.
    _put(client, src, b"brand new content")
    _put(client, dst, b"target v2")
    r = client.get(f"/revision-leases/{lid}/content")
    assert r.status_code == 200
    assert r.content == body
    # Byte-identical replay after the migration.
    assert client.get(f"/revision-leases/{lid}/content").content == body


# ---- release / unknown ids -----------------------------------------------------


def test_delete_is_idempotent(client):
    p = _path("delete-idem")
    _put(client, p, b"v1")
    lid = _lease(client, p, 1).json()["lease"]

    assert client.delete(f"/revision-leases/{lid}").status_code == 204
    # Repeated releases (lease already released) stay 204.
    assert client.delete(f"/revision-leases/{lid}").status_code == 204
    assert client.delete(f"/revision-leases/{lid}").status_code == 204
    # Deleting an expired (not released) lease also succeeds with 204.
    lid2 = _lease(client, p, 1).json()["lease"]
    _expire_lease(lid2)
    assert client.delete(f"/revision-leases/{lid2}").status_code == 204


def test_unknown_lease_is_404_not_410(client):
    r = client.get(f"/revision-leases/{'f' * 32}/content")
    assert r.status_code == 404
    assert r.json() == {"detail": "lease_not_found"}
    assert client.head(f"/revision-leases/{'e' * 32}/content").status_code == 404
    assert client.delete(f"/revision-leases/{'d' * 32}").status_code == 404


# ---- serialisation --------------------------------------------------------------


def test_lease_creation_and_gc_serialise(client):
    """Fire lease creation and GC execute at each other repeatedly.

    Every lease reported 201 must afterwards still serve its revision;
    GC must never commit the deletion of a revision that a committed
    lease binds (creation takes the object FOR UPDATE lock before the
    revision lookup and lease insert).
    """
    for i in range(8):
        p = _path(f"race-gc-{i}")
        _put(client, p, f"v{i}".encode())
        _put(client, p, f"newer-{i}".encode())
        barrier = threading.Barrier(2)
        outcomes = {}

        def do_lease():
            barrier.wait()
            outcomes["lease"] = _lease(client, p, 1)

        def do_gc():
            barrier.wait()
            outcomes["gc"] = _gc(client, [p], dry_run=False)

        threads = [
            threading.Thread(target=do_lease),
            threading.Thread(target=do_gc),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert outcomes["gc"].status_code == 200
        lr = outcomes["lease"]
        assert lr.status_code in (201, 404)
        if lr.status_code == 201:
            # The lease won (or GC committed first but retained under the
            # lock): the revision is intact and readable.
            lid = lr.json()["lease"]
            r = client.get(f"/revision-leases/{lid}/content")
            assert r.status_code == 200
            assert r.content == f"v{i}".encode()
            assert [d["revision_no"] for d in
                    outcomes["gc"].json()["deleted"]] == []
        else:
            # GC won and collected rev 1 before the lease lookup.
            assert [d["revision_no"] for d in
                    outcomes["gc"].json()["deleted"]] == [1]


def test_stream_survives_release_and_gc_mid_read(client):
    """Once a read starts, releasing the lease and running GC while bytes
    are still flowing must neither truncate the body nor change it."""
    p = _path("stream-release")
    body = bytes((i * 13 + 7) % 256 for i in range(3 * 1024 * 1024))
    _put(client, p, b"old")
    _put(client, p, body)
    lid = _lease(client, p, 2).json()["lease"]

    with client.stream("GET", f"/revision-leases/{lid}/content") as resp:
        assert resp.status_code == 200
        assert resp.headers["content-length"] == str(len(body))
        it = resp.iter_bytes()
        prefix = next(it)

        # Release the lease and immediately collect while the stream is
        # open: GC blocks on the share lock held by this read.
        assert client.delete(f"/revision-leases/{lid}").status_code == 204
        gc = threading.Thread(target=lambda: _gc(client, [p], dry_run=False))
        gc.start()
        time.sleep(0.3)  # let GC reach its lock wait
        rest = b"".join(it)
    gc.join(timeout=30)
    assert not gc.is_alive()

    # The whole body arrived intact and is the bound revision's content.
    assert prefix + rest == body


def test_stream_survives_expiry_mid_read(client):
    p = _path("stream-expire")
    body = bytes((i * 29 + 3) % 256 for i in range(2 * 1024 * 1024))
    _put(client, p, body)
    lid = _lease(client, p, 1).json()["lease"]

    with client.stream("GET", f"/revision-leases/{lid}/content") as resp:
        assert resp.status_code == 200
        it = resp.iter_bytes()
        prefix = next(it)
        _expire_lease(lid)  # becomes due while bytes are flowing
        rest = b"".join(it)

    assert prefix + rest == body
    # New reads after it finished see the expired lease as 410.
    assert client.get(f"/revision-leases/{lid}/content").status_code == 410


def test_range_stream_survives_release_mid_read(client):
    """A 206 partial stream also completes consistently after release."""
    p = _path("stream-range-release")
    body = bytes(range(256)) * (2 * 1024 * 16)  # 8 MiB
    _put(client, p, body)
    lid = _lease(client, p, 1).json()["lease"]

    with client.stream(
        "GET",
        f"/revision-leases/{lid}/content",
        headers={"Range": f"bytes=1000-{len(body) - 1}"},
    ) as resp:
        assert resp.status_code == 206
        assert resp.headers["content-range"] == (
            f"bytes 1000-{len(body) - 1}/{len(body)}"
        )
        it = resp.iter_bytes()
        prefix = next(it)
        assert client.delete(f"/revision-leases/{lid}").status_code == 204
        rest = b"".join(it)

    assert prefix + rest == body[1000:]
