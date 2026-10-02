"""End-to-end tests for historical revision leases.

Covers ``POST /revision-leases``, ``GET/HEAD
/revision-leases/{id}/content`` and ``DELETE /revision-leases/{id}``,
their interaction with ``POST /gc`` (the fourth retention reason), path
moves and the create-vs-GC row-lock serialisation.
"""
from __future__ import annotations

import asyncio
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone

import asyncpg

FUTURE = "2099-01-01T00:00:00Z"
LEASE_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def _path(name: str) -> str:
    return f"leases/{name}-{uuid.uuid4().hex[:12]}"


def _put(client, path: str, body: bytes, media_type: str = "application/octet-stream"):
    return client.put(
        f"/objects/{path}", content=body, headers={"Content-Type": media_type}
    )


def _revisions(client, path: str):
    """Publish three revisions; return (rev1_etag, rev2_etag, bodies)."""
    v1, v2, v3 = b"v1-the-first", b"v2-second-revision", b"v3-current-live"
    r1 = _put(client, path, v1, "text/plain")
    r2 = _put(client, path, v2, "text/plain; charset=utf-8")
    _put(client, path, v3)
    return r1.headers["etag"], r2.headers["etag"], (v1, v2, v3)


def _lease(client, path: str, revision_no: int, ttl: int = 3600):
    return client.post(
        "/revision-leases",
        json={"path": path, "revision_no": revision_no, "ttl_seconds": ttl},
    )


def _gc(client, path, before=FUTURE, dry_run=False):
    return client.post(
        "/gc",
        json={"paths": [path], "before": before, "dry_run": dry_run},
    )


async def _force_expire(lease_id: str) -> None:
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    try:
        await conn.execute(
            "UPDATE revision_leases SET expires_at = now() - interval '1 second'"
            " WHERE id = $1",
            lease_id,
        )
    finally:
        await conn.close()


def force_expire(lease_id: str) -> None:
    asyncio.run(_force_expire(lease_id))


# ---- creation -----------------------------------------------------------------


def test_create_lease_validation(client):
    p = _path("validation")
    _revisions(client, p)

    assert client.post("/revision-leases", content=b"not json").status_code == 400
    assert client.post("/revision-leases", json=[1]).status_code == 400
    assert client.post("/revision-leases", json={}).status_code == 400

    bad_paths = [None, "", 7, ["a"]]
    for v in bad_paths:
        assert _lease_with(client, {"path": v, "revision_no": 1, "ttl_seconds": 60}) == 400

    bad_nos = [0, -1, 1.5, "1", True, None]
    for v in bad_nos:
        assert _lease_with(
            client, {"path": p, "revision_no": v, "ttl_seconds": 60}
        ) == 400

    bad_ttls = [0, 59, 86401, -1, 60.0, "60", True, False, None]
    for v in bad_ttls:
        assert _lease_with(
            client, {"path": p, "revision_no": 1, "ttl_seconds": v}
        ) == 400


def _lease_with(client, body):
    return client.post("/revision-leases", json=body).status_code


def test_create_lease_on_historical_revision(client):
    p = _path("create-old")
    etag1, etag2, (v1, v2, v3) = _revisions(client, p)

    before = datetime.now(timezone.utc)
    r = _lease(client, p, 1, ttl=60)
    after = datetime.now(timezone.utc)
    assert r.status_code == 201
    data = r.json()
    assert LEASE_ID_RE.match(data["lease_id"])
    assert data["path"] == p
    assert data["revision_no"] == 1
    assert data["etag"] == etag1
    assert r.headers["etag"] == etag1
    assert data["ttl_seconds"] == 60

    created = datetime.fromisoformat(data["created_at"])
    expires = datetime.fromisoformat(data["expires_at"])
    delta = (expires - created).total_seconds()
    assert 59.9 <= delta <= 60.1
    assert before.timestamp() - 1 <= created.timestamp() <= after.timestamp() + 1
    # Last-Modified is the revision's creation time (same second as PUT).
    assert r.headers["last-modified"]

    # Boundary TTL values are accepted; current revisions are in storage too.
    assert _lease(client, p, 2, ttl=86400).status_code == 201
    assert _lease(client, p, 3, ttl=60).status_code == 201


def test_create_lease_unknown_revision_is_404(client):
    p = _path("missing-rev")
    _revisions(client, p)

    # Path never existed.
    r = _lease(client, _path("never-created"), 1)
    assert r.status_code == 404
    body = r.json()
    assert body["detail"] == "revision_not_found"
    assert body["revision_no"] == 1

    # Path exists, revision number does not (never published).
    r = _lease(client, p, 99)
    assert r.status_code == 404
    body = r.json()
    assert body["path"] == p and body["revision_no"] == 99

    # Revision existed but was collected: leasing it is still 404.
    _gc(client, p)  # deletes rev 1 and 2
    r = _lease(client, p, 1)
    assert r.status_code == 404
    assert r.json()["detail"] == "revision_not_found"


def test_lease_ids_are_random(client):
    p = _path("random-ids")
    _revisions(client, p)
    ids = {_lease(client, p, 1).json()["lease_id"] for _ in range(5)}
    assert len(ids) == 5
    assert all(LEASE_ID_RE.match(i) for i in ids)


# ---- content reads: conditional + range rules ---------------------------------


def test_leased_content_full_get_and_head(client):
    p = _path("content-full")
    etag1, _, (v1, v2, v3) = _revisions(client, p)
    lid = _lease(client, p, 1).json()["lease_id"]

    r = client.get(f"/revision-leases/{lid}/content")
    assert r.status_code == 200
    assert r.content == v1  # the OLD revision, not the current one
    assert r.headers["etag"] == etag1
    assert r.headers["content-type"] == "text/plain"
    assert r.headers["content-length"] == str(len(v1))
    assert r.headers["accept-ranges"] == "bytes"
    assert "last-modified" in r.headers

    h = client.head(f"/revision-leases/{lid}/content")
    assert h.status_code == 200
    assert h.content == b""
    assert h.headers["etag"] == etag1
    assert h.headers["content-length"] == str(len(v1))


def test_leased_content_conditionals(client):
    p = _path("content-cond")
    etag1, etag2, _ = _revisions(client, p)
    lid = _lease(client, p, 1).json()["lease_id"]
    url = f"/revision-leases/{lid}/content"

    # If-None-Match against the BOUND old revision -> 304 (not current).
    r = client.get(url, headers={"If-None-Match": etag1})
    assert r.status_code == 304
    assert r.headers["etag"] == etag1
    assert r.content == b""
    # The current revision's etag does not match the old one.
    r = client.get(url, headers={"If-None-Match": etag2})
    assert r.status_code == 200

    # If-Match: stale -> 412 with the bound revision's validators.
    r = client.get(url, headers={"If-Match": '"deadbeef"'})
    assert r.status_code == 412
    assert r.headers["etag"] == etag1

    # If-Modified-Since newer than the revision -> 304.
    r = client.get(url, headers={"If-Modified-Since": "Wed, 21 Oct 2099 07:28:00 GMT"})
    assert r.status_code == 304


def test_leased_content_ranges(client):
    p = _path("content-range")
    _, _, (v1, _, _) = _revisions(client, p)
    lid = _lease(client, p, 1).json()["lease_id"]
    url = f"/revision-leases/{lid}/content"

    r = client.get(url, headers={"Range": "bytes=0-3"})
    assert r.status_code == 206
    assert r.content == v1[0:4]
    assert r.headers["content-range"] == f"bytes 0-3/{len(v1)}"
    assert r.headers["content-length"] == "4"

    r = client.get(url, headers={"Range": "bytes=9999-"})
    assert r.status_code == 416
    assert r.headers["content-range"] == f"bytes */{len(v1)}"

    # If-Range mismatch -> full representation (200) of the bound revision.
    r = client.get(url, headers={"Range": "bytes=0-3", "If-Range": '"stale"'})
    assert r.status_code == 200
    assert r.content == v1

    # Multipart works too, and repeated requests are byte-identical.
    a = client.get(url, headers={"Range": "bytes=0-1,4-6"}).content
    b = client.get(url, headers={"Range": "bytes=0-1,4-6"}).content
    assert a == b and a.startswith(b"--byteranges_")


def test_leased_content_unknown_id_is_404(client):
    r = client.get("/revision-leases/" + ("f" * 32) + "/content")
    assert r.status_code == 404
    assert r.json()["detail"] == "lease_not_found"
    assert client.head("/revision-leases/" + ("a" * 32) + "/content").status_code == 404


# ---- binding survives path mutation and replays -------------------------------


def test_leased_content_immutable_after_republish_and_move(client):
    p = _path("rebind")
    p2 = _path("rebind-dst")
    etag1, _, (v1, v2, v3) = _revisions(client, p)
    lid = _lease(client, p, 1).json()["lease_id"]
    url = f"/revision-leases/{lid}/content"

    # Repeated and later reads return exactly the pinned snapshot...
    first = client.get(url)
    move_put = _put(client, p, b"v4-a-whole-new-revision")
    second = client.get(url)
    assert first.status_code == second.status_code == 200
    assert first.content == second.content == v1
    assert second.headers["etag"] == etag1
    # ...while /objects serves the current revision.
    assert client.get(f"/objects/{p}").content == b"v4-a-whole-new-revision"

    # Move the path away: /objects is 404, the lease still serves rev 1.
    r = client.post(
        "/moves",
        json={
            "source_path": p,
            "target_path": p2,
            "source_etag": move_put.headers["etag"],
            "source_revision_no": int(move_put.headers["x-revision-no"]),
        },
    )
    assert r.status_code == 201
    assert client.get(f"/objects/{p}").status_code == 404
    third = client.get(url)
    assert third.status_code == 200
    assert third.content == v1
    assert third.headers["etag"] == etag1


def test_lease_on_moved_away_path_revision(client):
    """A revision still stored under a detached (moved-away) path can be
    leased and read."""
    src = _path("moved-src")
    dst = _path("moved-dst")
    r1 = _put(client, src, b"leftover-history")
    r2 = client.post(
        "/moves",
        json={
            "source_path": src,
            "target_path": dst,
            "source_etag": r1.headers["etag"],
            "source_revision_no": 1,
        },
    )
    assert r2.status_code == 201

    r = _lease(client, src, 1)
    assert r.status_code == 201
    lid = r.json()["lease_id"]
    g = client.get(f"/revision-leases/{lid}/content")
    assert g.status_code == 200
    assert g.content == b"leftover-history"


# ---- GC integration ------------------------------------------------------------


def test_leased_revision_is_retained_by_gc(client):
    p = _path("gc-pin")
    _, _, (v1, v2, v3) = _revisions(client, p)
    lid = _lease(client, p, 1).json()["lease_id"]

    preview = _gc(client, p, dry_run=True).json()
    (obj,) = preview["objects"]
    assert [c["revision_no"] for c in obj["candidates"]] == [2]
    retained = {r["revision_no"]: r["reasons"] for r in obj["retained"]}
    assert retained[1] == ["revision_lease"]
    assert retained[3] == ["current"]

    # Execute collects only rev 2; the leased rev 1 survives and serves.
    executed = _gc(client, p).json()
    assert [d["revision_no"] for d in executed["deleted"]] == [2]
    # The execute response reports retention reasons too, re-validated
    # under the locks.
    retained = {(r["path"], r["revision_no"]): r["reasons"]
                for r in executed["retained"]}
    assert retained[(p, 1)] == ["revision_lease"]
    assert retained[(p, 3)] == ["current"]
    g = client.get(f"/revision-leases/{lid}/content")
    assert g.status_code == 200
    assert g.content == v1

    # Repeated GC deletes nothing while the lease lives.
    assert _gc(client, p).json()["deleted"] == []


def test_lease_reason_combines_with_current(client):
    p = _path("gc-current")
    _put(client, p, b"only-rev")
    _lease(client, p, 1)
    (obj,) = _gc(client, p, dry_run=True).json()["objects"]
    assert obj["candidates"] == []
    assert obj["retained"] == [
        {"revision_no": 1, "reasons": ["current", "revision_lease"]}
    ]


def test_released_lease_stops_retaining(client):
    p = _path("gc-release")
    _, _, _ = _revisions(client, p)
    lid = _lease(client, p, 1).json()["lease_id"]
    assert client.delete(f"/revision-leases/{lid}").status_code == 204

    (obj,) = _gc(client, p, dry_run=True).json()["objects"]
    assert [c["revision_no"] for c in obj["candidates"]] == [1, 2]
    assert _gc(client, p).status_code == 200
    # The revision is gone; reading the released lease is 410.
    assert client.get(f"/revision-leases/{lid}/content").status_code == 410


def test_expired_lease_stops_retaining(client):
    p = _path("gc-expire")
    _revisions(client, p)
    lid = _lease(client, p, 1).json()["lease_id"]
    force_expire(lid)

    (obj,) = _gc(client, p, dry_run=True).json()["objects"]
    assert [c["revision_no"] for c in obj["candidates"]] == [1, 2]
    assert _gc(client, p).status_code == 200
    assert client.get(f"/revision-leases/{lid}/content").status_code == 410


# ---- DELETE semantics ----------------------------------------------------------


def test_delete_lease_is_idempotent(client):
    p = _path("delete-idem")
    _revisions(client, p)
    lid = _lease(client, p, 1).json()["lease_id"]

    assert client.delete(f"/revision-leases/{lid}").status_code == 204
    assert client.delete(f"/revision-leases/{lid}").status_code == 204  # idempotent

    # Releasing an expired lease id is also 204.
    lid2 = _lease(client, p, 2).json()["lease_id"]
    force_expire(lid2)
    assert client.delete(f"/revision-leases/{lid2}").status_code == 204
    assert client.delete(f"/revision-leases/{lid2}").status_code == 204


def test_delete_unknown_lease_is_404(client):
    r = client.delete("/revision-leases/" + ("0" * 32))
    assert r.status_code == 404
    assert r.json()["detail"] == "lease_not_found"


def test_410_payload_distinguishes_released_and_expired(client):
    p = _path("gone-payload")
    _revisions(client, p)
    released = _lease(client, p, 1).json()["lease_id"]
    expired = _lease(client, p, 2).json()["lease_id"]
    client.delete(f"/revision-leases/{released}")
    force_expire(expired)

    r = client.get(f"/revision-leases/{released}/content")
    assert r.status_code == 410
    body = r.json()
    assert body["detail"] == "lease_released"
    assert body["lease_id"] == released
    assert body["path"] == p and body["revision_no"] == 1
    assert body["released_at"] is not None

    r = client.get(f"/revision-leases/{expired}/content")
    assert r.status_code == 410
    body = r.json()
    assert body["detail"] == "lease_expired"
    assert body["revision_no"] == 2
    assert body["released_at"] is None


# ---- streaming safety ----------------------------------------------------------


def test_stream_survives_release_and_gc_until_last_byte(client):
    """Once the response started, releasing the lease and running GC
    cannot truncate the stream or change its metadata; GC deletes only
    after the stream finishes and the share lock is released."""
    p = _path("stream-release")
    v1 = bytes((i * 13 + 7) % 256 for i in range(2 * 1024 * 1024))
    _put(client, p, v1)
    _put(client, p, b"current")
    lid = _lease(client, p, 1).json()["lease_id"]

    outcome = {}

    with client.stream("GET", f"/revision-leases/{lid}/content") as resp:
        assert resp.status_code == 200
        assert resp.headers["content-length"] == str(len(v1))
        it = resp.iter_bytes()
        first = next(it)

        def release_and_gc():
            outcome["delete"] = client.delete(f"/revision-leases/{lid}")
            outcome["gc"] = _gc(client, p)

        t = threading.Thread(target=release_and_gc)
        t.start()
        time.sleep(0.3)  # release done; GC waits on the stream's share lock
        rest = b"".join(it)
    t.join(timeout=30)
    assert not t.is_alive()

    assert first + rest == v1
    assert outcome["delete"].status_code == 204
    # GC could run only after the stream committed: rev 1 now collected.
    assert [d["revision_no"] for d in outcome["gc"].json()["deleted"]] == [1]
    # Subsequent read is 410; the lease was released.
    assert client.get(f"/revision-leases/{lid}/content").status_code == 410


def test_stream_survives_expiry_until_last_byte(client):
    p = _path("stream-expire")
    v1 = bytes((i * 29 + 3) % 256 for i in range(2 * 1024 * 1024))
    _put(client, p, v1)
    _put(client, p, b"current")
    lid = _lease(client, p, 1).json()["lease_id"]

    outcome = {}
    with client.stream("GET", f"/revision-leases/{lid}/content") as resp:
        assert resp.status_code == 200
        etag = resp.headers["etag"]
        it = resp.iter_bytes()
        first = next(it)

        def expire_and_gc():
            force_expire(lid)
            outcome["gc"] = _gc(client, p)

        t = threading.Thread(target=expire_and_gc)
        t.start()
        time.sleep(0.3)
        rest = b"".join(it)
    t.join(timeout=30)
    assert not t.is_alive()

    assert first + rest == v1
    assert [d["revision_no"] for d in outcome["gc"].json()["deleted"]] == [1]
    g = client.get(f"/revision-leases/{lid}/content")
    assert g.status_code == 410
    assert g.json()["detail"] == "lease_expired"
    assert etag  # stream headers belonged to rev 1 throughout


# ---- create vs GC serialisation ------------------------------------------------


def test_lease_creation_and_gc_are_serialised(client):
    """Race POST /revision-leases against POST /gc over many rounds:

    * lease commits first -> GC must retain the revision and the leased
      content must read back intact;
    * GC commits first -> the lease request is 404.

    A 201 lease whose revision was deleted (or a 404 with the revision
    still present) must never occur.
    """
    for i in range(8):
        p = _path(f"race-gc-{i}")
        _put(client, p, b"raced-old-revision")
        _put(client, p, b"raced-current-revision")

        barrier = threading.Barrier(2)
        results = {}

        def do_lease():
            barrier.wait()
            results["lease"] = _lease(client, p, 1)

        def do_gc():
            barrier.wait()
            results["gc"] = _gc(client, p)

        threads = [threading.Thread(target=do_lease), threading.Thread(target=do_gc)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        lease = results["lease"]
        deleted = [d["revision_no"] for d in results["gc"].json()["deleted"]]
        if lease.status_code == 201:
            # Lease won the object lock: GC saw it and retained rev 1.
            assert deleted == []
            lid = lease.json()["lease_id"]
            g = client.get(f"/revision-leases/{lid}/content")
            assert g.status_code == 200
            assert g.content == b"raced-old-revision"
            # And the next GC (lease still active) also retains it.
            assert _gc(client, p).json()["deleted"] == []
        else:
            # GC won: the historical revision is gone, leasing it a
            # clean 404 -- never a 201 on a deleted revision.
            assert lease.status_code == 404
            assert deleted == [1]
            assert lease.json()["detail"] == "revision_not_found"
        # The current revision is untouched in either ordering.
        assert client.get(f"/objects/{p}").content == b"raced-current-revision"
