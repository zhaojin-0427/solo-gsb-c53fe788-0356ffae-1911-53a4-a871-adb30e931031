"""End-to-end tests for historical revision reclamation (POST /gc)."""
from __future__ import annotations

import base64
import threading
import time
import uuid
from datetime import timedelta
from email.utils import parsedate_to_datetime

FUTURE = "2099-01-01T00:00:00Z"
PAST = "2020-01-01T00:00:00Z"


def _path(name: str) -> str:
    return f"gc/{name}-{uuid.uuid4().hex[:12]}"


def _put(client, path: str, body: bytes) -> str:
    r = client.put(f"/objects/{path}", content=body)
    assert r.status_code == 204
    return r.headers["x-revision-no"]


def _gc(client, paths, before=FUTURE, dry_run=True):
    return client.post(
        "/gc",
        json={"paths": paths, "before": before, "dry_run": dry_run},
    )


def _create_session(client, paths, target_prefix, segment_size=1):
    return client.post(
        "/copy-sessions",
        json={
            "source_paths": paths,
            "target_prefix": target_prefix,
            "segment_size": segment_size,
        },
    )


# ---- validation ---------------------------------------------------------------

def test_gc_validation(client):
    assert client.post("/gc", json={}).status_code == 400
    assert client.post("/gc", content=b"not json").status_code == 400
    assert client.post("/gc", json=["a"]).status_code == 400
    # paths: required, 1..100 non-empty strings.
    assert _gc(client, []).status_code == 400
    assert _gc(client, [f"p{i}" for i in range(101)]).status_code == 400
    assert _gc(client, "a.txt").status_code == 400
    assert _gc(client, [""]).status_code == 400
    assert _gc(client, ["a.txt", 7]).status_code == 400
    # before: required ISO 8601 timestamp.
    assert client.post("/gc", json={"paths": ["a.txt"]}).status_code == 400
    assert _gc(client, ["a.txt"], before="not-a-date").status_code == 400
    assert _gc(client, ["a.txt"], before="2026-13-45T99:99:99Z").status_code == 400
    assert (
        client.post("/gc", json={"paths": ["a.txt"], "before": 123}).status_code
        == 400
    )
    # dry_run: must be a real boolean when present.
    assert (
        client.post(
            "/gc",
            json={"paths": ["a.txt"], "before": FUTURE, "dry_run": "yes"},
        ).status_code
        == 400
    )
    assert (
        client.post(
            "/gc",
            json={"paths": ["a.txt"], "before": FUTURE, "dry_run": 1},
        ).status_code
        == 400
    )


def test_gc_exactly_100_paths_accepted(client):
    p = _path("hundred")
    _put(client, p, b"v1")
    paths = [p] + [f"missing/{i}" for i in range(99)]
    r = _gc(client, paths)
    assert r.status_code == 404  # passes validation, fails existence
    assert len(r.json()["missing"]) == 99


# ---- missing paths ------------------------------------------------------------

def test_gc_missing_path_is_404_and_deletes_nothing(client):
    p = _path("missing")
    _put(client, p, b"v1")
    _put(client, p, b"v2")
    missing = _path("never-created")

    r = _gc(client, [p, missing])
    assert r.status_code == 404
    assert r.json() == {"detail": "object_not_found", "missing": [missing]}

    # Same for the execute path: the whole request fails and the old
    # revision of the existing object is NOT collected (all-or-nothing).
    r = _gc(client, [p, missing], dry_run=False)
    assert r.status_code == 404
    assert r.json()["missing"] == [missing]

    preview = _gc(client, [p]).json()
    assert [c["revision_no"] for c in preview["objects"][0]["candidates"]] == [1]


def test_gc_placeholder_only_path_is_404(client):
    """A path that exists only as a copy-session placeholder row (no
    current revision) is 'not found', exactly like for GET."""
    prefix = _path("placeholder")
    src = f"{prefix}/src.txt"
    _put(client, src, b"alpha")
    r = _create_session(client, [src], "backup/")
    assert r.status_code == 201
    placeholder = f"backup/{src}"  # object row exists, current_id NULL

    assert _gc(client, [placeholder]).status_code == 404
    assert _gc(client, [placeholder], dry_run=False).status_code == 404


# ---- dry run ------------------------------------------------------------------

def test_dry_run_classifies_and_changes_nothing(client):
    p = _path("preview")
    _put(client, p, b"v1-aaaa")
    _put(client, p, b"v2-bb")
    _put(client, p, b"v3-c")

    r = _gc(client, [p])
    assert r.status_code == 200
    data = r.json()
    assert data["dry_run"] is True
    assert data["before"].startswith("2099-01-01T00:00:00")
    (obj,) = data["objects"]
    assert obj["path"] == p
    assert obj["candidates"] == [
        {"revision_no": 1, "bytes": 7},
        {"revision_no": 2, "bytes": 5},
    ]
    assert obj["releasable_bytes"] == 12
    assert obj["retained"] == [{"revision_no": 3, "reasons": ["current"]}]
    assert data["total_releasable_bytes"] == 12

    # Nothing changed: current revision still served, preview repeatable.
    assert client.get(f"/objects/{p}").content == b"v3-c"
    assert _gc(client, [p]).content == r.content

    # A cutoff in the past considers nothing at all.
    data = _gc(client, [p], before=PAST).json()
    (obj,) = data["objects"]
    assert obj["candidates"] == []
    assert obj["retained"] == []
    assert data["total_releasable_bytes"] == 0


def test_dry_run_is_default_and_naive_before_is_utc(client):
    p = _path("defaults")
    _put(client, p, b"v1")
    _put(client, p, b"v2")
    # dry_run omitted -> preview; naive timestamp -> interpreted as UTC.
    r = client.post(
        "/gc", json={"paths": [p], "before": "2099-01-01T00:00:00"}
    )
    assert r.status_code == 200
    assert r.json()["dry_run"] is True
    assert _gc(client, [p], dry_run=False).json()["deleted_count"] == 1


def test_before_cutoff_is_strict(client):
    p = _path("cutoff")
    r1 = client.put(f"/objects/{p}", content=b"v1")
    lm = parsedate_to_datetime(r1.headers["last-modified"])  # == created_at
    _put(client, p, b"v2")

    # before == created_at of rev 1: NOT strictly earlier -> not considered.
    data = _gc(client, [p], before=lm.isoformat()).json()
    (obj,) = data["objects"]
    assert obj["candidates"] == []
    assert obj["retained"] == []

    # One second later: rev 1 qualifies, the current rev 2 is retained.
    data = _gc(client, [p], before=(lm + timedelta(seconds=1)).isoformat()).json()
    (obj,) = data["objects"]
    assert [c["revision_no"] for c in obj["candidates"]] == [1]
    assert obj["retained"] == [{"revision_no": 2, "reasons": ["current"]}]


def test_dedup_and_path_sorted_response(client):
    pa = _path("sorted-a")
    pb = _path("sorted-b")
    _put(client, pa, b"a1")
    _put(client, pa, b"a2")
    _put(client, pb, b"b1")

    r = _gc(client, [pb, pa, pb, pa])
    assert r.status_code == 200
    objects = r.json()["objects"]
    assert [o["path"] for o in objects] == sorted([pa, pb])
    assert objects[0]["candidates"] == [{"revision_no": 1, "bytes": 2}]
    assert objects[1]["candidates"] == []


# ---- execute ------------------------------------------------------------------

def test_execute_deletes_then_reruns_empty_and_numbering_continues(client):
    p = _path("execute")
    _put(client, p, b"v1-aaaa")
    _put(client, p, b"v2-bb")
    _put(client, p, b"v3-c")

    r = _gc(client, [p], dry_run=False)
    assert r.status_code == 200
    data = r.json()
    assert data["dry_run"] is False
    assert data["deleted"] == [
        {"path": p, "revision_no": 1, "bytes": 7},
        {"path": p, "revision_no": 2, "bytes": 5},
    ]
    assert data["deleted_count"] == 2
    assert data["deleted_bytes"] == 12

    # Current revision untouched.
    g = client.get(f"/objects/{p}")
    assert g.status_code == 200
    assert g.content == b"v3-c"

    # Repeated execution returns an empty list.
    again = _gc(client, [p], dry_run=False).json()
    assert again["deleted"] == []
    assert again["deleted_count"] == 0
    assert again["deleted_bytes"] == 0
    # And the preview confirms nothing is collectable any more.
    (obj,) = _gc(client, [p]).json()["objects"]
    assert obj["candidates"] == []
    assert obj["retained"] == [{"revision_no": 3, "reasons": ["current"]}]

    # Revision numbers keep increasing; deleted numbers are never reused.
    assert _put(client, p, b"v4") == "4"
    assert client.get(f"/objects/{p}").content == b"v4"


def test_execute_multiple_paths_sorted_deleted_list(client):
    pa = _path("multi-a")
    pb = _path("multi-b")
    _put(client, pa, b"a1")
    _put(client, pa, b"a2")
    _put(client, pb, b"b1")
    _put(client, pb, b"b2")
    _put(client, pb, b"b3")

    data = _gc(client, [pb, pa], dry_run=False).json()
    assert data["deleted"] == [
        {"path": pa, "revision_no": 1, "bytes": 2},
        {"path": pb, "revision_no": 1, "bytes": 2},
        {"path": pb, "revision_no": 2, "bytes": 2},
    ]
    assert data["deleted_bytes"] == 6
    assert client.get(f"/objects/{pa}").content == b"a2"
    assert client.get(f"/objects/{pb}").content == b"b3"


# ---- retention: copy-session manifest and target baselines ---------------------

def test_manifest_pinned_revision_is_retained(client):
    prefix = _path("pin-manifest")
    src = f"{prefix}/a.txt"
    _put(client, src, b"alpha-v1")
    sid = _create_session(client, [src], "backup/").json()["session"]
    _put(client, src, b"alpha-v2")

    (obj,) = _gc(client, [src]).json()["objects"]
    assert obj["candidates"] == []
    assert obj["retained"] == [
        {"revision_no": 1, "reasons": ["copy_manifest"]},
        {"revision_no": 2, "reasons": ["current"]},
    ]

    # Execute collects nothing; the pinned snapshot still serves rev 1.
    assert _gc(client, [src], dry_run=False).json()["deleted"] == []
    seg = client.get(f"/copy-sessions/{sid}/segments").json()
    assert base64.b64decode(seg["entries"][0]["content_b64"]) == b"alpha-v1"


def test_target_baseline_pinned_revision_is_retained(client):
    prefix = _path("pin-baseline")
    src = f"{prefix}/a.txt"
    target = f"backup/{src}"
    _put(client, src, b"alpha")
    _put(client, target, b"target-v1")
    _create_session(client, [src], "backup/")
    _put(client, target, b"target-v2")

    (obj,) = _gc(client, [target]).json()["objects"]
    assert obj["candidates"] == []
    assert obj["retained"] == [
        {"revision_no": 1, "reasons": ["target_baseline"]},
        {"revision_no": 2, "reasons": ["current"]},
    ]
    assert _gc(client, [target], dry_run=False).json()["deleted"] == []


def test_retention_reasons_combine(client):
    prefix = _path("pin-both")
    src = f"{prefix}/a.txt"
    _put(client, src, b"alpha")
    _create_session(client, [src], "backup/")  # pins rev 1, still current

    (obj,) = _gc(client, [src]).json()["objects"]
    assert obj["candidates"] == []
    assert obj["retained"] == [
        {"revision_no": 1, "reasons": ["current", "copy_manifest"]}
    ]


def test_unpinned_old_revisions_collected_pinned_kept(client):
    """Mixed object: one old revision pinned by a manifest, another old
    revision unpinned -- only the unpinned one is collected."""
    prefix = _path("pin-mixed")
    src = f"{prefix}/a.txt"
    _put(client, src, b"v1")               # rev 1: will be manifest-pinned
    sid = _create_session(client, [src], "backup/").json()["session"]
    _put(client, src, b"v2")               # rev 2: old, unpinned
    _put(client, src, b"v3")               # rev 3: current

    data = _gc(client, [src], dry_run=False).json()
    assert data["deleted"] == [{"path": src, "revision_no": 2, "bytes": 2}]

    # The pinned rev 1 still serves the session snapshot; rev 3 is live.
    seg = client.get(f"/copy-sessions/{sid}/segments").json()
    assert base64.b64decode(seg["entries"][0]["content_b64"]) == b"v1"
    assert client.get(f"/objects/{src}").content == b"v3"
    # Acknowledging the session still works after the collection.
    r = client.post(f"/copy-sessions/{sid}/acks", json={"cursor": seg["cursor"]})
    assert r.status_code == 200
    assert client.get(f"/objects/backup/{src}").content == b"v1"


# ---- concurrency ----------------------------------------------------------------

def test_streaming_get_never_loses_its_revision(client):
    """A streaming GET holds the object share lock until the last byte;
    a concurrent GC execute must wait and must not collect the revision
    being streamed."""
    p = _path("stream")
    v1 = b"old-revision-bytes"
    v2 = bytes((i * 7 + 13) % 256 for i in range(2 * 1024 * 1024))
    _put(client, p, v1)
    _put(client, p, v2)

    gc_result = {}

    def run_gc():
        gc_result["resp"] = _gc(client, [p], dry_run=False)

    with client.stream("GET", f"/objects/{p}") as resp:
        assert resp.status_code == 200
        it = resp.iter_bytes()
        first = next(it)
        t = threading.Thread(target=run_gc)
        t.start()
        time.sleep(0.3)  # let the GC reach its lock wait
        rest = b"".join(it)
    t.join(timeout=30)
    assert not t.is_alive()

    # The in-flight response stayed bound to revision 2, intact.
    assert first + rest == v2
    # GC collected only the old, unreferenced revision 1.
    data = gc_result["resp"].json()
    assert [d["revision_no"] for d in data["deleted"]] == [1]
    assert client.get(f"/objects/{p}").content == v2


def test_gc_and_put_interleave_safely(client):
    """GC execute racing a PUT: either order is fine -- no error, the
    freshly published revision is never collected, numbering continues."""
    for i in range(6):
        p = _path(f"race-put-{i}")
        _put(client, p, b"v1")
        _put(client, p, b"v2")
        barrier = threading.Barrier(2)
        outcomes = {}

        def do_put():
            barrier.wait()
            outcomes["put"] = client.put(f"/objects/{p}", content=b"v3")

        def do_gc():
            barrier.wait()
            outcomes["gc"] = _gc(client, [p], dry_run=False)

        threads = [threading.Thread(target=do_put), threading.Thread(target=do_gc)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert outcomes["put"].status_code == 204
        assert outcomes["put"].headers["x-revision-no"] == "3"
        assert outcomes["gc"].status_code == 200
        deleted_nos = [d["revision_no"] for d in outcomes["gc"].json()["deleted"]]
        assert 3 not in deleted_nos  # never collects the live revision
        assert client.get(f"/objects/{p}").content == b"v3"
        # Numbering continues strictly upwards afterwards.
        assert _put(client, p, b"v4") == "4"


def test_gc_and_ack_interleave_never_collects_pinned(client):
    """GC execute racing a segment acknowledgement: the manifest-pinned
    source revision must survive either order, and the ack must still
    apply the pinned bytes to the target."""
    for i in range(6):
        prefix = _path(f"race-ack-{i}")
        src = f"{prefix}/a.txt"
        target = f"backup/{src}"
        _put(client, src, b"pinned-bytes")
        sid = _create_session(client, [src], "backup/").json()["session"]
        cursor = client.get(f"/copy-sessions/{sid}/segments").json()["cursor"]
        _put(client, src, b"newer-bytes")  # rev 1 now old, manifest-pinned

        barrier = threading.Barrier(2)
        outcomes = {}

        def do_ack():
            barrier.wait()
            outcomes["ack"] = client.post(
                f"/copy-sessions/{sid}/acks", json={"cursor": cursor}
            )

        def do_gc():
            barrier.wait()
            outcomes["gc"] = _gc(client, [src], dry_run=False)

        threads = [threading.Thread(target=do_ack), threading.Thread(target=do_gc)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert outcomes["ack"].status_code == 200
        assert outcomes["gc"].status_code == 200
        # The pinned revision is never collected.
        assert outcomes["gc"].json()["deleted"] == []
        # The ack applied the pinned snapshot bytes, not the newer ones.
        assert client.get(f"/objects/{target}").content == b"pinned-bytes"
        # The session snapshot is still servable after the collection.
        seg = client.get(f"/copy-sessions/{sid}/segments").json()
        assert base64.b64decode(seg["entries"][0]["content_b64"]) == b"pinned-bytes"


def test_concurrent_gc_executes_converge(client):
    """Two overlapping GC executes serialise on the object locks: each
    revision is collected exactly once, and the loser's re-classification
    under the locks finds nothing left to delete."""
    p = _path("race-gc")
    for i in range(4):
        _put(client, p, f"v{i}".encode())

    barrier = threading.Barrier(2)
    outcomes = []

    def run_gc():
        barrier.wait()
        outcomes.append(_gc(client, [p], dry_run=False).json())

    threads = [threading.Thread(target=run_gc) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    deleted = sorted(
        (d["revision_no"] for data in outcomes for d in data["deleted"])
    )
    assert deleted == [1, 2, 3]  # exactly once each; rev 4 is current
    assert client.get(f"/objects/{p}").content == b"v3"
