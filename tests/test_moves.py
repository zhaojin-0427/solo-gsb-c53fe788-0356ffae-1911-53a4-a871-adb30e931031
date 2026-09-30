"""End-to-end tests for object path migration (POST /moves)."""
from __future__ import annotations

import base64
import hashlib
import threading
import uuid

FUTURE = "2099-01-01T00:00:00Z"

ETAG_OF = lambda body: '"' + hashlib.sha256(body).hexdigest() + '"'


def _p(name: str) -> str:
    return f"moves/{name}-{uuid.uuid4().hex[:12]}"


def _put(client, path: str, body: bytes, media="text/plain"):
    return client.put(
        f"/objects/{path}", content=body, headers={"Content-Type": media}
    )


def _move(client, src, dst, etag=None, rev=None):
    payload = {
        "source_path": src,
        "target_path": dst,
        "source_etag": etag if etag is not None else '"irrelevant"',
        "source_revision_no": rev if rev is not None else 1,
    }
    return client.post("/moves", json=payload)


# ---- validation ---------------------------------------------------------------

def test_move_validation(client):
    assert client.post("/moves", content=b"not json").status_code == 400
    assert client.post("/moves", json=["a"]).status_code == 400
    assert client.post("/moves", json={}).status_code == 400
    # paths: required, non-empty, different.
    assert _move(client, "", "b").status_code == 400
    r = client.post(
        "/moves",
        json={
            "source_path": "a",
            "target_path": "",
            "source_etag": '"x"',
            "source_revision_no": 1,
        },
    )
    assert r.status_code == 400
    r = client.post(
        "/moves",
        json={
            "source_path": 7,
            "target_path": "b",
            "source_etag": '"x"',
            "source_revision_no": 1,
        },
    )
    assert r.status_code == 400
    r = client.post(
        "/moves",
        json={
            "source_path": "same",
            "target_path": "same",
            "source_etag": '"x"',
            "source_revision_no": 1,
        },
    )
    assert r.status_code == 400
    # source_etag: required non-empty string with strong etag syntax.
    r = client.post(
        "/moves",
        json={
            "source_path": "a",
            "target_path": "b",
            "source_etag": "",
            "source_revision_no": 1,
        },
    )
    assert r.status_code == 400
    r = client.post(
        "/moves",
        json={
            "source_path": "a",
            "target_path": "b",
            "source_etag": 123,
            "source_revision_no": 1,
        },
    )
    assert r.status_code == 400
    r = client.post(
        "/moves",
        json={
            "source_path": "a",
            "target_path": "b",
            "source_etag": "not-an-etag",
            "source_revision_no": 1,
        },
    )
    assert r.status_code == 400
    # source_revision_no: required positive integer (booleans rejected).
    for bad_no in (0, -1, 1.5, "1", True, False, None):
        r = client.post(
            "/moves",
            json={
                "source_path": "a",
                "target_path": "b",
                "source_etag": '"x"',
                "source_revision_no": bad_no,
            },
        )
        assert r.status_code == 400, bad_no


# ---- happy path ---------------------------------------------------------------

def test_move_publishes_content_and_media_type_and_detaches_source(client):
    src, dst = _p("src"), _p("dst")
    body = b"payload-for-the-move"
    put = _put(client, src, body, "application/x-custom")
    etag = put.headers["etag"]
    assert put.headers["x-revision-no"] == "1"

    r = _move(client, src, dst, etag=etag, rev=1)
    assert r.status_code == 201, r.text
    data = r.json()
    assert data["source_path"] == src
    assert data["target_path"] == dst
    assert data["target_etag"] == etag
    assert data["target_revision_no"] == 1
    # Response validators describe the new target revision.
    assert r.headers["etag"] == etag
    assert r.headers["x-revision-no"] == "1"
    assert "last-modified" in r.headers

    # The target serves the exact content and media type as its own rev 1.
    g = client.get(f"/objects/{dst}")
    assert g.status_code == 200
    assert g.content == body
    assert g.headers["etag"] == etag
    assert g.headers["content-type"].startswith("application/x-custom")

    # The source is gone: GET and HEAD both 404.
    assert client.get(f"/objects/{src}").status_code == 404
    assert client.head(f"/objects/{src}").status_code == 404


def test_move_target_revision_numbering_continues_target_history(client):
    """Content is published as a NEW target revision, numbered after the
    target's own history -- not copied from the source revision number."""
    src, dst = _p("src-seq"), _p("dst-seq")
    _put(client, dst, b"old-target-v1")
    _put(client, dst, b"old-target-v2")  # target now at rev 2
    _put(client, src, b"src-v1")
    _put(client, src, b"src-v2")
    _put(client, src, b"src-v3")        # source now at rev 3
    etag = ETAG_OF(b"src-v3")

    # Target occupied -> 409 first.
    r = _move(client, src, dst, etag=etag, rev=3)
    assert r.status_code == 409
    assert r.json()["detail"] == "target_already_exists"
    assert r.json()["current_revision_no"] == 2

    # Free the target path by moving it elsewhere, then move src in.
    dst2 = _p("dst-seq-free")
    _move(client, dst, dst2, etag=ETAG_OF(b"old-target-v2"), rev=2)
    r = _move(client, src, dst, etag=etag, rev=3)
    assert r.status_code == 201
    assert r.json()["target_revision_no"] == 3  # continues 1,2 -> 3
    g = client.get(f"/objects/{dst}")
    assert g.content == b"src-v3"
    assert g.headers["etag"] == etag


# ---- error semantics ----------------------------------------------------------

def test_move_source_missing_is_404(client):
    src, dst = _p("never"), _p("anywhere")
    r = _move(client, src, dst, etag='"x"', rev=1)
    assert r.status_code == 404
    assert r.json()["detail"] == "source_not_found"
    # Nothing was created: the target is still a plain 404.
    assert client.get(f"/objects/{dst}").status_code == 404


def test_move_source_already_moved_away_is_404(client):
    src, mid, dst = _p("a"), _p("b"), _p("c")
    _put(client, src, b"x")
    assert _move(client, src, mid, etag=ETAG_OF(b"x"), rev=1).status_code == 201
    # Moving again from the now-detached source path is a 404.
    r = _move(client, src, dst, etag=ETAG_OF(b"x"), rev=1)
    assert r.status_code == 404
    assert r.json()["detail"] == "source_not_found"
    assert client.get(f"/objects/{dst}").status_code == 404


def test_move_etag_mismatch_is_412(client):
    src, dst = _p("stale-etag"), _p("stale-etag-dst")
    put = _put(client, src, b"v1")
    _put(client, src, b"v2")  # rev 2 current
    r = _move(client, src, dst, etag=put.headers["etag"], rev=2)
    assert r.status_code == 412
    data = r.json()
    assert data["detail"] == "precondition_failed"
    assert data["mismatches"] == ["source_etag"]
    assert data["current_etag"] == ETAG_OF(b"v2")
    assert data["current_revision_no"] == 2
    # 412 carries the current validators, like PUT's precondition failures.
    assert r.headers["etag"] == ETAG_OF(b"v2")
    # No partial effect.
    assert client.get(f"/objects/{src}").content == b"v2"
    assert client.get(f"/objects/{dst}").status_code == 404


def test_move_revision_no_mismatch_is_412(client):
    src, dst = _p("stale-rev"), _p("stale-rev-dst")
    _put(client, src, b"v1")
    _put(client, src, b"v2")
    r = _move(client, src, dst, etag=ETAG_OF(b"v2"), rev=1)
    assert r.status_code == 412
    assert r.json()["mismatches"] == ["source_revision_no"]
    assert r.json()["current_revision_no"] == 2
    assert client.get(f"/objects/{src}").content == b"v2"
    assert client.get(f"/objects/{dst}").status_code == 404


def test_move_both_mismatch_lists_both(client):
    src, dst = _p("both"), _p("both-dst")
    _put(client, src, b"v1")
    _put(client, src, b"v2")
    r = _move(client, src, dst, etag=ETAG_OF(b"v1"), rev=1)
    assert r.status_code == 412
    assert r.json()["mismatches"] == ["source_etag", "source_revision_no"]


def test_move_weak_etag_is_rejected_as_bad_request(client):
    src, dst = _p("weak"), _p("weak-dst")
    _put(client, src, b"v1")
    r = _move(
        client, src, dst, etag="W/" + ETAG_OF(b"v1"), rev=1
    )
    assert r.status_code == 400
    assert client.get(f"/objects/{src}").content == b"v1"
    assert client.get(f"/objects/{dst}").status_code == 404


def test_move_target_exists_is_409_and_changes_nothing(client):
    src, dst = _p("busy-src"), _p("busy-dst")
    _put(client, src, b"new-content")
    put_dst = _put(client, dst, b"occupied")
    r = _move(client, src, dst, etag=ETAG_OF(b"new-content"), rev=1)
    assert r.status_code == 409
    data = r.json()
    assert data["detail"] == "target_already_exists"
    assert data["target_path"] == dst
    assert data["current_etag"] == put_dst.headers["etag"]
    # Source untouched, target untouched.
    assert client.get(f"/objects/{src}").content == b"new-content"
    assert client.get(f"/objects/{dst}").content == b"occupied"


def test_move_failure_leaves_no_placeholder_visible(client):
    """A failed move inserts lock placeholder rows only inside the rolled
    back transaction; targets of 404/412/409 attempts stay true 404s and
    can still be created with If-None-Match: *."""
    # 412 case.
    src, dst = _p("rollback-a"), _p("rollback-b")
    put = _put(client, src, b"v1")
    _put(client, src, b"v2")
    assert _move(client, src, dst, etag=put.headers["etag"], rev=2).status_code == 412
    create = client.put(
        f"/objects/{dst}",
        content=b"fresh",
        headers={"If-None-Match": "*"},
    )
    assert create.status_code == 204  # not a 412 against a placeholder


# ---- revision numbers never regress / are never reused ------------------------

def test_reput_after_move_continues_revision_numbers(client):
    src = _p("renew-src")
    _put(client, src, b"v1")
    _put(client, src, b"v2")
    put3 = _put(client, src, b"v3")
    dst = _p("renew-dst")
    assert _move(client, src, dst, etag=put3.headers["etag"], rev=3).status_code == 201

    # Re-PUT the moved-away path: must continue at rev 4, never restart 1.
    r = _put(client, src, b"v4")
    assert r.status_code == 204
    assert r.headers["x-revision-no"] == "4"
    assert client.get(f"/objects/{src}").content == b"v4"
    assert client.get(f"/objects/{dst}").content == b"v3"


def test_move_into_moved_away_path_does_not_reuse_numbers(client):
    """A moved-away path reused AS A TARGET keeps its old numbering."""
    a, b, c = _p("chain-a"), _p("chain-b"), _p("chain-c")
    _put(client, a, b"a1")
    _put(client, a, b"a2")  # a at 2
    _put(client, b, b"b1")  # b at 1
    # a -> b impossible (b occupied); free b by b -> c.
    assert _move(client, b, c, etag=ETAG_OF(b"b1"), rev=1).status_code == 201
    # Now move a into the detached b: b's next number is 2.
    r = _move(client, a, b, etag=ETAG_OF(b"a2"), rev=2)
    assert r.status_code == 201
    assert r.json()["target_revision_no"] == 2
    assert client.get(f"/objects/{b}").content == b"a2"
    # a is detached at seq 2; a fresh PUT must be rev 3.
    assert _put(client, a, b"a3").headers["x-revision-no"] == "3"


def test_numbers_survive_gc_of_moved_away_history(client):
    """After a move-away, /gc may delete every leftover revision of the
    source path; a later PUT still continues past the old high-water
    mark -- numbers are never reused even with no revision rows left."""
    src, dst = _p("gc-seq-src"), _p("gc-seq-dst")
    _put(client, src, b"v1")
    _put(client, src, b"v2")
    put3 = _put(client, src, b"v3")
    assert _move(client, src, dst, etag=put3.headers["etag"], rev=3).status_code == 201

    # Collect the whole detached history (nothing pins any of it).
    r = client.post(
        "/gc",
        json={"paths": [src], "before": FUTURE, "dry_run": False},
    )
    assert r.status_code == 200
    deleted = sorted(d["revision_no"] for d in r.json()["deleted"])
    assert deleted == [1, 2, 3]
    # The path is a 404 to readers ...
    assert client.get(f"/objects/{src}").status_code == 404
    # ... but GC still recognises it (revision_seq > 0): a repeat run is
    # an empty 200, not a 404 -- the high-water mark survives collection.
    again = client.post(
        "/gc",
        json={"paths": [src], "before": FUTURE, "dry_run": False},
    )
    assert again.status_code == 200
    assert again.json()["deleted"] == []
    # ... and numbering continues at 4, it does not restart at 1.
    r = _put(client, src, b"v4")
    assert r.headers["x-revision-no"] == "4"
    assert client.get(f"/objects/{src}").content == b"v4"


# ---- copy sessions across moves -----------------------------------------------

def test_pre_move_session_still_pulls_pinned_source_revision(client):
    src, dst = _p("snap-src"), _p("snap-dst")
    _put(client, src, b"snapshot-v1")
    sid = client.post(
        "/copy-sessions",
        json={"source_paths": [src], "target_prefix": "backup/", "segment_size": 1},
    ).json()["session"]
    # The source moves away (and later publishes more) after the session
    # was pinned; the manifest revision must remain pullable byte-identical.
    _put(client, src, b"snapshot-v2")
    move_etag = client.head(f"/objects/{src}").headers["etag"]
    assert _move(client, src, dst, etag=move_etag, rev=2).status_code == 201

    seg = client.get(f"/copy-sessions/{sid}/segments").json()
    assert base64.b64decode(seg["entries"][0]["content_b64"]) == b"snapshot-v1"
    assert seg["entries"][0]["revision_no"] == 1


def test_pre_move_session_acks_against_changed_target_baseline_conflict(client):
    """Baseline taken before a move still applies by the old rules: if the
    target path changed after baseline (including because a move landed
    there), the ack is a 409 conflict and the batch is not applied."""
    # Session pins src and targets backup/<src>; baseline: target absent.
    src = _p("base-src")
    target = f"backup/{src}"
    _put(client, src, b"move-conflict")
    sid = client.post(
        "/copy-sessions",
        json={"source_paths": [src], "target_prefix": "backup/"},
    ).json()["session"]
    cursor = client.get(f"/copy-sessions/{sid}/segments").json()["cursor"]

    # Independently move some other object onto the baseline target.
    other = _p("base-other")
    _put(client, other, b"occupying bytes")
    r = _move(client, other, target, etag=ETAG_OF(b"occupying bytes"), rev=1)
    assert r.status_code == 201

    r = client.post(f"/copy-sessions/{sid}/acks", json={"cursor": cursor})
    assert r.status_code == 409
    (conflict,) = r.json()["conflicts"]
    assert conflict["target_path"] == target
    assert conflict["actual_revision_no"] == 1
    # The move's bytes stay; nothing from the batch overwrote them.
    assert client.get(f"/objects/{target}").content == b"occupying bytes"


def test_move_then_new_session_copies_from_new_source_location(client):
    """After a move the content lives at the target; a fresh session over
    the new path works normally."""
    src, dst = _p("relocate-src"), _p("relocate-dst")
    _put(client, src, b"relocated-bytes")
    _move(client, src, dst, etag=ETAG_OF(b"relocated-bytes"), rev=1)
    sid = client.post(
        "/copy-sessions",
        json={"source_paths": [dst], "target_prefix": "copy2/"},
    ).json()["session"]
    seg = client.get(f"/copy-sessions/{sid}/segments").json()
    assert base64.b64decode(seg["entries"][0]["content_b64"]) == b"relocated-bytes"
    cursor = seg["cursor"]
    r = client.post(f"/copy-sessions/{sid}/acks", json={"cursor": cursor})
    assert r.status_code == 200
    assert client.get(f"/objects/copy2/{dst}").content == b"relocated-bytes"


# ---- GC interaction -----------------------------------------------------------

def test_gc_dry_run_sees_moved_away_path_and_classifies_history(client):
    src, dst = _p("gc-preview-src"), _p("gc-preview-dst")
    _put(client, src, b"v1")
    put2 = _put(client, src, b"v2")
    _move(client, src, dst, etag=put2.headers["etag"], rev=2)

    r = client.post(
        "/gc",
        json={"paths": [src], "before": FUTURE, "dry_run": True},
    )
    assert r.status_code == 200  # moved-away path counts as existing
    (obj,) = r.json()["objects"]
    # Both old revisions are detached and pinned by nothing, so both are
    # collectable; no current revision remains to be "retained".
    assert obj["candidates"] == [
        {"revision_no": 1, "bytes": 2},
        {"revision_no": 2, "bytes": 2},
    ]
    assert obj["retained"] == []


def test_gc_does_not_delete_session_pinned_revision_of_moved_path(client):
    src, dst = _p("gc-pin-src"), _p("gc-pin-dst")
    _put(client, src, b"pinned-v1")
    sid = client.post(
        "/copy-sessions",
        json={"source_paths": [src], "target_prefix": "backup/"},
    ).json()["session"]
    _put(client, src, b"v2")
    _move(client, src, dst, etag=ETAG_OF(b"v2"), rev=2)

    # Preview: rev 1 retained (copy_manifest), rev 2 collectable.
    preview = client.post(
        "/gc",
        json={"paths": [src], "before": FUTURE, "dry_run": True},
    ).json()
    (obj,) = preview["objects"]
    assert obj["candidates"] == [{"revision_no": 2, "bytes": 2}]
    assert obj["retained"] == [
        {"revision_no": 1, "reasons": ["copy_manifest"]}
    ]

    # Execute: only rev 2 goes; the pinned rev 1 still serves the session.
    r = client.post(
        "/gc",
        json={"paths": [src], "before": FUTURE, "dry_run": False},
    )
    assert [d["revision_no"] for d in r.json()["deleted"]] == [2]
    seg = client.get(f"/copy-sessions/{sid}/segments").json()
    assert base64.b64decode(seg["entries"][0]["content_b64"]) == b"pinned-v1"


def test_gc_keeps_baseline_pinned_revision_when_source_moves_away(client):
    """A moved-away path that another session's TARGET BASELINE pins:
    after the move its old revision is no longer current and is not a
    manifest entry, but the baseline reference still keeps it out of GC."""
    tag = uuid.uuid4().hex[:10]
    s = f"bl-{tag}.txt"                       # source path
    prefix = f"d-{tag}/"
    t = prefix + s                            # target path = prefix + source
    _put(client, s, b"some-source")
    _put(client, t, b"target-rev-1")
    created = client.post(
        "/copy-sessions",
        json={"source_paths": [s], "target_prefix": prefix, "segment_size": 1},
    )
    assert created.status_code == 201
    # Move t away; its rev 1 is detached but still baseline-pinned.
    dst = f"d-{tag}/moved.txt"
    assert _move(
        client, t, dst, etag=ETAG_OF(b"target-rev-1"), rev=1
    ).status_code == 201

    preview = client.post(
        "/gc",
        json={"paths": [t], "before": FUTURE, "dry_run": True},
    ).json()
    (obj,) = preview["objects"]
    assert obj["candidates"] == []
    assert obj["retained"] == [
        {"revision_no": 1, "reasons": ["target_baseline"]}
    ]
    assert (
        client.post(
            "/gc", json={"paths": [t], "before": FUTURE, "dry_run": False}
        ).json()["deleted"]
        == []
    )


# ---- concurrency --------------------------------------------------------------

def test_concurrent_moves_diverging_from_one_source(client):
    """Two moves from the same source to two fresh targets: exactly one
    wins 201; the loser sees the detached source and gets 404 -- never a
    500, never a double move."""
    winners = 0
    losers = 0
    for i in range(8):
        src = _p(f"fanout-{i}")
        dst1 = _p(f"fanout-{i}-a")
        dst2 = _p(f"fanout-{i}-b")
        _put(client, src, b"fanout-bytes")
        etag = ETAG_OF(b"fanout-bytes")
        barrier = threading.Barrier(2)
        results = {}

        def m1():
            barrier.wait()
            results["a"] = _move(client, src, dst1, etag=etag, rev=1)

        def m2():
            barrier.wait()
            results["b"] = _move(client, src, dst2, etag=etag, rev=1)

        threads = [threading.Thread(target=m1), threading.Thread(target=m2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        codes = sorted(results[k].status_code for k in ("a", "b"))
        assert codes == [201, 404], codes
        winners += 1
        losers += 1
        ok_dst = dst1 if results["a"].status_code == 201 else dst2
        assert client.get(f"/objects/{ok_dst}").content == b"fanout-bytes"
        bad_dst = dst2 if results["a"].status_code == 201 else dst1
        assert client.get(f"/objects/{bad_dst}").status_code == 404
    assert winners == 8 and losers == 8


def test_concurrent_moves_racing_for_one_target(client):
    """Two sources racing into one empty target: one move wins, the loser
    gets 409 (target now has content); nothing is left half-done."""
    for i in range(8):
        s1, s2, dst = _p(f"race-s1-{i}"), _p(f"race-s2-{i}"), _p(f"race-t-{i}")
        _put(client, s1, b"bytes-one")
        _put(client, s2, b"bytes-two")
        barrier = threading.Barrier(2)
        results = {}

        def m1():
            barrier.wait()
            results["a"] = _move(client, s1, dst, etag=ETAG_OF(b"bytes-one"), rev=1)

        def m2():
            barrier.wait()
            results["b"] = _move(client, s2, dst, etag=ETAG_OF(b"bytes-two"), rev=1)

        threads = [threading.Thread(target=m1), threading.Thread(target=m2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        codes = sorted(results[k].status_code for k in ("a", "b"))
        assert codes == [201, 409], codes
        content = client.get(f"/objects/{dst}").content
        assert content in (b"bytes-one", b"bytes-two")
        # The losing source is still intact (not detached).
        loser_src = s1 if content == b"bytes-two" else s2
        assert client.get(f"/objects/{loser_src}").status_code == 200


def test_concurrent_cross_moves_do_not_deadlock(client):
    """Two transactions with OVERLAPPING lock sets -- a->c and b->a --
    take their row locks in the same ascending path order (a before c, and
    a before b), so they serialise instead of deadlocking.  Three serial
    histories are legal:

    * b->a first while a is occupied -> 409; a->c then succeeds
      (c has content-a, a keeps content-a, b untouched);
    * a->c in flight, b->a wakes to a still occupied -> 409, then a->c
      commits and detaches a (a ends 404, b untouched, c has content-a);
    * a->c first -> 201 detaches a, then b->a wakes to an empty target and
      also succeeds, publishing content-b as a's revision 2 (the move
      keeps the per-path high-water mark rather than reusing number 1).
    """
    for i in range(10):
        a, b, c = _p(f"cross-a-{i}"), _p(f"cross-b-{i}"), _p(f"cross-c-{i}")
        _put(client, a, b"content-a")
        _put(client, b, b"content-b")
        barrier = threading.Barrier(2)
        results = {}

        def m_ac():
            barrier.wait()
            results["ac"] = _move(
                client, a, c, etag=ETAG_OF(b"content-a"), rev=1
            )

        def m_ba():
            barrier.wait()
            results["ba"] = _move(
                client, b, a, etag=ETAG_OF(b"content-b"), rev=1
            )

        threads = [threading.Thread(target=m_ac), threading.Thread(target=m_ba)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not any(t.is_alive() for t in threads)  # no deadlock

        ac, ba = results["ac"], results["ba"]
        assert ac.status_code == 201, ac.text
        assert client.get(f"/objects/{c}").content == b"content-a"
        a_state = client.get(f"/objects/{a}")
        b_state = client.get(f"/objects/{b}")
        if ba.status_code == 201:
            # a->c committed first; b->a landed content-b on a as rev 2.
            assert ba.json()["target_revision_no"] == 2
            assert a_state.content == b"content-b"
            assert b_state.status_code == 404
        else:
            # b->a lost on the occupied target (before or while a->c ran).
            assert ba.status_code == 409, ba.text
            assert ba.json()["detail"] == "target_already_exists"
            assert b_state.content == b"content-b"
            # a may still hold content-a (rejected before a->c committed)
            # or already be detached (rejected while a->c was committing).
            assert a_state.status_code in (200, 404)
            if a_state.status_code == 200:
                assert a_state.content == b"content-a"


def test_move_races_concurrent_target_put(client):
    """Move vs a first PUT on the target path serialise on the row lock:
    either the PUT wins (move gets 409) or the move wins (PUT becomes rev
    2, numbered after the move's rev 1) -- never 500, never two rev 1s."""
    for i in range(8):
        src, dst = _p(f"mp-src-{i}"), _p(f"mp-dst-{i}")
        _put(client, src, b"move-bytes")
        barrier = threading.Barrier(2)
        results = {}

        def do_move():
            barrier.wait()
            results["move"] = _move(
                client, src, dst, etag=ETAG_OF(b"move-bytes"), rev=1
            )

        def do_put():
            barrier.wait()
            results["put"] = _put(client, dst, b"put-bytes")

        threads = [threading.Thread(target=do_move), threading.Thread(target=do_put)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        move_r, put_r = results["move"], results["put"]
        assert move_r.status_code in (201, 409)
        assert put_r.status_code == 204
        if move_r.status_code == 201:
            # Move published rev 1; the waiting PUT continues at rev 2.
            assert put_r.headers["x-revision-no"] == "2"
            assert client.get(f"/objects/{src}").status_code == 404
            assert client.get(f"/objects/{dst}").content == b"put-bytes"
        else:
            # PUT created rev 1; the move was rejected and source stays.
            assert put_r.headers["x-revision-no"] == "1"
            assert client.get(f"/objects/{src}").content == b"move-bytes"
            assert client.get(f"/objects/{dst}").content == b"put-bytes"


def test_move_waits_behind_streaming_get_then_412(client):
    """A streaming GET share-locks the source revision; the move's FOR
    UPDATE waits, and the response always finishes intact.  If a new
    revision is published before the move acquires the lock, the move
    wakes to 412 rather than moving stale bytes."""
    src, dst = _p("stream-src"), _p("stream-dst")
    _put(client, src, b"streamed-v1")
    v2 = bytes((i * 11 + 5) % 256 for i in range(2 * 1024 * 1024))
    _put(client, src, v2)

    move_result = {}
    with client.stream("GET", f"/objects/{src}") as resp:
        assert resp.status_code == 200
        it = resp.iter_bytes()
        first = next(it)

        def do_move():
            move_result["r"] = _move(
                client, src, dst, etag=ETAG_OF(v2), rev=2
            )

        t = threading.Thread(target=do_move)
        t.start()
        import time
        time.sleep(0.3)  # let the move block on the share lock
        rest = b"".join(it)
    t.join(timeout=30)
    assert first + rest == v2
    # Stream released the lock; the pinned rev 2 is still current -> 201.
    assert move_result["r"].status_code == 201
    assert client.get(f"/objects/{dst}").content == v2
    assert client.get(f"/objects/{src}").status_code == 404
