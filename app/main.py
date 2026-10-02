"""FastAPI origin application.

Request flow for GET/HEAD/PUT on ``/objects/{path}``:

1. open a transaction and lock the object row (FOR SHARE for safe
   methods, FOR UPDATE for publishes);
2. evaluate RFC 9110 13.2.2 preconditions against the locked, current
   revision (304 / 412 / 404 short-circuit here);
3. only then parse Range / If-Range and decide 200 / 206 / 416;
4. stream the response body from the very revision selected in step 1,
   keeping the transaction (and its locks) open until the last byte.

Resumable copy sessions (``/copy-sessions``) add snapshot replication on
top: a session pins a manifest of source revisions in one database
snapshot, serves it in cursor-addressed segments that are byte-identical
on every pull, and applies each acknowledged segment to the target
prefix as a single atomic batch.

Historical revision reclamation (``POST /gc``) deletes revisions that
nothing can reach any more -- never the current revision, never a
revision pinned by a copy-session manifest or target baseline -- either
as a read-only preview (``dry_run``) or as one locked, re-validated,
all-or-nothing transaction.

Object path moves (``POST /moves``) atomically migrate a path: when the
source's strong ETag AND revision number match its current revision and
the target has no content, one transaction locks both endpoints in
ascending path order, publishes the source content and media type as a
new target revision and detaches the source (its GET/HEAD become 404).
A per-path revision high-water mark keeps numbers monotonic when a
moved-away path is later re-PUT or reused as a target, even after /gc
has removed every leftover revision.

Revision leases (``POST /revision-leases``) pin one still-stored revision
for 60..86400 seconds.  Creation locks the object row FOR UPDATE before
reading the revision and inserting the lease, so it is impossible to
report a lease created for a revision that GC removes in the same
instant; GC preview and execute report ``revision_lease`` as a retention
reason and never delete a pinned revision.  GET/HEAD on
``/revision-leases/{id}/content`` serve the bound revision (by immutable
revision id) through the same conditional/Range machinery, so path
moves and re-PUTs never change the response, and the share lock held for
the whole stream keeps bytes and metadata intact even if the lease
expires or is released mid-read.  DELETE releases idempotently; unknown
ids are 404 and reads of expired/released/consumed leases are 410.
"""
from __future__ import annotations

import base64
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from . import db as database
from .config import (
    COPY_MAX_SEGMENT_SIZE,
    COPY_SEGMENT_SIZE,
    DATABASE_URL,
    GC_MAX_PATHS,
    LEASE_MAX_TTL,
    LEASE_MIN_TTL,
)
from .copy_sessions import (
    CopyCursor,
    decode_cursor,
    encode_cursor,
    manifest_hash,
    segment_digest,
    source_set_hash,
)
from .dates import http_date, parse_http_date
from .etags import make_etag, parse_etag
from .preconditions import (
    Outcome,
    RequestConditions,
    SelectedRepresentation,
    evaluate,
)
from .ranges import (
    ByteRange,
    MultipartPart,
    closing_delimiter,
    if_range_matches,
    multipart_boundary,
    multipart_content_length,
    multipart_content_type,
    parse_byte_ranges,
)

DEFAULT_MEDIA_TYPE = "application/octet-stream"

# HTTP status replayed alongside a stored copy-session batch result.
ACK_STATUS_CODES = {"applied": 200, "conflict": 409}


@dataclass(slots=True)
class Plan:
    status: int
    headers: dict[str, str]
    kind: str = "empty"            # empty | full | single | multipart
    ranges: list[ByteRange] | None = None
    boundary: str | None = None


def validator_headers(rev) -> dict[str, str]:
    return {
        "ETag": str(make_etag(rev.sha256)),
        "Last-Modified": http_date(rev.created_at),
    }


def _stream_revision_response(pool, conn, tx, rev, plan: Plan) -> StreamingResponse:
    """Stream ``plan`` for the already-selected, locked revision ``rev``.

    The transaction (and its object-row lock) is committed only after the
    last byte has been yielded and rolled back on any error; the pooled
    connection is released in ``finally``.  Shared by the ordinary
    GET /objects path and GET /revision-leases/{id}/content so an old
    revision bound to a lease streams under exactly the same guarantees.
    """
    if plan.kind == "full":
        async def body() -> AsyncIterator[bytes]:
            async for chunk in database.stream_revision(
                conn, rev.id, 0, rev.length - 1
            ):
                yield chunk
    elif plan.kind == "single":
        rng = plan.ranges[0]

        async def body() -> AsyncIterator[bytes]:
            async for chunk in database.stream_revision(
                conn, rev.id, rng.first, rng.last
            ):
                yield chunk
    else:
        ranges = plan.ranges
        boundary = plan.boundary

        async def body() -> AsyncIterator[bytes]:
            for rng in ranges:
                part = MultipartPart(
                    rng.first, rng.last, rev.length, rev.media_type
                )
                yield part.header_block(boundary)
                async for chunk in database.stream_revision(
                    conn, rev.id, rng.first, rng.last
                ):
                    yield chunk
                yield b"\r\n"
            yield closing_delimiter(boundary)

    async def wrapped() -> AsyncIterator[bytes]:
        try:
            async for chunk in body():
                yield chunk
            await tx.commit()
        except Exception:
            await tx.rollback()
            raise
        finally:
            await pool.release(conn)

    return StreamingResponse(
        wrapped(), status_code=plan.status, headers=plan.headers
    )


def build_plan(rev, request: Request) -> Plan:
    """Translate request + locked revision into a response plan.

    Precondition evaluation happens before this is called; Range /
    If-Range handling lives here (preconditions first, ranges second).
    """
    etag = make_etag(rev.sha256)
    headers = validator_headers(rev)

    def entity_headers() -> dict[str, str]:
        h = dict(headers)
        h["Accept-Ranges"] = "bytes"
        return h

    def full_plan() -> Plan:
        h = entity_headers()
        h["Content-Type"] = rev.media_type
        h["Content-Length"] = str(rev.length)
        return Plan(status=200, headers=h, kind="full")

    range_raw = request.headers.get("range")
    if not range_raw:
        return full_plan()

    ranges = parse_byte_ranges(range_raw, rev.length)
    if ranges is None:
        # Malformed Range: ignore it and send the whole representation.
        return full_plan()

    # If-Range: on mismatch, fall back to the full representation.
    if_range_raw = request.headers.get("if-range")
    if if_range_raw is not None:
        if not if_range_matches(if_range_raw, etag, rev.created_at):
            return full_plan()

    if not ranges:
        # Every requested range is unsatisfiable.
        h = entity_headers()
        h["Content-Range"] = f"bytes */{rev.length}"
        return Plan(status=416, headers=h)

    h = entity_headers()
    if len(ranges) == 1:
        rng = ranges[0]
        h["Content-Type"] = rev.media_type
        h["Content-Range"] = rng.content_range(rev.length)
        h["Content-Length"] = str(rng.length)
        return Plan(status=206, headers=h, kind="single", ranges=ranges)

    boundary = multipart_boundary(etag, ranges, rev.media_type)
    total = multipart_content_length(ranges, rev.length, rev.media_type, boundary)
    h["Content-Type"] = multipart_content_type(boundary)
    h["Content-Length"] = str(total)
    return Plan(
        status=206, headers=h, kind="multipart",
        ranges=ranges, boundary=boundary,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Read the DSN at startup so tests / containers can inject it via env.
    dsn = os.environ.get("DATABASE_URL", DATABASE_URL)
    app.state.pool = await database.create_pool(dsn)
    app.state.copy_secret = await database.ensure_copy_secret(app.state.pool)
    try:
        yield
    finally:
        await app.state.pool.close()


app = FastAPI(title="Conditional Request Object Origin", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


def _conditions_from_request(method: str, request: Request) -> RequestConditions:
    return RequestConditions(
        method=method,
        if_match_raw=request.headers.get("if-match"),
        if_none_match_raw=request.headers.get("if-none-match"),
        if_unmodified_since=parse_http_date(request.headers["if-unmodified-since"])
        if request.headers.get("if-unmodified-since")
        else None,
        if_modified_since=parse_http_date(request.headers["if-modified-since"])
        if request.headers.get("if-modified-since")
        else None,
    )


def _selected(rev) -> SelectedRepresentation:
    if rev is None:
        return SelectedRepresentation(exists=False)
    return SelectedRepresentation(
        exists=True,
        etag=make_etag(rev.sha256),
        last_modified=rev.created_at,
    )


@app.api_route("/objects/{path:path}", methods=["GET", "HEAD"])
async def read_object(path: str, request: Request):
    pool = request.app.state.pool
    conn = await pool.acquire()
    tx = conn.transaction()
    await tx.start()
    streaming = False
    try:
        rev = await database.current_revision(conn, path, for_update=False)
        conditions = _conditions_from_request(request.method, request)

        # Preconditions first (RFC 9110 13.2.2), ranges afterwards.
        outcome = evaluate(conditions, _selected(rev))
        if outcome is Outcome.PRECONDITION_FAILED:
            resp_headers = validator_headers(rev) if rev else {}
            await tx.rollback()
            return JSONResponse(
                {"detail": "Precondition Failed"},
                status_code=412, headers=resp_headers,
            )
        if rev is None:
            await tx.rollback()
            return JSONResponse({"detail": "Not Found"}, status_code=404)
        if outcome is Outcome.NOT_MODIFIED:
            headers = validator_headers(rev)
            await tx.commit()
            return Response(status_code=304, headers=headers)

        plan = build_plan(rev, request)
        if plan.status == 416:
            await tx.rollback()
            return Response(status_code=416, headers=plan.headers)

        if request.method == "HEAD":
            # Headers already describe the entity/part; no body is sent.
            await tx.commit()
            return Response(status_code=plan.status, headers=plan.headers)

        streaming = True
        return _stream_revision_response(pool, conn, tx, rev, plan)
    finally:
        if not streaming:
            await pool.release(conn)


@app.put("/objects/{path:path}")
async def publish_object(path: str, request: Request):
    body = await request.body()
    media_type = request.headers.get("content-type", DEFAULT_MEDIA_TYPE)

    pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Lock the object row first (empty placeholder rows included),
            # then evaluate conditions against the current revision: this
            # serialises concurrent publishers AND copy-session applies on
            # the same row without ever inserting a revision before locking.
            rev = await database.lock_new_or_existing_object(conn, path)
            conditions = _conditions_from_request("PUT", request)
            outcome = evaluate(conditions, _selected(rev))
            if outcome is not Outcome.PASS:
                # PUT + If-None-Match match, or If-Match/I-U-S failure.
                resp_headers = validator_headers(rev) if rev else {}
                return JSONResponse(
                    {"detail": "Precondition Failed"},
                    status_code=412, headers=resp_headers,
                )

            digest, revision_no, created_at = await database.publish_revision(
                conn, path, body, media_type
            )

    headers = {
        "ETag": str(make_etag(digest)),
        "Last-Modified": http_date(created_at),
        "X-Revision-No": str(revision_no),
    }
    return Response(status_code=204, headers=headers)


# ---- object path moves --------------------------------------------------------


def _move_bad_request(detail: str) -> JSONResponse:
    return JSONResponse({"detail": detail}, status_code=400)


@app.post("/moves")
async def move_object(request: Request):
    """Atomically migrate a path: ``POST /moves``.

    The request pins the source by *both* its strong ETag and its
    revision number.  When the source exists, both validators match its
    current revision and the target currently has no content, one
    transaction locks both endpoints in ascending path order, publishes
    the source content and media type as a new target revision, and
    detaches the source (GET/HEAD on it becomes 404).  Failures:

    * 404 -- the source path has no current revision (missing or already
      moved away);
    * 412 -- the source changed: its strong ETag or revision number no
      longer matches;
    * 409 -- the target already has content.

    On any failure the transaction rolls back, so no partial move is ever
    visible.  Revision numbers come from the per-path high-water mark, so
    re-PUTting a moved-away path (or reusing it as a move target) can
    neither regress nor reuse old numbers.
    """
    try:
        body = await request.json()
    except Exception:
        return _move_bad_request("request body must be valid JSON")
    if not isinstance(body, dict):
        return _move_bad_request("request body must be a JSON object")

    source_path = body.get("source_path")
    target_path = body.get("target_path")
    if not isinstance(source_path, str) or not source_path:
        return _move_bad_request("source_path must be a non-empty string")
    if not isinstance(target_path, str) or not target_path:
        return _move_bad_request("target_path must be a non-empty string")
    if source_path == target_path:
        return _move_bad_request("source_path and target_path must differ")

    source_etag_raw = body.get("source_etag")
    if not isinstance(source_etag_raw, str) or not source_etag_raw:
        return _move_bad_request("source_etag must be a non-empty string")
    source_etag = parse_etag(source_etag_raw)
    if source_etag is None or source_etag.weak:
        # A weak validator can never prove the exact current revision:
        # the move needs the strong identity returned by GET/PUT.
        return _move_bad_request("source_etag must be a strong entity-tag")

    source_revision_no = body.get("source_revision_no")
    if (
        not isinstance(source_revision_no, int)
        or isinstance(source_revision_no, bool)
        or source_revision_no < 1
    ):
        return _move_bad_request(
            "source_revision_no must be a positive integer"
        )

    pool = request.app.state.pool
    conn = await pool.acquire()
    tx = conn.transaction()
    await tx.start()
    try:
        try:
            endpoints = await database.lock_move_endpoints(
                conn, source_path, target_path
            )
            source = endpoints[source_path]
            target = endpoints[target_path]

            # 404 first: a detached source (moved away) is absent here.
            if source.current_id is None:
                await tx.rollback()
                return JSONResponse(
                    {"detail": "source_not_found", "source_path": source_path},
                    status_code=404,
                )

            mismatches = []
            if source.sha256 != source_etag.opaque:
                mismatches.append("source_etag")
            if source.revision_no != source_revision_no:
                mismatches.append("source_revision_no")
            if mismatches:
                await tx.rollback()
                return JSONResponse(
                    {
                        "detail": "precondition_failed",
                        "source_path": source_path,
                        "mismatches": mismatches,
                        "current_etag": str(make_etag(source.sha256)),
                        "current_revision_no": source.revision_no,
                    },
                    status_code=412,
                    headers=validator_headers(source),
                )

            # 409 last: the target must still be empty under the lock, so
            # a concurrent first publish cannot slip past an unlocked row.
            if target.current_id is not None:
                await tx.rollback()
                return JSONResponse(
                    {
                        "detail": "target_already_exists",
                        "target_path": target_path,
                        "current_etag": str(make_etag(target.sha256)),
                        "current_revision_no": target.revision_no,
                    },
                    status_code=409,
                )

            # Re-publish the source's exact bytes and media type as a NEW
            # target revision (fresh revision id and per-target number),
            # then detach the source -- one transaction, no partial state.
            content = await database.fetch_revision_content(
                conn, source.current_id
            )
            digest, target_revision_no, created_at = (
                await database.publish_revision(
                    conn, target_path, content, source.media_type
                )
            )
            await database.detach_object(conn, source_path)
            await tx.commit()
        except Exception:
            await tx.rollback()
            raise
    finally:
        await pool.release(conn)

    return JSONResponse(
        {
            "source_path": source_path,
            "target_path": target_path,
            "target_etag": str(make_etag(digest)),
            "target_revision_no": target_revision_no,
            "source_etag": str(make_etag(digest)),
            "source_revision_no": source_revision_no,
        },
        status_code=201,
        headers={
            "ETag": str(make_etag(digest)),
            "Last-Modified": http_date(created_at),
            "X-Revision-No": str(target_revision_no),
        },
    )


# ---- resumable copy sessions -------------------------------------------------


def _mint_cursor(secret: str, session, segment: int, last_path: str) -> str:
    """Cursor identifying ``segment`` of ``session`` (deterministic)."""
    return encode_cursor(
        secret,
        CopyCursor(
            session_id=session.id,
            segment=segment,
            manifest_sha256=session.manifest_sha256,
            last_path=last_path,
            target_prefix=session.target_prefix,
            source_hash=session.source_hash,
        ),
    )


async def _resume_payload(conn, secret: str, session) -> dict:
    """Recovery position: first un-acked segment + last confirmed cursor.

    ``last_confirmed_cursor`` is the token to hand to ``POST
    /copy-sessions`` when a session must be abandoned (conflict,
    failure): the new session resumes right after that segment.
    """
    next_segment = session.next_segment
    last_confirmed = None
    if next_segment > 0:
        entries = await database.fetch_segment_entries(
            conn, session.id, next_segment - 1
        )
        if entries:
            last_confirmed = _mint_cursor(
                secret, session, next_segment - 1, entries[-1].source_path
            )
    return {"next_segment": next_segment, "last_confirmed_cursor": last_confirmed}


async def _cursor_expired(conn, secret: str, session, detail: str) -> JSONResponse:
    return JSONResponse(
        {
            "detail": detail,
            "status": "cursor_expired",
            "session": session.id,
            "conflicts": [],
            "resume": await _resume_payload(conn, secret, session),
        },
        status_code=409,
    )


def _bad_request(detail: str) -> JSONResponse:
    return JSONResponse({"detail": detail}, status_code=400)


def _valid_cursor_for(secret: str, token: str, session) -> CopyCursor | None:
    """Decode a token and bind it to this session's immutable manifest."""
    cur = decode_cursor(secret, token)
    if (
        cur is None
        or cur.session_id != session.id
        or cur.manifest_sha256 != session.manifest_sha256
        or cur.segment >= session.total_segments
    ):
        return None
    return cur


@app.post("/copy-sessions", status_code=201)
async def create_copy_session(request: Request):
    try:
        body = await request.json()
    except Exception:
        return _bad_request("request body must be valid JSON")
    if not isinstance(body, dict):
        return _bad_request("request body must be a JSON object")

    source_paths = body.get("source_paths")
    if (
        not isinstance(source_paths, list)
        or not source_paths
        or not all(isinstance(p, str) and p for p in source_paths)
    ):
        return _bad_request("source_paths must be a non-empty list of paths")

    target_prefix = body.get("target_prefix", "")
    if not isinstance(target_prefix, str):
        return _bad_request("target_prefix must be a string")

    segment_size = body.get("segment_size")
    if segment_size is None:
        segment_size = COPY_SEGMENT_SIZE
    if (
        not isinstance(segment_size, int)
        or isinstance(segment_size, bool)
        or not 1 <= segment_size <= COPY_MAX_SEGMENT_SIZE
    ):
        return _bad_request(
            f"segment_size must be an integer in [1, {COPY_MAX_SEGMENT_SIZE}]"
        )

    resume_token = body.get("cursor")
    if resume_token is not None and not isinstance(resume_token, str):
        return _bad_request("cursor must be a string")

    # De-duplicate; the snapshot query returns stable path order anyway.
    paths = sorted(set(source_paths))
    source_hash = source_set_hash(paths)
    secret = request.app.state.copy_secret

    # Optional resume: continue right after the last confirmed segment of
    # a previous session over the same source set and target prefix.
    resume_after = None
    if resume_token is not None:
        cur = decode_cursor(secret, resume_token)
        if (
            cur is None
            or cur.target_prefix != target_prefix
            or cur.source_hash != source_hash
        ):
            return JSONResponse(
                {
                    "detail": "cursor_expired",
                    "status": "cursor_expired",
                    "conflicts": [],
                    "resume": None,
                },
                status_code=409,
            )
        resume_after = cur.last_path

    pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Manifest shape depends only on request parameters, not the
            # database, so build it before taking locks.
            provisional = [
                (p, target_prefix + p)
                for p in paths
                if resume_after is None or p > resume_after
            ]
            target_paths = [t for _, t in provisional]

            # Freeze every involved path in one globally sorted lock set:
            # placeholders are created for targets without a revision, and
            # the same ascending order acknowledgements use for FOR UPDATE
            # guarantees creation cannot deadlock with a concurrent apply.
            await database.lock_path_set(
                conn, sorted(set(paths + target_paths))
            )

            # Both snapshots are now read inside the held locks, so the
            # pinned source revisions and the target baselines come from
            # one frozen boundary (a concurrent publish blocks until
            # commit and cannot sneak between the two reads).
            rows = await database.snapshot_source_revisions(conn, paths)
            found = {r["source_path"] for r in rows}
            missing = [p for p in paths if p not in found]
            if missing:
                return JSONResponse(
                    {"detail": "source_not_found", "missing": missing},
                    status_code=404,
                )

            rev_by_path = {r["source_path"]: r for r in rows}
            entries: list[database.CopyEntry] = []
            for source_path, target_path in provisional:
                row = rev_by_path[source_path]
                entries.append(
                    database.CopyEntry(
                        ordinal=len(entries),
                        segment_no=len(entries) // segment_size,
                        source_path=source_path,
                        target_path=target_path,
                        revision_id=row["revision_id"],
                        revision_no=row["revision_no"],
                        sha256=row["sha256"],
                        media_type=row["media_type"],
                        length=row["length"],
                    )
                )

            baseline_rows = await database.snapshot_target_baselines(
                conn, target_paths
            )
            baselines = [
                database.TargetBaseline(
                    ordinal=e.ordinal,
                    target_path=e.target_path,
                    revision_id=baseline_rows[e.target_path]["revision_id"],
                    revision_no=baseline_rows[e.target_path]["revision_no"],
                    sha256=baseline_rows[e.target_path]["sha256"],
                )
                for e in entries
            ]

            session_id = uuid.uuid4().hex
            mhash = manifest_hash(entries)
            await database.insert_copy_session(
                conn,
                session_id,
                target_prefix,
                source_hash,
                mhash,
                segment_size,
                entries,
                baselines,
            )

    total_segments = (
        (len(entries) + segment_size - 1) // segment_size if entries else 0
    )
    return JSONResponse(
        {
            "session": session_id,
            "target_prefix": target_prefix,
            "manifest_sha256": mhash,
            "source_hash": source_hash,
            "segment_size": segment_size,
            "total_entries": len(entries),
            "total_segments": total_segments,
            "resumed_after": resume_after,
            "manifest": [
                {
                    "source_path": e.source_path,
                    "target_path": e.target_path,
                    "revision_no": e.revision_no,
                    "sha256": e.sha256,
                    "media_type": e.media_type,
                    "length": e.length,
                }
                for e in entries
            ],
        },
        status_code=201,
    )


@app.get("/copy-sessions/{session_id}")
async def describe_copy_session(session_id: str, request: Request):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        session = await database.get_copy_session(conn, session_id)
        if session is None:
            return JSONResponse({"detail": "session not found"}, status_code=404)
        resume = await _resume_payload(conn, request.app.state.copy_secret, session)
    return {
        "session": session.id,
        "target_prefix": session.target_prefix,
        "manifest_sha256": session.manifest_sha256,
        "source_hash": session.source_hash,
        "segment_size": session.segment_size,
        "total_entries": session.total_entries,
        "total_segments": session.total_segments,
        "next_segment": session.next_segment,
        "status": (
            "complete"
            if session.next_segment >= session.total_segments
            else "open"
        ),
        "resume": resume,
        "created_at": session.created_at.isoformat(),
    }


@app.get("/copy-sessions/{session_id}/segments")
async def pull_copy_segment(
    session_id: str, request: Request, cursor: str | None = None
):
    """Fetch the segment *after* ``cursor`` (the first when omitted).

    Segments are served from the persisted manifest, so repeated pulls
    return identical bytes, digests and cursors even if the source
    objects have published newer revisions since the session was created.
    """
    pool = request.app.state.pool
    secret = request.app.state.copy_secret
    async with pool.acquire() as conn:
        session = await database.get_copy_session(conn, session_id)
        if session is None:
            return JSONResponse({"detail": "session not found"}, status_code=404)

        if cursor is None:
            segment = 0
        else:
            cur = _valid_cursor_for(secret, cursor, session)
            if cur is None:
                return await _cursor_expired(conn, secret, session, "cursor_expired")
            segment = cur.segment + 1

        if segment >= session.total_segments:
            return JSONResponse(
                {
                    "detail": "no_more_segments",
                    "session": session.id,
                    "total_segments": session.total_segments,
                },
                status_code=404,
            )

        entries = await database.fetch_segment_entries(conn, session.id, segment)
        out_entries = []
        for entry in entries:
            content = await database.fetch_revision_content(conn, entry.revision_id)
            out_entries.append(
                {
                    "source_path": entry.source_path,
                    "target_path": entry.target_path,
                    "revision_no": entry.revision_no,
                    "sha256": entry.sha256,
                    "media_type": entry.media_type,
                    "length": entry.length,
                    "content_b64": base64.b64encode(content).decode("ascii"),
                }
            )
        return {
            "session": session.id,
            "segment": segment,
            "total_segments": session.total_segments,
            "cursor": _mint_cursor(
                secret, session, segment, entries[-1].source_path
            ),
            "has_more": segment + 1 < session.total_segments,
            "segment_sha256": segment_digest(entries),
            "entries": out_entries,
        }


@app.post("/copy-sessions/{session_id}/acks")
async def ack_copy_segment(session_id: str, request: Request):
    """Acknowledge one segment: apply it to the target prefix atomically.

    Targets are checked against the revision baselines captured at
    session creation, so any publish after the baseline -- even of the
    identical bytes -- is a 409 conflict.  On conflict or failure
    nothing is applied and a structured result (conflicting objects +
    resume position) is returned.  The result is persisted, so repeated
    or replayed acknowledgements return the very same batch result.
    """
    try:
        body = await request.json()
    except Exception:
        return _bad_request("request body must be valid JSON")
    token = body.get("cursor") if isinstance(body, dict) else None
    if not isinstance(token, str) or not token:
        return _bad_request("cursor is required")

    pool = request.app.state.pool
    secret = request.app.state.copy_secret
    async with pool.acquire() as conn:
        session = await database.get_copy_session(conn, session_id)
        if session is None:
            return JSONResponse({"detail": "session not found"}, status_code=404)

        cur = _valid_cursor_for(secret, token, session)
        if cur is None:
            return await _cursor_expired(conn, secret, session, "cursor_expired")

        # Replay: an acknowledged segment always yields its stored result.
        stored = await database.fetch_ack(conn, session.id, cur.segment)
        if stored is not None:
            status, result = stored
            return JSONResponse(result, status_code=ACK_STATUS_CODES[status])

        # Segments are acknowledged strictly in order; anything else is
        # an expired cursor from the client's point of view.
        if cur.segment != session.next_segment:
            return await _cursor_expired(conn, secret, session, "cursor_expired")

        # Sessions created before baselines existed have no revision
        # identity for their targets: a digest-only check cannot tell
        # "unchanged" from "re-published identical bytes", so unfinished
        # segments must be re-pinned as a new session instead of being
        # let through on matching hashes.  (Already-persisted acks were
        # replayed above and keep their original result.)
        if not await database.session_has_baselines(conn, session.id):
            return JSONResponse(
                {
                    "detail": "baseline_missing",
                    "status": "baseline_missing",
                    "session": session.id,
                    "segment": cur.segment,
                    "cursor": token,
                    "applied": [],
                    "conflicts": [],
                    "resume": await _resume_payload(conn, secret, session),
                },
                status_code=409,
            )

        entries = await database.fetch_segment_entries(conn, session.id, cur.segment)
        baselines = await database.fetch_segment_baselines(
            conn, session.id, cur.segment
        )
        resume_before = await _resume_payload(conn, secret, session)

        tx = conn.transaction()
        await tx.start()
        try:
            # Lock EVERY target object row in ascending path order first --
            # including the current_id-NULL placeholders left for targets
            # that did not exist at session creation -- and only then read
            # the current revisions.  An inner join on revisions would skip
            # an empty placeholder and lock nothing, letting a concurrent
            # first PUT race the apply (both inserting revision 1, or a 500
            # instead of a conflict).  Whichever transaction takes the lock
            # first wins; the loser wakes up and sees the committed state.
            current = await database.lock_target_objects(
                conn, sorted({e.target_path for e in entries})
            )
            applied, conflicts = await database.apply_copy_entries(
                conn, entries, baselines, current
            )
        except Exception:
            await tx.rollback()
            return JSONResponse(
                {
                    "detail": "batch apply failed; no changes were committed",
                    "status": "failed",
                    "session": session.id,
                    "segment": cur.segment,
                    "cursor": token,
                    "applied": [],
                    "conflicts": [],
                    "resume": resume_before,
                },
                status_code=500,
            )

        if conflicts:
            # Roll back the whole batch, then persist the conflict result
            # on its own so replays return it verbatim.
            await tx.rollback()
            result = {
                "status": "conflict",
                "session": session.id,
                "segment": cur.segment,
                "cursor": token,
                "applied": [],
                "conflicts": conflicts,
                "resume": resume_before,
            }
            async with conn.transaction():
                inserted = await database.insert_ack(
                    conn, session.id, cur.segment, token, "conflict", result
                )
            if not inserted:
                stored = await database.fetch_ack(conn, session.id, cur.segment)
                if stored is not None:
                    status, result = stored
                    return JSONResponse(
                        result, status_code=ACK_STATUS_CODES[status]
                    )
            return JSONResponse(result, status_code=409)

        result = {
            "status": "applied",
            "session": session.id,
            "segment": cur.segment,
            "cursor": token,
            "applied": applied,
            "conflicts": [],
            "resume": {
                "next_segment": cur.segment + 1,
                "last_confirmed_cursor": token,
            },
        }
        # The batch, its ack record and the resume position commit together.
        inserted = await database.insert_ack(
            conn, session.id, cur.segment, token, "applied", result
        )
        if not inserted:
            # A concurrent ack of the same segment won; replay its result.
            await tx.rollback()
            stored = await database.fetch_ack(conn, session.id, cur.segment)
            if stored is not None:
                status, result = stored
                return JSONResponse(result, status_code=ACK_STATUS_CODES[status])
            return await _cursor_expired(conn, secret, session, "cursor_expired")
        await database.advance_session(conn, session.id, cur.segment + 1)
        await tx.commit()
        return JSONResponse(result, status_code=200)


# ---- historical revision reclamation ------------------------------------------


def _parse_iso8601(raw: str) -> datetime | None:
    """Parse an ISO 8601 timestamp; a naive value is read as UTC."""
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _gc_not_found(missing: list[str]) -> JSONResponse:
    return JSONResponse(
        {"detail": "object_not_found", "missing": missing},
        status_code=404,
    )


def _gc_retained_entry(rev) -> dict:
    reasons = []
    if rev.is_current:
        reasons.append("current")
    if rev.in_manifest:
        reasons.append("copy_manifest")
    if rev.in_baseline:
        reasons.append("target_baseline")
    if rev.in_lease:
        reasons.append("revision_lease")
    return {"revision_no": rev.revision_no, "reasons": reasons}


def _gc_preview(paths: list[str], before: datetime, classified) -> dict:
    """Dry-run report: per path, the candidate revisions (with their
    bytes), the releasable total, and the retained revisions with the
    reasons they must survive a collection."""
    per_path: dict[str, dict] = {
        p: {"candidates": [], "retained": []} for p in paths
    }
    for rev in classified:
        bucket = per_path[rev.path]
        if rev.retained:
            bucket["retained"].append(_gc_retained_entry(rev))
        else:
            bucket["candidates"].append(
                {"revision_no": rev.revision_no, "bytes": rev.length}
            )
    objects = []
    total = 0
    for p in paths:
        bucket = per_path[p]
        releasable = sum(c["bytes"] for c in bucket["candidates"])
        total += releasable
        objects.append(
            {
                "path": p,
                "candidates": bucket["candidates"],
                "releasable_bytes": releasable,
                "retained": bucket["retained"],
            }
        )
    return {
        "dry_run": True,
        "before": before.isoformat(),
        "objects": objects,
        "total_releasable_bytes": total,
    }


@app.post("/gc")
async def gc_revisions(request: Request):
    """Reclaim historical revisions -- or preview the reclamation.

    Only revisions created strictly before ``before`` are considered at
    all, and a revision is deleted only when nothing can still reach it:
    the current revision of every object, every revision pinned by a
    copy-session manifest, every revision referenced by a target
    baseline and every revision pinned by an active revision lease are
    always retained.  ``dry_run`` (default true) reports candidates,
    candidates, releasable bytes and retention reasons from one
    repeatable-read snapshot without changing anything.  With
    ``dry_run=false`` the object rows are locked in ascending path order
    -- the same global order publishers, session creation and
    acknowledgements use -- the classification is repeated under those
    locks, and the survivors are deleted in the same transaction, so a
    concurrent PUT / session / acknowledgement / streaming GET either
    already committed (and is visible to the re-classification) or waits
    until this transaction commits; any failure rolls the whole batch
    back.
    """
    try:
        body = await request.json()
    except Exception:
        return _bad_request("request body must be valid JSON")
    if not isinstance(body, dict):
        return _bad_request("request body must be a JSON object")

    raw_paths = body.get("paths")
    if (
        not isinstance(raw_paths, list)
        or not raw_paths
        or len(raw_paths) > GC_MAX_PATHS
        or not all(isinstance(p, str) and p for p in raw_paths)
    ):
        return _bad_request(
            f"paths must be a list of 1 to {GC_MAX_PATHS} non-empty paths"
        )

    before_raw = body.get("before")
    if not isinstance(before_raw, str):
        return _bad_request("before must be an ISO 8601 timestamp")
    before = _parse_iso8601(before_raw)
    if before is None:
        return _bad_request("before must be an ISO 8601 timestamp")

    dry_run = body.get("dry_run", True)
    if not isinstance(dry_run, bool):
        return _bad_request("dry_run must be a boolean")

    # De-duplicate; every response is ordered by this stable path order.
    paths = sorted(set(raw_paths))
    pool = request.app.state.pool

    if dry_run:
        async with pool.acquire() as conn:
            async with conn.transaction(
                isolation="repeatable_read", readonly=True
            ):
                # One snapshot for the existence check and the
                # classification, so the preview is self-consistent.
                existing = await database.paths_existing(conn, paths)
                missing = [p for p in paths if p not in existing]
                if missing:
                    return _gc_not_found(missing)
                classified = await database.classify_revisions(
                    conn, paths, before
                )
        return _gc_preview(paths, before, classified)

    deleted: list[dict] = []
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Lock first (no placeholder inserts: GC never creates
            # objects), then re-classify under the held locks and delete
            # in the same transaction -- any failure rolls the whole
            # batch back, leaving no partial collection behind.
            locked = await database.lock_existing_objects(conn, paths)
            # Live objects and moved-away paths (revision_seq > 0, even
            # after every historical revision was collected) count; only a
            # bare current_id-NULL placeholder left by session creation is
            # reported as missing, exactly like the preview path.
            missing = [
                p
                for p in paths
                if p not in locked
                or (
                    locked[p]["current_id"] is None
                    and locked[p]["revision_seq"] == 0
                )
            ]
            if missing:
                return _gc_not_found(missing)
            classified = await database.classify_revisions(conn, paths, before)
            victims = [r for r in classified if not r.retained]
            await database.delete_revisions(conn, [r.id for r in victims])
            deleted = [
                {
                    "path": r.path,
                    "revision_no": r.revision_no,
                    "bytes": r.length,
                }
                for r in victims
            ]
            # The execute path reports the same retention reasons the
            # preview does (including revision_lease), per path order.
            retained = {}
            for r in classified:
                if r.retained:
                    retained.setdefault(r.path, []).append(_gc_retained_entry(r))
            retained = [
                {"path": p, "retained": retained.get(p, [])} for p in paths
            ]
    return {
        "dry_run": False,
        "before": before.isoformat(),
        "deleted": deleted,
        "deleted_count": len(deleted),
        "deleted_bytes": sum(d["bytes"] for d in deleted),
        "retained": retained,
    }


# ---- revision leases ----------------------------------------------------------


def _lease_gone(lease, state: str) -> JSONResponse:
    """410 for an expired/released/consumed lease (the id WAS issued)."""
    return JSONResponse(
        {
            "detail": "lease_not_active",
            "lease": lease.id,
            "state": state,
            "expires_at": lease.expires_at.isoformat(),
            "released_at": (
                lease.released_at.isoformat() if lease.released_at else None
            ),
        },
        status_code=410,
    )


@app.post("/revision-leases", status_code=201)
async def create_revision_lease(request: Request):
    """Pin one still-stored revision for a bounded lifetime.

    The request gives ``path``, a positive ``revision_no`` and a TTL of
    60..86400 seconds.  Creation takes the object row FOR UPDATE lock --
    the very lock publishers and the GC execute path take -- and only
    then looks the revision up and inserts the lease in the same
    transaction, so the lease can never be reported created for a
    revision that was concurrently collected (GC blocks on the row lock
    until commit, then sees the lease and retains the revision).  A
    revision already gone -- or a path with no such revision number -- is
    404; malformed parameters are 400.  The response carries a random
    unguessable lease id, the server-computed expiry instant and the
    bound revision's strong ETag.
    """
    try:
        body = await request.json()
    except Exception:
        return _bad_request("request body must be valid JSON")
    if not isinstance(body, dict):
        return _bad_request("request body must be a JSON object")

    path = body.get("path")
    if not isinstance(path, str) or not path:
        return _bad_request("path must be a non-empty string")

    revision_no = body.get("revision_no")
    if (
        not isinstance(revision_no, int)
        or isinstance(revision_no, bool)
        or revision_no < 1
    ):
        return _bad_request("revision_no must be a positive integer")

    ttl = body.get("ttl_seconds")
    if (
        not isinstance(ttl, int)
        or isinstance(ttl, bool)
        or not LEASE_MIN_TTL <= ttl <= LEASE_MAX_TTL
    ):
        return _bad_request(
            f"ttl_seconds must be an integer in "
            f"[{LEASE_MIN_TTL}, {LEASE_MAX_TTL}]"
        )

    pool = request.app.state.pool
    conn = await pool.acquire()
    tx = conn.transaction()
    await tx.start()
    try:
        try:
            # Lock the object row BEFORE reading the revision: the same
            # FOR UPDATE lock publishers and GC take, so a concurrent
            # collection cannot delete the revision between our lookup
            # and the lease insert ("lease succeeded but revision lost").
            await database.lock_object_for_lease(conn, path)
            rev = await database.find_revision(conn, path, revision_no)
            if rev is None:
                await tx.rollback()
                return JSONResponse(
                    {
                        "detail": "revision_not_found",
                        "path": path,
                        "revision_no": revision_no,
                    },
                    status_code=404,
                )
            lease_id = secrets.token_hex(16)
            expires_at = await database.insert_lease(
                conn, lease_id, path, rev, ttl
            )
            await tx.commit()
        except Exception:
            await tx.rollback()
            raise
    finally:
        await pool.release(conn)

    return JSONResponse(
        {
            "lease": lease_id,
            "path": path,
            "revision_no": rev.revision_no,
            "etag": str(make_etag(rev.sha256)),
            "ttl_seconds": ttl,
            "expires_at": expires_at.isoformat(),
        },
        status_code=201,
        headers={
            "Location": f"/revision-leases/{lease_id}/content",
            "ETag": str(make_etag(rev.sha256)),
            "Last-Modified": http_date(rev.created_at),
        },
    )


@app.api_route(
    "/revision-leases/{lease_id}/content", methods=["GET", "HEAD"]
)
async def read_lease_content(lease_id: str, request: Request):
    """Serve the revision the lease bound at creation -- never anything newer.

    The lease row is read first: an unknown id is 404; an expired,
    released or consumed lease is 410.  Only then is the object row
    share-locked (the same lock a streaming GET /objects holds) and the
    bound revision id loaded, after which the ordinary RFC 9110
    precondition chain and Range/If-Range rules run unchanged.  Because
    the binding is the immutable revision *id*, a path migration or a
    re-PUT cannot alter the response; because the share lock is held
    until the last byte, an expiry/release that becomes due while bytes
    are flowing -- or a DELETE arriving mid-stream -- neither truncates
    the body nor changes the already-sent metadata, and GC simply waits
    for the lock before it can collect anything.
    """
    pool = request.app.state.pool
    conn = await pool.acquire()
    tx = conn.transaction()
    await tx.start()
    streaming = False
    try:
        lease = await database.get_lease(conn, lease_id)
        if lease is None:
            await tx.rollback()
            return JSONResponse({"detail": "lease_not_found"}, status_code=404)
        state = lease.state_at(datetime.now(timezone.utc))
        if state != "active" or lease.revision_id is None:
            await tx.rollback()
            return _lease_gone(lease, state if lease.revision_id is not None
                               else "consumed")

        # From here on the lease is active; share-lock the object (blocking
        # GC and publishers for the life of the stream) and load the bound
        # revision by its immutable id.
        await database.lock_object_for_read(conn, lease.object_path)
        rev = await database.get_revision_by_id(conn, lease.revision_id)
        if rev is None:
            # Cannot happen for an active lease: the active classification
            # and GC both run against the same row lock, but if the row was
            # ever missing, the content is definitively gone -- 410.
            await tx.rollback()
            return _lease_gone(lease, "consumed")

        conditions = _conditions_from_request(request.method, request)
        outcome = evaluate(conditions, _selected(rev))
        if outcome is Outcome.PRECONDITION_FAILED:
            await tx.rollback()
            return JSONResponse(
                {"detail": "Precondition Failed"},
                status_code=412,
                headers=validator_headers(rev),
            )
        if outcome is Outcome.NOT_MODIFIED:
            await tx.commit()
            return Response(status_code=304, headers=validator_headers(rev))

        plan = build_plan(rev, request)
        if plan.status == 416:
            await tx.rollback()
            return Response(status_code=416, headers=plan.headers)

        if request.method == "HEAD":
            await tx.commit()
            return Response(status_code=plan.status, headers=plan.headers)

        streaming = True
        return _stream_revision_response(pool, conn, tx, rev, plan)
    finally:
        if not streaming:
            await pool.release(conn)


@app.delete("/revision-leases/{lease_id}")
async def release_revision_lease(lease_id: str, request: Request):
    """Idempotently release a lease.

    Releasing an active lease succeeds with 204; repeating the DELETE --
    or deleting an already expired lease -- also returns 204 (the state
    converges to "released").  Only an id that was never issued is 404.
    Releasing touches only the lease row, so it never blocks a read
    currently streaming the pinned revision.
    """
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        lease = await database.get_lease(conn, lease_id)
        if lease is None:
            return JSONResponse({"detail": "lease_not_found"}, status_code=404)
        await database.release_lease(conn, lease_id)
    return Response(status_code=204)
