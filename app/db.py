"""PostgreSQL access: connection pool, revision lookup and publishing.

All conditional decisions run inside a transaction that locks the
``objects`` row of the path (``SELECT ... FOR SHARE`` for reads,
``FOR UPDATE`` for writes).  Concurrent publishes therefore serialise on
that row, and a streaming GET keeps its share lock until the body has
been fully sent, so the revision selected for the headers can neither
disappear nor be replaced while bytes are streamed.

Every writer takes the ``objects`` row lock *before* touching
``revisions``: the locking statement selects the object row alone
(no join), so even an empty placeholder row (``current_id`` NULL) is
locked; the current revision is read in a separate statement only after
the locks are held.  Locking through an INNER JOIN instead would return
nothing for such a row -- no lock -- letting a first publisher insert
revision 1 before it blocks, which deadlocks against a concurrent
acknowledgement that already holds the placeholder.

Copy sessions reuse the same building blocks: the manifest is captured
with ``FOR SHARE`` snapshots, and acknowledging a segment first locks all
target rows of the batch -- empty placeholders included -- in one
ascending-path ``FOR UPDATE`` statement, then reads their current
revisions inside the held locks and applies its entries (so concurrent
acknowledgements cannot deadlock).

When a session is created a *revision baseline* is recorded for every
target path in the same snapshot transaction (a NULL revision means the
target did not exist).  Acknowledgements compare the locked target's
current revision identity against that baseline: any publish after the
baseline -- even of byte-identical content -- is reported as a conflict,
because a digest match alone cannot distinguish "unchanged" from
"re-published since the session was pinned".

Revision reclamation (``POST /gc``) deletes historical revisions, but
only ones nothing can still reach: the execute path locks every
requested object row ``FOR UPDATE`` in one ascending-path statement --
the same global order publishers, session creation and acknowledgements
use -- and only then classifies revisions inside the held locks.  A
revision is deleted solely when, at that locked moment, it is not the
object's current revision and no copy-session manifest entry or target
baseline references it, so a concurrent PUT, session creation,
acknowledgement or streaming GET can never lose a revision it is using
nor be left with a dangling reference.

Path moves (``POST /moves``) publish the source's current revision as a
new revision of the target path and detach the source row (its readers
get 404), all in one transaction that locks both endpoints in ascending
path order.  The per-path ``revision_seq`` high-water mark keeps
revision numbers monotonic after a move-away even when /gc later removes
every leftover revision row.

Revision leases (``POST /revision-leases``) are a fourth GC retention
reason on equal footing with the current revision, manifests and
baselines.  Lease creation takes the owning object row FOR UPDATE
*before* looking up the pinned revision, serialising it against GC
execute in the same global lock order: a committed lease is visible to
GC's locked classification (an active lease is one whose
``released_at IS NULL AND expires_at > now()``), and a revision GC
already deleted cannot be leased (404).  The lease table deliberately
has no foreign key to ``revisions`` -- after expiry or release, GC may
collect the bound revision while the lease row stays (so reads answer
410 rather than 404) -- instead a content read first reads the lease,
then share-locks its object row and only then re-checks lease liveness
and revision existence; the transaction and share lock are held until
the last streamed byte, so expiry/release during a read never tears the
response, and a DELETE-based release is idempotent.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from dataclasses import dataclass
from datetime import datetime

import asyncpg

from .config import CHUNK_SIZE, SCHEMA_SQL


@dataclass(frozen=True, slots=True)
class CurrentRevision:
    id: int
    revision_no: int
    sha256: str
    media_type: str
    length: int
    created_at: datetime


async def create_pool(database_url: str, retries: int = 30) -> asyncpg.Pool:
    """Create the pool and ensure the schema exists.

    Retries briefly so `docker compose up` works while PostgreSQL is
    still initialising.
    """
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            pool: asyncpg.Pool = await asyncpg.create_pool(
                database_url, min_size=1, max_size=20
            )
            async with pool.acquire() as conn:
                await conn.execute(SCHEMA_SQL)
            return pool
        except (OSError, asyncpg.PostgresError) as exc:  # not ready yet
            last_error = exc
            await asyncio.sleep(1)
    raise RuntimeError(
        f"could not connect to PostgreSQL after {retries} attempts"
    ) from last_error


async def current_revision(
    conn: asyncpg.Connection, path: str, *, for_update: bool
) -> CurrentRevision | None:
    """Lock the objects row and fetch its current revision.

    ``FOR SHARE`` (safe methods) blocks concurrent writes but allows
    parallel readers; ``FOR UPDATE`` (publishing) is exclusive.
    """
    lock = "FOR UPDATE OF o" if for_update else "FOR SHARE OF o"
    row = await conn.fetchrow(
        f"""
        SELECT r.id, r.revision_no, r.sha256, r.media_type,
               octet_length(r.content) AS length, r.created_at
          FROM objects o
          JOIN revisions r ON r.id = o.current_id
         WHERE o.path = $1
         {lock}
        """,
        path,
    )
    if row is None:
        return None
    return _to_revision(row)


async def lock_new_or_existing_object(
    conn: asyncpg.Connection, path: str
) -> CurrentRevision | None:
    """Writer path: create the object row if needed, lock it, then read.

    The upsert guarantees two concurrent first-publishers serialise on
    the same primary-key row instead of colliding on INSERT.  The object
    row is locked FIRST by a plain ``FOR UPDATE`` without joining
    revisions, so an empty placeholder (``current_id`` NULL, left by
    copy-session creation) is locked even though it has no revision; the
    current revision is read only afterwards.  Joining revisions inside
    the locking statement would lock nothing for a placeholder, letting
    a first publisher insert revision 1 *before* it blocks on a
    concurrent acknowledgement holding the row (the two would then wait
    on each other -- ack on the revision unique index, PUT on the object
    row -- and one would be killed with a deadlock).  The loser of the
    row lock instead waits and, on wake-up, continues the revision
    sequence (max + 1).
    """
    await conn.execute(
        "INSERT INTO objects (path) VALUES ($1) ON CONFLICT (path) DO NOTHING",
        path,
    )
    await conn.execute(
        "SELECT path FROM objects WHERE path = $1 FOR UPDATE",
        path,
    )
    row = await conn.fetchrow(
        """
        SELECT r.id, r.revision_no, r.sha256, r.media_type,
               octet_length(r.content) AS length, r.created_at
          FROM objects o
          JOIN revisions r ON r.id = o.current_id
         WHERE o.path = $1
        """,
        path,
    )
    return None if row is None else _to_revision(row)


def _to_revision(row: asyncpg.Record) -> CurrentRevision:
    return CurrentRevision(
        id=row["id"],
        revision_no=row["revision_no"],
        sha256=row["sha256"],
        media_type=row["media_type"],
        length=row["length"],
        created_at=row["created_at"],
    )


async def stream_revision(
    conn: asyncpg.Connection,
    revision_id: int,
    first: int,
    last: int,
    chunk_size: int = CHUNK_SIZE,
):
    """Yield bytes [first, last] (inclusive) of a revision, in slices."""
    pos = first
    while pos <= last:
        end = min(pos + chunk_size - 1, last)
        # substring(bytea FROM start FOR length), SQL positions are 1-based.
        chunk = await conn.fetchval(
            "SELECT substring(content FROM $2 FOR $3) FROM revisions WHERE id = $1",
            revision_id,
            pos + 1,
            end - pos + 1,
        )
        if not chunk:
            break
        yield bytes(chunk)
        pos = end + 1


async def publish_revision(
    conn: asyncpg.Connection,
    path: str,
    content: bytes,
    media_type: str,
) -> tuple[str, int, datetime]:
    """Insert a new revision and point the object at it (one transaction).

    Returns (sha256 hex, revision number, creation time).  The SHA-256 is
    computed in Python so it is available for the response ETag without a
    second round trip.  Callers must already hold the FOR UPDATE lock on
    the objects row (see lock_new_or_existing_object).
    """
    digest = hashlib.sha256(content).hexdigest()
    # Number from the per-path high-water mark rather than max(revision_no)
    # alone: a move-away leaves the object row detached (current_id NULL),
    # and /gc may later delete every leftover revision, after which
    # max(revision_no) would be NULL and numbers would restart at 1.
    revision_no = await conn.fetchval(
        """
        SELECT GREATEST(
                   revision_seq,
                   COALESCE((SELECT max(revision_no) FROM revisions
                              WHERE object_path = $1), 0)
               ) + 1
          FROM objects WHERE path = $1
        """,
        path,
    )
    new_id = await conn.fetchval(
        """
        INSERT INTO revisions (object_path, revision_no, content, sha256, media_type)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """,
        path,
        revision_no,
        content,
        digest,
        media_type,
    )
    await conn.execute(
        "UPDATE objects SET current_id = $2, revision_seq = $3 WHERE path = $1",
        path,
        new_id,
        revision_no,
    )
    created_at = await conn.fetchval(
        "SELECT created_at FROM revisions WHERE id = $1", new_id
    )
    return digest, revision_no, created_at


# ---- path moves --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MoveEndpoint:
    """State of one move endpoint read while its object row is locked."""

    path: str
    exists: bool
    current_id: int | None
    revision_no: int | None
    sha256: str | None
    media_type: str | None
    length: int | None
    revision_seq: int
    created_at: datetime | None


async def lock_move_endpoints(
    conn: asyncpg.Connection, source_path: str, target_path: str
) -> dict[str, MoveEndpoint]:
    """Lock both move endpoints ``FOR UPDATE`` in ascending path order.

    Placeholder rows are inserted for both paths first (also in ascending
    order), so every endpoint has a real row to lock -- the move can make
    an absent path into its target and can race a concurrent first PUT on
    either path.  All locks are taken in one globally ascending-path
    statement, the same order publishers, session creation and
    acknowledgements use, so concurrent moves can never deadlock against
    them (two moves exchanging endpoints both try the same order; the
    second waits on the first row it needs and then re-reads fresh state).

    Only after both rows are locked are their current revisions joined:
    the read cannot go stale before the move is decided.
    """
    for p in sorted({source_path, target_path}):
        await conn.execute(
            "INSERT INTO objects (path) VALUES ($1) ON CONFLICT (path) DO NOTHING",
            p,
        )
    rows = await conn.fetch(
        """
        SELECT o.path,
               (o.current_id IS NOT NULL) AS exists,
               o.current_id,
               r.revision_no, r.sha256, r.media_type,
               octet_length(r.content) AS length, o.revision_seq,
               r.created_at
          FROM objects o
          LEFT JOIN revisions r ON r.id = o.current_id
         WHERE o.path = ANY($1::text[])
         ORDER BY o.path ASC
         FOR UPDATE OF o
        """,
        [source_path, target_path],
    )
    return {
        r["path"]: MoveEndpoint(
            path=r["path"],
            exists=r["exists"],
            current_id=r["current_id"],
            revision_no=r["revision_no"],
            sha256=r["sha256"],
            media_type=r["media_type"],
            length=r["length"],
            revision_seq=r["revision_seq"],
            created_at=r["created_at"],
        )
        for r in rows
    }


async def detach_object(conn: asyncpg.Connection, path: str) -> None:
    """Drop the current pointer of a moved-away path (one UPDATE).

    The revision rows stay behind -- they are immutable history that may
    be pinned by a copy-session manifest, and they anchor numbering until
    /gc collects them; ``revision_seq`` keeps the high-water mark either
    way.  Readers see the path as 404 immediately after commit.
    """
    await conn.execute(
        "UPDATE objects SET current_id = NULL WHERE path = $1", path
    )


# ---- copy sessions -----------------------------------------------------------

COPY_SECRET_KEY = "copy_cursor_secret"


@dataclass(frozen=True, slots=True)
class CopySession:
    id: str
    target_prefix: str
    source_hash: str
    manifest_sha256: str
    segment_size: int
    total_entries: int
    total_segments: int
    next_segment: int
    created_at: datetime


@dataclass(frozen=True, slots=True)
class CopyEntry:
    ordinal: int
    segment_no: int
    source_path: str
    target_path: str
    revision_id: int
    revision_no: int
    sha256: str
    media_type: str
    length: int


@dataclass(frozen=True, slots=True)
class TargetBaseline:
    """Revision identity of a target path captured at session creation.

    ``revision_id is None`` means the target had no current revision
    (its object row was absent or pointed at no revision).  The id --
    never the digest -- is the identity compared on acknowledgement, so
    re-publishing identical bytes after the baseline is detectable.
    """

    ordinal: int
    target_path: str
    revision_id: int | None
    revision_no: int | None
    sha256: str | None


async def ensure_copy_secret(pool: asyncpg.Pool) -> str:
    """Return the HMAC key that signs copy cursors, creating it once.

    The secret lives in the database so cursors stay valid across
    restarts; concurrent first-boots converge on whichever insert wins.
    """
    async with pool.acquire() as conn:
        secret = await conn.fetchval(
            "SELECT value FROM service_meta WHERE key = $1", COPY_SECRET_KEY
        )
        if secret:
            return secret
        await conn.execute(
            "INSERT INTO service_meta (key, value) VALUES ($1, $2)"
            " ON CONFLICT (key) DO NOTHING",
            COPY_SECRET_KEY,
            secrets.token_hex(32),
        )
        return await conn.fetchval(
            "SELECT value FROM service_meta WHERE key = $1", COPY_SECRET_KEY
        )


async def snapshot_source_revisions(
    conn: asyncpg.Connection, paths: list[str]
) -> list[asyncpg.Record]:
    """Lock the source set boundary and read current revisions, one snapshot.

    A single ``SELECT ... FOR SHARE`` statement: every matching objects
    row is locked against concurrent publishes until the caller commits,
    and all rows are read from the same statement snapshot, ordered by
    stable path and revision order.
    """
    return await conn.fetch(
        """
        SELECT o.path AS source_path, r.id AS revision_id, r.revision_no,
               r.sha256, r.media_type, octet_length(r.content) AS length
          FROM objects o
          JOIN revisions r ON r.id = o.current_id
         WHERE o.path = ANY($1::text[])
         ORDER BY o.path ASC, r.revision_no ASC
         FOR SHARE OF o
        """,
        paths,
    )


async def lock_path_set(conn: asyncpg.Connection, paths: list[str]) -> None:
    """Ensure object rows exist for every path and share-lock them.

    Placeholder rows (``current_id`` NULL) are inserted first so even
    paths with no revision yet get a real row to lock: a concurrent
    publisher of such a path then blocks until this transaction commits
    instead of slipping an invisible INSERT past the baseline snapshot.
    All rows -- source and target -- are locked in one ascending-order
    statement, which is the same global order acknowledgements use for
    their ``FOR UPDATE`` locks, so creation and application cannot
    deadlock against each other.
    """
    await conn.executemany(
        "INSERT INTO objects (path) VALUES ($1) ON CONFLICT (path) DO NOTHING",
        [(p,) for p in paths],
    )
    await conn.execute(
        """
        SELECT o.path
          FROM objects o
         WHERE o.path = ANY($1::text[])
         ORDER BY o.path ASC
         FOR SHARE OF o
        """,
        paths,
    )


async def snapshot_target_baselines(
    conn: asyncpg.Connection, target_paths: list[str]
) -> dict[str, asyncpg.Record]:
    """Read current revision identity of every target path.

    Callers must have already share-locked the rows (``lock_path_set``);
    the LEFT JOIN keeps target paths without a revision, represented by a
    record whose revision columns are NULL.
    """
    rows = await conn.fetch(
        """
        SELECT o.path AS target_path, r.id AS revision_id,
               r.revision_no, r.sha256
          FROM objects o
          LEFT JOIN revisions r ON r.id = o.current_id
         WHERE o.path = ANY($1::text[])
        """,
        target_paths,
    )
    return {r["target_path"]: r for r in rows}


async def insert_copy_session(
    conn: asyncpg.Connection,
    session_id: str,
    target_prefix: str,
    source_hash: str,
    manifest_sha256: str,
    segment_size: int,
    entries: list[CopyEntry],
    baselines: list[TargetBaseline],
) -> None:
    """Persist the session header, manifest entries and target baselines
    (one transaction with the snapshot that produced them)."""
    total_entries = len(entries)
    total_segments = (
        (total_entries + segment_size - 1) // segment_size
        if total_entries
        else 0
    )
    await conn.execute(
        """
        INSERT INTO copy_sessions
            (id, target_prefix, source_hash, manifest_sha256,
             segment_size, total_entries, total_segments)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        session_id,
        target_prefix,
        source_hash,
        manifest_sha256,
        segment_size,
        total_entries,
        total_segments,
    )
    await conn.executemany(
        """
        INSERT INTO copy_session_entries
            (session_id, ordinal, segment_no, source_path, target_path,
             revision_id, revision_no, sha256, media_type, length)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        [
            (
                session_id,
                e.ordinal,
                e.segment_no,
                e.source_path,
                e.target_path,
                e.revision_id,
                e.revision_no,
                e.sha256,
                e.media_type,
                e.length,
            )
            for e in entries
        ],
    )
    await conn.executemany(
        """
        INSERT INTO copy_session_target_baselines
            (session_id, ordinal, target_path, revision_id,
             revision_no, sha256)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        [
            (
                session_id,
                b.ordinal,
                b.target_path,
                b.revision_id,
                b.revision_no,
                b.sha256,
            )
            for b in baselines
        ],
    )


def _to_copy_session(row: asyncpg.Record) -> CopySession:
    return CopySession(
        id=row["id"],
        target_prefix=row["target_prefix"],
        source_hash=row["source_hash"],
        manifest_sha256=row["manifest_sha256"],
        segment_size=row["segment_size"],
        total_entries=row["total_entries"],
        total_segments=row["total_segments"],
        next_segment=row["next_segment"],
        created_at=row["created_at"],
    )


async def get_copy_session(
    conn: asyncpg.Connection, session_id: str
) -> CopySession | None:
    row = await conn.fetchrow(
        """
        SELECT id, target_prefix, source_hash, manifest_sha256,
               segment_size, total_entries, total_segments, next_segment,
               created_at
          FROM copy_sessions
         WHERE id = $1
        """,
        session_id,
    )
    return None if row is None else _to_copy_session(row)


def _to_copy_entry(row: asyncpg.Record) -> CopyEntry:
    return CopyEntry(
        ordinal=row["ordinal"],
        segment_no=row["segment_no"],
        source_path=row["source_path"],
        target_path=row["target_path"],
        revision_id=row["revision_id"],
        revision_no=row["revision_no"],
        sha256=row["sha256"],
        media_type=row["media_type"],
        length=row["length"],
    )


async def fetch_segment_entries(
    conn: asyncpg.Connection, session_id: str, segment_no: int
) -> list[CopyEntry]:
    """Manifest entries of one segment, in stable manifest order."""
    rows = await conn.fetch(
        """
        SELECT ordinal, segment_no, source_path, target_path, revision_id,
               revision_no, sha256, media_type, length
          FROM copy_session_entries
         WHERE session_id = $1 AND segment_no = $2
         ORDER BY ordinal
        """,
        session_id,
        segment_no,
    )
    return [_to_copy_entry(r) for r in rows]


def _to_target_baseline(row: asyncpg.Record) -> TargetBaseline:
    return TargetBaseline(
        ordinal=row["ordinal"],
        target_path=row["target_path"],
        revision_id=row["revision_id"],
        revision_no=row["revision_no"],
        sha256=row["sha256"],
    )


async def session_has_baselines(
    conn: asyncpg.Connection, session_id: str
) -> bool:
    """Whether this session carries target revision baselines.

    Sessions created before the baseline upgrade have no rows here; their
    unfinished segments must not be acknowledged on digest evidence
    alone.
    """
    return await conn.fetchval(
        "SELECT EXISTS ("
        "SELECT 1 FROM copy_session_target_baselines WHERE session_id = $1)",
        session_id,
    )


async def fetch_segment_baselines(
    conn: asyncpg.Connection, session_id: str, segment_no: int
) -> dict[str, TargetBaseline]:
    """Baselines for the targets of one segment, keyed by target path."""
    rows = await conn.fetch(
        """
        SELECT b.ordinal, b.target_path, b.revision_id,
               b.revision_no, b.sha256
          FROM copy_session_target_baselines b
          JOIN copy_session_entries e
            ON e.session_id = b.session_id
           AND e.target_path = b.target_path
         WHERE b.session_id = $1 AND e.segment_no = $2
        """,
        session_id,
        segment_no,
    )
    return {r["target_path"]: _to_target_baseline(r) for r in rows}


async def fetch_revision_content(
    conn: asyncpg.Connection, revision_id: int
) -> bytes:
    """Full content of a pinned revision (revisions are immutable)."""
    content = await conn.fetchval(
        "SELECT content FROM revisions WHERE id = $1", revision_id
    )
    return bytes(content)


async def fetch_ack(
    conn: asyncpg.Connection, session_id: str, segment_no: int
) -> tuple[str, dict] | None:
    """Stored batch result for a segment, if it was already acknowledged."""
    row = await conn.fetchrow(
        """
        SELECT status, result FROM copy_session_acks
         WHERE session_id = $1 AND segment_no = $2
        """,
        session_id,
        segment_no,
    )
    if row is None:
        return None
    return row["status"], json.loads(row["result"])


def _canonical_json(result: dict) -> str:
    """Serialise exactly like ``JSONResponse`` (compact, insertion order),
    so a stored result replays byte-identically to the original response."""
    return json.dumps(result, ensure_ascii=False, separators=(",", ":"))


async def insert_ack(
    conn: asyncpg.Connection,
    session_id: str,
    segment_no: int,
    cursor: str,
    status: str,
    result: dict,
) -> bool:
    """Persist the batch result; False when a concurrent ack already did.

    ``ON CONFLICT DO NOTHING`` makes concurrent acknowledgements of the
    same segment converge on the first stored result instead of failing
    with a unique-violation.
    """
    row = await conn.fetchrow(
        """
        INSERT INTO copy_session_acks
            (session_id, segment_no, cursor, status, result)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (session_id, segment_no) DO NOTHING
        RETURNING segment_no
        """,
        session_id,
        segment_no,
        cursor,
        status,
        _canonical_json(result),
    )
    return row is not None


async def advance_session(
    conn: asyncpg.Connection, session_id: str, next_segment: int
) -> None:
    await conn.execute(
        "UPDATE copy_sessions SET next_segment = $2 WHERE id = $1",
        session_id,
        next_segment,
    )


async def lock_target_objects(
    conn: asyncpg.Connection, target_paths: list[str]
) -> dict[str, asyncpg.Record]:
    """FOR UPDATE-lock every target object row, then read current revisions.

    This is deliberately TWO statements, and the order matters:

    1. Lock every objects row (placeholder rows are inserted first) in a
       plain ``SELECT ... FOR UPDATE`` -- no join with ``revisions`` --
       so even an empty ``current_id`` NULL placeholder is locked.  A
       lock query that INNER JOINs revisions returns no row for such a
       placeholder and locks nothing, letting a concurrent first publish
       race the apply (both inserting revision 1, or a 500 instead of a
       conflict).  Rows are taken in ascending path order -- the same
       global order used by session creation (FOR SHARE) and one-row
       PUTs -- so concurrent acks, publishes and session creations
       serialise without deadlocks.
    2. Only after EVERY target row is locked, LEFT JOIN revisions and
       read the current revision identity.  The read cannot block and
       cannot go stale while the batch runs: writers always take this
       same object row lock before inserting a revision, and we now hold
       it for every target until commit/rollback.  Folding the read into
       statement 1 would mix per-row locking with the snapshot: a
       row locked early could be published by another transaction
       between that point and the later ``max(revision_no)``, letting a
       waking ack insert a ghost revision numbered max+1 instead of
       seeing the publish and reporting the conflict.

    Returns each path's current revision identity (revision columns NULL
    for an empty placeholder).
    """
    await conn.executemany(
        "INSERT INTO objects (path) VALUES ($1) ON CONFLICT (path) DO NOTHING",
        [(p,) for p in target_paths],
    )
    await conn.execute(
        """
        SELECT o.path
          FROM objects o
         WHERE o.path = ANY($1::text[])
         ORDER BY o.path ASC
         FOR UPDATE OF o
        """,
        target_paths,
    )
    rows = await conn.fetch(
        """
        SELECT o.path AS path, r.id AS revision_id, r.revision_no, r.sha256
          FROM objects o
          LEFT JOIN revisions r ON r.id = o.current_id
         WHERE o.path = ANY($1::text[])
        """,
        target_paths,
    )
    return {r["path"]: r for r in rows}


async def apply_copy_entries(
    conn: asyncpg.Connection,
    entries: list[CopyEntry],
    baselines: dict[str, TargetBaseline],
    current: dict[str, asyncpg.Record],
) -> tuple[list[dict], list[dict]]:
    """Apply one segment to the target prefix inside the caller's transaction.

    ``current`` is the locked snapshot produced by
    :func:`lock_target_objects`: every target object row -- including an
    empty ``current_id`` NULL placeholder -- was already taken
    ``FOR UPDATE`` in ascending path order, and the current revision
    identity read *after* that lock.  No query in here joins revisions
    before locking the object, so a concurrent first publish cannot slip
    past an unlocked placeholder.

    Each entry is checked against the revision baseline captured when the
    session was created:

    * baseline missing the revision and the locked row still empty ->
      publish the pinned content as the next revision (``max + 1``):
      when the ack merely waited behind another writer it continues the
      revision sequence instead of colliding on revision 1;
    * baseline revision identity unchanged and digest equal ->
      idempotent no-op (``already_present``);
    * anything else is a conflict -- including a re-publish of the very
      same bytes, which a digest comparison could not distinguish from
      "never changed".

    The caller decides: commit everything, or roll back and report the
    conflicts — never a partial application.
    """
    applied: list[dict] = []
    conflicts: list[dict] = []
    for entry in sorted(entries, key=lambda e: e.target_path):
        baseline = baselines[entry.target_path]
        # Locked in lock_target_objects; never re-read via an un-locked join.
        row = current[entry.target_path]

        if row["revision_id"] is None:
            if baseline.revision_id is None:
                # Baseline said "absent" and the locked row is still empty:
                # publish the pinned content as the next revision number.
                # Going through publish_revision (max+1 rather than a
                # hard-coded 1) is what lets an ack that waited behind
                # another transaction continue the sequence.
                content = await fetch_revision_content(conn, entry.revision_id)
                digest, revision_no, _ = await publish_revision(
                    conn, entry.target_path, content, entry.media_type
                )
                applied.append(
                    {
                        "target_path": entry.target_path,
                        "source_path": entry.source_path,
                        "revision_no": revision_no,
                        "sha256": digest,
                        "action": "created",
                    }
                )
            else:
                # Existed at baseline, current revision gone since.
                conflicts.append(
                    {
                        "target_path": entry.target_path,
                        "source_path": entry.source_path,
                        "reason": "target_changed_after_baseline",
                        "expected_sha256": entry.sha256,
                        "actual_sha256": None,
                        "actual_revision_no": None,
                        "baseline_revision_no": baseline.revision_no,
                        "baseline_sha256": baseline.sha256,
                    }
                )
            continue

        if row["revision_id"] == baseline.revision_id:
            # Same revision identity as at baseline: nothing was published
            # here since the session was pinned.
            if row["sha256"] == entry.sha256:
                applied.append(
                    {
                        "target_path": entry.target_path,
                        "source_path": entry.source_path,
                        "revision_no": row["revision_no"],
                        "sha256": entry.sha256,
                        "action": "already_present",
                    }
                )
            else:
                # Cannot happen on immutable revisions, but never apply
                # over content that contradicts the pinned digest.
                conflicts.append(
                    {
                        "target_path": entry.target_path,
                        "source_path": entry.source_path,
                        "reason": "content_diverged",
                        "expected_sha256": entry.sha256,
                        "actual_sha256": row["sha256"],
                        "actual_revision_no": row["revision_no"],
                        "baseline_revision_no": baseline.revision_no,
                        "baseline_sha256": baseline.sha256,
                    }
                )
            continue

        # A different revision identity: the target was published after
        # the baseline -- even byte-identical content counts as a change.
        reason = (
            "target_republished_same_content"
            if row["sha256"] == entry.sha256
            else "content_diverged"
        )
        conflicts.append(
            {
                "target_path": entry.target_path,
                "source_path": entry.source_path,
                "reason": reason,
                "expected_sha256": entry.sha256,
                "actual_sha256": row["sha256"],
                "actual_revision_no": row["revision_no"],
                "baseline_revision_no": baseline.revision_no,
                "baseline_sha256": baseline.sha256,
            }
        )
    return applied, conflicts


# ---- historical revision reclamation ------------------------------------------


@dataclass(frozen=True, slots=True)
class GcRevision:
    """A revision older than the GC cutoff, with its retention facts.

    ``retained`` is the disjunction of the four reasons a revision may
    not be collected: it is the object's current revision, a copy-session
    manifest pins it, a target baseline references it, or an active
    revision lease holds it.
    """

    id: int
    path: str
    revision_no: int
    length: int
    created_at: datetime
    is_current: bool
    in_manifest: bool
    in_baseline: bool
    in_lease: bool

    @property
    def retained(self) -> bool:
        return (
            self.is_current
            or self.in_manifest
            or self.in_baseline
            or self.in_lease
        )


async def lock_existing_objects(
    conn: asyncpg.Connection, paths: list[str]
) -> dict[str, asyncpg.Record]:
    """FOR UPDATE-lock existing object rows in ascending path order.

    Unlike the copy-session helpers this never inserts placeholder rows:
    reclamation must not create objects.  Rows are locked in one
    ascending-path statement -- the same global order publishers, session
    creation (FOR SHARE) and acknowledgements use -- so a GC transaction
    serialises against all of them without deadlocks.  Anything that
    could still reference a revision of these paths (a streaming GET's
    share lock, a session being created, an acknowledgement publishing
    to a target) either already committed -- and is visible to the
    classification that follows -- or blocks until this transaction
    commits.  Returns path -> row (``current_id`` may be NULL for a
    placeholder left by session creation).
    """
    rows = await conn.fetch(
        """
        SELECT o.path, o.current_id, o.revision_seq
          FROM objects o
         WHERE o.path = ANY($1::text[])
         ORDER BY o.path ASC
         FOR UPDATE OF o
        """,
        paths,
    )
    return {r["path"]: r for r in rows}


async def paths_existing(conn: asyncpg.Connection, paths: list[str]) -> set[str]:
    """Subset of ``paths`` that currently exist for reclamation.

    Two kinds of object rows count as existing:

    * paths with a current revision (ordinary live objects), and
    * paths with revisions but no current revision -- a moved-away path,
      whose detached row keeps its revision history until collection.

    A bare placeholder row (``current_id`` NULL and ``revision_seq`` = 0,
    left by copy-session creation for a target that never existed) does
    not count: such a path is a 404 for readers, so it is a 404 for
    reclamation too.  A moved-away path keeps ``revision_seq > 0`` even
    after every historical revision has been collected, so it remains a
    valid GC target that simply reports empty lists on repeat -- exactly
    like collecting a live object that has no history left.
    """
    rows = await conn.fetch(
        """
        SELECT o.path
          FROM objects o
         WHERE o.path = ANY($1::text[])
           AND (o.current_id IS NOT NULL OR o.revision_seq > 0)
        """,
        paths,
    )
    return {r["path"] for r in rows}


async def classify_revisions(
    conn: asyncpg.Connection, paths: list[str], before: datetime
) -> list[GcRevision]:
    """Revisions of ``paths`` created strictly before ``before``.

    Every row carries its retention facts, evaluated in the
    caller's transaction (for the execute path: under the held FOR
    UPDATE locks, so the facts cannot go stale before the DELETE).
    Ordered by ascending path and revision number -- the fixed order the
    responses are built in.
    """
    rows = await conn.fetch(
        """
        SELECT r.id, r.object_path AS path, r.revision_no,
               octet_length(r.content) AS length, r.created_at,
               (o.current_id = r.id) AS is_current,
               EXISTS (SELECT 1 FROM copy_session_entries e
                        WHERE e.revision_id = r.id) AS in_manifest,
               EXISTS (SELECT 1 FROM copy_session_target_baselines b
                        WHERE b.revision_id = r.id) AS in_baseline,
               EXISTS (SELECT 1 FROM revision_leases l
                        WHERE l.revision_id = r.id
                          AND l.released_at IS NULL
                          AND l.expires_at > now()) AS in_lease
          FROM revisions r
          JOIN objects o ON o.path = r.object_path
         WHERE r.object_path = ANY($1::text[])
           AND r.created_at < $2
         ORDER BY r.object_path ASC, r.revision_no ASC
        """,
        paths,
        before,
    )
    return [
        GcRevision(
            id=row["id"],
            path=row["path"],
            revision_no=row["revision_no"],
            length=row["length"],
            created_at=row["created_at"],
            is_current=row["is_current"],
            in_manifest=row["in_manifest"],
            in_baseline=row["in_baseline"],
            in_lease=row["in_lease"],
        )
        for row in rows
    ]


async def delete_revisions(
    conn: asyncpg.Connection, revision_ids: list[int]
) -> None:
    """Delete revisions by id, inside the caller's transaction.

    Callers must hold the FOR UPDATE locks on the owning object rows and
    must have classified under those locks, so every id here is
    unreferenced (not current, not pinned by a manifest or baseline) and
    the delete cannot violate the foreign keys from ``objects``,
    ``copy_session_entries`` or ``copy_session_target_baselines``.  The
    current revision -- always the highest revision number -- is never
    deleted, so ``max(revision_no) + 1`` keeps the per-object numbering
    strictly increasing across collections.
    """
    if not revision_ids:
        return
    await conn.execute(
        "DELETE FROM revisions WHERE id = ANY($1::bigint[])",
        revision_ids,
    )


# ---- historical revision leases -----------------------------------------------


@dataclass(frozen=True, slots=True)
class RevisionLease:
    """One revision lease as stored (``revision_id`` may outlive the
    revision row -- see the table comment in config.SCHEMA_SQL)."""

    id: str
    object_path: str
    revision_id: int
    revision_no: int
    created_at: datetime
    expires_at: datetime
    released_at: datetime | None


@dataclass(frozen=True, slots=True)
class LeasedRevision:
    """Outcome of pinning a lease's revision for a content read:

    ``"missing"``  -- no lease row exists for the id (HTTP 404);
    ``"released"`` -- released lease (HTTP 410);
    ``"expired"``  -- never released but past ``expires_at`` (HTTP 410);
    ``"active"``   -- valid lease, ``revision`` is locked FOR SHARE and
                      the caller's transaction owns that lock until the
                      full body has been streamed.
    """

    status: str
    lease: RevisionLease | None = None
    revision: CurrentRevision | None = None


def _to_lease(row: asyncpg.Record) -> RevisionLease:
    return RevisionLease(
        id=row["id"],
        object_path=row["object_path"],
        revision_id=row["revision_id"],
        revision_no=row["revision_no"],
        created_at=row["created_at"],
        expires_at=row["expires_at"],
        released_at=row["released_at"],
    )


async def lock_path_revision_for_lease(
    conn: asyncpg.Connection, path: str, revision_no: int
) -> CurrentRevision | None:
    """Lock the object row FOR UPDATE and fetch one of its revisions.

    Lease creation and GC execute both take the object row lock before
    touching ``revisions`` (the same global order every writer uses), so
    they are strictly serialised per object: a lease whose revision
    lookup succeeds is committed before GC's re-classification can see
    the revision, and a revision GC has just deleted cannot be leased.
    Returns None when the path has no object row (no placeholder is ever
    created by leasing) or the revision number is not stored.
    """
    await conn.execute(
        """
        SELECT o.path
          FROM objects o
         WHERE o.path = $1
         FOR UPDATE OF o
        """,
        path,
    )
    row = await conn.fetchrow(
        """
        SELECT r.id, r.revision_no, r.sha256, r.media_type,
               octet_length(r.content) AS length, r.created_at
          FROM revisions r
         WHERE r.object_path = $1 AND r.revision_no = $2
        """,
        path,
        revision_no,
    )
    return None if row is None else _to_revision(row)


async def insert_revision_lease(
    conn: asyncpg.Connection,
    lease_id: str,
    path: str,
    rev: CurrentRevision,
    ttl_seconds: int,
) -> RevisionLease:
    """Persist a lease expiring ``ttl_seconds`` after the database clock.

    ``expires_at`` is computed by PostgreSQL (``now() + interval``) so
    that GC's ``expires_at > now()`` liveness test uses one clock.
    """
    row = await conn.fetchrow(
        """
        INSERT INTO revision_leases
            (id, object_path, revision_id, revision_no, expires_at)
        VALUES ($1, $2, $3, $4, now() + make_interval(secs => $5))
        RETURNING id, object_path, revision_id, revision_no,
                  created_at, expires_at, released_at
        """,
        lease_id,
        path,
        rev.id,
        rev.revision_no,
        ttl_seconds,
    )
    return _to_lease(row)


async def pin_leased_revision(conn: asyncpg.Connection, lease_id: str) -> LeasedRevision:
    """Resolve a lease and lock its revision's object row FOR SHARE.

    Two statements are required: only the lease row knows which object
    row to lock.  The lease is read first (no lock -- releases do not
    block reads), the object row is share-locked, and only then are the
    lease status and revision existence re-checked.  Under the held
    share lock GC (FOR UPDATE) cannot delete the revision, and a path
    move / PUT cannot move the current pointer: the response is bound to
    the leased revision until the caller commits after the last byte,
    even if the lease expires or is released while bytes are in flight.
    """
    row = await conn.fetchrow(
        """
        SELECT id, object_path, revision_id, revision_no,
               created_at, expires_at, released_at
          FROM revision_leases
         WHERE id = $1
        """,
        lease_id,
    )
    if row is None:
        return LeasedRevision(status="missing")
    lease = _to_lease(row)

    await conn.execute(
        "SELECT path FROM objects WHERE path = $1 FOR SHARE",
        lease.object_path,
    )

    # Re-check under the held object lock: a GC that committed before
    # the lock was taken may have collected the revision (an expired /
    # released lease no longer retains it).  now() is the transaction
    # time, which also governs the GC liveness test.
    fresh = await conn.fetchrow(
        """
        SELECT released_at, (expires_at <= now()) AS expired
          FROM revision_leases
         WHERE id = $1
        """,
        lease_id,
    )
    if fresh["released_at"] is not None or fresh["expired"]:
        status = "released" if fresh["released_at"] is not None else "expired"
        return LeasedRevision(status=status, lease=lease)

    rev_row = await conn.fetchrow(
        """
        SELECT r.id, r.revision_no, r.sha256, r.media_type,
               octet_length(r.content) AS length, r.created_at
          FROM revisions r
         WHERE r.id = $1
        """,
        lease.revision_id,
    )
    if rev_row is None:
        # The revision was collected before this read acquired the lock;
        # the lease must already be inactive (active leases retain it).
        return LeasedRevision(status="expired", lease=lease)
    return LeasedRevision(
        status="active", lease=lease, revision=_to_revision(rev_row)
    )


async def release_revision_lease(conn: asyncpg.Connection, lease_id: str) -> str:
    """Idempotently release a lease.

    ``"missing"`` -- no lease with this id ever existed (HTTP 404);
    ``"released_now"`` -- this call marked it released (HTTP 204);
    ``"already_released"`` -- a prior DELETE did, or it has since
    expired (also HTTP 204: release of a known lease id is idempotent).
    """
    row = await conn.fetchrow(
        "SELECT released_at, now() >= expires_at AS expired"
        "  FROM revision_leases WHERE id = $1",
        lease_id,
    )
    if row is None:
        return "missing"
    if row["released_at"] is not None or row["expired"]:
        return "already_released"
    updated = await conn.fetchval(
        "UPDATE revision_leases SET released_at = now()"
        " WHERE id = $1 AND released_at IS NULL AND expires_at > now()"
        " RETURNING id",
        lease_id,
    )
    return "released_now" if updated is not None else "already_released"
