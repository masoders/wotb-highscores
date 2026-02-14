import aiosqlite
import difflib
from contextlib import asynccontextmanager
from . import config, utils

SCHEMA = """
CREATE TABLE IF NOT EXISTS tanks (
    name TEXT PRIMARY KEY,
    name_norm TEXT NOT NULL UNIQUE,
    tier INTEGER NOT NULL,
    type TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS submissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_name_raw TEXT NOT NULL,
    player_name_norm TEXT NOT NULL,
    tank_name TEXT NOT NULL,
    score INTEGER NOT NULL,
    submitted_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (tank_name) REFERENCES tanks(name)
      ON UPDATE CASCADE
      ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_submissions_player_tank
ON submissions (tank_name, player_name_norm);

CREATE TABLE IF NOT EXISTS tank_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    details TEXT NOT NULL,
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS score_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    submission_id INTEGER,
    tank_name TEXT NOT NULL,
    player_name_raw TEXT NOT NULL,
    player_name_norm TEXT NOT NULL,
    old_score INTEGER,
    new_score INTEGER,
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL,
    details TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS tank_index_posts (
    tier INTEGER NOT NULL,
    type TEXT NOT NULL,
    thread_id INTEGER NOT NULL,
    forum_channel_id INTEGER NOT NULL,
    PRIMARY KEY (tier, type)
);

CREATE TABLE IF NOT EXISTS tank_aliases (
    alias_norm TEXT PRIMARY KEY,
    alias_raw TEXT NOT NULL,
    tank_name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clan_players (
    region TEXT NOT NULL,
    account_id INTEGER NOT NULL,
    clan_id INTEGER NOT NULL,
    player_name_raw TEXT NOT NULL,
    player_name_norm TEXT NOT NULL,
    last_synced_at TEXT NOT NULL,
    PRIMARY KEY (region, account_id)
);

CREATE INDEX IF NOT EXISTS idx_clan_players_region_name
ON clan_players (region, player_name_raw);

CREATE INDEX IF NOT EXISTS idx_clan_players_region_norm
ON clan_players (region, player_name_norm);

CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
"""

async def _apply_sqlite_pragmas(db: aiosqlite.Connection):
    # Connection-level safety/perf defaults for SQLite in this bot workload.
    await db.execute("PRAGMA foreign_keys = ON;")
    await db.execute("PRAGMA busy_timeout = 5000;")
    await db.execute("PRAGMA journal_mode = WAL;")
    await db.execute("PRAGMA synchronous = NORMAL;")

@asynccontextmanager
async def _connect_db():
    async with aiosqlite.connect(config.DB_PATH) as conn:
        await _apply_sqlite_pragmas(conn)
        yield conn

async def _table_columns(db: aiosqlite.Connection, table_name: str) -> set[str]:
    cur = await db.execute(f"PRAGMA table_info({table_name})")
    rows = await cur.fetchall()
    await cur.close()
    return {str(r[1]) for r in rows}

async def _migration_001_cleanup_submission_indexes(db: aiosqlite.Connection):
    await db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_submissions_player_tank "
        "ON submissions (tank_name, player_name_norm)"
    )
    # Remove duplicate legacy indexes on the same key.
    await db.execute("DROP INDEX IF EXISTS ux_submissions_tank_player")
    await db.execute("DROP INDEX IF EXISTS ux_submissions_best_per_tank_player")

async def _migration_002_backfill_tank_name_norm(db: aiosqlite.Connection):
    cols = await _table_columns(db, "tanks")
    if "name_norm" not in cols:
        await db.execute("ALTER TABLE tanks ADD COLUMN name_norm TEXT")

    cur = await db.execute("SELECT rowid, name, COALESCE(name_norm, '') FROM tanks")
    rows = await cur.fetchall()
    await cur.close()

    for rowid, name, existing_norm in rows:
        normalized = utils.norm_tank_name(str(name or ""))
        if str(existing_norm or "") != normalized:
            await db.execute(
                "UPDATE tanks SET name_norm = ? WHERE rowid = ?",
                (normalized, int(rowid)),
            )

    cur = await db.execute(
        "SELECT name_norm, COUNT(*) FROM tanks GROUP BY name_norm HAVING COUNT(*) > 1"
    )
    duplicates = await cur.fetchall()
    await cur.close()
    if duplicates:
        sample = ", ".join([f"{n}({c})" for n, c in duplicates[:5]])
        raise RuntimeError(
            "Duplicate normalized tank names prevent unique index creation: "
            + sample
        )

    await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tanks_name_norm ON tanks(name_norm)")

async def _migration_003_add_query_indexes(db: aiosqlite.Connection):
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_submissions_tank_score_id "
        "ON submissions (tank_name, score DESC, id ASC)"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_tanks_tier_type_name "
        "ON tanks (tier, type, name)"
    )

async def _migration_004_add_score_change_indexes(db: aiosqlite.Connection):
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_score_changes_created_at "
        "ON score_changes (created_at DESC, id DESC)"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_score_changes_submission_id "
        "ON score_changes (submission_id)"
    )

async def _migration_005_backfill_player_name_norm(db: aiosqlite.Connection):
    cur = await db.execute(
        """
        SELECT id, player_name_raw, player_name_norm, tank_name, score, submitted_by, created_at
        FROM submissions
        ORDER BY id ASC
        """
    )
    rows = await cur.fetchall()
    await cur.close()
    if not rows:
        return

    # Build the target normalized key from current normalization rules.
    normalized_rows: list[tuple[int, str, str, str, int, str, str]] = []
    for sid, player_raw, player_norm, tank_name, score, submitted_by, created_at in rows:
        raw = str(player_raw or "")
        existing_norm = str(player_norm or "")
        new_norm = utils.normalize_player(raw)
        if not new_norm:
            # Keep a stable fallback for malformed historical rows.
            new_norm = existing_norm or f"legacy-{int(sid)}"
        normalized_rows.append(
            (
                int(sid),
                raw,
                new_norm,
                str(tank_name),
                int(score),
                str(submitted_by),
                str(created_at),
            )
        )

    by_key: dict[tuple[str, str], list[tuple[int, str, str, str, int, str, str]]] = {}
    for row in normalized_rows:
        sid, _raw, new_norm, tank_name, _score, _submitted_by, _created_at = row
        key = (tank_name, new_norm)
        by_key.setdefault(key, []).append(row)

    ids_to_delete: set[int] = set()
    ids_to_update_norm: list[tuple[str, int]] = []

    for (_tank_name, _new_norm), grouped in by_key.items():
        # Keep the strongest row when legacy variants collapse:
        # higher score first, then latest created_at, then highest id.
        grouped_sorted = sorted(
            grouped,
            key=lambda r: (int(r[4]), str(r[6]), int(r[0])),
            reverse=True,
        )
        winner = grouped_sorted[0]
        winner_id = int(winner[0])

        for row in grouped_sorted[1:]:
            ids_to_delete.add(int(row[0]))

        ids_to_update_norm.append((str(winner[2]), winner_id))

    if ids_to_delete:
        await db.executemany(
            "DELETE FROM submissions WHERE id = ?",
            [(sid,) for sid in sorted(ids_to_delete)],
        )
        # Best-effort audit for migration dedupe actions.
        await db.executemany(
            """
            INSERT INTO score_changes (
                action, submission_id, tank_name, player_name_raw, player_name_norm,
                old_score, new_score, actor, created_at, details
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    "delete",
                    int(sid),
                    str(tank_name),
                    str(player_raw),
                    str(new_norm),
                    int(score),
                    None,
                    "migration",
                    utils.utc_now_z(),
                    "dedupe-after-player-normalization-backfill",
                )
                for sid, player_raw, new_norm, tank_name, score, _submitted_by, _created_at in normalized_rows
                if int(sid) in ids_to_delete
            ],
        )

    if ids_to_update_norm:
        await db.executemany(
            "UPDATE submissions SET player_name_norm = ? WHERE id = ?",
            ids_to_update_norm,
        )

    cur = await db.execute(
        """
        SELECT tank_name, player_name_norm, COUNT(*)
        FROM submissions
        GROUP BY tank_name, player_name_norm
        HAVING COUNT(*) > 1
        """
    )
    collisions = await cur.fetchall()
    await cur.close()
    if collisions:
        sample = ", ".join([f"{t}:{p}({c})" for t, p, c in collisions[:5]])
        raise RuntimeError(
            "Duplicate (tank_name, player_name_norm) rows remain after player normalization backfill: "
            + sample
        )

async def _migration_006_canonicalize_player_display_names(db: aiosqlite.Connection):
    # Pick one canonical display value per normalized name:
    # latest created_at, then latest id.
    cur = await db.execute(
        """
        WITH ranked AS (
            SELECT
                player_name_norm,
                player_name_raw,
                created_at,
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY player_name_norm
                    ORDER BY created_at DESC, id DESC
                ) AS rn
            FROM submissions
        )
        SELECT player_name_norm, player_name_raw
        FROM ranked
        WHERE rn = 1
        """
    )
    canon_rows = await cur.fetchall()
    await cur.close()
    if not canon_rows:
        return

    canonical_by_norm = {
        str(norm): str(raw)
        for norm, raw in canon_rows
        if norm and raw
    }
    if not canonical_by_norm:
        return

    cur = await db.execute(
        "SELECT id, player_name_norm, player_name_raw, tank_name, score FROM submissions"
    )
    existing_rows = await cur.fetchall()
    await cur.close()

    updates: list[tuple[str, int]] = []
    audit_rows: list[tuple[str, int, str, str, str, int, int, str, str, str]] = []
    now = utils.utc_now_z()

    for sid, norm, raw, tank_name, score in existing_rows:
        norm_s = str(norm or "")
        raw_s = str(raw or "")
        canonical = canonical_by_norm.get(norm_s, raw_s)
        if canonical == raw_s:
            continue
        updates.append((canonical, int(sid)))
        audit_rows.append(
            (
                "edit",
                int(sid),
                str(tank_name),
                canonical,
                norm_s,
                int(score),
                int(score),
                "migration",
                now,
                f"canonicalize-player-display:{raw_s}->{canonical}",
            )
        )

    if updates:
        await db.executemany(
            "UPDATE submissions SET player_name_raw = ? WHERE id = ?",
            updates,
        )
    if audit_rows:
        await db.executemany(
            """
            INSERT INTO score_changes (
                action, submission_id, tank_name, player_name_raw, player_name_norm,
                old_score, new_score, actor, created_at, details
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            audit_rows,
        )

async def _migration_007_add_tank_aliases_table(db: aiosqlite.Connection):
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS tank_aliases (
            alias_norm TEXT PRIMARY KEY,
            alias_raw TEXT NOT NULL,
            tank_name TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

async def _migration_008_enforce_tank_integrity(db: aiosqlite.Connection):
    # Ensure submissions point at canonical tank names before we add FK constraints.
    cur = await db.execute(
        """
        SELECT DISTINCT s.tank_name
        FROM submissions s
        LEFT JOIN tanks t ON t.name = s.tank_name
        WHERE t.name IS NULL
        """
    )
    orphan_names = [str(r[0]) for r in await cur.fetchall() if r and r[0]]
    await cur.close()
    for orphan_name in orphan_names:
        cur = await db.execute(
            "SELECT name FROM tanks WHERE name_norm = ? LIMIT 1",
            (utils.norm_tank_name(orphan_name),),
        )
        resolved = await cur.fetchone()
        await cur.close()
        if not resolved:
            continue
        canonical = str(resolved[0])
        await db.execute(
            "UPDATE submissions SET tank_name = ? WHERE tank_name = ?",
            (canonical, orphan_name),
        )

    cur = await db.execute(
        """
        SELECT s.tank_name, COUNT(*)
        FROM submissions s
        LEFT JOIN tanks t ON t.name = s.tank_name
        WHERE t.name IS NULL
        GROUP BY s.tank_name
        """
    )
    unresolved = await cur.fetchall()
    await cur.close()
    if unresolved:
        sample = ", ".join([f"{n}({c})" for n, c in unresolved[:5]])
        raise RuntimeError(
            "Cannot enforce FK integrity; submissions reference missing tanks: " + sample
        )

    await db.execute("PRAGMA foreign_keys = OFF;")
    try:
        await db.execute("ALTER TABLE tanks RENAME TO tanks_old")
        await db.execute("ALTER TABLE submissions RENAME TO submissions_old")

        await db.execute(
            """
            CREATE TABLE tanks (
                name TEXT PRIMARY KEY,
                name_norm TEXT NOT NULL UNIQUE,
                tier INTEGER NOT NULL,
                type TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name_raw TEXT NOT NULL,
                player_name_norm TEXT NOT NULL,
                tank_name TEXT NOT NULL,
                score INTEGER NOT NULL,
                submitted_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (tank_name) REFERENCES tanks(name)
                  ON UPDATE CASCADE
                  ON DELETE RESTRICT
            )
            """
        )
        await db.execute(
            """
            INSERT INTO tanks (name, name_norm, tier, type, created_at)
            SELECT name, name_norm, tier, type, created_at
            FROM tanks_old
            """
        )
        await db.execute(
            """
            INSERT INTO submissions (id, player_name_raw, player_name_norm, tank_name, score, submitted_by, created_at)
            SELECT id, player_name_raw, player_name_norm, tank_name, score, submitted_by, created_at
            FROM submissions_old
            """
        )
        await db.execute("DROP TABLE submissions_old")
        await db.execute("DROP TABLE tanks_old")

        # Recreate required indexes after table rebuild.
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_submissions_player_tank "
            "ON submissions (tank_name, player_name_norm)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_submissions_tank_score_id "
            "ON submissions (tank_name, score DESC, id ASC)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_tanks_tier_type_name "
            "ON tanks (tier, type, name)"
        )
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tanks_name_norm ON tanks(name_norm)")
    finally:
        await db.execute("PRAGMA foreign_keys = ON;")


async def _migration_009_add_clan_player_tracking(db: aiosqlite.Connection):
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS clan_players (
            region TEXT NOT NULL,
            account_id INTEGER NOT NULL,
            clan_id INTEGER NOT NULL,
            player_name_raw TEXT NOT NULL,
            player_name_norm TEXT NOT NULL,
            last_synced_at TEXT NOT NULL,
            PRIMARY KEY (region, account_id)
        )
        """
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_clan_players_region_name ON clan_players (region, player_name_raw)"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_clan_players_region_norm ON clan_players (region, player_name_norm)"
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

async def _run_migrations(db: aiosqlite.Connection):
    migrations = [
        (1, _migration_001_cleanup_submission_indexes),
        (2, _migration_002_backfill_tank_name_norm),
        (3, _migration_003_add_query_indexes),
        (4, _migration_004_add_score_change_indexes),
        (5, _migration_005_backfill_player_name_norm),
        (6, _migration_006_canonicalize_player_display_names),
        (7, _migration_007_add_tank_aliases_table),
        (8, _migration_008_enforce_tank_integrity),
        (9, _migration_009_add_clan_player_tracking),
    ]
    for version, fn in migrations:
        cur = await db.execute(
            "SELECT 1 FROM schema_migrations WHERE version = ?",
            (version,),
        )
        already_applied = await cur.fetchone()
        await cur.close()
        if already_applied:
            continue
        await fn(db)
        await db.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            (version, utils.utc_now_z()),
        )

async def init_db():
    async with _connect_db() as db:
        await db.executescript(SCHEMA)
        await _run_migrations(db)
        await db.commit()

async def get_tank(name: str):
    async with _connect_db() as db:
        cur = await db.execute("SELECT name, tier, type FROM tanks WHERE name_norm = ?", (utils.norm_tank_name(name),))
        return await cur.fetchone()

async def list_tanks(tier: int | None = None, ttype: str | None = None):
    q = "SELECT name, tier, type FROM tanks"
    args = []
    wh = []
    if tier is not None:
        wh.append("tier = ?")
        args.append(tier)
    if ttype is not None:
        wh.append("type = ?")
        args.append(ttype)
    if wh:
        q += " WHERE " + " AND ".join(wh)
    q += " ORDER BY tier DESC, type, name"
    async with _connect_db() as db:
        cur = await db.execute(q, tuple(args))
        return await cur.fetchall()

async def list_tank_names(query: str = "", limit: int = 25) -> list[str]:
    q = (query or "").strip()
    limit = max(1, min(limit, 25))
    async with _connect_db() as db:
        if q:
            cur = await db.execute(
                """
                SELECT name
                FROM tanks
                WHERE name LIKE ?
                ORDER BY tier DESC, type ASC, name ASC
                LIMIT ?
                """,
                (f"%{q}%", limit),
            )
        else:
            cur = await db.execute(
                """
                SELECT name
                FROM tanks
                ORDER BY tier DESC, type ASC, name ASC
                LIMIT ?
                """,
                (limit,),
            )
        rows = await cur.fetchall()
        await cur.close()
    return [str(r[0]) for r in rows if r and r[0]]

async def suggest_tank_names(tank_input: str, limit: int = 3) -> list[str]:
    candidate = (tank_input or "").strip()
    if not candidate:
        return []
    async with _connect_db() as db:
        cur = await db.execute(
            """
            SELECT name
            FROM tanks
            ORDER BY tier DESC, type ASC, name ASC
            LIMIT 500
            """
        )
        rows = await cur.fetchall()
        await cur.close()
    choices = [str(r[0]) for r in rows if r and r[0]]
    if not choices:
        return []
    exact_norm = utils.norm_tank_name(candidate)
    filtered = [c for c in choices if utils.norm_tank_name(c) != exact_norm]
    return difflib.get_close_matches(candidate, filtered, n=max(1, min(limit, 5)), cutoff=0.72)

async def upsert_tank_alias(alias_raw: str, tank_name: str, created_at: str):
    alias_norm = utils.norm_tank_name(alias_raw)
    async with _connect_db() as db:
        await db.execute(
            """
            INSERT INTO tank_aliases (alias_norm, alias_raw, tank_name, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(alias_norm) DO UPDATE SET
              alias_raw = excluded.alias_raw,
              tank_name = excluded.tank_name,
              created_at = excluded.created_at
            """,
            (alias_norm, str(alias_raw), str(tank_name), str(created_at)),
        )
        await db.commit()

async def list_tank_aliases(limit: int = 200):
    limit = max(1, min(limit, 500))
    async with _connect_db() as db:
        cur = await db.execute(
            """
            SELECT alias_raw, tank_name, created_at
            FROM tank_aliases
            ORDER BY alias_raw COLLATE NOCASE ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cur.fetchall()
        await cur.close()
    return [(str(r[0]), str(r[1]), str(r[2])) for r in rows]

async def _log_score_change_conn(
    conn: aiosqlite.Connection,
    *,
    action: str,
    submission_id: int | None,
    tank_name: str,
    player_name_raw: str,
    player_name_norm: str,
    old_score: int | None,
    new_score: int | None,
    actor: str,
    created_at: str,
    details: str = "",
):
    await conn.execute(
        """
        INSERT INTO score_changes (
            action, submission_id, tank_name, player_name_raw, player_name_norm,
            old_score, new_score, actor, created_at, details
        )
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            action,
            submission_id,
            tank_name,
            player_name_raw,
            player_name_norm,
            old_score,
            new_score,
            actor,
            created_at,
            details,
        ),
    )

async def clan_players_last_sync(region: str) -> str | None:
    key = f"wg:last_sync:{region.strip().lower()}"
    async with _connect_db() as conn:
        cur = await conn.execute(
            "SELECT value FROM sync_state WHERE key = ? LIMIT 1",
            (key,),
        )
        row = await cur.fetchone()
        await cur.close()
    return str(row[0]) if row and row[0] else None

async def get_sync_state(key: str) -> str | None:
    async with _connect_db() as conn:
        cur = await conn.execute(
            "SELECT value FROM sync_state WHERE key = ? LIMIT 1",
            (str(key),),
        )
        row = await cur.fetchone()
        await cur.close()
    return str(row[0]) if row and row[0] else None

async def set_sync_state(key: str, value: str, updated_at: str | None = None):
    ts = str(updated_at or utils.utc_now_z())
    async with _connect_db() as conn:
        await conn.execute(
            """
            INSERT INTO sync_state (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value = excluded.value,
              updated_at = excluded.updated_at
            """,
            (str(key), str(value), ts),
        )
        await conn.commit()

async def replace_clan_players(
    *,
    region: str,
    members: list[tuple[int, int, str]],
    synced_at: str,
) -> dict[str, object]:
    region_norm = region.strip().lower()
    deduped: dict[int, tuple[int, str, str]] = {}
    for account_id, clan_id, player_name_raw in members:
        try:
            raw = utils.validate_text("Player", str(player_name_raw), 64)
        except Exception:
            continue
        deduped[int(account_id)] = (int(clan_id), raw, utils.normalize_player(raw))

    async with _connect_db() as conn:
        cur = await conn.execute(
            """
            SELECT account_id, player_name_raw
            FROM clan_players
            WHERE region = ?
            """,
            (region_norm,),
        )
        existing_rows = await cur.fetchall()
        await cur.close()
        existing_by_id = {int(r[0]): str(r[1]) for r in existing_rows}

        incoming_ids = set(deduped.keys())
        existing_ids = set(existing_by_id.keys())
        added_ids = sorted(incoming_ids - existing_ids)
        removed_ids = sorted(existing_ids - incoming_ids)
        renamed = []
        for account_id in sorted(incoming_ids & existing_ids):
            old_name = existing_by_id[account_id]
            new_name = deduped[account_id][1]
            if old_name != new_name:
                renamed.append((old_name, new_name))

        if removed_ids:
            placeholders = ",".join("?" for _ in removed_ids)
            await conn.execute(
                f"DELETE FROM clan_players WHERE region = ? AND account_id IN ({placeholders})",
                (region_norm, *removed_ids),
            )

        if deduped:
            await conn.executemany(
                """
                INSERT INTO clan_players (
                    region, account_id, clan_id, player_name_raw, player_name_norm, last_synced_at
                )
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(region, account_id) DO UPDATE SET
                  clan_id = excluded.clan_id,
                  player_name_raw = excluded.player_name_raw,
                  player_name_norm = excluded.player_name_norm,
                  last_synced_at = excluded.last_synced_at
                """,
                [
                    (region_norm, account_id, clan_id, raw, norm, synced_at)
                    for account_id, (clan_id, raw, norm) in deduped.items()
                ],
            )

        key = f"wg:last_sync:{region_norm}"
        await conn.execute(
            """
            INSERT INTO sync_state (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value = excluded.value,
              updated_at = excluded.updated_at
            """,
            (key, synced_at, synced_at),
        )
        await conn.commit()

    added_names = sorted([deduped[i][1] for i in added_ids], key=str.casefold)
    removed_names = sorted([existing_by_id[i] for i in removed_ids], key=str.casefold)
    return {
        "total": len(deduped),
        "added_count": len(added_names),
        "removed_count": len(removed_names),
        "renamed_count": len(renamed),
        "added_names": added_names,
        "removed_names": removed_names,
        "renamed": renamed,
        "synced_at": synced_at,
    }

async def _list_player_names_from_submissions(query: str = "", limit: int = 25) -> list[str]:
    q = (query or "").strip()
    limit = max(1, min(limit, 500))
    async with _connect_db() as db:
        if q:
            cur = await db.execute(
                """
                WITH ranked AS (
                    SELECT
                        player_name_norm,
                        player_name_raw,
                        created_at,
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY player_name_norm
                            ORDER BY created_at DESC, id DESC
                        ) AS rn
                    FROM submissions
                )
                SELECT player_name_raw
                FROM ranked
                WHERE rn = 1 AND player_name_raw LIKE ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (f"%{q}%", limit),
            )
        else:
            cur = await db.execute(
                """
                WITH ranked AS (
                    SELECT
                        player_name_norm,
                        player_name_raw,
                        created_at,
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY player_name_norm
                            ORDER BY created_at DESC, id DESC
                        ) AS rn
                    FROM submissions
                )
                SELECT player_name_raw
                FROM ranked
                WHERE rn = 1
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            )
        rows = await cur.fetchall()
        await cur.close()
    return [str(r[0]) for r in rows if r and r[0]]

async def get_player_name_canonical(player_input: str) -> str:
    player_norm = utils.normalize_player(player_input)
    if not player_norm:
        return player_input
    async with _connect_db() as db:
        cur = await db.execute(
            """
            SELECT player_name_raw
            FROM clan_players
            WHERE region = ? AND player_name_norm = ?
            ORDER BY player_name_raw COLLATE NOCASE ASC
            LIMIT 1
            """,
            (config.WG_API_REGION, player_norm),
        )
        row = await cur.fetchone()
        await cur.close()
        if row and row[0]:
            return str(row[0])

        cur = await db.execute(
            """
            SELECT player_name_raw
            FROM submissions
            WHERE player_name_norm = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (player_norm,),
        )
        row = await cur.fetchone()
        await cur.close()
    return str(row[0]) if row and row[0] else player_input

async def suggest_player_names(player_input: str, limit: int = 3) -> list[str]:
    candidate = (player_input or "").strip()
    if not candidate:
        return []
    async with _connect_db() as db:
        cur = await db.execute(
            """
            SELECT player_name_raw
            FROM clan_players
            WHERE region = ?
            ORDER BY player_name_raw COLLATE NOCASE ASC
            LIMIT 500
            """,
            (config.WG_API_REGION,),
        )
        rows = await cur.fetchall()
        await cur.close()
    choices = [str(r[0]) for r in rows if r and r[0]]
    if not choices:
        choices = await _list_player_names_from_submissions(query="", limit=25)
    if not choices:
        return []

    exact_norm = utils.normalize_player(candidate)
    filtered = [c for c in choices if utils.normalize_player(c) != exact_norm]
    return difflib.get_close_matches(candidate, filtered, n=max(1, min(limit, 5)), cutoff=0.82)

async def list_player_names(query: str = "", limit: int = 25) -> list[str]:
    q = (query or "").strip()
    limit = max(1, min(limit, 25))
    async with _connect_db() as db:
        if q:
            cur = await db.execute(
                """
                SELECT player_name_raw
                FROM clan_players
                WHERE region = ? AND player_name_raw LIKE ?
                ORDER BY player_name_raw COLLATE NOCASE ASC
                LIMIT ?
                """,
                (config.WG_API_REGION, f"%{q}%", limit),
            )
        else:
            cur = await db.execute(
                """
                SELECT player_name_raw
                FROM clan_players
                WHERE region = ?
                ORDER BY player_name_raw COLLATE NOCASE ASC
                LIMIT ?
                """,
                (config.WG_API_REGION, limit),
            )
        rows = await cur.fetchall()
        await cur.close()
    names = [str(r[0]) for r in rows if r and r[0]]
    if names:
        return names
    return await _list_player_names_from_submissions(query=q, limit=limit)

async def canonical_player_name_map() -> dict[str, str]:
    async with _connect_db() as db:
        cur = await db.execute(
            """
            WITH ranked AS (
                SELECT
                    player_name_norm,
                    player_name_raw,
                    created_at,
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY player_name_norm
                        ORDER BY created_at DESC, id DESC
                    ) AS rn
                FROM submissions
            )
            SELECT player_name_norm, player_name_raw
            FROM ranked
            WHERE rn = 1
            """
        )
        rows = await cur.fetchall()
        await cur.close()
    return {
        str(r[0]): str(r[1])
        for r in rows
        if r and r[0] and r[1]
    }

async def insert_submission(
    player_raw: str,
    player_norm: str,
    tank_name: str,
    score: int,
    submitted_by: str,
    created_at: str):
    async with _connect_db() as conn:
        cur = await conn.execute(
            """
            SELECT id, score, player_name_raw
            FROM submissions
            WHERE tank_name = ? AND player_name_norm = ?
            LIMIT 1
            """,
            (tank_name, player_norm),
        )
        existing = await cur.fetchone()
        await cur.close()

        if existing is None:
            cur = await conn.execute(
                """
                INSERT INTO submissions (player_name_raw, player_name_norm, tank_name, score, submitted_by, created_at)
                VALUES (?,?,?,?,?,?)
                """,
                (player_raw, player_norm, tank_name, score, submitted_by, created_at),
            )
            submission_id = int(cur.lastrowid)
            await _log_score_change_conn(
                conn,
                action="add",
                submission_id=submission_id,
                tank_name=tank_name,
                player_name_raw=player_raw,
                player_name_norm=player_norm,
                old_score=None,
                new_score=score,
                actor=submitted_by,
                created_at=created_at,
            )
            await conn.commit()
            return {
                "status": "added",
                "submission_id": submission_id,
                "old_score": None,
                "new_score": score,
            }

        submission_id, existing_score, _existing_player_raw = int(existing[0]), int(existing[1]), str(existing[2])
        if score <= existing_score:
            return {
                "status": "ignored",
                "submission_id": submission_id,
                "old_score": existing_score,
                "new_score": score,
            }

        await conn.execute(
            """
            UPDATE submissions
            SET player_name_raw = ?, score = ?, submitted_by = ?, created_at = ?
            WHERE id = ?
            """,
            (player_raw, score, submitted_by, created_at, submission_id),
        )
        await _log_score_change_conn(
            conn,
            action="edit",
            submission_id=submission_id,
            tank_name=tank_name,
            player_name_raw=player_raw,
            player_name_norm=player_norm,
            old_score=existing_score,
            new_score=score,
            actor=submitted_by,
            created_at=created_at,
            details="upsert-higher-score",
        )
        await conn.commit()
        return {
            "status": "updated",
            "submission_id": submission_id,
            "old_score": existing_score,
            "new_score": score,
        }

async def get_best_for_tank(tank_name: str):
    async with _connect_db() as db:
        cur = await db.execute("""
        SELECT id, player_name_raw, score, created_at
        FROM submissions
        WHERE tank_name = ?
        ORDER BY score DESC, id ASC
        LIMIT 1;
        """, (tank_name,))
        return await cur.fetchone()

async def list_tanks_with_best_scores():
    """
    Return one row per tank with roster metadata and best score holder (if any).
    Row format: (tank_name, score|None, player_name|None, tier, type)
    """
    sql = """
    WITH ranked AS (
        SELECT
            s.tank_name,
            s.score,
            s.player_name_raw,
            ROW_NUMBER() OVER (
                PARTITION BY s.tank_name
                ORDER BY s.score DESC, s.id ASC
            ) AS rn
        FROM submissions s
    )
    SELECT
        t.name AS tank_name,
        r.score AS score,
        r.player_name_raw AS player_name,
        t.tier AS tier,
        t.type AS type
    FROM tanks t
    LEFT JOIN ranked r
      ON r.tank_name = t.name AND r.rn = 1
    ORDER BY t.tier ASC, t.type ASC, t.name ASC
    """
    async with _connect_db() as db:
        cur = await db.execute(sql)
        rows = await cur.fetchall()
        await cur.close()
        return rows

async def get_champion():
    async with _connect_db() as db:
        cur = await db.execute("""
        SELECT s.id, s.player_name_raw, s.tank_name, s.score,
               s.submitted_by, s.created_at, t.tier, t.type
        FROM submissions s
        JOIN tanks t ON t.name = s.tank_name
        ORDER BY s.score DESC, s.id ASC
        LIMIT 1;
        """)
        return await cur.fetchone()

async def get_recent(limit: int):
    async with _connect_db() as db:
        cur = await db.execute("""
        SELECT s.id, s.player_name_raw, s.tank_name, s.score,
               s.submitted_by, s.created_at, t.tier, t.type
        FROM submissions s
        JOIN tanks t ON t.name = s.tank_name
        ORDER BY s.id DESC
        LIMIT ?;
        """, (limit,))
        return await cur.fetchall()

async def top_holders_by_tank(limit: int = 10):
    limit = max(1, min(limit, 25))
    async with _connect_db() as db:
        cur = await db.execute("""
        WITH ranked AS (
            SELECT
                s.player_name_raw,
                s.player_name_norm,
                s.tank_name,
                s.score,
                s.id,
                ROW_NUMBER() OVER (
                    PARTITION BY s.tank_name
                    ORDER BY s.score DESC, s.id ASC
                ) AS rn
            FROM submissions s
        )
        SELECT player_name_raw, COUNT(*) AS tops
        FROM ranked
        WHERE rn = 1
        GROUP BY player_name_norm
        ORDER BY tops DESC, MIN(id) ASC
        LIMIT ?;
        """, (limit,))
        return await cur.fetchall()

async def top_holders_by_tier_type(limit: int = 10):
    limit = max(1, min(limit, 25))
    async with _connect_db() as db:
        cur = await db.execute("""
        WITH ranked AS (
            SELECT
                s.player_name_raw,
                s.player_name_norm,
                t.tier,
                t.type,
                s.score,
                s.id,
                ROW_NUMBER() OVER (
                    PARTITION BY t.tier, t.type
                    ORDER BY s.score DESC, s.id ASC
                ) AS rn
            FROM submissions s
            JOIN tanks t ON t.name = s.tank_name
        )
        SELECT player_name_raw, COUNT(*) AS tops
        FROM ranked
        WHERE rn = 1
        GROUP BY player_name_norm
        ORDER BY tops DESC, MIN(id) ASC
        LIMIT ?;
        """, (limit,))
        return await cur.fetchall()

async def stats_top_per_tier(limit_per_tier: int = 3):
    limit_per_tier = max(1, min(limit_per_tier, 10))
    async with _connect_db() as db:
        cur = await db.execute(
            """
            WITH ranked AS (
                SELECT
                    t.tier AS tier,
                    s.tank_name AS tank_name,
                    s.player_name_raw AS player_name,
                    s.score AS score,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.tier
                        ORDER BY s.score DESC, s.id ASC
                    ) AS rn
                FROM submissions s
                JOIN tanks t ON t.name = s.tank_name
            )
            SELECT tier, rn, tank_name, player_name, score
            FROM ranked
            WHERE rn <= ?
            ORDER BY tier DESC, rn ASC
            """,
            (limit_per_tier,),
        )
        return await cur.fetchall()

async def stats_most_recorded_tanks(limit: int = 10):
    limit = max(1, min(limit, 50))
    async with _connect_db() as db:
        cur = await db.execute(
            """
            SELECT tank_name, COUNT(*) AS submissions_count
            FROM submissions
            GROUP BY tank_name
            ORDER BY submissions_count DESC, tank_name ASC
            LIMIT ?
            """,
            (limit,),
        )
        return await cur.fetchall()

async def stats_unique_player_count() -> int:
    async with _connect_db() as db:
        cur = await db.execute(
            "SELECT COUNT(DISTINCT player_name_norm) FROM submissions"
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row[0] if row and row[0] is not None else 0)

async def stats_submissions_by_year():
    async with _connect_db() as db:
        cur = await db.execute(
            """
            SELECT SUBSTR(created_at, 1, 4) AS year_key, COUNT(*) AS submissions_count
            FROM submissions
            WHERE LENGTH(created_at) >= 4
            GROUP BY year_key
            ORDER BY year_key DESC
            """
        )
        return await cur.fetchall()

async def stats_submissions_by_month():
    async with _connect_db() as db:
        cur = await db.execute(
            """
            SELECT SUBSTR(created_at, 1, 7) AS month_key, COUNT(*) AS submissions_count
            FROM submissions
            WHERE LENGTH(created_at) >= 7
            GROUP BY month_key
            ORDER BY month_key DESC
            """
        )
        return await cur.fetchall()

async def counts():
    async with _connect_db() as db:
        cur = await db.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM tanks) AS tanks_count,
                (SELECT COUNT(*) FROM submissions) AS submissions_count,
                (SELECT COUNT(*) FROM tank_index_posts) AS index_count
            """
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return 0, 0, 0
        return int(row[0]), int(row[1]), int(row[2])

async def migration_version() -> int:
    async with _connect_db() as db:
        cur = await db.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
        row = await cur.fetchone()
        await cur.close()
        return int(row[0] if row and row[0] is not None else 0)

async def log_tank_change(action: str, details: str, actor: str, created_at: str):
    async with _connect_db() as db:
        await db.execute(
            "INSERT INTO tank_changes (action, details, actor, created_at) VALUES (?,?,?,?)",
            (action, details, actor, created_at),
        )
        await db.commit()

async def add_tank(name: str, tier: int, ttype: str, actor: str, created_at: str):
    async with _connect_db() as db:
        name_norm = utils.norm_tank_name(name)
        await db.execute(
            "INSERT INTO tanks (name, name_norm, tier, type, created_at) VALUES (?,?,?,?,?)",
            (name, name_norm, tier, ttype, created_at),
        )
        await db.commit()
    await log_tank_change("add", f"{name}|tier={tier}|type={ttype}", actor, created_at)

async def add_tanks_bulk(rows: list[tuple[str, int, str]], actor: str, created_at: str) -> tuple[int, int]:
    """
    Add many tanks in one transaction.
    Returns (added, skipped_existing_or_duplicate_in_payload).
    """
    if not rows:
        return 0, 0

    normalized_rows: list[tuple[str, str, int, str, str]] = []
    for name, tier, ttype in rows:
        normalized_rows.append(
            (
                str(name),
                utils.norm_tank_name(str(name)),
                int(tier),
                str(ttype),
                str(created_at),
            )
        )

    async with _connect_db() as db:
        cur = await db.execute("SELECT name_norm FROM tanks")
        existing = {str(r[0]) for r in await cur.fetchall()}
        await cur.close()

        to_insert: list[tuple[str, str, int, str, str]] = []
        seen_in_payload: set[str] = set()
        for row in normalized_rows:
            _name, name_norm, _tier, _ttype, _created = row
            if name_norm in existing or name_norm in seen_in_payload:
                continue
            seen_in_payload.add(name_norm)
            to_insert.append(row)

        if to_insert:
            await db.executemany(
                "INSERT INTO tanks (name, name_norm, tier, type, created_at) VALUES (?,?,?,?,?)",
                to_insert,
            )
            await db.executemany(
                "INSERT INTO tank_changes (action, details, actor, created_at) VALUES (?,?,?,?)",
                [
                    ("add", f"{name}|tier={tier}|type={ttype}", actor, created_at)
                    for (name, _name_norm, tier, ttype, _created_at) in to_insert
                ],
            )
            await db.commit()

    added = len(to_insert)
    skipped = len(rows) - added
    return added, skipped

async def edit_tank(
    name: str,
    tier: int,
    ttype: str,
    actor: str,
    created_at: str,
    new_name: str | None = None,
):
    existing = await get_tank(name)
    if not existing:
        raise ValueError("Tank not found.")
    old_name = str(existing[0])
    old_name_norm = utils.norm_tank_name(old_name)
    final_name = str(new_name or old_name).strip()
    final_name_norm = utils.norm_tank_name(final_name)

    async with _connect_db() as db:
        if final_name_norm != old_name_norm:
            cur = await db.execute(
                "SELECT name FROM tanks WHERE name_norm = ? LIMIT 1",
                (final_name_norm,),
            )
            conflict = await cur.fetchone()
            await cur.close()
            if conflict:
                raise ValueError("Another tank already exists with that name.")

        await db.execute(
            """
            UPDATE tanks
            SET tier = ?, type = ?, name = ?, name_norm = ?
            WHERE name_norm = ?
            """,
            (tier, ttype, final_name, final_name_norm, old_name_norm),
        )
        if final_name_norm != old_name_norm:
            await db.execute(
                """
                INSERT INTO tank_aliases (alias_norm, alias_raw, tank_name, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(alias_norm) DO UPDATE SET
                  alias_raw = excluded.alias_raw,
                  tank_name = excluded.tank_name,
                  created_at = excluded.created_at
                """,
                (old_name_norm, old_name, final_name, created_at),
            )
            await db.execute(
                "UPDATE tank_aliases SET tank_name = ? WHERE tank_name = ?",
                (final_name, old_name),
            )
        await db.commit()
    await log_tank_change(
        "edit",
        f"{old_name}->{final_name}|tier={tier}|type={ttype}",
        actor,
        created_at,
    )

async def tank_has_submissions(name: str) -> bool:
    async with _connect_db() as db:
        cur = await db.execute("SELECT 1 FROM submissions WHERE tank_name = ? LIMIT 1", (name,))
        return (await cur.fetchone()) is not None

async def remove_tank(name: str, actor: str, created_at: str):
    existing = await get_tank(name)
    canonical_name = str(existing[0]) if existing else str(name)
    if await tank_has_submissions(canonical_name):
        raise ValueError("Tank has submissions and cannot be removed.")
    async with _connect_db() as db:
        await db.execute("DELETE FROM tanks WHERE name_norm = ?", (utils.norm_tank_name(canonical_name),))
        await db.commit()
    await log_tank_change("remove", canonical_name, actor, created_at)

async def merge_tank_into(
    source_name: str,
    target_name: str,
    actor: str,
    created_at: str,
    remove_source: bool = True,
):
    """
    Merge source tank into target tank.
    - Moves submissions from source -> target.
    - Resolves player collisions by keeping higher score on target.
    - Creates/updates alias source -> target.
    - Optionally removes source tank from roster.
    """
    src = await get_tank(source_name)
    dst = await get_tank(target_name)
    if not src or not dst:
        return {"error": "tank_not_found"}

    src_name = str(src[0])
    src_tier = int(src[1])
    src_type = str(src[2])
    dst_name = str(dst[0])
    dst_tier = int(dst[1])
    dst_type = str(dst[2])

    moved = 0
    deleted = 0
    upgraded = 0

    async with _connect_db() as conn:
        cur = await conn.execute(
            """
            SELECT id, player_name_raw, player_name_norm, score, submitted_by, created_at
            FROM submissions
            WHERE tank_name = ?
            ORDER BY id ASC
            """,
            (src_name,),
        )
        src_rows = await cur.fetchall()
        await cur.close()

        for sid, player_raw, player_norm, src_score, submitted_by, sub_created in src_rows:
            sid = int(sid)
            player_raw = str(player_raw)
            player_norm = str(player_norm)
            src_score = int(src_score)
            submitted_by = str(submitted_by)
            sub_created = str(sub_created)

            cur = await conn.execute(
                """
                SELECT id, score
                FROM submissions
                WHERE tank_name = ? AND player_name_norm = ?
                LIMIT 1
                """,
                (dst_name, player_norm),
            )
            dst_row = await cur.fetchone()
            await cur.close()

            if not dst_row:
                await conn.execute(
                    "UPDATE submissions SET tank_name = ?, submitted_by = ?, created_at = ? WHERE id = ?",
                    (dst_name, actor, created_at, sid),
                )
                await _log_score_change_conn(
                    conn,
                    action="edit",
                    submission_id=sid,
                    tank_name=dst_name,
                    player_name_raw=player_raw,
                    player_name_norm=player_norm,
                    old_score=src_score,
                    new_score=src_score,
                    actor=actor,
                    created_at=created_at,
                    details=f"merge-tank-move:{src_name}->{dst_name}",
                )
                moved += 1
                continue

            dst_sid = int(dst_row[0])
            dst_score = int(dst_row[1])
            if src_score > dst_score:
                await conn.execute(
                    """
                    UPDATE submissions
                    SET player_name_raw = ?, score = ?, submitted_by = ?, created_at = ?
                    WHERE id = ?
                    """,
                    (player_raw, src_score, submitted_by, sub_created, dst_sid),
                )
                await _log_score_change_conn(
                    conn,
                    action="edit",
                    submission_id=dst_sid,
                    tank_name=dst_name,
                    player_name_raw=player_raw,
                    player_name_norm=player_norm,
                    old_score=dst_score,
                    new_score=src_score,
                    actor=actor,
                    created_at=created_at,
                    details=f"merge-tank-upgrade:{src_name}->{dst_name}",
                )
                upgraded += 1

            await conn.execute("DELETE FROM submissions WHERE id = ?", (sid,))
            await _log_score_change_conn(
                conn,
                action="delete",
                submission_id=sid,
                tank_name=src_name,
                player_name_raw=player_raw,
                player_name_norm=player_norm,
                old_score=src_score,
                new_score=None,
                actor=actor,
                created_at=created_at,
                details=f"merge-tank-delete-source:{src_name}->{dst_name}",
            )
            deleted += 1

        if remove_source:
            cur = await conn.execute("SELECT 1 FROM submissions WHERE tank_name = ? LIMIT 1", (src_name,))
            has_leftovers = await cur.fetchone()
            await cur.close()
            if not has_leftovers:
                await conn.execute("DELETE FROM tanks WHERE name_norm = ?", (utils.norm_tank_name(src_name),))

        await conn.execute(
            """
            INSERT INTO tank_aliases (alias_norm, alias_raw, tank_name, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(alias_norm) DO UPDATE SET
              alias_raw = excluded.alias_raw,
              tank_name = excluded.tank_name,
              created_at = excluded.created_at
            """,
            (utils.norm_tank_name(src_name), src_name, dst_name, created_at),
        )
        await conn.commit()

    await log_tank_change(
        "merge",
        f"{src_name}|tier={src_tier}|type={src_type} -> {dst_name}|tier={dst_tier}|type={dst_type}|moved={moved}|deleted={deleted}|upgraded={upgraded}|remove_source={remove_source}",
        actor,
        created_at,
    )
    return {
        "source": src_name,
        "target": dst_name,
        "moved": moved,
        "deleted": deleted,
        "upgraded": upgraded,
        "remove_source": bool(remove_source),
    }

async def tank_changes(limit: int = 25):
    limit = max(1, min(limit, 50))
    async with _connect_db() as db:
        cur = await db.execute(
            "SELECT id, action, details, actor, created_at FROM tank_changes ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return await cur.fetchall()

async def get_champion_filtered(tier: int | None = None, ttype: str | None = None):
    # If no filters, return global champion (same as get_champion)
    q = """
    SELECT s.id, s.player_name_raw, s.tank_name, s.score,
           s.submitted_by, s.created_at, t.tier, t.type
    FROM submissions s
    JOIN tanks t ON t.name = s.tank_name
    """
    args = []
    wh = []
    if tier is not None:
        wh.append("t.tier = ?")
        args.append(tier)
    if ttype is not None:
        wh.append("t.type = ?")
        args.append(ttype)
    if wh:
        q += " WHERE " + " AND ".join(wh)
    q += " ORDER BY s.score DESC, s.id ASC LIMIT 1;"
    async with _connect_db() as db:
        cur = await db.execute(q, tuple(args))
        return await cur.fetchone()

async def best_per_tank_for_bucket(tier: int, type_: str):
    sql = """
    WITH latest_change AS (
      SELECT
        sc.submission_id,
        sc.details,
        ROW_NUMBER() OVER (
          PARTITION BY sc.submission_id
          ORDER BY sc.id DESC
        ) AS rn
      FROM score_changes sc
      WHERE sc.submission_id IS NOT NULL
    ),
    ranked AS (
      SELECT
        s.*,
        ROW_NUMBER() OVER (
          PARTITION BY s.tank_name
          ORDER BY s.score DESC, s.created_at DESC
        ) AS rn
      FROM submissions s
    )
    SELECT
      t.name AS tank_name,
      t.tier AS tier,
      t.type AS type,
      r.score AS score,
      r.player_name_raw AS player_name,
      r.created_at AS created_at,
      CASE
        WHEN lc.details LIKE 'bulk-import%' THEN 1
        ELSE 0
      END AS is_imported
    FROM tanks t
    LEFT JOIN ranked r
      ON r.tank_name = t.name AND r.rn = 1
    LEFT JOIN latest_change lc
      ON lc.submission_id = r.id AND lc.rn = 1
    WHERE t.tier = ? AND t.type = ?
    ORDER BY (r.score IS NULL) ASC, r.score DESC, t.name ASC
    """
    async with _connect_db() as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(sql, (tier, type_))
        rows = await cur.fetchall()
        await cur.close()
    # Return as dicts to match your renderer expectations
    return [dict(r) for r in rows]

async def get_bucket_snapshot_rows(tier: int, ttype: str):
    # Backwards-compatible alias used by forum_index.py
    return await best_per_tank_for_bucket(tier, ttype)

async def get_index_thread_id(tier: int, type_: str) -> int | None:
    sql = "SELECT thread_id FROM tank_index_posts WHERE tier = ? AND type = ?"
    async with _connect_db() as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(sql, (tier, type_))
        row = await cur.fetchone()
        await cur.close()
    if not row:
        return None
    return int(row["thread_id"])


async def upsert_index_thread(tier: int, type_: str, thread_id: int, forum_channel_id: int):
    sql = """
    INSERT INTO tank_index_posts (tier, type, thread_id, forum_channel_id)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(tier, type) DO UPDATE SET
      thread_id = excluded.thread_id,
      forum_channel_id = excluded.forum_channel_id
    """
    async with _connect_db() as conn:
        await conn.execute(sql, (tier, type_, int(thread_id), int(forum_channel_id)))
        await conn.commit()

async def clear_index_threads():
    async with _connect_db() as conn:
        await conn.execute("DELETE FROM tank_index_posts")
        await conn.commit()

async def list_tier_type_buckets():
    sql = "SELECT DISTINCT tier, type FROM tanks ORDER BY tier DESC, type ASC"
    async with _connect_db() as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(sql)
        rows = await cur.fetchall()
        await cur.close()
    return [(int(r["tier"]), str(r["type"])) for r in rows]

async def list_index_mappings() -> set[tuple[int, str]]:
    sql = "SELECT tier, type FROM tank_index_posts"
    async with _connect_db() as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(sql)
        rows = await cur.fetchall()
        await cur.close()
    return {(int(r["tier"]), str(r["type"])) for r in rows}

async def get_tank_canonical(tank_input: str):
    """Return (canonical_name, tier, type) for any user-entered tank name (case-insensitive)."""
    tank_norm = utils.norm_tank_name(tank_input)
    sql = "SELECT name, tier, type FROM tanks WHERE name_norm = ?"
    async with _connect_db() as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(sql, (tank_norm,))
        row = await cur.fetchone()
        await cur.close()
        if row:
            return (str(row["name"]), int(row["tier"]), str(row["type"]))

        # alias fallback
        cur = await conn.execute(
            """
            SELECT t.name, t.tier, t.type
            FROM tank_aliases a
            JOIN tanks t ON t.name = a.tank_name
            WHERE a.alias_norm = ?
            LIMIT 1
            """,
            (tank_norm,),
        )
        row = await cur.fetchone()
        await cur.close()
    if row:
        return (str(row["name"]), int(row["tier"]), str(row["type"]))
    return None


async def get_submission_by_id(submission_id: int):
    async with _connect_db() as db:
        cur = await db.execute(
            """
            SELECT id, player_name_raw, player_name_norm, tank_name, score, submitted_by, created_at
            FROM submissions
            WHERE id = ?
            LIMIT 1
            """,
            (int(submission_id),),
        )
        row = await cur.fetchone()
        await cur.close()
        return row

async def edit_submission_score(
    submission_id: int,
    new_score: int,
    actor: str,
    created_at: str,
    new_player_raw: str | None = None,
    new_player_norm: str | None = None,
):
    async with _connect_db() as conn:
        cur = await conn.execute(
            """
            SELECT id, player_name_raw, player_name_norm, tank_name, score
            FROM submissions
            WHERE id = ?
            LIMIT 1
            """,
            (int(submission_id),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None

        sid = int(row[0])
        old_player_raw = str(row[1])
        old_player_norm = str(row[2])
        tank_name = str(row[3])
        old_score = int(row[4])
        player_raw = str(new_player_raw) if new_player_raw is not None else old_player_raw
        player_norm = str(new_player_norm) if new_player_norm is not None else old_player_norm

        try:
            await conn.execute(
                """
                UPDATE submissions
                SET player_name_raw = ?, player_name_norm = ?, score = ?, submitted_by = ?, created_at = ?
                WHERE id = ?
                """,
                (player_raw, player_norm, int(new_score), actor, created_at, sid),
            )
        except aiosqlite.IntegrityError:
            return {"error": "duplicate_player_for_tank", "tank_name": tank_name}

        details = "manual-edit"
        if player_raw != old_player_raw:
            details += f"|player:{old_player_raw}->{player_raw}"
        await _log_score_change_conn(
            conn,
            action="edit",
            submission_id=sid,
            tank_name=tank_name,
            player_name_raw=player_raw,
            player_name_norm=player_norm,
            old_score=old_score,
            new_score=int(new_score),
            actor=actor,
            created_at=created_at,
            details=details,
        )
        await conn.commit()
    return {
        "id": sid,
        "tank_name": tank_name,
        "old_player_raw": old_player_raw,
        "new_player_raw": player_raw,
        "old_score": old_score,
        "new_score": int(new_score),
    }

async def delete_submission(submission_id: int, actor: str, created_at: str, hard_delete: bool = False):
    async with _connect_db() as conn:
        cur = await conn.execute(
            """
            SELECT id, player_name_raw, player_name_norm, tank_name, score
            FROM submissions
            WHERE id = ?
            LIMIT 1
            """,
            (int(submission_id),),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None

        sid = int(row[0])
        player_raw = str(row[1])
        player_norm = str(row[2])
        tank_name = str(row[3])
        old_score = int(row[4])

        new_score: int | None = None
        details = "manual-delete-revert"
        if hard_delete:
            await conn.execute("DELETE FROM submissions WHERE id = ?", (sid,))
            details = "manual-delete-hard"
        else:
            # "Delete" acts as a score revert:
            # - if there is score history, restore the prior value
            # - otherwise set score to zero
            cur = await conn.execute(
                """
                SELECT old_score
                FROM score_changes
                WHERE submission_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (sid,),
            )
            prev = await cur.fetchone()
            await cur.close()
            new_score = 0
            if prev and prev[0] is not None:
                try:
                    new_score = int(prev[0])
                except Exception:
                    new_score = 0

            await conn.execute(
                "UPDATE submissions SET score = ?, submitted_by = ?, created_at = ? WHERE id = ?",
                (int(new_score), actor, created_at, sid),
            )
        await _log_score_change_conn(
            conn,
            action="delete",
            submission_id=sid,
            tank_name=tank_name,
            player_name_raw=player_raw,
            player_name_norm=player_norm,
            old_score=old_score,
            new_score=(None if hard_delete else int(new_score or 0)),
            actor=actor,
            created_at=created_at,
            details=details,
        )
        await conn.commit()
    return {
        "id": sid,
        "tank_name": tank_name,
        "old_score": old_score,
        "new_score": (None if hard_delete else int(new_score or 0)),
        "hard_delete": bool(hard_delete),
    }

async def score_changes(limit: int = 25):
    limit = max(1, min(limit, 100))
    async with _connect_db() as db:
        cur = await db.execute(
            """
            SELECT id, action, submission_id, tank_name, player_name_raw, old_score, new_score, actor, created_at, details
            FROM score_changes
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return await cur.fetchall()

async def insert_submissions_bulk(rows):
    """
    rows: list of tuples
      (player_raw, player_norm, tank_name, score, submitted_by, created_at)
    Returns: dict with attempted/added/updated/ignored counters.
    """
    if not rows:
        return {"attempted": 0, "added": 0, "updated": 0, "ignored": 0}
    added = 0
    updated = 0
    ignored = 0
    async with _connect_db() as conn:
        for player_raw, player_norm, tank_name, score, submitted_by, created_at in rows:
            cur = await conn.execute(
                """
                SELECT id, score, player_name_raw
                FROM submissions
                WHERE tank_name = ? AND player_name_norm = ?
                LIMIT 1
                """,
                (tank_name, player_norm),
            )
            existing = await cur.fetchone()
            await cur.close()

            if existing is None:
                cur = await conn.execute(
                    """
                    INSERT INTO submissions (
                        player_name_raw, player_name_norm, tank_name, score, submitted_by, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (player_raw, player_norm, tank_name, score, submitted_by, created_at),
                )
                submission_id = int(cur.lastrowid)
                await _log_score_change_conn(
                    conn,
                    action="add",
                    submission_id=submission_id,
                    tank_name=tank_name,
                    player_name_raw=player_raw,
                    player_name_norm=player_norm,
                    old_score=None,
                    new_score=int(score),
                    actor=submitted_by,
                    created_at=created_at,
                    details="bulk-import",
                )
                added += 1
                continue

            submission_id, existing_score, _existing_player_raw = int(existing[0]), int(existing[1]), str(existing[2])
            if int(score) <= existing_score:
                ignored += 1
                continue

            await conn.execute(
                """
                UPDATE submissions
                SET player_name_raw = ?, score = ?, submitted_by = ?, created_at = ?
                WHERE id = ?
                """,
                (player_raw, int(score), submitted_by, created_at, submission_id),
            )
            await _log_score_change_conn(
                conn,
                action="edit",
                submission_id=submission_id,
                tank_name=tank_name,
                player_name_raw=player_raw,
                player_name_norm=player_norm,
                old_score=existing_score,
                new_score=int(score),
                actor=submitted_by,
                created_at=created_at,
                details="bulk-import-higher-score",
            )
            updated += 1

        await conn.commit()

    return {"attempted": len(rows), "added": added, "updated": updated, "ignored": ignored}
