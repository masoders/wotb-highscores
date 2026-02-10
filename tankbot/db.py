import aiosqlite
from . import config, utils

SCHEMA = """
CREATE TABLE IF NOT EXISTS tanks (
    name TEXT NOT NULL,
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
    created_at TEXT NOT NULL
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

CREATE TABLE IF NOT EXISTS tank_index_posts (
    tier INTEGER NOT NULL,
    type TEXT NOT NULL,
    thread_id INTEGER NOT NULL,
    forum_channel_id INTEGER NOT NULL,
    PRIMARY KEY (tier, type)
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

async def _run_migrations(db: aiosqlite.Connection):
    migrations = [
        (1, _migration_001_cleanup_submission_indexes),
        (2, _migration_002_backfill_tank_name_norm),
        (3, _migration_003_add_query_indexes),
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
    async with aiosqlite.connect(config.DB_PATH) as db:
        await _apply_sqlite_pragmas(db)
        await db.executescript(SCHEMA)
        await _run_migrations(db)
        await db.commit()

async def get_tank(name: str):
    async with aiosqlite.connect(config.DB_PATH) as db:
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
    async with aiosqlite.connect(config.DB_PATH) as db:
        cur = await db.execute(q, tuple(args))
        return await cur.fetchall()

async def insert_submission(
    player_raw: str,
    player_norm: str,
    tank_name: str,
    score: int,
    submitted_by: str,
    created_at: str):
    async with aiosqlite.connect(config.DB_PATH) as conn:
        await conn.execute(
            """
            INSERT INTO submissions (player_name_raw, player_name_norm, tank_name, score, submitted_by, created_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(tank_name, player_name_norm) DO UPDATE SET
                player_name_raw = excluded.player_name_raw,
                score = excluded.score,
                submitted_by = excluded.submitted_by,
                created_at = excluded.created_at
            WHERE excluded.score > submissions.score
            """,
            (player_raw, player_norm, tank_name, score, submitted_by, created_at),
        )
        await conn.commit()

async def get_best_for_tank(tank_name: str):
    async with aiosqlite.connect(config.DB_PATH) as db:
        cur = await db.execute("""
        SELECT id, player_name_raw, score, created_at
        FROM submissions
        WHERE tank_name = ?
        ORDER BY score DESC, id ASC
        LIMIT 1;
        """, (tank_name,))
        return await cur.fetchone()

async def get_champion():
    async with aiosqlite.connect(config.DB_PATH) as db:
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
    async with aiosqlite.connect(config.DB_PATH) as db:
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
    async with aiosqlite.connect(config.DB_PATH) as db:
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
    async with aiosqlite.connect(config.DB_PATH) as db:
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

async def counts():
    async with aiosqlite.connect(config.DB_PATH) as db:
        c1 = await (await db.execute("SELECT COUNT(*) FROM tanks")).fetchone()
        c2 = await (await db.execute("SELECT COUNT(*) FROM submissions")).fetchone()
        c3 = await (await db.execute("SELECT COUNT(*) FROM tank_index_posts")).fetchone()
        return int(c1[0]), int(c2[0]), int(c3[0])

async def migration_version() -> int:
    async with aiosqlite.connect(config.DB_PATH) as db:
        cur = await db.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
        row = await cur.fetchone()
        await cur.close()
        return int(row[0] if row and row[0] is not None else 0)

async def log_tank_change(action: str, details: str, actor: str, created_at: str):
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute(
            "INSERT INTO tank_changes (action, details, actor, created_at) VALUES (?,?,?,?)",
            (action, details, actor, created_at),
        )
        await db.commit()

async def add_tank(name: str, tier: int, ttype: str, actor: str, created_at: str):
    async with aiosqlite.connect(config.DB_PATH) as db:
        name_norm = utils.norm_tank_name(name)
        await db.execute(
            "INSERT INTO tanks (name, name_norm, tier, type, created_at) VALUES (?,?,?,?,?)",
            (name, name_norm, tier, ttype, created_at),
        )
        await db.commit()
    await log_tank_change("add", f"{name}|tier={tier}|type={ttype}", actor, created_at)

async def edit_tank(name: str, tier: int, ttype: str, actor: str, created_at: str):
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute("UPDATE tanks SET tier = ?, type = ?, name = ? WHERE name_norm = ?",(tier, ttype, name, utils.norm_tank_name(name)),)
        await db.commit()
    await log_tank_change("edit", f"{name}|tier={tier}|type={ttype}", actor, created_at)

async def tank_has_submissions(name: str) -> bool:
    async with aiosqlite.connect(config.DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM submissions WHERE tank_name = ? LIMIT 1", (name,))
        return (await cur.fetchone()) is not None

async def remove_tank(name: str, actor: str, created_at: str):
    if await tank_has_submissions(name):
        raise ValueError("Tank has submissions and cannot be removed.")
    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.execute("DELETE FROM tanks WHERE name_norm = ?", (utils.norm_tank_name(name),))
        await db.commit()
    await log_tank_change("remove", f"{name}", actor, created_at)

async def tank_changes(limit: int = 25):
    limit = max(1, min(limit, 50))
    async with aiosqlite.connect(config.DB_PATH) as db:
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
    async with aiosqlite.connect(config.DB_PATH) as db:
        cur = await db.execute(q, tuple(args))
        return await cur.fetchone()

async def best_per_tank_for_bucket(tier: int, type_: str):
    sql = """
    WITH ranked AS (
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
      r.created_at AS created_at
    FROM tanks t
    LEFT JOIN ranked r
      ON r.tank_name = t.name AND r.rn = 1
    WHERE t.tier = ? AND t.type = ?
    ORDER BY (r.score IS NULL) ASC, r.score DESC, t.name ASC
    """
    async with aiosqlite.connect(config.DB_PATH) as conn:
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
    async with aiosqlite.connect(config.DB_PATH) as conn:
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
    async with aiosqlite.connect(config.DB_PATH) as conn:
        await conn.execute(sql, (tier, type_, int(thread_id), int(forum_channel_id)))
        await conn.commit()

async def list_tier_type_buckets():
    sql = "SELECT DISTINCT tier, type FROM tanks ORDER BY tier DESC, type ASC"
    async with aiosqlite.connect(config.DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(sql)
        rows = await cur.fetchall()
        await cur.close()
    return [(int(r["tier"]), str(r["type"])) for r in rows]

async def get_tank_canonical(tank_input: str):
    """Return (canonical_name, tier, type) for any user-entered tank name (case-insensitive)."""
    tank_norm = utils.norm_tank_name(tank_input)
    sql = "SELECT name, tier, type FROM tanks WHERE name_norm = ?"
    async with aiosqlite.connect(config.DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(sql, (tank_norm,))
        row = await cur.fetchone()
        await cur.close()
    if not row:
        return None
    return (str(row["name"]), int(row["tier"]), str(row["type"]))


async def insert_submissions_bulk(rows):
    """
    rows: list of tuples
      (player_raw, player_norm, tank_name, score, submitted_by, created_at)
    Returns: number of rows attempted (not necessarily inserted/updated).
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO submissions (
        player_name_raw,
        player_name_norm,
        tank_name,
        score,
        submitted_by,
        created_at
    )
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(tank_name, player_name_norm)
    DO UPDATE SET
        player_name_raw = excluded.player_name_raw,
        score = excluded.score,
        submitted_by = excluded.submitted_by,
        created_at = excluded.created_at
    WHERE excluded.score > submissions.score;
    """

    async with aiosqlite.connect(config.DB_PATH) as db:
        await db.executemany(sql, rows)
        await db.commit()

    return len(rows)
