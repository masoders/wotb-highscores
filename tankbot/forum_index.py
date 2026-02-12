import discord
import re

from . import config, db, utils

_SNAPSHOT_MARKER = "_Snapshot page "
_SNAPSHOT_MARKER_RE = re.compile(r"(?m)^_?Snapshot page \d+/\d+_?$")


def _split_into_pages(header_lines: list[str], table_lines: list[str], footer_lines: list[str], max_len: int = 1900) -> list[str]:
    """
    Builds multiple Discord-safe message pages.
    - header_lines: shown on every page (with page marker added automatically)
    - table_lines: the table text (already formatted monospaced lines)
    - footer_lines: shown only on the LAST page
    """
    if max_len > 2000:
        max_len = 2000

    pages: list[str] = []
    footer_text = ("\n".join(footer_lines)).strip()
    footer_block = f"\n\n{footer_text}" if footer_text else ""

    # We'll include footer only on last page, so chunk without it first.
    base_header = "\n".join([l for l in header_lines if l is not None]).strip()

    i = 0
    while i < len(table_lines):
        # Page marker goes in header so we can identify/delete extra pages later
        # (and so users see it)
        page_header = base_header
        # We’ll temporarily write marker with unknown total pages, and fix later.
        page_header = (page_header + "\n\n" if page_header else "") + "TB_SNAPSHOT_PAGE"

        body_lines = []
        # reserve space for header + possible footer on last page (we handle later)
        current = page_header + "\n\n```text\n"
        remaining = max_len - len(current) - len("\n```")

        # Fill as many table lines as fit
        while i < len(table_lines):
            line = table_lines[i]
            need = len(line) + 1  # + newline
            if need > remaining:
                break
            body_lines.append(line)
            remaining -= need
            i += 1

        page_text = page_header + "\n\n```text\n" + "\n".join(body_lines) + "\n```"
        pages.append(page_text)

    # Add footer to last page if it fits, otherwise make a final footer-only page.
    if pages:
        last = pages[-1]
        if footer_block and (len(last) + len(footer_block) <= max_len):
            pages[-1] = last + footer_block
        elif footer_block:
            pages.append("TB_SNAPSHOT_PAGE\n\n" + footer_block)

    # Fix page markers with actual numbering
    total = len(pages)
    fixed: list[str] = []
    for idx, p in enumerate(pages, start=1):
        fixed.append(p.replace("TB_SNAPSHOT_PAGE", f"_Snapshot page {idx}/{total}_", 1))
    return fixed

async def upsert_bucket_thread(bot: discord.Client, tier: int, ttype: str):
    forum = bot.get_channel(config.TANK_INDEX_FORUM_CHANNEL_ID)
    if forum is None:
        forum = await bot.fetch_channel(config.TANK_INDEX_FORUM_CHANNEL_ID)

    # Optional but recommended: hard fail early if misconfigured
    if not isinstance(forum, discord.ForumChannel):
        raise TypeError("TANK_INDEX_FORUM_CHANNEL_ID must point to a Forum Channel")

    thread_id = await db.get_index_thread_id(tier, ttype)
    rows = await db.get_bucket_snapshot_rows(tier, ttype)
    pages = render_bucket_snapshot_pages(tier, ttype, rows)
    if not pages:
        pages = [f"Leaderboard — Tier {tier} / {utils.title_case_type(ttype)}"]

    # CREATE
    if not thread_id:
        thread_name = f"Leaderboard — Tier {tier} / {utils.title_case_type(ttype)}"
        thread = await forum.create_thread(name=thread_name, content=pages[0])
        thread_obj = thread.thread if hasattr(thread, "thread") else thread
        await db.upsert_index_thread(tier, ttype, thread_obj.id, forum.id)

        for p in pages[1:]:
            await thread_obj.send(p)
        return

    # UPDATE EXISTING
    thread = forum.get_thread(thread_id)
    if thread is None:
        thread = await bot.fetch_channel(thread_id)
    await _sync_snapshot_pages(bot, thread, pages, tier, ttype)

async def targeted_update(bot: discord.Client, tier: int, ttype: str):
    await upsert_bucket_thread(bot, tier, ttype)


async def rebuild_all(bot: discord.Client):
    # 1) Get the forum channel
    forum = bot.get_channel(config.TANK_INDEX_FORUM_CHANNEL_ID) or await bot.fetch_channel(config.TANK_INDEX_FORUM_CHANNEL_ID)

    # 2) Get all buckets from DB (distinct tier/type from tanks)
    buckets = await db.list_tier_type_buckets()  # you may need to implement or rename

    for tier, type_ in buckets:
        # 3) Get existing thread mapping (if any)
        thread_id = await db.get_index_thread_id(tier, type_)  # returns int | None

        # 4) Create thread if missing
        if not thread_id:
            title = f"Leaderboard — Tier {tier} / {type_.replace('td', 'Tank Destroyer').title()}"
            # Create a forum post/thread
            thread = await forum.create_thread(
                name=title,
                content="(initializing…)",  # will be replaced by snapshot edit
            )
            # discord.py returns ThreadWithMessage or similar depending on version
            # Make sure we end up with a Thread object and an id:
            thread_obj = thread.thread if hasattr(thread, "thread") else thread
            thread_id = thread_obj.id

            await db.upsert_index_thread(tier, type_, thread_id, config.TANK_INDEX_FORUM_CHANNEL_ID)

        # 5) Now thread_id is guaranteed, update snapshot
        await update_bucket_thread_snapshot(
            bot,
            forum_channel_id=config.TANK_INDEX_FORUM_CHANNEL_ID,
            thread_id=thread_id,
            tier=tier,
            type_=type_,
        )

async def rebuild_missing(bot: discord.Client):
    # Only ensure mappings exist for current tiers/types. If missing mapping, create.
    tanks = await db.list_tanks()
    mappings = await db.list_index_mappings()
    types = sorted({t[2] for t in tanks})
    tiers = sorted({int(t[1]) for t in tanks})
    for ttype in types:
        for tier in tiers:
            if (int(tier), str(ttype)) not in mappings:
                await upsert_bucket_thread(bot, tier, ttype)

def _sorted_snapshot_rows(rows: list[dict]) -> list[dict]:
    return sorted(
        rows,
        key=lambda r: str(r.get("tank_name") or "").casefold(),
    )

async def update_bucket_thread_snapshot(bot: discord.Client, forum_channel_id: int, thread_id: int, tier: int, type_: str):
    forum = bot.get_channel(forum_channel_id)
    if forum is None:
        forum = await bot.fetch_channel(forum_channel_id)

    thread = forum.get_thread(thread_id)
    if thread is None:
        thread = await bot.fetch_channel(thread_id)

    rows = await db.get_bucket_snapshot_rows(tier, type_)
    pages = render_bucket_snapshot_pages(tier, type_, rows)
    await _sync_snapshot_pages(bot, thread, pages, tier, type_)

async def _resolve_starter_message(thread: discord.Thread) -> discord.Message | None:
    starter_id = getattr(thread, "starter_message_id", None)
    if starter_id:
        try:
            return await thread.fetch_message(starter_id)
        except Exception:
            pass
    async for msg in thread.history(limit=1, oldest_first=True):
        return msg
    return None

def _is_snapshot_page_message(msg: discord.Message) -> bool:
    content = msg.content or ""
    if content.startswith(_SNAPSHOT_MARKER):
        return True
    # Snapshot marker is embedded as a standalone line after the header.
    return bool(_SNAPSHOT_MARKER_RE.search(content))

async def _recent_snapshot_page_messages(
    thread: discord.Thread,
    *,
    bot_user_id: int,
    starter_id: int,
    desired_extra_pages: int,
) -> list[discord.Message]:
    # Bounded scan: enough slack to catch stale pages without walking full history.
    history_limit = max(100, min(500, desired_extra_pages * 20 + 100))
    found: list[discord.Message] = []
    async for msg in thread.history(limit=history_limit):
        if msg.id == starter_id:
            continue
        if not msg.author or msg.author.id != bot_user_id:
            continue
        if not _is_snapshot_page_message(msg):
            continue
        found.append(msg)
    found.reverse()  # oldest -> newest
    return found

async def _sync_snapshot_pages(
    bot: discord.Client,
    thread: discord.Thread,
    pages: list[str],
    tier: int,
    ttype: str,
):
    if not pages:
        pages = [f"Leaderboard - Tier {tier} / {utils.title_case_type(ttype)}"]

    starter = await _resolve_starter_message(thread)
    if starter is None:
        starter = await thread.send(pages[0])
    elif starter.content != pages[0]:
        await starter.edit(content=pages[0])

    if bot.user is None:
        return

    desired_extra = pages[1:]
    existing_extra = await _recent_snapshot_page_messages(
        thread,
        bot_user_id=bot.user.id,
        starter_id=starter.id,
        desired_extra_pages=len(desired_extra),
    )

    keep_n = min(len(existing_extra), len(desired_extra))
    for idx in range(keep_n):
        msg = existing_extra[idx]
        new_content = desired_extra[idx]
        if msg.content != new_content:
            try:
                await msg.edit(content=new_content)
            except Exception:
                pass

    for msg in existing_extra[keep_n:]:
        try:
            await msg.delete()
        except Exception:
            pass

    for content in desired_extra[keep_n:]:
        await thread.send(content)

def render_bucket_snapshot_pages(tier: int, type_: str, rows: list[dict]) -> list[str]:
    title = f"**{utils.title_case_type(type_)} — Tier {tier}**"
    rows = _sorted_snapshot_rows(rows)

    # Build "best per tank" list from rows
    scored = [r for r in rows if r.get("score") is not None]
    latest = max(scored, key=lambda r: r.get("created_at", ""), default=None)
    top = max(scored, key=lambda r: r.get("score", 0), default=None)

    latest_line = "Latest: —"
    if latest:
        latest_line = f"Latest: {latest['tank_name']}  {latest['score']}  {latest['player_name']}  {utils.fmt_utc(latest['created_at'])}"

    top_line = "Top:    —"
    if top:
        top_line = f"Top:    {top['tank_name']}  {top['score']}  {top['player_name']}"

    header_lines = [
        title,
        latest_line,
        top_line,
    ]

    # Build monospaced table lines (header + rows)
    table_header = ["TANK", "BEST", "PLAYER", "WHEN"]
    table_rows: list[list[str]] = [table_header]

    for r in rows:
        tank = r.get("tank_name") or "—"
        score = r.get("score")
        player = r.get("player_name") or "—"
        when = utils.fmt_utc(r["created_at"]) if r.get("created_at") else "—"
        table_rows.append([
            str(tank),
            "—" if score is None else str(score),
            str(player),
            str(when),
        ])

    # Column widths: tune these if you want prettier
    widths = [28, 6, 20, 20]
    table_text = utils.fmt_table(table_rows, widths)
    table_lines = table_text.splitlines()

    footer_lines = ["Snapshot: best score per tank in this Tier×Type bucket."]

    return _split_into_pages(header_lines, table_lines, footer_lines, max_len=1900)

def render_bucket_snapshot(tier: int, type_: str, rows: list[dict]) -> str:
    type_label = {"light":"Light","medium":"Medium","heavy":"Heavy","td":"Tank Destroyer"}.get(type_, type_.title())
    rows = _sorted_snapshot_rows(rows)

    header_lines = [f"{type_label} — Tier {tier}"]

    scored = [r for r in rows if r.get("score") is not None]
    if scored:
        latest = max(scored, key=lambda r: r.get("created_at") or "")
        top = max(scored, key=lambda r: int(r.get("score") or 0))

        latest_when = utils.fmt_utc(latest.get("created_at"))
        header_lines.append(
            f"Latest: {latest['tank_name']}  {latest['score']}  {utils.clip(latest.get('player_name') or '—', 16)}  {latest_when}"
        )
        header_lines.append(
            f"Top:    {top['tank_name']}  {top['score']}  {utils.clip(top.get('player_name') or '—', 16)}"
        )

    # Build table rows (strings)
    table_rows = []
    table_rows.append(["TANK", "BEST", "PLAYER", "WHEN"])
    table_rows.append(["-"*28, "-"*6, "-"*20, "-"*20])

    for r in rows:
        tank = r["tank_name"]
        if r.get("score") is None:
            table_rows.append([tank, "—", "—", "—"])
        else:
            when = utils.fmt_utc(r.get("created_at"))
            player = utils.clip(r.get("player_name") or "—", 20)
            table_rows.append([tank, str(r["score"]), player, when])

    # Column widths (tweak to taste)
    widths = [28, 6, 20, 20]
    table_text = utils.fmt_table(table_rows, widths)

    # Wrap in code block for monospace rendering
    body = "```text\n" + table_text + "\n```"

    footer_lines = ["_Snapshot: best score per tank in this Tier×Type bucket._"]

    # Use your length limiter but now counting codeblock text
    return utils.build_snapshot_text(header_lines, [body], footer_lines)
