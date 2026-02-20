import discord
import re
from datetime import datetime, timezone

from . import config, db, utils

_SNAPSHOT_MARKER = "_Snapshot page "
_SNAPSHOT_MARKER_RE = re.compile(r"(?m)^_?Snapshot page \d+/\d+_?$")
_SNAPSHOT_PAGE_1_RE = re.compile(r"(?m)^_?Snapshot page 1/\d+_?$")
_INDEX_TITLE_RE = re.compile(r"(?m)^\*\*.+ — Tier \d+\*\*$")
_TIER_SEPARATOR_MARKER = "_TB_TIER_SEPARATOR_"
_TIER_SEPARATOR_TIER_RE = re.compile(r"\bTier\s+(\d+)\b")
_TIER_SEPARATOR_LINE_RE = re.compile(r"(?im)^\s*[─━-]+\s*Tier\s+(\d+)\s*[─━-]+\s*$")
_FORUM_TIER_SEPARATOR_NAME_RE = re.compile(r"^[-─━]+\s*Tier\s+(\d+)\s*[-─━]+$")


def _bucket_title(tier: int, ttype: str) -> str:
    return f"**{utils.title_case_type(ttype)} — Tier {tier}**"


def _has_forum_index() -> bool:
    return config.TANK_INDEX_FORUM_CHANNEL_ID > 0


def _has_normal_index() -> bool:
    return config.TANK_INDEX_NORMAL_CHANNEL_ID > 0


def _ensure_index_configured():
    if _has_forum_index() or _has_normal_index():
        return
    raise RuntimeError(
        "Index channel is not configured. Set TANK_INDEX_NORMAL_CHANNEL_ID or TANK_INDEX_FORUM_CHANNEL_ID."
    )


async def _resolve_forum_channel(bot: discord.Client) -> discord.ForumChannel:
    forum = bot.get_channel(config.TANK_INDEX_FORUM_CHANNEL_ID)
    if forum is None:
        forum = await bot.fetch_channel(config.TANK_INDEX_FORUM_CHANNEL_ID)
    if not isinstance(forum, discord.ForumChannel):
        raise TypeError("TANK_INDEX_FORUM_CHANNEL_ID must point to a Forum Channel")
    return forum


async def _resolve_normal_channel(bot: discord.Client) -> discord.TextChannel:
    channel = bot.get_channel(config.TANK_INDEX_NORMAL_CHANNEL_ID)
    if channel is None:
        channel = await bot.fetch_channel(config.TANK_INDEX_NORMAL_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        raise TypeError("TANK_INDEX_NORMAL_CHANNEL_ID must point to a Text Channel")
    return channel


def _safe_text(value: object, *, fallback: str = "—") -> str:
    raw = str(value) if value is not None else fallback
    if not raw:
        raw = fallback
    return discord.utils.escape_mentions(raw)


def _safe_inline_text(value: object, *, fallback: str = "—") -> str:
    raw = _safe_text(value, fallback=fallback)
    # Header lines are outside code blocks, so escape markdown control chars.
    return discord.utils.escape_markdown(raw, as_needed=False)


def _fmt_local(iso: str | None) -> str:
    if not iso:
        return "—"
    s = str(iso).strip()
    while s.endswith("ZZ"):
        s = s[:-1]
    try:
        ts = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        local_tz = datetime.now().astimezone().tzinfo
        if local_tz is not None:
            ts = ts.astimezone(local_tz)
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        raw = str(iso).strip()
        raw = re.sub(r"(?<=\d)T(?=\d)", " ", raw)
        raw = re.sub(r"\s*UTC\b", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"(?:\+00:00|Z)\b", "", raw)
        return raw.strip() or "—"


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
    _ensure_index_configured()
    if _has_forum_index():
        await _upsert_bucket_forum_thread(bot, tier, ttype)
    if _has_normal_index():
        await upsert_bucket_message(bot, tier, ttype)


async def _upsert_bucket_forum_thread(bot: discord.Client, tier: int, ttype: str):
    forum = await _resolve_forum_channel(bot)

    thread_id = await db.get_index_thread_id(tier, ttype)
    rows = await db.get_bucket_snapshot_rows(tier, ttype)
    pages = render_bucket_snapshot_pages(tier, ttype, rows)
    if not pages:
        pages = [f"Leaderboard — Tier {tier} / {utils.title_case_type(ttype)}"]

    # CREATE
    if not thread_id:
        thread_name = f"Leaderboard — Tier {tier} / {utils.title_case_type(ttype)}"
        try:
            thread = await forum.create_thread(
                name=thread_name,
                content=pages[0],
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except TypeError:
            # Compatibility with discord.py variants that don't accept allowed_mentions here.
            thread = await forum.create_thread(name=thread_name, content=pages[0])
        thread_obj = thread.thread if hasattr(thread, "thread") else thread
        await db.upsert_index_thread(tier, ttype, thread_obj.id, forum.id)

        for p in pages[1:]:
            await thread_obj.send(p, allowed_mentions=discord.AllowedMentions.none())
        return

    # UPDATE EXISTING
    thread = forum.get_thread(thread_id)
    if thread is None:
        thread = await bot.fetch_channel(thread_id)
    await _sync_snapshot_pages(bot, thread, pages, tier, ttype)

async def targeted_update(bot: discord.Client, tier: int, ttype: str):
    await upsert_bucket_thread(bot, tier, ttype)


async def rebuild_all(bot: discord.Client):
    _ensure_index_configured()

    normal_channel: discord.TextChannel | None = None
    forum: discord.ForumChannel | None = None
    if _has_normal_index():
        normal_channel = await _resolve_normal_channel(bot)
        if bot.user is not None:
            async for msg in normal_channel.history(limit=5000):
                if not msg.author or msg.author.id != bot.user.id:
                    continue
                try:
                    await msg.delete()
                except Exception:
                    pass

    if _has_forum_index():
        forum = await _resolve_forum_channel(bot)
        # 1) Delete all existing forum threads/posts (active + archived) in this index forum
        seen_ids: set[int] = set()
        for th in list(getattr(forum, "threads", []) or []):
            try:
                await th.delete()
                seen_ids.add(int(th.id))
            except Exception:
                pass
        try:
            async for th in forum.archived_threads(limit=1000):
                if int(th.id) in seen_ids:
                    continue
                try:
                    await th.delete()
                    seen_ids.add(int(th.id))
                except Exception:
                    pass
        except Exception:
            pass
        # DB mappings are forum-thread mappings.
        await db.clear_index_threads()

    buckets = await db.list_tier_type_buckets()
    if normal_channel is not None:
        last_tier: int | None = None
        for tier, type_ in buckets:
            if last_tier != int(tier):
                await normal_channel.send(
                    _tier_separator_text(int(tier)),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                last_tier = int(tier)
            await upsert_bucket_message(bot, int(tier), str(type_))

    if forum is not None:
        # Forum channels show newest threads first. Create threads in reverse bucket
        # order and add per-tier separator threads so visible order is:
        # Tier N: Light, Medium, Heavy, TD, then Tier N-1, ...
        forum_buckets = list(reversed([(int(tier), str(type_)) for tier, type_ in buckets]))
        prev_tier: int | None = None
        for tier, type_ in forum_buckets:
            if prev_tier is not None and tier != prev_tier:
                await _create_forum_tier_separator_thread(forum, prev_tier)
            await _upsert_bucket_forum_thread(bot, int(tier), str(type_))
            prev_tier = int(tier)
        if prev_tier is not None:
            await _create_forum_tier_separator_thread(forum, prev_tier)

async def rebuild_missing(bot: discord.Client):
    # Ensure index snapshots exist/are refreshed in every configured destination.
    normal_channel: discord.TextChannel | None = None
    separator_tiers: set[int] = set()
    if _has_normal_index():
        normal_channel = await _resolve_normal_channel(bot)
        if bot.user is not None:
            separator_tiers = await _existing_tier_separators_in_channel(
                normal_channel,
                bot_user_id=bot.user.id,
            )

    buckets = await db.list_tier_type_buckets()
    for tier, type_ in buckets:
        tier_int = int(tier)
        if normal_channel is not None:
            if tier_int not in separator_tiers:
                await normal_channel.send(
                    _tier_separator_text(tier_int),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                separator_tiers.add(tier_int)
            await upsert_bucket_message(bot, int(tier), str(type_))

    if _has_forum_index():
        forum = await _resolve_forum_channel(bot)
        for tier, type_ in buckets:
            await _upsert_bucket_forum_thread(bot, int(tier), str(type_))
        forum_separator_tiers = await _existing_forum_separator_tiers(forum)
        for tier in sorted({int(t) for t, _tp in buckets}):
            if int(tier) in forum_separator_tiers:
                continue
            await _create_forum_tier_separator_thread(forum, int(tier))

def _sorted_snapshot_rows(rows: list[dict]) -> list[dict]:
    return sorted(
        rows,
        key=lambda r: str(r.get("tank_name") or "").casefold(),
    )

async def update_bucket_thread_snapshot(bot: discord.Client, forum_channel_id: int, thread_id: int, tier: int, type_: str):
    channel = bot.get_channel(forum_channel_id)
    if channel is None:
        channel = await bot.fetch_channel(forum_channel_id)
    rows = await db.get_bucket_snapshot_rows(tier, type_)
    pages = render_bucket_snapshot_pages(tier, type_, rows)
    if isinstance(channel, discord.ForumChannel):
        thread = channel.get_thread(thread_id)
        if thread is None:
            thread = await bot.fetch_channel(thread_id)
        await _sync_snapshot_pages(bot, thread, pages, tier, type_)
        return
    if isinstance(channel, discord.TextChannel):
        try:
            starter = await channel.fetch_message(thread_id)
        except Exception:
            starter = None
        await _sync_snapshot_pages_in_channel(bot, channel, starter, pages, tier, type_)
        return
    raise TypeError("Index channel mapping points to unsupported channel type")

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


def _is_bucket_snapshot_message(msg: discord.Message, tier: int, ttype: str) -> bool:
    content = msg.content or ""
    return _bucket_title(tier, ttype) in content and _is_snapshot_page_message(msg)


def _is_bucket_starter_message(msg: discord.Message, tier: int, ttype: str) -> bool:
    content = msg.content or ""
    if _bucket_title(tier, ttype) not in content:
        return False
    return bool(_SNAPSHOT_PAGE_1_RE.search(content))


def _is_index_snapshot_message(msg: discord.Message) -> bool:
    content = msg.content or ""
    return _is_snapshot_page_message(msg) and bool(_INDEX_TITLE_RE.search(content))


def _tier_separator_text(tier: int) -> str:
    return f"━━━━━━━━━━━━━━━━━━ Tier {int(tier)} ━━━━━━━━━━━━━━━━━━"


def _is_tier_separator_message(msg: discord.Message) -> bool:
    return _tier_from_separator_content(msg.content or "") is not None


def _tier_from_separator_content(content: str) -> int | None:
    raw = str(content or "")
    # Backward compatibility for old separator messages that include marker text.
    if _TIER_SEPARATOR_MARKER in raw:
        match = _TIER_SEPARATOR_TIER_RE.search(raw)
        if not match:
            return None
        try:
            return int(match.group(1))
        except Exception:
            return None

    match = _TIER_SEPARATOR_LINE_RE.search(raw)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _forum_tier_separator_name(tier: int) -> str:
    return f"──────── Tier {int(tier)} ────────"


def _tier_from_forum_separator_name(name: str) -> int | None:
    match = _FORUM_TIER_SEPARATOR_NAME_RE.match(str(name or "").strip())
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


async def _create_forum_tier_separator_thread(forum: discord.ForumChannel, tier: int):
    name = _forum_tier_separator_name(int(tier))
    # Forum threads require starter content; use an invisible char so only
    # the separator title is visible in the thread list.
    content = "\u200b"
    try:
        await forum.create_thread(
            name=name,
            content=content,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except TypeError:
        await forum.create_thread(name=name, content=content)


async def _existing_forum_separator_tiers(forum: discord.ForumChannel) -> set[int]:
    out: set[int] = set()
    for th in list(getattr(forum, "threads", []) or []):
        tier = _tier_from_forum_separator_name(getattr(th, "name", ""))
        if tier is not None:
            out.add(int(tier))
    try:
        async for th in forum.archived_threads(limit=1000):
            tier = _tier_from_forum_separator_name(getattr(th, "name", ""))
            if tier is not None:
                out.add(int(tier))
    except Exception:
        pass
    return out


async def _existing_tier_separators_in_channel(
    channel: discord.TextChannel,
    *,
    bot_user_id: int,
) -> set[int]:
    tiers: set[int] = set()
    async for msg in channel.history(limit=5000):
        if not msg.author or msg.author.id != bot_user_id:
            continue
        tier = _tier_from_separator_content(msg.content or "")
        if tier is not None:
            tiers.add(int(tier))
    return tiers

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
        starter = await thread.send(pages[0], allowed_mentions=discord.AllowedMentions.none())
    elif starter.content != pages[0]:
        await starter.edit(content=pages[0], allowed_mentions=discord.AllowedMentions.none())

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
                await msg.edit(content=new_content, allowed_mentions=discord.AllowedMentions.none())
            except Exception:
                pass

    for msg in existing_extra[keep_n:]:
        try:
            await msg.delete()
        except Exception:
            pass

    for content in desired_extra[keep_n:]:
        await thread.send(content, allowed_mentions=discord.AllowedMentions.none())


async def _recent_snapshot_page_messages_in_channel(
    channel: discord.TextChannel,
    *,
    bot_user_id: int,
    starter_id: int,
    tier: int,
    ttype: str,
    desired_extra_pages: int,
) -> list[discord.Message]:
    history_limit = max(100, min(1000, desired_extra_pages * 20 + 300))
    found: list[discord.Message] = []
    async for msg in channel.history(limit=history_limit):
        if msg.id == starter_id:
            continue
        if not msg.author or msg.author.id != bot_user_id:
            continue
        if not _is_bucket_snapshot_message(msg, tier, ttype):
            continue
        found.append(msg)
    found.reverse()
    return found


async def _find_bucket_starter_message_in_channel(
    channel: discord.TextChannel,
    *,
    bot_user_id: int,
    tier: int,
    ttype: str,
) -> discord.Message | None:
    async for msg in channel.history(limit=5000):
        if not msg.author or msg.author.id != bot_user_id:
            continue
        if _is_bucket_starter_message(msg, tier, ttype):
            return msg
    return None


async def _sync_snapshot_pages_in_channel(
    bot: discord.Client,
    channel: discord.TextChannel,
    starter: discord.Message | None,
    pages: list[str],
    tier: int,
    ttype: str,
):
    if not pages:
        pages = [f"Leaderboard - Tier {tier} / {utils.title_case_type(ttype)}"]

    if starter is None:
        starter = await channel.send(pages[0], allowed_mentions=discord.AllowedMentions.none())
    elif starter.content != pages[0]:
        await starter.edit(content=pages[0], allowed_mentions=discord.AllowedMentions.none())

    if bot.user is None:
        return

    desired_extra = pages[1:]
    existing_extra = await _recent_snapshot_page_messages_in_channel(
        channel,
        bot_user_id=bot.user.id,
        starter_id=starter.id,
        tier=tier,
        ttype=ttype,
        desired_extra_pages=len(desired_extra),
    )

    keep_n = min(len(existing_extra), len(desired_extra))
    for idx in range(keep_n):
        msg = existing_extra[idx]
        new_content = desired_extra[idx]
        if msg.content != new_content:
            try:
                await msg.edit(content=new_content, allowed_mentions=discord.AllowedMentions.none())
            except Exception:
                pass

    for msg in existing_extra[keep_n:]:
        try:
            await msg.delete()
        except Exception:
            pass

    for content in desired_extra[keep_n:]:
        await channel.send(content, allowed_mentions=discord.AllowedMentions.none())


async def upsert_bucket_message(bot: discord.Client, tier: int, ttype: str):
    channel = await _resolve_normal_channel(bot)
    rows = await db.get_bucket_snapshot_rows(tier, ttype)
    pages = render_bucket_snapshot_pages(tier, ttype, rows)
    if not pages:
        pages = [f"Leaderboard — Tier {tier} / {utils.title_case_type(ttype)}"]

    starter: discord.Message | None = None
    if bot.user is not None:
        starter = await _find_bucket_starter_message_in_channel(
            channel,
            bot_user_id=bot.user.id,
            tier=tier,
            ttype=ttype,
        )
        if starter is None:
            separators = await _existing_tier_separators_in_channel(
                channel,
                bot_user_id=bot.user.id,
            )
            if int(tier) not in separators:
                await channel.send(
                    _tier_separator_text(int(tier)),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
    await _sync_snapshot_pages_in_channel(bot, channel, starter, pages, tier, ttype)

def render_bucket_snapshot_pages(tier: int, type_: str, rows: list[dict]) -> list[str]:
    title = _bucket_title(tier, type_)
    rows = _sorted_snapshot_rows(rows)

    # Build "best per tank" list from rows
    scored = [r for r in rows if r.get("score") is not None and int(r.get("score") or 0) > 0]
    latest = max(scored, key=lambda r: r.get("created_at", ""), default=None)
    top = max(scored, key=lambda r: r.get("score", 0), default=None)

    latest_line = "Latest: —"
    if latest:
        latest_line = (
            f"Latest: {_safe_inline_text(latest.get('tank_name'))}  {latest['score']}  "
            f"{_safe_inline_text(latest.get('player_name'))}  {_safe_inline_text(_fmt_local(latest.get('created_at')))}"
        )

    top_line = "Top:    —"
    if top:
        top_line = (
            f"Top:    {_safe_inline_text(top.get('tank_name'))}  "
            f"{top['score']}  {_safe_inline_text(top.get('player_name'))}"
        )

    header_lines = [
        title,
        latest_line,
        top_line,
    ]

    # Build monospaced table lines (header + rows)
    table_header = ["TANK", "BEST", "PLAYER", "WHEN"]
    table_rows: list[list[str]] = [table_header]

    for r in rows:
        tank = _safe_text(r.get("tank_name"))
        score = r.get("score")
        player = _safe_text(r.get("player_name"))
        when = _safe_text(_fmt_local(r["created_at"])) if r.get("created_at") else "—"
        score_val = int(score) if score is not None else 0
        score_text = "-" if score is None or score_val <= 0 else str(score_val)
        player_text = "-" if score is None or score_val <= 0 else player
        table_rows.append([
            tank,
            score_text,
            player_text,
            when,
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

    scored = [r for r in rows if r.get("score") is not None and int(r.get("score") or 0) > 0]
    if scored:
        latest = max(scored, key=lambda r: r.get("created_at") or "")
        top = max(scored, key=lambda r: int(r.get("score") or 0))

        latest_when = _fmt_local(latest.get("created_at"))
        header_lines.append(
            f"Latest: {_safe_inline_text(latest.get('tank_name'))}  {latest['score']}  "
            f"{_safe_inline_text(utils.clip(latest.get('player_name') or '—', 16))}  {_safe_inline_text(latest_when)}"
        )
        header_lines.append(
            f"Top:    {_safe_inline_text(top.get('tank_name'))}  {top['score']}  "
            f"{_safe_inline_text(utils.clip(top.get('player_name') or '—', 16))}"
        )

    # Build table rows (strings)
    table_rows = []
    table_rows.append(["TANK", "BEST", "PLAYER", "WHEN"])
    table_rows.append(["-"*28, "-"*6, "-"*20, "-"*20])

    for r in rows:
        tank = _safe_text(r["tank_name"])
        score_val = int(r.get("score") or 0) if r.get("score") is not None else 0
        if r.get("score") is None or score_val <= 0:
            table_rows.append([tank, "-", "-", "—"])
        else:
            when = _safe_text(_fmt_local(r.get("created_at")))
            player = _safe_text(utils.clip(r.get("player_name") or "—", 20))
            table_rows.append([tank, str(score_val), player, when])

    # Column widths (tweak to taste)
    widths = [28, 6, 20, 20]
    table_text = utils.fmt_table(table_rows, widths)

    # Wrap in code block for monospace rendering
    body = "```text\n" + table_text + "\n```"

    footer_lines = ["_Snapshot: best score per tank in this Tier×Type bucket._"]

    # Use your length limiter but now counting codeblock text
    return utils.build_snapshot_text(header_lines, [body], footer_lines)
