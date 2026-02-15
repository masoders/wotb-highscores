import datetime as dt
import asyncio
import logging
import os
import platform
import subprocess
import sys
import urllib.request
import urllib.error
from zoneinfo import ZoneInfo
import discord
from discord import app_commands

from . import config, db, backup, wg_sync, logging_setup, utils

_started_at = dt.datetime.utcnow()
log = logging.getLogger(__name__)
_git_sha_cache: str | None = None

def _local_tz() -> dt.tzinfo:
    tz = dt.datetime.now().astimezone().tzinfo
    return tz if tz is not None else dt.timezone.utc

def _safe_zoneinfo(name: str, fallback: dt.tzinfo) -> dt.tzinfo:
    try:
        return ZoneInfo(name)
    except Exception:
        return fallback

def _fmt_ts(value: str | None, to_tz: dt.tzinfo | None = None) -> str:
    if not value:
        return "n/a"
    try:
        s = str(value).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        ts = dt.datetime.fromisoformat(s)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        if to_tz is not None:
            ts = ts.astimezone(to_tz)
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)

def _fmt_local_dt(value: dt.datetime | None, to_tz: dt.tzinfo | None = None) -> str:
    if value is None:
        return "n/a"
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    if to_tz is not None:
        value = value.astimezone(to_tz)
    return value.strftime("%Y-%m-%d %H:%M")

def _fmt_ok(value: bool | None) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return "n/a"

def _fmt_bytes(value: int | None) -> str:
    if value is None or value < 0:
        return "n/a"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024.0 or unit == "TB":
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return "n/a"

def _db_file_stats() -> dict[str, int]:
    def _sz(path: str) -> int:
        try:
            return int(os.path.getsize(path))
        except Exception:
            return -1
    base = config.DB_PATH
    return {
        "db_bytes": _sz(base),
        "wal_bytes": _sz(base + "-wal"),
        "shm_bytes": _sz(base + "-shm"),
    }

def _process_metrics() -> dict[str, str]:
    rss_text = "n/a"
    try:
        import resource  # stdlib on Unix
        ru = resource.getrusage(resource.RUSAGE_SELF)
        rss = float(ru.ru_maxrss)
        # Linux reports KB; macOS reports bytes.
        if sys.platform != "darwin":
            rss *= 1024.0
        rss_text = _fmt_bytes(int(rss))
    except Exception:
        rss_text = "n/a"

    cpu_text = "n/a"
    try:
        t = os.times()
        cpu_used = float(t.user + t.system)
        up = float(max(1, uptime_seconds()))
        cpu_text = f"{(cpu_used / up) * 100.0:.2f}% avg"
    except Exception:
        cpu_text = "n/a"

    load_text = "n/a"
    try:
        load = os.getloadavg()
        load_text = f"{load[0]:.2f}/{load[1]:.2f}/{load[2]:.2f}"
    except Exception:
        load_text = "n/a"

    return {
        "rss": rss_text,
        "cpu": cpu_text,
        "loadavg": load_text,
        "fds": _open_fd_count(),
    }

def _open_fd_count() -> str:
    for path in ("/proc/self/fd", "/dev/fd"):
        try:
            return str(len(os.listdir(path)))
        except Exception:
            continue
    return "n/a"

def _git_sha() -> str:
    global _git_sha_cache
    if _git_sha_cache is not None:
        return _git_sha_cache
    env_sha = os.getenv("GIT_COMMIT", "").strip()
    if env_sha:
        _git_sha_cache = env_sha[:12]
        return _git_sha_cache
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            timeout=1.0,
        )
        _git_sha_cache = out.decode("utf-8", errors="ignore").strip() or "n/a"
    except Exception:
        _git_sha_cache = "n/a"
    return _git_sha_cache

async def _probe_dashboard() -> tuple[bool | None, str]:
    if not config.DASHBOARD_ENABLED:
        return None, "disabled"
    host = config.DASHBOARD_BIND.strip() or "127.0.0.1"
    if host in {"0.0.0.0", "::"}:
        host = "127.0.0.1"
    host_part = f"[{host}]" if ":" in host and not host.startswith("[") else host
    url = f"http://{host_part}:{config.DASHBOARD_PORT}/healthz"

    def _do_probe() -> tuple[bool, str]:
        req = urllib.request.Request(url)
        if config.DASHBOARD_TOKEN:
            req.add_header("Authorization", f"Bearer {config.DASHBOARD_TOKEN}")
        try:
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                return True, f"http {int(resp.status)}"
        except urllib.error.HTTPError as exc:
            return False, f"http {int(exc.code)}"
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

    return await asyncio.to_thread(_do_probe)

def _validate_timezones() -> list[str]:
    issues: list[str] = []
    for key, value in [("BACKUP_TZ", config.BACKUP_TZ), ("WG_REFRESH_TZ", config.WG_REFRESH_TZ)]:
        try:
            ZoneInfo(str(value))
        except Exception:
            issues.append(f"{key}=invalid({value})")
    return issues

def _dashboard_config_flags() -> list[str]:
    flags: list[str] = []
    if config.DASHBOARD_ENABLED and not config.DASHBOARD_TOKEN:
        flags.append("DASHBOARD_TOKEN missing")
    bind = (config.DASHBOARD_BIND or "").strip()
    if config.DASHBOARD_ENABLED and bind not in {"127.0.0.1", "::1", "localhost"}:
        flags.append(f"DASHBOARD_BIND not loopback ({bind})")
    return flags

def _count_pending_tasks() -> int:
    try:
        return sum(1 for t in asyncio.all_tasks() if not t.done())
    except Exception:
        return -1

def _truncate_items(values: list[str], limit: int = 8) -> str:
    if not values:
        return "none"
    if len(values) <= limit:
        return ", ".join(values)
    return ", ".join(values[:limit]) + f", +{len(values) - limit} more"

def _safe_display_name(name: str, max_len: int = 40) -> str:
    txt = str(name or "").strip()
    if not txt:
        txt = "unnamed"
    if len(txt) <= max_len:
        return txt
    return txt[: max_len - 1] + "…"

async def _resolve_channel_anywhere(bot: discord.Client, channel_id: int) -> discord.abc.GuildChannel | None:
    if channel_id <= 0:
        return None
    ch = bot.get_channel(channel_id)
    if ch is None:
        try:
            ch = await bot.fetch_channel(channel_id)
        except Exception:
            return None
    if isinstance(ch, discord.abc.GuildChannel):
        return ch
    return None

def _perm_names(perms: discord.Permissions, wanted: tuple[str, ...]) -> list[str]:
    missing: list[str] = []
    for key in wanted:
        if not bool(getattr(perms, key, False)):
            missing.append(key)
    return missing

def _resource_specs() -> list[dict[str, object]]:
    return [
        {
            "label": "backup",
            "id": int(config.BACKUP_CHANNEL_ID),
            "required": ("view_channel", "send_messages", "attach_files", "read_message_history"),
            "sensitive": True,
        },
        {
            "label": "audit_log",
            "id": int(config.AUDIT_LOG_CHANNEL_ID),
            "required": ("view_channel", "send_messages", "read_message_history"),
            "sensitive": True,
        },
        {
            "label": "announce",
            "id": int(config.ANNOUNCE_CHANNEL_ID),
            "required": ("view_channel", "send_messages", "read_message_history"),
            "sensitive": False,
        },
        {
            "label": "index_normal",
            "id": int(config.TANK_INDEX_NORMAL_CHANNEL_ID),
            "required": ("view_channel", "send_messages", "read_message_history"),
            "sensitive": False,
        },
        {
            "label": "index_forum",
            "id": int(config.TANK_INDEX_FORUM_CHANNEL_ID),
            "required": ("view_channel", "send_messages", "create_public_threads", "send_messages_in_threads"),
            "sensitive": False,
        },
    ]

def _resolve_commander_role(guild: discord.Guild) -> discord.Role | None:
    if config.COMMANDER_ROLE_ID > 0:
        role = guild.get_role(config.COMMANDER_ROLE_ID)
        if role is not None:
            return role
    for role in guild.roles:
        if role.name == config.COMMANDER_ROLE_NAME:
            return role
    return None

def uptime_seconds() -> int:
    return int((dt.datetime.utcnow() - _started_at).total_seconds())

def fmt_uptime() -> str:
    s = uptime_seconds()
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if d:
        return f"{d}d {h:02d}h {m:02d}m {s:02d}s"
    return f"{h:02d}h {m:02d}m {s:02d}s"

class System(app_commands.Group):
    def __init__(self):
        super().__init__(name="system", description="System commands (admins only)")

system = System()

@system.command(name="health", description="Show system health (admins only)")
async def system_health(interaction: discord.Interaction):
    member = interaction.user
    if not isinstance(member, discord.Member) or not (member.guild_permissions.manage_guild or member.guild_permissions.administrator):
        await interaction.response.send_message("Nope. You need **Manage Server** to use this.", ephemeral=True)
        return

    try:
        from . import main
        await interaction.response.defer(ephemeral=True, thinking=False)
    except discord.NotFound:
        # Interaction token expired before we could acknowledge.
        return

    db_latency_ms = -1.0
    try:
        t0 = dt.datetime.utcnow()
        tanks, subs, idx = await db.counts()
        db_latency_ms = (dt.datetime.utcnow() - t0).total_seconds() * 1000.0
        db_ok = True
    except Exception as e:
        tanks, subs, idx = 0, 0, 0
        db_ok = False
        db_err = f"{type(e).__name__}: {e}"

    migration_ver = 0
    if db_ok:
        try:
            migration_ver = await db.migration_version()
        except Exception:
            migration_ver = 0
    try:
        db_diag = await db.health_diagnostics()
    except Exception as e:
        db_diag = {
            "journal_mode": "n/a",
            "synchronous": "n/a",
            "integrity": f"error:{type(e).__name__}",
            "orphan_submissions": -1,
            "duplicate_index_mappings": -1,
        }

    last_backup_utc, last_backup_ok, last_backup_msg = backup.last_backup_status()
    backup_last_scheduled_fn = getattr(backup, "last_scheduled_backup_utc", None)
    last_scheduled_backup_utc = backup_last_scheduled_fn() if callable(backup_last_scheduled_fn) else None
    if not last_backup_utc:
        last_backup_utc = await db.get_sync_state("backup:last")
    if last_backup_ok is None:
        stored_ok = await db.get_sync_state("backup:last_ok")
        if stored_ok in {"1", "0"}:
            last_backup_ok = stored_ok == "1"
    if not last_backup_msg:
        last_backup_msg = await db.get_sync_state("backup:last_msg")
    if not last_scheduled_backup_utc:
        last_scheduled_backup_utc = await db.get_sync_state("backup:last_scheduled")
    local_tz = _local_tz()
    backup_tz = _safe_zoneinfo(config.BACKUP_TZ, local_tz)
    display_tz = local_tz
    now_backup_local = dt.datetime.now(backup_tz)
    next_backup = getattr(backup.weekly_backup_loop, "next_run", backup.next_weekly_run(now_backup_local))

    wg_last_status_fn = getattr(wg_sync, "last_wg_sync_status", None)
    if callable(wg_last_status_fn):
        last_wg_utc, last_wg_ok, last_wg_msg = wg_last_status_fn()
    else:
        last_wg_utc, last_wg_ok, last_wg_msg = None, None, None
    wg_last_scheduled_fn = getattr(wg_sync, "last_scheduled_wg_sync_utc", None)
    last_scheduled_wg_utc = wg_last_scheduled_fn() if callable(wg_last_scheduled_fn) else None
    wg_region = config.WG_API_REGION
    if not last_wg_utc:
        last_wg_utc = await db.get_sync_state(f"wg:last:{wg_region}") or await db.get_sync_state(f"wg:last_sync:{wg_region}")
    if last_wg_ok is None:
        stored_wg_ok = await db.get_sync_state(f"wg:last_ok:{wg_region}")
        if stored_wg_ok in {"1", "0"}:
            last_wg_ok = stored_wg_ok == "1"
    if not last_wg_msg:
        last_wg_msg = await db.get_sync_state(f"wg:last_msg:{wg_region}")
    # Backward compatibility: older builds only persisted wg:last_sync:<region>.
    if last_wg_utc and last_wg_ok is None:
        last_wg_ok = True
    if not last_scheduled_wg_utc:
        last_scheduled_wg_utc = await db.get_sync_state(f"wg:last_scheduled:{wg_region}")
    wg_tz = _safe_zoneinfo(config.WG_REFRESH_TZ, local_tz)
    now_wg_local = dt.datetime.now(wg_tz)
    wg_next_fn = getattr(wg_sync, "next_wg_sync_run", None)
    next_wg = wg_next_fn() if callable(wg_next_fn) else None
    if next_wg is None and config.WG_SYNC_ENABLED:
        next_wg = now_wg_local.replace(
            hour=config.WG_REFRESH_HOUR,
            minute=config.WG_REFRESH_MINUTE,
            second=0,
            microsecond=0,
        )
        if next_wg <= now_wg_local:
            next_wg += dt.timedelta(days=1)

    db_files = _db_file_stats()
    proc = _process_metrics()
    pending_tasks = _count_pending_tasks()
    failures = logging_setup.recent_failures(limit=3)
    tz_issues = _validate_timezones()
    config_flags = _dashboard_config_flags()
    dash_probe_ok, dash_probe_msg = await _probe_dashboard()

    bot_latency_ms = float(getattr(main.bot, "latency", 0.0) or 0.0) * 1000.0
    bot_ready = bool(main.bot.is_ready())
    bot_closed = bool(main.bot.is_closed())
    guild_count = len(getattr(main.bot, "guilds", []))

    perm_issues: list[str] = []
    guild = interaction.guild
    if guild and main.bot.user:
        me = guild.get_member(main.bot.user.id)
        if me:
            if config.BACKUP_CHANNEL_ID:
                ch = guild.get_channel(config.BACKUP_CHANNEL_ID)
                if ch is not None:
                    p = ch.permissions_for(me)
                    missing = []
                    if not p.view_channel:
                        missing.append("view_channel")
                    if not p.send_messages:
                        missing.append("send_messages")
                    if not p.attach_files:
                        missing.append("attach_files")
                    if missing:
                        perm_issues.append(f"backup_channel:{','.join(missing)}")
        else:
            perm_issues.append("bot_member_not_in_guild_cache")

    lines = []
    lines.append("**System health**")
    lines.append(f"- Uptime: `{fmt_uptime()}`")
    lines.append(f"- Bot ready: `{bot_ready}` | Closed: `{bot_closed}` | WS latency: `{bot_latency_ms:.1f}ms` | Guilds: `{guild_count}`")
    lines.append(f"- DB: `{'OK' if db_ok else 'FAIL'}`")
    if not db_ok:
        lines.append(f"- DB error: `{db_err}`")
    lines.append(f"- Tanks: `{tanks}` | Submissions: `{subs}` | Index mappings: `{idx}`")
    lines.append(f"- DB schema version: `v{migration_ver}`")
    lines.append(
        f"- DB internals: `journal={db_diag.get('journal_mode','n/a')}` "
        f"`sync={db_diag.get('synchronous','n/a')}` "
        f"`quick_check={db_diag.get('integrity','n/a')}` "
        f"`latency={db_latency_ms:.1f}ms`"
    )
    lines.append(
        f"- DB files: `db={_fmt_bytes(db_files.get('db_bytes', -1))}` "
        f"`wal={_fmt_bytes(db_files.get('wal_bytes', -1))}` "
        f"`shm={_fmt_bytes(db_files.get('shm_bytes', -1))}`"
    )
    lines.append(f"- Backups enabled: `{config.BACKUP_CHANNEL_ID != 0}`")
    lines.append(f"- Last backup: `{_fmt_ts(last_backup_utc, display_tz)}` (`{_fmt_ok(last_backup_ok)}`) `{last_backup_msg or 'n/a'}`")
    lines.append(f"- Last scheduled backup: `{_fmt_ts(last_scheduled_backup_utc, display_tz)}`")
    lines.append(f"- Next scheduled backup: `{_fmt_local_dt(next_backup, display_tz)}`")
    lines.append(f"- WG refresh enabled: `{config.WG_SYNC_ENABLED}`")
    lines.append(f"- Last WG refresh: `{_fmt_ts(last_wg_utc, display_tz)}` (`{_fmt_ok(last_wg_ok)}`) `{last_wg_msg or 'n/a'}`")
    lines.append(f"- Last scheduled WG refresh: `{_fmt_ts(last_scheduled_wg_utc, display_tz)}`")
    lines.append(
        f"- Next scheduled WG refresh: `{_fmt_local_dt(next_wg, display_tz)}`"
    )
    lines.append(
        f"- Runtime resources: `rss={proc.get('rss','n/a')}` `cpu={proc.get('cpu','n/a')}` `load={proc.get('loadavg','n/a')}` `fds={proc.get('fds','n/a')}`"
    )
    lines.append(
        f"- Queue depth: `pending_asyncio_tasks={pending_tasks}` "
        f"`backup_loop_running={backup.weekly_backup_loop.is_running()}` "
        f"`wg_loop_running={wg_sync.daily_clan_sync_loop.is_running()}`"
    )
    lines.append(
        f"- Build/runtime: `sha={_git_sha()}` "
        f"`python={platform.python_version()}` "
        f"`discord.py={getattr(discord, '__version__', 'n/a')}` "
        f"`started_utc={_started_at.strftime('%Y-%m-%d %H:%M')}`"
    )
    lines.append(
        f"- Dashboard: `{config.DASHBOARD_ENABLED}` on `{config.DASHBOARD_BIND}:{config.DASHBOARD_PORT}` "
        f"`token_set={bool(config.DASHBOARD_TOKEN)}` "
        f"`probe={_fmt_ok(dash_probe_ok)}` `{dash_probe_msg}`"
    )

    anomaly_bits = [
        f"orphan_submissions={db_diag.get('orphan_submissions', -1)}",
        f"duplicate_index_mappings={db_diag.get('duplicate_index_mappings', -1)}",
    ]
    lines.append(f"- Data integrity: `{' '.join(anomaly_bits)}`")

    cfg_issues = tz_issues + config_flags + perm_issues
    lines.append(f"- Config/permission issues: `{'; '.join(cfg_issues) if cfg_issues else 'none'}`")

    if failures:
        summary = " | ".join(
            [f"{f.get('ts','?')} {f.get('logger','?')}: {str(f.get('message',''))[:70]}" for f in failures]
        )
        lines.append(f"- Recent failures: `{summary}`")
    else:
        lines.append("- Recent failures: `none`")

    try:
        await interaction.followup.send("\n".join(lines), ephemeral=True)
    except discord.NotFound:
        return

@system.command(name="audit_access", description="Audit bot-managed channel access and permission risks (admins only)")
async def system_audit_access(interaction: discord.Interaction):
    member = interaction.user
    if not isinstance(member, discord.Member) or not (member.guild_permissions.manage_guild or member.guild_permissions.administrator):
        await interaction.response.send_message("Nope. You need **Manage Server** to use this.", ephemeral=True)
        return

    try:
        from . import main
        await interaction.response.defer(ephemeral=True, thinking=False)
    except discord.NotFound:
        return

    guild = interaction.guild
    bot_user = main.bot.user
    if guild is None or bot_user is None:
        await interaction.followup.send("❌ Audit unavailable outside a guild context.", ephemeral=True)
        return

    me = guild.get_member(bot_user.id)
    if me is None:
        await interaction.followup.send("❌ Bot member is not in guild cache; retry in a few seconds.", ephemeral=True)
        return

    commander_role = _resolve_commander_role(guild)
    commander_label = (
        f"@{_safe_display_name(commander_role.name)}" if commander_role is not None else f"(missing role: {config.COMMANDER_ROLE_NAME})"
    )

    high_risk_role_names: list[str] = []
    for role in guild.roles:
        if role.is_default():
            continue
        p = role.permissions
        if p.administrator or p.manage_guild or p.manage_roles:
            high_risk_role_names.append(f"@{_safe_display_name(role.name)}")

    high_risk_member_names: list[str] = []
    for m in guild.members:
        if m.bot:
            continue
        gp = m.guild_permissions
        if gp.administrator or gp.manage_guild or gp.manage_roles:
            high_risk_member_names.append(_safe_display_name(m.display_name))

    findings_ok: list[str] = []
    findings_warn: list[str] = []
    findings_fail: list[str] = []

    if commander_role is None:
        findings_fail.append(
            f"Commander role not found. Set `COMMANDER_ROLE_ID` or ensure role name `{config.COMMANDER_ROLE_NAME}` exists."
        )

    specs = [s for s in _resource_specs() if int(s.get("id", 0) or 0) > 0]
    if not specs:
        findings_warn.append("No managed channels configured (`BACKUP/AUDIT/ANNOUNCE/TANK_INDEX_*`).")

    for spec in specs:
        label = str(spec["label"])
        ch_id = int(spec["id"])
        required = tuple(spec["required"])  # type: ignore[arg-type]
        sensitive = bool(spec["sensitive"])
        ch = await _resolve_channel_anywhere(main.bot, ch_id)

        if ch is None:
            findings_fail.append(f"{label}: channel `{ch_id}` not found or not accessible to bot.")
            continue

        if int(getattr(ch, "guild", guild).id) != int(guild.id):
            findings_warn.append(
                f"{label}: channel `{ch.id}` is in another guild (`{getattr(ch.guild, 'name', 'unknown')}`); "
                "current audit cannot validate non-bot principals there."
            )

        bot_missing = _perm_names(ch.permissions_for(me), required)
        if bot_missing:
            findings_fail.append(f"{label}: bot missing `{', '.join(bot_missing)}` in `#{_safe_display_name(ch.name)}`.")
        else:
            findings_ok.append(f"{label}: bot perms OK in `#{_safe_display_name(ch.name)}`.")

        everyone_perms = ch.permissions_for(guild.default_role)
        if label in {"index_normal", "index_forum"}:
            if not everyone_perms.view_channel:
                findings_fail.append(f"{label}: @everyone is missing `view_channel` in `#{_safe_display_name(ch.name)}`.")
            if not everyone_perms.read_message_history:
                findings_fail.append(
                    f"{label}: @everyone is missing `read_message_history` in `#{_safe_display_name(ch.name)}`."
                )
            forbidden_everyone = []
            for perm_key in (
                "send_messages",
                "send_messages_in_threads",
                "create_public_threads",
                "create_private_threads",
                "manage_messages",
                "manage_channels",
                "manage_threads",
                "attach_files",
                "embed_links",
                "mention_everyone",
                "use_application_commands",
            ):
                if bool(getattr(everyone_perms, perm_key, False)):
                    forbidden_everyone.append(perm_key)
            if forbidden_everyone:
                findings_fail.append(
                    f"{label}: @everyone has extra perms `{', '.join(forbidden_everyone)}` in `#{_safe_display_name(ch.name)}`."
                )

        if label in {"backup", "audit_log"}:
            # Strict policy requested: only bot + commander role should access these channels.
            if everyone_perms.view_channel or everyone_perms.send_messages:
                findings_fail.append(
                    f"{label}: @everyone must not access `#{_safe_display_name(ch.name)}` "
                    f"(view={everyone_perms.view_channel}, send={everyone_perms.send_messages})."
                )

            if commander_role is not None:
                commander_perms = ch.permissions_for(commander_role)
                if not commander_perms.view_channel:
                    findings_fail.append(
                        f"{label}: commander role {commander_label} missing `view_channel` in `#{_safe_display_name(ch.name)}`."
                    )
                if not commander_perms.read_message_history:
                    findings_fail.append(
                        f"{label}: commander role {commander_label} missing `read_message_history` in `#{_safe_display_name(ch.name)}`."
                    )

            unexpected_roles: list[str] = []
            for role in guild.roles:
                if role.is_default():
                    continue
                if commander_role is not None and role.id == commander_role.id:
                    continue
                perms = ch.permissions_for(role)
                if perms.view_channel or perms.send_messages:
                    unexpected_roles.append(f"@{_safe_display_name(role.name)}")
            if unexpected_roles:
                findings_fail.append(
                    f"{label}: unexpected role access in `#{_safe_display_name(ch.name)}` -> "
                    f"{_truncate_items(sorted(set(unexpected_roles)), limit=10)}"
                )

            unexpected_members: list[str] = []
            for m in guild.members:
                if m.bot or m.id == bot_user.id:
                    continue
                if utils.has_commander_role(m):
                    continue
                perms = ch.permissions_for(m)
                if perms.view_channel or perms.send_messages:
                    unexpected_members.append(_safe_display_name(m.display_name))
            if unexpected_members:
                findings_fail.append(
                    f"{label}: unexpected member access in `#{_safe_display_name(ch.name)}` -> "
                    f"{_truncate_items(sorted(set(unexpected_members)), limit=10)}"
                )

    lines: list[str] = []
    lines.append("**Access audit (managed resources)**")
    lines.append(f"- Guild: `{guild.name}` (`{guild.id}`)")
    lines.append(f"- Commander role: `{commander_label}`")
    lines.append(f"- Managed resources checked: `{len(specs)}`")
    lines.append(f"- High-privilege roles (server-wide): `{_truncate_items(sorted(set(high_risk_role_names)), limit=10)}`")
    lines.append(f"- High-privilege members (server-wide): `{_truncate_items(sorted(set(high_risk_member_names)), limit=10)}`")
    lines.append(f"- OK findings: `{len(findings_ok)}` | Warnings: `{len(findings_warn)}` | Failures: `{len(findings_fail)}`")

    if findings_fail:
        lines.append("- Failures:")
        for item in findings_fail[:12]:
            lines.append(f"  - {item}")
    if findings_warn:
        lines.append("- Warnings:")
        for item in findings_warn[:16]:
            lines.append(f"  - {item}")
    if findings_ok:
        lines.append("- Passed:")
        for item in findings_ok[:12]:
            lines.append(f"  - {item}")

    if len("\n".join(lines)) > 1850:
        shortened = lines[:8]
        shortened.append("- Output truncated. Resolve top failures/warnings first, then rerun.")
        lines = shortened

    try:
        await interaction.followup.send("\n".join(lines), ephemeral=True)
    except discord.NotFound:
        return

@system.command(name="reload", description="Reload command modules and sync (admins only)")
async def system_reload(interaction: discord.Interaction):
    member = interaction.user
    if not isinstance(member, discord.Member) or not (member.guild_permissions.manage_guild or member.guild_permissions.administrator):
        await interaction.response.send_message("Nope. You need **Manage Server** to use this.", ephemeral=True)
        return

    try:
        await interaction.response.send_message("⏳ Reloading modules and syncing commands…", ephemeral=True)
    except discord.NotFound:
        return

    from . import main  # local import to avoid circular import at module load time

    async def _run_reload():
        try:
            ok, msg = await asyncio.wait_for(main.reload_runtime(), timeout=60)
        except asyncio.TimeoutError:
            ok, msg = False, "Reload timed out after 60s. Try a process restart."
        except Exception as exc:
            ok, msg = False, f"{type(exc).__name__}: {exc}"

        try:
            await interaction.followup.send(("✅ " if ok else "❌ ") + msg, ephemeral=True)
        except Exception:
            log.exception("Failed to send reload follow-up")

    asyncio.create_task(_run_reload())
