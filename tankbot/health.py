import datetime as dt
import asyncio
import logging
from zoneinfo import ZoneInfo
import discord
from discord import app_commands

from . import config, db, backup, wg_sync

_started_at = dt.datetime.utcnow()
log = logging.getLogger(__name__)

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
        await interaction.response.defer(ephemeral=True, thinking=False)
    except discord.NotFound:
        # Interaction token expired before we could acknowledge.
        return

    try:
        tanks, subs, idx = await db.counts()
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

    lines = []
    lines.append("**System health**")
    lines.append(f"- Uptime: `{fmt_uptime()}`")
    lines.append(f"- DB: `{'OK' if db_ok else 'FAIL'}`")
    if not db_ok:
        lines.append(f"- DB error: `{db_err}`")
    lines.append(f"- Tanks: `{tanks}` | Submissions: `{subs}` | Index mappings: `{idx}`")
    lines.append(f"- DB schema version: `v{migration_ver}`")
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
    lines.append(f"- Dashboard: `{config.DASHBOARD_ENABLED}` on `{config.DASHBOARD_BIND}:{config.DASHBOARD_PORT}`")

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
