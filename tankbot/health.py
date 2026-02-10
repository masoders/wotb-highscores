import datetime as dt
from zoneinfo import ZoneInfo
import discord
from discord import app_commands

from . import config, db, backup

_started_at = dt.datetime.utcnow()

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

    last_utc, last_ok, last_msg = backup.last_backup_status()
    tz = ZoneInfo(config.BACKUP_TZ)
    now_local = dt.datetime.now(tz)
    nxt = getattr(backup.weekly_backup_loop, "next_run", backup.next_weekly_run(now_local))

    lines = []
    lines.append("**System health**")
    lines.append(f"- Uptime: `{fmt_uptime()}`")
    lines.append(f"- DB: `{'OK' if db_ok else 'FAIL'}`")
    if not db_ok:
        lines.append(f"- DB error: `{db_err}`")
    lines.append(f"- Tanks: `{tanks}` | Submissions: `{subs}` | Index mappings: `{idx}`")
    lines.append(f"- DB schema version: `v{migration_ver}`")
    lines.append(f"- Backups enabled: `{config.BACKUP_CHANNEL_ID != 0}`")
    lines.append(f"- Last backup: `{last_utc or 'n/a'}` (`{last_ok}`) `{last_msg or ''}`")
    lines.append(f"- Next backup: `{nxt.isoformat()}` ({config.BACKUP_TZ})")
    lines.append(f"- Dashboard: `{config.DASHBOARD_ENABLED}` on `{config.DASHBOARD_BIND}:{config.DASHBOARD_PORT}`")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)

@system.command(name="reload", description="Reload command modules and sync (admins only)")
async def system_reload(interaction: discord.Interaction):
    member = interaction.user
    if not isinstance(member, discord.Member) or not (member.guild_permissions.manage_guild or member.guild_permissions.administrator):
        await interaction.response.send_message("Nope. You need **Manage Server** to use this.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    from . import main  # local import to avoid circular import at module load time
    ok, msg = await main.reload_runtime()
    await interaction.followup.send(("✅ " if ok else "❌ ") + msg, ephemeral=True)
