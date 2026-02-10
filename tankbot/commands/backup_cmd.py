import discord
from discord import app_commands

from .. import backup, utils, config
grp = app_commands.Group(name="backup", description="Backup commands")

class Backup(app_commands.Group):
    def __init__(self):
        super().__init__(name="backup", description="Database backups (commanders only)")

def register(tree: app_commands.CommandTree, bot: discord.Client, guild: discord.Object | None):
    grp = Backup()
    tree.add_command(grp, guild=guild)

    @grp.command(name="run_now", description="Run a DB backup now (commanders only)")
    async def run_now(interaction: discord.Interaction):
        member = interaction.user
        if not isinstance(member, discord.Member) or not utils.has_commander_role(member):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can run backups.", ephemeral=True)
            return
        await interaction.response.send_message("Running backup…", ephemeral=True)
        ok, msg = await backup.run_backup_now(bot)
        await interaction.followup.send(("✅ " if ok else "❌ ") + msg, ephemeral=True)

    @grp.command(name="status", description="Show backup schedule status (commanders only)")
    async def status(interaction: discord.Interaction):
        member = interaction.user
        if not isinstance(member, discord.Member) or not utils.has_commander_role(member):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can view backup status.", ephemeral=True)
            return

        tz = config.BACKUP_TZ
        last_utc, last_ok, last_msg = backup.last_backup_status()
        # compute next run if available
        nxt = getattr(backup.weekly_backup_loop, "next_run", None)
        nxt_text = nxt.isoformat() if nxt else "n/a"

        await interaction.response.send_message(
            f"Backup guild: `{config.BACKUP_GUILD_ID or config.GUILD_ID or 'auto'}`\n"
            f"Backup channel: `{config.BACKUP_CHANNEL_ID}`\n"
            f"Schedule: weekday={config.BACKUP_WEEKDAY} time={config.BACKUP_HOUR:02d}:{config.BACKUP_MINUTE:02d} ({tz})\n"
            f"Last backup: `{last_utc or 'n/a'}` ok=`{last_ok}` `{last_msg or ''}`\n"
            f"Next run: `{nxt_text}` ({tz})",
            ephemeral=True
        )

    @grp.command(name="verify_latest", description="Verify latest backup file in the backup channel (commanders only)")
    @app_commands.describe(scan_limit="How many recent messages to scan (10-200)")
    async def verify_latest(interaction: discord.Interaction, scan_limit: int = 50):
        member = interaction.user
        if not isinstance(member, discord.Member) or not utils.has_commander_role(member):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can verify backups.", ephemeral=True)
            return
        scan_limit = max(10, min(scan_limit, 200))
        await interaction.response.send_message("Verifying latest backup…", ephemeral=True)
        ok, msg = await backup.verify_latest_backup(bot, scan_limit=scan_limit)
        await interaction.followup.send(("✅ " if ok else "❌ ") + msg, ephemeral=True)
