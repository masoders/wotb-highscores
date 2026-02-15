import discord
from discord import app_commands
from .. import utils

def setup(tree: app_commands.CommandTree, *, guild: discord.abc.Snowflake | None = None):
    @tree.command(name="help", description="Show commands you can use", guild=guild)
    async def help_command(interaction: discord.Interaction):
        member = interaction.user
        is_admin = isinstance(member, discord.Member) and utils.can_manage(member)
        is_commander = isinstance(member, discord.Member) and utils.has_commander_role(member)

        lines = [
            "**Tank Highscore Bot — Help**",
            "",
            "**Public commands:**",
            "- `/help` — show this help message",
            "- `/highscore show` — show champion (global or filtered)",
            "- `/highscore qualify` — check if damage would qualify (no submit)",
            "",
        ]

        if is_commander:
            lines.extend([
                "**Commander commands:**",
                "- `/highscore submit` — submit a new highscore",
                "- `/highscore history` — recent submissions + stats",
                "- `/highscore edit` — edit submission by id (score/player)",
                "- `/highscore delete` — revert or hard-delete submission by id",
                "- `/highscore refresh_web` — regenerate static leaderboard webpage",
                "- `/highscore refresh_players` — refresh WG clan player list now",
                "- `/tank add|edit|remove|rename|list` — manage roster",
                "- `/tank export_csv|export_scores_csv` — export roster/scores CSV",
                "- `/backup run_now|status|verify_latest` — backup operations",
                "",
            ])

        if is_admin:
            lines.extend([
                "**Admin commands (Manage Server):**",
                "- `/highscore import_scores` — import historical scores CSV",
                "- `/highscore changes` — damage audit trail",
                "- `/tank alias_add|alias_list|alias_seed_common` — alias management",
                "- `/tank merge` — merge duplicate tank into canonical tank",
                "- `/tank changes|preview_import|import_csv` — tank audit/import",
                "- `/tank rebuild_index|rebuild_index_missing` — index snapshot rebuild/repair",
                "- `/system health|audit_access|reload` — runtime health + access audit + command reload",
                "",
            ])

        lines.append("_Commands shown depend on your permissions._")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)
