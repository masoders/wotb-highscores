import discord
from discord import app_commands
from .. import utils

def setup(tree: app_commands.CommandTree, *, guild: discord.abc.Snowflake | None = None):
    @tree.command(name="help", description="Show commands you can use", guild=guild)
    async def help_command(interaction: discord.Interaction):
        member = interaction.user
        is_admin = isinstance(member, discord.Member) and utils.can_manage(member)
        is_commander = isinstance(member, discord.Member) and utils.has_commander_role(member)

        lines = []
        lines.append("**Tank Highscore Bot — Help**")
        lines.append("")
        lines.append("**Public commands:**")
        lines.append("- `/highscore show` — show current champion")
        lines.append("- `/highscore history` — recent results + stats")
        lines.append("- `/highscore qualify` — check if a score would qualify")
        lines.append("")

        if is_commander:
            lines.append("**Commander commands:**")
            lines.append("- `/highscore submit|edit|delete` — add or correct scores")
            lines.append("- `/highscore refresh_web` — regenerate static leaderboard webpage")
            lines.append("- /highscore import_scores — import historical scores from CSV")
            lines.append("- `/tank add|edit|remove|list|export_csv|export_scores_csv` — roster updates and export")
            lines.append("- `/backup …` — backups and status")
            lines.append("")

        if is_admin:
            lines.append("**Admin commands:**")
            lines.append("- `/highscore changes` — score audit trail")
            lines.append("- `/tank changes|preview_import|import_csv|rebuild_index...` — advanced tank admin")
            lines.append("- `/system health` — system health")
            lines.append("")

        lines.append("_Commands shown depend on your permissions._")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)
