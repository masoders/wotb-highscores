import discord
from discord import app_commands
import datetime as dt
import importlib
import logging

from . import config, db, backup, health, logging_setup
from .commands import help_cmd, highscore, tank, backup_cmd

intents = discord.Intents.default()
intents.members = True

bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

def _guild_obj():
    return discord.Object(id=config.GUILD_ID) if config.GUILD_ID else None

async def _register_and_sync_commands(*, reload_modules: bool = False):
    global help_cmd, highscore, tank, backup_cmd, health

    if reload_modules:
        # Reload dependency modules first so command modules bind fresh imports.
        from . import utils as _utils, forum_index as _forum_index
        from . import db as _db
        _utils = importlib.reload(_utils)
        _forum_index = importlib.reload(_forum_index)
        _db = importlib.reload(_db)

        help_cmd = importlib.reload(help_cmd)
        highscore = importlib.reload(highscore)
        tank = importlib.reload(tank)
        backup_cmd = importlib.reload(backup_cmd)
        health = importlib.reload(health)

    guild = _guild_obj()

    # Clear prior tree to avoid duplicate registrations.
    tree.clear_commands(guild=None)
    if guild:
        tree.clear_commands(guild=guild)

    help_cmd.setup(tree, guild=guild)
    highscore.register(tree, bot, guild=guild)
    tank.register(tree, bot, guild=guild)
    backup_cmd.register(tree, bot, guild=guild)
    tree.add_command(health.system, guild=guild)

    if guild:
        await tree.sync(guild=guild)
    else:
        await tree.sync()

async def reload_runtime() -> tuple[bool, str]:
    try:
        await db.init_db()
        await _register_and_sync_commands(reload_modules=True)
        return True, "Reloaded modules and synced commands."
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

@bot.event
async def on_ready():
    logging_setup.setup_logging()
    await db.init_db()

    # Start backup scheduler
    if not backup.weekly_backup_loop.is_running():
        backup.weekly_backup_loop.start(bot)

    await _register_and_sync_commands(reload_modules=False)

    logging.getLogger(__name__).info("Logged in as %s (id=%s)", bot.user, bot.user.id)

def run():
    if not config.DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN is missing")
    bot.run(config.DISCORD_TOKEN)
