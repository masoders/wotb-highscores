import discord
from discord import app_commands
import importlib
import inspect
import logging
import time
from collections.abc import Callable
from typing import Any

from . import config, db, backup, health, logging_setup, metrics, static_site, wg_sync, tank_name_sync
from .commands import help_cmd, highscore, tank, backup_cmd

intents = discord.Intents.default()
intents.members = True

bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)
_log = logging.getLogger(__name__)


def _wrap_command_callback(callback: Callable[..., Any]):
    if getattr(callback, "__tankbot_latency_wrapped__", False):
        return callback

    async def _wrapped(*args, **kwargs):
        t0 = time.perf_counter()
        try:
            result = callback(*args, **kwargs)
            if inspect.isawaitable(result):
                return await result
            return result
        finally:
            metrics.record_command_latency_ms((time.perf_counter() - t0) * 1000.0)

    setattr(_wrapped, "__tankbot_latency_wrapped__", True)
    return _wrapped


def _instrument_command(command: Any):
    callback_attr = None
    if hasattr(command, "_callback"):
        callback_attr = "_callback"
    elif hasattr(command, "callback"):
        callback_attr = "callback"

    if callback_attr is not None:
        callback = getattr(command, callback_attr, None)
        if callable(callback):
            wrapped = _wrap_command_callback(callback)
            if wrapped is not callback:
                try:
                    setattr(command, callback_attr, wrapped)
                except Exception as exc:
                    _log.debug("Failed to instrument command callback (%s): %s", callback_attr, exc)

    for subcommand in list(getattr(command, "commands", ())):
        _instrument_command(subcommand)


def _instrument_registered_commands(guild: discord.Object | None):
    try:
        commands = tree.get_commands(guild=guild)
    except Exception:
        commands = []
    for command in commands:
        _instrument_command(command)

def _guild_obj():
    return discord.Object(id=config.GUILD_ID) if config.GUILD_ID else None

async def _register_and_sync_commands(*, reload_modules: bool = False):
    global help_cmd, highscore, tank, backup_cmd, health, backup, wg_sync, tank_name_sync

    if reload_modules:
        # Reload dependency modules first so command modules bind fresh imports.
        from . import config as _config, utils as _utils, forum_index as _forum_index
        from . import db as _db
        from . import backup as _backup, wg_sync as _wg_sync, tank_name_sync as _tank_name_sync
        _config = importlib.reload(_config)
        _utils = importlib.reload(_utils)
        _forum_index = importlib.reload(_forum_index)
        _db = importlib.reload(_db)
        _backup = importlib.reload(_backup)
        _wg_sync = importlib.reload(_wg_sync)
        _tank_name_sync = importlib.reload(_tank_name_sync)
        backup = _backup
        wg_sync = _wg_sync
        tank_name_sync = _tank_name_sync

        help_cmd = importlib.reload(help_cmd)
        highscore = importlib.reload(highscore)
        tank = importlib.reload(tank)
        backup_cmd = importlib.reload(backup_cmd)
        health = importlib.reload(health)

    # Defensive fix: runtime reload can hit MissingApplicationID on some sessions.
    # Ensure the running client has an application_id before tree.sync().
    if bot.application_id is None and bot.user is not None:
        try:
            app_info = await bot.application_info()
            bot._connection.application_id = int(app_info.id)  # type: ignore[attr-defined]
        except Exception:
            # Let sync raise the original error if resolution fails.
            pass

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
    _instrument_registered_commands(guild)

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
    if not wg_sync.daily_clan_sync_loop.is_running():
        wg_sync.daily_clan_sync_loop.start(bot)
    if not tank_name_sync.monthly_tank_sync_loop.is_running():
        tank_name_sync.monthly_tank_sync_loop.start(bot)

    await _register_and_sync_commands(reload_modules=False)
    await wg_sync.bootstrap_if_needed()
    await tank_name_sync.bootstrap_if_needed()

    try:
        await static_site.generate_leaderboard_page()
    except Exception:
        logging.getLogger(__name__).exception("Failed to generate static leaderboard page at startup")

    logging.getLogger(__name__).info("Logged in as %s (id=%s)", bot.user, bot.user.id)

def run():
    if not config.DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN is missing")
    bot.run(config.DISCORD_TOKEN)
