import logging
from datetime import datetime, timezone

import discord

from . import config

logger = logging.getLogger(__name__)


def _audit_channel_id() -> int:
    return int(getattr(config, "AUDIT_LOG_CHANNEL_ID", 0) or 0)


async def _resolve_channel(bot: discord.Client) -> discord.abc.Messageable | None:
    channel_id = _audit_channel_id()
    if channel_id <= 0:
        return None
    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except Exception:
            logger.warning("Failed to fetch audit log channel id=%s", channel_id, exc_info=True)
            return None
    if not isinstance(channel, discord.abc.Messageable):
        logger.warning("Audit log channel id=%s is not messageable", channel_id)
        return None
    return channel


async def send(bot: discord.Client, message: str) -> bool:
    channel = await _resolve_channel(bot)
    if channel is None:
        return False
    try:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        payload = f"[{ts}] {message}"
        await channel.send(payload[:1900], allowed_mentions=discord.AllowedMentions.none())
        return True
    except Exception:
        logger.warning("Failed to send audit log message", exc_info=True)
        return False
