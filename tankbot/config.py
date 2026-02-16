import os
import re
from dotenv import load_dotenv

load_dotenv()

def _int_env(key: str, default: int) -> int:
    raw = os.getenv(key, str(default))
    try:
        return int((raw or "").strip())
    except Exception:
        return int(default)

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
GUILD_ID = _int_env("GUILD_ID", 0)

TANK_INDEX_FORUM_CHANNEL_ID = _int_env("TANK_INDEX_FORUM_CHANNEL_ID", 0)
TANK_INDEX_NORMAL_CHANNEL_ID = _int_env("TANK_INDEX_NORMAL_CHANNEL_ID", 0)
ANNOUNCE_CHANNEL_ID = _int_env("ANNOUNCE_CHANNEL_ID", 0)
AUDIT_LOG_CHANNEL_ID = _int_env("AUDIT_LOG_CHANNEL_ID", 0)

COMMANDER_ROLE_NAME = os.getenv("COMMANDER_ROLE_NAME", "Clan Commander")
COMMANDER_ROLE_ID = _int_env("COMMANDER_ROLE_ID", 0)
MAX_SCORE = _int_env("MAX_SCORE", 100000)

DB_PATH = os.getenv("DB_PATH", "highscores.db")

# Backups
BACKUP_CHANNEL_ID = _int_env("BACKUP_CHANNEL_ID", 0)
BACKUP_GUILD_ID = _int_env("BACKUP_GUILD_ID", 0)  # optional admin server
BACKUP_WEEKDAY = _int_env("BACKUP_WEEKDAY", 6)     # 0=Mon .. 6=Sun
BACKUP_HOUR = _int_env("BACKUP_HOUR", 3)
BACKUP_MINUTE = _int_env("BACKUP_MINUTE", 0)
BACKUP_TZ = os.getenv("BACKUP_TZ", "Europe/Helsinki")

# Encryption (optional)
BACKUP_ENCRYPTION_PASSPHRASE = os.getenv("BACKUP_ENCRYPTION_PASSPHRASE", "")
BACKUP_ENCRYPTION_SALT = os.getenv("BACKUP_ENCRYPTION_SALT", "")  # base64 urlsafe salt (optional; generated if empty)
BACKUP_VERIFY_MAX_ATTACHMENT_BYTES = max(1, _int_env("BACKUP_VERIFY_MAX_ATTACHMENT_BYTES", 25 * 1024 * 1024))
BACKUP_VERIFY_MAX_ZIP_BYTES = max(1, _int_env("BACKUP_VERIFY_MAX_ZIP_BYTES", 50 * 1024 * 1024))
BACKUP_VERIFY_MAX_DB_BYTES = max(1, _int_env("BACKUP_VERIFY_MAX_DB_BYTES", 50 * 1024 * 1024))

# Dashboard (read-only web UI)
DASHBOARD_ENABLED = os.getenv("DASHBOARD_ENABLED", "0") in ("1", "true", "True", "yes", "YES")
DASHBOARD_BIND = os.getenv("DASHBOARD_BIND", "127.0.0.1")
DASHBOARD_PORT = _int_env("DASHBOARD_PORT", 8080)
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")  # optional bearer token for access

# Static leaderboard webpage
WEB_LEADERBOARD_ENABLED = os.getenv("WEB_LEADERBOARD_ENABLED", "1") in ("1", "true", "True", "yes", "YES")
WEB_OUTPUT_PATH = os.getenv("WEB_OUTPUT_PATH", "web/leaderboard.html")
WEB_CLAN_NAME = os.getenv("WEB_CLAN_NAME", "Tank Highscore Clan")
WEB_CLAN_MOTTO = os.getenv("WEB_CLAN_MOTTO", "")
WEB_CLAN_DESCRIPTION = os.getenv("WEB_CLAN_DESCRIPTION", "")
WEB_BANNER_URL = os.getenv("WEB_BANNER_URL", "")
_web_clan_name_case = os.getenv("WEB_CLAN_NAME_CASE", "normal").strip().lower()
WEB_CLAN_NAME_CASE = _web_clan_name_case if _web_clan_name_case in {"normal", "uppercase"} else "normal"
_web_clan_name_align = os.getenv("WEB_CLAN_NAME_ALIGN", "center").strip().lower()
WEB_CLAN_NAME_ALIGN = _web_clan_name_align if _web_clan_name_align in {"center", "left"} else "center"
_web_font_mode = os.getenv("WEB_FONT_MODE", "sans").strip().lower()
WEB_FONT_MODE = _web_font_mode if _web_font_mode in {"sans", "monospace"} else "sans"


def _web_hex_color(key: str, default: str) -> str:
    value = os.getenv(key, default).strip()
    if re.fullmatch(r"#[0-9a-fA-F]{6}", value):
        return value
    return default


WEB_BG_COLOR = _web_hex_color("WEB_BG_COLOR", "#0b1221")
WEB_FONT_COLOR = _web_hex_color("WEB_FONT_COLOR", "#ecf1ff")
WEB_DAMAGE_COLOR = _web_hex_color("WEB_DAMAGE_COLOR", "#6ef0b6")
WEB_TANK_NAME_COLOR = _web_hex_color("WEB_TANK_NAME_COLOR", "#ecf1ff")
WEB_PLAYER_NAME_COLOR = _web_hex_color("WEB_PLAYER_NAME_COLOR", "#ecf1ff")
WEB_CLAN_NAME_COLOR = _web_hex_color("WEB_CLAN_NAME_COLOR", "#ecf1ff")
WEB_MOTTO_COLOR = _web_hex_color("WEB_MOTTO_COLOR", "#adc0ea")
WEB_LEADERBOARD_COLOR = _web_hex_color("WEB_LEADERBOARD_COLOR", "#f1f6ff")


def _csv_ints(value: str) -> list[int]:
    out: list[int] = []
    for raw in (value or "").split(","):
        item = raw.strip()
        if not item:
            continue
        try:
            out.append(int(item))
        except Exception:
            continue
    return out


# Wargaming API clan sync
WG_API_APPLICATION_ID = os.getenv("WG_API_APPLICATION_ID", "").strip()
_wg_game = os.getenv("WG_API_GAME", "wotb").strip().lower()
WG_API_GAME = _wg_game if _wg_game in {"wot", "wotb"} else "wotb"
_wg_region = os.getenv("WG_API_REGION", "eu").strip().lower()
WG_API_REGION = _wg_region if _wg_region in {"eu", "na", "com", "asia"} else "eu"
WG_CLAN_IDS = _csv_ints(os.getenv("WG_CLAN_IDS", ""))
WG_REFRESH_HOUR = max(0, min(23, _int_env("WG_REFRESH_HOUR", 4)))
WG_REFRESH_MINUTE = max(0, min(59, _int_env("WG_REFRESH_MINUTE", 0)))
WG_REFRESH_TZ = os.getenv("WG_REFRESH_TZ", "UTC").strip() or "UTC"
WG_API_TIMEOUT_SECONDS = max(5, min(60, _int_env("WG_API_TIMEOUT_SECONDS", 15)))
WG_SYNC_ENABLED = bool(WG_API_APPLICATION_ID and WG_CLAN_IDS)

# WG Blitz encyclopedia tank-name sync (suggestions)
WG_TANKS_API_APPLICATION_ID = os.getenv(
    "WG_TANKS_API_APPLICATION_ID",
    "c9daca4281064c19f93e714acd0a6967",
).strip()
_wg_tanks_region = os.getenv("WG_TANKS_API_REGION", "eu").strip().lower()
WG_TANKS_API_REGION = _wg_tanks_region if _wg_tanks_region in {"eu", "na", "com", "asia"} else "eu"
WG_TANKS_SYNC_DAY = max(1, min(28, _int_env("WG_TANKS_SYNC_DAY", 1)))
WG_TANKS_SYNC_HOUR = max(0, min(23, _int_env("WG_TANKS_SYNC_HOUR", 4)))
WG_TANKS_SYNC_MINUTE = max(0, min(59, _int_env("WG_TANKS_SYNC_MINUTE", 10)))
WG_TANKS_SYNC_TZ = os.getenv("WG_TANKS_SYNC_TZ", "UTC").strip() or "UTC"
WG_TANKS_API_TIMEOUT_SECONDS = max(5, min(60, _int_env("WG_TANKS_API_TIMEOUT_SECONDS", 20)))
_wg_tanks_sync_enabled_raw = os.getenv("WG_TANKS_SYNC_ENABLED", "1").strip().lower()
WG_TANKS_SYNC_ENABLED = (
    _wg_tanks_sync_enabled_raw in {"1", "true", "yes", "on"}
    and bool(WG_TANKS_API_APPLICATION_ID)
)
