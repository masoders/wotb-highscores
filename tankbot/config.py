import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))

TANK_INDEX_FORUM_CHANNEL_ID = int(os.getenv("TANK_INDEX_FORUM_CHANNEL_ID", "0"))
ANNOUNCE_CHANNEL_ID = int(os.getenv("ANNOUNCE_CHANNEL_ID", "0"))

COMMANDER_ROLE_NAME = os.getenv("COMMANDER_ROLE_NAME", "Clan Commander")
MAX_SCORE = int(os.getenv("MAX_SCORE", "100000"))

DB_PATH = os.getenv("DB_PATH", "highscores.db")

# Backups
BACKUP_CHANNEL_ID = int(os.getenv("BACKUP_CHANNEL_ID", "0"))
BACKUP_GUILD_ID = int(os.getenv("BACKUP_GUILD_ID", "0"))  # optional admin server
BACKUP_WEEKDAY = int(os.getenv("BACKUP_WEEKDAY", "6"))     # 0=Mon .. 6=Sun
BACKUP_HOUR = int(os.getenv("BACKUP_HOUR", "3"))
BACKUP_MINUTE = int(os.getenv("BACKUP_MINUTE", "0"))
BACKUP_TZ = os.getenv("BACKUP_TZ", "Europe/Helsinki")

# Encryption (optional)
BACKUP_ENCRYPTION_PASSPHRASE = os.getenv("BACKUP_ENCRYPTION_PASSPHRASE", "")
BACKUP_ENCRYPTION_SALT = os.getenv("BACKUP_ENCRYPTION_SALT", "")  # base64 urlsafe salt (optional; generated if empty)
BACKUP_VERIFY_MAX_ATTACHMENT_BYTES = max(1, int(os.getenv("BACKUP_VERIFY_MAX_ATTACHMENT_BYTES", str(25 * 1024 * 1024))))
BACKUP_VERIFY_MAX_ZIP_BYTES = max(1, int(os.getenv("BACKUP_VERIFY_MAX_ZIP_BYTES", str(50 * 1024 * 1024))))
BACKUP_VERIFY_MAX_DB_BYTES = max(1, int(os.getenv("BACKUP_VERIFY_MAX_DB_BYTES", str(50 * 1024 * 1024))))

# Dashboard (read-only web UI)
DASHBOARD_ENABLED = os.getenv("DASHBOARD_ENABLED", "0") in ("1", "true", "True", "yes", "YES")
DASHBOARD_BIND = os.getenv("DASHBOARD_BIND", "127.0.0.1")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8080"))
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")  # optional bearer token for access

# Static leaderboard webpage
WEB_LEADERBOARD_ENABLED = os.getenv("WEB_LEADERBOARD_ENABLED", "1") in ("1", "true", "True", "yes", "YES")
WEB_OUTPUT_PATH = os.getenv("WEB_OUTPUT_PATH", "web/leaderboard.html")
WEB_CLAN_NAME = os.getenv("WEB_CLAN_NAME", "Tank Highscore Clan")
WEB_CLAN_MOTTO = os.getenv("WEB_CLAN_MOTTO", "")
WEB_BANNER_URL = os.getenv("WEB_BANNER_URL", "")
