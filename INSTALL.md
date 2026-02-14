# Tank Highscore Bot Installation

This guide is for installing and running the bot from this repository.

## Prerequisites

- Python `3.11+`
- A Discord bot token
- A Discord server where the bot is installed with `applications.commands`

## 1) Clone and enter the project

```bash
git clone <your-repo-url> tankbot-test
cd tankbot-test
```

## 2) Create `.env`

Create `~/tankbot-test/.env`:

```env
DISCORD_TOKEN=YOUR_BOT_TOKEN
GUILD_ID=YOUR_GUILD_ID

TANK_INDEX_FORUM_CHANNEL_ID=YOUR_FORUM_CHANNEL_ID
ANNOUNCE_CHANNEL_ID=YOUR_ANNOUNCE_CHANNEL_ID
BACKUP_CHANNEL_ID=YOUR_BACKUP_CHANNEL_ID

COMMANDER_ROLE_NAME=Clan Commander
COMMANDER_ROLE_ID=ROLE_ID
MAX_SCORE=100000
DB_PATH=highscores.db

BACKUP_WEEKDAY=6
BACKUP_HOUR=3
BACKUP_MINUTE=0
BACKUP_TZ=Europe/Helsinki

# Optional WG clan player sync
WG_API_APPLICATION_ID=
WG_API_GAME=wotb
WG_API_REGION=eu
WG_CLAN_IDS=
WG_REFRESH_HOUR=4
WG_REFRESH_MINUTE=0
WG_REFRESH_TZ=Europe/Helsinki
WG_API_TIMEOUT_SECONDS=15

# Optional
BACKUP_GUILD_ID=
BACKUP_ENCRYPTION_PASSPHRASE=
BACKUP_ENCRYPTION_SALT=
LOG_LEVEL=INFO
LOG_PATH=tankbot.log
```

`COMMANDER_ROLE_ID` takes precedence; `COMMANDER_ROLE_NAME` is used only when `COMMANDER_ROLE_ID=0`.

## 3) Install dependencies and start

Use the included startup script:

```bash
./startup.sh
```

What this does:
- Verifies Python version (`3.11+`)
- Creates `venv` if missing
- Installs `requirements.txt`
- Starts `bot.py` in the background
- Writes PID to `~/tankbot-test/tankbot.pid`
- Writes logs to `~/tankbot-test/tankbot.log`

## 4) Stop the bot

```bash
./stop.sh
```

This reads `~/tankbot-test/tankbot.pid`, validates it is the bot process, and stops it safely.

## 5) Check status and logs

```bash
cat tankbot.pid
tail -f tankbot.log
```

If `tankbot.pid` exists but the process is gone, running `./startup.sh` or `./stop.sh` will clean stale PID state.

## 6) First-time Discord setup

Run these admin commands after startup:

```text
/tank import_csv
/tank rebuild_index
```

Then verify backup flow:

```text
/backup run_now
/backup verify_latest
```

Optional WG sync check:

```text
/highscore refresh_players
```
