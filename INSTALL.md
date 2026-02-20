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
DISCORD_TOKEN=
GUILD_ID=

TANK_INDEX_FORUM_CHANNEL_ID=
TANK_INDEX_NORMAL_CHANNEL_ID=
ANNOUNCE_CHANNEL_ID=
BACKUP_CHANNEL_ID=

COMMANDER_ROLE_NAME=
COMMANDER_ROLE_ID=
MAX_SCORE=
DB_PATH=

BACKUP_WEEKDAY=
BACKUP_HOUR=
BACKUP_MINUTE=
BACKUP_TZ=

# Optional WG clan player sync
WG_API_APPLICATION_ID=
WG_API_GAME=
WG_API_REGION=
WG_CLAN_IDS=
WG_REFRESH_HOUR=
WG_REFRESH_MINUTE=
WG_REFRESH_TZ=
WG_API_TIMEOUT_SECONDS=

# Optional
BACKUP_GUILD_ID=
BACKUP_ENCRYPTION_PASSPHRASE=
BACKUP_ENCRYPTION_SALT=
LOG_LEVEL=
LOG_PATH=
```

`COMMANDER_ROLE_ID` takes precedence; `COMMANDER_ROLE_NAME` is used only when `COMMANDER_ROLE_ID` is not set.
If `TANK_INDEX_NORMAL_CHANNEL_ID` is set, index snapshots are also posted in that text channel. If both index channel IDs are set, both destinations are populated.

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
