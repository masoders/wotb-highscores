# Tank Highscore Bot (Discord) — DB-only

This bot maintains:
- a **global highscore leaderboard** (filterable by tier/type)
- an **admin-governed tank roster** stored in SQLite
- a **static leaderboard webpage** (Tier -> Type -> Tank) regenerated on updates
- an **indexed Forum channel** with one thread per Tier+Type:
  - canonical thread titles enforced
  - starter post updated + pinned
  - threads locked (read-only)
  - auto-tagging by Tier and Type
  - targeted updates (only affected threads updated)

## Requirements
- Python 3.10+ (3.11 recommended)
- discord.py 2.3+
- aiosqlite, python-dotenv

## Installation
For repository-based installation and process management (`startup.sh` / `stop.sh`), see `INSTALL.md`.

Install:
```bash
pip install -U discord.py aiosqlite python-dotenv
```

## Discord Setup
1) Create a **Forum channel** for the tank index (e.g. `#tank-index`).
2) Create tags in that forum:
   - Tier 1 .. Tier 10
   - Light, Medium, Heavy, Tank Destroyer
3) Invite the bot with permissions on that forum:
   - Manage Threads
   - Manage Messages
   - Read Message History
   - Send Messages

## .env
```env
DISCORD_TOKEN=YOUR_TOKEN
GUILD_ID=YOUR_GUILD_ID
ANNOUNCE_CHANNEL_ID=CHANNEL_FOR_CHAMPION_ANNOUNCEMENTS
AUDIT_LOG_CHANNEL_ID=PRIVATE_LOG_CHANNEL_ID
TANK_INDEX_FORUM_CHANNEL_ID=FORUM_CHANNEL_ID
COMMANDER_ROLE_NAME=Clan Commander
MAX_SCORE=100000
DB_PATH=highscores.db
TANKS_SEED_CSV_PATH=tanks.csv
```

## First run seeding (CSV seed-only)
If `tanks` table is empty, the bot will seed tanks from `tanks.csv` (header: name,tier,type).
After that, the roster is DB-only (managed via commands).

Example `tanks.csv`:
```csv
name,tier,type
Tiger II,8,heavy
Leopard 1,10,medium
SU-100,6,td
```

## Commands
See `docs.md`.

## Static Leaderboard Webpage
Generated as a static HTML file and refreshed on score/tank updates.
Manual refresh command (commander): `/highscore refresh_web`

```env
WEB_LEADERBOARD_ENABLED=1
WEB_OUTPUT_PATH=web/leaderboard.html
WEB_CLAN_NAME=Your Clan Name
WEB_CLAN_MOTTO=Victory through discipline
WEB_BANNER_URL=
```

## Backups
Enable weekly backups posted to a locked channel:
```env
BACKUP_CHANNEL_ID=
BACKUP_WEEKDAY=6
BACKUP_HOUR=3
BACKUP_MINUTE=0
BACKUP_TZ=Europe/Helsinki
```

Admin commands:
- `/backup run_now`
- `/backup status`

```env
BACKUP_GUILD_ID=   # Optional admin/backup server ID
```


### /highscore qualify
Check whether a given score would set a new record for a tank (no submission). Shows current tank record, delta, and whether it would beat the global champion.


### /help
Shows commands available to you based on your role (public, commander, admin).


## Read-only Web Dashboard
```env
DASHBOARD_ENABLED=1
DASHBOARD_BIND=127.0.0.1
DASHBOARD_PORT=8080
DASHBOARD_TOKEN=
```


## Encrypted Backups (Optional)
```env
BACKUP_ENCRYPTION_PASSPHRASE=
BACKUP_ENCRYPTION_SALT=
```
Decrypt helper: `decrypt_backup.py`.


## Reverse Proxy (Caddy)
Use `Caddyfile.dashboard.example` as a starting point. Keep the dashboard bound to `127.0.0.1` and expose it only via HTTPS reverse proxy.


## Dashboard Security
- **Strict mode:** dashboard refuses to start unless `DASHBOARD_TOKEN` is set.
- **Auth:** `Authorization: Bearer <token>` or `?token=`.
- **Rate limiting:** 60 requests / 60 seconds per IP (in-memory).
- **Health endpoint:** `/healthz` (still requires token in strict mode).


## Self-contained encrypted backups
When encryption is enabled, backups are uploaded as `.zip.enc` with an embedded header that contains the salt. You can decrypt using `decrypt_backup.py --in <file>.enc --out <file>.zip --passphrase <pass>`.


## Scheduled backup guild fallback
If `BACKUP_GUILD_ID` is not set, scheduled backups will use `GUILD_ID` (recommended). For multi-guild usage, set `BACKUP_GUILD_ID` explicitly.


## Backup reliability
Backups are created using SQLite's **backup API** to ensure a consistent snapshot even while the bot is running.


## Input limits
Tank names and player names are limited to **64 characters** and must be single-line (no control characters).


## Logging
Logs go to console and to a rotating file `tankbot.log` (1MB x 5). Configure via:
```env
LOG_LEVEL=INFO
LOG_PATH=tankbot.log
```


## Backup verification
Admin command:
- `/backup verify_latest` — downloads the newest backup attachment in the backup channel and runs `PRAGMA integrity_check`.
Encrypted backups require `BACKUP_ENCRYPTION_PASSPHRASE` to be set.
