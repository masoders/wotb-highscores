# Tank Highscore Bot (Discord) — DB-only

This bot maintains:
- a **global highscore leaderboard** (filterable by tier/type)
- an **admin-governed tank roster** stored in SQLite
- a **WG clan player sync** (daily + manual refresh) for player-name tracking/autocomplete
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
1) Choose one index destination:
   - Forum mode: create a **Forum channel** (e.g. `#tank-index`).
   - Normal mode: create a **Text channel** (e.g. `#tank-index`).
2) If using forum mode, create tags in that forum:
   - Tier 1 .. Tier 10
   - Light, Medium, Heavy, Tank Destroyer
3) Invite the bot with permissions on the chosen index channel:
   - Manage Threads (forum mode)
   - Manage Messages
   - Read Message History
   - Send Messages

## .env
```env
DISCORD_TOKEN=
GUILD_ID=
ANNOUNCE_CHANNEL_ID=
AUDIT_LOG_CHANNEL_ID=
TANK_INDEX_FORUM_CHANNEL_ID=
TANK_INDEX_NORMAL_CHANNEL_ID=
BACKUP_CHANNEL_ID=
COMMANDER_ROLE_ID=
COMMANDER_ROLE_NAME=
MAX_SCORE=
DB_PATH=

WG_API_APPLICATION_ID=
WG_API_GAME=
WG_API_REGION=
WG_CLAN_IDS=
WG_REFRESH_HOUR=
WG_REFRESH_MINUTE=
WG_REFRESH_TZ=
WG_API_TIMEOUT_SECONDS=

WG_TANKS_SYNC_ENABLED=
WG_TANKS_API_APPLICATION_ID=
WG_TANKS_API_REGION=
WG_TANKS_SYNC_DAY=
WG_TANKS_SYNC_HOUR=
WG_TANKS_SYNC_MINUTE=
WG_TANKS_SYNC_TZ=
WG_TANKS_API_TIMEOUT_SECONDS=
WG_TANKS_WEBPAGE_NAME=

WG_TANKOPEDIA_REGION=
WG_TANKOPEDIA_LANGUAGE=
WG_TANKOPEDIA_SYNC_ENABLED=
WG_TANKOPEDIA_SYNC_INTERVAL_HOURS=

BACKUP_WEEKDAY=
BACKUP_HOUR=
BACKUP_MINUTE=
BACKUP_TZ=
```

`TANK_INDEX_NORMAL_CHANNEL_ID` is optional. If set, the bot also posts index snapshots as normal text messages in that channel. If both `TANK_INDEX_NORMAL_CHANNEL_ID` and `TANK_INDEX_FORUM_CHANNEL_ID` are set, both destinations are kept updated.

`COMMANDER_ROLE_ID` takes precedence; `COMMANDER_ROLE_NAME` is used only when `COMMANDER_ROLE_ID` is not set.

## WG Player Sync
Configure WG API and one or more clan IDs in `.env`.

What it does:
- refreshes tracked clan players once per day automatically
- allows commanders to force refresh with `/highscore refresh_players`
- removes players who left tracked clans
- stores renamed players as canonical names
- powers player-name autocomplete in score commands

If you want health output and schedules to look local, set `WG_REFRESH_TZ` and `BACKUP_TZ` to your local timezone.

## WG Tank Name Sync
What it does:
- refreshes WG Blitz encyclopedia tank names once per month
- allows admins to force refresh with `/system sync_tanks`
- stores names in DB for tank-name suggestions in commands

If you want this schedule to look local in health output, set `WG_TANKS_SYNC_TZ`.

## Tankopedia Browser (Static)
Command:
```bash
python -m tankbot.tools.sync_tankopedia
```

What it does:
- checks WG `tanks_updated_at` via `/wotb/encyclopedia/info/`
- skips fetch when unchanged (`unchanged; skipped`)
- fetches vehicles from `/wotb/encyclopedia/vehicles/`
- stores full tank payload (including vehicle characteristics) in SQLite
- generates static browser files:
  - `tanks/index.html`
  - `tanks/app.js`
  - `tanks/styles.css`
  - `tanks/tanks.json`

Output HTML filename is controlled by:
- `WG_TANKS_WEBPAGE_NAME`
  - Value must include directory and filename (example: `tanks/index.html`)

Optional flags:
```bash
python -m tankbot.tools.sync_tankopedia --force --output-dir tanks
```

## Commands
See `docs.md`.

## Static Leaderboard Webpage
Generated as a static HTML file and refreshed on score/tank updates.
Manual refresh command (commander): `/highscore refresh_web`

```env
WEB_LEADERBOARD_ENABLED=
WEB_OUTPUT_PATH=
WEB_CLAN_NAME=
WEB_CLAN_MOTTO=
WEB_BANNER_URL=
```

## Backups
Enable weekly backups posted to a locked channel:
```env
BACKUP_CHANNEL_ID=
BACKUP_WEEKDAY=
BACKUP_HOUR=
BACKUP_MINUTE=
BACKUP_TZ=
```

Commander commands:
- `/backup run_now`
- `/backup status`
- `/backup verify_latest`

```env
BACKUP_GUILD_ID=
```


### /highscore qualify
Check whether a given score would set a new record for a tank (no submission). Shows current tank record, delta, and whether it would beat the global champion.


### /help
Shows commands available to you based on your role (public, commander, admin).


## Read-only Web Dashboard
```env
DASHBOARD_ENABLED=
DASHBOARD_BIND=
DASHBOARD_PORT=
DASHBOARD_TOKEN=
```


## Encrypted Backups (Optional)
```env
BACKUP_ENCRYPTION_PASSPHRASE=
BACKUP_ENCRYPTION_SALT=
```
Decrypt helper: `decrypt_backup.py`.


## Reverse Proxy (Caddy)
Use `Caddyfile.dashboard.example` as a starting point. Keep the dashboard bound to loopback and expose it only via HTTPS reverse proxy.


## Dashboard Security
- Set `DASHBOARD_TOKEN` to require bearer-token access for dashboard endpoints.
- Use your configured dashboard token auth mechanism when token protection is enabled.
- Keep `DASHBOARD_BIND` on loopback and expose via HTTPS reverse proxy only.


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
LOG_LEVEL=
LOG_PATH=
```


## Backup verification
Commander command:
- `/backup verify_latest` — downloads the newest backup attachment in the backup channel and runs `PRAGMA integrity_check`.
Encrypted backups require `BACKUP_ENCRYPTION_PASSPHRASE` to be set.
