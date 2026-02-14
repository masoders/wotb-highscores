# Commands

Legend:
- Required params: `<param>`
- Optional params: `[param]`
- Types: `type=light|medium|heavy|td`, `tier=1..10`

## Public
- `/help`
  - Description: Show commands you can use based on your role.
  - Example: `/help`
- `/highscore show [tier] [type]`
  - Description: Show current champion (global or filtered).
  - Example: `/highscore show tier:10 type:heavy`
- `/highscore qualify <tank> <score> [player]`
  - Description: Check if a score would become #1 for that tank (no submission).
  - Example: `/highscore qualify tank:"Tiger II" score:3120 player:"PlayerOne"`
- `/highscore history [limit]`
  - Description: Show recent submissions and stats.
  - Example: `/highscore history limit:10`

## Commander
- `/highscore submit <player> <tank> <score>`
  - Description: Submit a new highscore (must beat current tank record). Player names are normalized and existing canonical names are reused.
  - Example: `/highscore submit player:"PlayerOne" tank:"Tiger II" score:3120`
- `/highscore edit <submission_id> <score>`
  - Description: Edit an existing submission score by id.
  - Example: `/highscore edit submission_id:245 score:7210`
- `/highscore delete <submission_id>`
  - Description: Delete an existing submission by id.
  - Example: `/highscore delete submission_id:245`
- `/highscore refresh_web`
  - Description: Regenerate static leaderboard webpage manually.
  - Example: `/highscore refresh_web`
- `/highscore refresh_players`
  - Description: Refresh WG clan player list now and show added/removed/renamed names.
  - Example: `/highscore refresh_players`
- `/tank add <name> <tier> <type>`
  - Description: Add a tank to roster.
  - Example: `/tank add name:"Tiger II" tier:8 type:heavy`
- `/tank edit <name> <tier> <type>`
  - Description: Edit a tank's tier/type.
  - Example: `/tank edit name:"Tiger II" tier:8 type:heavy`
- `/tank remove <name>`
  - Description: Remove a tank (only if no submissions exist).
  - Example: `/tank remove name:"Tiger II"`
- `/tank list [tier] [type]`
  - Description: List tanks with optional filters.
  - Example: `/tank list tier:8 type:heavy`
- `/tank export_csv`
  - Description: Export tank roster as CSV.
  - Example: `/tank export_csv`
- `/tank export_scores_csv`
  - Description: Export current best score per tank as CSV.
  - Example: `/tank export_scores_csv`
- `/backup run_now`
  - Description: Run database backup immediately.
  - Example: `/backup run_now`
- `/backup status`
  - Description: Show backup schedule and latest status.
  - Example: `/backup status`
- `/backup verify_latest [scan_limit]`
  - Description: Verify integrity of latest backup file in backup channel.
  - Example: `/backup verify_latest scan_limit:80`

## Admin (Manage Server or Administrator)
- `/highscore import_scores <file> [dry_run] [confirm] [update_index]`
  - Description: Import historical scores from CSV.
  - Example (preview): `/highscore import_scores file:import.csv dry_run:true`
  - Example (apply): `/highscore import_scores file:import.csv dry_run:false confirm:YES update_index:true`
- `/highscore changes [limit]`
  - Description: Show score audit trail (add/edit/delete with actor and timestamps).
  - Example: `/highscore changes limit:20`
- `/tank changes [limit]`
  - Description: Show tank change log.
  - Example: `/tank changes limit:20`
- `/tank preview_import <csv_file> [delete_missing]`
  - Description: Preview roster CSV changes without applying.
  - Example: `/tank preview_import csv_file:tanks.csv delete_missing:false`
- `/tank import_csv <file>`
  - Description: Import tanks from CSV (`name,tier,type`).
  - Example: `/tank import_csv file:tanks.csv`
- `/tank rebuild_index`
  - Description: Rebuild all forum index threads.
  - Example: `/tank rebuild_index`
- `/tank rebuild_index_missing`
  - Description: Create/repair missing index threads only.
  - Example: `/tank rebuild_index_missing`
- `/system health`
  - Description: Show runtime, DB, backup, WG refresh, and dashboard health.
  - Example: `/system health`
- `/system reload`
  - Description: Reload command modules and sync slash commands.
  - Example: `/system reload`

# Forum Index Rules
- One thread per Tier (1..10) + Type (light/medium/heavy/td)
- Thread title enforced: `Tier N – <Type>`
- Starter post updated and pinned on changes
- Thread locked (read-only)
- Tags enforced: `Tier N` + `<Type>`

# Notes
- If tags can't be created by the API/version, the bot will skip tag creation and still function.
- If the bot lacks Manage Messages, it won't be able to pin/edit; it will fall back to sending messages.

## History Output
- Grouped by Tank Type → Tier
- Highlights the current global champion with 🏆 TOP
- Includes stats:
  - Most #1 tanks
  - Most #1 Tier×Type buckets


## Backup (Commander)
- `/backup run_now` — run an immediate DB backup and post to backup channel
- `/backup status` — show schedule and next run

Backups require env vars: BACKUP_CHANNEL_ID, BACKUP_WEEKDAY, BACKUP_HOUR, BACKUP_MINUTE, BACKUP_TZ

## WG Clan Player Sync
Required env vars:
- `WG_API_APPLICATION_ID`
- `WG_API_REGION` (`eu|na|com|asia`)
- `WG_CLAN_IDS` (comma-separated clan ids)

Optional env vars:
- `WG_API_GAME` (`wotb` default)
- `WG_REFRESH_HOUR`, `WG_REFRESH_MINUTE`, `WG_REFRESH_TZ`
- `WG_API_TIMEOUT_SECONDS`

Behavior:
- Daily automatic refresh from WG API.
- Commander-triggered refresh via `/highscore refresh_players`.
- Players who left tracked clans are removed from `clan_players`.
- Player-name autocomplete in `/highscore submit|edit|qualify` uses tracked clan players first.


Admin Server Option:
- Set BACKUP_GUILD_ID to post backups to a separate admin server
- If unset, backups post to the active server


### /highscore qualify
Check whether a given score would set a new record for a tank (no submission). Shows current tank record, delta, and whether it would beat the global champion.


### /help
Shows commands available to you based on your role (public, commander, admin).


## Web Dashboard (Read-only)
Enable with env vars:
```env
DASHBOARD_ENABLED=1
DASHBOARD_BIND=127.0.0.1
DASHBOARD_PORT=8080
DASHBOARD_TOKEN=   # optional
```
Endpoints: `/` overview, `/tanks`, `/recent`.
If DASHBOARD_TOKEN is set, use `Authorization: Bearer <token>` or `?token=`.

## Static Leaderboard Webpage
The bot can generate a modern static webpage grouped as:
- main level: Tier
- sub level: Tank type

It is auto-updated whenever scores/tanks are changed and can be manually refreshed with:
- `/highscore refresh_web` (commander only)

Configure via env vars:
```env
WEB_LEADERBOARD_ENABLED=1
WEB_OUTPUT_PATH=web/leaderboard.html
WEB_CLAN_NAME=Your Clan Name
WEB_CLAN_MOTTO=Victory through discipline
WEB_BANNER_URL=https://example.com/clan-banner.jpg
```


## Encrypted Backups (Optional)
Set:
```env
BACKUP_ENCRYPTION_PASSPHRASE=your-strong-passphrase
BACKUP_ENCRYPTION_SALT=   # optional; will be generated and printed as SALT_B64
```
Backups will be uploaded as `.zip.enc` with a note containing `SALT_B64`.
To decrypt: use `decrypt_backup.py`.


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
