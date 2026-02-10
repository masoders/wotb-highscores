# Commands

## Highscore
- `/highscore submit player tank score`
  - Commander-only (role name configured by `COMMANDER_ROLE_NAME`)
- `/highscore show [tier] [type]`
- `/highscore history [limit]`

## Tank admin
(Admin = Manage Server or Administrator)
- `/tank add name tier type`
- `/tank edit name tier type`
- `/tank remove name` (blocked if submissions exist for tank)
- `/tank list [tier] [type]`
- `/tank changes [limit]`
- `/tank export_csv`
- `/tank preview_import csv_file [delete_missing]`
- `/tank import_csv csv_file [delete_missing]` (diff-logged)
- `/tank rebuild_index` (force rebuild all)
- `/tank rebuild_index_missing` (create/repair missing; validates thread existence and parent)

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


## Backup (Admin)
- `/backup run_now` — run an immediate DB backup and post to backup channel
- `/backup status` — show schedule and next run

Backups require env vars: BACKUP_CHANNEL_ID, BACKUP_WEEKDAY, BACKUP_HOUR, BACKUP_MINUTE, BACKUP_TZ


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
