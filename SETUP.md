# Tank Highscore Bot — Complete Setup Guide

**Version:** repository build  
**Audience:** Discord server admins  
**Python:** 3.11+

---

## 1. Overview
This bot tracks tank highscores, allows controlled submissions, auto-maintains read-only forum leaderboards, runs encrypted weekly backups, and provides verification and health checks.
It can also track clan members from WG API for player autocomplete and roster cleanup.

---

## 2. Required Components

### Software
- Python **3.11+**
- discord.py **2.4.x**
- SQLite (built-in)
- Optional: Caddy **2.x** (for dashboard HTTPS)

### Discord Access
- Administrator access to the server
- Ability to create roles, channels, and forum channels

---

## 3. Discord Configuration

### 3.1 Roles

#### Clan Commander
Used to submit scores.

Permissions:
- No special permissions required

Save the **Role ID**.

---

### 3.2 Channels

#### Announcement Channel
- Type: Text
- Bot permissions: Send Messages

Save the **Channel ID**.

#### Backup Channel (PRIVATE)
- Type: Text
- Bot permissions:
  - Send Messages
  - Attach Files
  - Read Message History
- Admins only access

Save the **Channel ID**.

#### Audit Log Channel (PRIVATE)
- Type: Text
- Bot permissions:
  - View Channel
  - Send Messages
- Admin-only access recommended

Save the **Channel ID**.

#### Forum Channel (Leaderboard Index)
- Type: Forum
- Bot permissions:
  - View Channel
  - Send Messages
  - Create Public Threads
  - Manage Threads
  - Manage Messages
  - Manage Channels

Save the **Forum Channel ID**.

---

## 4. Discord Bot Setup

1. Create application at https://discord.com/developers/applications
2. Add bot
3. Enable **Server Members Intent**
4. Copy **Bot Token**
5. Invite bot with scopes:
   - bot
   - applications.commands
6. Recommended permission: Administrator

---

## 5. Installation

```bash
git clone <your-repo-url> tankbot-test
cd tankbot-test
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## 6. Configuration (.env)

Create `.env` file:

```env
DISCORD_TOKEN=
GUILD_ID=

COMMANDER_ROLE_ID=
COMMANDER_ROLE_NAME=

ANNOUNCE_CHANNEL_ID=
BACKUP_CHANNEL_ID=
AUDIT_LOG_CHANNEL_ID=
TANK_INDEX_FORUM_CHANNEL_ID=
TANK_INDEX_NORMAL_CHANNEL_ID=

DB_PATH=
MAX_SCORE=

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

BACKUP_ENCRYPTION_PASSPHRASE=

LOG_LEVEL=
LOG_PATH=
```

If `TANK_INDEX_NORMAL_CHANNEL_ID` is set, index snapshots are also posted in that text channel. If both `TANK_INDEX_NORMAL_CHANNEL_ID` and `TANK_INDEX_FORUM_CHANNEL_ID` are set, both destinations are populated.

`COMMANDER_ROLE_ID` takes precedence; `COMMANDER_ROLE_NAME` is used only when `COMMANDER_ROLE_ID` is not set.

---

## 7. First Run

```bash
python bot.py
```

Commands will auto-register.

---

## 8. Initial Tank Setup

### CSV Import (Recommended)

```csv
name,tier,type
Tiger II,8,heavy
T-34,5,medium
```

Discord:
```
/tank preview_import csv_file:tanks.csv
/tank import_csv file:tanks.csv
```

---

## 9. Build Forum Index

```
/tank rebuild_index
```

Creates locked, pinned, tagged threads per Tier × Type.

---

## 10. Commands

### Users
- /help
- /highscore show
- /highscore qualify

### Clan Commanders
- /highscore submit
- /highscore history
- /highscore edit
- /highscore delete
- /highscore refresh_web
- /highscore refresh_players
- /tank add
- /tank edit
- /tank remove
- /tank rename
- /tank list
- /tank export_csv
- /tank export_scores_csv
- /backup run_now
- /backup status
- /backup verify_latest

### Admins
- /highscore import_scores
- /highscore changes
- /tank alias_add
- /tank alias_list
- /tank alias_seed_common
- /tank merge
- /tank changes
- /tank preview_import
- /tank import_csv
- /tank rebuild_index
- /tank rebuild_index_missing
- /system health
- /system audit_access
- /system reload

---

## 11. Backup Verification (IMPORTANT)

Run monthly:
```
/backup verify_latest
```

---

## 12. Permissions Summary

| Feature | Permission |
|------|-----------|
| Slash commands | applications.commands |
| Forum threads | Create Public Threads |
| Lock threads | Manage Threads |
| Pin starter | Manage Messages |
| Tags | Manage Channels |
| Backups | Attach Files + Read History |

---

## 13. Troubleshooting

- Check logs: tankbot.log
- Run: /system health
- Most issues are missing permissions

---

## Final Note

If backups are not verified, they are not backups.
