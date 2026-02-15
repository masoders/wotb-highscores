# Tank Highscore Bot — Complete Setup Guide

**Version:** v11  
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
unzip tank_highscore_bot_release_v15.zip
cd tank_highscore_bot
python3.11 -m venv venv
./start.sh
source venv/bin/activate
pip install -r requirements.txt
```

---

## 6. Configuration (.env)

Create `.env` file:

```env
DISCORD_TOKEN=YOUR_TOKEN
GUILD_ID=SERVER_ID

COMMANDER_ROLE_ID=ROLE_ID
COMMANDER_ROLE_NAME=Clan Commander

ANNOUNCE_CHANNEL_ID=CHANNEL_ID
BACKUP_CHANNEL_ID=CHANNEL_ID
AUDIT_LOG_CHANNEL_ID=CHANNEL_ID
TANK_INDEX_FORUM_CHANNEL_ID=FORUM_ID
TANK_INDEX_NORMAL_CHANNEL_ID=0

DB_PATH=highscores.db
MAX_SCORE=100000

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

BACKUP_ENCRYPTION_PASSPHRASE=LONG_RANDOM_STRING

LOG_LEVEL=INFO
LOG_PATH=tankbot.log
```

If `TANK_INDEX_NORMAL_CHANNEL_ID` is set (non-zero), index snapshots are also posted in that text channel. If both `TANK_INDEX_NORMAL_CHANNEL_ID` and `TANK_INDEX_FORUM_CHANNEL_ID` are set, both destinations are populated.

`COMMANDER_ROLE_ID` takes precedence; `COMMANDER_ROLE_NAME` is used only when `COMMANDER_ROLE_ID=0`.

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
/tank import_csv csv_file:tanks.csv
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
- /highscore history
- /highscore qualify

### Clan Commanders
- /highscore submit
- /highscore edit
- /highscore delete
- /highscore refresh_web
- /highscore refresh_players
- /backup run_now
- /backup status
- /backup verify_latest

### Admins
- /tank …
- /system health
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
