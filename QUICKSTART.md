# Tank Highscore Bot — Quickstart (10 minutes)

If you don’t want to read everything, do THIS.

For full repository installation with background start/stop scripts, see `INSTALL.md`.

## 1. Discord (5 minutes)
1. Create bot in Discord Developer Portal
2. Enable **Server Members Intent**
3. Invite bot with **Administrator** permission

Create:
- Role: Clan Commander
- Channels:
  - Announcement (text)
  - Backups (PRIVATE text)
  - Tank Leaderboards (Forum)

## 2. Install (3 minutes)
```bash
git clone <your-repo-url> tankbot-test
cd tankbot-test
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 3. Configure (2 minutes)
Create `.env` using `SETUP.md` template.
`COMMANDER_ROLE_ID` takes precedence; `COMMANDER_ROLE_NAME` is used only when the ID is not set.
Minimum required:
- DISCORD_TOKEN
- GUILD_ID
- COMMANDER_ROLE_ID
- ANNOUNCE_CHANNEL_ID
- BACKUP_CHANNEL_ID
- TANK_INDEX_FORUM_CHANNEL_ID (or `TANK_INDEX_NORMAL_CHANNEL_ID`)

Optional WG player tracking:
- WG_API_APPLICATION_ID
- WG_API_REGION
- WG_CLAN_IDS

## 4. First run
```bash
python bot.py
```

## 5. Load tanks + build index
```text
/tank import_csv file:tanks.csv
/tank rebuild_index
```

## 6. Verify backups
```text
/backup run_now
/backup verify_latest
```

## 7. (Optional) Verify WG player sync
```text
/highscore refresh_players
```

You are live.
