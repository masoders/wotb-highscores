# Disaster Recovery — Tank Highscore Bot

This document explains how to recover from the worst cases:
bot crash, server loss, or database corruption.

---

## Scenario 1: Bot process crashed

1. Restart the bot:
   ```bash
   source venv/bin/activate
   python bot.py
   ```
2. Verify health:
   ```text
   /system health
   ```

No data loss expected.

---

## Scenario 2: Database corruption or loss

### Step 1: Stop the bot
```bash
Ctrl+C
```

### Step 2: Retrieve latest backup
From the **backup channel**, download the newest:
- `highscores_backup_*.zip`
- or `highscores_backup_*.zip.enc`

---

### Step 3: Verify backup integrity (recommended)
Temporarily restart the bot and run:
```text
/backup verify_latest
```
If verification fails, pick an older backup.

---

### Step 4: Restore database

#### If backup is NOT encrypted
```bash
unzip highscores_backup_YYYYMMDD_HHMMSSZ.zip
mv highscores.db highscores.db.restored
```

#### If backup IS encrypted
```bash
python decrypt_backup.py   --in highscores_backup_YYYYMMDD_HHMMSSZ.zip.enc   --out restore.zip   --passphrase "YOUR_PASSPHRASE"

unzip restore.zip
mv highscores.db highscores.db.restored
```

Replace existing DB:
```bash
mv highscores.db.restored highscores.db
```

---

### Step 5: Restart bot
```bash
python bot.py
```

---

## Scenario 3: Forum index is broken

Symptoms:
- Missing threads
- Threads unlocked or unpinned

Fix:
```text
/tank rebuild_index
```

---

## Scenario 4: Permissions accidentally changed

Use:
- PERMISSIONS_CHECKLIST.md
- SCREENSHOTS.md

Then:
```text
/tank rebuild_index
```

---

## Scenario 5: Lost encryption passphrase

Outcome:
- Encrypted backups are **unrecoverable**
- Data is permanently lost

Prevention:
- Store passphrase in password manager
- Restrict admin access

---

## Post-recovery checklist

- `/system health` → OK
- `/highscore show` → works
- `/backup run_now`
- `/backup verify_latest`

If all pass, recovery is complete.