# Discord Permissions Checklist

Use this checklist when something doesn’t work.

## Bot Global Permissions
Recommended:
- Administrator ✅

Minimum alternative:
- Manage Roles
- Manage Channels
- Manage Messages
- Create Public Threads
- Send Messages
- Attach Files
- Read Message History

---

## Forum Channel (Tank Leaderboards)

Bot must have:
- View Channel
- Send Messages
- Create Public Threads
- Manage Threads (lock)
- Manage Messages (pin)
- Manage Channels (tags)

Symptoms if missing:
- Threads not created
- Threads not locked
- Starter not pinned
- Tags not applied

---

## Backup Channel (PRIVATE)

Bot must have:
- View Channel
- Send Messages
- Attach Files
- Read Message History

Symptoms if missing:
- No backups posted
- /backup verify_latest fails

---

## Announcement Channel

Bot must have:
- View Channel
- Send Messages

Symptoms if missing:
- No “NEW RECORD” announcements

---

## Role Checks

Clan Commander role:
- Must be assigned to users submitting scores
- Role ID must match `.env`

Symptoms if wrong:
- /highscore submit denied

---

## Debugging Order
1. Check permissions
2. Run `/system health`
3. Check `tankbot.log`