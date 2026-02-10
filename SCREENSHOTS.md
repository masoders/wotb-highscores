# Discord Screenshot Guide (What To Click)

## Enable Server Members Intent
Discord Developer Portal → Bot → Privileged Gateway Intents → Enable *Server Members Intent*

## Get IDs
User Settings → Advanced → Enable Developer Mode
Right-click role/channel → Copy ID

## Forum Permissions
Forum Channel → Edit Channel → Permissions:
- Bot:
  - View Channel
  - Send Messages
  - Create Public Threads
  - Manage Threads
  - Manage Messages
  - Manage Channels

## Backup Channel Permissions
- Bot:
  - Send Messages
  - Attach Files
  - Read Message History
- Everyone else: Denied

## Invite Bot
OAuth2 → URL Generator:
- Scopes: bot, applications.commands
- Permissions: Administrator