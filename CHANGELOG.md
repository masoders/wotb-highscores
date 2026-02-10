# Changelog — Tank Highscore Bot

## v14
- Added CHANGELOG.md and DISASTER_RECOVERY.md
- No code changes

## v13
- Added QUICKSTART.md
- Added PERMISSIONS_CHECKLIST.md

## v12
- Added SETUP.md, SAFE_DEFAULTS.md, SCREENSHOTS.md to release package

## v11
- Added `/backup verify_latest`
- Backup integrity verification via `PRAGMA integrity_check`

## v10
- Reliable SQLite backups using SQLite backup API
- Input validation (length + control characters)
- Structured logging with rotation

## v9
- Restored admin commands (`/tank`, `/backup`)
- Restored forum index automation (locked, pinned, tagged threads)
- Filtered champions by tier/type

## v8
- Hardened dashboard (strict token, rate limiting)
- Added Caddy reverse proxy example

## v7
- Modularized codebase
- Added optional encrypted backups
- Added system health command

## v6
- Permission-aware `/help` command

## v5
- Added `/highscore qualify`

## v1–v4
- Core highscore tracking, submissions, leaderboards