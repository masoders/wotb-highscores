import os
import logging
import datetime as dt
import hashlib
import sqlite3
import asyncio
import base64
import zipfile
from zoneinfo import ZoneInfo
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.fernet import Fernet

import discord
from discord.ext import tasks

from . import config, db

log = logging.getLogger(__name__)
from .utils import utc_now_z

_last_backup_utc: str | None = None
_last_backup_ok: bool | None = None
_last_backup_msg: str | None = None

def last_backup_status():
    return _last_backup_utc, _last_backup_ok, _last_backup_msg

def _derive_fernet():
    if not config.BACKUP_ENCRYPTION_PASSPHRASE:
        return None, None
    if config.BACKUP_ENCRYPTION_SALT:
        salt = base64.urlsafe_b64decode(config.BACKUP_ENCRYPTION_SALT.encode("utf-8"))
    else:
        salt = os.urandom(16)
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=200_000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(config.BACKUP_ENCRYPTION_PASSPHRASE.encode("utf-8")))
    return Fernet(key), base64.urlsafe_b64encode(salt).decode("utf-8")

async def create_backup_file() -> tuple[str, str, str]:
    """Create backup safely. Returns (path, sha256_hex, note). If encryption enabled, returns .enc file."""
    if not os.path.exists(config.DB_PATH):
        raise FileNotFoundError(f"DB not found: {config.DB_PATH}")

    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")
    tmp_db = f"{config.DB_PATH}.{ts}.backup.db"
    zip_name = f"highscores_backup_{ts}.zip"
    zip_path = os.path.join(os.getcwd(), zip_name)

    def _sqlite_backup():
        # Use SQLite backup API for a consistent snapshot.
        src = sqlite3.connect(config.DB_PATH)
        try:
            dst = sqlite3.connect(tmp_db)
            try:
                src.backup(dst)
                dst.commit()
            finally:
                dst.close()
        finally:
            src.close()

    await asyncio.to_thread(_sqlite_backup)

    try:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(tmp_db, arcname="highscores.db")
    finally:
        try:
            os.remove(tmp_db)
        except Exception:
            pass

    fernet, salt_b64 = _derive_fernet()
    note = ""
    out_path = zip_path
    if fernet is not None:
        with open(zip_path, "rb") as f:
            data = f.read()
        enc = fernet.encrypt(data)

        # Self-contained format:
        # TANKBOT1
        header_str = f"TANKBOT1\nSALT_B64:{salt_b64}\n\n"
        header = header_str.encode("utf-8")
        blob = header + enc
        out_path = zip_path + ".enc"
        with open(out_path, "wb") as f:
            f.write(blob)
        os.remove(zip_path)
        note = "Encrypted (Fernet). Salt embedded in file header."

    h = hashlib.sha256()
    with open(out_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)

    return out_path, h.hexdigest(), note



def next_weekly_run(now_local: dt.datetime) -> dt.datetime:
    target = now_local.replace(hour=config.BACKUP_HOUR, minute=config.BACKUP_MINUTE, second=0, microsecond=0)
    days_ahead = (config.BACKUP_WEEKDAY - target.weekday()) % 7
    if days_ahead == 0 and target <= now_local:
        days_ahead = 7
    return target + dt.timedelta(days=days_ahead)


def get_backup_guild(bot: discord.Client, interaction: discord.Interaction | None = None) -> discord.Guild | None:
    # If admin server is configured, use it.
    if config.BACKUP_GUILD_ID:
        return bot.get_guild(config.BACKUP_GUILD_ID)

    # If a guild is configured for command sync/testing, use it for scheduled backups too.
    if config.GUILD_ID:
        return bot.get_guild(config.GUILD_ID)

    # Fallback: use the interaction guild when available.
    return interaction.guild if interaction else None


@tasks.loop(minutes=1)
async def weekly_backup_loop(bot: discord.Client):
    global _last_backup_utc, _last_backup_ok, _last_backup_msg

    if config.BACKUP_CHANNEL_ID == 0:
        return

    tz = ZoneInfo(config.BACKUP_TZ)
    now_local = dt.datetime.now(tz)

    if not hasattr(weekly_backup_loop, "next_run"):
        weekly_backup_loop.next_run = next_weekly_run(now_local)

    if now_local < weekly_backup_loop.next_run:
        return

    weekly_backup_loop.next_run = next_weekly_run(now_local + dt.timedelta(seconds=1))

    guild = get_backup_guild(bot, None)
    if guild is None:
        return

    channel = guild.get_channel(config.BACKUP_CHANNEL_ID)
    if channel is None:
        try:
            channel = await guild.fetch_channel(config.BACKUP_CHANNEL_ID)
        except Exception:
            return

    path = None
    try:
        path, sha_hex, note = await create_backup_file()
        fname = os.path.basename(path)
        msg = (
            f"🧰 **Weekly DB backup**\n"
            f"- File: `{fname}`\n"
            f"- SHA-256: `{sha_hex}`\n"
            f"- Created (UTC): `{utc_now_z()}`"
        )
        if note:
            msg += f"\n- {note}"
        await channel.send(content=msg, file=discord.File(path, filename=fname))
        _last_backup_utc, _last_backup_ok, _last_backup_msg = utc_now_z(), True, fname
    except Exception as e:
        _last_backup_utc, _last_backup_ok, _last_backup_msg = utc_now_z(), False, f"{type(e).__name__}: {e}"
        log.error(f"Backup failed: {type(e).__name__}: {e}")
        try:
            await channel.send(f"❌ Backup failed: `{type(e).__name__}: {e}`")
        except Exception:
            pass
    finally:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception:
            pass


async def run_backup_now(bot: discord.Client) -> tuple[bool, str]:
    if config.BACKUP_CHANNEL_ID == 0:
        return False, "BACKUP_CHANNEL_ID is not set."

    guild = get_backup_guild(bot, None)
    if guild is None:
        return False, "Backup guild not found."

    channel = guild.get_channel(config.BACKUP_CHANNEL_ID)
    if channel is None:
        try:
            channel = await guild.fetch_channel(config.BACKUP_CHANNEL_ID)
        except Exception:
            return False, "Backup channel not found (check BACKUP_CHANNEL_ID)."

    path = None
    try:
        path, sha_hex, note = await create_backup_file()
        fname = os.path.basename(path)
        msg = (
            f"🧰 **Manual DB backup**\n"
            f"- File: `{fname}`\n"
            f"- SHA-256: `{sha_hex}`\n"
            f"- Created (UTC): `{utc_now_z()}`"
        )
        if note:
            msg += f"\n- {note}"
        await channel.send(content=msg, file=discord.File(path, filename=fname))
        return True, f"Posted `{fname}` to backup channel."
    except Exception as e:
        return False, f"Backup failed: {type(e).__name__}: {e}"
    finally:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception:
            pass


# ---- Backup verification ----
def _parse_enc_header(blob: bytes) -> tuple[str, bytes]:
    # TANKBOT1\nSALT_B64:<salt>\n\n<ciphertext>
    if not blob.startswith(b"TANKBOT1\n"):
        raise ValueError("Not a TANKBOT1 encrypted backup file")
    parts = blob.split(b"\n\n", 1)
    if len(parts) != 2:
        raise ValueError("Invalid encrypted backup header")
    header, ciphertext = parts
    lines = header.decode("utf-8").splitlines()
    salt_line = [l for l in lines if l.startswith("SALT_B64:")]
    if not salt_line:
        raise ValueError("Missing SALT_B64 in header")
    salt_b64 = salt_line[0].split(":", 1)[1].strip()
    return salt_b64, ciphertext

def _decrypt_enc_blob(blob: bytes) -> bytes:
    if not config.BACKUP_ENCRYPTION_PASSPHRASE:
        raise ValueError("BACKUP_ENCRYPTION_PASSPHRASE is not set; cannot verify encrypted backups.")
    salt_b64, ciphertext = _parse_enc_header(blob)
    fernet = derive_fernet_from_salt(config.BACKUP_ENCRYPTION_PASSPHRASE, salt_b64)
    return fernet.decrypt(ciphertext)

def derive_fernet_from_salt(passphrase: str, salt_b64: str) -> Fernet:
    salt = base64.urlsafe_b64decode(salt_b64.encode("utf-8"))
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=200_000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(passphrase.encode("utf-8")))
    return Fernet(key)

async def verify_latest_backup(bot: discord.Client, scan_limit: int = 50) -> tuple[bool, str]:
    """Download the newest backup file from the backup channel, and run PRAGMA integrity_check on the DB inside."""
    if config.BACKUP_CHANNEL_ID == 0:
        return False, "BACKUP_CHANNEL_ID is not set."
    if bot.user is None:
        return False, "Bot user is not ready yet."

    guild = get_backup_guild(bot, None)
    if guild is None:
        return False, "Backup guild not found. Set BACKUP_GUILD_ID or GUILD_ID."

    channel = guild.get_channel(config.BACKUP_CHANNEL_ID)
    if channel is None:
        try:
            channel = await guild.fetch_channel(config.BACKUP_CHANNEL_ID)
        except Exception:
            return False, "Backup channel not found (check BACKUP_CHANNEL_ID)."

    # Find newest message with a backup attachment
    import io, zipfile, tempfile, sqlite3, hashlib, re

    patt = re.compile(r"^highscores_backup_\d{8}_\d{6}Z\.zip(\.enc)?$")
    target = None
    skipped_too_large = False
    async for msg in channel.history(limit=scan_limit):
        if msg.author is None or msg.author.id != bot.user.id:
            continue
        if not msg.attachments:
            continue
        for a in msg.attachments:
            if not patt.match(a.filename):
                continue
            if a.size > config.BACKUP_VERIFY_MAX_ATTACHMENT_BYTES:
                skipped_too_large = True
                continue
            target = (msg, a)
            break
        if target:
            break

    if not target:
        if skipped_too_large:
            return False, (
                f"No verifiable backup found in last {scan_limit} messages "
                f"(matching files exceeded {config.BACKUP_VERIFY_MAX_ATTACHMENT_BYTES // (1024 * 1024)}MB limit)."
            )
        return False, f"No backup attachments found in last {scan_limit} messages."

    _, att = target
    if att.size > config.BACKUP_VERIFY_MAX_ATTACHMENT_BYTES:
        return False, (
            f"Attachment too large: {att.size} bytes "
            f"(limit {config.BACKUP_VERIFY_MAX_ATTACHMENT_BYTES} bytes)."
        )
    blob = await att.read()
    if len(blob) > config.BACKUP_VERIFY_MAX_ATTACHMENT_BYTES:
        return False, (
            f"Downloaded payload too large: {len(blob)} bytes "
            f"(limit {config.BACKUP_VERIFY_MAX_ATTACHMENT_BYTES} bytes)."
        )

    sha = hashlib.sha256(blob).hexdigest()
    is_enc = att.filename.endswith(".enc")

    try:
        if is_enc:
            zip_bytes = _decrypt_enc_blob(blob)
        else:
            zip_bytes = blob

        if len(zip_bytes) > config.BACKUP_VERIFY_MAX_ZIP_BYTES:
            return False, (
                f"Decrypted zip payload too large: {len(zip_bytes)} bytes "
                f"(limit {config.BACKUP_VERIFY_MAX_ZIP_BYTES} bytes)."
            )

        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            members = zf.infolist()
            total_uncompressed = sum(i.file_size for i in members)
            if total_uncompressed > config.BACKUP_VERIFY_MAX_ZIP_BYTES:
                return False, (
                    f"Zip uncompressed size too large: {total_uncompressed} bytes "
                    f"(limit {config.BACKUP_VERIFY_MAX_ZIP_BYTES} bytes)."
                )
            if "highscores.db" not in zf.namelist():
                return False, "Zip does not contain highscores.db"
            db_info = zf.getinfo("highscores.db")
            if db_info.file_size > config.BACKUP_VERIFY_MAX_DB_BYTES:
                return False, (
                    f"DB payload too large: {db_info.file_size} bytes "
                    f"(limit {config.BACKUP_VERIFY_MAX_DB_BYTES} bytes)."
                )
            db_bytes = zf.read("highscores.db")

        with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
            tmp.write(db_bytes)
            tmp.flush()
            con = sqlite3.connect(tmp.name)
            try:
                row = con.execute("PRAGMA integrity_check;").fetchone()
                ok = (row and row[0] == "ok")
            finally:
                con.close()

        if ok:
            return True, f"✅ Verified `{att.filename}` — integrity_check=ok — sha256={sha[:12]}…"
        return False, f"❌ Verified `{att.filename}` — integrity_check FAILED — sha256={sha[:12]}…"
    except Exception as e:
        return False, f"❌ Verify failed for `{att.filename}`: {type(e).__name__}: {e}"
