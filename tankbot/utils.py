import datetime as dt
import discord
import re
import unicodedata
from . import config

from datetime import datetime, timezone

_WS_RE = re.compile(r"\s+")

def normalize_tank(name: str) -> str:
    """
    Normalizes tank names for case-insensitive lookup.
    Keep punctuation (., -, ') because your DB name_norm contains it.
    Only normalize: unicode, whitespace, and case.
    """
    if name is None:
        return ""
    s = str(name)
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u00a0", " ")          # non-breaking spaces
    s = s.strip().lower()
    s = _WS_RE.sub(" ", s)                # collapse whitespace
    return s

def norm_tank_name(s: str) -> str:
    # case-insensitive, trim, collapse internal whitespace
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def loose_tank_key(s: str) -> str:
    """
    Aggressive key used only for tolerant matching during imports.
    - case/space insensitive
    - diacritics insensitive
    - punctuation insensitive
    - strips words like "number" that appear in some aliases
    """
    raw = unicodedata.normalize("NFKD", (s or ""))
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    raw = raw.casefold()
    raw = raw.replace("\u00a0", " ")
    raw = re.sub(r"\bobj\.\b|\bobj\b", "object", raw)
    raw = re.sub(r"\bmle\.\b|\bmle\b", " ", raw)
    raw = re.sub(r"\bnumber\b", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return re.sub(r"[^a-z0-9]+", "", raw)

def fmt_utc(iso: str | None) -> str:
    if not iso:
        return "—"
    s = iso.strip()
    # tolerate accidental double Z
    while s.endswith("ZZ"):
        s = s[:-1]
    # Parse common ISO formats
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        # fallback: keep a readable timestamp but strip timezone markers
        raw = str(iso).strip()
        raw = re.sub(r"(?<=\d)T(?=\d)", " ", raw)
        raw = re.sub(r"\s*UTC\b", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"(?:\+00:00|Z)\b", "", raw)
        return raw.strip() or "—"
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M")

def title_case_type(t: str) -> str:
    return {
        "light": "Light",
        "medium": "Medium",
        "heavy": "Heavy",
        "td": "Tank Destroyer",
    }.get(t.lower(), t)

def has_commander_role(member: discord.Member) -> bool:
    if config.COMMANDER_ROLE_ID > 0:
        return any(int(r.id) == config.COMMANDER_ROLE_ID for r in member.roles)
    return any(r.name == config.COMMANDER_ROLE_NAME for r in member.roles)

def can_manage(member: discord.Member) -> bool:
    return member.guild_permissions.manage_guild or member.guild_permissions.administrator

def normalize_player(name: str) -> str:
    s = unicodedata.normalize("NFKC", (name or ""))
    s = s.replace("\u00a0", " ")
    s = s.strip().casefold()
    s = _WS_RE.sub(" ", s)
    return s

def utc_now_z() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def validate_text(label: str, value: str, max_len: int = 64) -> str:
    v = (value or "").strip()
    if not v:
        raise ValueError(f"{label} is required.")
    if len(v) > max_len:
        raise ValueError(f"{label} is too long (max {max_len} chars).")
    # Disallow newlines and control characters
    for ch in v:
        if ch in ("\n", "\r", "\t"):
            raise ValueError(f"{label} must be a single line.")
        if ord(ch) < 32:
            raise ValueError(f"{label} contains invalid control characters.")
    return v

def clip(s: str | None, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def build_snapshot_text(header_lines: list[str], row_lines: list[str], footer_lines: list[str]) -> str:
    # Discord hard limit
    DISCORD_MAX = 2000
    SAFETY = 150  # room for footer/truncation note

    out: list[str] = []
    out.extend(header_lines)
    out.append("")

    current = "\n".join(out) + "\n"
    remaining = DISCORD_MAX - SAFETY - len(current)

    included = 0
    for line in row_lines:
        if len(line) + 1 > remaining:
            break
        out.append(line)
        remaining -= (len(line) + 1)
        included += 1

    if included < len(row_lines):
        hidden = len(row_lines) - included
        out.append(f"… (+{hidden} more tanks)")

    out.append("")
    out.extend(footer_lines)

    text = "\n".join(out)
    return text[:DISCORD_MAX]

def fmt_table(rows: list[list[str]], widths: list[int]) -> str:
    out = []
    for r in rows:
        cells = []
        for i, cell in enumerate(r):
            w = widths[i]
            s = (cell or "")
            # right-align numbers in column 1 (Best)
            if i == 1:
                cells.append(s.rjust(w))
            else:
                cells.append(s.ljust(w))
        out.append("  ".join(cells).rstrip())
    return "\n".join(out)
