#!/usr/bin/env python3
"""
Parse your pasted leaderboard text into TWO CSV files:
  1) tanks.csv  -> tank_name,tier,type
  2) scores.csv -> tank_name,score,player_name,created_at,submitted_by

Rules:
- Sections look like: "Tier 8 - Heavy"
- Asterisks (*) anywhere in tank names are ignored/removed
- Lines with "-   -" (no score / no player) still produce a tank row, but NO score row
- Spacing doesn't matter; we parse from the RIGHT: "<tank name>  <score>  <player>"
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path


SECTION_RE = re.compile(r"^\s*Tier\s+(?P<tier>\d{1,2})\s*-\s*(?P<type>[A-Za-z ]+?)\s*$", re.IGNORECASE)
SEPARATOR_RE = re.compile(r"^\s*-{3,}\s*$")

# no-submission row (ends with dash dash)
NO_SUB_RE = re.compile(r"^(?P<tank>.+?)\s+[-\u2013\u2014]+\s+[-\u2013\u2014]+\s*$")

# normal scored row: tank + score + player, parsed from the RIGHT
SCORE_RE = re.compile(r"^(?P<tank>.+)\s+(?P<score>\d{1,6})\s+(?P<player>\S.*)$")


def normalize_type(raw: str) -> str:
    t = " ".join(raw.strip().lower().split())
    t = re.sub(r"\btank\b", "", t).strip()
    # allow common variants
    if "light" in t:
        return "light"
    if "medium" in t:
        return "medium"
    if "heavy" in t:
        return "heavy"
    if t in ("td", "tank destroyer", "tank destroyers", "destroyer", "tankdestroyer") or "destroyer" in t:
        return "td"
    # fallback: keep letters only
    return re.sub(r"[^a-z]", "", t) or t


def clean_tank_name(raw: str) -> str:
    # remove asterisks and collapse whitespace
    return " ".join(raw.replace("*", "").split()).strip()


def clean_line(raw: str) -> str:
    s = raw.strip()
    # Discord blockquote prefix if pasted from chat.
    s = re.sub(r"^\s*>\s*", "", s)
    # Bold/code formatting markers are noise for parsing.
    s = s.replace("`", "").replace("**", "")
    # Normalize unicode dashes to a regular hyphen.
    s = s.replace("\u2013", "-").replace("\u2014", "-")
    # Drop zero-width spaces sometimes introduced by copy/paste.
    s = s.replace("\u200b", "")
    return s


def parse_tank_line(line: str) -> dict | None:
    s = clean_line(line)
    if not s:
        return None
    if SEPARATOR_RE.match(s):
        return None

    m = NO_SUB_RE.match(s)
    if m:
        return {"tank": clean_tank_name(m.group("tank")), "score": None, "player": None}

    m = SCORE_RE.match(s)
    if not m:
        return {"error": f"Unrecognized row format: {line!r}"}

    tank = clean_tank_name(m.group("tank"))
    score = int(m.group("score"))
    player = m.group("player").strip()
    return {"tank": tank, "score": score, "player": player}


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert leaderboard text to tanks.csv + scores.csv")
    ap.add_argument("input", help="Path to the text file containing the leaderboard (use - for stdin)")
    ap.add_argument("--tanks-out", default="tanks.csv", help="Output CSV for tanks (default: tanks.csv)")
    ap.add_argument("--scores-out", default="scores.csv", help="Output CSV for scores (default: scores.csv)")
    ap.add_argument("--submitted-by", default="", help="Value for scores.csv submitted_by column")
    ap.add_argument("--created-at", default="", help="Value for scores.csv created_at column (ISO8601 or blank)")
    ap.add_argument("--max-score", type=int, default=100000, help="Ignore scores outside 1..max_score (default 100000)")
    ap.add_argument("--strict", action="store_true", help="Exit non-zero if any parsing errors are found")
    args = ap.parse_args()

    # read input
    if args.input == "-":
        content = sys.stdin.read()
    else:
        content = Path(args.input).read_text(encoding="utf-8", errors="replace")

    current_tier: int | None = None
    current_type: str | None = None

    # de-dupe tanks by (tier,type,tank_name)
    tanks_seen: set[tuple[int, str, str]] = set()
    tanks_rows: list[tuple[str, int, str]] = []

    # de-dupe scores by (tank_name, player_name, score, created_at) - you can change if needed
    scores_seen: set[tuple[str, str, int, str]] = set()
    scores_rows: list[tuple[str, int, str, str, str]] = []

    errors: list[str] = []

    for raw_line in content.splitlines():
        line = raw_line.rstrip("\n")

        sec = SECTION_RE.match(line)
        if sec:
            current_tier = int(sec.group("tier"))
            current_type = normalize_type(sec.group("type"))
            continue

        # ignore separators and blanks early
        if not line.strip() or SEPARATOR_RE.match(line.strip()):
            continue

        # if we haven't seen a section header yet, skip junk
        if current_tier is None or current_type is None:
            continue

        parsed = parse_tank_line(line)
        if not parsed:
            continue

        if "error" in parsed:
            errors.append(parsed["error"])
            continue

        tank = parsed["tank"]
        # write tank row always (even if no score)
        tank_key = (current_tier, current_type, tank.lower())
        if tank_key not in tanks_seen:
            tanks_seen.add(tank_key)
            tanks_rows.append((tank, current_tier, current_type))

        # write score row only if present
        score = parsed["score"]
        player = parsed["player"]
        if score is None or player is None:
            continue

        if not (1 <= score <= args.max_score):
            # ignore out-of-range
            continue

        score_key = (tank.lower(), player.strip().lower(), int(score), args.created_at)
        if score_key in scores_seen:
            continue
        scores_seen.add(score_key)

        scores_rows.append((tank, int(score), player.strip(), args.created_at, args.submitted_by))

    # write outputs
    with open(args.tanks_out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tank_name", "tier", "type"])
        for tank_name, tier, ttype in sorted(tanks_rows, key=lambda r: (r[1], r[2], r[0].lower())):
            w.writerow([tank_name, tier, ttype])

    with open(args.scores_out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tank_name", "score", "player_name", "created_at", "submitted_by"])
        # keep original order (often matches your source list)
        for row in scores_rows:
            w.writerow(row)

    # print summary
    print(f"Sections parsed: {'yes' if tanks_rows else 'no'}")
    print(f"Tanks written: {len(tanks_rows)} -> {args.tanks_out}")
    print(f"Scores written: {len(scores_rows)} -> {args.scores_out}")
    if errors:
        print(f"Parse errors: {len(errors)}")
        for e in errors[:20]:
            print(" -", e)
        if len(errors) > 20:
            print(f" - ...and {len(errors) - 20} more")

    if args.strict and errors:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
