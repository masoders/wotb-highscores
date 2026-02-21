from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from html import escape
import json
import os
from pathlib import Path
from urllib.parse import quote

from . import config, db, utils

TYPE_ORDER = {"heavy": 0, "medium": 1, "light": 2, "td": 3}


def _safe_web_text(value: object, *, fallback: str = "—", quote: bool = False) -> str:
    raw = str(value) if value is not None else fallback
    if not raw:
        raw = fallback
    cleaned = "".join(ch for ch in raw if ch == "\n" or ord(ch) >= 32)
    # Neutralize mention-like strings for safer sharing/copying contexts.
    cleaned = cleaned.replace("@", "@\u200b")
    return escape(cleaned, quote=quote)


def _safe_web_multiline(value: object, *, fallback: str = "—") -> str:
    raw = str(value) if value is not None else fallback
    if not raw:
        raw = fallback
    # Allow escaped newlines from .env values (e.g. "\\n") and real newlines.
    text = raw.replace("\\n", "\n").strip()
    parts = text.split("\n")
    return "<br>".join(_safe_web_text(part, fallback="", quote=False) for part in parts)


def _fmt_local(iso: str | None) -> str:
    if not iso:
        return "—"
    s = str(iso).strip()
    while s.endswith("ZZ"):
        s = s[:-1]
    try:
        ts = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        local_tz = datetime.now().astimezone().tzinfo
        if local_tz is not None:
            ts = ts.astimezone(local_tz)
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        raw = str(iso).strip().replace("T", " ")
        raw = raw.replace("+00:00", "").replace("Z", "")
        return raw.strip() or "—"


def _json_for_html(obj: object) -> str:
    text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return text.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")


def _sorted_snapshot_rows(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: str(r.get("tank_name") or "").casefold())


def _build_styles(font_family: str, theme: dict[str, str]) -> str:
    styles = """
:root {
  --bg-0: #0b1221;
  --bg-1: #131f37;
  --panel: #0f1729cc;
  --line: #2c3c5f;
  --text: #ecf1ff;
  --muted: #adc0ea;
  --accent: #6ee7ff;
  --accent-2: #3aa5ff;
  --good: #6ef0b6;
  --font-color: __FONT_COLOR__;
  --damage-color: __DAMAGE_COLOR__;
  --tank-name-color: __TANK_NAME_COLOR__;
  --player-name-color: __PLAYER_NAME_COLOR__;
  --clan-name-color: __CLAN_NAME_COLOR__;
  --motto-color: __MOTTO_COLOR__;
  --leaderboard-color: __LEADERBOARD_COLOR__;
}
*, *::before, *::after {
  box-sizing: border-box;
  font-family: inherit;
}
body {
  margin: 0;
  color: var(--font-color);
  font-family: __FONT_FAMILY__;
  background:
    radial-gradient(circle at 0% 0%, #1f2f53 0%, transparent 45%),
    radial-gradient(circle at 100% 100%, #123a67 0%, transparent 40%),
    linear-gradient(180deg, __BG_COLOR__, var(--bg-1));
  min-height: 100vh;
}
.wrap {
  width: min(1200px, 94vw);
  margin: 28px auto 64px;
}
.hero {
  position: relative;
  border: 1px solid var(--line);
  border-radius: 22px;
  overflow: hidden;
  background: linear-gradient(135deg, #122746ee, #1b3f70cc);
  box-shadow: 0 18px 45px #04081288;
}
.hero img {
  width: 100%;
  max-height: 300px;
  object-fit: cover;
  display: block;
  opacity: 0.88;
}
.hero .overlay {
  position: absolute;
  inset: 0;
  background: linear-gradient(180deg, transparent 0%, #0a1428ee 90%);
}
.hero-content {
  position: absolute;
  left: 24px;
  right: 24px;
  bottom: 18px;
  text-align: center;
}
.hero.no-banner {
  padding: 22px 24px;
}
.hero.no-banner .hero-content {
  position: static;
  left: auto;
  right: auto;
  bottom: auto;
}
.hero-content.left {
  text-align: left;
}
.hero-content.center {
  text-align: center;
}
h1 {
  margin: 0 0 6px;
  font-size: clamp(1.5rem, 4vw, 2.6rem);
  letter-spacing: 0.02em;
  color: var(--clan-name-color);
}
.meta {
  margin: 0;
  color: var(--motto-color);
  font-size: 0.96rem;
}
.section-title {
  margin: 34px 0 16px;
  font-size: 1.45rem;
  color: var(--leaderboard-color);
}
.star-legend {
  margin: -6px 0 12px;
  color: var(--muted);
  font-size: 0.84rem;
}
.view-controls {
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 10px;
  margin-bottom: 14px;
}
.view-row {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}
.view-row.top {
  align-items: center;
}
.view-row.bottom {
  align-items: flex-start;
}
.view-label {
  color: var(--muted);
  font-size: 0.92rem;
}
.view-toggle {
  display: inline-flex;
  border: 1px solid #3a527b;
  border-radius: 999px;
  background: #12213d;
  padding: 2px;
}
.view-toggle button {
  border: 0;
  background: transparent;
  color: #c6d8ff;
  padding: 7px 12px;
  border-radius: 999px;
  cursor: pointer;
  font: inherit;
  font-size: 0.88rem;
}
.view-toggle button.active {
  background: linear-gradient(180deg, #367dcb, #2b5baf);
  color: #f3f8ff;
}
.tankopedia-nav-link {
  margin-left: auto;
  border: 1px solid #3a527b;
  border-radius: 999px;
  background: #12213d;
  color: #d0defe;
  text-decoration: none;
  padding: 7px 12px;
  font-size: 0.86rem;
  line-height: 1.2;
}
.tankopedia-nav-link:hover {
  background: #1e3357;
  color: #eef5ff;
}
.bulk-actions {
  display: inline-flex;
  gap: 8px;
}
.filter-tools {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}
.filter-tools label {
  color: var(--muted);
  font-size: 0.84rem;
}
.filter-tools select,
.filter-tools input {
  border: 1px solid #446291;
  background: #162746;
  color: #d0defe;
  border-radius: 9px;
  padding: 7px 10px;
  min-height: 35px;
  line-height: 1.2;
  font: inherit;
  font-size: 0.85rem;
}
.filter-tools select {
  appearance: none;
}
.filter-tools input {
  min-width: 170px;
}
.filter-tools button {
  border: 1px solid #446291;
  background: #162746;
  color: #d0defe;
  border-radius: 9px;
  padding: 7px 10px;
  min-height: 35px;
  line-height: 1.2;
  cursor: pointer;
  font: inherit;
  font-size: 0.85rem;
}
.player-tools {
  display: none;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}
.player-tools label {
  color: var(--muted);
  font-size: 0.85rem;
}
.player-search {
  display: inline-flex;
  align-items: center;
  gap: 7px;
}
.player-search input {
  border: 1px solid #446291;
  background: #162746;
  color: #d0defe;
  border-radius: 999px;
  padding: 7px 12px;
  min-width: 220px;
  font: inherit;
  font-size: 0.85rem;
}
.player-search input::placeholder {
  color: #91aad8;
}
.player-sort-chips {
  display: inline-flex;
  gap: 7px;
}
.player-sort-chips button {
  border: 1px solid #446291;
  background: #162746;
  color: #d0defe;
  border-radius: 999px;
  padding: 7px 11px;
  cursor: pointer;
  font: inherit;
  font-size: 0.82rem;
}
.player-sort-chips button.active {
  background: linear-gradient(180deg, #2f6ec2, #254f99);
  color: #eef5ff;
}
.bulk-actions button {
  border: 1px solid #446291;
  background: #162746;
  color: #d0defe;
  border-radius: 9px;
  padding: 7px 10px;
  cursor: pointer;
  font: inherit;
  font-size: 0.85rem;
}
.bulk-actions button:hover { background: #1e3357; }
.tier-card {
  border: 1px solid var(--line);
  border-radius: 18px;
  background: var(--panel);
  backdrop-filter: blur(4px);
  margin-bottom: 20px;
  overflow: hidden;
}
.tier-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 14px 16px;
  background: linear-gradient(90deg, #1b3257, #1b2c49);
  cursor: pointer;
  list-style: none;
}
.tier-head::-webkit-details-marker { display: none; }
.tier-head h2 {
  margin: 0;
  font-size: 1.1rem;
}
.tier-count {
  color: var(--muted);
  font-size: 0.9rem;
}
.tier-body {
  padding: 8px 10px 12px;
}
.type-block {
  border: 1px solid #2a3f63;
  border-radius: 12px;
  margin: 10px 4px;
  overflow: hidden;
}
.type-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 12px;
  background: #1a2b47;
  cursor: pointer;
  list-style: none;
}
.type-head::-webkit-details-marker { display: none; }
.type-count {
  color: var(--muted);
  font-size: 0.85rem;
}
.type-title {
  margin: 0;
  font-size: 0.95rem;
  color: #c5d6ff;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.table-wrap {
  padding: 8px 10px 10px;
  overflow-x: auto;
}
.view-panel[data-main-view="player"] { display: none; }
.view-panel[data-main-view="stats"] { display: none; }
table {
  width: 100%;
  border-collapse: collapse;
  overflow: hidden;
  border-radius: 12px;
  table-layout: fixed;
}
th, td {
  padding: 10px 9px;
  border-bottom: 1px solid #26385a;
  font-size: 0.93rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
th {
  text-align: left;
  color: #c3d5ff;
  font-weight: 700;
  position: sticky;
  top: 0;
  background: #162746;
  z-index: 1;
}
.col-tank { width: 44%; }
.col-type { width: 14%; }
.col-tier { width: 10%; }
.col-score { width: 16%; }
.col-player { width: 30%; }
.col-tankopedia { width: 10%; }
.col-p-tank { width: 40%; }
.col-p-type { width: 18%; }
.col-p-tier { width: 10%; }
.col-p-score { width: 32%; }
.score-head, .score {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
tbody tr:hover { background: #253a5a66; }
.data-row { cursor: pointer; }
.data-row.latest-submission td { font-weight: 700; }
.row-detail { display: none; }
.row-detail td {
  white-space: normal;
  color: var(--muted);
  font-size: 0.84rem;
}
.data-row.expanded + .row-detail { display: table-row; }
.score {
  font-weight: 700;
  color: var(--damage-color);
}
.muted { color: var(--muted); }
.tank-name { color: var(--tank-name-color); }
.tank-link {
  color: inherit;
  text-decoration: none;
  border-bottom: 1px dotted #5f7db4;
}
.tank-link:hover {
  text-decoration: underline;
}
.tank-stars {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  margin-left: 6px;
}
.tank-star {
  display: inline-block;
  font-size: 0.84em;
  line-height: 1;
  color: var(--muted);
}
.player-name { color: var(--player-name-color); }
.tankopedia-head,
.tankopedia-col {
  text-align: center;
}
.tankopedia-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  color: #cfe1ff;
  text-decoration: none;
  border: 1px solid #4d6796;
  border-radius: 7px;
  width: 24px;
  height: 24px;
  line-height: 1;
}
.tankopedia-icon:hover {
  background: #253a5a;
}
.player-link {
  color: inherit;
  text-decoration: none;
  border-bottom: 1px dotted #5f7db4;
}
.player-link:hover {
  text-decoration: underline;
}
.badge {
  display: inline-block;
  padding: 3px 8px;
  border-radius: 99px;
  border: 1px solid #4d6796;
  color: #d7e7ff;
  font-size: 0.78rem;
}
.footer {
  margin-top: 24px;
  color: var(--muted);
  font-size: 0.88rem;
}
.stats-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
}
.stats-card {
  border: 1px solid #2a3f63;
  border-radius: 12px;
  background: #13223dcc;
  padding: 12px;
}
.stats-card-wide { margin-top: 14px; }
.stats-title {
  margin: 0 0 10px;
  color: #dce8ff;
  font-size: 0.95rem;
  letter-spacing: 0.02em;
}
.stats-kpi {
  font-size: 2rem;
  font-weight: 800;
  color: var(--accent);
  line-height: 1;
}
.stats-table th, .stats-table td {
  padding: 8px 7px;
  font-size: 0.88rem;
}
.stats-rank, .stats-count, .stats-score { text-align: right; font-variant-numeric: tabular-nums; }
@media (max-width: 860px) {
  .hide-sm { display: none; }
  .view-row { align-items: flex-start; }
  .filter-tools { width: 100%; }
  .filter-tools input, .filter-tools select { width: 100%; }
  .col-tank { width: 58%; }
  .col-score { width: 20%; }
  .col-player { width: 32%; }
  .col-tankopedia { width: 12%; }
  .col-p-tank { width: 52%; }
  .col-p-type { width: 20%; }
  .col-p-tier { width: 10%; }
  .col-p-score { width: 18%; }
  th, td { padding: 11px 8px; }
  .stats-grid { grid-template-columns: 1fr; }
}
"""
    themed = styles.replace("__FONT_FAMILY__", font_family)
    themed = themed.replace("__BG_COLOR__", theme["bg_color"])
    themed = themed.replace("__FONT_COLOR__", theme["font_color"])
    themed = themed.replace("__DAMAGE_COLOR__", theme["damage_color"])
    themed = themed.replace("__TANK_NAME_COLOR__", theme["tank_name_color"])
    themed = themed.replace("__PLAYER_NAME_COLOR__", theme["player_name_color"])
    themed = themed.replace("__CLAN_NAME_COLOR__", theme["clan_name_color"])
    themed = themed.replace("__MOTTO_COLOR__", theme["motto_color"])
    themed = themed.replace("__LEADERBOARD_COLOR__", theme["leaderboard_color"])
    return themed


def _format_score(score: int | None) -> str:
    if score is None:
        return "-"
    try:
        iv = int(score)
    except Exception:
        return "-"
    if iv <= 0:
        return "-"
    return f"{iv:,}"


def _blitzstars_player_url(player_name: str) -> str | None:
    name = (player_name or "").strip()
    if not name or name in {"—", "-"}:
        return None
    region = (config.WG_API_REGION or "eu").strip().lower()
    return f"https://www.blitzstars.com/player/{quote(region, safe='')}/{quote(name, safe='')}"


def _render_player_link(player_name: str) -> str:
    safe_name = _safe_web_text(player_name)
    url = _blitzstars_player_url(player_name)
    if not url:
        return safe_name
    safe_url = _safe_web_text(url, quote=True)
    return f"<a class=\"player-link\" href=\"{safe_url}\" target=\"_blank\" rel=\"noopener noreferrer\">{safe_name}</a>"


def _tankopedia_relative_href() -> str | None:
    target_raw = str(getattr(config, "WG_TANKS_WEBPAGE_NAME", "") or "").strip()
    if not target_raw:
        return None
    source_path = Path(str(config.WEB_OUTPUT_PATH or "web/leaderboard.html"))
    source_dir = source_path.parent if str(source_path.parent) not in {"", "."} else Path(".")
    target_path = Path(target_raw)
    try:
        rel = os.path.relpath(str(target_path), start=str(source_dir))
    except Exception:
        rel = str(target_path)
    rel = str(rel or "").replace("\\", "/").strip()
    return rel or None


def _render_tank_link(
    tank_name: str,
    *,
    tankopedia_href: str | None,
    tankopedia_names_norm: set[str],
    tank_badges_by_norm: dict[str, tuple[bool, bool]],
) -> str:
    badges = _render_tank_badges(tank_name, tank_badges_by_norm=tank_badges_by_norm)
    safe_name = _safe_web_text(tank_name, fallback="Unknown")
    if not tankopedia_href:
        return f"{safe_name}{badges}"
    norm = utils.norm_tank_name(str(tank_name or ""))
    if not norm or norm not in tankopedia_names_norm:
        return f"{safe_name}{badges}"
    href = f"{tankopedia_href}?q={quote(str(tank_name or ''), safe='')}"
    safe_href = _safe_web_text(href, quote=True)
    return f"<a class=\"tank-link\" href=\"{safe_href}\">{safe_name}</a>{badges}"


def _render_tank_badges(
    tank_name: str,
    *,
    tank_badges_by_norm: dict[str, tuple[bool, bool]],
) -> str:
    norm = utils.norm_tank_name(str(tank_name or ""))
    if not norm:
        return ""
    is_collectible, is_premium = tank_badges_by_norm.get(norm, (False, False))
    if not (is_collectible or is_premium):
        return ""
    return (
        "<span class=\"tank-stars\">"
        "<span class=\"tank-star\" title=\"Premium/Collectible tank\" "
        "aria-label=\"Premium or collectible tank\">&#9733;</span>"
        "</span>"
    )


def _render_tankopedia_icon_link(
    tank_name: str,
    *,
    tankopedia_href: str | None,
    tankopedia_names_norm: set[str],
) -> str:
    if not tankopedia_href:
        return ""
    norm = utils.norm_tank_name(str(tank_name or ""))
    if not norm or norm not in tankopedia_names_norm:
        return ""
    href = f"{tankopedia_href}?q={quote(str(tank_name or ''), safe='')}"
    safe_href = _safe_web_text(href, quote=True)
    safe_name = _safe_web_text(tank_name, fallback="tank")
    return (
        f"<a class=\"tankopedia-icon\" href=\"{safe_href}\" "
        f"title=\"Open {safe_name} in Tankopedia\" "
        f"aria-label=\"Open {safe_name} in Tankopedia\">"
        "🔗"
        "</a>"
    )


def _render_rows(
    rows: list[dict],
    *,
    tankopedia_href: str | None,
    tankopedia_names_norm: set[str],
    tank_badges_by_norm: dict[str, tuple[bool, bool]],
) -> str:
    out: list[str] = []
    latest_idx = -1
    latest_key: tuple[str, int, str] | None = None
    for i, row in enumerate(rows):
        if bool(row.get("is_imported")):
            continue
        created_at = str(row.get("created_at") or "")
        if not created_at:
            continue
        tie_score = int(row.get("score")) if isinstance(row.get("score"), int) else -1
        tie_tank = str(row.get("tank_name") or "")
        key = (created_at, tie_score, tie_tank)
        if latest_key is None or key > latest_key:
            latest_key = key
            latest_idx = i

    for i, row in enumerate(rows):
        tank_raw = str(row.get("tank_name") or "")
        tank = (
            f"{_safe_web_text(tank_raw, fallback='Unknown')}"
            f"{_render_tank_badges(tank_raw, tank_badges_by_norm=tank_badges_by_norm)}"
        )
        score = row.get("score")
        score_val = int(score) if isinstance(score, int) else None
        player_raw = str(row.get("player_name") or "-")
        if score_val is None or score_val <= 0:
            player_raw = "-"
        player = _render_player_link(player_raw)
        tankopedia_icon = _render_tankopedia_icon_link(
            tank_raw,
            tankopedia_href=tankopedia_href,
            tankopedia_names_norm=tankopedia_names_norm,
        )
        score_text = _safe_web_text(_format_score(score_val), fallback="-")
        tier = _safe_web_text(row.get("tier"))
        ttype = _safe_web_text(utils.title_case_type(str(row.get("type") or "")))
        player_key = _safe_web_text(player_raw.casefold(), quote=True)
        is_latest = i == latest_idx
        row_class = "data-row latest-submission" if is_latest else "data-row"
        if is_latest:
            tank = f"<strong>{tank}</strong>"
            score_text = f"<strong>{score_text}</strong>"
            player = f"<strong>{player}</strong>"
        out.append(
            f"<tr class=\"{row_class}\" data-row-toggle=\"1\" data-player-key=\"{player_key}\" tabindex=\"0\">"
            f"<td class=\"tank-name\" data-label=\"Tank\">{tank}</td>"
            f"<td class=\"score\" data-label=\"Damage\">{score_text}</td>"
            f"<td class=\"player-name\" data-label=\"Player\">{player}</td>"
            f"<td class=\"tankopedia-col\" data-label=\"Tankopedia\">{tankopedia_icon}</td>"
            "</tr>"
            "<tr class=\"row-detail\">"
            f"<td colspan=\"4\">Tier {tier} • {ttype}</td>"
            "</tr>"
        )
    return "".join(out)


def _group_rows_by_player(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        score = row.get("score")
        score_val = int(score) if isinstance(score, int) else None
        player_key = str(row.get("player_name") or "-")
        if score_val is None or score_val <= 0:
            player_key = "-"
        grouped[player_key].append(row)
    return grouped


def _render_player_rows(
    rows: list[dict],
    *,
    tankopedia_href: str | None,
    tankopedia_names_norm: set[str],
    tank_badges_by_norm: dict[str, tuple[bool, bool]],
) -> str:
    out: list[str] = []
    sorted_rows = sorted(
        rows,
        key=lambda r: (
            -(int(r.get("score")) if isinstance(r.get("score"), int) else -1),
            str(r.get("tank_name") or "").casefold(),
        ),
    )
    latest_idx = -1
    latest_key: tuple[str, int, str] | None = None
    for i, row in enumerate(sorted_rows):
        if bool(row.get("is_imported")):
            continue
        created_at = str(row.get("created_at") or "")
        if not created_at:
            continue
        tie_score = int(row.get("score")) if isinstance(row.get("score"), int) else -1
        tie_tank = str(row.get("tank_name") or "")
        key = (created_at, tie_score, tie_tank)
        if latest_key is None or key > latest_key:
            latest_key = key
            latest_idx = i

    for i, row in enumerate(sorted_rows):
        tank_raw = str(row.get("tank_name") or "")
        tank = _render_tank_link(
            tank_raw,
            tankopedia_href=tankopedia_href,
            tankopedia_names_norm=tankopedia_names_norm,
            tank_badges_by_norm=tank_badges_by_norm,
        )
        ttype = _safe_web_text(utils.title_case_type(str(row.get("type") or "")))
        tier = _safe_web_text(row.get("tier"))
        score = row.get("score")
        score_text = _safe_web_text(_format_score(int(score) if isinstance(score, int) else None), fallback="-")
        row_class = "data-row latest-submission" if i == latest_idx else "data-row"
        if i == latest_idx:
            tank = f"<strong>{tank}</strong>"
            ttype = f"<strong>{ttype}</strong>"
            tier = f"<strong>{tier}</strong>"
            score_text = f"<strong>{score_text}</strong>"
        out.append(
            f"<tr class=\"{row_class}\" data-row-toggle=\"1\" tabindex=\"0\">"
            f"<td class=\"tank-name\" data-label=\"Tank\">{tank}</td>"
            f"<td data-label=\"Type\">{ttype}</td>"
            f"<td data-label=\"Tier\">{tier}</td>"
            f"<td class=\"score\" data-label=\"Damage\">{score_text}</td>"
            "</tr>"
            "<tr class=\"row-detail\">"
            f"<td colspan=\"4\">Type: {ttype} • Tier {tier}</td>"
            "</tr>"
        )
    return "".join(out)


def _render_player_blocks(
    rows: list[dict],
    *,
    tankopedia_href: str | None,
    tankopedia_names_norm: set[str],
    tank_badges_by_norm: dict[str, tuple[bool, bool]],
) -> str:
    grouped = _group_rows_by_player(rows)
    out: list[str] = []
    for player in sorted(grouped.keys(), key=lambda p: p.casefold()):
        safe_player = _render_player_link(player)
        safe_player_key = _safe_web_text(player.casefold(), quote=True)
        player_rows = grouped[player]
        tank_count = len(player_rows)
        out.extend(
            [
                f"<details class=\"type-block\" data-player-key=\"{safe_player_key}\" data-tank-count=\"{tank_count}\" open>",
                "<summary class=\"type-head\">",
                f"<div class=\"type-title\"><span class=\"badge\">{safe_player}</span></div>",
                f"<span class=\"type-count\">{tank_count} tanks</span>",
                "</summary>",
                "<div class=\"table-wrap\">",
                "<table>",
                "<colgroup>"
                "<col class=\"col-p-tank\" />"
                "<col class=\"col-p-type\" />"
                "<col class=\"col-p-tier\" />"
                "<col class=\"col-p-score\" />"
                "</colgroup>",
                "<thead><tr><th>Tank</th><th>Type</th><th>Tier</th><th class=\"score-head\">Damage</th></tr></thead>",
                f"<tbody>{_render_player_rows(player_rows, tankopedia_href=tankopedia_href, tankopedia_names_norm=tankopedia_names_norm, tank_badges_by_norm=tank_badges_by_norm)}</tbody>",
                "</table>",
                "</div>",
                "</details>",
            ]
        )
    return "".join(out)


def _render_stats_top_per_tier(
    rows: list[tuple[int, int, str, str, int]],
    *,
    tankopedia_href: str | None,
    tankopedia_names_norm: set[str],
    tank_badges_by_norm: dict[str, tuple[bool, bool]],
) -> str:
    grouped: dict[int, list[tuple[int, int, str, str, int]]] = defaultdict(list)
    for tier, rank, tank_name, player_name, score in rows:
        grouped[int(tier)].append((int(tier), int(rank), str(tank_name), str(player_name), int(score)))

    out: list[str] = []
    for tier in sorted(grouped.keys(), reverse=True):
        out.extend(
            [
                "<details class=\"type-block\" open>",
                "<summary class=\"type-head\">",
                f"<div class=\"type-title\"><span class=\"badge\">Tier {tier}</span></div>",
                "<span class=\"type-count\">Top 3 damage</span>",
                "</summary>",
                "<div class=\"table-wrap\">",
                "<table class=\"stats-table\">",
                "<thead><tr><th class=\"stats-rank\">#</th><th class=\"stats-score\">Damage</th><th>Player</th><th>Tank</th></tr></thead>",
                "<tbody>",
            ]
        )
        for _tier, rank, tank_name, player_name, score in grouped[tier]:
            tank_link = _render_tank_link(
                tank_name,
                tankopedia_href=tankopedia_href,
                tankopedia_names_norm=tankopedia_names_norm,
                tank_badges_by_norm=tank_badges_by_norm,
            )
            out.append(
                "<tr>"
                f"<td class=\"stats-rank\">{rank}</td>"
                f"<td class=\"stats-score\">{_safe_web_text(_format_score(score), fallback='-')}</td>"
                f"<td class=\"player-name\">{_render_player_link(player_name)}</td>"
                f"<td class=\"tank-name\">{tank_link}</td>"
                "</tr>"
            )
        out.extend(["</tbody>", "</table>", "</div>", "</details>"])
    if not out:
        return "<p class=\"muted\">No submission data yet.</p>"
    return "".join(out)


def _render_stats_tanks(
    rows: list[tuple[str, int]],
    *,
    tankopedia_href: str | None,
    tankopedia_names_norm: set[str],
    tank_badges_by_norm: dict[str, tuple[bool, bool]],
) -> str:
    out: list[str] = [
        "<table class=\"stats-table\">",
        "<thead><tr><th class=\"stats-rank\">#</th><th>Tank</th><th class=\"stats-count\">Submissions</th></tr></thead>",
        "<tbody>",
    ]
    for i, (tank_name, count) in enumerate(rows, start=1):
        tank_link = _render_tank_link(
            tank_name,
            tankopedia_href=tankopedia_href,
            tankopedia_names_norm=tankopedia_names_norm,
            tank_badges_by_norm=tank_badges_by_norm,
        )
        out.append(
            "<tr>"
            f"<td class=\"stats-rank\">{i}</td>"
            f"<td class=\"tank-name\">{tank_link}</td>"
            f"<td class=\"stats-count\">{int(count)}</td>"
            "</tr>"
        )
    if not rows:
        out.append("<tr><td class=\"muted\" colspan=\"3\">No data</td></tr>")
    out.extend(["</tbody>", "</table>"])
    return "".join(out)


def _render_stats_time(rows: list[tuple[str, int]], label: str) -> str:
    out: list[str] = [
        "<table class=\"stats-table\">",
        f"<thead><tr><th>{_safe_web_text(label)}</th><th class=\"stats-count\">Submissions</th></tr></thead>",
        "<tbody>",
    ]
    for key, count in rows:
        out.append(
            "<tr>"
            f"<td>{_safe_web_text(key)}</td>"
            f"<td class=\"stats-count\">{int(count)}</td>"
            "</tr>"
        )
    if not rows:
        out.append("<tr><td class=\"muted\" colspan=\"2\">No data</td></tr>")
    out.extend(["</tbody>", "</table>"])
    return "".join(out)


def _build_script() -> str:
    return """
(() => {
  const dataNode = document.getElementById("tb-data");
  const DATA = dataNode ? JSON.parse(dataNode.textContent || "{}") : {};
  const wrappers = Array.from(document.querySelectorAll(".table-wrap"));
  const buttons = Array.from(document.querySelectorAll("[data-view-btn]"));
  const actionButtons = Array.from(document.querySelectorAll("[data-bulk-action]"));
  const panels = Array.from(document.querySelectorAll("[data-main-view]"));
  const playerToolsWrap = document.querySelector("[data-player-tools-wrap]");
  const tankToolsWrap = document.querySelector("[data-tank-tools-wrap]");
  const playerSortButtons = Array.from(document.querySelectorAll("[data-player-sort-btn]"));
  const playerSearch = document.querySelector("[data-player-search]");
  const playerList = document.querySelector("[data-player-list]");
  const filterTier = document.querySelector("[data-filter-tier]");
  const filterType = document.querySelector("[data-filter-type]");
  const filterTankSearch = document.querySelector("[data-filter-tank-search]");
  const filterReset = document.querySelector("[data-filter-reset]");
  const changesTarget = document.querySelector("[data-recent-changes]");
  let current = "stats";
  let playerSortMode = "name-asc";
  let playerQuery = "";
  let tierFilter = "";
  let typeFilter = "";
  let tankQuery = "";

  const normalize = (v) => (v || "").toLocaleLowerCase().trim();
  const escapeHtml = (v) => String(v ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

  const setUrlState = () => {
    const url = new URL(window.location.href);
    const setParam = (k, v) => {
      if (v) url.searchParams.set(k, v);
      else url.searchParams.delete(k);
    };
    setParam("view", current);
    setParam("tier", tierFilter);
    setParam("type", typeFilter);
    setParam("tank", tankQuery);
    history.replaceState({}, "", url.toString());
  };

  const applyFromUrl = () => {
    const p = new URLSearchParams(window.location.search);
    const view = p.get("view");
    current = view === "tank" || view === "player" || view === "stats" ? view : "stats";
    tierFilter = (p.get("tier") || "").trim();
    typeFilter = normalize(p.get("type"));
    tankQuery = p.get("tank") || "";
  };

  const populateFilters = () => {
    const tiers = Array.isArray(DATA.tiers) ? DATA.tiers : [];
    const types = Array.isArray(DATA.types) ? DATA.types : [];
    if (filterTier) {
      filterTier.innerHTML = "<option value=''>All tiers</option>" + tiers.map((t) => (
        `<option value="${escapeHtml(t)}">Tier ${escapeHtml(t)}</option>`
      )).join("");
      filterTier.value = tierFilter;
    }
    if (filterType) {
      const typeLabel = (t) => {
        if (t === "td") return "Tank Destroyer";
        return t ? (t[0].toUpperCase() + t.slice(1)) : "";
      };
      filterType.innerHTML = "<option value=''>All types</option>" + types.map((t) => (
        `<option value="${escapeHtml(t)}">${escapeHtml(typeLabel(t))}</option>`
      )).join("");
      filterType.value = typeFilter;
    }
    if (filterTankSearch) {
      filterTankSearch.value = tankQuery;
    }
  };

  const renderRecentChanges = () => {
    if (!changesTarget) return;
    const rows = Array.isArray(DATA.recent_changes) ? DATA.recent_changes : [];
    if (!rows.length) {
      changesTarget.innerHTML = "<p class='muted'>No recent damage changes.</p>";
      return;
    }
    changesTarget.innerHTML = "<table class='stats-table'><thead><tr><th>ID</th><th>Action</th><th>Tank</th><th>Player</th><th class='stats-score'>Damage</th><th class='hide-sm'>When</th></tr></thead><tbody>" +
      rows.slice(0, 10).map((r) => (
        `<tr><td>#${escapeHtml(r.id)}</td><td>${escapeHtml(r.action)}</td><td>${escapeHtml(r.tank_name)}</td><td>${escapeHtml(r.player_name)}</td><td class='stats-score'>${escapeHtml(r.score_change)}</td><td class='hide-sm'>${escapeHtml(r.when)}</td></tr>`
      )).join("") +
      "</tbody></table>";
  };

  const sortPlayerBlocks = () => {
    if (!playerList) return;
    const blocks = Array.from(playerList.querySelectorAll(":scope > details.type-block"));
    blocks.sort((a, b) => {
      const aKey = a.getAttribute("data-player-key") || "";
      const bKey = b.getAttribute("data-player-key") || "";
      if (playerSortMode === "tank-count-desc") {
        const aCount = Number(a.getAttribute("data-tank-count") || "0");
        const bCount = Number(b.getAttribute("data-tank-count") || "0");
        if (aCount !== bCount) return bCount - aCount;
      }
      return aKey.localeCompare(bKey);
    });
    blocks.forEach((block) => playerList.appendChild(block));
  };

  const filterPlayerBlocks = () => {
    if (!playerList) return;
    const q = normalize(playerQuery);
    const blocks = Array.from(playerList.querySelectorAll(":scope > details.type-block"));
    blocks.forEach((block) => {
      const key = normalize(block.getAttribute("data-player-key") || "");
      const visible = (!q || key.includes(q));
      block.style.display = visible ? "" : "none";
    });
  };

  const filterTankBlocks = () => {
    const tankNeedle = normalize(tankQuery);
    const tierCards = Array.from(document.querySelectorAll('[data-main-view="tank"] .tier-card'));
    tierCards.forEach((tierCard) => {
      const tierVal = tierCard.getAttribute("data-tier") || "";
      const tierMatches = !tierFilter || tierVal === tierFilter;
      const typeBlocks = Array.from(tierCard.querySelectorAll(":scope .type-block"));
      let tierVisibleTypes = 0;
      typeBlocks.forEach((typeBlock) => {
        const typeVal = normalize(typeBlock.getAttribute("data-type") || "");
        const typeMatches = !typeFilter || typeVal === typeFilter;
        const rowPairs = Array.from(typeBlock.querySelectorAll(":scope tbody tr.data-row"));
        let visibleRows = 0;
        rowPairs.forEach((row) => {
          const detail = row.nextElementSibling;
          const tankCell = row.querySelector("td.tank-name");
          const tankText = normalize(tankCell ? tankCell.textContent : "");
          const tankMatches = !tankNeedle || tankText.includes(tankNeedle);
          row.style.display = tankMatches ? "" : "none";
          if (detail && detail.classList.contains("row-detail")) detail.style.display = "none";
          row.classList.remove("expanded");
          if (tankMatches) visibleRows += 1;
        });
        const visible = tierMatches && typeMatches && visibleRows > 0;
        typeBlock.style.display = visible ? "" : "none";
        if (visible) tierVisibleTypes += 1;
      });
      tierCard.style.display = tierVisibleTypes > 0 ? "" : "none";
    });
  };

  const applyPlayerView = () => {
    sortPlayerBlocks();
    filterPlayerBlocks();
    playerSortButtons.forEach((btn) => {
      const active = (btn.getAttribute("data-player-sort-btn") || "") === playerSortMode;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-pressed", active ? "true" : "false");
    });
  };

  const update = (view) => {
    current = view;
    wrappers.forEach((el) => el.setAttribute("data-view", view));
    panels.forEach((panel) => {
      const panelView = panel.getAttribute("data-main-view");
      panel.style.display = panelView === view ? "block" : "none";
    });
    buttons.forEach((btn) => {
      const active = btn.getAttribute("data-view-btn") === view;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-pressed", active ? "true" : "false");
    });
    if (playerToolsWrap) {
      playerToolsWrap.style.display = view === "player" ? "flex" : "none";
    }
    if (tankToolsWrap) {
      tankToolsWrap.style.display = view === "tank" ? "inline-flex" : "none";
    }
    setUrlState();
  };

  buttons.forEach((btn) => {
    btn.addEventListener("click", () => update(btn.getAttribute("data-view-btn") || "stats"));
  });

  if (playerSortButtons.length) {
    playerSortButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        playerSortMode = btn.getAttribute("data-player-sort-btn") || "name-asc";
        applyPlayerView();
      });
    });
  }
  if (playerSearch) {
    playerSearch.addEventListener("input", () => {
      playerQuery = playerSearch.value || "";
      filterPlayerBlocks();
      setUrlState();
    });
  }
  if (filterTier) {
    filterTier.addEventListener("change", () => {
      tierFilter = filterTier.value || "";
      filterTankBlocks();
      setUrlState();
    });
  }
  if (filterType) {
    filterType.addEventListener("change", () => {
      typeFilter = normalize(filterType.value || "");
      filterTankBlocks();
      setUrlState();
    });
  }
  if (filterTankSearch) {
    filterTankSearch.addEventListener("input", () => {
      tankQuery = filterTankSearch.value || "";
      filterTankBlocks();
      setUrlState();
    });
  }
  if (filterReset) {
    filterReset.addEventListener("click", () => {
      tierFilter = "";
      typeFilter = "";
      tankQuery = "";
      if (filterTier) filterTier.value = "";
      if (filterType) filterType.value = "";
      if (filterTankSearch) filterTankSearch.value = "";
      filterTankBlocks();
      setUrlState();
    });
  }
  applyPlayerView();
  filterTankBlocks();
  renderRecentChanges();

  const setAllDetails = (openState) => {
    const activePanel = document.querySelector(`[data-main-view="${current}"]`);
    if (!activePanel) return;
    activePanel.querySelectorAll("details").forEach((el) => {
      el.open = openState;
    });
  };

  actionButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const action = btn.getAttribute("data-bulk-action");
      if (action === "expand") setAllDetails(true);
      if (action === "collapse") setAllDetails(false);
    });
  });

  document.addEventListener("click", (event) => {
    const row = event.target && event.target.closest ? event.target.closest("tr[data-row-toggle]") : null;
    if (!row) return;
    const detail = row.nextElementSibling;
    if (!detail || !detail.classList.contains("row-detail")) return;
    const expanded = row.classList.toggle("expanded");
    detail.style.display = expanded ? "table-row" : "none";
  });
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const row = event.target && event.target.closest ? event.target.closest("tr[data-row-toggle]") : null;
    if (!row) return;
    event.preventDefault();
    row.click();
  });

  applyFromUrl();
  populateFilters();
  filterPlayerBlocks();
  filterTankBlocks();
  update(current);
})();
"""


def _render_html(
    clan_name: str,
    clan_motto: str | None,
    clan_description: str | None,
    banner_url: str | None,
    clan_name_align: str,
    font_family: str,
    theme: dict[str, str],
    grouped: dict[int, dict[str, list[dict]]],
    player_rows: list[dict],
    tank_total: int,
    top_per_tier_rows: list[tuple[int, int, str, str, int]],
    top_tanks_rows: list[tuple[str, int]],
    unique_player_count: int,
    yearly_rows: list[tuple[str, int]],
    monthly_rows: list[tuple[str, int]],
    data_blob: str,
    tankopedia_href: str | None,
    tankopedia_names_norm: set[str],
    tank_badges_by_norm: dict[str, tuple[bool, bool]],
) -> str:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    banner = ""
    tankopedia_nav_link = ""
    safe_align = clan_name_align if clan_name_align in {"left", "center"} else "center"
    hero_class = "hero"
    if banner_url:
        safe_banner = _safe_web_text(banner_url, quote=True, fallback="")
        banner = (
            f"<img src=\"{safe_banner}\" alt=\"{_safe_web_text(clan_name)} banner\" />"
            "<div class=\"overlay\"></div>"
        )
    else:
        hero_class = "hero no-banner"
    if tankopedia_href:
        safe_tankopedia_href = _safe_web_text(tankopedia_href, quote=True)
        tankopedia_nav_link = (
            f"<a class=\"tankopedia-nav-link\" href=\"{safe_tankopedia_href}\" "
            "aria-label=\"Open Tankopedia\">Tankopedia</a>"
        )
    content = [
        "<!doctype html>",
        "<html lang=\"en\">",
        "<head>",
        "<meta charset=\"utf-8\" />",
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />",
        f"<title>{_safe_web_text(clan_name)} Leaderboard</title>",
        "<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">",
        "<link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>",
        "<link href=\"https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&family=Plus+Jakarta+Sans:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">",
        f"<style>{_build_styles(font_family, theme)}</style>",
        "</head>",
        "<body>",
        "<main class=\"wrap\">",
        f"<section class=\"{hero_class}\">",
        banner,
        f"<div class=\"hero-content {safe_align}\">",
        f"<h1>{_safe_web_text(clan_name)}</h1>",
        (f"<p class=\"meta\">{_safe_web_multiline(clan_motto)}</p>" if clan_motto else ""),
        (f"<p class=\"meta\">{_safe_web_multiline(clan_description)}</p>" if clan_description else ""),
        "</div>",
        "</section>",
        "<h2 class=\"section-title\">Leaderboard</h2>",
        "<p class=\"star-legend\">&#9733; = Premium/Collectible</p>",
        "<div class=\"view-controls\">",
        "<div class=\"view-row top\">",
        "<span class=\"view-label\">View</span>",
        "<div class=\"view-toggle\" role=\"group\" aria-label=\"Leaderboard view\">",
        "<button type=\"button\" data-view-btn=\"stats\" class=\"active\" aria-pressed=\"true\">Statistics</button>",
        "<button type=\"button\" data-view-btn=\"tank\" aria-pressed=\"false\">By Tank</button>",
        "<button type=\"button\" data-view-btn=\"player\" aria-pressed=\"false\">By Player</button>",
        "</div>",
        tankopedia_nav_link,
        "</div>",
        "<div class=\"view-row bottom\">",
        "<div class=\"bulk-actions\" role=\"group\" aria-label=\"Expand and collapse\">",
        "<button type=\"button\" data-bulk-action=\"collapse\">Collapse All</button>",
        "<button type=\"button\" data-bulk-action=\"expand\">Expand All</button>",
        "</div>",
        "<div class=\"filter-tools\" data-tank-tools-wrap role=\"group\" aria-label=\"Leaderboard filters\" style=\"display:none;\">",
        "<label for=\"filter-tier\">Tier</label>",
        "<select id=\"filter-tier\" data-filter-tier><option value=\"\">All tiers</option></select>",
        "<label for=\"filter-type\">Type</label>",
        "<select id=\"filter-type\" data-filter-type><option value=\"\">All types</option></select>",
        "<label for=\"filter-tank-search\">Tank</label>",
        "<input id=\"filter-tank-search\" data-filter-tank-search type=\"search\" placeholder=\"Search tank\" autocomplete=\"off\" />",
        "<button type=\"button\" data-filter-reset>Reset Filters</button>",
        "</div>",
        "<div class=\"player-tools\" data-player-tools-wrap>",
        "<div class=\"player-sort-chips\" role=\"group\" aria-label=\"Player sorting\">",
        "<button type=\"button\" data-player-sort-btn=\"name-asc\" aria-pressed=\"true\" class=\"active\">A-Z</button>",
        "<button type=\"button\" data-player-sort-btn=\"tank-count-desc\" aria-pressed=\"false\">Most Tanks</button>",
        "</div>",
        "<div class=\"player-search\">",
        "<label for=\"player-search\">Find Player</label>",
        "<input id=\"player-search\" data-player-search type=\"search\" placeholder=\"Type a player name\" autocomplete=\"off\" />",
        "</div>",
        "</div>",
        "</div>",
        "</div>",
    ]
    content.extend(
        [
            "<section class=\"view-panel\" data-main-view=\"stats\">",
            "<div class=\"tier-card\">",
            "<div class=\"tier-body\">",
            "<div class=\"stats-grid\">",
            "<div class=\"stats-card\">",
            "<h3 class=\"stats-title\">Unique Players</h3>",
            f"<div class=\"stats-kpi\">{unique_player_count}</div>",
            "</div>",
            "<div class=\"stats-card\">",
            "<h3 class=\"stats-title\">Top 10 Most Recorded Tanks</h3>",
            _render_stats_tanks(
                top_tanks_rows,
                tankopedia_href=tankopedia_href,
                tankopedia_names_norm=tankopedia_names_norm,
                tank_badges_by_norm=tank_badges_by_norm,
            ),
            "</div>",
            "<div class=\"stats-card\">",
            "<h3 class=\"stats-title\">Submissions Per Year</h3>",
            _render_stats_time(yearly_rows, "Year"),
            "</div>",
            "<div class=\"stats-card\">",
            "<h3 class=\"stats-title\">Submissions Per Month</h3>",
            _render_stats_time(monthly_rows, "Month"),
            "</div>",
            "</div>",
            "<div class=\"stats-card stats-card-wide\">",
            "<h3 class=\"stats-title\">Recent Damage Changes</h3>",
            "<div data-recent-changes></div>",
            "</div>",
            "<div class=\"stats-card stats-card-wide\">",
            "<h3 class=\"stats-title\">Top 3 Per Tier (all tanks)</h3>",
            _render_stats_top_per_tier(
                top_per_tier_rows,
                tankopedia_href=tankopedia_href,
                tankopedia_names_norm=tankopedia_names_norm,
                tank_badges_by_norm=tank_badges_by_norm,
            ),
            "</div>",
            "</div>",
            "</div>",
            "</section>",
        ]
    )
    content.append("<section class=\"view-panel\" data-main-view=\"tank\">")
    for tier in sorted(grouped.keys(), reverse=True):
        tier_block = grouped[tier]
        bucket_count = sum(len(rows) for rows in tier_block.values())
        content.extend(
            [
                f"<details class=\"tier-card\" data-tier=\"{int(tier)}\" open>",
                "<summary class=\"tier-head\">",
                f"<h2>Tier {tier}</h2>",
                f"<span class=\"tier-count\">{bucket_count} tanks</span>",
                "</summary>",
                "<div class=\"tier-body\">",
            ]
        )
        for ttype in sorted(tier_block.keys(), key=lambda v: TYPE_ORDER.get(v, 99)):
            title = _safe_web_text(utils.title_case_type(ttype))
            rows = tier_block[ttype]
            row_count = len(rows)
            content.extend(
                [
                    f"<details class=\"type-block\" data-type=\"{_safe_web_text(ttype, quote=True)}\" open>",
                    "<summary class=\"type-head\">",
                    f"<div class=\"type-title\"><span class=\"badge\">{title}</span></div>",
                    f"<span class=\"type-count\">{row_count} tanks</span>",
                    "</summary>",
                    "<div class=\"table-wrap\">",
                    "<table>",
                    "<colgroup>"
                    "<col class=\"col-tank\" />"
                    "<col class=\"col-score\" />"
                    "<col class=\"col-player\" />"
                    "<col class=\"col-tankopedia\" />"
                    "</colgroup>",
                    "<thead><tr><th>Tank</th><th class=\"score-head\">Damage</th><th>Player</th><th class=\"tankopedia-head\">🔗</th></tr></thead>",
                    f"<tbody>{_render_rows(rows, tankopedia_href=tankopedia_href, tankopedia_names_norm=tankopedia_names_norm, tank_badges_by_norm=tank_badges_by_norm)}</tbody>",
                    "</table>",
                    "</div>",
                    "</details>",
                ]
            )
        content.append("</div>")
        content.append("</details>")
    content.append("</section>")
    content.extend(
        [
            "<section class=\"view-panel\" data-main-view=\"player\">",
            "<div class=\"tier-card\">",
            "<div class=\"tier-body\">",
            "<div data-player-list>",
            _render_player_blocks(
                player_rows,
                tankopedia_href=tankopedia_href,
                tankopedia_names_norm=tankopedia_names_norm,
                tank_badges_by_norm=tank_badges_by_norm,
            ),
            "</div>",
            "</div>",
            "</div>",
            "</section>",
        ]
    )

    content.extend(
        [
            "<p class=\"footer\">",
            f"Generated at {_safe_web_text(_fmt_local(now))} • Tanks listed: {tank_total}",
            "</p>",
            "</main>",
            f"<script id=\"tb-data\" type=\"application/json\">{data_blob}</script>",
            f"<script>{_build_script()}</script>",
            "</body>",
            "</html>",
        ]
    )
    return "".join(content)


async def generate_leaderboard_page() -> str | None:
    if not config.WEB_LEADERBOARD_ENABLED:
        return None

    tanks = await db.list_tanks()
    tankopedia_badges = await db.list_tankopedia_tank_badges()
    tankopedia_names_norm = {
        utils.norm_tank_name(name)
        for name, _is_premium, _is_collectible in tankopedia_badges
        if str(name or "").strip()
    }
    tank_badges_by_norm: dict[str, tuple[bool, bool]] = {}
    for name, is_premium, is_collectible in tankopedia_badges:
        norm = utils.norm_tank_name(name)
        if not norm:
            continue
        prev_collectible, prev_premium = tank_badges_by_norm.get(norm, (False, False))
        tank_badges_by_norm[norm] = (
            prev_collectible or bool(int(is_collectible)),
            prev_premium or bool(int(is_premium)),
        )
    tankopedia_href = _tankopedia_relative_href()
    top_per_tier_rows = await db.stats_top_per_tier(limit_per_tier=3)
    top_tanks_rows = await db.stats_most_recorded_tanks(limit=10)
    unique_player_count = await db.stats_unique_player_count()
    yearly_rows = await db.stats_submissions_by_year()
    monthly_rows = await db.stats_submissions_by_month()
    recent_changes_rows = await db.score_changes(limit=10)
    grouped: dict[int, dict[str, list[dict]]] = defaultdict(dict)
    player_rows: list[dict] = []
    seen_buckets: set[tuple[int, str]] = set()
    for _name, tier, ttype in tanks:
        key = (int(tier), str(ttype))
        if key in seen_buckets:
            continue
        seen_buckets.add(key)
        rows = await db.best_per_tank_for_bucket(int(tier), str(ttype))
        sorted_rows = _sorted_snapshot_rows(rows)
        grouped[int(tier)][str(ttype)] = sorted_rows
        for row in sorted_rows:
            player_rows.append(
                {
                    **row,
                    "tier": int(tier),
                    "type": str(ttype),
                }
            )

    recent_changes: list[dict[str, str]] = []
    for cid, action, _sid, tank_name, player_name, old_score, new_score, _actor, created_at, _details in recent_changes_rows:
        old_text = _format_score(int(old_score)) if old_score is not None else "-"
        new_text = _format_score(int(new_score)) if new_score is not None else "-"
        display_player = str(player_name)
        if str(action).strip().lower() == "delete":
            try:
                if new_score is None or int(new_score) <= 0:
                    display_player = "-"
            except Exception:
                display_player = "-"
        recent_changes.append(
            {
                "id": int(cid),
                "action": str(action),
                "tank_name": str(tank_name),
                "player_name": display_player,
                "score_change": f"{old_text} -> {new_text}",
                "when": _fmt_local(str(created_at)),
            }
        )

    data_payload = {
        "tiers": sorted({int(t) for _n, t, _ty in tanks}, reverse=True),
        "types": sorted({str(ty) for _n, _t, ty in tanks}, key=lambda v: TYPE_ORDER.get(v, 99)),
        "recent_changes": recent_changes,
    }

    clan_name_raw = config.WEB_CLAN_NAME
    if config.WEB_CLAN_NAME_CASE == "uppercase":
        clan_name_raw = clan_name_raw.upper()
    font_family = "\"Plus Jakarta Sans\", \"Segoe UI\", sans-serif"
    if config.WEB_FONT_MODE == "monospace":
        font_family = "\"IBM Plex Mono\", \"JetBrains Mono\", \"SFMono-Regular\", Menlo, Monaco, Consolas, \"Liberation Mono\", \"Courier New\", monospace"

    html = _render_html(
        clan_name=clan_name_raw,
        clan_motto=(config.WEB_CLAN_MOTTO or "").strip() or None,
        clan_description=(config.WEB_CLAN_DESCRIPTION or "").strip() or None,
        banner_url=config.WEB_BANNER_URL or None,
        clan_name_align=config.WEB_CLAN_NAME_ALIGN,
        font_family=font_family,
        theme={
            "bg_color": config.WEB_BG_COLOR,
            "font_color": config.WEB_FONT_COLOR,
            "damage_color": config.WEB_DAMAGE_COLOR,
            "tank_name_color": config.WEB_TANK_NAME_COLOR,
            "player_name_color": config.WEB_PLAYER_NAME_COLOR,
            "clan_name_color": config.WEB_CLAN_NAME_COLOR,
            "motto_color": config.WEB_MOTTO_COLOR,
            "leaderboard_color": config.WEB_LEADERBOARD_COLOR,
        },
        grouped=grouped,
        player_rows=player_rows,
        tank_total=len(tanks),
        top_per_tier_rows=top_per_tier_rows,
        top_tanks_rows=top_tanks_rows,
        unique_player_count=unique_player_count,
        yearly_rows=yearly_rows,
        monthly_rows=monthly_rows,
        data_blob=_json_for_html(data_payload),
        tankopedia_href=tankopedia_href,
        tankopedia_names_norm=tankopedia_names_norm,
        tank_badges_by_norm=tank_badges_by_norm,
    )
    output_path = Path(config.WEB_OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return str(output_path)
