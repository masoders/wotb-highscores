from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from . import config, db, utils

TYPE_ORDER = {"heavy": 0, "medium": 1, "light": 2, "td": 3}


def _sorted_snapshot_rows(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: str(r.get("tank_name") or "").casefold())


def _build_styles() -> str:
    return """
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
}
* { box-sizing: border-box; }
body {
  margin: 0;
  color: var(--text);
  font-family: "Plus Jakarta Sans", "Segoe UI", sans-serif;
  background:
    radial-gradient(circle at 0% 0%, #1f2f53 0%, transparent 45%),
    radial-gradient(circle at 100% 100%, #123a67 0%, transparent 40%),
    linear-gradient(180deg, var(--bg-0), var(--bg-1));
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
}
h1 {
  margin: 0 0 6px;
  font-size: clamp(1.5rem, 4vw, 2.6rem);
  letter-spacing: 0.02em;
}
.meta {
  margin: 0;
  color: var(--muted);
  font-size: 0.96rem;
}
.section-title {
  margin: 34px 0 16px;
  font-size: 1.45rem;
  color: #f1f6ff;
}
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
}
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
}
.col-tank { width: 44%; }
.col-score { width: 16%; }
.col-player { width: 20%; }
.col-updated { width: 20%; }
.score-head, .score {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
tbody tr:hover { background: #253a5a66; }
.score {
  font-weight: 700;
  color: var(--good);
}
.muted { color: var(--muted); }
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
@media (max-width: 860px) {
  .hide-sm { display: none; }
  .col-tank { width: 58%; }
  .col-score { width: 22%; }
  .col-player { width: 20%; }
  th, td { padding: 9px 7px; }
}
"""


def _format_score(score: int | None) -> str:
    if score is None:
        return "-"
    return f"{int(score):,}"


def _render_rows(rows: list[dict]) -> str:
    out: list[str] = []
    for row in rows:
        tank = escape(str(row.get("tank_name") or "Unknown"))
        score = row.get("score")
        player = escape(str(row.get("player_name") or "—"))
        when = escape(utils.fmt_utc(row.get("created_at")))
        score_text = escape(_format_score(score if isinstance(score, int) else None))
        out.append(
            "<tr>"
            f"<td>{tank}</td>"
            f"<td class=\"score\">{score_text}</td>"
            f"<td>{player}</td>"
            f"<td class=\"hide-sm muted\">{when}</td>"
            "</tr>"
        )
    return "".join(out)


def _render_html(
    clan_name: str,
    clan_motto: str | None,
    banner_url: str | None,
    grouped: dict[int, dict[str, list[dict]]],
    tank_total: int,
) -> str:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    banner = ""
    if banner_url:
        safe_banner = escape(banner_url, quote=True)
        banner = (
            f"<img src=\"{safe_banner}\" alt=\"{escape(clan_name)} banner\" />"
            "<div class=\"overlay\"></div>"
        )
    content = [
        "<!doctype html>",
        "<html lang=\"en\">",
        "<head>",
        "<meta charset=\"utf-8\" />",
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />",
        f"<title>{escape(clan_name)} Leaderboard</title>",
        "<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">",
        "<link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>",
        "<link href=\"https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">",
        f"<style>{_build_styles()}</style>",
        "</head>",
        "<body>",
        "<main class=\"wrap\">",
        "<section class=\"hero\">",
        banner if banner else "<div style=\"height: 220px\"></div>",
        "<div class=\"hero-content\">",
        f"<h1>{escape(clan_name)}</h1>",
        (f"<p class=\"meta\">{escape(clan_motto)}</p>" if clan_motto else ""),
        "<p class=\"meta\">Static highscore board by Tier (main level) and Tank Type (sub level). Click headers to collapse.</p>",
        "</div>",
        "</section>",
        "<h2 class=\"section-title\">Leaderboard</h2>",
    ]

    for tier in sorted(grouped.keys(), reverse=True):
        tier_block = grouped[tier]
        bucket_count = sum(len(rows) for rows in tier_block.values())
        content.extend(
            [
                "<details class=\"tier-card\" open>",
                "<summary class=\"tier-head\">",
                f"<h2>Tier {tier}</h2>",
                f"<span class=\"tier-count\">{bucket_count} tanks</span>",
                "</summary>",
                "<div class=\"tier-body\">",
            ]
        )
        for ttype in sorted(tier_block.keys(), key=lambda v: TYPE_ORDER.get(v, 99)):
            title = escape(utils.title_case_type(ttype))
            rows = tier_block[ttype]
            row_count = len(rows)
            content.extend(
                [
                    "<details class=\"type-block\" open>",
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
                    "<col class=\"col-updated\" />"
                    "</colgroup>",
                    "<thead><tr><th>Tank</th><th class=\"score-head\">Best Score</th><th>Player</th><th class=\"hide-sm\">Updated</th></tr></thead>",
                    f"<tbody>{_render_rows(rows)}</tbody>",
                    "</table>",
                    "</div>",
                    "</details>",
                ]
            )
        content.append("</div>")
        content.append("</details>")

    content.extend(
        [
            "<p class=\"footer\">",
            f"Generated at {escape(now)} UTC • Tanks listed: {tank_total}",
            "</p>",
            "</main>",
            "</body>",
            "</html>",
        ]
    )
    return "".join(content)


async def generate_leaderboard_page() -> str | None:
    if not config.WEB_LEADERBOARD_ENABLED:
        return None

    tanks = await db.list_tanks()
    grouped: dict[int, dict[str, list[dict]]] = defaultdict(dict)
    seen_buckets: set[tuple[int, str]] = set()
    for _name, tier, ttype in tanks:
        key = (int(tier), str(ttype))
        if key in seen_buckets:
            continue
        seen_buckets.add(key)
        rows = await db.best_per_tank_for_bucket(int(tier), str(ttype))
        grouped[int(tier)][str(ttype)] = _sorted_snapshot_rows(rows)

    html = _render_html(
        clan_name=config.WEB_CLAN_NAME,
        clan_motto=(config.WEB_CLAN_MOTTO or "").strip() or None,
        banner_url=config.WEB_BANNER_URL or None,
        grouped=grouped,
        tank_total=len(tanks),
    )
    output_path = Path(config.WEB_OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return str(output_path)
