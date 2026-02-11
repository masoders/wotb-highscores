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
.view-controls {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 14px;
  flex-wrap: wrap;
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
.bulk-actions {
  display: inline-flex;
  gap: 8px;
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
}
.col-tank { width: 44%; }
.col-type { width: 14%; }
.col-tier { width: 10%; }
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
  .col-tank { width: 58%; }
  .col-score { width: 22%; }
  .col-player { width: 20%; }
  th, td { padding: 9px 7px; }
  .stats-grid { grid-template-columns: 1fr; }
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


def _group_rows_by_player(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        player_key = str(row.get("player_name") or "—")
        grouped[player_key].append(row)
    return grouped


def _render_player_rows(rows: list[dict]) -> str:
    out: list[str] = []
    sorted_rows = sorted(
        rows,
        key=lambda r: (
            -(int(r.get("score")) if isinstance(r.get("score"), int) else -1),
            str(r.get("tank_name") or "").casefold(),
        ),
    )
    for row in sorted_rows:
        tank = escape(str(row.get("tank_name") or "Unknown"))
        ttype = escape(utils.title_case_type(str(row.get("type") or "")))
        tier = escape(str(row.get("tier") or "—"))
        score = row.get("score")
        when = escape(utils.fmt_utc(row.get("created_at")))
        score_text = escape(_format_score(score if isinstance(score, int) else None))
        out.append(
            "<tr>"
            f"<td>{tank}</td>"
            f"<td>{ttype}</td>"
            f"<td>{tier}</td>"
            f"<td class=\"score\">{score_text}</td>"
            f"<td class=\"hide-sm muted\">{when}</td>"
            "</tr>"
        )
    return "".join(out)


def _render_player_blocks(rows: list[dict]) -> str:
    grouped = _group_rows_by_player(rows)
    out: list[str] = []
    for player in sorted(grouped.keys(), key=lambda p: p.casefold()):
        safe_player = escape(player)
        player_rows = grouped[player]
        tank_count = len(player_rows)
        out.extend(
            [
                "<details class=\"type-block\" open>",
                "<summary class=\"type-head\">",
                f"<div class=\"type-title\"><span class=\"badge\">{safe_player}</span></div>",
                f"<span class=\"type-count\">{tank_count} tanks</span>",
                "</summary>",
                "<div class=\"table-wrap\">",
                "<table>",
                "<colgroup>"
                "<col class=\"col-tank\" />"
                "<col class=\"col-type\" />"
                "<col class=\"col-tier\" />"
                "<col class=\"col-score\" />"
                "<col class=\"col-updated\" />"
                "</colgroup>",
                "<thead><tr><th>Tank</th><th>Type</th><th>Tier</th><th class=\"score-head\">Best Score</th><th class=\"hide-sm\">Updated</th></tr></thead>",
                f"<tbody>{_render_player_rows(player_rows)}</tbody>",
                "</table>",
                "</div>",
                "</details>",
            ]
        )
    return "".join(out)


def _render_stats_top_per_tier(rows: list[tuple[int, int, str, str, int]]) -> str:
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
                "<span class=\"type-count\">Top 3 scores</span>",
                "</summary>",
                "<div class=\"table-wrap\">",
                "<table class=\"stats-table\">",
                "<thead><tr><th class=\"stats-rank\">#</th><th class=\"stats-score\">Score</th><th>Player</th><th>Tank</th></tr></thead>",
                "<tbody>",
            ]
        )
        for _tier, rank, tank_name, player_name, score in grouped[tier]:
            out.append(
                "<tr>"
                f"<td class=\"stats-rank\">{rank}</td>"
                f"<td class=\"stats-score\">{escape(_format_score(score))}</td>"
                f"<td>{escape(player_name)}</td>"
                f"<td>{escape(tank_name)}</td>"
                "</tr>"
            )
        out.extend(["</tbody>", "</table>", "</div>", "</details>"])
    if not out:
        return "<p class=\"muted\">No submission data yet.</p>"
    return "".join(out)


def _render_stats_tanks(rows: list[tuple[str, int]]) -> str:
    out: list[str] = [
        "<table class=\"stats-table\">",
        "<thead><tr><th class=\"stats-rank\">#</th><th>Tank</th><th class=\"stats-count\">Submissions</th></tr></thead>",
        "<tbody>",
    ]
    for i, (tank_name, count) in enumerate(rows, start=1):
        out.append(
            "<tr>"
            f"<td class=\"stats-rank\">{i}</td>"
            f"<td>{escape(str(tank_name))}</td>"
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
        f"<thead><tr><th>{escape(label)}</th><th class=\"stats-count\">Submissions</th></tr></thead>",
        "<tbody>",
    ]
    for key, count in rows:
        out.append(
            "<tr>"
            f"<td>{escape(str(key))}</td>"
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
  const wrappers = Array.from(document.querySelectorAll(".table-wrap"));
  const buttons = Array.from(document.querySelectorAll("[data-view-btn]"));
  const actionButtons = Array.from(document.querySelectorAll("[data-bulk-action]"));
  const panels = Array.from(document.querySelectorAll("[data-main-view]"));
  let current = "tank";

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
  };

  buttons.forEach((btn) => {
    btn.addEventListener("click", () => update(btn.getAttribute("data-view-btn") || "tank"));
  });

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

  update(current);
})();
"""


def _render_html(
    clan_name: str,
    clan_motto: str | None,
    banner_url: str | None,
    grouped: dict[int, dict[str, list[dict]]],
    player_rows: list[dict],
    tank_total: int,
    top_per_tier_rows: list[tuple[int, int, str, str, int]],
    top_tanks_rows: list[tuple[str, int]],
    unique_player_count: int,
    yearly_rows: list[tuple[str, int]],
    monthly_rows: list[tuple[str, int]],
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
        "<div class=\"view-controls\">",
        "<span class=\"view-label\">View</span>",
        "<div class=\"view-toggle\" role=\"group\" aria-label=\"Leaderboard view\">",
        "<button type=\"button\" data-view-btn=\"tank\" class=\"active\" aria-pressed=\"true\">By Tank</button>",
        "<button type=\"button\" data-view-btn=\"player\" aria-pressed=\"false\">By Player</button>",
        "<button type=\"button\" data-view-btn=\"stats\" aria-pressed=\"false\">Statistics</button>",
        "</div>",
        "<div class=\"bulk-actions\" role=\"group\" aria-label=\"Expand and collapse\">",
        "<button type=\"button\" data-bulk-action=\"collapse\">Collapse All</button>",
        "<button type=\"button\" data-bulk-action=\"expand\">Expand All</button>",
        "</div>",
        "</div>",
        "<section class=\"view-panel\" data-main-view=\"tank\">",
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
    content.append("</section>")
    content.extend(
        [
            "<section class=\"view-panel\" data-main-view=\"player\">",
            "<div class=\"tier-card\">",
            "<div class=\"tier-body\">",
            _render_player_blocks(player_rows),
            "</div>",
            "</div>",
            "</section>",
        ]
    )
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
            _render_stats_tanks(top_tanks_rows),
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
            "<div class=\"stats-card\" style=\"margin-top: 14px;\">",
            "<h3 class=\"stats-title\">Top 3 Per Tier (all tanks)</h3>",
            _render_stats_top_per_tier(top_per_tier_rows),
            "</div>",
            "</div>",
            "</div>",
            "</section>",
        ]
    )

    content.extend(
        [
            "<p class=\"footer\">",
            f"Generated at {escape(now)} UTC • Tanks listed: {tank_total}",
            "</p>",
            "</main>",
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
    top_per_tier_rows = await db.stats_top_per_tier(limit_per_tier=3)
    top_tanks_rows = await db.stats_most_recorded_tanks(limit=10)
    unique_player_count = await db.stats_unique_player_count()
    yearly_rows = await db.stats_submissions_by_year()
    monthly_rows = await db.stats_submissions_by_month()
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

    html = _render_html(
        clan_name=config.WEB_CLAN_NAME,
        clan_motto=(config.WEB_CLAN_MOTTO or "").strip() or None,
        banner_url=config.WEB_BANNER_URL or None,
        grouped=grouped,
        player_rows=player_rows,
        tank_total=len(tanks),
        top_per_tier_rows=top_per_tier_rows,
        top_tanks_rows=top_tanks_rows,
        unique_player_count=unique_player_count,
        yearly_rows=yearly_rows,
        monthly_rows=monthly_rows,
    )
    output_path = Path(config.WEB_OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return str(output_path)
