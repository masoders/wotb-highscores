from __future__ import annotations

import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from . import config, db


def _display_name_from_output_path(output_html: Path) -> str:
    stem = str(output_html.stem or "").strip()
    if not stem or stem.lower() == "index":
        return "Tankopedia Browser"
    cleaned = stem.replace("_", " ").replace("-", " ").strip()
    if not cleaned:
        return "Tankopedia Browser"
    return cleaned.title()


def _format_wg_updated(raw_value: str | None) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return "WG last updated: unavailable"
    # WG encyclopedia/info commonly returns epoch seconds for tanks_updated_at.
    try:
        epoch = int(raw)
        if epoch > 0:
            ts = datetime.fromtimestamp(epoch, tz=timezone.utc)
            return f"WG last updated: {ts.strftime('%Y-%m-%d')}"
    except Exception:
        pass
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ts = ts.astimezone(timezone.utc)
        return f"WG last updated: {ts.strftime('%Y-%m-%d')}"
    except Exception:
        return f"WG last updated: {raw}"


def _index_html(*, page_name: str, updated_text: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{page_name}</title>
  <link rel="stylesheet" href="./styles.css">
</head>
<body>
  <main class="wrap">
    <section class="hero no-banner">
      <div class="hero-content center">
        <h1>{page_name}</h1>
        <p class="meta">{updated_text}</p>
      </div>
    </section>

    <h2 class="section-title">Tankopedia</h2>
    <p class="star-legend">&#9733; = Premium/Collectible</p>
    <div class="view-controls">
      <div class="view-row top">
        <div class="filter-tools">
          <div class="filter-item">
            <label for="tierFilter">Tier</label>
            <select id="tierFilter">
              <option value="">All tiers</option>
              <option value="1">Tier 1</option>
              <option value="2">Tier 2</option>
              <option value="3">Tier 3</option>
              <option value="4">Tier 4</option>
              <option value="5">Tier 5</option>
              <option value="6">Tier 6</option>
              <option value="7">Tier 7</option>
              <option value="8">Tier 8</option>
              <option value="9">Tier 9</option>
              <option value="10">Tier 10</option>
            </select>
          </div>
          <div class="filter-item">
            <label for="typeFilter">Type</label>
            <select id="typeFilter">
              <option value="">All types</option>
            </select>
          </div>
          <div class="filter-item">
            <label for="sortSelect">Sort</label>
            <select id="sortSelect">
              <option value="default">Tier desc (default)</option>
              <option value="tier_asc">Tier asc</option>
              <option value="tier_desc">Tier desc</option>
              <option value="name_asc">Name asc</option>
              <option value="name_desc">Name desc</option>
            </select>
          </div>
          <div class="filter-item">
            <label for="premiumFilter">Premium</label>
            <select id="premiumFilter">
              <option value="">All</option>
              <option value="1">Premium only</option>
              <option value="0">Non-premium</option>
            </select>
          </div>
          <div class="filter-item">
            <label for="collectibleFilter">Collectible</label>
            <select id="collectibleFilter">
              <option value="">All</option>
              <option value="1">Collectible only</option>
              <option value="0">Non-collectible</option>
            </select>
          </div>
          <div class="filter-item">
            <label for="countryFilter">Country</label>
            <select id="countryFilter">
              <option value="">All countries</option>
            </select>
          </div>
          <div class="filter-item filter-search">
            <label for="searchInput">Search</label>
            <input id="searchInput" type="search" placeholder="Search tank name">
          </div>
        </div>
      </div>
      <div class="view-row bottom">
        <div class="bulk-actions">
          <button id="expandAllBtn" type="button">Expand all</button>
          <button id="collapseAllBtn" type="button">Collapse all</button>
          <button id="resetFiltersBtn" type="button">Reset filters</button>
        </div>
        <span id="resultCount" class="view-label">0 tanks</span>
      </div>
    </div>

    <section id="tankList"></section>
  </main>
  <script src="./app.js"></script>
</body>
</html>
"""


def _styles_css() -> str:
    return """:root {
  --bg-0: #0b1221;
  --bg-1: #131f37;
  --panel: #0f1729cc;
  --line: #2c3c5f;
  --text: #ecf1ff;
  --muted: #adc0ea;
  --accent: #6ee7ff;
  --accent-2: #3aa5ff;
}
*, *::before, *::after {
  box-sizing: border-box;
  font-family: inherit;
}
body {
  margin: 0;
  color: var(--text);
  font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
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
.hero-content {
  padding: 22px 24px;
  text-align: center;
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
  border: 1px solid #2a3f63;
  border-radius: 12px;
  background: #13223dcc;
  padding: 10px;
}
.view-row {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}
.view-row.bottom {
  align-items: center;
  width: 100%;
}
.view-label {
  color: var(--muted);
  font-size: 0.92rem;
  margin-left: auto;
  display: inline-flex;
  align-items: center;
  min-height: 35px;
  text-align: right;
}
.filter-tools {
  display: grid;
  width: 100%;
  grid-template-columns: repeat(4, minmax(160px, 1fr));
  gap: 10px;
}
.filter-item {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.filter-item label {
  color: var(--muted);
  font-size: 0.84rem;
}
.filter-item select,
.filter-item input,
.bulk-actions button {
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
.filter-item select,
.filter-item input {
  width: 100%;
}
.filter-item select {
  appearance: none;
}
.filter-item select:hover,
.filter-item input:hover,
.bulk-actions button:hover {
  background: #1e3357;
}
.filter-search {
  grid-column: span 2;
}
.bulk-actions {
  display: inline-flex;
  gap: 8px;
}
.bulk-actions button {
  cursor: pointer;
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
  overflow-x: auto;
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
  font-size: 0.9rem;
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
tbody tr:hover { background: #253a5a66; }
.data-row { cursor: pointer; }
.row-detail { display: none; }
.row-detail td {
  white-space: normal;
  color: var(--muted);
  font-size: 0.86rem;
  padding-top: 12px;
  padding-bottom: 12px;
}
.data-row.expanded + .row-detail { display: table-row; }
.tank-name { color: #ecf1ff; font-weight: 700; }
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
.badge {
  display: inline-block;
  padding: 3px 8px;
  border-radius: 99px;
  border: 1px solid #4d6796;
  color: #d7e7ff;
  font-size: 0.78rem;
}
.detail-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}
.detail-card {
  border: 1px solid #2a3f63;
  border-radius: 10px;
  background: #13223dcc;
  padding: 10px;
}
.detail-title {
  margin: 0 0 8px;
  color: #dce8ff;
  font-size: 0.92rem;
}
.kv {
  margin: 0;
  display: grid;
  grid-template-columns: repeat(2, minmax(120px, 1fr));
  gap: 6px 8px;
}
.kv div {
  border-bottom: 1px dotted #39507a;
  padding-bottom: 4px;
}
.kv dt { color: #9eb5df; font-size: 0.78rem; margin: 0; }
.kv dd { margin: 2px 0 0; color: #e4eeff; font-size: 0.84rem; }
.char-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}
.char-box {
  border: 1px solid #2a3f63;
  border-radius: 10px;
  background: #0f1c33cc;
  padding: 8px;
  display: flex;
  flex-direction: column;
  min-height: 260px;
}
.char-box .detail-title {
  margin-bottom: 6px;
}
.char-box .table-wrap {
  flex: 1;
  padding: 0;
}
.stats-table th, .stats-table td {
  padding: 7px;
  font-size: 0.84rem;
}
.stats-key { width: 60%; }
.empty {
  border: 1px solid #2a3f63;
  border-radius: 12px;
  padding: 16px;
  color: var(--muted);
}
@media (max-width: 860px) {
  .view-row { align-items: flex-start; }
  .filter-tools { grid-template-columns: 1fr; }
  .filter-search { grid-column: auto; }
  .filter-item input, .filter-item select { width: 100%; }
  .detail-grid { grid-template-columns: 1fr; }
  .char-grid { grid-template-columns: 1fr; }
}
"""


def _app_js() -> str:
    return """const state = {
  tanks: [],
  filtered: [],
  expandedIds: new Set(),
  openTierKeys: new Set(),
  openTypeKeys: new Set(),
};

const tierFilter = document.getElementById("tierFilter");
const typeFilter = document.getElementById("typeFilter");
const searchInput = document.getElementById("searchInput");
const sortSelect = document.getElementById("sortSelect");
const premiumFilter = document.getElementById("premiumFilter");
const collectibleFilter = document.getElementById("collectibleFilter");
const countryFilter = document.getElementById("countryFilter");
const tankList = document.getElementById("tankList");
const resultCount = document.getElementById("resultCount");
const expandAllBtn = document.getElementById("expandAllBtn");
const collapseAllBtn = document.getElementById("collapseAllBtn");
const resetFiltersBtn = document.getElementById("resetFiltersBtn");

function toText(v) {
  if (v === null || v === undefined) return "";
  return String(v);
}

function esc(v) {
  return toText(v)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function tierNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : -1;
}

function defaultCompare(a, b) {
  const tierCmp = tierNum(b.tier) - tierNum(a.tier);
  if (tierCmp !== 0) return tierCmp;
  const typeCmp = toText(a.type).localeCompare(toText(b.type), undefined, { sensitivity: "base" });
  if (typeCmp !== 0) return typeCmp;
  return toText(a.name).localeCompare(toText(b.name), undefined, { sensitivity: "base" });
}

function titleCaseType(value) {
  const raw = toText(value);
  if (!raw) return "Unknown";
  if (raw === "heavyTank") return "Heavy";
  if (raw === "mediumTank") return "Medium";
  if (raw === "lightTank") return "Light";
  if (raw === "AT-SPG") return "Tank Destroyer";
  if (raw === "spg") return "SPG";
  return raw;
}

function titleCaseWords(value) {
  const raw = toText(value).replaceAll("_", " ").replaceAll("-", " ").trim();
  if (!raw) return "Unknown";
  const normalized = raw.replace(/\s+/g, " ").toLowerCase();
  if (normalized === "uk") return "UK";
  if (normalized === "ussr") return "USSR";
  if (normalized === "usa") return "USA";
  return `${normalized[0].toUpperCase()}${normalized.slice(1)}`;
}

const TYPE_FILTER_ORDER = ["Light", "Medium", "Heavy", "Tank Destroyer"];

function applyFilters() {
  const tier = tierFilter.value;
  const type = typeFilter.value;
  const q = searchInput.value.trim().toLowerCase();
  const sort = sortSelect.value;
  const premium = premiumFilter.value;
  const collectible = collectibleFilter.value;
  const country = countryFilter.value;

  let rows = state.tanks.filter((tank) => {
    if (tier && String(tank.tier) !== tier) return false;
    if (type && toText(tank.type) !== type) return false;
    if (premium && (Number(tank.is_premium) ? "1" : "0") !== premium) return false;
    if (collectible && (Number(tank.is_collectible) ? "1" : "0") !== collectible) return false;
    if (country && toText(tank.nation) !== country) return false;
    if (q && !toText(tank.name).toLowerCase().includes(q)) return false;
    return true;
  });

  if (sort === "tier_asc") {
    rows.sort((a, b) => {
      const tierCmp = tierNum(a.tier) - tierNum(b.tier);
      if (tierCmp !== 0) return tierCmp;
      return defaultCompare(a, b);
    });
  } else if (sort === "tier_desc") {
    rows.sort((a, b) => {
      const tierCmp = tierNum(b.tier) - tierNum(a.tier);
      if (tierCmp !== 0) return tierCmp;
      return defaultCompare(a, b);
    });
  } else if (sort === "name_asc") {
    rows.sort((a, b) => toText(a.name).localeCompare(toText(b.name), undefined, { sensitivity: "base" }));
  } else if (sort === "name_desc") {
    rows.sort((a, b) => toText(b.name).localeCompare(toText(a.name), undefined, { sensitivity: "base" }));
  } else {
    rows.sort(defaultCompare);
  }

  state.filtered = rows;
  render();
}

function imageUrls(images) {
  if (!images || typeof images !== "object") return [];
  return Object.values(images).filter((v) => typeof v === "string" && v.startsWith("http"));
}

function flattenScalars(obj, prefix, out) {
  if (obj === null || obj === undefined) {
    return;
  }
  if (typeof obj === "string" || typeof obj === "number" || typeof obj === "boolean") {
    out.push([prefix || "value", obj]);
    return;
  }
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i += 1) {
      flattenScalars(obj[i], prefix ? `${prefix}[${i}]` : `[${i}]`, out);
    }
    return;
  }
  if (typeof obj === "object") {
    for (const [k, v] of Object.entries(obj)) {
      const key = prefix ? `${prefix}.${k}` : k;
      flattenScalars(v, key, out);
    }
  }
}

function titleCaseKey(value) {
  const raw = toText(value)
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .trim();
  if (!raw) return "Other";
  return raw
    .split(/\s+/)
    .map((part) => part ? `${part[0].toUpperCase()}${part.slice(1).toLowerCase()}` : "")
    .join(" ");
}

function pathSegments(path) {
  const raw = toText(path).trim();
  if (!raw) return [];
  return raw.match(/[^.[\]]+/g) || [];
}

function topLevelKeyFromPath(path) {
  const segments = pathSegments(path);
  return segments.length ? segments[0] : "";
}

function allFieldRows(details) {
  const source = (
    (details && details.raw && typeof details.raw === "object" && details.raw) ||
    (details && details.characteristics && typeof details.characteristics === "object" && details.characteristics) ||
    {}
  );
  const out = [];
  flattenScalars(source, "", out);
  return out;
}

const FIELD_GROUP_LABELS = {
  tank_id: "Tank",
  name: "Tank",
  short_name: "Tank",
  tier: "Tank",
  type: "Tank",
  nation: "Tank",
  is_premium: "Tank",
  is_collectible: "Tank",
  description: "Description",
  cost: "Cost",
  price_xp: "Research XP",
  prices_xp: "Research XP",
  next_tanks: "Next Tanks",
  default_profile: "Default Profile",
  modules_tree: "Modules Tree",
  guns: "Guns",
  turrets: "Turrets",
  engines: "Engines",
  suspensions: "Suspensions",
  radios: "Radios",
  images: "Images",
};

const FIELD_GROUP_ORDER = [
  "Tank",
  "Description",
  "Cost",
  "Research XP",
  "Next Tanks",
  "Default Profile: Core",
  "Default Profile: Armor",
  "Default Profile: Firepower",
  "Default Profile: Gun",
  "Default Profile: Shells",
  "Default Profile: Turret",
  "Default Profile: Engine",
  "Default Profile: Suspension",
  "Default Profile: Mobility",
  "Default Profile: Protection",
  "Default Profile: Misc",
  "Default Profile",
  "Modules Tree",
  "Guns",
  "Turrets",
  "Engines",
  "Suspensions",
  "Radios",
  "Images",
  "Other",
];

const DEFAULT_PROFILE_SECTION_LABELS = {
  armor: "Armor",
  firepower: "Firepower",
  shot_efficiency: "Firepower",
  gun: "Gun",
  gun_id: "Gun",
  shells: "Shells",
  turret: "Turret",
  turret_id: "Turret",
  engine: "Engine",
  engine_id: "Engine",
  suspension: "Suspension",
  suspension_id: "Suspension",
  speed_forward: "Mobility",
  speed_backward: "Mobility",
  maneuverability: "Mobility",
  weight: "Mobility",
  hull_weight: "Mobility",
  max_weight: "Mobility",
  hp: "Core",
  hull_hp: "Core",
  max_ammo: "Core",
  signal_range: "Core",
  battle_level_range_min: "Core",
  battle_level_range_max: "Core",
  profile_id: "Core",
  is_default: "Core",
  protection: "Protection",
};

function groupLabelForPath(path) {
  const segments = pathSegments(path);
  if (!segments.length) return "Other";
  const topKey = toText(segments[0]).toLowerCase();
  if (topKey === "default_profile") {
    const sectionKey = toText(segments[1]).toLowerCase();
    if (!sectionKey) return "Default Profile";
    const sectionLabel = DEFAULT_PROFILE_SECTION_LABELS[sectionKey] || "Misc";
    return `Default Profile: ${sectionLabel}`;
  }
  return FIELD_GROUP_LABELS[topKey] || titleCaseKey(topKey) || "Other";
}

function groupedAllFieldRows(details) {
  const grouped = new Map();
  for (const [k, v] of allFieldRows(details)) {
    const group = groupLabelForPath(k);
    if (!grouped.has(group)) grouped.set(group, []);
    grouped.get(group).push([k, v]);
  }
  for (const rows of grouped.values()) {
    rows.sort((a, b) => toText(a[0]).localeCompare(toText(b[0]), undefined, { sensitivity: "base" }));
  }
  return grouped;
}

function renderCharacteristicBox(title, rows) {
  const rowsHtml = rows.length
    ? rows.map(([k, v]) => `<tr><td class="stats-key">${esc(k)}</td><td>${esc(v)}</td></tr>`).join("")
    : "<tr><td colspan='2'>No values.</td></tr>";
  return `
    <section class="char-box">
      <h5 class="detail-title">${esc(title)}</h5>
      <div class="table-wrap">
        <table class="stats-table">
          <thead><tr><th class="stats-key">Property</th><th>Value</th></tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>
    </section>
  `;
}

function renderAllFieldBoxes(details) {
  const grouped = groupedAllFieldRows(details);
  if (!grouped.size) return "<div class=\\"empty\\">No data available.</div>";
  const ordered = [];
  for (const group of FIELD_GROUP_ORDER) {
    if (!grouped.has(group)) continue;
    ordered.push([group, grouped.get(group)]);
    grouped.delete(group);
  }
  const extras = Array.from(grouped.entries()).sort((a, b) => a[0].localeCompare(b[0], undefined, { sensitivity: "base" }));
  const visible = [...ordered, ...extras].filter(([group, rows]) => {
    if (!Array.isArray(rows) || !rows.length) return false;
    if (group === "Description") {
      return rows.some(([, value]) => {
        if (value === null || value === undefined) return false;
        if (typeof value === "string") return value.trim().length > 0;
        return true;
      });
    }
    return rows.length > 1;
  });
  if (!visible.length) return "<div class=\\"empty\\">No sections with more than one value.</div>";
  return visible
    .map(([group, rows]) => renderCharacteristicBox(group, rows))
    .join("");
}

function tankStarsHtml(tank) {
  if (!Number(tank.is_collectible) && !Number(tank.is_premium)) return "";
  return "<span class=\\"tank-stars\\"><span class=\\"tank-star\\" title=\\"Premium/Collectible tank\\" aria-label=\\"Premium or collectible tank\\">&#9733;</span></span>";
}

function groupByTierAndType(rows) {
  const grouped = new Map();
  for (const tank of rows) {
    const tierKey = String(tank.tier ?? "-");
    if (!grouped.has(tierKey)) grouped.set(tierKey, new Map());
    const byType = grouped.get(tierKey);
    const typeKey = toText(tank.type || "unknown");
    if (!byType.has(typeKey)) byType.set(typeKey, []);
    byType.get(typeKey).push(tank);
  }
  return grouped;
}

function renderTankRow(tank) {
  const details = tank.details || {};
  const rowId = String(tank.tank_id ?? "");
  const expanded = state.expandedIds.has(rowId);
  const stars = tankStarsHtml(tank);
  const boxHtml = renderAllFieldBoxes(details);

  return `
    <tr class="data-row ${expanded ? "expanded" : ""}" data-row-id="${esc(rowId)}" tabindex="0" aria-expanded="${expanded ? "true" : "false"}">
      <td class="tank-name">${esc(tank.name || "Unknown")}${stars}</td>
      <td>${esc(titleCaseType(tank.type))}</td>
      <td>${esc(tank.tier ?? "-")}</td>
      <td>${esc(titleCaseWords(tank.nation))}</td>
    </tr>
    <tr class="row-detail">
      <td colspan="4">
        <section class="detail-card">
          <h4 class="detail-title">Vehicle Fields (All)</h4>
          <div class="char-grid">${boxHtml}</div>
        </section>
      </td>
    </tr>
  `;
}

function captureOpenSections() {
  const tierKeys = new Set();
  const typeKeys = new Set();
  tankList.querySelectorAll("details.tier-card[open]").forEach((el) => {
    const tierKey = toText(el.getAttribute("data-tier")).trim();
    if (tierKey) tierKeys.add(tierKey);
  });
  tankList.querySelectorAll("details.type-block[open]").forEach((el) => {
    const tierKey = toText(el.getAttribute("data-tier")).trim();
    const typeKey = toText(el.getAttribute("data-type")).trim();
    if (!tierKey || !typeKey) return;
    typeKeys.add(`${tierKey}|${typeKey}`);
  });
  state.openTierKeys = tierKeys;
  state.openTypeKeys = typeKeys;
}

function render() {
  captureOpenSections();
  resultCount.textContent = `${state.filtered.length} tanks`;
  if (!state.filtered.length) {
    tankList.innerHTML = `<div class="empty">No tanks match current filters.</div>`;
    return;
  }

  const grouped = groupByTierAndType(state.filtered);
  const tierKeys = Array.from(grouped.keys()).sort((a, b) => tierNum(b) - tierNum(a));
  tankList.innerHTML = tierKeys.map((tierKey) => {
    const tierKeyText = toText(tierKey);
    const byType = grouped.get(tierKey);
    const typeKeys = Array.from(byType.keys()).sort((a, b) => titleCaseType(a).localeCompare(titleCaseType(b), undefined, { sensitivity: "base" }));
    const tierCount = Array.from(byType.values()).reduce((sum, rows) => sum + rows.length, 0);
    const tierOpen = state.openTierKeys.has(tierKeyText);
    const typeHtml = typeKeys.map((typeKey) => {
      const typeKeyText = toText(typeKey);
      const typeOpen = state.openTypeKeys.has(`${tierKeyText}|${typeKeyText}`);
      const tanks = byType.get(typeKey);
      const rowHtml = tanks.map((tank) => renderTankRow(tank)).join("");
      return `
        <details class="type-block" data-tier="${esc(tierKeyText)}" data-type="${esc(typeKeyText)}"${typeOpen ? " open" : ""}>
          <summary class="type-head">
            <h3 class="type-title">${esc(titleCaseType(typeKey))}</h3>
            <span class="type-count">${tanks.length} tanks</span>
          </summary>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Tank</th>
                  <th>Type</th>
                  <th>Tier</th>
                  <th>Nation</th>
                </tr>
              </thead>
              <tbody>${rowHtml}</tbody>
            </table>
          </div>
        </details>
      `;
    }).join("");
    return `
      <details class="tier-card" data-tier="${esc(tierKeyText)}"${tierOpen ? " open" : ""}>
        <summary class="tier-head">
          <h2>Tier ${esc(tierKey)}</h2>
          <span class="tier-count">${tierCount} tanks</span>
        </summary>
        <div class="tier-body">${typeHtml}</div>
      </details>
    `;
  }).join("");
}

function hydrateTypeFilter() {
  const types = Array.from(
    new Set(state.tanks.map((t) => toText(t.type)).filter((v) => v))
  ).sort((a, b) => {
    const aLabel = titleCaseType(a);
    const bLabel = titleCaseType(b);
    const aIdx = TYPE_FILTER_ORDER.indexOf(aLabel);
    const bIdx = TYPE_FILTER_ORDER.indexOf(bLabel);
    const aRank = aIdx >= 0 ? aIdx : TYPE_FILTER_ORDER.length;
    const bRank = bIdx >= 0 ? bIdx : TYPE_FILTER_ORDER.length;
    if (aRank !== bRank) return aRank - bRank;
    return aLabel.localeCompare(bLabel, undefined, { sensitivity: "base" });
  });
  for (const type of types) {
    const opt = document.createElement("option");
    opt.value = type;
    opt.textContent = titleCaseType(type);
    typeFilter.appendChild(opt);
  }
}

function hydrateCountryFilter() {
  const countries = Array.from(
    new Set(state.tanks.map((t) => toText(t.nation)).filter((v) => v))
  ).sort((a, b) => titleCaseWords(a).localeCompare(titleCaseWords(b), undefined, { sensitivity: "base" }));
  for (const country of countries) {
    const opt = document.createElement("option");
    opt.value = country;
    opt.textContent = titleCaseWords(country);
    countryFilter.appendChild(opt);
  }
}

expandAllBtn.addEventListener("click", () => {
  state.expandedIds = new Set(state.filtered.map((tank) => String(tank.tank_id ?? "")));
  render();
  tankList.querySelectorAll("details").forEach((el) => {
    el.open = true;
  });
});

collapseAllBtn.addEventListener("click", () => {
  state.expandedIds.clear();
  render();
  tankList.querySelectorAll("details").forEach((el) => {
    el.open = false;
  });
});

resetFiltersBtn.addEventListener("click", () => {
  tierFilter.value = "";
  typeFilter.value = "";
  sortSelect.value = "default";
  premiumFilter.value = "";
  collectibleFilter.value = "";
  countryFilter.value = "";
  searchInput.value = "";
  applyFilters();
});

tierFilter.addEventListener("change", applyFilters);
typeFilter.addEventListener("change", applyFilters);
sortSelect.addEventListener("change", applyFilters);
premiumFilter.addEventListener("change", applyFilters);
collectibleFilter.addEventListener("change", applyFilters);
countryFilter.addEventListener("change", applyFilters);
searchInput.addEventListener("input", applyFilters);

tankList.addEventListener("click", (event) => {
  const row = event.target.closest("tr.data-row");
  if (!row) return;
  const rowId = row.getAttribute("data-row-id") || "";
  if (!rowId) return;
  if (state.expandedIds.has(rowId)) {
    state.expandedIds.delete(rowId);
  } else {
    state.expandedIds = new Set([rowId]);
  }
  render();
});

tankList.addEventListener("keydown", (event) => {
  const row = event.target.closest("tr.data-row");
  if (!row) return;
  if (event.key !== "Enter" && event.key !== " ") return;
  event.preventDefault();
  row.click();
});

function openSectionsForExpandedRows() {
  if (!state.expandedIds.size) return;
  const rows = tankList.querySelectorAll("tr.data-row");
  rows.forEach((row) => {
    const rowId = row.getAttribute("data-row-id") || "";
    if (!state.expandedIds.has(rowId)) return;
    const typeBlock = row.closest("details.type-block");
    if (typeBlock) typeBlock.open = true;
    const tierCard = row.closest("details.tier-card");
    if (tierCard) tierCard.open = true;
  });
}

async function bootstrap() {
  const res = await fetch("./tanks.json", { cache: "no-store" });
  if (!res.ok) throw new Error(`Failed to load tanks.json (${res.status})`);
  state.tanks = await res.json();
  hydrateTypeFilter();
  hydrateCountryFilter();
  const params = new URLSearchParams(window.location.search);
  const searchParam = params.get("q") || params.get("tank") || "";
  if (searchParam) {
    searchInput.value = searchParam;
  }
  applyFilters();
  if (searchParam) {
    const norm = (v) => toText(v).trim().toLowerCase();
    const needle = norm(searchParam);
    const exact = state.filtered.filter((tank) => norm(tank.name) === needle);
    if (exact.length) {
      state.expandedIds = new Set(exact.map((tank) => String(tank.tank_id ?? "")));
      render();
      openSectionsForExpandedRows();
    } else if (state.filtered.length) {
      state.expandedIds = new Set([String(state.filtered[0].tank_id ?? "")]);
      render();
      openSectionsForExpandedRows();
    }
  }
}

bootstrap().catch((err) => {
  resultCount.textContent = "Failed to load data";
  tankList.innerHTML = `<div class="empty">${esc(err.message || err)}</div>`;
  console.error(err);
});
"""


async def generate_static_site(*, output_dir: str = "tanks") -> dict[str, object]:
    configured_output = str(getattr(config, "WG_TANKS_WEBPAGE_NAME", "tanks/index.html") or "").strip()
    if configured_output:
        output_html = Path(configured_output)
    else:
        output_html = Path(output_dir) / "index.html"
    output_base = output_html.parent if str(output_html.parent) not in {"", "."} else Path(".")
    page_name = escape(_display_name_from_output_path(output_html), quote=False)
    wg_updated_text = escape(_format_wg_updated(await db.get_tankopedia_meta("tanks_updated_at")), quote=False)
    rows = await db.list_tankopedia_tanks_for_export()
    payload: list[dict[str, object]] = []
    for row in rows:
        payload.append(
            {
                "tank_id": row.get("tank_id"),
                "name": row.get("name"),
                "tier": row.get("tier"),
                "type": row.get("type"),
                "nation": row.get("nation"),
                "is_premium": row.get("is_premium"),
                "is_collectible": row.get("is_collectible"),
                "details": {
                    "tank_id": row.get("tank_id"),
                    "name": row.get("name"),
                    "tier": row.get("tier"),
                    "type": row.get("type"),
                    "nation": row.get("nation"),
                    "is_premium": row.get("is_premium"),
                    "is_collectible": row.get("is_collectible"),
                    "characteristics": row.get("characteristics") or {},
                    "raw": row.get("raw") or {},
                },
            }
        )

    output_base.mkdir(parents=True, exist_ok=True)
    output_html.write_text(_index_html(page_name=page_name, updated_text=wg_updated_text), encoding="utf-8")
    (output_base / "styles.css").write_text(_styles_css(), encoding="utf-8")
    (output_base / "app.js").write_text(_app_js(), encoding="utf-8")
    (output_base / "tanks.json").write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    return {
        "output_dir": str(output_base),
        "output_html": str(output_html),
        "tank_count": len(payload),
    }
