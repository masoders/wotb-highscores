import discord, csv, io
from discord import app_commands
from datetime import datetime, timezone
from .. import config, db, utils, forum_index

grp = app_commands.Group(name="highscore", description="Highscore commands")

def _parse_iso8601(s: str) -> str | None:
    s = (s or "").strip()
    if not s:
        return None
    # Accept "Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None

def _highscore_gate_message(tank_name: str, score: int, best: tuple | None) -> tuple[bool, str]:
    """
    Return (qualifies, message) for tank-record qualification.
    Message wording is shared across /qualify and /submit.
    """
    if best is None:
        return True, f"✅ No record exists for **{tank_name}**. This would become **#1**."

    bid, bplayer, bscore, _bcreated = best
    if score > bscore:
        return True, (
            f"✅ Current #1 for **{tank_name}** is **{bscore}** by **{bplayer}** (#{bid}). "
            f"This beats it by **+{score - bscore}**."
        )
    if score == bscore:
        return False, (
            f"❌ Current #1 for **{tank_name}** is **{bscore}** by **{bplayer}** (#{bid}). "
            "Ties do not qualify."
        )
    return False, (
        f"❌ Current #1 for **{tank_name}** is **{bscore}** by **{bplayer}** (#{bid}). "
        f"Short by **{bscore - score}**."
    )

@grp.command(name="import_scores", description="Import historical scores from CSV (admins only)")
@app_commands.describe(
    file="CSV file",
    dry_run="Validate only (default true)",
    confirm="Type YES to apply when dry_run is false",
    update_index="Update leaderboard threads after import (set false for faster bulk import)",
)
async def import_scores(
    interaction: discord.Interaction,
    file: discord.Attachment,
    dry_run: bool = True,
    confirm: str | None = None,
    update_index: bool = True,
):
    member = interaction.user
    if not isinstance(member, discord.Member) or not utils.can_manage(member):
        await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    await interaction.followup.send("⏳ Validating CSV…", ephemeral=True)
    # Read CSV bytes
    data = await file.read()
    text = data.decode("utf-8-sig", errors="replace")

    reader = csv.DictReader(io.StringIO(text))

    if not reader.fieldnames:
        await interaction.followup.send("CSV has no header row.", ephemeral=True)
        return

    # Column aliasing (so people don’t brick imports by naming)
    def get(row, *keys):
        for k in keys:
            if k in row and row[k] is not None:
                return row[k]
        return ""

    errors: list[str] = []
    inserts: list[tuple[str, str, str, int, str, str]] = []
    touched_buckets: set[tuple[int, str]] = set()
    tank_lookup: dict[str, tuple[str, int, str]] = {}
    for name, tier, ttype in await db.list_tanks():
        tank_lookup[utils.norm_tank_name(str(name))] = (str(name), int(tier), str(ttype))

    submitted_by_default = interaction.user.display_name
    now = utils.utc_now_z()

    for i, row in enumerate(reader, start=2):  # line numbers: header is 1
        tank_in = get(row, "tank_name", "tank").strip()
        score_in = get(row, "score").strip()
        player_in = get(row, "player_name", "player").strip()
        created_in = get(row, "created_at", "timestamp", "date").strip()
        submitted_by = get(row, "submitted_by").strip() or submitted_by_default
        
        if not tank_in or not score_in or not player_in:
            errors.append(f"Line {i}: missing tank_name/score/player_name")
            continue

        try:
            score = int(score_in)
        except Exception:
            errors.append(f"Line {i}: invalid score '{score_in}'")
            continue

        if not (1 <= score <= config.MAX_SCORE):
            errors.append(f"Line {i}: score out of range (1..{config.MAX_SCORE}): {score}")
            continue

        # Resolve tank (case-insensitive) + bucket info
        t = tank_lookup.get(utils.norm_tank_name(tank_in))
        if not t:
            errors.append(f"Line {i}: unknown tank '{tank_in}'")
            continue

        tank_name, tier, ttype = t

        player_raw = utils.validate_text("Player", player_in, 64)
        player_norm = utils.normalize_player(player_raw)

        created_at = _parse_iso8601(created_in) or now
        inserts.append((player_raw, player_norm, tank_name, score, submitted_by, created_at))
        touched_buckets.add((int(tier), str(ttype)))

        # Safety: don’t allow insane imports by accident
        if len(inserts) > 5000:
            errors.append("Import aborted: >5000 valid rows (safety limit). Split your CSV.")
            break

    # Report validation summary first
    msg_lines = []
    msg_lines.append(f"Parsed: **{reader.line_num}** lines")
    msg_lines.append(f"Valid rows: **{len(inserts)}**")
    msg_lines.append(f"Errors: **{len(errors)}**")
    msg_lines.append(f"Dry-run: **{dry_run}**")

    if errors:
        # Don’t spam; show first 15
        msg_lines.append("")
        msg_lines.append("First errors:")
        for e in errors[:15]:
            msg_lines.append(f"- {e}")
        if len(errors) > 15:
            msg_lines.append(f"- ...and {len(errors) - 15} more")

    if dry_run:
        await interaction.followup.send("\n".join(msg_lines), ephemeral=True)
        return
    confirm_norm = (confirm or "").strip().upper()

    if confirm_norm != "YES":
        await interaction.followup.send("\n".join(msg_lines + ["", f"❌ To apply, set `confirm` to **YES**. (you sent: `{confirm}`)"]),ephemeral=True,)
        return  

    msg_lines.append("")
    
    # Apply
    if not inserts:
        msg_lines.append("")
        msg_lines.append("⚠️ Nothing to import (0 valid rows). No changes were made.")
        await interaction.followup.send("\n".join(msg_lines), ephemeral=True)
        return

    await interaction.followup.send(f"⏳ Applying **{len(inserts)}** rows to the database…", ephemeral=True)
    await db.insert_submissions_bulk(inserts)

    # Targeted updates only (after insert), optionally skipped for speed.
    if update_index:
        if touched_buckets:
            await interaction.followup.send(
                f"⏳ Updating **{len(touched_buckets)}** leaderboard buckets…",
                ephemeral=True,
            )
        for tier, ttype in touched_buckets:
            await forum_index.targeted_update(interaction.client, int(tier), str(ttype))
    else:
        msg_lines.append("⚠️ Skipped leaderboard thread updates (`update_index=false`).")
        msg_lines.append("Run `/tank rebuild_index` after import to refresh snapshots.")

    msg_lines.append("")
    msg_lines.append(f"✅ Import applied. Inserted **{len(inserts)}** rows.")
    await interaction.followup.send("\n".join(msg_lines), ephemeral=True)

@grp.command(name="submit", description="Submit a new highscore (commanders only)")
@app_commands.describe(player="Player name", tank="Tank name", score="Score (1..100000)")
async def submit(interaction: discord.Interaction, player: str, tank: str, score: int):
    member = interaction.user
    if not isinstance(member, discord.Member) or not utils.has_commander_role(member):
        await interaction.response.send_message("Nope. Only **Clan Commanders** can submit.", ephemeral=True)
        return
    tank = utils.validate_text('Tank', tank, 64)
    if not (1 <= score <= config.MAX_SCORE):
        await interaction.response.send_message(f"Score must be between 1 and {config.MAX_SCORE}.", ephemeral=True)
        return
    t = await db.get_tank_canonical(tank)
    if not t:
        await interaction.response.send_message("Unknown tank. Use an existing tank from the roster.", ephemeral=True)
        return

    player_raw = utils.validate_text('Player', player, 64)
    player_norm = utils.normalize_player(player_raw)
    tank_name, tier, ttype = t  # canonical casing + bucket info
    best = await db.get_best_for_tank(tank_name)
    qualifies, gate_msg = _highscore_gate_message(tank_name, score, best)
    if not qualifies:
        await interaction.response.send_message(f"❌ Not submitted. {gate_msg[2:]}", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    await db.insert_submission(
        player_raw,
        player_norm,
        tank_name,
        score,
        interaction.user.display_name,
        utils.utc_now_z(),
    )

    # Update only the relevant bucket thread
    await forum_index.targeted_update(interaction.client, int(tier), str(ttype))
    await interaction.followup.send(f"✅ Submission stored. {gate_msg[2:]}", ephemeral=True)

@grp.command(name="show", description="Show current champion (filters optional)")
@app_commands.describe(tier="Filter by tier (1..10)", type="Filter by type (light/medium/heavy/td)")
async def show(interaction: discord.Interaction, tier: int | None = None, type: str | None = None):
    if tier is not None and not (1 <= tier <= 10):
        await interaction.response.send_message("Tier must be 1..10.", ephemeral=True)
        return
    if type is not None:
        type = type.strip().lower()
        if type not in ("light","medium","heavy","td"):
            await interaction.response.send_message("Type must be one of: light, medium, heavy, td.", ephemeral=True)
            return

    champ = await db.get_champion_filtered(tier=tier, ttype=type)
    if not champ:
        await interaction.response.send_message("No submissions found for that filter.", ephemeral=True)
        return

    cid, player, tank, score, submitted_by, created, ctier, ctype = champ
    label = "Global champion" if tier is None and type is None else "Champion"
    await interaction.response.send_message(
        f"🏆 **{label}***{score}** — **{player}** ({tank}) • Tier {ctier} {utils.title_case_type(ctype)} • #{cid} • {created}Z",
        ephemeral=True)

@grp.command(name="qualify", description="Check if a score would qualify as a new tank record (no submission)")
@app_commands.describe(player="Player name (optional)", tank="Tank name", score="Score to compare")
async def qualify(interaction: discord.Interaction, tank: str, score: int, player: str | None = None):
    tank = utils.validate_text('Tank', tank, 64)
    if not (1 <= score <= config.MAX_SCORE):
        await interaction.response.send_message(f"Score must be between 1 and {config.MAX_SCORE}.", ephemeral=True)
        return
    t = await db.get_tank_canonical(tank)
    if not t:
        await interaction.response.send_message("Unknown tank. Pick an existing tank from the roster.", ephemeral=True)
        return

    if player is None or not player.strip():
        player = interaction.user.display_name
        player = utils.validate_text('Player', player, 64)

    tank_name, tier, ttype = t
    best = await db.get_best_for_tank(tank_name)
    champ = await db.get_champion()

    lines = []
    lines.append("**Qualification check**")
    lines.append(f"- Player: **{player}**")
    lines.append(f"- Tank: **{tank_name}** (Tier **{tier}**, **{utils.title_case_type(ttype)}**)")
    lines.append(f"- Your score: **{score}**")

    _ok, msg = _highscore_gate_message(tank_name, score, best)
    lines.append(msg)

    if champ:
        _, cplayer, ctank, cscore, *_ = champ
        if score > cscore:
            lines.append("")
            lines.append(f"🏆 Would also beat global champion (**{cscore}**, {ctank} by {cplayer}).")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)

@grp.command(name="history", description="Show recent submissions (grouped) + stats")
@app_commands.describe(limit="How many recent entries (1-25)")
async def history(interaction: discord.Interaction, limit: int = 10):
    await interaction.response.defer(ephemeral=True, thinking=True)
    limit = max(1, min(limit, 25))
    rows = await db.get_recent(limit)
    if not rows:
        await interaction.response.send_message("No submissions yet.", ephemeral=True)
        return
    await interaction.followup.send("⏳ Building history…", ephemeral=True)
    champ = await db.get_champion()
    champ_id = champ[0] if champ else None

    grouped: dict[str, dict[int, list[tuple]]] = {}
    for r in rows:
        _id, player, tank_name, score, submitted_by, created_at, tier, ttype = r
        grouped.setdefault(ttype, {}).setdefault(int(tier), []).append(r)

    type_order = ["heavy", "medium", "light", "td"]
    types_sorted = [t for t in type_order if t in grouped] + [t for t in grouped.keys() if t not in type_order]

    lines: list[str] = []
    for ttype in types_sorted:
        lines.append(f"## {utils.title_case_type(ttype)}")
        for tier in sorted(grouped[ttype].keys(), reverse=True):
            lines.append(f"**Tier {tier}**")
            for (_id, player, tank_name, score, submitted_by, created_at, _tier, _ttype) in grouped[ttype][tier]:
                badge = "🏆 **TOP** " if champ_id is not None and _id == champ_id else ""
                lines.append(f"{badge}**#{_id}** — **{score}** — **{player}** ({tank_name}) • {created_at}Z")
            lines.append("")

    tops_tanks = await db.top_holders_by_tank(limit=5)
    tops_buckets = await db.top_holders_by_tier_type(limit=5)

    lines.append("---")
    lines.append("### 📊 Stats (current #1 holders)")
    lines.append("**Most #1 tanks:**")
    for i, (p, cnt) in enumerate(tops_tanks, start=1):
        lines.append(f"{i}. **{p}** — {cnt} tank tops")
    lines.append("")
    lines.append("**Most #1 Tier×Type buckets:**")
    for i, (p, cnt) in enumerate(tops_buckets, start=1):
        lines.append(f"{i}. **{p}** — {cnt} bucket tops")

    msg = "\n".join(lines).strip()
    if len(msg) > 1800:
        msg = msg[:1800] + "\n…(truncated)"
    await interaction.followup.send(msg, ephemeral=True)

def register(tree: app_commands.CommandTree, bot: discord.Client, guild: discord.Object | None = None):
    tree.add_command(grp, guild=guild)
