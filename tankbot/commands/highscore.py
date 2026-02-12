import discord, csv, io
from discord import app_commands
from datetime import datetime, timezone
import difflib
import logging
from .. import config, db, utils, forum_index, static_site, audit_channel

grp = app_commands.Group(name="highscore", description="Highscore commands")
logger = logging.getLogger(__name__)

async def _refresh_webpage_notice(*, context: str) -> str:
    try:
        page_path = await static_site.generate_leaderboard_page()
    except Exception as exc:
        logger.exception("Failed to update static leaderboard page")
        return f"⚠️ {context} webpage update failed: `{type(exc).__name__}`"
    if not page_path:
        return "ℹ️ Static webpage generation is disabled."
    return f"🌐 Static webpage updated: `{page_path}`"

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

async def _resolve_player_for_storage(player_input: str) -> tuple[str, str, str | None, list[str]]:
    player_raw = utils.validate_text("Player", player_input, 64)
    player_norm = utils.normalize_player(player_raw)
    canonical = await db.get_player_name_canonical(player_raw)
    normalized_note: str | None = None
    if canonical != player_raw:
        normalized_note = f"Using existing player name **{canonical}** for consistency."
    suggestions = await db.suggest_player_names(player_raw, limit=3)
    return canonical, player_norm, normalized_note, suggestions

async def _resolve_tank_for_storage(tank_input: str) -> tuple[tuple[str, int, str] | None, list[str]]:
    tank_raw = utils.validate_text("Tank", tank_input, 64)
    canonical = await db.get_tank_canonical(tank_raw)
    if canonical:
        return canonical, []
    suggestions = await db.suggest_tank_names(tank_raw, limit=3)
    return None, suggestions

def _format_audit_score(value: int | None) -> str:
    return "—" if value is None else str(int(value))

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
    tank_lookup_loose: dict[str, list[tuple[str, int, str]]] = {}
    all_tanks: list[tuple[str, int, str]] = []
    for name, tier, ttype in await db.list_tanks():
        canonical = (str(name), int(tier), str(ttype))
        all_tanks.append(canonical)
        tank_lookup[utils.norm_tank_name(str(name))] = canonical
        loose = utils.loose_tank_key(str(name))
        tank_lookup_loose.setdefault(loose, []).append(canonical)
    auto_mapped: list[tuple[int, str, str, str]] = []

    def resolve_tank_row(tank_in: str) -> tuple[tuple[str, int, str] | None, str | None]:
        # 1) strict normalized key
        n = utils.norm_tank_name(tank_in)
        t = tank_lookup.get(n)
        if t:
            return t, None

        # 2) loose normalized key (punctuation/diacritics insensitive)
        lk = utils.loose_tank_key(tank_in)
        loose_hits = tank_lookup_loose.get(lk, [])
        if len(loose_hits) == 1:
            return loose_hits[0], "loose-exact"

        # 2b) common alias keys from imports (only if unique in DB)
        alias_targets = {
            "amxac46": "AMX AC mle. 46",
            "ru251": "Spähpanzer Ru 251",
            "progetto46": "Progetto M35 mod. 46",
            "obj244": "Object 244",
            "t25pilot1": "T25 Pilot Number 1",
            "t54mod1": "T-54 mod. 1",
            "sau40": "Somua SAu 40",
        }
        alias_target = alias_targets.get(lk)
        if alias_target:
            alias_key = utils.norm_tank_name(alias_target)
            t = tank_lookup.get(alias_key)
            if t:
                return t, "alias"

        # 3) containment on loose keys (handles shortened forms like "Ru 251")
        contain_hits = []
        if len(lk) >= 5:
            for n2, t2, ty2 in all_tanks:
                l2 = utils.loose_tank_key(n2)
                if lk in l2 and len(l2) >= len(lk):
                    contain_hits.append((n2, t2, ty2))
        if len(contain_hits) == 1:
            return contain_hits[0], "loose-contains"

        # 4) conservative fuzzy fallback by loose key
        ranked = []
        for n2, t2, ty2 in all_tanks:
            score = difflib.SequenceMatcher(None, lk, utils.loose_tank_key(n2)).ratio()
            ranked.append((score, (n2, t2, ty2)))
        ranked.sort(key=lambda x: x[0], reverse=True)
        if ranked:
            best_score, best_tank = ranked[0]
            second_score = ranked[1][0] if len(ranked) > 1 else 0.0
            # high confidence + separation from second best
            if best_score >= 0.90 and (best_score - second_score) >= 0.04:
                return best_tank, f"fuzzy:{best_score:.2f}"

        return None, None

    submitted_by_default = interaction.user.display_name
    now = utils.utc_now_z()
    canonical_players = await db.canonical_player_name_map()

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
        t, method = resolve_tank_row(tank_in)
        if not t:
            suggestions = await db.suggest_tank_names(tank_in, limit=3)
            msg = f"Line {i}: unknown tank '{tank_in}'"
            if suggestions:
                msg += " (did you mean: " + ", ".join(suggestions) + ")"
            errors.append(msg)
            continue
        if method:
            auto_mapped.append((i, tank_in, t[0], method))

        tank_name, tier, ttype = t

        player_raw = utils.validate_text("Player", player_in, 64)
        player_norm = utils.normalize_player(player_raw)
        canonical_player = canonical_players.get(player_norm, player_raw)
        canonical_players.setdefault(player_norm, canonical_player)

        created_at = _parse_iso8601(created_in) or now
        inserts.append((canonical_player, player_norm, tank_name, score, submitted_by, created_at))
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
    msg_lines.append(f"Auto-mapped tanks: **{len(auto_mapped)}**")
    msg_lines.append(f"Dry-run: **{dry_run}**")

    if errors:
        # Don’t spam; show first 15
        msg_lines.append("")
        msg_lines.append("First errors:")
        for e in errors[:15]:
            msg_lines.append(f"- {e}")
        if len(errors) > 15:
            msg_lines.append(f"- ...and {len(errors) - 15} more")
    if auto_mapped:
        msg_lines.append("")
        msg_lines.append("Auto-mapped tank names:")
        for ln, src, dst, method in auto_mapped[:10]:
            msg_lines.append(f"- Line {ln}: `{src}` -> **{dst}** ({method})")
        if len(auto_mapped) > 10:
            msg_lines.append(f"- ...and {len(auto_mapped) - 10} more")

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
    applied = await db.insert_submissions_bulk(inserts)

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

    msg_lines.append(await _refresh_webpage_notice(context="Import applied, but"))

    msg_lines.append("")
    msg_lines.append(
        "✅ Import applied. "
        f"Added **{applied['added']}**, updated **{applied['updated']}**, ignored **{applied['ignored']}**."
    )
    await audit_channel.send(
        interaction.client,
        (
            "🧾 [score import] "
            f"actor={interaction.user.display_name} "
            f"file={file.filename} "
            f"attempted={applied['attempted']} "
            f"added={applied['added']} "
            f"updated={applied['updated']} "
            f"ignored={applied['ignored']}"
        ),
    )
    await interaction.followup.send("\n".join(msg_lines), ephemeral=True)

@grp.command(name="submit", description="Submit a new highscore (commanders only)")
@app_commands.describe(player="Player name", tank="Tank name", score="Score (1..100000)")
async def submit(interaction: discord.Interaction, player: str, tank: str, score: int):
    member = interaction.user
    if not isinstance(member, discord.Member) or not utils.has_commander_role(member):
        await interaction.response.send_message("Nope. Only **Clan Commanders** can submit.", ephemeral=True)
        return
    if not (1 <= score <= config.MAX_SCORE):
        await interaction.response.send_message(f"Score must be between 1 and {config.MAX_SCORE}.", ephemeral=True)
        return
    t, tank_suggestions = await _resolve_tank_for_storage(tank)
    if not t:
        msg = "Unknown tank. Use an existing tank from the roster."
        if tank_suggestions:
            msg += "\nDid you mean: " + ", ".join([f"**{s}**" for s in tank_suggestions])
        await interaction.response.send_message(msg, ephemeral=True)
        return

    player_raw, player_norm, normalized_note, suggestions = await _resolve_player_for_storage(player)
    tank_name, tier, ttype = t  # canonical casing + bucket info
    best = await db.get_best_for_tank(tank_name)
    qualifies, gate_msg = _highscore_gate_message(tank_name, score, best)
    if not qualifies:
        await interaction.response.send_message(f"❌ Not submitted. {gate_msg[2:]}", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    outcome = await db.insert_submission(
        player_raw,
        player_norm,
        tank_name,
        score,
        interaction.user.display_name,
        utils.utc_now_z(),
    )

    if outcome["status"] == "ignored":
        await interaction.followup.send(
            f"❌ Not submitted. Existing score for **{player_raw}** on **{tank_name}** is higher or equal.",
            ephemeral=True,
        )
        return
    # Update only the relevant bucket thread
    await forum_index.targeted_update(interaction.client, int(tier), str(ttype))
    await audit_channel.send(
        interaction.client,
        (
            "🧾 [score submit] "
            f"action={outcome['status']} "
            f"submission_id={outcome['submission_id']} "
            f"tank={tank_name} "
            f"player={player_raw} "
            f"old={_format_audit_score(outcome['old_score'])} "
            f"new={_format_audit_score(outcome['new_score'])} "
            f"actor={interaction.user.display_name}"
        ),
    )

    msg = f"✅ Submission stored. {gate_msg[2:]}"
    if normalized_note:
        msg += f"\nℹ️ {normalized_note}"
    if suggestions:
        msg += "\n💡 Similar existing names: " + ", ".join([f"**{s}**" for s in suggestions])
    msg += "\n" + await _refresh_webpage_notice(context="Submission saved, but")
    await interaction.followup.send(msg, ephemeral=True)

@submit.autocomplete("player")
async def submit_player_autocomplete(_interaction: discord.Interaction, current: str):
    names = await db.list_player_names(query=current, limit=25)
    return [app_commands.Choice(name=n, value=n) for n in names[:25]]

@submit.autocomplete("tank")
async def submit_tank_autocomplete(_interaction: discord.Interaction, current: str):
    names = await db.list_tank_names(query=current, limit=25)
    return [app_commands.Choice(name=n, value=n) for n in names[:25]]

@grp.command(name="edit", description="Edit an existing submission by id (score and optional player)")
@app_commands.describe(
    submission_id="Submission id from history",
    score="New score (1..100000)",
    player="Optional new player name",
)
async def edit(interaction: discord.Interaction, submission_id: int, score: int, player: str | None = None):
    member = interaction.user
    if not isinstance(member, discord.Member) or not utils.has_commander_role(member):
        await interaction.response.send_message("Nope. Only **Clan Commanders** can edit scores.", ephemeral=True)
        return
    if not (1 <= score <= config.MAX_SCORE):
        await interaction.response.send_message(f"Score must be between 1 and {config.MAX_SCORE}.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    new_player_raw: str | None = None
    new_player_norm: str | None = None
    normalized_note: str | None = None
    suggestions: list[str] = []
    if player is not None and player.strip():
        new_player_raw, new_player_norm, normalized_note, suggestions = await _resolve_player_for_storage(player)

    updated = await db.edit_submission_score(
        submission_id=submission_id,
        new_score=score,
        actor=interaction.user.display_name,
        created_at=utils.utc_now_z(),
        new_player_raw=new_player_raw,
        new_player_norm=new_player_norm,
    )
    if not updated:
        await interaction.followup.send("Submission not found.", ephemeral=True)
        return
    if updated.get("error") == "duplicate_player_for_tank":
        await interaction.followup.send(
            "❌ Could not edit submission: that tank already has a score for this player.",
            ephemeral=True,
        )
        return

    tank_name = updated["tank_name"]
    tank = await db.get_tank_canonical(tank_name)
    if tank:
        _tank_name, tier, ttype = tank
        await forum_index.targeted_update(interaction.client, int(tier), str(ttype))
    await audit_channel.send(
        interaction.client,
        (
            "🧾 [score edit] "
            f"submission_id={submission_id} "
            f"tank={tank_name} "
            f"player_old={updated['old_player_raw']} "
            f"player_new={updated['new_player_raw']} "
            f"old={updated['old_score']} "
            f"new={updated['new_score']} "
            f"actor={interaction.user.display_name}"
        ),
    )

    notice = await _refresh_webpage_notice(context="Score edit saved, but")
    player_note = ""
    if updated["old_player_raw"] != updated["new_player_raw"]:
        player_note = (
            f"\n👤 Player changed from **{updated['old_player_raw']}** "
            f"to **{updated['new_player_raw']}**."
        )
    if normalized_note:
        player_note += f"\nℹ️ {normalized_note}"
    if suggestions:
        player_note += "\n💡 Similar existing names: " + ", ".join([f"**{s}**" for s in suggestions])
    await interaction.followup.send(
        (
            f"✅ Updated submission **#{submission_id}** on **{tank_name}** "
            f"from **{updated['old_score']}** to **{updated['new_score']}**."
            f"{player_note}\n{notice}"
        ),
        ephemeral=True,
    )

@grp.command(name="delete", description="Delete an existing submission by id (commanders only)")
@app_commands.describe(submission_id="Submission id from history")
async def delete(interaction: discord.Interaction, submission_id: int):
    member = interaction.user
    if not isinstance(member, discord.Member) or not utils.has_commander_role(member):
        await interaction.response.send_message("Nope. Only **Clan Commanders** can delete scores.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    deleted = await db.delete_submission(
        submission_id=submission_id,
        actor=interaction.user.display_name,
        created_at=utils.utc_now_z(),
    )
    if not deleted:
        await interaction.followup.send("Submission not found.", ephemeral=True)
        return

    tank = await db.get_tank_canonical(deleted["tank_name"])
    if tank:
        _tank_name, tier, ttype = tank
        await forum_index.targeted_update(interaction.client, int(tier), str(ttype))
    await audit_channel.send(
        interaction.client,
        (
            "🧾 [score delete] "
            f"submission_id={submission_id} "
            f"tank={deleted['tank_name']} "
            f"old={deleted['old_score']} "
            f"new=— "
            f"actor={interaction.user.display_name}"
        ),
    )

    notice = await _refresh_webpage_notice(context="Score deletion saved, but")
    await interaction.followup.send(
        (
            f"✅ Deleted submission **#{submission_id}** on **{deleted['tank_name']}** "
            f"(score **{deleted['old_score']}**).\n{notice}"
        ),
        ephemeral=True,
    )

@grp.command(name="changes", description="Show score audit trail (admin only)")
@app_commands.describe(limit="How many audit rows (1-50)")
async def changes(interaction: discord.Interaction, limit: int = 20):
    member = interaction.user
    if not isinstance(member, discord.Member) or not utils.can_manage(member):
        await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
        return
    rows = await db.score_changes(limit=limit)
    if not rows:
        await interaction.response.send_message("No score changes logged.", ephemeral=True)
        return
    lines = ["**Score changes**"]
    for _id, action, submission_id, tank_name, player_name, old_score, new_score, actor, created, details in rows:
        lines.append(
            f"- #{_id} **{action}** submission #{submission_id or '-'} "
            f"**{player_name}** ({tank_name}) "
            f"`{_format_audit_score(old_score)} -> {_format_audit_score(new_score)}` "
            f"by **{actor}** • {created}"
            + (f" • {details}" if details else "")
        )
    msg = "\n".join(lines)
    if len(msg) > 1800:
        msg = msg[:1800] + "\n…(truncated)"
    await interaction.response.send_message(msg, ephemeral=True)

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
    if not (1 <= score <= config.MAX_SCORE):
        await interaction.response.send_message(f"Score must be between 1 and {config.MAX_SCORE}.", ephemeral=True)
        return
    t, tank_suggestions = await _resolve_tank_for_storage(tank)
    if not t:
        msg = "Unknown tank. Pick an existing tank from the roster."
        if tank_suggestions:
            msg += "\nDid you mean: " + ", ".join([f"**{s}**" for s in tank_suggestions])
        await interaction.response.send_message(msg, ephemeral=True)
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

@qualify.autocomplete("tank")
async def qualify_tank_autocomplete(_interaction: discord.Interaction, current: str):
    names = await db.list_tank_names(query=current, limit=25)
    return [app_commands.Choice(name=n, value=n) for n in names[:25]]

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

@grp.command(name="refresh_web", description="Regenerate static leaderboard webpage (commanders only)")
async def refresh_web(interaction: discord.Interaction):
    member = interaction.user
    if not isinstance(member, discord.Member) or not utils.has_commander_role(member):
        await interaction.response.send_message("Nope. Only **Clan Commanders** can refresh the webpage.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True, thinking=True)
    notice = await _refresh_webpage_notice(context="Manual refresh requested, but")
    await interaction.followup.send(notice, ephemeral=True)

def register(tree: app_commands.CommandTree, bot: discord.Client, guild: discord.Object | None = None):
    tree.add_command(grp, guild=guild)
