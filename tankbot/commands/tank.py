import io
import csv
import discord
from discord import app_commands

from .. import db, forum_index, utils, static_site, audit_channel


class Tank(app_commands.Group):
    def __init__(self):
        super().__init__(name="tank", description="Tank roster commands")

def _require_admin(interaction: discord.Interaction) -> bool:
    m = interaction.user
    return isinstance(m, discord.Member) and utils.can_manage(m)

def _require_commander(interaction: discord.Interaction) -> bool:
    m = interaction.user
    return isinstance(m, discord.Member) and utils.has_commander_role(m)

async def _tank_not_found_message(name: str) -> str:
    suggestions = await db.suggest_tank_names(name, limit=3)
    msg = "Tank not found."
    if suggestions:
        msg += "\nDid you mean: " + ", ".join([f"**{s}**" for s in suggestions])
    return msg

async def _refresh_webpage():
    try:
        await static_site.generate_leaderboard_page()
    except Exception:
        # Keep tank operations successful even if page generation fails.
        return

def register(tree: app_commands.CommandTree, bot: discord.Client, guild: discord.Object | None):
    grp = Tank()
    tree.add_command(grp, guild=guild)

    @grp.command(name="add", description="Add a tank (commanders only)")
    async def add(interaction: discord.Interaction, name: str, tier: int, type: str):
        if not _require_commander(interaction):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can add tanks.", ephemeral=True)
            return
        name = utils.validate_text('Tank name', name, 64)
        type = type.strip().lower()
        if not (1 <= tier <= 10):
            await interaction.response.send_message("Tier must be 1..10.", ephemeral=True)
            return
        if type not in ("light", "medium", "heavy", "td"):
            await interaction.response.send_message("Type must be one of: light, medium, heavy, td.", ephemeral=True)
            return
        t = await db.get_tank_canonical(name)
        if t:
            await interaction.response.send_message("Tank already exists.", ephemeral=True)
            return

        await db.add_tank(name, tier, type, interaction.user.display_name, utils.utc_now_z())
        await forum_index.targeted_update(bot, tier, type)
        await _refresh_webpage()
        await audit_channel.send(
            interaction.client,
            (
                "🧾 [tank add] "
                f"name={name} "
                f"tier={tier} "
                f"type={type} "
                f"actor={interaction.user.display_name}"
            ),
        )
        await interaction.response.send_message(f"✅ Added **{name}** (Tier {tier}, {utils.title_case_type(type)}).", ephemeral=True)

    @add.autocomplete("name")
    async def add_name_autocomplete(_interaction: discord.Interaction, current: str):
        names = await db.list_tank_names(query=current, limit=25)
        return [app_commands.Choice(name=n, value=n) for n in names[:25]]

    @grp.command(name="edit", description="Edit tank tier/type (commanders only)")
    @app_commands.describe(
        name="Tank name",
        tier="Tier (1..10)",
        type="Type (light/medium/heavy/td)",
    )
    async def edit(interaction: discord.Interaction, name: str, tier: int, type: str):
        if not _require_commander(interaction):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can edit tanks.", ephemeral=True)
            return
        name = utils.validate_text('Tank name', name, 64)
        type = type.strip().lower()
        t = await db.get_tank(name)
        if not t:
            await interaction.response.send_message(await _tank_not_found_message(name), ephemeral=True)
            return
        old_tier, old_type = int(t[1]), t[2]
        if not (1 <= tier <= 10):
            await interaction.response.send_message("Tier must be 1..10.", ephemeral=True)
            return
        if type not in ("light", "medium", "heavy", "td"):
            await interaction.response.send_message("Type must be one of: light, medium, heavy, td.", ephemeral=True)
            return

        try:
            await db.edit_tank(
                name=name,
                tier=tier,
                ttype=type,
                actor=interaction.user.display_name,
                created_at=utils.utc_now_z(),
            )
        except ValueError as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
            return
        # Update both old and new buckets
        await forum_index.targeted_update(bot, old_tier, old_type)
        await forum_index.targeted_update(bot, tier, type)
        await _refresh_webpage()
        await audit_channel.send(
            interaction.client,
            (
                "🧾 [tank edit] "
                f"name={name} "
                f"old_tier={old_tier} "
                f"old_type={old_type} "
                f"new_tier={tier} "
                f"new_type={type} "
                f"actor={interaction.user.display_name}"
            ),
        )
        await interaction.response.send_message(f"✅ Updated **{name}**.", ephemeral=True)

    @grp.command(name="remove", description="Remove a tank (commanders only, only if no submissions)")
    async def remove(interaction: discord.Interaction, name: str):
        if not _require_commander(interaction):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can remove tanks.", ephemeral=True)
            return
        name = utils.validate_text('Tank name', name, 64)
        t = await db.get_tank(name)
        if not t:
            await interaction.response.send_message(await _tank_not_found_message(name), ephemeral=True)
            return
        tier, ttype = int(t[1]), t[2]
        try:
            await db.remove_tank(name, interaction.user.display_name, utils.utc_now_z())
        except Exception as e:
            await interaction.response.send_message(f"❌ {type(e).__name__}: {e}", ephemeral=True)
            return
        await forum_index.targeted_update(bot, tier, ttype)
        await _refresh_webpage()
        await audit_channel.send(
            interaction.client,
            (
                "🧾 [tank remove] "
                f"name={name} "
                f"tier={tier} "
                f"type={ttype} "
                f"actor={interaction.user.display_name}"
            ),
        )
        await interaction.response.send_message(f"✅ Removed **{name}**.", ephemeral=True)

    @grp.command(name="list", description="List tanks (commanders only)")
    async def list_cmd(interaction: discord.Interaction, tier: int | None = None, type: str | None = None):
        if not _require_commander(interaction):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can list tanks.", ephemeral=True)
            return
        if type is not None:
            type = type.strip().lower()
        rows = await db.list_tanks(tier=tier, ttype=type)
        if not rows:
            await interaction.response.send_message("No tanks found.", ephemeral=True)
            return
        lines = ["**Tanks**"]
        for n, tr, tp in rows[:200]:
            lines.append(f"- **{n}** — Tier {tr}, {utils.title_case_type(tp)}")
        msg = "\n".join(lines)
        if len(msg) > 1800:
            msg = msg[:1800] + "\n…(truncated)"
        await interaction.response.send_message(msg, ephemeral=True)

    @edit.autocomplete("name")
    async def edit_name_autocomplete(_interaction: discord.Interaction, current: str):
        names = await db.list_tank_names(query=current, limit=25)
        return [app_commands.Choice(name=n, value=n) for n in names[:25]]

    @remove.autocomplete("name")
    async def remove_name_autocomplete(_interaction: discord.Interaction, current: str):
        names = await db.list_tank_names(query=current, limit=25)
        return [app_commands.Choice(name=n, value=n) for n in names[:25]]

    @grp.command(name="rename", description="Rename a tank (commanders only)")
    @app_commands.describe(
        current_name="Current tank name",
        new_name="New corrected/canonical tank name",
    )
    async def rename(interaction: discord.Interaction, current_name: str, new_name: str):
        if not _require_commander(interaction):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can rename tanks.", ephemeral=True)
            return
        current_name = utils.validate_text("Tank name", current_name, 64)
        new_name = utils.validate_text("Tank name", new_name, 64)
        if utils.norm_tank_name(current_name) == utils.norm_tank_name(new_name):
            await interaction.response.send_message("Current and new names are the same.", ephemeral=True)
            return

        existing = await db.get_tank(current_name)
        if not existing:
            await interaction.response.send_message(await _tank_not_found_message(current_name), ephemeral=True)
            return
        tier, ttype = int(existing[1]), str(existing[2])

        try:
            await db.edit_tank(
                name=current_name,
                tier=tier,
                ttype=ttype,
                actor=interaction.user.display_name,
                created_at=utils.utc_now_z(),
                new_name=new_name,
            )
        except ValueError as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
            return

        await forum_index.targeted_update(bot, tier, ttype)
        await _refresh_webpage()
        await audit_channel.send(
            interaction.client,
            (
                "🧾 [tank rename] "
                f"from={current_name} "
                f"to={new_name} "
                f"tier={tier} "
                f"type={ttype} "
                f"actor={interaction.user.display_name}"
            ),
        )
        await interaction.response.send_message(
            f"✅ Renamed **{current_name}** -> **{new_name}**.",
            ephemeral=True,
        )

    @rename.autocomplete("current_name")
    async def rename_current_name_autocomplete(_interaction: discord.Interaction, current: str):
        names = await db.list_tank_names(query=current, limit=25)
        return [app_commands.Choice(name=n, value=n) for n in names[:25]]

    @grp.command(name="alias_add", description="Map an alternate tank name to a canonical tank (admin only)")
    @app_commands.describe(alias="Alias/variant name from imports", tank="Canonical tank name in roster")
    async def alias_add(interaction: discord.Interaction, alias: str, tank: str):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return
        alias = utils.validate_text("Alias", alias, 64)
        canonical = await db.get_tank_canonical(tank)
        if not canonical:
            await interaction.response.send_message("Canonical tank not found in roster.", ephemeral=True)
            return
        tank_name, tier, ttype = canonical
        await db.upsert_tank_alias(alias, tank_name, utils.utc_now_z())
        await audit_channel.send(
            interaction.client,
            (
                "🧾 [tank alias_add] "
                f"alias={alias} "
                f"tank={tank_name} "
                f"tier={tier} "
                f"type={ttype} "
                f"actor={interaction.user.display_name}"
            ),
        )
        await interaction.response.send_message(
            f"✅ Alias **{alias}** now maps to **{tank_name}** (Tier {tier}, {utils.title_case_type(ttype)}).",
            ephemeral=True,
        )

    @alias_add.autocomplete("tank")
    async def alias_add_tank_autocomplete(_interaction: discord.Interaction, current: str):
        names = await db.list_tank_names(query=current, limit=25)
        return [app_commands.Choice(name=n, value=n) for n in names[:25]]

    @grp.command(name="alias_list", description="List tank aliases (admin only)")
    async def alias_list(interaction: discord.Interaction, limit: int = 50):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return
        rows = await db.list_tank_aliases(limit=limit)
        if not rows:
            await interaction.response.send_message("No tank aliases configured.", ephemeral=True)
            return
        lines = ["**Tank aliases**"]
        for alias, tank_name, created in rows:
            lines.append(f"- **{alias}** -> **{tank_name}** • {created}Z")
        msg = "\n".join(lines)
        if len(msg) > 1800:
            msg = msg[:1800] + "\n…(truncated)"
        await interaction.response.send_message(msg, ephemeral=True)

    @grp.command(name="alias_seed_common", description="Seed common tank import aliases (admin only)")
    async def alias_seed_common(interaction: discord.Interaction):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return
        pairs = [
            ("AMX AC 46", "AMX AC mle. 46"),
            ("Ru 251", "Spähpanzer Ru 251"),
            ("Progetto 46", "Progetto M35 mod. 46"),
            ("Obj. 244", "Object 244"),
            ("T25 Pilot 1", "T25 Pilot Number 1"),
            ("T-54 mod. 1", "T-54 mod. 1"),
            ("SAu 40", "Somua SAu 40"),
            ("Skoda T 25", "Škoda T-25"),
            ("Skoda T 56", "Škoda T 56"),
            ("Kpz 07 RH", "Kpz 07 RH"),
            ("Pz. 38 nA", "Pz. 38 nA"),
            ("D.W. 2", "D.W.2"),
            ("T-28 mod. 1940", "T-28 mod.1940"),
        ]
        added = 0
        skipped = 0
        for alias, canonical_name in pairs:
            canonical = await db.get_tank_canonical(canonical_name)
            if not canonical:
                skipped += 1
                continue
            await db.upsert_tank_alias(alias, canonical[0], utils.utc_now_z())
            added += 1
        await audit_channel.send(
            interaction.client,
            (
                "🧾 [tank alias_seed_common] "
                f"actor={interaction.user.display_name} "
                f"added={added} "
                f"skipped={skipped}"
            ),
        )
        await interaction.response.send_message(
            f"✅ Seeded aliases. Added **{added}**, skipped **{skipped}** (missing canonical targets).",
            ephemeral=True,
        )

    @grp.command(name="merge", description="Merge duplicate tank variant into canonical tank (admin only)")
    @app_commands.describe(
        source="Old/duplicate tank name to merge from",
        target="Canonical tank name to merge into",
        remove_source="Remove source tank from roster if no submissions left (default true)",
    )
    async def merge(interaction: discord.Interaction, source: str, target: str, remove_source: bool = True):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return
        if utils.norm_tank_name(source) == utils.norm_tank_name(target):
            await interaction.response.send_message("Source and target cannot be the same tank.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        result = await db.merge_tank_into(
            source_name=source,
            target_name=target,
            actor=interaction.user.display_name,
            created_at=utils.utc_now_z(),
            remove_source=remove_source,
        )
        if result.get("error") == "tank_not_found":
            await interaction.followup.send("Source or target tank not found.", ephemeral=True)
            return

        canonical = await db.get_tank_canonical(result["target"])
        if canonical:
            _n, tier, ttype = canonical
            await forum_index.targeted_update(bot, int(tier), str(ttype))
        await _refresh_webpage()
        await audit_channel.send(
            interaction.client,
            (
                "🧾 [tank merge] "
                f"source={result['source']} "
                f"target={result['target']} "
                f"moved={result['moved']} "
                f"deleted={result['deleted']} "
                f"upgraded={result['upgraded']} "
                f"remove_source={result['remove_source']} "
                f"actor={interaction.user.display_name}"
            ),
        )
        await interaction.followup.send(
            (
                f"✅ Merged **{result['source']}** into **{result['target']}**.\n"
                f"- moved: **{result['moved']}**\n"
                f"- deleted source duplicates: **{result['deleted']}**\n"
                f"- upgraded target scores: **{result['upgraded']}**\n"
                f"- source removed: **{result['remove_source']}**"
            ),
            ephemeral=True,
        )

    @merge.autocomplete("source")
    async def merge_source_autocomplete(_interaction: discord.Interaction, current: str):
        names = await db.list_tank_names(query=current, limit=25)
        return [app_commands.Choice(name=n, value=n) for n in names[:25]]

    @merge.autocomplete("target")
    async def merge_target_autocomplete(_interaction: discord.Interaction, current: str):
        names = await db.list_tank_names(query=current, limit=25)
        return [app_commands.Choice(name=n, value=n) for n in names[:25]]

    @grp.command(name="changes", description="Show tank change log")
    async def changes(interaction: discord.Interaction, limit: int = 20):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return
        rows = await db.tank_changes(limit=limit)
        if not rows:
            await interaction.response.send_message("No changes logged.", ephemeral=True)
            return
        lines = ["**Tank changes**"]
        for _id, action, details, actor, created in rows:
            lines.append(f"- #{_id} **{action}** `{details}` by **{actor}** • {created}Z")
        msg = "\n".join(lines)
        if len(msg) > 1800:
            msg = msg[:1800] + "\n…(truncated)"
        await interaction.response.send_message(msg, ephemeral=True)

    @grp.command(name="export_csv", description="Export tank roster as CSV (commanders only)")
    async def export_csv(interaction: discord.Interaction):
        if not _require_commander(interaction):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can export tanks.", ephemeral=True)
            return
        rows = await db.list_tanks()
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["name", "tier", "type"])
        for n, tr, tp in rows:
            w.writerow([n, tr, tp])
        data = out.getvalue().encode("utf-8")
        await interaction.response.send_message("CSV export:", ephemeral=True, file=discord.File(io.BytesIO(data), filename="tanks.csv"))

    @grp.command(name="export_scores_csv", description="Export best score per tank as CSV (commanders only)")
    async def export_scores_csv(interaction: discord.Interaction):
        if not _require_commander(interaction):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can export tanks.", ephemeral=True)
            return
        rows = await db.list_tanks_with_best_scores()
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["tank", "score", "player", "tier", "type"])
        for tank_name, score, player_name, tier, ttype in rows:
            w.writerow([
                tank_name,
                ("" if score is None else score),
                ("" if player_name is None else player_name),
                tier,
                ttype,
            ])
        data = out.getvalue().encode("utf-8")
        await interaction.response.send_message(
            "CSV export (best score per tank):",
            ephemeral=True,
            file=discord.File(io.BytesIO(data), filename="tank_scores.csv"),
        )

    @grp.command(name="preview_import", description="Preview CSV import (no changes)")
    async def preview_import(interaction: discord.Interaction, csv_file: discord.Attachment, delete_missing: bool = False):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return
        raw = (await csv_file.read()).decode("utf-8", errors="replace")
        r = csv.DictReader(io.StringIO(raw))
        incoming = {}
        for row in r:
            name = utils.validate_text('Tank name', (row.get('name') or ''), 64) if (row.get('name') or '').strip() else ''
            if not name:
                continue
            incoming[name] = (int(row.get("tier") or 0), (row.get("type") or "").strip().lower())

        existing = {n: (int(t), tp) for n, t, tp in await db.list_tanks()}

        adds = [n for n in incoming.keys() if n not in existing]
        edits = [n for n in incoming.keys() if n in existing and incoming[n] != existing[n]]
        removes = [n for n in existing.keys() if n not in incoming] if delete_missing else []

        lines = ["**Preview import**"]
        lines.append(f"- Adds: {len(adds)}")
        lines.append(f"- Edits: {len(edits)}")
        lines.append(f"- Removes: {len(removes)} (delete_missing={delete_missing})")
        if adds:
            lines.append("\n**Adds**: " + ", ".join(adds[:30]) + ("…" if len(adds)>30 else ""))
        if edits:
            lines.append("\n**Edits**: " + ", ".join(edits[:30]) + ("…" if len(edits)>30 else ""))
        if removes:
            lines.append("\n**Removes**: " + ", ".join(removes[:30]) + ("…" if len(removes)>30 else ""))
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @grp.command(name="import_csv", description="Import tanks from CSV (admins only)")
    @app_commands.describe(file="CSV file with columns: name,tier,type")
    async def import_csv(interaction: discord.Interaction, file: discord.Attachment):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        data = await file.read()
        text = data.decode("utf-8-sig", errors="replace")

        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            await interaction.followup.send("CSV has no header row.", ephemeral=True)
            return

        to_add: list[tuple[str, int, str]] = []
        added = 0
        skipped = 0
        errors: list[str] = []
        created_at = utils.utc_now_z()

        for i, row in enumerate(reader, start=2):
            name = (row.get("name") or "").strip()
            tier_raw = (row.get("tier") or "").strip()
            ttype = (row.get("type") or "").strip().lower()

            if not name or not tier_raw or not ttype:
                errors.append(f"Line {i}: missing name/tier/type")
                continue
            try:
                tier = int(tier_raw)
            except ValueError:
                errors.append(f"Line {i}: invalid tier '{tier_raw}'")
                continue

            try:
                name = utils.validate_text("Tank name", name, 64)
            except Exception as e:
                errors.append(f"Line {i}: invalid name '{name}': {e}")
                continue

            to_add.append((name, tier, ttype))

        if to_add:
            added, skipped = await db.add_tanks_bulk(
                to_add,
                interaction.user.display_name,
                created_at,
            )
            await _refresh_webpage()
            await audit_channel.send(
                interaction.client,
                (
                    "🧾 [tank import] "
                    f"actor={interaction.user.display_name} "
                    f"file={file.filename} "
                    f"parsed={len(to_add)} "
                    f"added={added} "
                    f"skipped={skipped} "
                    f"errors={len(errors)}"
                ),
            )

        msg = [
            f"Parsed: **{reader.line_num}** lines",
            f"Added: **{added}**",
            f"Skipped: **{skipped}**",
            f"Errors: **{len(errors)}**",]
        if errors:
            msg.append("")
            msg.append("First errors:")
            msg.extend([f"- {e}" for e in errors[:15]])
            if len(errors) > 15:
                msg.append(f"- ...and {len(errors) - 15} more")

        await interaction.followup.send("\n".join(msg), ephemeral=True)

    @grp.command(name="rebuild_index", description="Delete and fully recreate the forum index (admins only)")
    async def rebuild_index(interaction: discord.Interaction):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await forum_index.rebuild_all(interaction.client)
        await interaction.followup.send("✅ Index rebuilt from scratch (all previous index posts removed and recreated).", ephemeral=True)

    @grp.command(name="rebuild_index_missing", description="Create/repair missing forum index threads")
    async def rebuild_index_missing(interaction: discord.Interaction):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        await forum_index.rebuild_missing(bot)
        await interaction.followup.send("✅ Missing threads repaired.", ephemeral=True)
