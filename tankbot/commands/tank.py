import io
import csv
import discord
from discord import app_commands

from .. import db, forum_index, utils


class Tank(app_commands.Group):
    def __init__(self):
        super().__init__(name="tank", description="Tank roster commands")

def _require_admin(interaction: discord.Interaction) -> bool:
    m = interaction.user
    return isinstance(m, discord.Member) and utils.can_manage(m)

def _require_commander(interaction: discord.Interaction) -> bool:
    m = interaction.user
    return isinstance(m, discord.Member) and utils.has_commander_role(m)

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
        await interaction.response.send_message(f"✅ Added **{name}** (Tier {tier}, {utils.title_case_type(type)}).", ephemeral=True)

    @grp.command(name="edit", description="Edit a tank (commanders only)")
    async def edit(interaction: discord.Interaction, name: str, tier: int, type: str):
        if not _require_commander(interaction):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can edit tanks.", ephemeral=True)
            return
        name = utils.validate_text('Tank name', name, 64)
        type = type.strip().lower()
        t = await db.get_tank(name)
        if not t:
            await interaction.response.send_message("Tank not found.", ephemeral=True)
            return
        old_tier, old_type = int(t[1]), t[2]
        if not (1 <= tier <= 10):
            await interaction.response.send_message("Tier must be 1..10.", ephemeral=True)
            return
        if type not in ("light", "medium", "heavy", "td"):
            await interaction.response.send_message("Type must be one of: light, medium, heavy, td.", ephemeral=True)
            return

        await db.edit_tank(name, tier, type, interaction.user.display_name, utils.utc_now_z())
        # Update both old and new buckets
        await forum_index.targeted_update(bot, old_tier, old_type)
        await forum_index.targeted_update(bot, tier, type)
        await interaction.response.send_message(f"✅ Updated **{name}**.", ephemeral=True)

    @grp.command(name="remove", description="Remove a tank (commanders only, only if no submissions)")
    async def remove(interaction: discord.Interaction, name: str):
        if not _require_commander(interaction):
            await interaction.response.send_message("Nope. Only **Clan Commanders** can remove tanks.", ephemeral=True)
            return
        name = utils.validate_text('Tank name', name, 64)
        t = await db.get_tank(name)
        if not t:
            await interaction.response.send_message("Tank not found.", ephemeral=True)
            return
        tier, ttype = int(t[1]), t[2]
        try:
            await db.remove_tank(name, interaction.user.display_name, utils.utc_now_z())
        except Exception as e:
            await interaction.response.send_message(f"❌ {type(e).__name__}: {e}", ephemeral=True)
            return
        await forum_index.targeted_update(bot, tier, ttype)
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

        added = 0
        skipped = 0
        errors: list[str] = []

        for i, row in enumerate(reader, start=2):
            name = (row.get("name") or "").strip()
            tier_raw = (row.get("tier") or "").strip()
            ttype = (row.get("type") or "").strip()

            if not name or not tier_raw or not ttype:
                errors.append(f"Line {i}: missing name/tier/type")
                continue
            try:
                tier = int(tier_raw)
            except ValueError:
                errors.append(f"Line {i}: invalid tier '{tier_raw}'")
                continue

            try:
                await db.add_tank(name, tier, ttype, interaction.user.display_name, utils.utc_now_z())
                added += 1
            except Exception as e:
            # Most common: unique constraint (already exists). Treat as skip.
                skipped += 1

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

    @grp.command(name="rebuild_index", description="Rebuild the forum index (admins only)")
    async def rebuild_index(interaction: discord.Interaction):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await forum_index.rebuild_all(interaction.client)
        await interaction.followup.send("✅ Index rebuilt.", ephemeral=True)

    @grp.command(name="rebuild_index_missing", description="Create/repair missing forum index threads")
    async def rebuild_index_missing(interaction: discord.Interaction):
        if not _require_admin(interaction):
            await interaction.response.send_message("Nope. You need **Manage Server**.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        await forum_index.rebuild_missing(bot)
        await interaction.followup.send("✅ Missing threads repaired.", ephemeral=True)
