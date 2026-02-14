import asyncio
import datetime as dt
import logging
from zoneinfo import ZoneInfo

import aiohttp
import discord
from discord.ext import tasks

from . import config, db, utils

log = logging.getLogger(__name__)
_last_wg_sync_utc: str | None = None
_last_wg_sync_ok: bool | None = None
_last_wg_sync_msg: str | None = None
_last_scheduled_wg_sync_utc: str | None = None


def last_wg_sync_status() -> tuple[str | None, bool | None, str | None]:
    return _last_wg_sync_utc, _last_wg_sync_ok, _last_wg_sync_msg


def last_scheduled_wg_sync_utc() -> str | None:
    return _last_scheduled_wg_sync_utc


def next_wg_sync_run() -> dt.datetime | None:
    return getattr(daily_clan_sync_loop, "next_run", None)


def _api_base_url(game: str, region: str) -> str:
    key = (region or "").strip().lower()
    game_key = (game or "").strip().lower()
    if game_key == "wot":
        mapping = {
            "eu": "https://api.worldoftanks.eu",
            "na": "https://api.worldoftanks.com",
            "com": "https://api.worldoftanks.com",
            "asia": "https://api.worldoftanks.asia",
        }
    elif game_key == "wotb":
        mapping = {
            "eu": "https://api.wotblitz.eu",
            "na": "https://api.wotblitz.com",
            "com": "https://api.wotblitz.com",
            "asia": "https://api.wotblitz.asia",
        }
    else:
        raise ValueError(f"Unsupported WG game: {game}")
    if key not in mapping:
        raise ValueError(f"Unsupported WG region: {region}")
    return mapping[key]


def _next_daily_run(now_local: dt.datetime) -> dt.datetime:
    target = now_local.replace(
        hour=config.WG_REFRESH_HOUR,
        minute=config.WG_REFRESH_MINUTE,
        second=0,
        microsecond=0,
    )
    if target <= now_local:
        target += dt.timedelta(days=1)
    return target


async def _fetch_clan_members(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    game: str,
    app_id: str,
    clan_id: int,
) -> dict[str, object]:
    game_path = "wotb" if game == "wotb" else "wot"
    url = f"{base_url}/{game_path}/clans/info/"
    params = {
        "application_id": app_id,
        "clan_id": str(int(clan_id)),
        "extra": "members",
        "fields": "members.account_id,members.account_name",
    }

    async with session.get(url, params=params) as response:
        text = await response.text()
        if response.status != 200:
            raise RuntimeError(f"WG API HTTP {response.status} for clan {clan_id}: {text[:200]}")

        payload = await response.json(content_type=None)

    if payload.get("status") != "ok":
        raise RuntimeError(
            f"WG API error for clan {clan_id}: code={payload.get('error', {}).get('code')} "
            f"message={payload.get('error', {}).get('message')}"
        )

    clan_data = (payload.get("data") or {}).get(str(int(clan_id))) or {}
    members = clan_data.get("members") or []
    if isinstance(members, dict):
        members = list(members.values())

    out: list[tuple[int, int, str]] = []
    for m in members:
        try:
            account_id = int(m.get("account_id"))
            account_name = utils.validate_text("Player", str(m.get("account_name") or ""), 64)
        except Exception:
            continue
        out.append((account_id, int(clan_id), account_name))

    if out:
        return {
            "clan_id": int(clan_id),
            "members": out,
            "source": "members",
            "count": len(out),
        }

    # Blitz APIs can return members_ids without full member objects.
    member_ids = clan_data.get("members_ids") or []
    ids: list[int] = []
    for raw_id in member_ids:
        try:
            ids.append(int(raw_id))
        except Exception:
            continue
    ids = sorted(set(ids))
    if not ids:
        return {
            "clan_id": int(clan_id),
            "members": [],
            "source": "none",
            "count": 0,
        }

    account_url = f"{base_url}/{game_path}/account/info/"
    names_by_id: dict[int, str] = {}
    chunk_size = 100
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        params = {
            "application_id": app_id,
            "account_id": ",".join([str(v) for v in chunk]),
            "fields": "nickname",
        }
        async with session.get(account_url, params=params) as response:
            text = await response.text()
            if response.status != 200:
                raise RuntimeError(f"WG API HTTP {response.status} for account/info clan {clan_id}: {text[:200]}")
            account_payload = await response.json(content_type=None)
        if account_payload.get("status") != "ok":
            raise RuntimeError(
                f"WG API account/info error for clan {clan_id}: "
                f"code={account_payload.get('error', {}).get('code')} "
                f"message={account_payload.get('error', {}).get('message')}"
            )
        data = account_payload.get("data") or {}
        for sid, row in data.items():
            try:
                account_id = int(sid)
                nickname = utils.validate_text("Player", str((row or {}).get("nickname") or ""), 64)
            except Exception:
                continue
            names_by_id[account_id] = nickname

    out_fallback = [(account_id, int(clan_id), names_by_id[account_id]) for account_id in ids if account_id in names_by_id]
    return {
        "clan_id": int(clan_id),
        "members": out_fallback,
        "source": "members_ids+account_info",
        "count": len(out_fallback),
    }


async def sync_now(*, actor: str = "system") -> dict[str, object]:
    global _last_wg_sync_utc, _last_wg_sync_ok, _last_wg_sync_msg

    if not config.WG_API_APPLICATION_ID:
        raise RuntimeError("WG_API_APPLICATION_ID is not configured")
    if not config.WG_CLAN_IDS:
        raise RuntimeError("WG_CLAN_IDS is not configured")

    try:
        region = config.WG_API_REGION
        game = config.WG_API_GAME
        base_url = _api_base_url(game, region)

        timeout = aiohttp.ClientTimeout(total=config.WG_API_TIMEOUT_SECONDS)
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks_ = [
                _fetch_clan_members(
                    session,
                    base_url=base_url,
                    game=game,
                    app_id=config.WG_API_APPLICATION_ID,
                    clan_id=clan_id,
                )
                for clan_id in config.WG_CLAN_IDS
            ]
            clan_payloads = await asyncio.gather(*tasks_)

        # account_id is unique for a region account; keep one row per account.
        deduped: dict[int, tuple[int, int, str]] = {}
        per_clan: list[dict[str, object]] = []
        for payload in clan_payloads:
            cid = int(payload.get("clan_id", 0))
            members = payload.get("members", [])
            source = str(payload.get("source", "unknown"))
            count = int(payload.get("count", 0))
            per_clan.append({"clan_id": cid, "count": count, "source": source})
            for account_id, clan_id, account_name in members:
                deduped[int(account_id)] = (int(account_id), int(clan_id), str(account_name))

        members = list(deduped.values())
        synced_at = utils.utc_now_z()
        result = await db.replace_clan_players(
            region=region,
            members=[(account_id, clan_id, name) for account_id, clan_id, name in members],
            synced_at=synced_at,
        )
        result["actor"] = actor
        result["region"] = region
        result["clan_ids"] = list(config.WG_CLAN_IDS)
        result["per_clan"] = per_clan
        _last_wg_sync_utc, _last_wg_sync_ok, _last_wg_sync_msg = (
            synced_at,
            True,
            f"total={result.get('total', 0)} added={result.get('added_count', 0)} removed={result.get('removed_count', 0)} actor={actor}",
        )
        try:
            await db.set_sync_state(f"wg:last:{region}", _last_wg_sync_utc, _last_wg_sync_utc)
            await db.set_sync_state(f"wg:last_ok:{region}", "1", _last_wg_sync_utc)
            await db.set_sync_state(f"wg:last_msg:{region}", _last_wg_sync_msg, _last_wg_sync_utc)
        except Exception:
            log.exception("Failed to persist wg:last status")
        return result
    except Exception as exc:
        _last_wg_sync_utc, _last_wg_sync_ok, _last_wg_sync_msg = (
            utils.utc_now_z(),
            False,
            f"{type(exc).__name__}: {exc}",
        )
        try:
            region = config.WG_API_REGION
            await db.set_sync_state(f"wg:last:{region}", _last_wg_sync_utc, _last_wg_sync_utc)
            await db.set_sync_state(f"wg:last_ok:{region}", "0", _last_wg_sync_utc)
            await db.set_sync_state(f"wg:last_msg:{region}", _last_wg_sync_msg, _last_wg_sync_utc)
        except Exception:
            log.exception("Failed to persist wg:last failure status")
        raise


async def bootstrap_if_needed() -> None:
    if not config.WG_SYNC_ENABLED:
        return
    last_sync = await db.clan_players_last_sync(config.WG_API_REGION)
    if last_sync:
        return
    try:
        result = await sync_now(actor="startup")
        log.info(
            "WG startup sync done: region=%s total=%s added=%s removed=%s",
            result.get("region"),
            result.get("total"),
            result.get("added_count"),
            result.get("removed_count"),
        )
    except Exception:
        log.exception("WG startup sync failed")


@tasks.loop(minutes=5)
async def daily_clan_sync_loop(bot: discord.Client):
    global _last_scheduled_wg_sync_utc

    del bot  # currently unused
    if not config.WG_SYNC_ENABLED:
        return

    try:
        tz = ZoneInfo(config.WG_REFRESH_TZ)
    except Exception:
        tz = ZoneInfo("UTC")

    now_local = dt.datetime.now(tz)
    if not hasattr(daily_clan_sync_loop, "next_run"):
        daily_clan_sync_loop.next_run = _next_daily_run(now_local)

    if now_local < daily_clan_sync_loop.next_run:
        return

    daily_clan_sync_loop.next_run = _next_daily_run(now_local + dt.timedelta(seconds=1))
    _last_scheduled_wg_sync_utc = utils.utc_now_z()
    try:
        await db.set_sync_state(
            f"wg:last_scheduled:{config.WG_API_REGION}",
            _last_scheduled_wg_sync_utc,
            _last_scheduled_wg_sync_utc,
        )
    except Exception:
        log.exception("Failed to persist wg:last_scheduled")

    try:
        result = await sync_now(actor="auto")
        log.info(
            "WG daily sync done: region=%s total=%s added=%s removed=%s",
            result.get("region"),
            result.get("total"),
            result.get("added_count"),
            result.get("removed_count"),
        )
    except Exception:
        log.exception("WG daily sync failed")
