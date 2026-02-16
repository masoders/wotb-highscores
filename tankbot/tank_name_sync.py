import datetime as dt
import logging
from zoneinfo import ZoneInfo

import aiohttp
import discord
from discord.ext import tasks

from . import audit_channel, config, db, utils

log = logging.getLogger(__name__)
_last_tank_sync_utc: str | None = None
_last_tank_sync_ok: bool | None = None
_last_tank_sync_msg: str | None = None
_last_scheduled_tank_sync_utc: str | None = None


def _cfg_bool(name: str, default: bool) -> bool:
    return bool(getattr(config, name, default))


def _cfg_str(name: str, default: str) -> str:
    return str(getattr(config, name, default))


def _cfg_int(name: str, default: int) -> int:
    try:
        return int(getattr(config, name, default))
    except Exception:
        return int(default)


def last_tank_sync_status() -> tuple[str | None, bool | None, str | None]:
    return _last_tank_sync_utc, _last_tank_sync_ok, _last_tank_sync_msg


def last_scheduled_tank_sync_utc() -> str | None:
    return _last_scheduled_tank_sync_utc


def next_tank_sync_run() -> dt.datetime | None:
    return getattr(monthly_tank_sync_loop, "next_run", None)


def _api_base_url(region: str) -> str:
    key = (region or "").strip().lower()
    mapping = {
        "eu": "https://api.wotblitz.eu",
        "na": "https://api.wotblitz.com",
        "com": "https://api.wotblitz.com",
        "asia": "https://api.wotblitz.asia",
    }
    if key not in mapping:
        raise ValueError(f"Unsupported WG region for tank sync: {region}")
    return mapping[key]


def _next_monthly_run(now_local: dt.datetime) -> dt.datetime:
    day = _cfg_int("WG_TANKS_SYNC_DAY", 1)
    target = now_local.replace(
        day=day,
        hour=_cfg_int("WG_TANKS_SYNC_HOUR", 4),
        minute=_cfg_int("WG_TANKS_SYNC_MINUTE", 10),
        second=0,
        microsecond=0,
    )
    if target <= now_local:
        if target.month == 12:
            target = target.replace(year=target.year + 1, month=1, day=day)
        else:
            target = target.replace(month=target.month + 1, day=day)
    return target


def _parse_utc_iso(raw: str | None) -> dt.datetime | None:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


async def sync_now(*, actor: str = "system") -> dict[str, object]:
    global _last_tank_sync_utc, _last_tank_sync_ok, _last_tank_sync_msg

    if not _cfg_bool("WG_TANKS_SYNC_ENABLED", True):
        raise RuntimeError("WG_TANKS_SYNC_ENABLED is false")
    app_id = _cfg_str("WG_TANKS_API_APPLICATION_ID", "c9daca4281064c19f93e714acd0a6967").strip()
    if not app_id:
        raise RuntimeError("WG_TANKS_API_APPLICATION_ID is not configured")

    region = _cfg_str("WG_TANKS_API_REGION", "eu").strip().lower() or "eu"
    try:
        base_url = _api_base_url(region)
        url = f"{base_url}/wotb/encyclopedia/vehicles/"
        params = {"application_id": app_id}
        timeout = aiohttp.ClientTimeout(total=_cfg_int("WG_TANKS_API_TIMEOUT_SECONDS", 20))

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as response:
                text = await response.text()
                if response.status != 200:
                    raise RuntimeError(f"WG API HTTP {response.status}: {text[:200]}")
                payload = await response.json(content_type=None)

        if payload.get("status") != "ok":
            error = payload.get("error", {})
            raise RuntimeError(
                "WG API error: "
                f"code={error.get('code')} message={error.get('message')}"
            )

        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("WG API returned invalid encyclopedia payload")

        fetched_rows: list[tuple[int, str, int | None, str | None, str | None, bool, bool]] = []
        for key, row in data.items():
            if not isinstance(row, dict):
                continue
            try:
                tank_id = int(row.get("tank_id") or key)
            except Exception:
                continue
            name = str(row.get("name") or "").strip()
            if not name:
                continue
            tier_raw = row.get("tier")
            try:
                tier = int(tier_raw) if tier_raw is not None else None
            except Exception:
                tier = None
            ttype = str(row.get("type") or "").strip().lower() or None
            nation = str(row.get("nation") or "").strip().lower() or None
            is_premium = bool(row.get("is_premium", False))
            is_collectible = bool(row.get("is_collectible", False))
            fetched_rows.append(
                (tank_id, name, tier, ttype, nation, is_premium, is_collectible)
            )
        if not fetched_rows:
            raise RuntimeError("WG API returned zero tank rows; refusing to replace cached catalog")

        synced_at = utils.utc_now_z()
        result = await db.replace_wg_tank_catalog(
            region=region,
            tanks=fetched_rows,
            synced_at=synced_at,
        )
        result["fetched_count"] = len(fetched_rows)
        result["actor"] = actor
        result["region"] = region

        _last_tank_sync_utc = synced_at
        _last_tank_sync_ok = True
        _last_tank_sync_msg = (
            f"fetched={result.get('fetched_count', 0)} "
            f"active={result.get('total_active', 0)} "
            f"added={result.get('added_count', 0)} "
            f"removed={result.get('removed_count', 0)} "
            f"renamed={result.get('renamed_count', 0)} "
            f"actor={actor}"
        )
        try:
            await db.set_sync_state(f"wg_tanks:last:{region}", _last_tank_sync_utc, _last_tank_sync_utc)
            await db.set_sync_state(f"wg_tanks:last_ok:{region}", "1", _last_tank_sync_utc)
            await db.set_sync_state(f"wg_tanks:last_msg:{region}", _last_tank_sync_msg, _last_tank_sync_utc)
        except Exception:
            log.exception("Failed to persist wg_tanks:last status")

        return result
    except Exception as exc:
        _last_tank_sync_utc = utils.utc_now_z()
        _last_tank_sync_ok = False
        _last_tank_sync_msg = f"{type(exc).__name__}: {exc}"
        try:
            await db.set_sync_state(f"wg_tanks:last:{region}", _last_tank_sync_utc, _last_tank_sync_utc)
            await db.set_sync_state(f"wg_tanks:last_ok:{region}", "0", _last_tank_sync_utc)
            await db.set_sync_state(f"wg_tanks:last_msg:{region}", _last_tank_sync_msg, _last_tank_sync_utc)
        except Exception:
            log.exception("Failed to persist wg_tanks:last failure status")
        raise


async def bootstrap_if_needed() -> None:
    if not _cfg_bool("WG_TANKS_SYNC_ENABLED", True):
        return

    region = _cfg_str("WG_TANKS_API_REGION", "eu").strip().lower() or "eu"
    active_count = await db.count_wg_tank_catalog(region=region, active_only=True)
    last_sync = await db.get_sync_state(f"wg_tanks:last:{region}")

    if active_count > 0 and last_sync:
        last_sync_dt = _parse_utc_iso(last_sync)
        if last_sync_dt is None:
            return
        age = dt.datetime.now(dt.timezone.utc) - last_sync_dt
        if age < dt.timedelta(days=27):
            return

    try:
        result = await sync_now(actor="startup")
        log.info(
            "WG tank encyclopedia startup sync done: region=%s active=%s added=%s removed=%s",
            result.get("region"),
            result.get("total_active"),
            result.get("added_count"),
            result.get("removed_count"),
        )
    except Exception:
        log.exception("WG tank encyclopedia startup sync failed")


@tasks.loop(hours=6)
async def monthly_tank_sync_loop(bot: discord.Client):
    global _last_scheduled_tank_sync_utc

    if not _cfg_bool("WG_TANKS_SYNC_ENABLED", True):
        return

    try:
        tz = ZoneInfo(_cfg_str("WG_TANKS_SYNC_TZ", "UTC"))
    except Exception:
        tz = ZoneInfo("UTC")

    now_local = dt.datetime.now(tz)
    if not hasattr(monthly_tank_sync_loop, "next_run"):
        monthly_tank_sync_loop.next_run = _next_monthly_run(now_local)

    if now_local < monthly_tank_sync_loop.next_run:
        return

    monthly_tank_sync_loop.next_run = _next_monthly_run(now_local + dt.timedelta(seconds=1))
    _last_scheduled_tank_sync_utc = utils.utc_now_z()
    region = _cfg_str("WG_TANKS_API_REGION", "eu").strip().lower() or "eu"
    try:
        await db.set_sync_state(
            f"wg_tanks:last_scheduled:{region}",
            _last_scheduled_tank_sync_utc,
            _last_scheduled_tank_sync_utc,
        )
    except Exception:
        log.exception("Failed to persist wg_tanks:last_scheduled")

    try:
        result = await sync_now(actor="auto")
        log.info(
            "WG tank encyclopedia monthly sync done: region=%s active=%s added=%s removed=%s",
            result.get("region"),
            result.get("total_active"),
            result.get("added_count"),
            result.get("removed_count"),
        )
        try:
            await audit_channel.send(
                bot,
                "system|scheduled_tank_name_sync|status=ok|"
                f"region={region}|active={result.get('total_active', 0)}|"
                f"added={result.get('added_count', 0)}|"
                f"removed={result.get('removed_count', 0)}",
            )
        except Exception:
            pass
    except Exception:
        log.exception("WG tank encyclopedia monthly sync failed")
        try:
            await audit_channel.send(
                bot,
                "system|scheduled_tank_name_sync|status=fail|"
                f"region={region}|error={_last_tank_sync_msg or 'unknown'}",
            )
        except Exception:
            pass
