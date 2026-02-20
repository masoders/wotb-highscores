import asyncio
import logging

import aiohttp

from . import config, db, utils

log = logging.getLogger(__name__)

TANKOPEDIA_SCHEMA_VERSION = "1"
_RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
_VEHICLE_FIELDS = ",".join(
    [
        "tank_id",
        "name",
        "tier",
        "type",
        "nation",
        "is_premium",
        "is_collectible",
        "images",
        "default_profile",
        "modules_tree",
        "next_tanks",
        "description",
        "short_name",
    ]
)


class _RetryableRequestError(RuntimeError):
    pass


def _api_base_url(region: str) -> str:
    key = str(region or "").strip().lower()
    mapping = {
        "eu": "https://api.wotblitz.eu",
        "na": "https://api.wotblitz.com",
        "com": "https://api.wotblitz.com",
        "asia": "https://api.wotblitz.asia",
    }
    if key not in mapping:
        raise ValueError(f"Unsupported WG region: {region}")
    return mapping[key]


async def _request_json_with_retries(
    session: aiohttp.ClientSession,
    *,
    url: str,
    params: dict[str, str],
    max_retries: int = 3,
) -> dict[str, object]:
    delay_seconds = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, params=params) as response:
                body_text = await response.text()
                if response.status != 200:
                    message = f"WG API HTTP {response.status}: {body_text[:200]}"
                    if response.status in _RETRYABLE_HTTP_CODES:
                        raise _RetryableRequestError(message)
                    raise RuntimeError(message)
                payload = await response.json(content_type=None)
            if payload.get("status") != "ok":
                error = payload.get("error", {})
                raise RuntimeError(
                    f"WG API error: code={error.get('code')} message={error.get('message')}"
                )
            return payload
        except (aiohttp.ClientError, asyncio.TimeoutError, _RetryableRequestError) as exc:
            if attempt >= max_retries:
                raise RuntimeError(f"WG request failed after {max_retries} attempts: {exc}") from exc
            await asyncio.sleep(delay_seconds)
            delay_seconds *= 2.0

    raise RuntimeError("WG request retry loop ended unexpectedly")


async def _fetch_tanks_updated_at(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    app_id: str,
    language: str,
) -> str:
    payload = await _request_json_with_retries(
        session,
        url=f"{base_url}/wotb/encyclopedia/info/",
        params={
            "application_id": app_id,
            "language": language,
            "fields": "tanks_updated_at",
        },
    )
    data = payload.get("data")
    if not isinstance(data, dict) or "tanks_updated_at" not in data:
        raise RuntimeError("WG API /encyclopedia/info missing tanks_updated_at")
    return str(data.get("tanks_updated_at"))


async def _fetch_vehicles(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    app_id: str,
    language: str,
) -> list[dict[str, object]]:
    vehicles_url = f"{base_url}/wotb/encyclopedia/vehicles/"
    try:
        payload = await _request_json_with_retries(
            session,
            url=vehicles_url,
            params={
                "application_id": app_id,
                "language": language,
                "fields": _VEHICLE_FIELDS,
            },
        )
    except RuntimeError as exc:
        # WG can reject certain field lists with INVALID_FIELDS depending on API changes.
        # Fall back to full payload to preserve compatibility and keep all characteristics.
        if "INVALID_FIELDS" not in str(exc).upper():
            raise
        payload = await _request_json_with_retries(
            session,
            url=vehicles_url,
            params={
                "application_id": app_id,
                "language": language,
            },
        )
    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("WG API /encyclopedia/vehicles returned invalid data")

    vehicles: list[dict[str, object]] = []
    for key, row in data.items():
        if not isinstance(row, dict):
            continue
        item = dict(row)
        if item.get("tank_id") is None:
            try:
                item["tank_id"] = int(key)
            except Exception:
                continue
        vehicles.append(item)
    if not vehicles:
        raise RuntimeError("WG API /encyclopedia/vehicles returned zero vehicles")
    return vehicles


async def sync_now(*, force: bool = False, actor: str = "cli") -> dict[str, object]:
    if not config.WG_TANKOPEDIA_SYNC_ENABLED:
        raise RuntimeError("WG_TANKOPEDIA_SYNC_ENABLED is false")
    if not config.WG_API_APPLICATION_ID:
        raise RuntimeError("WG_API_APPLICATION_ID is not configured")

    region = config.WG_TANKOPEDIA_REGION
    language = config.WG_TANKOPEDIA_LANGUAGE
    base_url = _api_base_url(region)
    timeout_seconds = max(10, min(20, int(config.WG_API_TIMEOUT_SECONDS)))
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    connector = aiohttp.TCPConnector(limit=10)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        remote_tanks_updated_at = await _fetch_tanks_updated_at(
            session,
            base_url=base_url,
            app_id=config.WG_API_APPLICATION_ID,
            language=language,
        )

        local_tanks_updated_at = await db.get_tankopedia_meta("tanks_updated_at")
        local_count = await db.count_tankopedia_tanks()
        if (
            not force
            and local_tanks_updated_at
            and str(local_tanks_updated_at) == str(remote_tanks_updated_at)
            and local_count > 0
        ):
            return {
                "changed": False,
                "message": "unchanged; skipped",
                "region": region,
                "language": language,
                "tanks_updated_at": remote_tanks_updated_at,
                "count": local_count,
                "actor": actor,
            }

        vehicles = await _fetch_vehicles(
            session,
            base_url=base_url,
            app_id=config.WG_API_APPLICATION_ID,
            language=language,
        )

    synced_at = utils.utc_now_z()
    db_result = await db.replace_tankopedia_snapshot(
        tanks=vehicles,
        tanks_updated_at=remote_tanks_updated_at,
        region=region,
        language=language,
        synced_at=synced_at,
        schema_version=TANKOPEDIA_SCHEMA_VERSION,
    )
    result = {
        "changed": True,
        "message": "updated",
        "region": region,
        "language": language,
        "tanks_updated_at": remote_tanks_updated_at,
        "last_sync_utc": synced_at,
        "actor": actor,
    }
    result.update(db_result)
    log.info(
        "Tankopedia sync done: region=%s lang=%s total=%s added=%s removed=%s updated=%s actor=%s",
        result.get("region"),
        result.get("language"),
        result.get("total_tanks"),
        result.get("added_count"),
        result.get("removed_count"),
        result.get("updated_count"),
        actor,
    )
    return result
