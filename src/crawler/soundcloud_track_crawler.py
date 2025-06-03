import asyncio
import traceback

import aiohttp
from aiohttp import ClientError

from src.crawler.soundcloud_follower import clickhouse_client, redis_client
from src.util.config import PROXY_TUNNEL, PROXY_USER_NAME, PROXY_PWD, SOUNDCLOUD_CLIENT_ID, PROXY_URL
from src.util.db import close_connections
from src.util.logger import logger
from src.util.transform_fields import parse_datetime, safe_int, safe_bool, safe_release_date, safe_str, \
    safe_nullable_string, transform_track_to_ck

CLICKHOUSE_TABLE = "tracks"
REDIS_KEY_IDENTIFIER = "lionel_2M"
REDIS_KEY = f"soundcloud:track:{REDIS_KEY_IDENTIFIER}:offset"

BATCH_SIZE = 1000
CONCURRENT_USERS = 8
TRACKS_LIMIT_PER_REQUEST = 100
RETRY_LIMIT = 10
RETRY_BACKOFF = 1.2

# 隧道域名:端口号
PROXY_TUNNEL = PROXY_TUNNEL
PROXY_USER_NAME = PROXY_USER_NAME
PROXY_PWD = PROXY_PWD

# --- SCHEMA INFO ---
TRACK_COLS = [
    "id","artwork_url","caption","commentable","comment_count","created_at","description",
    "downloadable","download_count","duration","full_duration","embeddable_by","genre",
    "has_downloads_left","kind","label_name","last_modified","license","likes_count",
    "permalink","permalink_url","playback_count","public","purchase_title","purchase_url",
    "release_date","reposts_count","secret_token","sharing","state","streamable","tag_list",
    "title","uri","urn","user_id","visuals","waveform_url","display_date","station_urn",
    "station_permalink","track_authorization","monetization_model","policy",
    "publisher_metadata_id","publisher_metadata_urn","publisher_metadata_artist",
    "publisher_metadata_album_title","publisher_metadata_contains_music",
    "publisher_metadata_upc_or_ean","publisher_metadata_isrc","publisher_metadata_explicit",
    "publisher_metadata_p_line","publisher_metadata_p_line_for_display",
    "publisher_metadata_c_line","publisher_metadata_c_line_for_display",
    "publisher_metadata_release_title"
]


HEADERS = {
    'Host': 'api-v2.soundcloud.com',
    'Origin': 'https://soundcloud.com',
    'Referer': 'https://soundcloud.com',
    'Sec-Fetch-Site': 'same-site',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': 'https://soundcloud.com',
}

# --- CLIENTS ---
ch_client = clickhouse_client
redis_client = redis_client

# --- STORAGE ---
def store_tracks(tracks):
    if tracks:
        rows = [transform_track_to_ck(track) for track in tracks]
        try:
            ch_client.insert(CLICKHOUSE_TABLE, rows, column_names=TRACK_COLS)
            logger.info(f"Inserted {len(rows)} tracks to ClickHouse")
        except Exception as e:
            logger.error(f"ClickHouse insert error: {traceback.format_exc()}")

# --- REDIS OFFSET ---
def get_next_batch_offset():
    try:
        val = redis_client.get(REDIS_KEY)
        return int(val or 2000000)
    except Exception as e:
        logger.error(f"Redis get offset error: {e}")
        return 0

def set_next_batch_offset(offset):
    try:
        redis_client.set(REDIS_KEY, str(offset))
    except Exception as e:
        logger.error(f"Redis set offset error: {e}")

def fetch_user_ids(offset, limit):
    try:
        query = f"SELECT id FROM users LIMIT {limit} OFFSET {offset}"
        return [row[0] for row in ch_client.query(query).result_rows]
    except Exception as e:
        logger.error(f"ClickHouse fetch_user_ids error: {e}")
        return []

async def fetch_json_with_retry(session, url, user_id, max_attempts=RETRY_LIMIT):
    delay = RETRY_BACKOFF
    last_exception = None
    for attempt in range(max_attempts):
        try:
            headers = HEADERS.copy()
            async with session.get(url, headers=headers, proxy=PROXY_URL) as resp:
                if resp.status == 200:
                    return await resp.json()
                text = await resp.text()
                logger.warning(f"User {user_id}: HTTP {resp.status} for {url} - {text}")
        except (ClientError, asyncio.TimeoutError) as e:
            last_exception = e
            logger.warning(
                f"User {user_id}: Attempt {attempt+1}/{max_attempts} - {e} on {url} - traceback: {traceback.format_exc()}"
            )
        delay = delay * 2
        await asyncio.sleep(delay)
        delay *= 2

    logger.error(f"User {user_id}: Failed after {max_attempts} attempts for {url}")
    if last_exception:
        raise last_exception
    raise Exception(f"User {user_id}: Unspecified download failure for {url}")

async def fetch_and_store_tracks_for_user(session, user_id):
    url = (f"https://api-v2.soundcloud.com/users/{user_id}/tracks"
           f"?client_id={SOUNDCLOUD_CLIENT_ID}&limit={TRACKS_LIMIT_PER_REQUEST}")
    while url:
        try:
            data = await fetch_json_with_retry(session, url, user_id)
        except Exception as e:
            logger.error(f"User {user_id}: Skipping due to repeated errors: {e}")
            break
        tracks = data.get("collection", [])
        store_tracks(tracks)
        next_href = data.get("next_href")
        if next_href:
            if 'client_id=' not in next_href:
                sep = '&' if '?' in next_href else '?'
                next_href += f'{sep}client_id={SOUNDCLOUD_CLIENT_ID}'
            if 'limit=' not in next_href:
                sep = '&' if '?' in next_href else '?'
                next_href += f'{sep}limit={TRACKS_LIMIT_PER_REQUEST}'
            url = next_href
            await asyncio.sleep(0.2)
        else:
            break


async def crawl_batch():
    offset = get_next_batch_offset()
    # 1000000 - 2000000 is my limit
    while offset <= 3000000:
        logger.info(f"Crawling offset ({offset}) tracks, size ({BATCH_SIZE})")
        user_ids = fetch_user_ids(offset, BATCH_SIZE)
        if not user_ids:
            logger.error("No user IDs fetched from ClickHouse. Exiting.")
            return
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600)) as session:
            sem = asyncio.Semaphore(CONCURRENT_USERS)
            async def sem_task(user_id):
                async with sem:
                    await fetch_and_store_tracks_for_user(session, user_id)
            tasks = [sem_task(uid) for uid in user_ids]
            await asyncio.gather(*tasks)
        set_next_batch_offset(offset + BATCH_SIZE)
        logger.info(f"Batch complete: users {offset} - {offset + BATCH_SIZE - 1}")
        offset += BATCH_SIZE

if __name__ == "__main__":
    try:
        asyncio.run(crawl_batch())
    except Exception as e:
        close_connections()
    except KeyboardInterrupt:
        close_connections()
