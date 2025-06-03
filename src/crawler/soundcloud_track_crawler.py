import asyncio
import random
import sys
import time
import traceback

import aiohttp
from aiohttp import ClientError

from src.util.config import PROXY_TUNNEL, PROXY_USER_NAME, PROXY_PWD, SOUNDCLOUD_CLIENT_ID, CLASH_URL
from src.util.db import close_connections, clickhouse_client, redis_client
from src.util.logger import logger
from src.util.transform_fields import transform_track_to_ck, TRACK_COLS

CLICKHOUSE_TABLE = "tracks"
REDIS_KEY_IDENTIFIER = "lionel_0M"
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
PROXY_AUTH = aiohttp.BasicAuth(PROXY_USER_NAME, PROXY_PWD)

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
        rows = [transform_track_to_ck(track) for track in tracks if track]
        try:
            ch_client.insert(CLICKHOUSE_TABLE, rows, column_names=TRACK_COLS)
        except Exception as e:
            logger.error(f"ClickHouse batch insert error: {traceback.format_exc()}")
            # 逐行插入, 定位异常行
            for idx, row in enumerate(rows):
                try:
                    ch_client.insert(CLICKHOUSE_TABLE, [row], column_names=TRACK_COLS)
                except Exception as ee:
                    logger.error(f"ClickHouse single insert error at row {idx}: {row}\n{traceback.format_exc()}")


# --- REDIS OFFSET ---
def get_next_batch_offset():
    try:
        val = redis_client.get(REDIS_KEY)
        return int(val or 0)
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
    last_exception = None
    for attempt in range(max_attempts):
        try:
            headers = HEADERS.copy()
            async with session.get(url, headers=headers, proxy=CLASH_URL) as resp:
                if resp.status == 200:
                    return await resp.json()
                logger.warning(f"User {user_id}: HTTP {resp.status} for {url}")
        except (ClientError, asyncio.TimeoutError) as e:
            last_exception = e
            logger.warning(
                f"User {user_id}: Attempt {attempt + 1}/{max_attempts} - {e} on {url}"
            )
        await asyncio.sleep(random.uniform(1, 10))

    logger.error(f"User {user_id}: Failed after {max_attempts} attempts for {url}")
    if last_exception:
        raise last_exception
    raise Exception(f"User {user_id}: Unspecified download failure for {url}")


async def fetch_and_store_tracks_for_user(session, user_id):
    url = (f"https://api-v2.soundcloud.com/users/{user_id}/tracks"
           f"?client_id={SOUNDCLOUD_CLIENT_ID}&limit={TRACKS_LIMIT_PER_REQUEST}")
    inserts = []
    while url:
        try:
            data = await fetch_json_with_retry(session, url, user_id)
        except Exception as e:
            logger.error(f"User {user_id}: Skipping due to repeated errors: {e}")
            return False
        tracks = data.get("collection", [])
        next_href = data.get("next_href")
        if next_href:
            if 'client_id=' not in next_href:
                next_href += f'&client_id={SOUNDCLOUD_CLIENT_ID}'
        url = next_href
        inserts.extend(tracks)

    store_tracks(inserts)
    return True


async def crawl_batch():
    offset = get_next_batch_offset()
    # 1000000 - 2000000 is my limit
    while offset <= 3000000:
        logger.info(f"Crawling offset ({offset}) tracks, size ({BATCH_SIZE})")
        user_ids = fetch_user_ids(offset, BATCH_SIZE)
        if not user_ids:
            logger.error("No user IDs fetched from ClickHouse. Exiting.")
            return
        begin_time = time.time()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600)) as session:
            sem = asyncio.Semaphore(CONCURRENT_USERS)
            async def sem_task(user_id):
                async with sem:
                    return await fetch_and_store_tracks_for_user(session, user_id)
            tasks = [sem_task(uid) for uid in user_ids]
            task_results = await asyncio.gather(*tasks)
            num_failures = sum(not res for res in task_results)
            failure_rate = num_failures / len(tasks)
            logger.info(
                f"Batch finished: {num_failures} failed out of {len(tasks)} tracks (failure rate: {failure_rate:.2%}) in {int(time.time() - begin_time)}s")
            if failure_rate > 0.10:
                logger.error(f"Failure rate {failure_rate:.2%} exceeds 10%, exiting immediately.")
                close_connections()
                sys.exit(1)
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
