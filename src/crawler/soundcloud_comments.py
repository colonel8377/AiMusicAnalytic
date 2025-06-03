import asyncio
import random
import traceback

import aiohttp
from aiohttp import ClientError

from src.util.config import SOUNDCLOUD_CLIENT_ID, PROXY_URL, PROXY_PWD, PROXY_TUNNEL, PROXY_USER_NAME
from src.util.db import close_connections, clickhouse_client, redis_client
from src.util.logger import logger
from src.util.transform_fields import transform_comment_to_ck, COMMENT_COLS

COMMENTS_CK_TABLE = "soundcloud_comments"
TRACKS_CK_TABLE = "tracks"

REDIS_OFFSET_DEFAULT_VAL = 0
REDIS_KEY_IDENTIFIER = f"lionel_{REDIS_OFFSET_DEFAULT_VAL}M"
QUERY_STOP_OFFSET = 1000000
REDIS_OFFSET_KEY = f"soundcloud:comments:{REDIS_KEY_IDENTIFIER}:track_query_offset"
REDIS_QUERY_KEY = "soundcloud:comments:last_ck_query"
BATCH_SIZE = 1000
CONCURRENT_COMMENTS = 256

PROXY_AUTH = aiohttp.BasicAuth(PROXY_USER_NAME, PROXY_PWD)

def store_comments(comments):
    if comments:
        rows = [transform_comment_to_ck(c) for c in comments]
        try:
            clickhouse_client.insert(COMMENTS_CK_TABLE, rows, column_names=COMMENT_COLS)
            logger.info(f"Inserted {len(rows)} comments to ClickHouse")
        except Exception:
            logger.error(f"ClickHouse insert error: {traceback.format_exc()}")

def get_next_track_offset():
    try:
        val = redis_client.get(REDIS_OFFSET_KEY)
        return int(val or REDIS_OFFSET_DEFAULT_VAL)
    except Exception as e:
        logger.error(f"Redis get offset error: {e}")
        return 0

def set_next_track_offset(offset):
    try:
        redis_client.set(REDIS_OFFSET_KEY, str(offset))
    except Exception as e:
        logger.error(f"Redis set offset error: {e}")

def set_last_ck_query(query):
    try:
        redis_client.set(REDIS_QUERY_KEY, query)
    except Exception as e:
        logger.error(f"Redis set last CK query error: {e}")

def fetch_track_ids(offset, limit):
    query = f"SELECT id FROM {TRACKS_CK_TABLE} ORDER BY id ASC LIMIT {limit} OFFSET {offset}"
    set_last_ck_query(query)
    try:
        return [row[0] for row in clickhouse_client.query(query).result_rows]
    except Exception as e:
        logger.error(f"ClickHouse fetch_track_ids error: {e}")
        return []

async def fetch_json_with_retry(session, url, track_id, max_attempts=10):
    delay = 1
    last_exception = None
    for attempt in range(max_attempts):
        try:
            async with session.get(url, proxy=PROXY_TUNNEL, proxy_auth=PROXY_AUTH) as resp:
                if resp.status == 200:
                    return await resp.json()
                text = await resp.text()
                logger.warning(f"Track {track_id}: HTTP {resp.status} for {url} - {text}")
        except (ClientError, asyncio.TimeoutError) as e:
            last_exception = e
            logger.warning(
                f"Track {track_id}: Attempt {attempt+1}/{max_attempts} - {e} on {url} - traceback: {traceback.format_exc()}"
            )
        await asyncio.sleep(delay)
        delay = min(delay * random.uniform(0.68, 2), 15)
    logger.error(f"Track {track_id}: Failed after {max_attempts} attempts for {url}")
    if last_exception:
        raise last_exception
    raise Exception(f"Track {track_id}: Unspecified download failure for {url}")

async def fetch_and_store_comments_for_track(session, track_id):
    url = (
        f"https://api-v2.soundcloud.com/tracks/{track_id}/comments"
        f"?sort=newest&threaded=1&client_id={SOUNDCLOUD_CLIENT_ID}&offset=0&limit=100"
    )
    comments = []
    while url:
        try:
            data = await fetch_json_with_retry(session, url, track_id)
        except Exception as e:
            logger.error(f"Track {track_id}: Skipping due to repeated errors: {e}")
            break
        collection = data.get("collection", [])
        comments.extend(collection)
        url = data.get("next_href")
    store_comments(comments)

async def crawl_comments_batch():
    offset = get_next_track_offset()
    logger.info(f"Starting crawl for {offset} tracks, stopping at {QUERY_STOP_OFFSET}")
    while offset <= QUERY_STOP_OFFSET:
        logger.info(f"Crawling comments for tracks at offset {offset}, batch size {BATCH_SIZE}")
        track_ids = fetch_track_ids(offset, BATCH_SIZE)
        if not track_ids:
            logger.info("No more tracks to process. Exiting.")
            return
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600)) as session:
            sem = asyncio.Semaphore(CONCURRENT_COMMENTS)
            async def sem_task(tid):
                async with sem:
                    await fetch_and_store_comments_for_track(session, tid)
            tasks = [sem_task(tid) for tid in track_ids]
            await asyncio.gather(*tasks)
        offset += BATCH_SIZE
        set_next_track_offset(offset)
        logger.info(f"Batch complete: track offset advanced to {offset}")

if __name__ == "__main__":
    try:
        asyncio.run(crawl_comments_batch())
    except Exception:
        close_connections()
    except KeyboardInterrupt:
        close_connections()