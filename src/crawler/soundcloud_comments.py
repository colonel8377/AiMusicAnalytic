import asyncio
import random
import time
import traceback
import sys
import aiohttp
from aiohttp import ClientError

from src.util.config import SOUNDCLOUD_CLIENT_ID, PROXY_PWD, PROXY_TUNNEL, PROXY_USER_NAME, CLASH_URL
from src.util.db import close_connections, clickhouse_client, redis_client
from src.util.logger import logger
from src.util.transform_fields import transform_comment_to_ck, COMMENT_COLS

COMMENTS_CK_TABLE = "soundcloud_comments"
TRACKS_CK_TABLE = "tracks"

REDIS_OFFSET_DEFAULT_VAL = 0
REMAINDER = 0
QUERY_STOP_OFFSET = 10000000
REDIS_OFFSET_KEY = f"soundcloud:comments:remainder:{REMAINDER}:track_query_offset"
REDIS_QUERY_KEY = "soundcloud:comments:last_ck_query"
BATCH_SIZE = 1000
CONCURRENT_COMMENTS = 8

PROXY_AUTH = aiohttp.BasicAuth(PROXY_USER_NAME, PROXY_PWD)

def store_comments(comments) -> bool:
    if comments:
        rows = [transform_comment_to_ck(c) for c in comments]
        try:
            clickhouse_client.insert(COMMENTS_CK_TABLE, rows, column_names=COMMENT_COLS)
            logger.debug(f"Inserted {len(rows)} comments to ClickHouse")
            return True
        except Exception:
            logger.error(f"ClickHouse insert error: {traceback.format_exc()}")
            return False
    return True

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
    query = f"""
        SELECT id FROM (
            SELECT id
            FROM soundcloud.tracks
            WHERE id NOT IN (
                SELECT track_id FROM soundcloud.soundcloud_comments GROUP BY track_id
            )
            GROUP BY id
        )
        WHERE id % 10 = {REMAINDER}
        ORDER BY id ASC
        LIMIT {limit} OFFSET {offset}
        """
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
            async with session.get(url, proxy=CLASH_URL) as resp:
                if resp.status == 200:
                    return await resp.json()
                logger.warning(f"Track {track_id}: HTTP {resp.status} for {url}")
        except (ClientError, asyncio.TimeoutError) as e:
            last_exception = e
            logger.warning(
                f"Track {track_id}: Attempt {attempt+1}/{max_attempts} - {e} on {url}"
            )
        await asyncio.sleep(random.uniform(0, 3))
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
    i = 0
    while url and i < 10000:
        try:
            logger.debug(f"Fetching comments for {i} url {url}")
            data = await fetch_json_with_retry(session, url, track_id)
        except Exception as e:
            logger.error(f"Track {track_id}: Skipping due to repeated errors: {e}")
            return False  # fail for this track
        collection = data.get("collection", [])
        comments.extend(collection)
        url = data.get("next_href")
        i += 1
    return store_comments(comments)

async def crawl_comments_batch():
    offset = get_next_track_offset()
    logger.info(f"Starting crawl for {offset} tracks, stopping at {QUERY_STOP_OFFSET}")
    while offset <= QUERY_STOP_OFFSET:
        logger.info(f"Crawling comments for tracks at offset {offset}, batch size {BATCH_SIZE}")
        track_ids = fetch_track_ids(offset, BATCH_SIZE)
        if not track_ids:
            logger.info("No more tracks to process. Exiting.")
            return
        begin_time = time.time()
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600), connector=conn) as session:
            sem = asyncio.Semaphore(CONCURRENT_COMMENTS)
            async def sem_task(tid):
                async with sem:
                    return await fetch_and_store_comments_for_track(session, tid)
            tasks = [sem_task(tid) for tid in track_ids]
            task_results = await asyncio.gather(*tasks)
            num_failures = sum(not res for res in task_results)
            failure_rate = num_failures / len(track_ids)
            logger.info(f"Batch finished: {num_failures} failed out of {len(track_ids)} tracks (failure rate: {failure_rate:.2%}) in {int(time.time() - begin_time)}s")
            if failure_rate > 0.10:
                logger.error(f"Failure rate {failure_rate:.2%} exceeds 10%, exiting immediately.")
                close_connections()
                sys.exit(1)
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