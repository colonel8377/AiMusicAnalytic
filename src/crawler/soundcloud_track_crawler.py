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
REMAINDER = "null"
REDIS_KEY = f"soundcloud:track:remainder:{REMAINDER}:offset"

BATCH_SIZE = 1000
CONCURRENT_USERS = 8
TRACKS_LIMIT_PER_REQUEST = 100
RETRY_LIMIT = 10
RETRY_BACKOFF = 1.2
GLOBAL_FETCH_LIMIT = 32        # 全局爬取并发数（track分页也受限）
INSERT_BATCH_SIZE = 1000       # ClickHouse 批量插入条数
INSERT_RETRY_MAX = 2           # 插入失败最大重试次数
# MAX_PAGES_PER_USER = 100       # 每个用户最多拉多少页track

PROXY_AUTH = aiohttp.BasicAuth(PROXY_USER_NAME, PROXY_PWD)

HEADERS = {
    'Host': 'api-v2.soundcloud.com',
    'Origin': 'https://soundcloud.com',
    'Referer': 'https://soundcloud.com',
    'Sec-Fetch-Site': 'same-site',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': 'https://soundcloud.com',
}

ch_client = clickhouse_client
redis_client = redis_client

def store_tracks_batch(tracks_batch):
    """
    批量写入ClickHouse，失败重试，彻底失败则退出进程
    """
    if not tracks_batch:
        return True
    rows = [transform_track_to_ck(track) for track in tracks_batch if track]
    if not rows:
        return True
    for attempt in range(INSERT_RETRY_MAX):
        try:
            ch_client.insert(CLICKHOUSE_TABLE, rows, column_names=TRACK_COLS)
            logger.info(f"Inserted {len(rows)} tracks to {CLICKHOUSE_TABLE}")
            return True
        except Exception as e:
            logger.error(f"ClickHouse batch insert error (attempt {attempt+1}/{INSERT_RETRY_MAX}): {e}")
            if attempt < INSERT_RETRY_MAX - 1:
                time.sleep(3)
    logger.critical("ClickHouse insert failed after retries, exiting.")
    close_connections()
    sys.exit(1)

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
        query = f"""
                SELECT id FROM (
                    SELECT id
                    FROM soundcloud.users
                    WHERE id NOT IN (
                        SELECT user_id FROM soundcloud.tracks GROUP BY user_id
                    )
                    GROUP BY id
                )
                ORDER BY id ASC
                LIMIT {limit} OFFSET {offset}
                """
        return [row[0] for row in ch_client.query(query).result_rows]
    except Exception as e:
        logger.error(f"ClickHouse fetch_user_ids error: {e}")
        return []

async def fetch_json_with_retry(session, url, user_id, fetch_limiter, max_attempts=RETRY_LIMIT):
    last_exception = None
    for attempt in range(max_attempts):
        try:
            headers = HEADERS.copy()
            async with fetch_limiter:
                async with session.get(url, headers=headers, proxy=PROXY_TUNNEL, proxy_auth=PROXY_AUTH) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    logger.warning(f"User {user_id}: HTTP {resp.status} for {url}")
        except (ClientError, asyncio.TimeoutError) as e:
            last_exception = e
            logger.warning(
                f"User {user_id}: Attempt {attempt + 1}/{max_attempts} - {e} on {url}"
            )
        await asyncio.sleep(random.uniform(0, 2))
    logger.error(f"User {user_id}: Failed after {max_attempts} attempts for {url}")
    if last_exception:
        raise last_exception
    raise Exception(f"User {user_id}: Unspecified download failure for {url}")

async def fetch_tracks_for_user(session, user_id, fetch_limiter):
    url = (f"https://api-v2.soundcloud.com/users/{user_id}/tracks"
           f"?client_id={SOUNDCLOUD_CLIENT_ID}&limit={TRACKS_LIMIT_PER_REQUEST}")
    page_idx = 0
    while url:
        try:
            data = await fetch_json_with_retry(session, url, user_id, fetch_limiter)
        except Exception as e:
            logger.error(f"User {user_id}: Skipping due to repeated errors: {e}")
            yield None  # 明确 yield 一个 None，外层可判断
            return
        tracks = data.get("collection", [])
        next_href = data.get("next_href")
        if next_href:
            if 'client_id=' not in next_href:
                next_href += f'&client_id={SOUNDCLOUD_CLIENT_ID}'
        url = next_href
        yield tracks
        page_idx += 1

async def producer(queue: asyncio.Queue, session, user_ids, fetch_limiter, fail_list):
    sem_user = asyncio.Semaphore(CONCURRENT_USERS)
    async def run_user(user_id):
        async with sem_user:
            ok = False
            async for tracks in fetch_tracks_for_user(session, user_id, fetch_limiter):
                if tracks is not None:
                    await queue.put(tracks)
                    ok = True
            if not ok:
                fail_list.append(user_id)
    tasks = [asyncio.create_task(run_user(uid)) for uid in user_ids]
    await asyncio.gather(*tasks)
    await queue.put(None)  # only one consumer; if multi-consumer, put None*CONSUMER_NUM

async def consumer(queue: asyncio.Queue, cid=0):
    buffer = []
    while True:
        batch = await queue.get()
        try:
            if batch is None:
                logger.info(f"Consumer {cid}: got end signal, flushing buffer and exiting.")
                if buffer:
                    store_tracks_batch(buffer)
                break
            buffer.extend(batch)
            if len(buffer) >= INSERT_BATCH_SIZE:
                store_tracks_batch(buffer)
                buffer.clear()
        finally:
            queue.task_done()

async def crawl_batch():
    offset = get_next_batch_offset()
    while True:
        logger.info(f"Crawling offset ({offset}) tracks, size ({BATCH_SIZE})")
        user_ids = fetch_user_ids(offset, BATCH_SIZE)
        if not user_ids:
            logger.error("No user IDs fetched from ClickHouse. Exiting.")
            return
        begin_time = time.time()
        queue = asyncio.Queue(maxsize=CONCURRENT_USERS * 10)
        fail_list = []
        fetch_limiter = asyncio.Semaphore(GLOBAL_FETCH_LIMIT)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600)) as session:
            prod = asyncio.create_task(producer(queue, session, user_ids, fetch_limiter, fail_list))
            cons = asyncio.create_task(consumer(queue, 0))
            await prod
            await queue.join()
            await cons
        num_failures = len(fail_list)
        failure_rate = num_failures / len(user_ids)
        logger.info(f"Batch finished: {num_failures} failed out of {len(user_ids)} users (failure rate: {failure_rate:.2%}) in {int(time.time() - begin_time)}s")
        if failure_rate > 0.05:
            logger.error(f"Failure rate {failure_rate:.2%} exceeds 5%, exiting immediately.")
            close_connections()
            sys.exit(1)
        set_next_batch_offset(offset + BATCH_SIZE)
        logger.info(f"Batch complete: users {offset} - {offset + BATCH_SIZE - 1}")
        offset += BATCH_SIZE

if __name__ == "__main__":
    try:
        asyncio.run(crawl_batch())
    except Exception:
        close_connections()
    except KeyboardInterrupt:
        close_connections()