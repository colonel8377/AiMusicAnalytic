import asyncio
import random
import sys
import time
import traceback

import aiohttp
from aiohttp import ClientError

from src.util.config import SOUNDCLOUD_CLIENT_ID, CLASH_URL
from src.util.constant import COMMENTS_CK_TABLE, REDIS_QUERY_KEY, INSERT_RETRY_MAX, INSERT_BATCH_SIZE, CONSUMER_NUM, \
    CONCURRENT_TRACKS, BATCH_SIZE, GLOBAL_FETCH_LIMIT
from src.util.db import close_connections, clickhouse_client, redis_client
from src.util.logger import logger
from src.util.transform_fields import transform_comment_to_ck, COMMENT_COLS

REMAINDER = 0

def store_comments_batch(comments_batch) -> bool:
    """
    批量写入ClickHouse，失败重试，彻底失败则退出进程
    comments_batch: List[dict]
    """
    if not comments_batch:
        return True
    rows = [transform_comment_to_ck(c) for c in comments_batch if c]
    if not rows:
        return True
    for attempt in range(INSERT_RETRY_MAX):
        try:
            clickhouse_client.insert(COMMENTS_CK_TABLE, rows, column_names=COMMENT_COLS)
            logger.info(f"Inserted {len(rows)} comments to ClickHouse")
            return True
        except Exception as e:
            logger.error(f"ClickHouse insert error (attempt {attempt+1}/{INSERT_RETRY_MAX}): {e}")
            if attempt < INSERT_RETRY_MAX - 1:
                time.sleep(1)
    logger.critical("ClickHouse insert failed after retries, exiting.")
    close_connections()
    sys.exit(1)

def set_last_ck_query(query):
    try:
        redis_client.set(REDIS_QUERY_KEY, query)
    except Exception as e:
        logger.error(f"Redis set last CK query error: {e}")

def fetch_track_ids(limit):
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
        LIMIT {limit}
        """
    set_last_ck_query(query)
    try:
        rows = clickhouse_client.query(query).result_rows
        return [row[0] for row in rows if row and isinstance(row[0], int)]
    except Exception as e:
        logger.error(f"ClickHouse fetch_track_ids error: {e}")
        return []

async def fetch_json_with_retry(session, url, track_id, fetch_limiter, max_attempts=10):
    last_exception = None
    for attempt in range(max_attempts):
        try:
            async with fetch_limiter:
                async with session.get(url, proxy=CLASH_URL) as resp:
                    if resp.status == 200:
                        try:
                            return await resp.json()
                        except Exception as parse_err:
                            logger.error(f"Track {track_id}: JSON parse error for {url}: {parse_err}")
                            return {}
                    logger.warning(f"Track {track_id}: HTTP {resp.status} for {url}")
        except (ClientError, asyncio.TimeoutError, aiohttp.ClientPayloadError) as e:
            last_exception = e
            logger.warning(
                f"Track {track_id}: Attempt {attempt+1}/{max_attempts} - {e} on {url}"
            )
        except Exception as e:
            last_exception = e
            logger.warning(
                f"Track {track_id}: Attempt {attempt+1}/{max_attempts} - Unexpected {e} on {url}"
            )
        await asyncio.sleep(random.uniform(0.5, 2.0))
    logger.error(f"Track {track_id}: Failed after {max_attempts} attempts for {url}")
    if last_exception:
        raise last_exception
    raise Exception(f"Track {track_id}: Unspecified download failure for {url}")

async def fetch_comments_for_track(session, track_id, queue: asyncio.Queue, fetch_limiter):
    first_url = (
        f"https://api-v2.soundcloud.com/tracks/{track_id}/comments"
        f"?sort=newest&threaded=1&client_id={SOUNDCLOUD_CLIENT_ID}&offset=0&limit=100"
    )
    url = first_url
    try:
        while url:
            data = await fetch_json_with_retry(session, url, track_id, fetch_limiter)
            collection = data.get("collection", [])
            if collection:
                await queue.put(collection)
            next_href = data.get("next_href")
            if not next_href or not isinstance(next_href, str) or next_href == url:
                break
            url = next_href
        return True
    except Exception as e:
        logger.error(f"Track {track_id} exception: {e}")
        return False

async def producer(queue: asyncio.Queue, session, track_ids, fail_list, fetch_limiter):
    sem_track = asyncio.Semaphore(CONCURRENT_TRACKS)
    async def run_track(track_id):
        async with sem_track:
            ok = await fetch_comments_for_track(session, track_id, queue, fetch_limiter)
            if not ok:
                fail_list.append(track_id)
    tasks = [asyncio.create_task(run_track(tid)) for tid in track_ids]
    await asyncio.gather(*tasks)
    for _ in range(CONSUMER_NUM):
        await queue.put(None)

async def consumer(queue: asyncio.Queue, cid=0):
    buffer = []
    while True:
        batch = await queue.get()
        try:
            if batch is None:
                logger.info(f"Consumer {cid}: got end signal, flushing buffer and exiting.")
                # flush buffer on exit
                if buffer:
                    store_comments_batch(buffer)
                break
            # batch is a list of comments, flatten into the buffer
            buffer.extend(batch)
            if len(buffer) >= INSERT_BATCH_SIZE:
                store_comments_batch(buffer)
                buffer.clear()
        finally:
            queue.task_done()

async def crawl_comments_batch():
    offset = 0
    logger.info(f"Starting crawl for {offset} tracks")
    while True:
        logger.info(f"Crawling comments for tracks batch size {BATCH_SIZE}")
        track_ids = fetch_track_ids(BATCH_SIZE)
        if not track_ids:
            logger.info("No more tracks to process. Exiting.")
            return
        begin_time = time.time()
        logger.info(f"Crawling comments for {len(track_ids)} tracks")
        queue = asyncio.Queue(maxsize=CONCURRENT_TRACKS * 10)
        fail_list = []
        conn = aiohttp.TCPConnector(ssl=False, limit=GLOBAL_FETCH_LIMIT)
        fetch_limiter = asyncio.Semaphore(GLOBAL_FETCH_LIMIT)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600), connector=conn) as session:
            prod = asyncio.create_task(producer(queue, session, track_ids, fail_list, fetch_limiter))
            consumers = [asyncio.create_task(consumer(queue, cid)) for cid in range(CONSUMER_NUM)]
            await prod
            await queue.join()
            await asyncio.gather(*consumers)
        failure_count = len(fail_list)
        failure_rate = failure_count / len(track_ids)
        logger.info(f"Batch finished: {failure_count} tracks failed out of {len(track_ids)} ({failure_rate:.2%}) in {int(time.time() - begin_time)}s")
        if failure_rate > 0.05:
            logger.error(f"Failure rate {failure_rate:.2%} exceeds 5%, exiting immediately.")
            close_connections()
            sys.exit(1)
        logger.info(f"Batch complete: track offset advanced to {offset} in {int(time.time() - begin_time)}s")

def main():
    try:
        asyncio.run(crawl_comments_batch())
    except Exception:
        logger.error(f"Top-level error: {traceback.format_exc()}")
        close_connections()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, closing connections")
        close_connections()

if __name__ == "__main__":
    main()