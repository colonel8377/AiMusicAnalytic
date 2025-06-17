import asyncio
import random
import sys
import time
import traceback
from typing import List, Tuple, Optional

import aiohttp

from src.util.config import SOUNDCLOUD_CLIENT_ID, SOUNDCLOUD_APP_VERSION, CLICKHOUSE_DATABASE, PROXY_TUNNEL
from src.util.constant import PROXY_AUTH, USER_CK_TABLE, SOUNDCLOUD_BASE_URL, CONCURRENT_USERS, BATCH_SIZE
from src.util.db import close_connections, redis_client, clickhouse_client
from src.util.logger import logger
from src.util.transform_fields import flatten_json, safe_str, safe_uint, parse_datetime, safe_json

CLIENT_ID = SOUNDCLOUD_CLIENT_ID
APP_VERSION = SOUNDCLOUD_APP_VERSION
REDIS_KEY = 'soundcloud:snowbase:ck_offset_limit'

def get_ck_offset_limit_from_redis() -> Tuple[int, int]:
    offset = redis_client.hget(REDIS_KEY, 'offset')
    limit = redis_client.hget(REDIS_KEY, 'limit')
    return int(offset) if offset else 0, int(limit) if limit else BATCH_SIZE

def set_ck_offset_limit_to_redis(offset: int, limit: int):
    redis_client.hset(REDIS_KEY, mapping={'offset': offset, 'limit': limit})

def get_seed_ids_from_ck(ch_client, offset=0, limit=BATCH_SIZE) -> List[int]:
    sql = (
        f"SELECT id FROM {CLICKHOUSE_DATABASE}.{USER_CK_TABLE} "
        f"ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
    )
    try:
        result = ch_client.query(sql)
        ids = [int(row[0]) for row in result.result_rows]
        logger.info(f"Fetched {len(ids)} seed ids from ClickHouse (offset={offset}, limit={limit})")
        return ids
    except Exception as e:
        logger.error(f"Error fetching seed ids from ClickHouse: {e}")
        return []

async def fetch_followers(
    session: aiohttp.ClientSession,
    user_id: int,
    url: str,
    max_retries: int = 10,
    retry_backoff: int = 1
) -> Optional[dict]:
    for attempt in range(max_retries):
        try:
            async with session.get(url, timeout=60, proxy=PROXY_TUNNEL, proxy_auth=PROXY_AUTH) as resp:
                if resp.status == 200:
                    return await resp.json()
                logger.warning(f"Failed to fetch followers for url {url}: HTTP {resp.status}")
        except Exception as e:
            logger.error(f"Exception in fetch_followers for user {user_id}: {e}. URL: {url}")
        await asyncio.sleep(random.uniform(1,10))
    logger.error(f"Exceeded max retries for fetch_followers for user {user_id}. URL: {url}")
    return None

def insert_records(records, user_id, client):
    if not records:
        return
    rows = []
    for rec in records:
        flat = flatten_json(rec)
        row = {
            'id': safe_uint(flat.get('id')),
            'avatar_url': safe_str(flat.get('avatar_url')),
            'city': safe_str(flat.get('city')),
            'comments_count': safe_uint(flat.get('comments_count')),
            'country_code': safe_str(flat.get('country_code')),
            'created_at': parse_datetime(flat.get('created_at')),
            'creator_subscriptions': [safe_json(rec.get('creator_subscriptions', []))],
            'creator_subscription': safe_json(rec.get('creator_subscription', {})),
            'description': safe_str(flat.get('description')),
            'followers_count': safe_uint(flat.get('followers_count')),
            'followings_count': safe_uint(flat.get('followings_count')),
            'first_name': safe_str(flat.get('first_name')),
            'full_name': safe_str(flat.get('full_name')),
            'groups_count': safe_uint(flat.get('groups_count')),
            'kind': safe_str(flat.get('kind')),
            'last_modified': parse_datetime(flat.get('last_modified')),
            'last_name': safe_str(flat.get('last_name')),
            'likes_count': safe_uint(flat.get('likes_count')),
            'playlist_likes_count': safe_uint(flat.get('playlist_likes_count')),
            'permalink': safe_str(flat.get('permalink')),
            'permalink_url': safe_str(flat.get('permalink_url')),
            'playlist_count': safe_uint(flat.get('playlist_count')),
            'reposts_count': safe_uint(flat.get('reposts_count')),
            'track_count': safe_uint(flat.get('track_count')),
            'uri': safe_str(flat.get('uri')),
            'urn': safe_str(flat.get('urn')),
            'username': safe_str(flat.get('username')),
            'verified': int(flat.get('verified', False)),
            'visuals': safe_json(rec.get('visuals', {})),
            'badges': safe_json(rec.get('badges', {})),
            'station_urn': safe_str(flat.get('station_urn')),
            'station_permalink': safe_str(flat.get('station_permalink')),
            '_raw.key': [],
            '_raw.value': [],
        }
        for k, v in rec.items():
            row['_raw.key'].append(safe_str(k))
            row['_raw.value'].append(safe_json(v))
        rows.append(tuple(row.values()))
    try:
        client.insert(
            USER_CK_TABLE,
            rows,
            column_names=[
                'id', 'avatar_url', 'city', 'comments_count', 'country_code', 'created_at',
                'creator_subscriptions', 'creator_subscription', 'description', 'followers_count',
                'followings_count', 'first_name', 'full_name', 'groups_count', 'kind', 'last_modified',
                'last_name', 'likes_count', 'playlist_likes_count', 'permalink', 'permalink_url',
                'playlist_count', 'reposts_count', 'track_count', 'uri', 'urn', 'username', 'verified',
                'visuals', 'badges', 'station_urn', 'station_permalink', '_raw.key', '_raw.value'
            ]
        )
        logger.debug(f"ClickHouse insert success, user id: {user_id}, table name: {USER_CK_TABLE}, length: {len(rows)}")
    except Exception as e:
        logger.error(f"ClickHouse insert failed: {e}")

async def snowball_user(user_id, ch_client):
    async with aiohttp.ClientSession() as session:
        url = (
            f"{SOUNDCLOUD_BASE_URL}/users/{user_id}/followers"
            f"?client_id={CLIENT_ID}&offset=0&limit=100&linked_partitioning=1"
            f"&app_version={APP_VERSION}&app_locale=en"
        )
        inserts = []
        while url:
            data = await fetch_followers(session, user_id, url)
            if not data or 'collection' not in data:
                logger.info(f"No data or collection for user {user_id}")
                return False
            collections = data['collection']
            if collections:
                inserts.extend(collections)
            next_href = data.get('next_href')
            url = (
                f"{next_href}&client_id={CLIENT_ID}"
                f"&linked_partitioning=1&app_version={APP_VERSION}&app_locale=en"
            ) if next_href else None
    insert_records(inserts, user_id, ch_client)
    return True

async def process_batch(offset, limit):
    ch_client = clickhouse_client
    snow_ids = get_seed_ids_from_ck(ch_client, offset=offset, limit=limit)
    if not snow_ids:
        logger.info("No more seed ids from CK. All done.")
        return False

    logger.info(f"Processing {len(snow_ids)} seed ids from CK.")
    semaphore = asyncio.Semaphore(CONCURRENT_USERS)
    begin_time = time.time()
    async def sem_task(user_id):
        async with semaphore:
            try:
                await snowball_user(user_id, ch_client)
            except Exception as e:
                logger.error(f"Error processing user {user_id}: {e}")

    tasks = [sem_task(uid) for uid in snow_ids]
    task_results = await asyncio.gather(*tasks)
    num_failures = sum(not res for res in task_results)
    failure_rate = num_failures / len(tasks)
    logger.info(
        f"Batch finished: {num_failures} failed out of {len(tasks)} tracks (failure rate: {failure_rate:.2%}) in {int(time.time() - begin_time)}s")
    if failure_rate > 0.10:
        logger.error(f"Failure rate {failure_rate:.2%} exceeds 10%, exiting immediately.")
        close_connections()
        sys.exit(1)
    return True

async def main():
    offset, limit = get_ck_offset_limit_from_redis()
    while True:
        logger.info(f"Snowballing batch: offset={offset}, limit={limit}")
        success = await process_batch(offset, limit)
        if not success:
            logger.info("No more batches to process. Exiting.")
            break
        offset += limit
        set_ck_offset_limit_to_redis(offset, limit)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception:
        logger.error(traceback.format_exc())
        close_connections()
    except KeyboardInterrupt:
        close_connections()