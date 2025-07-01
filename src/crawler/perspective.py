import asyncio
import random
from asyncio import Semaphore

import aiohttp
import aiosqlite

from src.util.config import SQLITE_FILE, PERSPECTIVE_API_KEYS
from src.util.db import redis_client, clickhouse_client, close_connections
from src.util.logger import logger
from src.util.transform_fields import parse_casted_at, remove_emoji

SQLITE_SOURCE_TABLE = "receiver_casts"
REDIS_OFFSET_KEY = "perspective:offset"
BATCH_SIZE = 1000
RETRY_LIMIT = 3
SEMAPHORE_LIMIT = 32  # Reasonable for public API
IS_RECEIVED = 1
PERSPECTIVE_ATTRIBUTES = [
    "TOXICITY", "SEVERE_TOXICITY", "IDENTITY_ATTACK",
    "INSULT", "PROFANITY", "THREAT"
]
CK_TABLE = "perspective_crawl_results"

def build_payload(text: str):
    return {
        "comment": {"text": text},
        "requestedAttributes": {attr: {} for attr in PERSPECTIVE_ATTRIBUTES}
    }

def extract_flat_response(response):
    flat = {}
    attr_scores = response.get("attributeScores", {})
    for attr in PERSPECTIVE_ATTRIBUTES:
        score = attr_scores.get(attr, {}).get("summaryScore", {}).get("value", None)
        flat[attr.lower()] = score
    flat["languages"] = ",".join(response.get("languages", []))
    flat["detected_languages"] = ",".join(response.get("detectedLanguages", []))
    return flat

async def get_offset(redis):
    try:
        offset = redis.get(REDIS_OFFSET_KEY)
        offset = int(offset) if offset else 0
        logger.info(f"Loaded offset={offset} from Redis.")
        return offset
    except Exception as e:
        logger.exception("Failed to load offset from Redis, using default 0.")
        return 0

async def set_offset(redis, offset):
    try:
        redis.set(REDIS_OFFSET_KEY, offset)
        logger.debug(f"Set offset={offset} in Redis.")
    except Exception as e:
        logger.warning(f"Failed to set offset in Redis: {e}")

def ensure_ck_table(client):
    try:
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {CK_TABLE} (
                fid Int64,
                cast_text String,
                hash String,
                casted_at DateTime,
                is_received UInt8,
                toxicity Float32,
                severe_toxicity Float32,
                identity_attack Float32,
                insult Float32,
                profanity Float32,
                threat Float32,
                languages String,
                detected_languages String
            ) ENGINE = ReplacingMergeTree()
            PARTITION BY toYYYYMM(casted_at)
            ORDER BY (fid, cast_text, hash)
            SETTINGS index_granularity = 8192
        """)
        logger.info(f"Ensured ClickHouse table {CK_TABLE} exists.")
    except Exception as e:
        logger.exception(f"Error creating ClickHouse table: {e}")

async def get_candidates(sqlite_client, offset, limit):
    try:
        async with sqlite_client.execute(
            f"SELECT fid, cast_text, hash, casted_at FROM {SQLITE_SOURCE_TABLE} LIMIT ? OFFSET ?", (limit, offset)
        ) as cursor:
            rows = await cursor.fetchall()
            logger.info(f"Generated {len(rows)} candidates from offset {offset}.")
            return rows  # List of tuples: (fid, cast_text, hash, casted_at)
    except Exception as e:
        logger.exception(f"Failed to generate candidates at offset {offset}: {e}")
        return []

def store_crawl_results(rows):
    if not rows:
        return
    try:
        columns = [
            "fid", "cast_text", "hash", "casted_at", "is_received",
            "toxicity", "severe_toxicity", "identity_attack",
            "insult", "profanity", "threat",
            "languages", "detected_languages"
        ]
        data = []
        for row in rows:
            # Cast all String columns to str (handle None as empty string)
            fid = int(row['fid']) if row['fid'] is not None else 0
            cast_text = str(row['cast_text']) if row['cast_text'] is not None else ''
            hash_ = str(row['hash']) if row['hash'] is not None else ''
            languages = str(row['languages']) if row.get('languages') is not None else ''
            detected_languages = str(row['detected_languages']) if row.get('detected_languages') is not None else ''
            # Prepare the record
            data.append((
                fid,
                cast_text,
                hash_,
                row['casted_at'],
                IS_RECEIVED,
                row.get('toxicity'),
                row.get('severe_toxicity'),
                row.get('identity_attack'),
                row.get('insult'),
                row.get('profanity'),
                row.get('threat'),
                languages,
                detected_languages,
            ))
        clickhouse_client.insert(CK_TABLE, data, column_names=columns)
        logger.info(f"Stored {len(rows)} crawl result(s) into {CK_TABLE}.")
    except Exception as e:
        logger.exception(f"Failed to store crawl results: {e}")

async def post_perspective(session, api_key, text, semaphore):
    url = f'https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze?key={api_key}'
    headers = {"Content-Type": "application/json"}
    payload = build_payload(text)
    for attempt in range(RETRY_LIMIT):
        try:
            async with semaphore:
                async with session.post(url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    try:
                        result = await resp.json()
                    except Exception:
                        result = await resp.text()
                    if resp.status == 200:
                        return extract_flat_response(result)
                    if resp.status == 400:
                        return None
                    else:
                        logger.warning(f"HTTP error {resp.status} for text={text}, attempt {attempt+1} | {result}")
        except Exception as e:
            logger.warning(f"API error for text={text}: {e}, attempt {attempt+1}")
        await asyncio.sleep(2 * (attempt + 1))
    logger.error(f"Failed all {RETRY_LIMIT} attempts for text={text[:30]}")
    return {k: None for k in [*map(str.lower, PERSPECTIVE_ATTRIBUTES), "languages", "detected_languages"]}

async def crawl_loop():
    ensure_ck_table(clickhouse_client)
    offset = await get_offset(redis_client)
    async with aiosqlite.connect(SQLITE_FILE) as sqlite_client:
        while True:
            candidates = await get_candidates(sqlite_client, offset, BATCH_SIZE)
            if not candidates:
                logger.info("No more candidates to crawl. Exiting loop.")
                break

            results = []
            semaphore = Semaphore(SEMAPHORE_LIMIT)
            async with aiohttp.ClientSession() as session:
                tasks = []
                for fid, cast_text, hash_, casted_at in candidates:
                    cast_text = remove_emoji(cast_text).strip()
                    if not cast_text or len(cast_text) == 0:
                        logger.debug(f"Empty cast_text for fid={fid}, skipping.")
                        continue
                    api_key = random.choice(PERSPECTIVE_API_KEYS)
                    tasks.append(
                        asyncio.create_task(
                            post_perspective(session, api_key, cast_text, semaphore)
                        )
                    )
                posts = await asyncio.gather(*tasks)
                for (fid, cast_text, hash_, casted_at), response in zip(candidates, posts):
                    if not response:
                        continue
                    row = {
                        "fid": fid,
                        "cast_text": cast_text,
                        "hash": hash_,
                        "casted_at": parse_casted_at(casted_at) if isinstance(casted_at, str) else casted_at
                    }
                    row.update(response)
                    results.append(row)

            store_crawl_results(results)
            offset += len(candidates)
            await set_offset(redis_client, offset)
            logger.info(f"Batch finished. Updated offset to {offset}.")

            if len(candidates) < BATCH_SIZE:
                logger.info("Last batch less than batch size, finishing crawl.")
                break

async def main():
    try:
        logger.info("Perspective crawling and integration started.")
        await crawl_loop()
        logger.info("Perspective crawling and integration finished.")
    except Exception as e:
        logger.exception(f"Fatal error in crawl main: {e}")
    finally:
        close_connections()

if __name__ == "__main__":
    asyncio.run(main())