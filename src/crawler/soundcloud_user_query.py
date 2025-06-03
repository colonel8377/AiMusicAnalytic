import random
import time

import httpx  # pip install httpx

from src.crawler.soundcloud_track_crawler import logger
from src.util.config import SOUNDCLOUD_CLIENT_ID
from src.util.db import clickhouse_client, redis_client, close_connections
from src.util.transform_fields import safe_int, safe_str, parse_datetime, flatten_json, safe_json

# CONFIGURATION
API_URL = f"https://api-v2.soundcloud.com/search/users?client_id={SOUNDCLOUD_CLIENT_ID}&offset=0&limit=100"

TABLE_NAME = "user_query"
REDIS_KEY_PREFIX = "soundcloud:user_query:"


def create_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id UInt64,
        avatar_url String,
        city String,
        comments_count Int32,
        country_code String,
        created_at DateTime,
        creator_subscriptions Array(String),
        creator_subscription String,
        description String,
        followers_count UInt32,
        followings_count UInt32,
        first_name String,
        full_name String,
        groups_count UInt32,
        kind String,
        last_modified DateTime,
        last_name String,
        likes_count UInt32,
        playlist_likes_count UInt32,
        permalink String,
        permalink_url String,
        playlist_count UInt32,
        reposts_count Nullable(Int32),
        track_count UInt32,
        uri String,
        urn String,
        username String,
        verified UInt8,
        visuals String,
        badges String,
        station_urn String,
        station_permalink String,
        query_keyword String,
        _raw Nested (
            key String,
            value String
        )
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(created_at)
    PRIMARY KEY id
    ORDER BY (id, username, created_at)
    SETTINGS index_granularity = 8192;
    """
    clickhouse_client.execute(ddl)

def insert_records(records, query_keyword):
    rows = []
    for rec in records:
        flat = flatten_json(rec)
        row = {
            'id': safe_int(flat.get('id')),
            'avatar_url': safe_str(flat.get('avatar_url')),
            'city': safe_str(flat.get('city')),
            'comments_count': safe_int(flat.get('comments_count')),
            'country_code': safe_str(flat.get('country_code')),
            'created_at': parse_datetime(flat.get('created_at')),
            'creator_subscriptions': [safe_json(rec.get('creator_subscriptions', []))],
            'creator_subscription': safe_json(rec.get('creator_subscription', {})),
            'description': safe_str(flat.get('description')),
            'followers_count': safe_int(flat.get('followers_count')),
            'followings_count': safe_int(flat.get('followings_count')),
            'first_name': safe_str(flat.get('first_name')),
            'full_name': safe_str(flat.get('full_name')),
            'groups_count': safe_int(flat.get('groups_count')),
            'kind': safe_str(flat.get('kind')),
            'last_modified': parse_datetime(flat.get('last_modified')),
            'last_name': safe_str(flat.get('last_name')),
            'likes_count': safe_int(flat.get('likes_count')),
            'playlist_likes_count': safe_int(flat.get('playlist_likes_count')),
            'permalink': safe_str(flat.get('permalink')),
            'permalink_url': safe_str(flat.get('permalink_url')),
            'playlist_count': safe_int(flat.get('playlist_count')),
            'reposts_count': rec.safe_int('reposts_count'),  # Nullable, so leave as is
            'track_count': safe_int(flat.get('track_count')),
            'uri': safe_str(flat.get('uri')),
            'urn': safe_str(flat.get('urn')),
            'username': safe_str(flat.get('username')),
            'verified': int(flat.get('verified', False)),
            'visuals': safe_json(rec.get('visuals', {})),
            'badges': safe_json(rec.get('badges', {})),
            'station_urn': safe_str(flat.get('station_urn')),
            'station_permalink': safe_str(flat.get('station_permalink')),
            'query_keyword': query_keyword,
            '_raw.key': [],
            '_raw.value': []
        }
        for k, v in rec.items():
            row['_raw.key'].append(safe_str(k))
            row['_raw.value'].append(safe_json(v))
        rows.append(tuple(row.values()))

    # Insert remaining rows
    if rows:
        try:
            clickhouse_client.execute(
                f"INSERT INTO {TABLE_NAME} VALUES",
                rows
            )
        except Exception as e:
            logger.error(f"Final batch insert failed: {e}")


def fetch_and_store(query_keyword, load_from_redis=True):
    key = REDIS_KEY_PREFIX + query_keyword
    last_url = redis_client.get(key)
    url = last_url.decode() if (load_from_redis and last_url) else API_URL + "&q=" + query_keyword
    with httpx.Client(timeout=30) as client:
        while url:
            logger.info(f"Fetching: {url}")
            try:
                resp = client.get(url, timeout=10)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"Error fetching or decoding JSON from {url}: {e}")
                break

            collection = data.get('collection', [])
            logger.info(f"Fetched {len(collection)} records from {url}")
            if collection:
                insert_records(collection, query_keyword)
            next_href = data.get('next_href')
            if next_href:
                if "client_id=" not in next_href:
                    next_href += f'&client_id={SOUNDCLOUD_CLIENT_ID}'
                url = next_href
            else:
                url = None
            redis_client.set(key, url if url else "")
            time.sleep(random.random())
    logger.info("Done fetching all data.")

def main():
    create_table()
    keywords = ['AIcreated', 'AIgenerated', 'AI create',
                'AI generated', 'AI created', 'AI Gen',
                'AIGen', 'AIGEN', 'AIGC', 'AI', 'AI Music',
                'ai', 'Artificial Intelligence',
                'ai created', 'ai create', 'ai generated']
    for query_keyword in keywords:
        fetch_and_store(query_keyword)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        close_connections()
    except KeyboardInterrupt:
        close_connections()