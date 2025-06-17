import random
import time

import httpx

from src.crawler.soundcloud_user_query import safe_json
from src.util.config import SOUNDCLOUD_CLIENT_ID
from src.util.constant import FOLLOWER_CK_TABLE, SOUNDCLOUD_BASE_URL
from src.util.db import close_connections, redis_client, clickhouse_client
from src.util.logger import logger
from src.util.transform_fields import flatten_json, safe_uint, safe_str, parse_datetime


REDIS_KEY = "soundcloud:last_url"

LIMIT=100
OFFSET=0
USER_ID = 193

API_URL = f"{SOUNDCLOUD_BASE_URL}/users/{USER_ID}/followers?client_id={SOUNDCLOUD_CLIENT_ID}&limit={LIMIT}&offset={OFFSET}"


def create_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {FOLLOWER_CK_TABLE} (
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


def insert_records(records):
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
            'reposts_count': safe_uint(rec.get('reposts_count')),  # Nullable, so leave as is
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
                f"INSERT INTO {FOLLOWER_CK_TABLE} VALUES",
                rows
            )
        except Exception as e:
            logger.error(f"Final batch insert failed: {e}")


def fetch_and_store(load_from_redis=False):
    last_url = redis_client.get(REDIS_KEY)
    url = last_url.decode() if (load_from_redis and last_url) else API_URL

    with httpx.Client(timeout=30) as client:
        while url:
            logger.info(f"Fetching: {url}")
            try:
                resp = client.get(url)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"Error fetching or decoding JSON from {url}: {e}")
                break

            collection = data.get('collection', [])
            logger.info(f"Fetched {len(collection)} records from {url}")
            if collection:
                insert_records(collection)

            next_href = data.get('next_href')
            if next_href:
                # Append client_id only if it's not already in the URL
                if "client_id=" not in next_href:
                    if "?" in next_href:
                        next_href += '&client_id=sobAMMic36lHKNZEwXsaEj5feRIAW9Yx'
                    else:
                        next_href += '?client_id=sobAMMic36lHKNZEwXsaEj5feRIAW9Yx'
                url = next_href
            else:
                url = None
            # Save current url to redis after each fetch
            redis_client.set(REDIS_KEY, url if url else "")
            time.sleep(random.random())
    logger.info("Done fetching all data.")


def main():
    create_table()
    fetch_and_store(load_from_redis=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        close_connections()
    except KeyboardInterrupt:
        close_connections()