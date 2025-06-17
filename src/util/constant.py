import aiohttp

from src.util.config import PROXY_USER_NAME, PROXY_PWD

REDIS_QUERY_KEY = "soundcloud:comments:last_ck_query"

COMMENTS_CK_TABLE = "soundcloud_comments"
TRACKS_CK_TABLE = "tracks"
FOLLOWER_CK_TABLE = "followers"
USER_CK_TABLE = 'users'
SOUNDCLOUD_BASE_URL = "https://api-v2.soundcloud.com"

BATCH_SIZE = 2500
CONCURRENT_TRACKS = 8
CONSUMER_NUM = 1
GLOBAL_FETCH_LIMIT = 32
INSERT_RETRY_MAX = 5  # 插入失败最大重试次数
INSERT_BATCH_SIZE = 1000  # 批量插入clickhouse的条数


CONCURRENT_USERS = 8
TRACKS_LIMIT_PER_REQUEST = 100
RETRY_LIMIT = 10
RETRY_BACKOFF = 1.2

PROXY_AUTH = aiohttp.BasicAuth(PROXY_USER_NAME, PROXY_PWD)
