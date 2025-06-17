import json
from datetime import datetime


def parse_datetime(val):
    if val is None:
        return datetime.fromisocalendar(1970, 1, 1)
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        v = val.replace("Z", "")
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(v, fmt)
            except Exception:
                continue
        try:
            return datetime.fromisoformat(v)
        except Exception:
            return datetime.fromisocalendar(1970, 1, 1)
    return datetime.fromisocalendar(1970, 1, 1)

def safe_release_date(val):
    dt = parse_datetime(val)
    if not dt or dt.year < 1970:
        return datetime.fromisocalendar(1970, 1, 1)
    return dt

def safe_int(val):
    if val is None or val == "":
        return None
    try:
        return int(val)
    except Exception:
        return None

def safe_uint(val):
    if val is None or val == "":
        return 0
    try:
        val = int(val)
        return val if 0 <= val <= 4294967295 else 0
    except Exception:
        return 0


def safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return ""

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], f'{name}{a}_')
        elif type(x) is list:
            out[name[:-1]] = json.dumps(x, ensure_ascii=False)
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

def safe_uint32(val):
    try:
        val = int(val)
        if 0 <= val <= 4294967295:
            return val
        return 0
    except Exception:
        return 0

def safe_str(val):
    return "" if val is None else str(val)

def safe_nullable_string(val):
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)

def safe_bool(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("1", "true", "yes")
    if isinstance(val, int):
        return val != 0
    return False

def safe_nullable_bool(val):
    if val is None:
        return None
    return safe_bool(val)

def safe_uint64(val):
    try:
        val = int(val)
        if 0 <= val <= 18446744073709551615:
            return val
        return 0
    except Exception:
        return 0

def safe_nullable_uint64(val):
    try:
        if val is None:
            return None
        val = int(val)
        if 0 <= val <= 18446744073709551615:
            return val
        return None
    except Exception:
        return None


MIN_CH_DATETIME = datetime(1970, 1, 2)
MAX_CH_DATETIME = datetime(2106, 2, 7, 6, 28, 15)

def safe_datetime(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        if not (MIN_CH_DATETIME <= val <= MAX_CH_DATETIME):
            return None
        return val
    if isinstance(val, str):
        v = val.replace("Z", "")
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(v, fmt)
                if not (MIN_CH_DATETIME <= dt <= MAX_CH_DATETIME):
                    return None
                return dt
            except Exception:
                continue
        try:
            dt = datetime.fromisoformat(v)
            if not (MIN_CH_DATETIME <= dt <= MAX_CH_DATETIME):
                return None
            return dt
        except Exception:
            return None
    return None

# --- FIELD GROUPS BASED ON DDL ---
NON_NULLABLE_UINT64 = [
    'id', 'user_id', 'comment_count', 'download_count', 'duration', 'full_duration',
    'likes_count', 'playback_count', 'reposts_count'
]
NON_NULLABLE_BOOL = [
    'commentable', 'downloadable', 'has_downloads_left', 'public', 'streamable'
]
NON_NULLABLE_STRING = [
    "artwork_url","embeddable_by","kind","license","permalink","permalink_url","sharing",
    "state","tag_list","title","uri","urn","waveform_url","station_urn","station_permalink",
    "track_authorization","monetization_model","policy"
]
NULLABLE_STRING = [
    "caption","description","genre","label_name","purchase_title","purchase_url","secret_token",
    "visuals","publisher_metadata_urn","publisher_metadata_artist","publisher_metadata_album_title",
    "publisher_metadata_upc_or_ean","publisher_metadata_isrc","publisher_metadata_p_line",
    "publisher_metadata_p_line_for_display","publisher_metadata_c_line","publisher_metadata_c_line_for_display",
    "publisher_metadata_release_title"
]
DATETIME_FIELDS = [
    "created_at", "last_modified", "release_date", "display_date"
]
NULLABLE_UINT64 = [
    "publisher_metadata_id"
]
NULLABLE_BOOL = [
    "publisher_metadata_contains_music","publisher_metadata_explicit"
]
PM_FIELDS = [
    "id", "urn", "artist", "album_title", "contains_music", "upc_or_ean", "isrc",
    "explicit", "p_line", "p_line_for_display", "c_line", "c_line_for_display", "release_title"
]

TRACK_COLS = [
    "id","artwork_url","caption","commentable","comment_count","created_at","description",
    "downloadable","download_count","duration","full_duration","embeddable_by","genre",
    "has_downloads_left","kind","label_name","last_modified","license","likes_count",
    "permalink","permalink_url","playback_count","public","purchase_title","purchase_url",
    "release_date","reposts_count","secret_token","sharing","state","streamable","tag_list",
    "title","uri","urn","user_id","visuals","waveform_url","display_date","station_urn",
    "station_permalink","track_authorization","monetization_model","policy",
    "publisher_metadata_id","publisher_metadata_urn","publisher_metadata_artist",
    "publisher_metadata_album_title","publisher_metadata_contains_music",
    "publisher_metadata_upc_or_ean","publisher_metadata_isrc","publisher_metadata_explicit",
    "publisher_metadata_p_line","publisher_metadata_p_line_for_display",
    "publisher_metadata_c_line","publisher_metadata_c_line_for_display",
    "publisher_metadata_release_title"
]

def transform_track_to_ck(track: dict) -> list:
    # Flatten publisher_metadata
    pm = track.pop("publisher_metadata", {}) or {}
    for pm_field in PM_FIELDS:
        track[f"publisher_metadata_{pm_field}"] = pm.get(pm_field, None)

    for k in DATETIME_FIELDS:
        track[k] = safe_datetime(track.get(k, None))
    for k in NON_NULLABLE_UINT64:
        track[k] = safe_uint64(track.get(k, 0))
    for k in NON_NULLABLE_BOOL:
        track[k] = safe_bool(track.get(k, False))
    for k in NON_NULLABLE_STRING:
        track[k] = safe_str(track.get(k, ""))
    for k in NULLABLE_STRING:
        track[k] = safe_nullable_string(track.get(k, None))
    for k in NULLABLE_UINT64:
        track[k] = safe_nullable_uint64(track.get(k, None))
    for k in NULLABLE_BOOL:
        track[k] = safe_nullable_bool(track.get(k, None))

    # Output in exact order required by ClickHouse
    return [track.get(col, None) for col in TRACK_COLS]

COMMENT_COLS = [
    "kind", "id", "body", "created_at", "timestamp", "track_id", "user_id", "self_urn"
]


def transform_comment_to_ck(comment: dict) -> list:
    return [
        safe_str(comment.get("kind")),
        safe_uint(comment.get("id")),
        safe_str(comment.get("body")),
        parse_datetime(comment.get("created_at")),
        safe_uint(comment.get("timestamp")),
        safe_uint(comment.get("track_id")),
        safe_uint(comment.get("user_id")),
        safe_str(comment.get("self", {}).get("urn")),
    ]