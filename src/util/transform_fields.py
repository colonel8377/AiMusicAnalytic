# --- TYPE HELPERS ---
import json
from datetime import datetime


def parse_datetime(val):
    if val is None:
        return None
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
            return None
    return None

def safe_release_date(val):
    dt = parse_datetime(val)
    if not dt or dt.year < 1970:
        return None
    return dt

def safe_int(val):
    try:
        return int(val)
    except Exception:
        return 0

def safe_bool(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("1", "true", "yes")
    if isinstance(val, int):
        return val != 0
    return False

def safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return ""

def safe_str(val):
    return str(val) if val is not None else ""

def safe_nullable_string(val):
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)

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


NON_NULLABLE_UINT32 = [
    'id', 'comment_count', 'download_count', 'duration', 'full_duration',
    'likes_count', 'playback_count', 'reposts_count', 'user_id'
]
NON_NULLABLE_BOOL = [
    'commentable', 'downloadable', 'has_downloads_left', 'public', 'streamable'
]
NON_NULLABLE_STRING = [
    "artwork_url","embeddable_by","kind","license","permalink","permalink_url","sharing",
    "state","tag_list","title","uri","urn","waveform_url","station_urn","station_permalink",
    "track_authorization","monetization_model","policy"
]
NULLABLE_STRING_SPECIAL = ["visuals"]
DATETIME_FIELDS = [
    "created_at", "last_modified", "release_date", "display_date"
]
PM_FIELDS = [
    "id", "urn", "artist", "album_title", "contains_music", "upc_or_ean", "isrc",
    "explicit", "p_line", "p_line_for_display", "c_line", "c_line_for_display", "release_title"
]

# --- MAIN TRACK TRANSFORM ---
def transform_track_to_ck(track: dict) -> list:
    # Flatten publisher_metadata
    pm = track.pop("publisher_metadata", {}) or {}
    for pm_field in PM_FIELDS:
        track[f"publisher_metadata_{pm_field}"] = pm.get(pm_field, None)

    # Fix datetimes
    for k in ["created_at", "last_modified", "display_date"]:
        track[k] = parse_datetime(track.get(k))
    track["release_date"] = safe_release_date(track.get("release_date"))

    # Non-nullable ints/bools/strings
    for k in NON_NULLABLE_UINT32:
        track[k] = safe_int(track.get(k, 0))
    for k in NON_NULLABLE_BOOL:
        track[k] = safe_bool(track.get(k, False))
    for k in NON_NULLABLE_STRING:
        track[k] = safe_str(track.get(k, ""))

    # Nullable string fields that may be dicts
    for k in NULLABLE_STRING_SPECIAL:
        track[k] = safe_nullable_string(track.get(k, None))

    # Output as ordered list
    return [track.get(col, None) for col in TRACK_COLS]
