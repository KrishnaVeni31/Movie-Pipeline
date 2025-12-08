"""
Enrich movies that are missing OMDb metadata (director/imdb_id) but have ratings.
This is a revised version of the original script with bug fixes and improvements:
 - fixes released date parsing bug
 - avoids caching fast-mode "errors"
 - preserves cached results even when OMDb becomes rate-limited
 - reduces frequent disk writes for the cache (save every N iterations + at end)
 - safer imdb_id collision handling
 - validates and casts `year` before sending to OMDb
 - closes requests session on exit
 - removes unused imports
 - small logging improvements

Usage:
  python enrich_missing_fixed.py            # default: batch 500 (process up to 500 movies)
  python enrich_missing_fixed.py --batch 1000
  python enrich_missing_fixed.py --fast     # skip OMDb network calls (test only)
  python enrich_missing_fixed.py --limit 50 --fast

"""
import argparse
import json
import time
from pathlib import Path
from datetime import datetime
import os
import sqlite3
import requests
from dotenv import load_dotenv
load_dotenv()

# CONFIG
DB_PATH = Path("movies.db")
MOVIES_TABLE = "movies"
OMDB_CACHE_FILE = Path("omdb_cache.json")
DEFAULT_BATCH = 500
# polite pause (seconds) between calls; reduce for testing, increase for politeness
SLEEP_BETWEEN_CALLS = 0.15
# how often to persist cache to disk (iterations)
CACHE_SAVE_EVERY = 20
# default key - prefer environment variable or .env
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

# runtime flag to avoid repeated rate-limited attempts
OMDB_RATE_LIMITED = False
_session = requests.Session()


def load_cache(path: Path = OMDB_CACHE_FILE):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf8"))
        except Exception:
            # Don't crash for malformed cache; start fresh
            return {}
    return {}


def save_cache(cache: dict, path: Path = OMDB_CACHE_FILE):
    try:
        path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf8")
    except Exception as e:
        print(f"[cache] Failed to write cache to {path}: {e}")


def _normalize_year(year):
    """Return a string year or None. Handle floats/strings like '1994.0'."""
    if year is None:
        return None
    try:
        # some datasets have floats like 1994.0
        year_int = int(float(year))
        if year_int <= 0:
            return None
        return str(year_int)
    except Exception:
        return None


def query_omdb(title, year=None, cache=None, fast=False, timeout=(3, 10)):
    """Robust OMDb query — returns dict like OMDb or {"Response":"False","Error":...}.

    Behaviour:
      - If the key exists in cache, return cached value.
      - If fast==True, do not call network and do NOT cache the "fast-mode" sentinel.
      - If OMDB_RATE_LIMITED is True and key not cached, return a rate-limited error (but do not write that into cache).
    """
    global OMDB_RATE_LIMITED, _session
    if cache is None:
        cache = {}
    key = f"{title}||{year}"
    # return cached result if present
    if key in cache:
        return cache[key]

    if fast:
        # don't pollute cache with fast-mode errors; caller can decide how to handle
        return {"Response": "False", "Error": "fast-mode: skipped"}

    if OMDB_RATE_LIMITED:
        # do not write a rate-limit sentinel into cache here; allow cached entries to be used,
        # but if there's no cache, tell the caller we've hit the rate limit so it can stop.
        return {"Response": "False", "Error": "rate-limited/invalid-key detected"}

    if not OMDB_API_KEY:
        return {"Response": "False", "Error": "No API key set"}

    params = {"apikey": OMDB_API_KEY, "t": title}
    normalized = _normalize_year(year)
    if normalized:
        params["y"] = normalized

    try:
        r = _session.get("http://www.omdbapi.com/", params=params, timeout=timeout)
        try:
            data = r.json()
        except ValueError:
            data = {"Response": "False", "Error": f"HTTP {r.status_code}"}
    except requests.exceptions.RequestException as e:
        data = {"Response": "False", "Error": f"Network error: {e}"}

    err = (data.get("Error") or "").lower()
    if "request limit" in err or "limit reached" in err or "invalid api key" in err or "rate limit" in err:
        # set global flag so future network calls are avoided this run
        OMDB_RATE_LIMITED = True
        print(f"[omdb] rate-limited/invalid-key detected: '{data.get('Error')}'. Stopping further OMDb calls this run.")
        # do not cache this error under the key; allow existing cache to be used
        return {"Response": "False", "Error": data.get("Error", "rate limited/invalid key")}

    # good or bad, cache the response for future runs (but only if network call actually happened)
    cache[key] = data
    # polite pause
    time.sleep(SLEEP_BETWEEN_CALLS)
    return data


def clean_omdb_response(r):
    if not r or r.get("Response") == "False":
        return None
    imdb_id = r.get("imdbID")
    director = r.get("Director")
    plot = r.get("Plot")
    box_office = r.get("BoxOffice")
    released = r.get("Released")
    runtime = r.get("Runtime")
    runtime_minutes = None
    if runtime and "min" in str(runtime):
        try:
            runtime_minutes = int(str(runtime).split()[0])
        except Exception:
            runtime_minutes = None
    released_date = None
    if released and released != "N/A":
        # try a sequence of formats; if a format fails, just try the next one
        for fmt in ("%d %b %Y", "%Y-%m-%d", "%d %B %Y"):
            try:
                released_date = datetime.strptime(released, fmt).date().isoformat()
                break
            except Exception:
                continue
    return {
        "imdb_id": imdb_id,
        "director": director if director and director != "N/A" else None,
        "plot": plot if plot and plot != "N/A" else None,
        "box_office": box_office if box_office and box_office != "N/A" else None,
        "released": released_date,
        "runtime_minutes": runtime_minutes,
    }


def fetch_candidates(conn, batch):
    """
    Return list of (id, title, year) for movies that:
      - have at least 1 rating
      - and (director IS NULL OR imdb_id IS NULL)
    Ordered by number of ratings DESC so we enrich popular movies first.
    """
    cur = conn.cursor()
    q = f"""
    SELECT m.id, m.title, m.year, COUNT(r.rating) AS cnt
    FROM movies m
    JOIN ratings r ON r.movie_id = m.id
    WHERE (m.director IS NULL OR m.imdb_id IS NULL)
    GROUP BY m.id
    ORDER BY cnt DESC
    LIMIT :batch
    """
    cur.execute(q, {"batch": batch})
    rows = cur.fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def update_movie(conn, movie_id, data):
    """
    Update movies table with fields returned by OMDb (upsert style).
    Only update columns if not None.
    Returns True if any column was updated, False otherwise.
    """
    cur = conn.cursor()
    fields = []
    params = {"id": movie_id}
    if data.get("imdb_id"):
        fields.append("imdb_id = :imdb_id"); params["imdb_id"] = data["imdb_id"]
    if data.get("director"):
        fields.append("director = :director"); params["director"] = data["director"]
    if data.get("plot"):
        fields.append("plot = :plot"); params["plot"] = data["plot"]
    if data.get("box_office"):
        fields.append("box_office = :box_office"); params["box_office"] = data["box_office"]
    if data.get("released"):
        fields.append("released = :released"); params["released"] = data["released"]
    # runtime_minutes can be 0 therefore check is not None
    if data.get("runtime_minutes") is not None:
        fields.append("runtime_minutes = :runtime_minutes"); params["runtime_minutes"] = data["runtime_minutes"]

    if not fields:
        return False
    set_clause = ", ".join(fields)
    sql = f"UPDATE movies SET {set_clause} WHERE id = :id"
    cur.execute(sql, params)
    return True


def main(batch=DEFAULT_BATCH, fast=False):
    global OMDB_RATE_LIMITED, _session
    if not DB_PATH.exists():
        raise SystemExit(f"{DB_PATH} not found. Run etl.py first.")

    cache = load_cache()
    conn = sqlite3.connect(str(DB_PATH))

    candidates = fetch_candidates(conn, batch)
    if not candidates:
        print("No candidate movies found (all rated movies already have director/imdb).")
        conn.close()
        return

    print(f"Found {len(candidates)} candidate movies to enrich (batch={batch}). fast={fast}")
    enriched = 0
    skipped = 0
    iterations = 0

    try:
        for movie_id, title, year in candidates:
            iterations += 1
            # if we've hit a rate-limited state and there's no cached entry for this title/year,
            # stop trying (but still process cached items earlier)
            if OMDB_RATE_LIMITED and not fast:
                print("[omdb] detected rate-limited state. Stopping.")
                break

            # query cache + API
            omdb_raw = query_omdb(title, year=year, cache=cache, fast=fast)

            # if the call returned a rate-limit error (and not a cached value), stop the run
            err = (omdb_raw.get("Error") or "").lower() if isinstance(omdb_raw, dict) else ""
            if "rate-limited" in err or "limit reached" in err or "invalid api key" in err:
                print(f"Stopping because OMDb returned an error: {omdb_raw.get('Error')}")
                break

            omdb = clean_omdb_response(omdb_raw) if omdb_raw else None
            if not omdb:
                skipped += 1
                err_msg = (omdb_raw.get("Error") if isinstance(omdb_raw, dict) else "N/A")
                print(f"Skipped: {title} (no OMDb data or cached error: {err_msg})")
                # periodically save cache
                if iterations % CACHE_SAVE_EVERY == 0:
                    save_cache(cache)
                continue

            try:
                updated = update_movie(conn, movie_id, omdb)
                conn.commit()
                if updated:
                    enriched += 1
                    print(f"Enriched: id={movie_id} title='{title}' imdb={omdb.get('imdb_id')} director={omdb.get('director')}")
                else:
                    skipped += 1
            except sqlite3.IntegrityError as e:
                # likely imdb_id UNIQUE conflict; skip setting imdb_id for this movie
                print("IntegrityError on update (likely imdb_id collision). Attempting non-imdb update.")
                # attempt to update only non-imdb fields
                data_no_imdb = omdb.copy()
                data_no_imdb["imdb_id"] = None
                try:
                    updated2 = update_movie(conn, movie_id, data_no_imdb)
                    conn.commit()
                    if updated2:
                        enriched += 1
                except Exception as e2:
                    print(f"Failed to update non-imdb fields: {e2}")

            # periodically save cache
            if iterations % CACHE_SAVE_EVERY == 0:
                save_cache(cache)

        # final save
        save_cache(cache)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            _session.close()
        except Exception:
            pass

    print(f"Done. Enriched={enriched} skipped={skipped}. Cache saved to {OMDB_CACHE_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="Number of movies to enrich in this run")
    parser.add_argument("--fast", action="store_true", help="Skip OMDb network calls (test mode)")
    args = parser.parse_args()
    main(batch=args.batch, fast=args.fast)

