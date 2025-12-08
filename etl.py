"""
Optimized and fixed ETL for MovieLens + OMDb enrichment.

Key fixes / improvements vs original:
- cache key includes movieId to avoid cross-title collisions
- fast mode and rate-limit responses are NOT written into cache
- more robust OMDb search fallback: prefer exact-title match from search results
- released date parsing bug fixed (don't overwrite on except)
- session closed on exit
- periodic cache saves and final save
- bulk/transactional insertion for ratings to speed up large datasets
- safer casting of year and timestamp values
- fewer per-row DB round-trips for ratings (use executemany in chunks)

Usage: same as original.
"""
import os
import re
import json
import time
import argparse
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
load_dotenv()

# CONFIG
DB_URL = os.environ.get("DB_URL", "sqlite:///movies.db")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_CACHE_FILE = Path("omdb_cache.json")
MOVIES_CSV = Path("movies.csv")
RATINGS_CSV = Path("ratings.csv")
SLEEP_BETWEEN_CALLS = 0.12
CACHE_SAVE_EVERY = 50
RATINGS_CHUNK = 2000

OMDB_RATE_LIMITED = False
_session = requests.Session()


def load_cache(path: Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf8"))
        except Exception:
            return {}
    return {}


def save_cache(cache, path: Path):
    try:
        path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf8")
    except Exception as e:
        print(f"[cache] failed to write cache: {e}")


def _normalize_year(year):
    if year is None or (isinstance(year, float) and pd.isna(year)):
        return None
    try:
        return int(float(year))
    except Exception:
        return None


def parse_title_and_year(title):
    m = re.match(r"^(?P<title>.*)\s+\((?P<year>\d{4})\)$", str(title))
    if m:
        return m.group("title").strip(), _normalize_year(m.group("year"))
    return title, None


def _call_omdb(params, timeout=(3, 8)):
    try:
        r = _session.get("http://www.omdbapi.com/", params=params, timeout=timeout)
        try:
            return r.json()
        except ValueError:
            return {"Response": "False", "Error": f"HTTP {r.status_code}"}
    except requests.RequestException as e:
        return {"Response": "False", "Error": f"Network: {e}"}


def _choose_best_search_result(title, results):
    """Prefer exact (case-insensitive) title matches, else fall back to first result."""
    if not results:
        return None
    t_norm = re.sub(r"\W+", "", title or "").lower()
    for r in results:
        rtitle = r.get("Title", "")
        if re.sub(r"\W+", "", rtitle).lower() == t_norm:
            return r
    return results[0]


def query_omdb(movie_id, title, year=None, cache=None, fast=False):
    """Query OMDb. Cache key includes movie_id to avoid collisions.
    - Do not write fast-mode or rate-limit sentinel into cache
    - If cache hit: return cached
    """
    global OMDB_RATE_LIMITED
    if cache is None:
        cache = {}
    key = f"{movie_id}||{title}||{year}"
    if key in cache:
        return cache[key]

    if fast:
        return {"Response": "False", "Error": "fast-mode: skipped"}

    if OMDB_RATE_LIMITED:
        return {"Response": "False", "Error": "rate-limited/invalid-key"}

    if not OMDB_API_KEY:
        return {"Response": "False", "Error": "No API key set"}

    # try exact title lookup first
    params = {"apikey": OMDB_API_KEY, "t": title}
    if year:
        params["y"] = str(year)
    data = _call_omdb(params)
    if data.get("Response") == "True":
        cache[key] = data
        time.sleep(SLEEP_BETWEEN_CALLS)
        return data

    # detect rate-limit / invalid key
    err = (data.get("Error") or "").lower()
    if "limit" in err or "invalid api key" in err or "rate limited" in err:
        OMDB_RATE_LIMITED = True
        # do NOT cache this sentinel under the key
        print(f"[omdb] rate-limited/invalid key: {data.get('Error')}")
        return {"Response": "False", "Error": data.get("Error")}

    # fallback: search and try to pick best match
    search_params = {"apikey": OMDB_API_KEY, "s": title, "type": "movie"}
    if year:
        search_params["y"] = str(year)
    search = _call_omdb(search_params)
    if search.get("Response") == "True" and search.get("Search"):
        best = _choose_best_search_result(title, search.get("Search"))
        imdb = best.get("imdbID") if best else None
        if imdb:
            detail = _call_omdb({"apikey": OMDB_API_KEY, "i": imdb})
            if detail.get("Response") == "True":
                cache[key] = detail
                time.sleep(SLEEP_BETWEEN_CALLS)
                return detail

    # final fallback: cache original failure (from initial t= call)
    cache[key] = data
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


def print_progress(current, total, start_time, extra=""):
    elapsed = time.time() - start_time
    rate = current / elapsed if elapsed > 0 else 0
    eta = (total - current) / rate if rate > 0 else float('inf')
    print(f"[{current}/{total}] elapsed={elapsed:.1f}s rate={rate:.2f} rows/s eta={eta:.1f}s {extra}")


def _chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(size):
                chunk.append(next(it))
        except StopIteration:
            if chunk:
                yield chunk
            break
        yield chunk


def main(limit=None, fast=False, verbose=False):
    t0 = time.time()
    print("Starting ETL...", f"fast_mode={fast}", f"limit={limit}")
    if not MOVIES_CSV.exists() or not RATINGS_CSV.exists():
        raise FileNotFoundError("Place movies.csv and ratings.csv in the project folder before running.")

    cache = load_cache(OMDB_CACHE_FILE)
    movies_df = pd.read_csv(MOVIES_CSV)
    ratings_df = pd.read_csv(RATINGS_CSV)
    print(f"Loaded CSVs: movies={len(movies_df)} ratings={len(ratings_df)} rows")

    if limit:
        movies_df = movies_df.head(limit)
    total_movies = len(movies_df)

    engine = create_engine(DB_URL, future=True)

    # apply schema if present
    schema_path = Path("schema.sql")
    if schema_path.exists():
        with engine.begin() as conn:
            sql_text = schema_path.read_text()
            statements = [s.strip() for s in sql_text.split(';') if s.strip()]
            for stmt in statements:
                conn.exec_driver_sql(stmt)

    # upsert movies + genres
    movie_start = time.time()
    with engine.begin() as conn:
        processed = 0
        last_report = time.time()
        for idx, row in movies_df.iterrows():
            processed += 1
            movie_id = int(row["movieId"])
            raw_title = row["title"]
            genres_raw = row.get("genres", "")
            title, year = parse_title_and_year(raw_title)

            omdb_raw = query_omdb(movie_id, title, year=year, cache=cache, fast=fast)
            omdb = clean_omdb_response(omdb_raw) if omdb_raw else None

            imdb_id = omdb.get("imdb_id") if omdb else None
            director = omdb.get("director") if omdb else None
            plot = omdb.get("plot") if omdb else None
            box_office = omdb.get("box_office") if omdb else None
            released = omdb.get("released") if omdb else None
            runtime = omdb.get("runtime_minutes") if omdb else None

            upsert_sql = text("""
                INSERT INTO movies (id, title, year, imdb_id, director, plot, box_office, released, runtime_minutes)
                VALUES (:id, :title, :year, :imdb_id, :director, :plot, :box_office, :released, :runtime)
                ON CONFLICT(id) DO UPDATE SET
                  title=excluded.title,
                  year=excluded.year,
                  imdb_id=COALESCE(excluded.imdb_id, movies.imdb_id),
                  director=COALESCE(excluded.director, movies.director),
                  plot=COALESCE(excluded.plot, movies.plot),
                  box_office=COALESCE(excluded.box_office, movies.box_office),
                  released=COALESCE(excluded.released, movies.released),
                  runtime_minutes=COALESCE(excluded.runtime_minutes, movies.runtime_minutes)
            """)

            params = {
                "id": movie_id,
                "title": title,
                "year": int(year) if year is not None else None,
                "imdb_id": imdb_id,
                "director": director,
                "plot": plot,
                "box_office": box_office,
                "released": released,
                "runtime": runtime
            }

            try:
                conn.execute(upsert_sql, params)
            except IntegrityError as e:
                # likely imdb_id unique collision; retry without imdb
                msg = str(e).lower()
                if "imdb_id" in msg or "unique constraint failed: movies.imdb_id" in msg:
                    params_no_imdb = params.copy()
                    params_no_imdb["imdb_id"] = None
                    fallback_sql = text("""
                        INSERT INTO movies (id, title, year, imdb_id, director, plot, box_office, released, runtime_minutes)
                        VALUES (:id, :title, :year, :imdb_id, :director, :plot, :box_office, :released, :runtime)
                        ON CONFLICT(id) DO UPDATE SET
                          title=excluded.title,
                          year=excluded.year,
                          director=COALESCE(excluded.director, movies.director),
                          plot=COALESCE(excluded.plot, movies.plot),
                          box_office=COALESCE(excluded.box_office, movies.box_office),
                          released=COALESCE(excluded.released, movies.released),
                          runtime_minutes=COALESCE(excluded.runtime_minutes, movies.runtime_minutes)
                    """)
                    conn.execute(fallback_sql, params_no_imdb)
                else:
                    raise

            # genres
            genre_list = [g for g in str(genres_raw).split("|") if g and g != "(no genres listed)"]
            for g in genre_list:
                conn.execute(text("INSERT OR IGNORE INTO genres (name) VALUES (:name)"), {"name": g})
                res = conn.execute(text("SELECT id FROM genres WHERE name = :name"), {"name": g})
                gid = res.scalar_one_or_none()
                if gid:
                    conn.execute(text("INSERT OR IGNORE INTO movie_genres (movie_id, genre_id) VALUES (:movie_id, :genre_id)"),
                                 {"movie_id": movie_id, "genre_id": gid})

            now = time.time()
            if verbose or (now - last_report >= 5) or (processed % 200 == 0) or processed == total_movies:
                print_progress(processed, total_movies, movie_start, extra=f"title='{str(title)[:30]}'")
                last_report = now

            if processed % CACHE_SAVE_EVERY == 0:
                save_cache(cache, OMDB_CACHE_FILE)

    # final cache save
    save_cache(cache, OMDB_CACHE_FILE)
    movie_time = time.time() - movie_start
    print(f"Movies processed: {total_movies} (took {movie_time:.2f}s)")

    # -------------------------
    # Insert ratings efficiently in chunks
    # -------------------------
    ratings_start = time.time()

    # find valid movie ids
    with engine.begin() as conn:
        res = conn.execute(text("SELECT id FROM movies"))
        existing_ids = {row[0] for row in res.fetchall()}

    processed_ids = set(movies_df["movieId"].astype(int).tolist())
    valid_movie_ids = existing_ids.union(processed_ids)

    original_ratings_count = len(ratings_df)
    ratings_df_filtered = ratings_df[ratings_df["movieId"].isin(valid_movie_ids)].copy()
    filtered_count = len(ratings_df_filtered)
    dropped = original_ratings_count - filtered_count
    if dropped > 0:
        print(f"Filtered ratings: dropping {dropped} ratings that reference movies not present in DB (safe for --limit)")

    # prepare tuples for bulk insert
    to_insert = []
    for r in ratings_df_filtered.itertuples(index=False):
        ts = None
        if hasattr(r, 'timestamp') and not pd.isna(getattr(r, 'timestamp')):
            try:
                ts = int(getattr(r, 'timestamp'))
            except Exception:
                ts = None
        to_insert.append({
            "user_id": int(r.userId),
            "movie_id": int(r.movieId),
            "rating": float(r.rating),
            "timestamp": ts
        })

    inserted = 0
    insert_sql = text("""
        INSERT OR IGNORE INTO ratings (user_id, movie_id, rating, timestamp)
        VALUES (:user_id, :movie_id, :rating, :timestamp)
    """)

    with engine.begin() as conn:
        for chunk in _chunked_iterable(to_insert, RATINGS_CHUNK):
            conn.execute(insert_sql, chunk)
            inserted += len(chunk)
            if verbose:
                print(f"Inserted ~{inserted} ratings...")

    ratings_time = time.time() - ratings_start
    total_time = time.time() - t0
    print(f"Ratings inserted (attempted): {inserted} (took {ratings_time:.2f}s)")
    print(f"ETL finished. Total time: {total_time:.2f}s. DB at: {DB_URL}")

    # cleanup
    try:
        _session.close()
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Process only first N movies (for testing)")
    parser.add_argument("--fast", action="store_true", help="Skip OMDb API calls (fast local runs)")
    parser.add_argument("--verbose", action="store_true", help="Verbose progress output")
    args = parser.parse_args()
    main(limit=args.limit, fast=args.fast, verbose=args.verbose)
