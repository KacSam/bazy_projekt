import argparse
import ast
import glob
import math
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import kagglehub
import pandas as pd
import pymongo
import psycopg2
import pymysql


@dataclass
class Config:
    kaggle_dataset: str = "rodolfofigueroa/spotify-12m-songs"
    row_limit: int = 50000
    csv_path: str | None = None


def pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        key = candidate.lower()
        if key in lower_map:
            return lower_map[key]
    return None


def parse_list_field(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    s = str(value).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return [str(v).strip().strip("\"").strip("'") for v in parsed if str(v).strip()]
        except Exception:
            pass
    if "|" in s:
        return [p.strip() for p in s.split("|") if p.strip()]
    if ";" in s:
        return [p.strip() for p in s.split(";") if p.strip()]
    if "," in s:
        return [p.strip() for p in s.split(",") if p.strip()]
    return [s]


def as_bool(value: Any) -> bool | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False
    return None


def to_number(value: Any, cast_type=float) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        return cast_type(value)
    except Exception:
        return None


def to_date(value: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in ["%Y-%m-%d", "%Y-%m", "%Y"]:
        try:
            parsed = datetime.strptime(s, fmt)
            if fmt == "%Y":
                return f"{parsed.year}-01-01"
            if fmt == "%Y-%m":
                return f"{parsed.year}-{parsed.month:02d}-01"
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def infer_popularity_bucket(popularity: int | None) -> int:
    if popularity is None:
        return -1
    if popularity < 20:
        return 0
    if popularity < 40:
        return 1
    if popularity < 60:
        return 2
    if popularity < 80:
        return 3
    return 4


def discover_csv(dataset_id: str) -> str:
    dataset_dir = kagglehub.dataset_download(dataset_id)
    csv_files = glob.glob(os.path.join(dataset_dir, "**", "*.csv"), recursive=True)
    if not csv_files:
        raise RuntimeError(f"Nie znaleziono pliku CSV w dataset: {dataset_id}")
    # Select the largest CSV to maximize chance this is the primary data file.
    csv_files.sort(key=lambda p: os.path.getsize(p), reverse=True)
    return csv_files[0]


def load_dataframe(dataset_id: str, row_limit: int, csv_path: str | None = None) -> pd.DataFrame:
    if csv_path is None:
        csv_path = discover_csv(dataset_id)
    print(f"[INFO] CSV source: {csv_path}")
    df = pd.read_csv(csv_path, nrows=row_limit)
    if df.empty:
        raise RuntimeError("Wczytany dataset jest pusty.")
    print(f"[INFO] Loaded rows: {len(df)}")
    print(f"[INFO] Columns: {', '.join(df.columns)}")
    return df


def normalize_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    col_track_id = pick_column(df, ["spotify_track_id", "track_id", "id", "uri"])
    col_track_name = pick_column(df, ["track_name", "name", "song", "song_name", "title"])
    col_duration = pick_column(df, ["duration_ms", "duration"])
    col_explicit = pick_column(df, ["explicit"])
    col_popularity = pick_column(df, ["popularity"])
    col_album_name = pick_column(df, ["album_name", "album", "album_title"])
    col_release_date = pick_column(df, ["release_date", "album_release_date", "year"])
    col_artists = pick_column(df, ["artists", "artist_name", "artist", "artist_names"])
    col_artist_ids = pick_column(df, ["artist_ids", "id_artists", "spotify_artist_id"])
    col_genres = pick_column(df, ["genres", "genre"])

    col_danceability = pick_column(df, ["danceability"])
    col_energy = pick_column(df, ["energy"])
    col_key = pick_column(df, ["key"])
    col_loudness = pick_column(df, ["loudness"])
    col_mode = pick_column(df, ["mode"])
    col_speechiness = pick_column(df, ["speechiness"])
    col_acousticness = pick_column(df, ["acousticness"])
    col_instrumentalness = pick_column(df, ["instrumentalness"])
    col_liveness = pick_column(df, ["liveness"])
    col_valence = pick_column(df, ["valence"])
    col_tempo = pick_column(df, ["tempo"])
    col_time_signature = pick_column(df, ["time_signature"])

    if not col_track_id:
        raise RuntimeError("Brak kolumny identyfikatora utworu (track_id/id).")

    records: list[dict[str, Any]] = []
    for i, row in df.iterrows():
        raw_track_id = row.get(col_track_id)
        if pd.isna(raw_track_id):
            continue
        track_id = str(raw_track_id).strip()
        if not track_id:
            continue

        track_name = str(row.get(col_track_name)).strip() if col_track_name and not pd.isna(row.get(col_track_name)) else "UNKNOWN_TRACK"
        album_name = str(row.get(col_album_name)).strip() if col_album_name and not pd.isna(row.get(col_album_name)) else "UNKNOWN_ALBUM"

        artists = parse_list_field(row.get(col_artists)) if col_artists else []
        artist_ids = parse_list_field(row.get(col_artist_ids)) if col_artist_ids else []
        if not artists:
            artists = ["UNKNOWN_ARTIST"]

        if artist_ids and len(artist_ids) != len(artists):
            # Align lengths to avoid index errors.
            if len(artist_ids) < len(artists):
                artist_ids.extend([""] * (len(artists) - len(artist_ids)))
            else:
                artist_ids = artist_ids[: len(artists)]

        artist_pairs = []
        for idx, artist_name in enumerate(artists):
            aid = artist_ids[idx] if idx < len(artist_ids) else ""
            artist_pairs.append(
                {
                    "artist_name": artist_name,
                    "spotify_artist_id": aid or None,
                }
            )

        genres = parse_list_field(row.get(col_genres)) if col_genres else []

        rec = {
            "spotify_track_id": track_id,
            "track_name": track_name,
            "duration_ms": to_number(row.get(col_duration), int) if col_duration else None,
            "explicit": as_bool(row.get(col_explicit)) if col_explicit else None,
            "popularity": to_number(row.get(col_popularity), int) if col_popularity else None,
            "album_name": album_name,
            "release_date": to_date(row.get(col_release_date)) if col_release_date else None,
            "artists": artist_pairs,
            "genres": genres,
            "audio_features": {
                "danceability": to_number(row.get(col_danceability), float) if col_danceability else None,
                "energy": to_number(row.get(col_energy), float) if col_energy else None,
                "key": to_number(row.get(col_key), int) if col_key else None,
                "loudness": to_number(row.get(col_loudness), float) if col_loudness else None,
                "mode": to_number(row.get(col_mode), int) if col_mode else None,
                "speechiness": to_number(row.get(col_speechiness), float) if col_speechiness else None,
                "acousticness": to_number(row.get(col_acousticness), float) if col_acousticness else None,
                "instrumentalness": to_number(row.get(col_instrumentalness), float) if col_instrumentalness else None,
                "liveness": to_number(row.get(col_liveness), float) if col_liveness else None,
                "valence": to_number(row.get(col_valence), float) if col_valence else None,
                "tempo": to_number(row.get(col_tempo), float) if col_tempo else None,
                "time_signature": to_number(row.get(col_time_signature), int) if col_time_signature else None,
            },
        }
        records.append(rec)

        if i > 0 and i % 50000 == 0:
            print(f"[INFO] Normalized rows: {i}")

    # Deduplicate by spotify_track_id.
    dedup = {r["spotify_track_id"]: r for r in records}
    normalized = list(dedup.values())
    print(f"[INFO] Normalized unique tracks: {len(normalized)}")
    return normalized


def read_sql_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def split_sql_statements(sql: str) -> list[str]:
    return [stmt.strip() for stmt in sql.split(";") if stmt.strip()]


def load_postgres(records: list[dict[str, Any]], root_dir: str) -> None:
    print("[INFO] Loading PostgreSQL...")
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "55432")),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
        dbname=os.getenv("POSTGRES_DB", "spotify_db"),
    )
    conn.autocommit = False

    schema_path = os.path.join(root_dir, "sql", "postgres_schema.sql")
    statements = split_sql_statements(read_sql_file(schema_path))

    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)

        album_map: dict[str, int] = {}
        artist_map: dict[tuple[str, str | None], int] = {}
        genre_map: dict[str, int] = {}

        for rec in records:
            if rec["album_name"] not in album_map:
                cur.execute(
                    """
                    INSERT INTO albums (album_name, release_date, total_tracks, album_type)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING album_id
                    """,
                    (rec["album_name"], rec["release_date"], None, None),
                )
                row = cur.fetchone()
                if row:
                    album_id = row[0]
                    album_map[rec["album_name"]] = album_id
                else:
                    cur.execute("SELECT album_id FROM albums WHERE album_name = %s LIMIT 1", (rec["album_name"],))
                    album_map[rec["album_name"]] = cur.fetchone()[0]

            for ap in rec["artists"]:
                key = (ap["artist_name"], ap["spotify_artist_id"])
                if key in artist_map:
                    continue
                cur.execute(
                    """
                    INSERT INTO artists (artist_name, spotify_artist_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING artist_id
                    """,
                    key,
                )
                arow = cur.fetchone()
                if arow:
                    artist_map[key] = arow[0]
                else:
                    if key[1] is not None:
                        cur.execute(
                            "SELECT artist_id FROM artists WHERE spotify_artist_id = %s LIMIT 1",
                            (key[1],),
                        )
                        x = cur.fetchone()
                        if x:
                            artist_map[key] = x[0]
                    if key not in artist_map:
                        cur.execute(
                            "SELECT artist_id FROM artists WHERE artist_name = %s LIMIT 1",
                            (key[0],),
                        )
                        artist_map[key] = cur.fetchone()[0]

            for g in rec["genres"]:
                if g not in genre_map:
                    cur.execute(
                        """
                        INSERT INTO genres (genre_name)
                        VALUES (%s)
                        ON CONFLICT DO NOTHING
                        RETURNING genre_id
                        """,
                        (g,),
                    )
                    grow = cur.fetchone()
                    if grow:
                        genre_map[g] = grow[0]
                    else:
                        cur.execute("SELECT genre_id FROM genres WHERE genre_name = %s LIMIT 1", (g,))
                        genre_map[g] = cur.fetchone()[0]

        track_map: dict[str, int] = {}
        for rec in records:
            album_id = album_map.get(rec["album_name"])
            cur.execute(
                """
                INSERT INTO tracks (spotify_track_id, track_name, duration_ms, explicit, popularity, album_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (spotify_track_id) DO NOTHING
                RETURNING track_id
                """,
                (
                    rec["spotify_track_id"],
                    rec["track_name"],
                    rec["duration_ms"],
                    rec["explicit"],
                    rec["popularity"],
                    album_id,
                ),
            )
            row = cur.fetchone()
            if row:
                track_map[rec["spotify_track_id"]] = row[0]
            else:
                cur.execute(
                    "SELECT track_id FROM tracks WHERE spotify_track_id = %s",
                    (rec["spotify_track_id"],),
                )
                track_map[rec["spotify_track_id"]] = cur.fetchone()[0]

            af = rec["audio_features"]
            cur.execute(
                """
                INSERT INTO track_audio_features
                (track_id, danceability, energy, key, loudness, mode, speechiness, acousticness,
                 instrumentalness, liveness, valence, tempo, time_signature)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (track_id) DO NOTHING
                """,
                (
                    track_map[rec["spotify_track_id"]],
                    af["danceability"],
                    af["energy"],
                    af["key"],
                    af["loudness"],
                    af["mode"],
                    af["speechiness"],
                    af["acousticness"],
                    af["instrumentalness"],
                    af["liveness"],
                    af["valence"],
                    af["tempo"],
                    af["time_signature"],
                ),
            )

            for ap in rec["artists"]:
                akey = (ap["artist_name"], ap["spotify_artist_id"])
                artist_id = artist_map.get(akey)
                if artist_id is None:
                    continue
                cur.execute(
                    "INSERT INTO track_artists (track_id, artist_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (track_map[rec["spotify_track_id"]], artist_id),
                )
                cur.execute(
                    "INSERT INTO album_artists (album_id, artist_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (album_id, artist_id),
                )

            for g in rec["genres"]:
                gid = genre_map.get(g)
                if gid is None:
                    continue
                cur.execute(
                    "INSERT INTO track_genres (track_id, genre_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (track_map[rec["spotify_track_id"]], gid),
                )

        conn.commit()

    conn.close()
    print("[INFO] PostgreSQL done.")


def load_mariadb(records: list[dict[str, Any]], root_dir: str) -> None:
    print("[INFO] Loading MariaDB...")
    conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "admin"),
        password=os.getenv("MYSQL_PASSWORD", "password"),
        database=os.getenv("MYSQL_DATABASE", "spotify_db"),
        autocommit=False,
    )

    schema_path = os.path.join(root_dir, "sql", "mariadb_schema.sql")
    statements = split_sql_statements(read_sql_file(schema_path))

    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)

        album_map: dict[str, int] = {}
        artist_map: dict[tuple[str, str | None], int] = {}
        genre_map: dict[str, int] = {}

        for rec in records:
            if rec["album_name"] not in album_map:
                cur.execute(
                    "INSERT IGNORE INTO albums (album_name, release_date, total_tracks, album_type) VALUES (%s, %s, %s, %s)",
                    (rec["album_name"], rec["release_date"], None, None),
                )
                cur.execute("SELECT album_id FROM albums WHERE album_name = %s LIMIT 1", (rec["album_name"],))
                album_id = cur.fetchone()[0]
                album_map[rec["album_name"]] = album_id

            for ap in rec["artists"]:
                key = (ap["artist_name"], ap["spotify_artist_id"])
                if key in artist_map:
                    continue
                cur.execute(
                    "INSERT IGNORE INTO artists (artist_name, spotify_artist_id) VALUES (%s, %s)",
                    key,
                )
                if key[1] is not None:
                    cur.execute("SELECT artist_id FROM artists WHERE spotify_artist_id = %s LIMIT 1", (key[1],))
                    row = cur.fetchone()
                    if row:
                        artist_map[key] = row[0]
                        continue
                cur.execute("SELECT artist_id FROM artists WHERE artist_name = %s LIMIT 1", (key[0],))
                artist_map[key] = cur.fetchone()[0]

            for g in rec["genres"]:
                if g not in genre_map:
                    cur.execute("INSERT IGNORE INTO genres (genre_name) VALUES (%s)", (g,))
                    cur.execute("SELECT genre_id FROM genres WHERE genre_name = %s LIMIT 1", (g,))
                    genre_map[g] = cur.fetchone()[0]

        track_map: dict[str, int] = {}
        for rec in records:
            album_id = album_map.get(rec["album_name"])
            cur.execute(
                """
                INSERT IGNORE INTO tracks (spotify_track_id, track_name, duration_ms, explicit, popularity, album_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    rec["spotify_track_id"],
                    rec["track_name"],
                    rec["duration_ms"],
                    rec["explicit"],
                    rec["popularity"],
                    album_id,
                ),
            )
            cur.execute("SELECT track_id FROM tracks WHERE spotify_track_id = %s LIMIT 1", (rec["spotify_track_id"],))
            track_id = cur.fetchone()[0]
            track_map[rec["spotify_track_id"]] = track_id

            af = rec["audio_features"]
            cur.execute(
                """
                INSERT IGNORE INTO track_audio_features
                (track_id, danceability, energy, `key`, loudness, `mode`, speechiness, acousticness,
                 instrumentalness, liveness, valence, tempo, time_signature)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    track_id,
                    af["danceability"],
                    af["energy"],
                    af["key"],
                    af["loudness"],
                    af["mode"],
                    af["speechiness"],
                    af["acousticness"],
                    af["instrumentalness"],
                    af["liveness"],
                    af["valence"],
                    af["tempo"],
                    af["time_signature"],
                ),
            )

            for ap in rec["artists"]:
                akey = (ap["artist_name"], ap["spotify_artist_id"])
                artist_id = artist_map.get(akey)
                if artist_id is None:
                    continue
                cur.execute("INSERT IGNORE INTO track_artists (track_id, artist_id) VALUES (%s, %s)", (track_id, artist_id))
                cur.execute("INSERT IGNORE INTO album_artists (album_id, artist_id) VALUES (%s, %s)", (album_id, artist_id))

            for g in rec["genres"]:
                gid = genre_map.get(g)
                if gid is None:
                    continue
                cur.execute("INSERT IGNORE INTO track_genres (track_id, genre_id) VALUES (%s, %s)", (track_id, gid))

        conn.commit()

    conn.close()
    print("[INFO] MariaDB done.")


def load_mongodb(records: list[dict[str, Any]]) -> None:
    print("[INFO] Loading MongoDB...")
    mongo_user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
    mongo_pass = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "password")
    mongo_host = os.getenv("MONGO_HOST", "localhost")
    mongo_port = int(os.getenv("MONGO_PORT", "27017"))
    mongo_db = os.getenv("MONGO_DB", "spotify_db")
    uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/admin"
    last_error: Exception | None = None
    client = None
    for _ in range(12):
        try:
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            break
        except Exception as exc:
            last_error = exc
            time.sleep(5)
    if client is None:
        raise RuntimeError(f"Nie można połączyć z MongoDB pod URI {uri}: {last_error}")

    db = client[mongo_db]

    songs_col = db["songs"]
    artists_col = db["artists"]
    albums_col = db["albums"]

    songs_col.create_index("spotify_track_id", unique=True)
    songs_col.create_index("popularity")
    songs_col.create_index("track_name")
    songs_col.create_index("artists.artist_name")
    songs_col.create_index("audio_features.tempo")

    artist_docs: dict[tuple[str, str | None], dict[str, Any]] = {}
    album_docs: dict[str, dict[str, Any]] = {}

    song_ops = []
    for rec in records:
        for ap in rec["artists"]:
            key = (ap["artist_name"], ap["spotify_artist_id"])
            artist_docs[key] = {
                "artist_name": ap["artist_name"],
                "spotify_artist_id": ap["spotify_artist_id"],
            }

        album_docs[rec["album_name"]] = {
            "album_name": rec["album_name"],
            "release_date": rec["release_date"],
            "artists": [ap["artist_name"] for ap in rec["artists"]],
        }

        doc = {
            "spotify_track_id": rec["spotify_track_id"],
            "track_name": rec["track_name"],
            "duration_ms": rec["duration_ms"],
            "explicit": rec["explicit"],
            "popularity": rec["popularity"],
            "album": {
                "album_name": rec["album_name"],
                "release_date": rec["release_date"],
            },
            "artists": rec["artists"],
            "genres": rec["genres"],
            "audio_features": rec["audio_features"],
        }
        song_ops.append(
            pymongo.UpdateOne(
                {"spotify_track_id": rec["spotify_track_id"]},
                {"$set": doc},
                upsert=True,
            )
        )

    if song_ops:
        songs_col.bulk_write(song_ops, ordered=False)

    if artist_docs:
        artists_col.delete_many({})
        artists_col.insert_many(list(artist_docs.values()))

    if album_docs:
        albums_col.delete_many({})
        albums_col.insert_many(list(album_docs.values()))

    print("[INFO] MongoDB done.")


def load_cassandra(records: list[dict[str, Any]], root_dir: str) -> None:
    print("[INFO] Loading Cassandra...")
    def cql_escape(value: str) -> str:
        return value.replace("'", "''")

    def cql_text(value: Any) -> str:
        if value is None:
            return "null"
        return f"'{cql_escape(str(value))}'"

    def cql_int(value: Any) -> str:
        return "null" if value is None else str(int(value))

    def cql_float(value: Any) -> str:
        return "null" if value is None else str(float(value))

    def cql_bool(value: Any) -> str:
        if value is None:
            return "null"
        return "true" if bool(value) else "false"

    def cql_list(values: list[str]) -> str:
        escaped = [f"'{cql_escape(v)}'" for v in values]
        return "[" + ", ".join(escaped) + "]"

    container_id = subprocess.check_output(
        ["docker", "compose", "ps", "-q", "cassandra"],
        cwd=root_dir,
        text=True,
    ).strip()
    if not container_id:
        raise RuntimeError("Nie znaleziono uruchomionego kontenera usługi cassandra.")

    cql_lines: list[str] = []
    cql_lines.append(read_sql_file(os.path.join(root_dir, "cql", "cassandra_schema.cql")))
    cql_lines.append("USE spotify_db;")

    for rec in records:
        popularity = rec["popularity"] if rec["popularity"] is not None else -1
        p_bucket = infer_popularity_bucket(rec["popularity"])
        artists = [a["artist_name"] for a in rec["artists"]] or ["UNKNOWN_ARTIST"]
        genres = rec["genres"] or ["UNKNOWN_GENRE"]

        cql_lines.append(
            "INSERT INTO songs_by_track_id (track_id, track_name, album_name, release_date, duration_ms, popularity, explicit, tempo, energy, artists, genres) "
            f"VALUES ({cql_text(rec['spotify_track_id'])}, {cql_text(rec['track_name'])}, {cql_text(rec['album_name'])}, {cql_text(rec['release_date'] or '')}, "
            f"{cql_int(rec['duration_ms'])}, {cql_int(popularity)}, {cql_bool(rec['explicit'])}, {cql_float(rec['audio_features']['tempo'])}, "
            f"{cql_float(rec['audio_features']['energy'])}, {cql_list(artists)}, {cql_list(genres)});"
        )

        cql_lines.append(
            "INSERT INTO songs_by_popularity (popularity_bucket, popularity, track_id, track_name, album_name) "
            f"VALUES ({cql_int(p_bucket)}, {cql_int(popularity)}, {cql_text(rec['spotify_track_id'])}, {cql_text(rec['track_name'])}, {cql_text(rec['album_name'])});"
        )

        for ap in rec["artists"]:
            artist_id = ap["spotify_artist_id"] or ap["artist_name"]
            cql_lines.append(
                "INSERT INTO songs_by_artist (artist_id, track_id, artist_name, track_name, album_name, release_date, popularity) "
                f"VALUES ({cql_text(artist_id)}, {cql_text(rec['spotify_track_id'])}, {cql_text(ap['artist_name'])}, {cql_text(rec['track_name'])}, "
                f"{cql_text(rec['album_name'])}, {cql_text(rec['release_date'] or '')}, {cql_int(popularity)});"
            )

        for g in genres:
            cql_lines.append(
                "INSERT INTO songs_by_genre (genre, popularity, track_id, track_name, album_name) "
                f"VALUES ({cql_text(g)}, {cql_int(popularity)}, {cql_text(rec['spotify_track_id'])}, {cql_text(rec['track_name'])}, {cql_text(rec['album_name'])});"
            )

    cql_payload = "\n".join(cql_lines)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cql", delete=False, encoding="utf-8") as tmp:
        tmp.write(cql_payload)
        tmp_path = tmp.name

    try:
        subprocess.run(["docker", "cp", tmp_path, f"{container_id}:/tmp/load_spotify.cql"], check=True)
        subprocess.run(["docker", "exec", container_id, "cqlsh", "-f", "/tmp/load_spotify.cql"], check=True)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    print("[INFO] Cassandra done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Spotify Kaggle data to PostgreSQL, MariaDB, MongoDB and Cassandra")
    parser.add_argument("--rows", type=int, default=int(os.getenv("SPOTIFY_LOAD_LIMIT", "50000")))
    parser.add_argument("--csv", default=os.getenv("SPOTIFY_CSV_PATH"), help="Optional local CSV path; skips Kaggle download")
    parser.add_argument("--skip-postgres", action="store_true")
    parser.add_argument("--skip-mariadb", action="store_true")
    parser.add_argument("--skip-mongodb", action="store_true")
    parser.add_argument("--skip-cassandra", action="store_true")
    args = parser.parse_args()

    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    cfg = Config(row_limit=args.rows, csv_path=args.csv)

    df = load_dataframe(cfg.kaggle_dataset, cfg.row_limit, cfg.csv_path)
    records = normalize_records(df)

    if not args.skip_postgres:
        load_postgres(records, root_dir)
    if not args.skip_mariadb:
        load_mariadb(records, root_dir)
    if not args.skip_mongodb:
        load_mongodb(records)
    if not args.skip_cassandra:
        load_cassandra(records, root_dir)

    print("[INFO] Data load completed.")


if __name__ == "__main__":
    main()
