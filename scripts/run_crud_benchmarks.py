import argparse
import csv
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import psycopg2
import pymongo
import pymysql
from cassandra.cluster import Cluster


@dataclass
class BenchContext:
    size: int
    batch_size: int
    read_limit: int
    seed: int


def now_utc() -> datetime:
    return datetime.utcnow()


def elapsed_ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000.0


def popularity_bucket(popularity: int) -> int:
    if popularity < 20:
        return 0
    if popularity < 40:
        return 1
    if popularity < 60:
        return 2
    if popularity < 80:
        return 3
    return 4


def chunked(items: Iterable[Any], size: int) -> Iterable[list[Any]]:
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def make_bench_id(idx: int) -> str:
    return f"bench_{idx}"


def make_temp_prefix(label: str) -> str:
    return f"tmp_{label}_{uuid.uuid4().hex[:8]}"


def build_record(bench_id: str, idx: int, seed: int) -> dict[str, Any]:
    rng = random.Random(seed + idx)
    return {
        "bench_id": bench_id,
        "track_name": f"Track {bench_id}",
        "popularity": rng.randint(0, 100),
        "duration_ms": rng.randint(60_000, 320_000),
        "explicit": rng.choice([True, False]),
        "tempo": round(rng.uniform(60.0, 200.0), 3),
        "created_at": now_utc(),
        "updated_at": now_utc(),
        "is_deleted": False,
    }


def pick_ids(size: int, count: int, seed: int, salt: int) -> list[str]:
    count = min(count, max(size, 1))
    rng = random.Random(seed + salt)
    return [make_bench_id(i) for i in rng.sample(range(size), count)]


def pick_cassandra_connection_class() -> Any | None:
    if sys.version_info < (3, 12):
        return None
    try:
        from cassandra.io.twistedreactor import TwistedConnection

        return TwistedConnection
    except Exception:
        return None


class BaseAdapter:
    name: str = "base"

    def setup(self) -> None:
        raise NotImplementedError

    def seed(self, ctx: BenchContext) -> None:
        raise NotImplementedError

    def create_single(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        raise NotImplementedError

    def create_batch(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        raise NotImplementedError

    def create_validated(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        raise NotImplementedError

    def read_by_id(self, ctx: BenchContext) -> tuple[float, int]:
        raise NotImplementedError

    def read_filter(self, ctx: BenchContext) -> tuple[float, int]:
        raise NotImplementedError

    def read_aggregate(self, ctx: BenchContext) -> tuple[float, int]:
        raise NotImplementedError

    def update_single(self, ctx: BenchContext) -> tuple[float, int]:
        raise NotImplementedError

    def update_multi(self, ctx: BenchContext) -> tuple[float, int]:
        raise NotImplementedError

    def update_batch(self, ctx: BenchContext) -> tuple[float, int]:
        raise NotImplementedError

    def delete_by_id(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        raise NotImplementedError

    def delete_by_condition(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        raise NotImplementedError

    def soft_delete(self, ctx: BenchContext) -> tuple[float, int]:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class PostgresAdapter(BaseAdapter):
    name = "postgres"

    def __init__(self) -> None:
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
            dbname=os.getenv("POSTGRES_DB", "spotify_db"),
        )
        self.conn.autocommit = True

    def setup(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bench_tracks (
                    bench_id TEXT PRIMARY KEY,
                    track_name TEXT NOT NULL,
                    popularity INT,
                    duration_ms INT,
                    explicit BOOLEAN,
                    tempo DOUBLE PRECISION,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    is_deleted BOOLEAN DEFAULT FALSE
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_bench_popularity ON bench_tracks(popularity)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_bench_is_deleted ON bench_tracks(is_deleted)")

    def seed(self, ctx: BenchContext) -> None:
        with self.conn.cursor() as cur:
            for batch in chunked(range(ctx.size), ctx.batch_size):
                rows = []
                for idx in batch:
                    rec = build_record(make_bench_id(idx), idx, ctx.seed)
                    rows.append(
                        (
                            rec["bench_id"],
                            rec["track_name"],
                            rec["popularity"],
                            rec["duration_ms"],
                            rec["explicit"],
                            rec["tempo"],
                            rec["created_at"],
                            rec["updated_at"],
                            rec["is_deleted"],
                        )
                    )
                cur.executemany(
                    """
                    INSERT INTO bench_tracks
                    (bench_id, track_name, popularity, duration_ms, explicit, tempo, created_at, updated_at, is_deleted)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (bench_id) DO NOTHING
                    """,
                    rows,
                )

    def _insert_records(self, records: list[dict[str, Any]]) -> None:
        with self.conn.cursor() as cur:
            rows = [
                (
                    r["bench_id"],
                    r["track_name"],
                    r["popularity"],
                    r["duration_ms"],
                    r["explicit"],
                    r["tempo"],
                    r["created_at"],
                    r["updated_at"],
                    r["is_deleted"],
                )
                for r in records
            ]
            cur.executemany(
                """
                INSERT INTO bench_tracks
                (bench_id, track_name, popularity, duration_ms, explicit, tempo, created_at, updated_at, is_deleted)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (bench_id) DO NOTHING
                """,
                rows,
            )

    def _delete_ids(self, ids: list[str]) -> None:
        if not ids:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM bench_tracks WHERE bench_id = ANY(%s)",
                (ids,),
            )

    def create_single(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        bench_id = f"{prefix}_0"
        record = build_record(bench_id, ctx.size + 1, ctx.seed)
        start = time.perf_counter()
        self._insert_records([record])
        elapsed = elapsed_ms(start)
        self._delete_ids([bench_id])
        return elapsed, 1

    def create_batch(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        count = ctx.batch_size
        records = [build_record(f"{prefix}_{i}", ctx.size + i, ctx.seed) for i in range(count)]
        start = time.perf_counter()
        self._insert_records(records)
        elapsed = elapsed_ms(start)
        self._delete_ids([r["bench_id"] for r in records])
        return elapsed, count

    def create_validated(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        record = build_record(f"{prefix}_0", ctx.size + 2, ctx.seed)

        def is_valid(rec: dict[str, Any]) -> bool:
            if not rec["bench_id"] or not rec["track_name"]:
                return False
            if rec["popularity"] is None or not 0 <= rec["popularity"] <= 100:
                return False
            if rec["duration_ms"] is None or rec["duration_ms"] <= 0:
                return False
            return True

        start = time.perf_counter()
        if is_valid(record):
            self._insert_records([record])
        elapsed = elapsed_ms(start)
        self._delete_ids([record["bench_id"]])
        return elapsed, 1

    def read_by_id(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 2)
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute("SELECT * FROM bench_tracks WHERE bench_id = %s", (bench_id,))
            cur.fetchone()
            elapsed = elapsed_ms(start)
        return elapsed, 1

    def read_filter(self, ctx: BenchContext) -> tuple[float, int]:
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                "SELECT * FROM bench_tracks WHERE popularity >= %s LIMIT %s",
                (80, ctx.read_limit),
            )
            rows = cur.fetchall()
            elapsed = elapsed_ms(start)
        return elapsed, len(rows)

    def read_aggregate(self, ctx: BenchContext) -> tuple[float, int]:
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute("SELECT AVG(popularity) FROM bench_tracks WHERE is_deleted = FALSE")
            cur.fetchone()
            elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_single(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 3)
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                "UPDATE bench_tracks SET track_name = %s, updated_at = %s WHERE bench_id = %s",
                ("Updated Track", now_utc(), bench_id),
            )
            elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_multi(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 4)
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                """
                UPDATE bench_tracks
                SET track_name = %s, tempo = tempo + 0.5, updated_at = %s
                WHERE bench_id = %s
                """,
                ("Updated Multi", now_utc(), bench_id),
            )
            elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_batch(self, ctx: BenchContext) -> tuple[float, int]:
        ids = pick_ids(ctx.size, ctx.batch_size, ctx.seed, 77)
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                "UPDATE bench_tracks SET tempo = tempo + 0.1, updated_at = %s WHERE bench_id = ANY(%s)",
                (now_utc(), ids),
            )
            elapsed = elapsed_ms(start)
        return elapsed, len(ids)

    def delete_by_id(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        bench_id = f"{prefix}_0"
        record = build_record(bench_id, ctx.size + 10, ctx.seed)
        self._insert_records([record])
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute("DELETE FROM bench_tracks WHERE bench_id = %s", (bench_id,))
            elapsed = elapsed_ms(start)
        return elapsed, 1

    def delete_by_condition(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        records = [build_record(f"{prefix}_{i}", ctx.size + 20 + i, ctx.seed) for i in range(ctx.batch_size)]
        for r in records:
            r["popularity"] = 1
        self._insert_records(records)
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                """
                DELETE FROM bench_tracks
                WHERE popularity < %s AND bench_id LIKE %s
                """,
                (5, f"{prefix}%"),
            )
            elapsed = elapsed_ms(start)
        return elapsed, len(records)

    def soft_delete(self, ctx: BenchContext) -> tuple[float, int]:
        ids = pick_ids(ctx.size, ctx.batch_size, ctx.seed, 99)
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                "UPDATE bench_tracks SET is_deleted = TRUE, updated_at = %s WHERE bench_id = ANY(%s)",
                (now_utc(), ids),
            )
            elapsed = elapsed_ms(start)
        return elapsed, len(ids)

    def close(self) -> None:
        self.conn.close()


class MariaDbAdapter(BaseAdapter):
    name = "mariadb"

    def __init__(self) -> None:
        self.conn = pymysql.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            user=os.getenv("MYSQL_USER", "admin"),
            password=os.getenv("MYSQL_PASSWORD", "password"),
            database=os.getenv("MYSQL_DATABASE", "spotify_db"),
            autocommit=True,
        )

    def setup(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bench_tracks (
                    bench_id VARCHAR(64) PRIMARY KEY,
                    track_name VARCHAR(255) NOT NULL,
                    popularity INT,
                    duration_ms INT,
                    explicit BOOLEAN,
                    tempo DOUBLE,
                    created_at DATETIME,
                    updated_at DATETIME,
                    is_deleted BOOLEAN DEFAULT FALSE
                )
                """
            )
            cur.execute("CREATE INDEX idx_bench_popularity ON bench_tracks(popularity)")
            cur.execute("CREATE INDEX idx_bench_is_deleted ON bench_tracks(is_deleted)")

    def seed(self, ctx: BenchContext) -> None:
        with self.conn.cursor() as cur:
            for batch in chunked(range(ctx.size), ctx.batch_size):
                rows = []
                for idx in batch:
                    rec = build_record(make_bench_id(idx), idx, ctx.seed)
                    rows.append(
                        (
                            rec["bench_id"],
                            rec["track_name"],
                            rec["popularity"],
                            rec["duration_ms"],
                            rec["explicit"],
                            rec["tempo"],
                            rec["created_at"],
                            rec["updated_at"],
                            rec["is_deleted"],
                        )
                    )
                cur.executemany(
                    """
                    INSERT IGNORE INTO bench_tracks
                    (bench_id, track_name, popularity, duration_ms, explicit, tempo, created_at, updated_at, is_deleted)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

    def _insert_records(self, records: list[dict[str, Any]]) -> None:
        with self.conn.cursor() as cur:
            rows = [
                (
                    r["bench_id"],
                    r["track_name"],
                    r["popularity"],
                    r["duration_ms"],
                    r["explicit"],
                    r["tempo"],
                    r["created_at"],
                    r["updated_at"],
                    r["is_deleted"],
                )
                for r in records
            ]
            cur.executemany(
                """
                INSERT IGNORE INTO bench_tracks
                (bench_id, track_name, popularity, duration_ms, explicit, tempo, created_at, updated_at, is_deleted)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )

    def _delete_ids(self, ids: list[str]) -> None:
        if not ids:
            return
        placeholders = ",".join(["%s"] * len(ids))
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM bench_tracks WHERE bench_id IN ({placeholders})", ids)

    def create_single(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        bench_id = f"{prefix}_0"
        record = build_record(bench_id, ctx.size + 1, ctx.seed)
        start = time.perf_counter()
        self._insert_records([record])
        elapsed = elapsed_ms(start)
        self._delete_ids([bench_id])
        return elapsed, 1

    def create_batch(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        count = ctx.batch_size
        records = [build_record(f"{prefix}_{i}", ctx.size + i, ctx.seed) for i in range(count)]
        start = time.perf_counter()
        self._insert_records(records)
        elapsed = elapsed_ms(start)
        self._delete_ids([r["bench_id"] for r in records])
        return elapsed, count

    def create_validated(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        record = build_record(f"{prefix}_0", ctx.size + 2, ctx.seed)

        def is_valid(rec: dict[str, Any]) -> bool:
            if not rec["bench_id"] or not rec["track_name"]:
                return False
            if rec["popularity"] is None or not 0 <= rec["popularity"] <= 100:
                return False
            if rec["duration_ms"] is None or rec["duration_ms"] <= 0:
                return False
            return True

        start = time.perf_counter()
        if is_valid(record):
            self._insert_records([record])
        elapsed = elapsed_ms(start)
        self._delete_ids([record["bench_id"]])
        return elapsed, 1

    def read_by_id(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 2)
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute("SELECT * FROM bench_tracks WHERE bench_id = %s", (bench_id,))
            cur.fetchone()
            elapsed = elapsed_ms(start)
        return elapsed, 1

    def read_filter(self, ctx: BenchContext) -> tuple[float, int]:
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                "SELECT * FROM bench_tracks WHERE popularity >= %s LIMIT %s",
                (80, ctx.read_limit),
            )
            rows = cur.fetchall()
            elapsed = elapsed_ms(start)
        return elapsed, len(rows)

    def read_aggregate(self, ctx: BenchContext) -> tuple[float, int]:
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute("SELECT AVG(popularity) FROM bench_tracks WHERE is_deleted = FALSE")
            cur.fetchone()
            elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_single(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 3)
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                "UPDATE bench_tracks SET track_name = %s, updated_at = %s WHERE bench_id = %s",
                ("Updated Track", now_utc(), bench_id),
            )
            elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_multi(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 4)
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                """
                UPDATE bench_tracks
                SET track_name = %s, tempo = tempo + 0.5, updated_at = %s
                WHERE bench_id = %s
                """,
                ("Updated Multi", now_utc(), bench_id),
            )
            elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_batch(self, ctx: BenchContext) -> tuple[float, int]:
        ids = pick_ids(ctx.size, ctx.batch_size, ctx.seed, 77)
        placeholders = ",".join(["%s"] * len(ids))
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                f"UPDATE bench_tracks SET tempo = tempo + 0.1, updated_at = %s WHERE bench_id IN ({placeholders})",
                [now_utc()] + ids,
            )
            elapsed = elapsed_ms(start)
        return elapsed, len(ids)

    def delete_by_id(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        bench_id = f"{prefix}_0"
        record = build_record(bench_id, ctx.size + 10, ctx.seed)
        self._insert_records([record])
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute("DELETE FROM bench_tracks WHERE bench_id = %s", (bench_id,))
            elapsed = elapsed_ms(start)
        return elapsed, 1

    def delete_by_condition(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        records = [build_record(f"{prefix}_{i}", ctx.size + 20 + i, ctx.seed) for i in range(ctx.batch_size)]
        for r in records:
            r["popularity"] = 1
        self._insert_records(records)
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                "DELETE FROM bench_tracks WHERE popularity < %s AND bench_id LIKE %s",
                (5, f"{prefix}%"),
            )
            elapsed = elapsed_ms(start)
        return elapsed, len(records)

    def soft_delete(self, ctx: BenchContext) -> tuple[float, int]:
        ids = pick_ids(ctx.size, ctx.batch_size, ctx.seed, 99)
        placeholders = ",".join(["%s"] * len(ids))
        with self.conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(
                f"UPDATE bench_tracks SET is_deleted = TRUE, updated_at = %s WHERE bench_id IN ({placeholders})",
                [now_utc()] + ids,
            )
            elapsed = elapsed_ms(start)
        return elapsed, len(ids)

    def close(self) -> None:
        self.conn.close()


class MongoAdapter(BaseAdapter):
    name = "mongodb"

    def __init__(self) -> None:
        mongo_user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
        mongo_pass = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "password")
        mongo_host = os.getenv("MONGO_HOST", "localhost")
        mongo_port = int(os.getenv("MONGO_PORT", "27017"))
        mongo_db = os.getenv("MONGO_DB", "spotify_db")
        uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/admin"
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[mongo_db]
        self.col = self.db["bench_tracks"]

    def setup(self) -> None:
        self.col.create_index("popularity")
        self.col.create_index("is_deleted")

    def seed(self, ctx: BenchContext) -> None:
        for batch in chunked(range(ctx.size), ctx.batch_size):
            docs = []
            for idx in batch:
                rec = build_record(make_bench_id(idx), idx, ctx.seed)
                doc = {
                    "_id": rec["bench_id"],
                    **rec,
                }
                docs.append(doc)
            if docs:
                try:
                    self.col.insert_many(docs, ordered=False)
                except pymongo.errors.BulkWriteError:
                    pass

    def _insert_docs(self, docs: list[dict[str, Any]]) -> None:
        if docs:
            self.col.insert_many(docs, ordered=False)

    def _delete_ids(self, ids: list[str]) -> None:
        if ids:
            self.col.delete_many({"_id": {"$in": ids}})

    def create_single(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        bench_id = f"{prefix}_0"
        rec = build_record(bench_id, ctx.size + 1, ctx.seed)
        doc = {"_id": rec["bench_id"], **rec}
        start = time.perf_counter()
        self._insert_docs([doc])
        elapsed = elapsed_ms(start)
        self._delete_ids([bench_id])
        return elapsed, 1

    def create_batch(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        count = ctx.batch_size
        docs = []
        for i in range(count):
            rec = build_record(f"{prefix}_{i}", ctx.size + i, ctx.seed)
            docs.append({"_id": rec["bench_id"], **rec})
        start = time.perf_counter()
        self._insert_docs(docs)
        elapsed = elapsed_ms(start)
        self._delete_ids([d["_id"] for d in docs])
        return elapsed, count

    def create_validated(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        rec = build_record(f"{prefix}_0", ctx.size + 2, ctx.seed)

        def is_valid(doc: dict[str, Any]) -> bool:
            if not doc.get("bench_id") or not doc.get("track_name"):
                return False
            if doc.get("popularity") is None or not 0 <= doc.get("popularity") <= 100:
                return False
            if doc.get("duration_ms") is None or doc.get("duration_ms") <= 0:
                return False
            return True

        doc = {"_id": rec["bench_id"], **rec}
        start = time.perf_counter()
        if is_valid(doc):
            self._insert_docs([doc])
        elapsed = elapsed_ms(start)
        self._delete_ids([rec["bench_id"]])
        return elapsed, 1

    def read_by_id(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 2)
        start = time.perf_counter()
        self.col.find_one({"_id": bench_id})
        elapsed = elapsed_ms(start)
        return elapsed, 1

    def read_filter(self, ctx: BenchContext) -> tuple[float, int]:
        start = time.perf_counter()
        rows = list(self.col.find({"popularity": {"$gte": 80}}).limit(ctx.read_limit))
        elapsed = elapsed_ms(start)
        return elapsed, len(rows)

    def read_aggregate(self, ctx: BenchContext) -> tuple[float, int]:
        start = time.perf_counter()
        list(self.col.aggregate([{"$match": {"is_deleted": False}}, {"$group": {"_id": None, "avg_popularity": {"$avg": "$popularity"}}}]))
        elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_single(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 3)
        start = time.perf_counter()
        self.col.update_one({"_id": bench_id}, {"$set": {"track_name": "Updated Track", "updated_at": now_utc()}})
        elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_multi(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 4)
        start = time.perf_counter()
        self.col.update_one(
            {"_id": bench_id},
            {"$set": {"track_name": "Updated Multi", "updated_at": now_utc()}, "$inc": {"tempo": 0.5}},
        )
        elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_batch(self, ctx: BenchContext) -> tuple[float, int]:
        ids = pick_ids(ctx.size, ctx.batch_size, ctx.seed, 77)
        start = time.perf_counter()
        self.col.update_many({"_id": {"$in": ids}}, {"$inc": {"tempo": 0.1}, "$set": {"updated_at": now_utc()}})
        elapsed = elapsed_ms(start)
        return elapsed, len(ids)

    def delete_by_id(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        bench_id = f"{prefix}_0"
        rec = build_record(bench_id, ctx.size + 10, ctx.seed)
        self._insert_docs([{"_id": rec["bench_id"], **rec}])
        start = time.perf_counter()
        self.col.delete_one({"_id": bench_id})
        elapsed = elapsed_ms(start)
        return elapsed, 1

    def delete_by_condition(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        docs = []
        for i in range(ctx.batch_size):
            rec = build_record(f"{prefix}_{i}", ctx.size + 20 + i, ctx.seed)
            rec["popularity"] = 1
            docs.append({"_id": rec["bench_id"], **rec})
        self._insert_docs(docs)
        start = time.perf_counter()
        self.col.delete_many({"popularity": {"$lt": 5}, "bench_id": {"$regex": f"^{prefix}"}})
        elapsed = elapsed_ms(start)
        return elapsed, len(docs)

    def soft_delete(self, ctx: BenchContext) -> tuple[float, int]:
        ids = pick_ids(ctx.size, ctx.batch_size, ctx.seed, 99)
        start = time.perf_counter()
        self.col.update_many({"_id": {"$in": ids}}, {"$set": {"is_deleted": True, "updated_at": now_utc()}})
        elapsed = elapsed_ms(start)
        return elapsed, len(ids)

    def close(self) -> None:
        self.client.close()


class CassandraAdapter(BaseAdapter):
    name = "cassandra"

    def __init__(self) -> None:
        host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
        port = int(os.getenv("CASSANDRA_PORT", "9042"))
        connection_class = pick_cassandra_connection_class()
        if connection_class is None:
            self.cluster = Cluster([host], port=port)
        else:
            self.cluster = Cluster([host], port=port, connection_class=connection_class)
        self.session = self.cluster.connect()

    def setup(self) -> None:
        self.session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS spotify_db
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        self.session.set_keyspace("spotify_db")
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS bench_tracks_by_id (
                bench_id text PRIMARY KEY,
                track_name text,
                popularity int,
                duration_ms int,
                explicit boolean,
                tempo double,
                created_at timestamp,
                updated_at timestamp,
                is_deleted boolean
            )
            """
        )
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS bench_tracks_by_popularity (
                popularity_bucket int,
                popularity int,
                bench_id text,
                track_name text,
                PRIMARY KEY ((popularity_bucket), popularity, bench_id)
            ) WITH CLUSTERING ORDER BY (popularity DESC, bench_id ASC)
            """
        )

    def seed(self, ctx: BenchContext) -> None:
        for batch in chunked(range(ctx.size), ctx.batch_size):
            for idx in batch:
                rec = build_record(make_bench_id(idx), idx, ctx.seed)
                self._insert_record(rec)

    def _insert_record(self, rec: dict[str, Any]) -> None:
        bucket = popularity_bucket(rec["popularity"])
        self.session.execute(
            """
            INSERT INTO bench_tracks_by_id
            (bench_id, track_name, popularity, duration_ms, explicit, tempo, created_at, updated_at, is_deleted)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                rec["bench_id"],
                rec["track_name"],
                rec["popularity"],
                rec["duration_ms"],
                rec["explicit"],
                rec["tempo"],
                rec["created_at"],
                rec["updated_at"],
                rec["is_deleted"],
            ),
        )
        self.session.execute(
            """
            INSERT INTO bench_tracks_by_popularity
            (popularity_bucket, popularity, bench_id, track_name)
            VALUES (%s, %s, %s, %s)
            """,
            (bucket, rec["popularity"], rec["bench_id"], rec["track_name"]),
        )

    def _delete_ids(self, ids: list[str]) -> None:
        for bench_id in ids:
            self.session.execute("DELETE FROM bench_tracks_by_id WHERE bench_id = %s", (bench_id,))

    def _delete_from_popularity(self, bucket: int, bench_id: str, popularity: int) -> None:
        self.session.execute(
            "DELETE FROM bench_tracks_by_popularity WHERE popularity_bucket = %s AND popularity = %s AND bench_id = %s",
            (bucket, popularity, bench_id),
        )

    def create_single(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        bench_id = f"{prefix}_0"
        rec = build_record(bench_id, ctx.size + 1, ctx.seed)
        start = time.perf_counter()
        self._insert_record(rec)
        elapsed = elapsed_ms(start)
        self._delete_ids([bench_id])
        self._delete_from_popularity(popularity_bucket(rec["popularity"]), bench_id, rec["popularity"])
        return elapsed, 1

    def create_batch(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        count = ctx.batch_size
        records = [build_record(f"{prefix}_{i}", ctx.size + i, ctx.seed) for i in range(count)]
        start = time.perf_counter()
        for rec in records:
            self._insert_record(rec)
        elapsed = elapsed_ms(start)
        self._delete_ids([r["bench_id"] for r in records])
        for rec in records:
            self._delete_from_popularity(popularity_bucket(rec["popularity"]), rec["bench_id"], rec["popularity"])
        return elapsed, count

    def create_validated(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        record = build_record(f"{prefix}_0", ctx.size + 2, ctx.seed)

        def is_valid(rec: dict[str, Any]) -> bool:
            if not rec["bench_id"] or not rec["track_name"]:
                return False
            if rec["popularity"] is None or not 0 <= rec["popularity"] <= 100:
                return False
            if rec["duration_ms"] is None or rec["duration_ms"] <= 0:
                return False
            return True

        start = time.perf_counter()
        if is_valid(record):
            self._insert_record(record)
        elapsed = elapsed_ms(start)
        self._delete_ids([record["bench_id"]])
        self._delete_from_popularity(popularity_bucket(record["popularity"]), record["bench_id"], record["popularity"])
        return elapsed, 1

    def read_by_id(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 2)
        start = time.perf_counter()
        self.session.execute("SELECT * FROM bench_tracks_by_id WHERE bench_id = %s", (bench_id,)).one()
        elapsed = elapsed_ms(start)
        return elapsed, 1

    def read_filter(self, ctx: BenchContext) -> tuple[float, int]:
        bucket = 4
        start = time.perf_counter()
        rows = self.session.execute(
            f"SELECT * FROM bench_tracks_by_popularity WHERE popularity_bucket = {bucket} LIMIT {ctx.read_limit}"
        ).all()
        elapsed = elapsed_ms(start)
        return elapsed, len(rows)

    def read_aggregate(self, ctx: BenchContext) -> tuple[float, int]:
        bucket = 4
        start = time.perf_counter()
        self.session.execute(
            f"SELECT COUNT(*) FROM bench_tracks_by_popularity WHERE popularity_bucket = {bucket}"
        ).one()
        elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_single(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 3)
        start = time.perf_counter()
        self.session.execute(
            "UPDATE bench_tracks_by_id SET track_name = %s, updated_at = %s WHERE bench_id = %s",
            ("Updated Track", now_utc(), bench_id),
        )
        elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_multi(self, ctx: BenchContext) -> tuple[float, int]:
        bench_id = make_bench_id(ctx.size // 4)
        start = time.perf_counter()
        self.session.execute(
            """
            UPDATE bench_tracks_by_id
            SET track_name = %s, tempo = tempo + 0.5, updated_at = %s
            WHERE bench_id = %s
            """,
            ("Updated Multi", now_utc(), bench_id),
        )
        elapsed = elapsed_ms(start)
        return elapsed, 1

    def update_batch(self, ctx: BenchContext) -> tuple[float, int]:
        ids = pick_ids(ctx.size, ctx.batch_size, ctx.seed, 77)
        start = time.perf_counter()
        for bench_id in ids:
            self.session.execute(
                "UPDATE bench_tracks_by_id SET tempo = tempo + 0.1, updated_at = %s WHERE bench_id = %s",
                (now_utc(), bench_id),
            )
        elapsed = elapsed_ms(start)
        return elapsed, len(ids)

    def delete_by_id(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        bench_id = f"{prefix}_0"
        record = build_record(bench_id, ctx.size + 10, ctx.seed)
        self._insert_record(record)
        start = time.perf_counter()
        self.session.execute("DELETE FROM bench_tracks_by_id WHERE bench_id = %s", (bench_id,))
        elapsed = elapsed_ms(start)
        self._delete_from_popularity(popularity_bucket(record["popularity"]), bench_id, record["popularity"])
        return elapsed, 1

    def delete_by_condition(self, ctx: BenchContext, prefix: str) -> tuple[float, int]:
        records = [build_record(f"{prefix}_{i}", ctx.size + 20 + i, ctx.seed) for i in range(ctx.batch_size)]
        for r in records:
            r["popularity"] = 1
            self._insert_record(r)
        start = time.perf_counter()
        for r in records:
            self.session.execute("DELETE FROM bench_tracks_by_id WHERE bench_id = %s", (r["bench_id"],))
            self._delete_from_popularity(popularity_bucket(r["popularity"]), r["bench_id"], r["popularity"])
        elapsed = elapsed_ms(start)
        return elapsed, len(records)

    def soft_delete(self, ctx: BenchContext) -> tuple[float, int]:
        ids = pick_ids(ctx.size, ctx.batch_size, ctx.seed, 99)
        start = time.perf_counter()
        for bench_id in ids:
            self.session.execute(
                "UPDATE bench_tracks_by_id SET is_deleted = true, updated_at = %s WHERE bench_id = %s",
                (now_utc(), bench_id),
            )
        elapsed = elapsed_ms(start)
        return elapsed, len(ids)

    def close(self) -> None:
        self.session.shutdown()
        self.cluster.shutdown()


@dataclass
class Scenario:
    op: str
    name: str
    func: Any


def run_scenarios(adapter: BaseAdapter, ctx: BenchContext, repeats: int) -> list[dict[str, Any]]:
    scenarios = [
        Scenario("CREATE", "insert_single", adapter.create_single),
        Scenario("CREATE", "insert_batch", adapter.create_batch),
        Scenario("CREATE", "insert_validated", adapter.create_validated),
        Scenario("READ", "by_id", adapter.read_by_id),
        Scenario("READ", "filter_popularity", adapter.read_filter),
        Scenario("READ", "aggregate", adapter.read_aggregate),
        Scenario("UPDATE", "single_field", adapter.update_single),
        Scenario("UPDATE", "multi_field", adapter.update_multi),
        Scenario("UPDATE", "batch", adapter.update_batch),
        Scenario("DELETE", "by_id", adapter.delete_by_id),
        Scenario("DELETE", "by_condition", adapter.delete_by_condition),
        Scenario("DELETE", "soft_delete", adapter.soft_delete),
    ]

    results: list[dict[str, Any]] = []
    for scenario in scenarios:
        samples: list[float] = []
        throughputs: list[float] = []
        for i in range(repeats):
            prefix = make_temp_prefix(f"{scenario.name}_{i}")
            if scenario.op == "CREATE":
                elapsed, records = scenario.func(ctx, prefix)
            elif scenario.op == "DELETE" and scenario.name in {"by_id", "by_condition"}:
                elapsed, records = scenario.func(ctx, prefix)
            else:
                elapsed, records = scenario.func(ctx)
            samples.append(elapsed)
            if records:
                throughputs.append(records / max(elapsed, 0.001) * 1000.0)

        avg_ms = sum(samples) / len(samples)
        throughput = sum(throughputs) / len(throughputs) if throughputs else None
        results.append(
            {
                "db": adapter.name,
                "size": ctx.size,
                "operation": scenario.op,
                "scenario": scenario.name,
                "avg_ms": round(avg_ms, 3),
                "throughput_rec_s": round(throughput, 3) if throughput is not None else None,
                "samples_ms": [round(s, 3) for s in samples],
            }
        )
    return results


def parse_sizes(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def select_adapters(names: list[str]) -> list[BaseAdapter]:
    adapters: list[BaseAdapter] = []
    for name in names:
        if name == "postgres":
            adapters.append(PostgresAdapter())
        elif name == "mariadb":
            adapters.append(MariaDbAdapter())
        elif name == "mongodb":
            adapters.append(MongoAdapter())
        elif name == "cassandra":
            adapters.append(CassandraAdapter())
    return adapters


def write_outputs(results: list[dict[str, Any]], output_base: str) -> None:
    output_dir = os.path.dirname(output_base)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    json_path = f"{output_base}.json"
    csv_path = f"{output_base}.csv"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["db", "size", "operation", "scenario", "avg_ms", "throughput_rec_s", "samples_ms"],
        )
        writer.writeheader()
        for row in results:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run CRUD benchmarks for SQL/NoSQL backends")
    parser.add_argument("--sizes", default="10000,100000,1000000", help="Comma-separated dataset sizes")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--read-limit", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dbs", default="postgres,mariadb,mongodb,cassandra")
    parser.add_argument("--skip-seed", action="store_true")
    parser.add_argument("--output", default="results/crud_benchmarks")
    args = parser.parse_args()

    sizes = parse_sizes(args.sizes)
    dbs = [x.strip() for x in args.dbs.split(",") if x.strip()]

    adapters = select_adapters(dbs)
    try:
        all_results: list[dict[str, Any]] = []
        for adapter in adapters:
            adapter.setup()
            for size in sizes:
                ctx = BenchContext(size=size, batch_size=args.batch_size, read_limit=args.read_limit, seed=args.seed)
                if not args.skip_seed:
                    adapter.seed(ctx)
                all_results.extend(run_scenarios(adapter, ctx, args.repeats))
            adapter.close()
        write_outputs(all_results, args.output)
    finally:
        for adapter in adapters:
            try:
                adapter.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
