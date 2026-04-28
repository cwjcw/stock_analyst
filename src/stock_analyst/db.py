from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "stock_analyst.db"


def connect_db(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_database_and_tables(db_path: Path | str = DB_PATH) -> None:
    conn = connect_db(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                email TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                password_hash TEXT,
                phone TEXT NOT NULL DEFAULT '',
                bio TEXT NOT NULL DEFAULT '',
                risk_level TEXT NOT NULL DEFAULT '稳健',
                strategy_note TEXT NOT NULL DEFAULT '',
                last_login_at TEXT
            );

            CREATE TABLE IF NOT EXISTS stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_id TEXT NOT NULL,
                stock_name TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL,
                group_name TEXT NOT NULL DEFAULT '默认分组',
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                UNIQUE(stock_id, user_id, group_name)
            );

            CREATE TABLE IF NOT EXISTS realtime_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                tick_time TEXT NOT NULL,
                last_price REAL,
                open REAL,
                high REAL,
                low REAL,
                last_close REAL,
                amount REAL,
                volume REAL,
                bid1 REAL,
                ask1 REAL,
                bid1_vol REAL,
                ask1_vol REAL,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            );

            CREATE INDEX IF NOT EXISTS idx_tick_user_time
                ON realtime_ticks(user_id, ts_code, tick_time);

            CREATE TABLE IF NOT EXISTS indicator_snapshot (
                user_id TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                calc_time TEXT NOT NULL,
                last_price REAL,
                trend_direction TEXT,
                signal_summary TEXT,
                indicators_json TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                PRIMARY KEY (user_id, ts_code)
            );

            CREATE TABLE IF NOT EXISTS meta_ingest_job (
                job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                dataset TEXT NOT NULL,
                ts_code TEXT,
                start_at TEXT NOT NULL,
                end_at TEXT,
                status TEXT NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                error_msg TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            );

            CREATE INDEX IF NOT EXISTS idx_ingest_dataset_time
                ON meta_ingest_job(dataset, start_at);

            CREATE INDEX IF NOT EXISTS idx_ingest_code_time
                ON meta_ingest_job(ts_code, start_at);

            CREATE TABLE IF NOT EXISTS meta_checkpoint (
                task_name TEXT PRIMARY KEY,
                last_success_code TEXT,
                last_success_date TEXT,
                failed_codes_json TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            );
            """
        )
        conn.execute(
            """
            INSERT INTO user_profiles (user_id, display_name)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                display_name=excluded.display_name,
                updated_at=datetime('now', 'localtime')
            """,
            ("user001", "default_user"),
        )
        conn.commit()
    finally:
        conn.close()


def _normalize_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False)
    return value


def upsert_dataframe(conn: sqlite3.Connection, table: str, df: pd.DataFrame, key_cols: Iterable[str]) -> int:
    if df is None or df.empty:
        return 0
    key_cols = list(key_cols)
    cols = list(df.columns)
    non_key_cols = [col for col in cols if col not in key_cols]

    col_sql = ", ".join(f'"{col}"' for col in cols)
    placeholders = ", ".join(["?"] * len(cols))
    update_sql = ", ".join(f'"{col}"=excluded."{col}"' for col in non_key_cols)
    conflict_sql = ", ".join(f'"{col}"' for col in key_cols)
    if update_sql:
        sql = f'INSERT INTO "{table}" ({col_sql}) VALUES ({placeholders}) ON CONFLICT({conflict_sql}) DO UPDATE SET {update_sql}'
    else:
        sql = f'INSERT OR IGNORE INTO "{table}" ({col_sql}) VALUES ({placeholders})'

    data = [tuple(_normalize_value(value) for value in row) for row in df.itertuples(index=False, name=None)]
    conn.executemany(sql, data)
    return len(data)


def try_insert_ingest_job(
    source: str,
    dataset: str,
    ts_code: str | None,
    status: str,
    row_count: int,
    error_msg: str | None = None,
) -> None:
    try:
        init_database_and_tables()
        conn = connect_db()
        try:
            now = dt.datetime.now().isoformat(timespec="seconds")
            conn.execute(
                """
                INSERT INTO meta_ingest_job
                (source, dataset, ts_code, start_at, end_at, status, row_count, error_msg)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (source, dataset, ts_code, now, now, status, int(row_count), error_msg),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass
