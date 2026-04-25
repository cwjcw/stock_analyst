from __future__ import annotations

import datetime as dt
from typing import Iterable

import pandas as pd
import pymysql
from pymysql.connections import Connection

from .config import MARIADB


def connect_server() -> Connection:
    return pymysql.connect(
        host=MARIADB.host,
        port=MARIADB.port,
        user=MARIADB.user,
        password=MARIADB.password,
        charset=MARIADB.charset,
        connect_timeout=3,
        read_timeout=5,
        write_timeout=5,
        autocommit=False,
    )


def connect_db() -> Connection:
    return pymysql.connect(
        host=MARIADB.host,
        port=MARIADB.port,
        user=MARIADB.user,
        password=MARIADB.password,
        database=MARIADB.database,
        charset=MARIADB.charset,
        connect_timeout=3,
        read_timeout=5,
        write_timeout=5,
        autocommit=False,
    )


def init_database_and_tables() -> None:
    schema_statements = [
        f"CREATE DATABASE IF NOT EXISTS `{MARIADB.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;",
        f"USE `{MARIADB.database}`;",
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            user_code VARCHAR(64) NOT NULL UNIQUE,
            user_name VARCHAR(128) NOT NULL,
            is_active TINYINT NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS user_watchlist (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            user_id BIGINT NOT NULL,
            ts_code VARCHAR(16) NOT NULL,
            note VARCHAR(255) NULL,
            is_active TINYINT NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_user_code (user_id, ts_code),
            KEY idx_watchlist_code (ts_code),
            CONSTRAINT fk_watchlist_user FOREIGN KEY (user_id) REFERENCES users(id)
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS stock_basic (
            ts_code VARCHAR(16) PRIMARY KEY,
            symbol VARCHAR(16),
            name VARCHAR(128),
            area VARCHAR(64),
            industry VARCHAR(128),
            market VARCHAR(64),
            list_date DATE NULL,
            is_hs VARCHAR(8),
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_quotes (
            ts_code VARCHAR(16) NOT NULL,
            trade_date DATE NOT NULL,
            open DECIMAL(18,4) NULL,
            high DECIMAL(18,4) NULL,
            low DECIMAL(18,4) NULL,
            close DECIMAL(18,4) NULL,
            pre_close DECIMAL(18,4) NULL,
            `change` DECIMAL(18,4) NULL,
            pct_chg DECIMAL(18,4) NULL,
            vol DECIMAL(20,4) NULL,
            amount DECIMAL(20,4) NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date),
            KEY idx_quotes_trade_date (trade_date)
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_basic (
            ts_code VARCHAR(16) NOT NULL,
            trade_date DATE NOT NULL,
            turnover_rate DECIMAL(18,6) NULL,
            turnover_rate_f DECIMAL(18,6) NULL,
            volume_ratio DECIMAL(18,6) NULL,
            pe_ttm DECIMAL(18,6) NULL,
            pb DECIMAL(18,6) NULL,
            total_mv DECIMAL(24,6) NULL,
            circ_mv DECIMAL(24,6) NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date),
            KEY idx_basic_trade_date (trade_date)
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS stk_limit (
            ts_code VARCHAR(16) NOT NULL,
            trade_date DATE NOT NULL,
            up_limit DECIMAL(18,4) NULL,
            down_limit DECIMAL(18,4) NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date),
            KEY idx_limit_trade_date (trade_date)
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS moneyflow (
            ts_code VARCHAR(16) NOT NULL,
            trade_date DATE NOT NULL,
            buy_sm_vol DECIMAL(24,4) NULL,
            buy_sm_amount DECIMAL(24,4) NULL,
            sell_sm_vol DECIMAL(24,4) NULL,
            sell_sm_amount DECIMAL(24,4) NULL,
            buy_md_vol DECIMAL(24,4) NULL,
            buy_md_amount DECIMAL(24,4) NULL,
            sell_md_vol DECIMAL(24,4) NULL,
            sell_md_amount DECIMAL(24,4) NULL,
            buy_lg_vol DECIMAL(24,4) NULL,
            buy_lg_amount DECIMAL(24,4) NULL,
            sell_lg_vol DECIMAL(24,4) NULL,
            sell_lg_amount DECIMAL(24,4) NULL,
            buy_elg_vol DECIMAL(24,4) NULL,
            buy_elg_amount DECIMAL(24,4) NULL,
            sell_elg_vol DECIMAL(24,4) NULL,
            sell_elg_amount DECIMAL(24,4) NULL,
            net_mf_vol DECIMAL(24,4) NULL,
            net_mf_amount DECIMAL(24,4) NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date),
            KEY idx_moneyflow_trade_date (trade_date)
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS realtime_ticks (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            user_id BIGINT NOT NULL,
            ts_code VARCHAR(16) NOT NULL,
            tick_time DATETIME NOT NULL,
            last_price DECIMAL(18,4) NULL,
            open DECIMAL(18,4) NULL,
            high DECIMAL(18,4) NULL,
            low DECIMAL(18,4) NULL,
            last_close DECIMAL(18,4) NULL,
            amount DECIMAL(24,4) NULL,
            volume DECIMAL(24,4) NULL,
            bid1 DECIMAL(18,4) NULL,
            ask1 DECIMAL(18,4) NULL,
            bid1_vol DECIMAL(24,4) NULL,
            ask1_vol DECIMAL(24,4) NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_tick_user_time (user_id, ts_code, tick_time),
            CONSTRAINT fk_tick_user FOREIGN KEY (user_id) REFERENCES users(id)
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS indicator_snapshot (
            user_id BIGINT NOT NULL,
            ts_code VARCHAR(16) NOT NULL,
            calc_time DATETIME NOT NULL,
            last_price DECIMAL(18,4) NULL,
            trend_direction VARCHAR(16) NULL,
            signal_summary VARCHAR(255) NULL,
            indicators_json JSON NULL,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, ts_code),
            KEY idx_indicator_calc_time (calc_time),
            CONSTRAINT fk_indicator_user FOREIGN KEY (user_id) REFERENCES users(id)
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS meta_ingest_job (
            job_id BIGINT PRIMARY KEY AUTO_INCREMENT,
            source VARCHAR(32) NOT NULL,
            dataset VARCHAR(64) NOT NULL,
            ts_code VARCHAR(16) NULL,
            start_at DATETIME NOT NULL,
            end_at DATETIME NULL,
            status VARCHAR(16) NOT NULL,
            row_count INT NOT NULL DEFAULT 0,
            error_msg TEXT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_ingest_dataset_time (dataset, start_at),
            KEY idx_ingest_code_time (ts_code, start_at)
        ) ENGINE=InnoDB;
        """,
        """
        CREATE TABLE IF NOT EXISTS meta_checkpoint (
            task_name VARCHAR(128) PRIMARY KEY,
            last_success_code VARCHAR(16) NULL,
            last_success_date DATE NULL,
            failed_codes_json JSON NULL,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
        """,
    ]

    conn = connect_server()
    try:
        with conn.cursor() as cur:
            for stmt in schema_statements:
                cur.execute(stmt)
            cur.execute(
                """
                INSERT INTO users (user_code, user_name, is_active)
                VALUES (%s, %s, 1)
                ON DUPLICATE KEY UPDATE user_name = VALUES(user_name), is_active = 1
                """,
                ("user001", "default_user"),
            )
        conn.commit()
    finally:
        conn.close()


def _normalize_value(v):
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, dt.date):
        return v
    return v


def upsert_dataframe(conn: Connection, table: str, df: pd.DataFrame, key_cols: Iterable[str]) -> int:
    if df is None or df.empty:
        return 0
    key_cols = list(key_cols)
    cols = list(df.columns)
    non_key_cols = [c for c in cols if c not in key_cols]
    if not non_key_cols:
        return 0

    col_sql = ", ".join(f"`{c}`" for c in cols)
    placeholder = ", ".join(["%s"] * len(cols))
    update_sql = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in non_key_cols)
    sql = f"INSERT INTO `{table}` ({col_sql}) VALUES ({placeholder}) ON DUPLICATE KEY UPDATE {update_sql}"

    data = [tuple(_normalize_value(v) for v in row) for row in df.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        cur.executemany(sql, data)
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
        conn = connect_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO meta_ingest_job
                    (source, dataset, ts_code, start_at, end_at, status, row_count, error_msg)
                    VALUES (%s,%s,%s,NOW(),NOW(),%s,%s,%s)
                    """,
                    (source, dataset, ts_code, status, int(row_count), error_msg),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass
