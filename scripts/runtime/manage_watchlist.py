from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from stock_analyst.db import connect_db


def ensure_user(conn, user_code: str, user_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (user_code, user_name, is_active)
            VALUES (%s,%s,1)
            ON DUPLICATE KEY UPDATE user_name=VALUES(user_name), is_active=1
            """,
            (user_code, user_name),
        )
        conn.commit()
        cur.execute("SELECT id FROM users WHERE user_code=%s", (user_code,))
        return int(cur.fetchone()[0])


def add_stock(conn, user_id: int, ts_code: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO user_watchlist (user_id, ts_code, is_active)
            VALUES (%s,%s,1)
            ON DUPLICATE KEY UPDATE is_active=1
            """,
            (user_id, ts_code),
        )
    conn.commit()


def remove_stock(conn, user_id: int, ts_code: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE user_watchlist SET is_active=0 WHERE user_id=%s AND ts_code=%s",
            (user_id, ts_code),
        )
    conn.commit()


def list_stocks(conn, user_code: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.user_code, w.ts_code, w.is_active
            FROM users u
            LEFT JOIN user_watchlist w ON w.user_id=u.id
            WHERE u.user_code=%s
            ORDER BY w.ts_code
            """,
            (user_code,),
        )
        rows = cur.fetchall()
    print(f"user={user_code}")
    for r in rows:
        if r[1] is not None:
            print(f"  {r[1]} active={r[2]}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage users and watchlist")
    parser.add_argument("--user-code", required=True)
    parser.add_argument("--user-name", default="user")
    parser.add_argument("--add", nargs="*", default=[])
    parser.add_argument("--remove", nargs="*", default=[])
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    conn = connect_db()
    try:
        user_id = ensure_user(conn, args.user_code, args.user_name)
        for code in args.add:
            add_stock(conn, user_id, code)
            print(f"added {code}")
        for code in args.remove:
            remove_stock(conn, user_id, code)
            print(f"removed {code}")
        if args.list or (not args.add and not args.remove):
            list_stocks(conn, args.user_code)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

