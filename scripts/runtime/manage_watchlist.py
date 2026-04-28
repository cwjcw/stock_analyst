from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_analyst.db import connect_db, init_database_and_tables
from stock_analyst.stock_reference import normalize_ts_code


def ensure_user(conn, user_id: str, display_name: str) -> None:
    conn.execute(
        """
        INSERT INTO user_profiles (user_id, display_name)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            display_name=excluded.display_name,
            updated_at=datetime('now', 'localtime')
        """,
        (user_id, display_name),
    )
    conn.commit()


def add_stock(conn, user_id: str, ts_code: str, stock_name: str = "", group_name: str = "默认分组") -> None:
    conn.execute(
        """
        INSERT INTO stocks (stock_id, stock_name, user_id, group_name)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(stock_id, user_id, group_name) DO UPDATE SET
            stock_name=excluded.stock_name
        """,
        (normalize_ts_code(ts_code), stock_name, user_id, group_name),
    )
    conn.commit()


def remove_stock(conn, user_id: str, ts_code: str, group_name: str | None = None) -> None:
    ts_code = normalize_ts_code(ts_code)
    if group_name:
        conn.execute(
            "DELETE FROM stocks WHERE user_id=? AND stock_id=? AND group_name=?",
            (user_id, ts_code, group_name),
        )
    else:
        conn.execute("DELETE FROM stocks WHERE user_id=? AND stock_id=?", (user_id, ts_code))
    conn.commit()


def list_stocks(conn, user_id: str) -> None:
    rows = conn.execute(
        """
        SELECT stock_id, stock_name, group_name
        FROM stocks
        WHERE user_id=?
        ORDER BY stock_id, group_name
        """,
        (user_id,),
    ).fetchall()
    print(f"user={user_id} count={len(rows)}")
    for row in rows:
        name = f" {row['stock_name']}" if row["stock_name"] else ""
        print(f"  {row['stock_id']}{name} group={row['group_name']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage local SQLite watchlist")
    parser.add_argument("--user-code", required=True, help="local user_id, e.g. cwjcw")
    parser.add_argument("--user-name", default="user")
    parser.add_argument("--group-name", default="默认分组")
    parser.add_argument("--add", nargs="*", default=[])
    parser.add_argument("--remove", nargs="*", default=[])
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    init_database_and_tables()
    conn = connect_db()
    try:
        ensure_user(conn, args.user_code, args.user_name)
        for code in args.add:
            add_stock(conn, args.user_code, code, group_name=args.group_name)
            print(f"added {normalize_ts_code(code)}")
        for code in args.remove:
            remove_stock(conn, args.user_code, code, group_name=args.group_name)
            print(f"removed {normalize_ts_code(code)}")
        if args.list or (not args.add and not args.remove):
            list_stocks(conn, args.user_code)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
