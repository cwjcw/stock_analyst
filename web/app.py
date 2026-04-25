from __future__ import annotations

import datetime as dt
import os
import random
import re
import smtplib
import sqlite3
import string
import subprocess
import sys
from email.mime.text import MIMEText
from pathlib import Path

from flask import Flask, jsonify, render_template, request, session
from werkzeug.security import check_password_hash, generate_password_hash

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_analyst.stock_reference import lookup_stock_reference, normalize_ts_code

DB_PATH = ROOT / "stock_analyst.db"
TS_CODE_RE = re.compile(r"^\d{6}\.(SZ|SH)$", re.IGNORECASE)

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.qiye.aliyun.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "stock@cuixiaoyuan.cn")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "weijie1981")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USERNAME)

app = Flask(
    __name__,
    template_folder=str(ROOT / "web" / "templates"),
    static_folder=str(ROOT / "web" / "static"),
)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "stock-analyst-cn-secret")


def _ok(data=None):
    return jsonify({"ok": True, "data": data or {}})


def _err(message: str, code: int = 400, extra: dict | None = None):
    payload = {"ok": False, "error": message}
    if extra:
        payload["data"] = extra
    return jsonify(payload), code


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def send_email(subject: str, recipient: str, html_body: str) -> None:
    if not recipient:
        raise ValueError("用户未设置邮箱，无法发送邮件。")
    message = MIMEText(html_body, "html", "utf-8")
    message["Subject"] = subject
    message["From"] = SMTP_FROM
    message["To"] = recipient
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(SMTP_FROM, [recipient], message.as_string())


def welcome_email_html(username: str, display_name: str) -> str:
    title = display_name or username
    return f"""
    <html>
      <body style="font-family:Arial,'Microsoft YaHei',sans-serif;color:#12202b;">
        <h2>欢迎加入技术指标计算助手</h2>
        <p>{title}，你好：</p>
        <p>你的账号已经注册成功，现在可以登录系统，管理自选股票并查看技术指标分析结果。</p>
        <p>我们会为你提供：</p>
        <ul>
          <li>趋势判断：帮助你判断这只股票值不值得参与</li>
          <li>时机判断：帮助你判断当天更适合买入、卖出还是观望</li>
          <li>自选分组管理：方便你按策略、题材、账户整理股票</li>
        </ul>
        <p>祝你使用顺利。</p>
        <p>技术指标计算助手</p>
      </body>
    </html>
    """


def reset_email_html(username: str, new_password: str) -> str:
    return f"""
    <html>
      <body style="font-family:Arial,'Microsoft YaHei',sans-serif;color:#12202b;">
        <h2>技术指标计算助手密码重置</h2>
        <p>用户 <strong>{username}</strong> 的密码已经重置。</p>
        <p>新的临时密码为：</p>
        <p style="font-size:20px;font-weight:700;color:#124b8c;">{new_password}</p>
        <p>请登录后尽快在用户中心修改密码。</p>
      </body>
    </html>
    """


def random_password(length: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.SystemRandom().choice(alphabet) for _ in range(length))


def _has_unique_stock_id_constraint(conn: sqlite3.Connection) -> bool:
    indexes = conn.execute("PRAGMA index_list(stocks)").fetchall()
    for index in indexes:
        if not index["unique"]:
            continue
        info = conn.execute(f"PRAGMA index_info({index['name']!r})").fetchall()
        cols = [row["name"] for row in info]
        if cols == ["stock_id"]:
            return True
    return False


def init_sqlite() -> None:
    conn = get_conn()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                email TEXT,
                password_hash TEXT,
                phone TEXT NOT NULL DEFAULT '',
                bio TEXT NOT NULL DEFAULT '',
                risk_level TEXT NOT NULL DEFAULT '稳健',
                strategy_note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
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
            """
        )

        user_columns = {row["name"] for row in conn.execute("PRAGMA table_info(user_profiles)").fetchall()}
        user_alters = {
            "password_hash": "ALTER TABLE user_profiles ADD COLUMN password_hash TEXT",
            "phone": "ALTER TABLE user_profiles ADD COLUMN phone TEXT NOT NULL DEFAULT ''",
            "bio": "ALTER TABLE user_profiles ADD COLUMN bio TEXT NOT NULL DEFAULT ''",
            "risk_level": "ALTER TABLE user_profiles ADD COLUMN risk_level TEXT NOT NULL DEFAULT '稳健'",
            "strategy_note": "ALTER TABLE user_profiles ADD COLUMN strategy_note TEXT NOT NULL DEFAULT ''",
            "last_login_at": "ALTER TABLE user_profiles ADD COLUMN last_login_at TEXT",
        }
        for column, sql in user_alters.items():
            if column not in user_columns:
                conn.execute(sql)

        stock_columns = {row["name"] for row in conn.execute("PRAGMA table_info(stocks)").fetchall()}
        if "group_name" not in stock_columns:
            conn.execute("ALTER TABLE stocks ADD COLUMN group_name TEXT NOT NULL DEFAULT '默认分组'")
        if "stock_name" not in stock_columns:
            conn.execute("ALTER TABLE stocks ADD COLUMN stock_name TEXT NOT NULL DEFAULT ''")

        if _has_unique_stock_id_constraint(conn):
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS stocks_v2 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_id TEXT NOT NULL,
                    stock_name TEXT NOT NULL DEFAULT '',
                    user_id TEXT NOT NULL,
                    group_name TEXT NOT NULL DEFAULT '默认分组',
                    created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                    UNIQUE(stock_id, user_id, group_name)
                );

                INSERT OR IGNORE INTO stocks_v2 (stock_id, stock_name, user_id, group_name, created_at)
                SELECT
                    stock_id,
                    COALESCE(stock_name, ''),
                    user_id,
                    COALESCE(NULLIF(group_name, ''), '默认分组'),
                    COALESCE(created_at, datetime('now', 'localtime'))
                FROM stocks;

                DROP TABLE stocks;
                ALTER TABLE stocks_v2 RENAME TO stocks;
                """
            )

        conn.execute(
            """
            INSERT OR IGNORE INTO user_profiles (user_id, display_name, email)
            SELECT DISTINCT user_id, user_id, ''
            FROM stocks
            WHERE user_id IS NOT NULL AND trim(user_id) <> ''
            """
        )
        conn.commit()
    finally:
        conn.close()


def fetch_user(user_id: str) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT
                user_id, display_name, email, phone, bio, risk_level,
                strategy_note, created_at, updated_at, last_login_at
            FROM user_profiles
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return dict(row)


def fetch_user_stocks(user_id: str) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT stock_id, stock_name, group_name, created_at
            FROM stocks
            WHERE user_id = ?
            ORDER BY group_name, stock_id
            """,
            (user_id,),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "ts_code": row["stock_id"],
            "stock_name": row["stock_name"] or "",
            "group_name": row["group_name"] or "默认分组",
            "created_at": row["created_at"],
        }
        for row in rows
    ]


def fetch_user_bundle(user_id: str) -> dict | None:
    user = fetch_user(user_id)
    if not user:
        return None
    user["stocks"] = fetch_user_stocks(user_id)
    return user


def register_user(user_id: str, display_name: str, email: str, password: str) -> tuple[dict, str | None]:
    conn = get_conn()
    try:
        existing_user = conn.execute(
            "SELECT user_id, email, password_hash FROM user_profiles WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if existing_user and (
            (existing_user["password_hash"] or "").strip() or (existing_user["email"] or "").strip()
        ):
            raise ValueError("用户名已存在，请更换一个用户名。")

        existing_email = conn.execute(
            "SELECT 1 FROM user_profiles WHERE lower(email) = lower(?) AND trim(email) <> ''",
            (email,),
        ).fetchone()
        if existing_email:
            raise ValueError("该邮箱已被注册，请直接登录或找回密码。")

        if existing_user:
            conn.execute(
                """
                UPDATE user_profiles
                SET display_name = ?,
                    email = ?,
                    password_hash = ?,
                    updated_at = datetime('now', 'localtime')
                WHERE user_id = ?
                """,
                (display_name, email, generate_password_hash(password), user_id),
            )
        else:
            conn.execute(
                """
                INSERT INTO user_profiles (
                    user_id, display_name, email, password_hash,
                    phone, bio, risk_level, strategy_note,
                    created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, '', '', '稳健', '',
                    datetime('now', 'localtime'),
                    datetime('now', 'localtime')
                )
                """,
                (user_id, display_name, email, generate_password_hash(password)),
            )
        conn.commit()
    finally:
        conn.close()

    warning = None
    try:
        send_email("欢迎使用技术指标计算助手", email, welcome_email_html(user_id, display_name))
    except Exception as exc:
        warning = f"注册成功，但欢迎邮件发送失败：{exc}"
    return fetch_user_bundle(user_id), warning


def authenticate_user(user_id: str, password: str) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT user_id, password_hash
            FROM user_profiles
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        if not row or not row["password_hash"]:
            return None
        if not check_password_hash(row["password_hash"], password):
            return None
        conn.execute(
            "UPDATE user_profiles SET last_login_at = datetime('now', 'localtime') WHERE user_id = ?",
            (user_id,),
        )
        conn.commit()
    finally:
        conn.close()
    return fetch_user_bundle(user_id)


def reset_password_by_username(user_id: str) -> tuple[str, str]:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT user_id, email FROM user_profiles WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if not row:
            raise ValueError("未找到该用户名对应的账号。")
        email = (row["email"] or "").strip()
        if not email:
            raise ValueError("该账号未设置邮箱，无法重置密码。")

        new_password = random_password(8)
        send_email("技术指标计算助手密码重置", email, reset_email_html(user_id, new_password))
        conn.execute(
            """
            UPDATE user_profiles
            SET password_hash = ?, updated_at = datetime('now', 'localtime')
            WHERE user_id = ?
            """,
            (generate_password_hash(new_password), user_id),
        )
        conn.commit()
        return email, new_password
    finally:
        conn.close()


def update_profile(user_id: str, payload: dict) -> dict:
    display_name = (payload.get("display_name") or "").strip() or user_id
    phone = (payload.get("phone") or "").strip()
    bio = (payload.get("bio") or "").strip()
    risk_level = (payload.get("risk_level") or "稳健").strip() or "稳健"
    strategy_note = (payload.get("strategy_note") or "").strip()

    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE user_profiles
            SET display_name = ?,
                phone = ?,
                bio = ?,
                risk_level = ?,
                strategy_note = ?,
                updated_at = datetime('now', 'localtime')
            WHERE user_id = ?
            """,
            (display_name, phone, bio, risk_level, strategy_note, user_id),
        )
        conn.commit()
    finally:
        conn.close()
    return fetch_user_bundle(user_id)


def change_password(user_id: str, current_password: str, new_password: str) -> None:
    conn = get_conn()
    try:
        row = conn.execute("SELECT password_hash FROM user_profiles WHERE user_id = ?", (user_id,)).fetchone()
        if not row or not row["password_hash"]:
            raise ValueError("当前账号尚未设置密码，请使用找回密码功能。")
        if not check_password_hash(row["password_hash"], current_password):
            raise ValueError("当前密码不正确。")
        conn.execute(
            """
            UPDATE user_profiles
            SET password_hash = ?, updated_at = datetime('now', 'localtime')
            WHERE user_id = ?
            """,
            (generate_password_hash(new_password), user_id),
        )
        conn.commit()
    finally:
        conn.close()


def add_stock(user_id: str, ts_code: str, stock_name: str = "", group_name: str = "默认分组") -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO stocks (stock_id, stock_name, user_id, group_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(stock_id, user_id, group_name) DO UPDATE SET
                stock_name = excluded.stock_name
            """,
            (ts_code, stock_name, user_id, group_name),
        )
        conn.commit()
    finally:
        conn.close()


def remove_stock(user_id: str, ts_code: str, group_name: str | None = None) -> None:
    conn = get_conn()
    try:
        if group_name:
            conn.execute(
                "DELETE FROM stocks WHERE user_id = ? AND stock_id = ? AND group_name = ?",
                (user_id, ts_code, group_name),
            )
        else:
            conn.execute(
                "DELETE FROM stocks WHERE user_id = ? AND stock_id = ?",
                (user_id, ts_code),
            )
        conn.commit()
    finally:
        conn.close()


def list_user_codes(user_id: str, group_name: str | None = None) -> list[str]:
    conn = get_conn()
    try:
        if group_name:
            rows = conn.execute(
                "SELECT DISTINCT stock_id FROM stocks WHERE user_id = ? AND group_name = ? ORDER BY stock_id",
                (user_id, group_name),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT stock_id FROM stocks WHERE user_id = ? ORDER BY stock_id",
                (user_id,),
            ).fetchall()
    finally:
        conn.close()
    return [str(row["stock_id"]).upper() for row in rows]


def report_file_path(ts_code: str) -> Path:
    return ROOT / "reports" / f"{ts_code.replace('.', '_')}_indicator_report_{dt.date.today().strftime('%Y%m%d')}.md"


def recent_reports_for_user(user_id: str, limit: int = 10) -> list[dict]:
    reports: list[dict] = []
    for code in list_user_codes(user_id):
        path = report_file_path(code)
        if not path.exists():
            continue
        reports.append(
            {
                "ts_code": code,
                "path": path.as_posix(),
                "mtime": dt.datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "size_kb": round(path.stat().st_size / 1024, 2),
            }
        )
    reports.sort(key=lambda item: item["mtime"], reverse=True)
    return reports[:limit]


def dashboard_summary(user_id: str) -> dict:
    user = fetch_user_bundle(user_id)
    stocks = user["stocks"] if user else []
    groups = sorted({item["group_name"] or "默认分组" for item in stocks})
    return {
        "stock_count": len(stocks),
        "group_count": len(groups),
        "groups": groups,
        "report_count": len(recent_reports_for_user(user_id, limit=100)),
        "latest_reports": recent_reports_for_user(user_id, limit=6),
    }


def run_latest_minute_fetch(ts_codes: list[str], period: str = "1m") -> None:
    if not ts_codes:
        return
    cmd = [
        sys.executable,
        "scripts/data/qmt_fetch_all_kline_once.py",
        "--period",
        period,
        "--ts-code",
        *ts_codes,
        "--out-dir",
        "data/qmt_selected_minutes",
    ]
    subprocess.run(cmd, cwd=ROOT, check=True)


def run_report(ts_code: str, minute_period: str = "1m") -> Path:
    cmd = [
        sys.executable,
        "scripts/report/generate_md_report.py",
        "--ts-code",
        ts_code,
        "--minute-period",
        minute_period,
    ]
    subprocess.run(cmd, cwd=ROOT, check=True)
    return report_file_path(ts_code)


def parse_report(md_text: str) -> dict:
    trend = "-"
    timing = "-"
    final_signal = "-"
    for line in md_text.splitlines():
        if line.startswith("- 趋势判断："):
            trend = line.split("：", 1)[1].strip()
        if line.startswith("- 时机判断："):
            timing = line.split("：", 1)[1].strip()
        if line.startswith("- 最终判定："):
            final_signal = line.split("：", 1)[1].strip()
        if line.startswith("- 综合信号：") and final_signal == "-":
            final_signal = line.split("：", 1)[1].strip()
    return {"trend_decision": trend, "timing_decision": timing, "final_signal": final_signal}


def current_user_id() -> str | None:
    user_id = session.get("user_id")
    return str(user_id) if user_id else None


def fallback_stock_name(raw_code: str) -> str:
    ts_code = normalize_ts_code(raw_code)
    symbol = ts_code.split(".")[0] if "." in ts_code else ts_code
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT stock_name
            FROM stocks
            WHERE stock_id = ? OR stock_id LIKE ?
            ORDER BY stock_name DESC
            LIMIT 1
            """,
            (ts_code, f"{symbol}.%"),
        ).fetchone()
    finally:
        conn.close()
    return (row["stock_name"] or "").strip() if row else ""


def require_login() -> str:
    user_id = current_user_id()
    if not user_id:
        raise PermissionError("请先登录后再继续操作。")
    return user_id


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/health")
def api_health():
    return _ok({"sqlite": str(DB_PATH), "exists": DB_PATH.exists()})


@app.post("/api/auth/register")
def api_register():
    payload = request.get_json(silent=True) or {}
    user_id = (payload.get("user_id") or "").strip()
    email = (payload.get("email") or "").strip()
    password = (payload.get("password") or "").strip()
    display_name = (payload.get("display_name") or "").strip() or user_id

    if not user_id:
        return _err("请输入用户名。")
    if not email:
        return _err("请输入注册邮箱。")
    if len(password) < 8:
        return _err("密码长度至少为 8 位。")

    try:
        user, warning = register_user(user_id, display_name, email, password)
        session["user_id"] = user_id
        return _ok({"user": user, "warning": warning or ""})
    except ValueError as exc:
        return _err(str(exc))
    except Exception as exc:
        return _err(f"注册失败：{exc}", 500)


@app.post("/api/auth/login")
def api_login():
    payload = request.get_json(silent=True) or {}
    user_id = (payload.get("user_id") or "").strip()
    password = (payload.get("password") or "").strip()
    if not user_id or not password:
        return _err("请输入用户名和密码。")

    user = authenticate_user(user_id, password)
    if not user:
        return _err("用户名或密码不正确。", 401)
    session["user_id"] = user_id
    return _ok({"user": user})


@app.post("/api/auth/forgot-password")
def api_forgot_password():
    payload = request.get_json(silent=True) or {}
    user_id = (payload.get("user_id") or "").strip()
    if not user_id:
        return _err("请输入用户名。")
    try:
        email, _ = reset_password_by_username(user_id)
        return _ok({"message": f"新的随机密码已经发送到注册邮箱：{email}"})
    except ValueError as exc:
        return _err(str(exc))
    except Exception as exc:
        return _err(f"重置密码失败：{exc}", 500)


@app.post("/api/auth/logout")
def api_logout():
    session.clear()
    return _ok({"message": "已退出登录。"})


@app.get("/api/me")
def api_me():
    user_id = current_user_id()
    if not user_id:
        return _ok({"authenticated": False})
    user = fetch_user_bundle(user_id)
    if not user:
        session.clear()
        return _ok({"authenticated": False})
    return _ok({"authenticated": True, "user": user, "summary": dashboard_summary(user_id)})


@app.get("/api/dashboard")
def api_dashboard():
    try:
        user_id = require_login()
        return _ok({"summary": dashboard_summary(user_id)})
    except PermissionError as exc:
        return _err(str(exc), 401)


@app.post("/api/me/profile")
def api_update_profile():
    try:
        user_id = require_login()
        payload = request.get_json(silent=True) or {}
        user = update_profile(user_id, payload)
        return _ok({"user": user})
    except PermissionError as exc:
        return _err(str(exc), 401)
    except Exception as exc:
        return _err(f"保存用户资料失败：{exc}", 500)


@app.post("/api/me/password")
def api_change_password():
    try:
        user_id = require_login()
        payload = request.get_json(silent=True) or {}
        current_password = (payload.get("current_password") or "").strip()
        new_password = (payload.get("new_password") or "").strip()
        if len(new_password) < 8:
            return _err("新密码长度至少为 8 位。")
        change_password(user_id, current_password, new_password)
        return _ok({"message": "密码修改成功。"})
    except PermissionError as exc:
        return _err(str(exc), 401)
    except ValueError as exc:
        return _err(str(exc))
    except Exception as exc:
        return _err(f"修改密码失败：{exc}", 500)


@app.get("/api/stocks")
def api_stocks():
    try:
        user_id = require_login()
        return _ok({"stocks": fetch_user_stocks(user_id)})
    except PermissionError as exc:
        return _err(str(exc), 401)


@app.post("/api/stocks/lookup")
def api_stock_lookup():
    try:
        require_login()
        payload = request.get_json(silent=True) or {}
        raw_code = (payload.get("code") or "").strip()
        if not raw_code:
            return _err("请输入股票代码。")
        result = lookup_stock_reference(raw_code)
        if not result.get("stock_name"):
            result["stock_name"] = fallback_stock_name(raw_code)
        if not result.get("stock_name"):
            return _err("未找到对应股票，请确认代码是否正确。")
        if not result.get("concept_boards"):
            result["warning"] = "概念板块使用离线缓存，当前未命中可用板块数据。"
        return _ok(result)
    except PermissionError as exc:
        return _err(str(exc), 401)
    except Exception as exc:
        payload = request.get_json(silent=True) or {}
        fallback_name = fallback_stock_name(payload.get("code") or "")
        if fallback_name:
            return _ok(
                {
                    "input_code": payload.get("code") or "",
                    "ts_code": normalize_ts_code(payload.get("code") or ""),
                    "symbol": str(payload.get("code") or "").strip()[:6],
                    "stock_name": fallback_name,
                    "concept_boards": [],
                    "warning": "股票名称已从本地数据补全，概念板块离线缓存当前不可用。",
                }
            )
        return _err(f"股票信息查询失败：{exc}", 500)


@app.post("/api/stocks/add")
def api_add_stock():
    try:
        user_id = require_login()
        payload = request.get_json(silent=True) or {}
        ts_code = normalize_ts_code((payload.get("ts_code") or "").strip().upper())
        stock_name = (payload.get("stock_name") or "").strip()
        group_name = (payload.get("group_name") or "默认分组").strip() or "默认分组"
        if not TS_CODE_RE.match(ts_code):
            return _err("股票代码格式应为 000001.SZ。")
        add_stock(user_id, ts_code, stock_name, group_name)
        return _ok({"stocks": fetch_user_stocks(user_id)})
    except PermissionError as exc:
        return _err(str(exc), 401)
    except Exception as exc:
        return _err(f"加入股票失败：{exc}", 500)


@app.post("/api/stocks/remove")
def api_remove_stock():
    try:
        user_id = require_login()
        payload = request.get_json(silent=True) or {}
        ts_code = (payload.get("ts_code") or "").strip().upper()
        group_name = (payload.get("group_name") or "").strip() or None
        if not TS_CODE_RE.match(ts_code):
            return _err("股票代码格式应为 000001.SZ。")
        remove_stock(user_id, ts_code, group_name)
        return _ok({"stocks": fetch_user_stocks(user_id)})
    except PermissionError as exc:
        return _err(str(exc), 401)
    except Exception as exc:
        return _err(f"移除股票失败：{exc}", 500)


@app.get("/api/reports")
def api_reports():
    try:
        user_id = require_login()
        return _ok({"reports": recent_reports_for_user(user_id)})
    except PermissionError as exc:
        return _err(str(exc), 401)


@app.post("/api/analyze")
def api_analyze():
    try:
        user_id = require_login()
        payload = request.get_json(silent=True) or {}
        ts_codes = [str(code).upper() for code in (payload.get("ts_codes") or []) if str(code).strip()]
        group_name = (payload.get("group_name") or "").strip() or None
        period = (payload.get("minute_period") or "1m").strip()
        if period not in {"1m", "5m", "10m"}:
            return _err("分钟周期只能是 1m、5m 或 10m。")
        if not ts_codes:
            ts_codes = list_user_codes(user_id, group_name)
        if not ts_codes:
            return _err("当前没有可分析的股票。")

        run_latest_minute_fetch(ts_codes, period=period)
        results = []
        for code in ts_codes:
            report_path = run_report(code, minute_period=period)
            markdown = report_path.read_text(encoding="utf-8-sig")
            results.append(
                {
                    "ts_code": code,
                    "report_path": report_path.as_posix(),
                    "markdown": markdown,
                    **parse_report(markdown),
                }
            )
        return _ok({"results": results, "count": len(results), "minute_period": period})
    except PermissionError as exc:
        return _err(str(exc), 401)
    except subprocess.CalledProcessError as exc:
        return _err(f"分析流程执行失败：{exc}", 500)
    except Exception as exc:
        return _err(f"分析失败：{exc}", 500)


if __name__ == "__main__":
    init_sqlite()
    app.run(host="0.0.0.0", port=8080, debug=False)
