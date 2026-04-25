from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys
import time
from pathlib import Path

import tushare as ts
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]


def load_token() -> str:
    token = (os.getenv("TUSHARE_TOKEN") or "").strip().strip('"').strip("'")
    if token:
        return token
    env = ROOT / ".env"
    if env.exists():
        vals = dotenv_values(env)
        return (vals.get("TUSHARE_TOKEN") or "").strip().strip('"').strip("'")
    return ""


def is_trade_day(token: str, day: dt.date) -> bool:
    pro = ts.pro_api(token)
    df = pro.trade_cal(exchange="SSE", start_date=day.strftime("%Y%m%d"), end_date=day.strftime("%Y%m%d"))
    if df is None or df.empty:
        return False
    return str(df.iloc[0].get("is_open", "0")) == "1"


def run_once(period: str, lookback_days: int, out_dir: str, download_history: bool, repair_missing: bool) -> None:
    cmd = [
        sys.executable,
        "scripts/data/qmt_fetch_all_kline_once.py",
        "--period",
        period,
        "--out-dir",
        out_dir,
    ]
    if download_history:
        cmd.append("--download-history")
    if repair_missing:
        cmd.append("--repair-missing")
    print(f"[scheduler] run: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=ROOT, check=True)


def sleep_until(target: dt.datetime) -> None:
    while True:
        now = dt.datetime.now()
        if now >= target:
            return
        time.sleep(min(30, max(1, int((target - now).total_seconds()))))


def run_at_planned_time(
    planned_time: dt.datetime,
    period: str,
    lookback_days: int,
    out_dir: str,
    download_history: bool,
    repair_missing: bool,
) -> None:
    if dt.datetime.now() > planned_time:
        print(f"[scheduler] skip past slot {planned_time.strftime('%H:%M')}")
        return
    sleep_until(planned_time)
    run_once(period, lookback_days, out_dir, download_history, repair_missing)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="On trade day: fetch the latest minute bar for selected stocks at 09:15/09:20/09:25, then every 10m from 09:30."
    )
    parser.add_argument("--period", default="1m", choices=["1m", "5m", "10m"], help="recommended default is 1m")
    parser.add_argument("--lookback-days", type=int, default=1)
    parser.add_argument("--out-dir", default="data/qmt_selected_minutes")
    parser.add_argument("--until", default="15:00", help="stop time HH:MM")
    parser.add_argument("--download-history", action="store_true", help="download history each run before fetch")
    parser.add_argument(
        "--no-repair-missing",
        action="store_true",
        help="disable targeted history download for codes that miss on fast fetch",
    )
    args = parser.parse_args()

    token = load_token()
    if not token:
        raise RuntimeError("Missing TUSHARE_TOKEN")

    today = dt.date.today()
    if not is_trade_day(token, today):
        print(f"[scheduler] {today.isoformat()} is not a trade day, exit.")
        return

    now = dt.datetime.now()
    day = now.date()
    t_0915 = dt.datetime.combine(day, dt.time(9, 15))
    t_0920 = dt.datetime.combine(day, dt.time(9, 20))
    t_0925 = dt.datetime.combine(day, dt.time(9, 25))
    t_0930 = dt.datetime.combine(day, dt.time(9, 30))
    t_1130 = dt.datetime.combine(day, dt.time(11, 30))
    t_1300 = dt.datetime.combine(day, dt.time(13, 0))
    until_h, until_m = [int(x) for x in args.until.split(":")]
    t_until = dt.datetime.combine(day, dt.time(until_h, until_m))

    if now > t_until:
        print(f"[scheduler] now is after {args.until}, exit.")
        return

    if now < t_0915:
        print("[scheduler] waiting until 09:15 ...")
        sleep_until(t_0915)

    repair_missing = not args.no_repair_missing

    # Auction phase: fixed snapshots at 09:15 / 09:20 / 09:25
    for slot in [t_0915, t_0920, t_0925]:
        if slot < t_until:
            run_at_planned_time(
                slot,
                args.period,
                args.lookback_days,
                args.out_dir,
                args.download_history,
                repair_missing,
            )

    # Phase 2A: every 10 minutes from 09:30 to 11:30
    if dt.datetime.now() < t_0930:
        sleep_until(t_0930)
    cursor = max(dt.datetime.now(), t_0930)
    while cursor < min(t_1130, t_until):
        run_once(args.period, args.lookback_days, args.out_dir, args.download_history, repair_missing)
        cursor = dt.datetime.now() + dt.timedelta(minutes=10)
        if cursor >= min(t_1130, t_until):
            break
        sleep_until(cursor)

    # Midday break: skip 11:30-13:00
    if dt.datetime.now() < t_1300 and t_1300 < t_until:
        print("[scheduler] lunch break, waiting until 13:00 ...")
        sleep_until(t_1300)

    # Phase 2B: every 10 minutes from 13:00 to until
    cursor = max(dt.datetime.now(), t_1300)
    while cursor < t_until:
        run_once(args.period, args.lookback_days, args.out_dir, args.download_history, repair_missing)
        cursor = dt.datetime.now() + dt.timedelta(minutes=10)
        if cursor >= t_until:
            break
        sleep_until(cursor)

    print("[scheduler] done")


if __name__ == "__main__":
    main()
