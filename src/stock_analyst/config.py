from __future__ import annotations

import os
from pathlib import Path
from dataclasses import dataclass

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(ENV_PATH)


@dataclass(frozen=True)
class RuntimeConfig:
    tushare_token: str = os.getenv("TUSHARE_TOKEN", "")
    qmt_session_path: str = os.getenv("QMT_SESSION_PATH", r"D:\gjzqqmt\QMTclient")
    default_user_code: str = os.getenv("DEFAULT_USER_CODE", "user001")


RUNTIME = RuntimeConfig()
