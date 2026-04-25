from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(ENV_PATH)


@dataclass(frozen=True)
class MariaDBConfig:
    host: str = os.getenv("MARIADB_HOST", "192.168.1.251")
    port: int = int(os.getenv("MARIADB_PORT", "3307"))
    user: str = os.getenv("MARIADB_USER", "root")
    password: str = os.getenv("MARIADB_PASSWORD", "weijie81")
    database: str = os.getenv("MARIADB_DATABASE", "stock")
    charset: str = "utf8mb4"


@dataclass(frozen=True)
class RuntimeConfig:
    tushare_token: str = os.getenv("TUSHARE_TOKEN", "")
    qmt_session_path: str = os.getenv("QMT_SESSION_PATH", r"D:\gjzqqmt\QMTclient")
    default_user_code: str = os.getenv("DEFAULT_USER_CODE", "user001")


MARIADB = MariaDBConfig()
RUNTIME = RuntimeConfig()

