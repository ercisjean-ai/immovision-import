import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_SQLITE_FILENAME = "immovision-local.db"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class RuntimeConfig:
    backend_name: str
    supabase_url: str | None = None
    supabase_key: str | None = None
    sqlite_path: Path | None = None


def resolve_sqlite_path(raw_value: str | None = None) -> Path:
    if raw_value:
        return Path(raw_value).expanduser().resolve()
    return (PROJECT_ROOT / DEFAULT_SQLITE_FILENAME).resolve()


def load_config() -> RuntimeConfig:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if supabase_url and supabase_key:
        return RuntimeConfig(
            backend_name="supabase",
            supabase_url=supabase_url,
            supabase_key=supabase_key,
        )

    sqlite_path = resolve_sqlite_path(os.getenv("SQLITE_PATH"))
    return RuntimeConfig(backend_name="sqlite", sqlite_path=sqlite_path)
