import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class RuntimeConfig:
    backend_name: str
    supabase_url: str | None = None
    supabase_key: str | None = None
    sqlite_path: Path | None = None


def load_config() -> RuntimeConfig:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if supabase_url and supabase_key:
        return RuntimeConfig(
            backend_name="supabase",
            supabase_url=supabase_url,
            supabase_key=supabase_key,
        )

    sqlite_path = Path(os.getenv("SQLITE_PATH", "immovision-local.db")).resolve()
    return RuntimeConfig(backend_name="sqlite", sqlite_path=sqlite_path)
