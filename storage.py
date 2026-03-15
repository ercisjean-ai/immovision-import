from config import RuntimeConfig
from sqlite_storage import SQLiteStorage
from storage_base import StorageBackend
from supabase_storage import SupabaseStorage

__all__ = [
    "StorageBackend",
    "SQLiteStorage",
    "SupabaseStorage",
    "build_storage",
]


def build_storage(config: RuntimeConfig) -> StorageBackend:
    if config.backend_name == "supabase":
        return SupabaseStorage(config.supabase_url or "", config.supabase_key or "")
    if config.sqlite_path is None:
        raise RuntimeError("Chemin SQLite manquant.")
    return SQLiteStorage(config.sqlite_path)
