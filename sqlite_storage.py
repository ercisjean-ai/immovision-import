import sqlite3
from pathlib import Path
from typing import Any

from config import utcnow_iso
from normalization import build_listing_payload
from storage_base import StorageBackend


class SQLiteStorage(StorageBackend):
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self._ensure_schema()

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        cursor = self.connection.execute(sql, params)
        self.connection.commit()
        return cursor

    def _fetchall(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> list[dict[str, Any]]:
        cursor = self.connection.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    def _fetchone(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> dict[str, Any] | None:
        cursor = self.connection.execute(sql, params)
        row = cursor.fetchone()
        return dict(row) if row else None

    def _ensure_schema(self) -> None:
        schema_statements = [
            """
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                live_count INTEGER NOT NULL DEFAULT 0,
                last_sync TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS search_targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                target_name TEXT,
                search_url TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS discovered_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                search_target_id INTEGER,
                source_url TEXT NOT NULL UNIQUE,
                source_listing_id TEXT,
                last_seen_at TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                is_queued INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS import_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                source_listing_id TEXT NOT NULL UNIQUE,
                source_url TEXT NOT NULL,
                title TEXT,
                description TEXT,
                price REAL,
                postal_code TEXT,
                commune TEXT,
                property_type TEXT,
                transaction_type TEXT,
                existing_units INTEGER,
                surface REAL,
                is_copro INTEGER NOT NULL DEFAULT 0,
                is_new_build INTEGER NOT NULL DEFAULT 0,
                is_live_data INTEGER NOT NULL DEFAULT 1,
                is_active INTEGER NOT NULL DEFAULT 1,
                notes TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS normalized_listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                source_listing_id TEXT NOT NULL UNIQUE,
                source_url TEXT NOT NULL,
                title TEXT,
                description TEXT,
                price REAL,
                postal_code TEXT,
                commune TEXT,
                property_type TEXT,
                transaction_type TEXT,
                existing_units INTEGER,
                surface REAL,
                is_copro INTEGER NOT NULL DEFAULT 0,
                is_new_build INTEGER NOT NULL DEFAULT 0,
                is_live_data INTEGER NOT NULL DEFAULT 1,
                last_seen_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS listing_analysis (
                listing_id INTEGER PRIMARY KEY,
                zone_label TEXT,
                strategy_compatible INTEGER,
                compatibility_reason TEXT,
                price_per_unit REAL,
                estimated_rent_per_unit REAL,
                estimated_total_rent_monthly REAL,
                estimated_total_rent_annual REAL,
                estimated_monthly_loan_payment REAL,
                estimated_gross_yield REAL,
                estimated_monthly_spread REAL,
                rental_score_label TEXT,
                investment_score REAL,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS listing_price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id INTEGER NOT NULL,
                price REAL NOT NULL,
                observed_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS source_syncs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                status TEXT NOT NULL,
                listings_found INTEGER NOT NULL,
                listings_imported INTEGER NOT NULL,
                error_message TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL
            )
            """,
        ]

        for statement in schema_statements:
            self.connection.execute(statement)

        self.connection.commit()
        self._execute(
            """
            INSERT INTO sources (name)
            VALUES (?)
            ON CONFLICT(name) DO NOTHING
            """,
            ("Immoweb",),
        )

    def get_source_id(self, source_name: str) -> str:
        row = self._fetchone("SELECT id FROM sources WHERE name = ?", (source_name,))
        if row is None:
            cursor = self._execute(
                "INSERT INTO sources (name) VALUES (?)",
                (source_name,),
            )
            return str(cursor.lastrowid)
        return str(row["id"])

    def fetch_search_targets(self) -> list[dict[str, Any]]:
        return self._fetchall(
            """
            SELECT id, source_name, target_name, search_url, is_active
            FROM search_targets
            WHERE is_active = 1
            ORDER BY id
            """
        )

    def fetch_import_queue(self) -> list[dict[str, Any]]:
        return self._fetchall(
            """
            SELECT *
            FROM import_queue
            WHERE is_active = 1
            ORDER BY id
            """
        )

    def upsert_discovered_url(self, item: dict[str, Any]) -> None:
        self._execute(
            """
            INSERT INTO discovered_urls (
                source_name,
                search_target_id,
                source_url,
                source_listing_id,
                last_seen_at,
                is_active,
                is_queued
            )
            VALUES (?, ?, ?, ?, ?, 1, 0)
            ON CONFLICT(source_url) DO UPDATE SET
                source_name = excluded.source_name,
                search_target_id = excluded.search_target_id,
                source_listing_id = excluded.source_listing_id,
                last_seen_at = excluded.last_seen_at,
                is_active = 1
            """,
            (
                item["source_name"],
                item["search_target_id"],
                item["source_url"],
                item["source_listing_id"],
                utcnow_iso(),
            ),
        )

    def queue_new_discoveries(self) -> int:
        discovered = self._fetchall(
            """
            SELECT *
            FROM discovered_urls
            WHERE is_active = 1 AND is_queued = 0
            ORDER BY id
            """
        )

        queued = 0
        for item in discovered:
            source_listing_id = item.get("source_listing_id")
            if not source_listing_id:
                continue

            existing = self._fetchone(
                "SELECT id FROM import_queue WHERE source_listing_id = ?",
                (source_listing_id,),
            )

            if existing:
                self._execute(
                    "UPDATE discovered_urls SET is_queued = 1 WHERE id = ?",
                    (item["id"],),
                )
                continue

            self._execute(
                """
                INSERT INTO import_queue (
                    source_name,
                    source_listing_id,
                    source_url,
                    is_active,
                    is_live_data,
                    notes,
                    updated_at
                )
                VALUES (?, ?, ?, 1, 1, ?, ?)
                """,
                (
                    item["source_name"],
                    source_listing_id,
                    item["source_url"],
                    "URL decouverte automatiquement depuis search_targets",
                    utcnow_iso(),
                ),
            )
            self._execute(
                "UPDATE discovered_urls SET is_queued = 1 WHERE id = ?",
                (item["id"],),
            )
            queued += 1

        return queued

    def upsert_listing(self, item: dict[str, Any]) -> str:
        source_id = self.get_source_id(item["source_name"])
        payload = build_listing_payload(item, source_id)
        self._execute(
            """
            INSERT INTO normalized_listings (
                source_id,
                source_listing_id,
                source_url,
                title,
                description,
                price,
                postal_code,
                commune,
                property_type,
                transaction_type,
                existing_units,
                surface,
                is_copro,
                is_new_build,
                is_live_data,
                last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_listing_id) DO UPDATE SET
                source_id = excluded.source_id,
                source_url = excluded.source_url,
                title = excluded.title,
                description = excluded.description,
                price = excluded.price,
                postal_code = excluded.postal_code,
                commune = excluded.commune,
                property_type = excluded.property_type,
                transaction_type = excluded.transaction_type,
                existing_units = excluded.existing_units,
                surface = excluded.surface,
                is_copro = excluded.is_copro,
                is_new_build = excluded.is_new_build,
                is_live_data = excluded.is_live_data,
                last_seen_at = excluded.last_seen_at
            """,
            (
                payload["source_id"],
                payload["source_listing_id"],
                payload["source_url"],
                payload["title"],
                payload["description"],
                payload["price"],
                payload["postal_code"],
                payload["commune"],
                payload["property_type"],
                payload["transaction_type"],
                payload["existing_units"],
                payload["surface"],
                int(bool(payload["is_copro"])),
                int(bool(payload["is_new_build"])),
                int(bool(payload["is_live_data"])),
                payload["last_seen_at"],
            ),
        )
        row = self._fetchone(
            """
            SELECT id
            FROM normalized_listings
            WHERE source_listing_id = ?
            """,
            (payload["source_listing_id"],),
        )
        if row is None:
            raise RuntimeError("Impossible de retrouver le listing apres upsert SQLite.")
        return str(row["id"])

    def upsert_analysis(self, analysis_payload: dict[str, Any]) -> None:
        self._execute(
            """
            INSERT INTO listing_analysis (
                listing_id,
                zone_label,
                strategy_compatible,
                compatibility_reason,
                price_per_unit,
                estimated_rent_per_unit,
                estimated_total_rent_monthly,
                estimated_total_rent_annual,
                estimated_monthly_loan_payment,
                estimated_gross_yield,
                estimated_monthly_spread,
                rental_score_label,
                investment_score,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_id) DO UPDATE SET
                zone_label = excluded.zone_label,
                strategy_compatible = excluded.strategy_compatible,
                compatibility_reason = excluded.compatibility_reason,
                price_per_unit = excluded.price_per_unit,
                estimated_rent_per_unit = excluded.estimated_rent_per_unit,
                estimated_total_rent_monthly = excluded.estimated_total_rent_monthly,
                estimated_total_rent_annual = excluded.estimated_total_rent_annual,
                estimated_monthly_loan_payment = excluded.estimated_monthly_loan_payment,
                estimated_gross_yield = excluded.estimated_gross_yield,
                estimated_monthly_spread = excluded.estimated_monthly_spread,
                rental_score_label = excluded.rental_score_label,
                investment_score = excluded.investment_score,
                updated_at = excluded.updated_at
            """,
            (
                int(analysis_payload["listing_id"]),
                analysis_payload["zone_label"],
                int(bool(analysis_payload["strategy_compatible"])),
                analysis_payload["compatibility_reason"],
                analysis_payload["price_per_unit"],
                analysis_payload["estimated_rent_per_unit"],
                analysis_payload["estimated_total_rent_monthly"],
                analysis_payload["estimated_total_rent_annual"],
                analysis_payload["estimated_monthly_loan_payment"],
                analysis_payload["estimated_gross_yield"],
                analysis_payload["estimated_monthly_spread"],
                analysis_payload["rental_score_label"],
                analysis_payload["investment_score"],
                analysis_payload["updated_at"],
            ),
        )

    def insert_price_history(self, listing_id: str, price: Any) -> None:
        if price is None:
            return

        self._execute(
            """
            INSERT INTO listing_price_history (listing_id, price, observed_at)
            VALUES (?, ?, ?)
            """,
            (int(listing_id), price, utcnow_iso()),
        )

    def update_source_counts(self) -> None:
        sources = self._fetchall("SELECT id, name FROM sources ORDER BY id")
        for source in sources:
            count_row = self._fetchone(
                """
                SELECT COUNT(*) AS total
                FROM normalized_listings
                WHERE source_id = ? AND is_live_data = 1
                """,
                (source["id"],),
            )
            self._execute(
                """
                UPDATE sources
                SET live_count = ?, last_sync = ?
                WHERE id = ?
                """,
                (count_row["total"], utcnow_iso(), source["id"]),
            )

    def insert_sync_log(
        self,
        status: str,
        listings_found: int,
        listings_imported: int,
        error_message: str | None = None,
    ) -> None:
        source_row = self._fetchone(
            "SELECT id FROM sources WHERE name = ?",
            ("Immoweb",),
        )
        self._execute(
            """
            INSERT INTO source_syncs (
                source_id,
                status,
                listings_found,
                listings_imported,
                error_message,
                started_at,
                finished_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_row["id"] if source_row else None,
                status,
                listings_found,
                listings_imported,
                error_message,
                utcnow_iso(),
                utcnow_iso(),
            ),
        )

    def seed_import_queue_item(self, item: dict[str, Any]) -> None:
        self._execute(
            """
            INSERT INTO import_queue (
                source_name,
                source_listing_id,
                source_url,
                title,
                description,
                price,
                postal_code,
                commune,
                property_type,
                transaction_type,
                existing_units,
                surface,
                is_copro,
                is_new_build,
                is_live_data,
                is_active,
                notes,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_listing_id) DO UPDATE SET
                source_url = excluded.source_url,
                title = excluded.title,
                description = excluded.description,
                price = excluded.price,
                postal_code = excluded.postal_code,
                commune = excluded.commune,
                property_type = excluded.property_type,
                transaction_type = excluded.transaction_type,
                existing_units = excluded.existing_units,
                surface = excluded.surface,
                is_copro = excluded.is_copro,
                is_new_build = excluded.is_new_build,
                is_live_data = excluded.is_live_data,
                is_active = excluded.is_active,
                notes = excluded.notes,
                updated_at = excluded.updated_at
            """,
            (
                item["source_name"],
                item["source_listing_id"],
                item["source_url"],
                item.get("title"),
                item.get("description"),
                item.get("price"),
                item.get("postal_code"),
                item.get("commune"),
                item.get("property_type"),
                item.get("transaction_type"),
                item.get("existing_units"),
                item.get("surface"),
                int(bool(item.get("is_copro", False))),
                int(bool(item.get("is_new_build", False))),
                int(bool(item.get("is_live_data", True))),
                int(bool(item.get("is_active", True))),
                item.get("notes"),
                item.get("updated_at") or utcnow_iso(),
            ),
        )
