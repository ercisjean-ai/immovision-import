import sqlite3
from pathlib import Path
from typing import Any

from config import utcnow_iso
from normalization import build_listing_payload, normalize_feed_listing
from storage_base import (
    ListingUpsertResult,
    StorageBackend,
    build_effective_item,
    compute_observation_change,
    merge_listing_payload,
    serialize_changed_fields,
)


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
                source_listing_id TEXT NOT NULL,
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
                copro_status TEXT,
                is_new_build INTEGER NOT NULL DEFAULT 0,
                is_live_data INTEGER NOT NULL DEFAULT 1,
                data_origin TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                notes TEXT,
                updated_at TEXT,
                UNIQUE(source_name, source_listing_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS normalized_listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                source_name TEXT NOT NULL,
                source_listing_id TEXT NOT NULL,
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
                copro_status TEXT,
                is_new_build INTEGER NOT NULL DEFAULT 0,
                is_live_data INTEGER NOT NULL DEFAULT 1,
                data_origin TEXT,
                last_seen_at TEXT,
                first_seen_at TEXT,
                observation_count INTEGER NOT NULL DEFAULT 0,
                last_observation_status TEXT,
                last_changed_at TEXT,
                last_changed_fields TEXT,
                last_price_change_at TEXT,
                UNIQUE(source_name, source_listing_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS listing_analysis (
                listing_id INTEGER PRIMARY KEY,
                zone_label TEXT,
                strategy_compatible INTEGER,
                strategy_label TEXT,
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
                investment_score_label TEXT,
                confidence_score REAL,
                confidence_label TEXT,
                confidence_reason TEXT,
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
            CREATE TABLE IF NOT EXISTS listing_observation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id INTEGER NOT NULL,
                source_name TEXT NOT NULL,
                source_listing_id TEXT,
                source_url TEXT NOT NULL,
                title TEXT,
                price REAL,
                commune TEXT,
                postal_code TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                observation_status TEXT NOT NULL DEFAULT 'seen',
                changed_fields TEXT,
                is_price_changed INTEGER NOT NULL DEFAULT 0,
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
        self._ensure_import_queue_columns()
        self._ensure_normalized_listing_columns()
        self._ensure_import_queue_identity()
        self._ensure_normalized_listing_identity()
        self._ensure_listing_analysis_columns()
        self._ensure_observation_history_columns()
        self._execute(
            """
            INSERT INTO sources (name)
            VALUES (?)
            ON CONFLICT(name) DO NOTHING
            """,
            ("Immoweb",),
        )

    def _get_table_sql(self, table_name: str) -> str:
        row = self.connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return str(row["sql"] or "") if row else ""

    def _has_unique_index(
        self,
        table_name: str,
        expected_columns: tuple[str, ...],
    ) -> bool:
        for index in self.connection.execute(f"PRAGMA index_list({table_name})"):
            if not index["unique"]:
                continue
            index_name = index["name"]
            columns = tuple(
                row["name"]
                for row in self.connection.execute(f"PRAGMA index_info({index_name})")
            )
            if columns == expected_columns:
                return True
        return False

    def _ensure_import_queue_identity(self) -> None:
        sql = self._get_table_sql("import_queue")
        if self._has_unique_index("import_queue", ("source_name", "source_listing_id")) and (
            "source_listing_id TEXT NOT NULL UNIQUE" not in sql
        ):
            return

        self.connection.execute("DROP TABLE IF EXISTS import_queue_v2")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS import_queue_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                source_listing_id TEXT NOT NULL,
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
                copro_status TEXT,
                is_new_build INTEGER NOT NULL DEFAULT 0,
                is_live_data INTEGER NOT NULL DEFAULT 1,
                data_origin TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                notes TEXT,
                updated_at TEXT,
                UNIQUE(source_name, source_listing_id)
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO import_queue_v2 (
                id,
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
                copro_status,
                is_new_build,
                is_live_data,
                data_origin,
                is_active,
                notes,
                updated_at
            )
            SELECT
                id,
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
                copro_status,
                is_new_build,
                is_live_data,
                data_origin,
                is_active,
                notes,
                updated_at
            FROM import_queue
            """
        )
        self.connection.execute("DROP TABLE import_queue")
        self.connection.execute("ALTER TABLE import_queue_v2 RENAME TO import_queue")
        self.connection.commit()

    def _ensure_normalized_listing_identity(self) -> None:
        sql = self._get_table_sql("normalized_listings")
        has_source_name_column = any(
            row["name"] == "source_name"
            for row in self.connection.execute("PRAGMA table_info(normalized_listings)")
        )
        if (
            has_source_name_column
            and self._has_unique_index(
                "normalized_listings",
                ("source_name", "source_listing_id"),
            )
            and "source_listing_id TEXT NOT NULL UNIQUE" not in sql
        ):
            return

        self.connection.execute("DROP TABLE IF EXISTS normalized_listings_v2")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS normalized_listings_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                source_name TEXT NOT NULL,
                source_listing_id TEXT NOT NULL,
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
                copro_status TEXT,
                is_new_build INTEGER NOT NULL DEFAULT 0,
                is_live_data INTEGER NOT NULL DEFAULT 1,
                data_origin TEXT,
                last_seen_at TEXT,
                first_seen_at TEXT,
                observation_count INTEGER NOT NULL DEFAULT 0,
                last_observation_status TEXT,
                last_changed_at TEXT,
                last_changed_fields TEXT,
                last_price_change_at TEXT,
                UNIQUE(source_name, source_listing_id)
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO normalized_listings_v2 (
                id,
                source_id,
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
                copro_status,
                is_new_build,
                is_live_data,
                data_origin,
                last_seen_at,
                first_seen_at,
                observation_count,
                last_observation_status,
                last_changed_at,
                last_changed_fields,
                last_price_change_at
            )
            SELECT
                normalized_listings.id,
                normalized_listings.source_id,
                COALESCE(sources.name, 'Unknown') AS source_name,
                normalized_listings.source_listing_id,
                normalized_listings.source_url,
                normalized_listings.title,
                normalized_listings.description,
                normalized_listings.price,
                normalized_listings.postal_code,
                normalized_listings.commune,
                normalized_listings.property_type,
                normalized_listings.transaction_type,
                normalized_listings.existing_units,
                normalized_listings.surface,
                normalized_listings.is_copro,
                normalized_listings.copro_status,
                normalized_listings.is_new_build,
                normalized_listings.is_live_data,
                normalized_listings.data_origin,
                normalized_listings.last_seen_at,
                normalized_listings.first_seen_at,
                normalized_listings.observation_count,
                normalized_listings.last_observation_status,
                normalized_listings.last_changed_at,
                normalized_listings.last_changed_fields,
                normalized_listings.last_price_change_at
            FROM normalized_listings
            LEFT JOIN sources ON sources.id = normalized_listings.source_id
            """
        )
        self.connection.execute("DROP TABLE normalized_listings")
        self.connection.execute(
            "ALTER TABLE normalized_listings_v2 RENAME TO normalized_listings"
        )
        self.connection.commit()

    def _ensure_listing_analysis_columns(self) -> None:
        columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(listing_analysis)")
        }
        if "investment_score_label" not in columns:
            self.connection.execute(
                "ALTER TABLE listing_analysis ADD COLUMN investment_score_label TEXT"
            )
            self.connection.commit()
        if "confidence_score" not in columns:
            self.connection.execute(
                "ALTER TABLE listing_analysis ADD COLUMN confidence_score REAL"
            )
            self.connection.commit()
        if "confidence_label" not in columns:
            self.connection.execute(
                "ALTER TABLE listing_analysis ADD COLUMN confidence_label TEXT"
            )
            self.connection.commit()
        if "confidence_reason" not in columns:
            self.connection.execute(
                "ALTER TABLE listing_analysis ADD COLUMN confidence_reason TEXT"
            )
            self.connection.commit()
        if "strategy_label" not in columns:
            self.connection.execute(
                "ALTER TABLE listing_analysis ADD COLUMN strategy_label TEXT"
            )
            self.connection.commit()

    def _ensure_import_queue_columns(self) -> None:
        columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(import_queue)")
        }
        if "copro_status" not in columns:
            self.connection.execute(
                "ALTER TABLE import_queue ADD COLUMN copro_status TEXT"
            )
            self.connection.commit()
        if "data_origin" not in columns:
            self.connection.execute(
                "ALTER TABLE import_queue ADD COLUMN data_origin TEXT"
            )
            self.connection.commit()
        self.connection.execute(
            """
            UPDATE import_queue
            SET copro_status = 'true'
            WHERE copro_status IS NULL AND is_copro = 1
            """
        )
        self.connection.execute(
            """
            UPDATE import_queue
            SET data_origin = 'seed'
            WHERE data_origin IS NULL
              AND LOWER(COALESCE(notes, '')) LIKE '%seed local%'
            """
        )
        self.connection.execute(
            """
            UPDATE import_queue
            SET data_origin = 'test'
            WHERE data_origin IS NULL
              AND (
                LOWER(COALESCE(source_url, '')) LIKE '%example.test%'
                OR LOWER(COALESCE(notes, '')) LIKE 'cas test%'
                OR LOWER(COALESCE(notes, '')) LIKE 'test%'
              )
            """
        )
        self.connection.execute(
            """
            UPDATE import_queue
            SET data_origin = 'file_feed'
            WHERE data_origin IS NULL
              AND LOWER(COALESCE(source_name, '')) IN ('filefeed', 'manualfeed')
            """
        )
        self.connection.commit()

    def _ensure_normalized_listing_columns(self) -> None:
        columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(normalized_listings)")
        }
        if "source_name" not in columns:
            self.connection.execute(
                "ALTER TABLE normalized_listings ADD COLUMN source_name TEXT"
            )
            self.connection.commit()
        self.connection.execute(
            """
            UPDATE normalized_listings
            SET source_name = (
                SELECT sources.name
                FROM sources
                WHERE sources.id = normalized_listings.source_id
            )
            WHERE source_name IS NULL OR source_name = ''
            """
        )
        self.connection.commit()
        if "first_seen_at" not in columns:
            self.connection.execute(
                "ALTER TABLE normalized_listings ADD COLUMN first_seen_at TEXT"
            )
            self.connection.commit()
        if "observation_count" not in columns:
            self.connection.execute(
                "ALTER TABLE normalized_listings ADD COLUMN observation_count INTEGER NOT NULL DEFAULT 0"
            )
            self.connection.commit()
        if "last_observation_status" not in columns:
            self.connection.execute(
                "ALTER TABLE normalized_listings ADD COLUMN last_observation_status TEXT"
            )
            self.connection.commit()
        if "last_changed_at" not in columns:
            self.connection.execute(
                "ALTER TABLE normalized_listings ADD COLUMN last_changed_at TEXT"
            )
            self.connection.commit()
        if "last_changed_fields" not in columns:
            self.connection.execute(
                "ALTER TABLE normalized_listings ADD COLUMN last_changed_fields TEXT"
            )
            self.connection.commit()
        if "last_price_change_at" not in columns:
            self.connection.execute(
                "ALTER TABLE normalized_listings ADD COLUMN last_price_change_at TEXT"
            )
            self.connection.commit()
        if "copro_status" not in columns:
            self.connection.execute(
                "ALTER TABLE normalized_listings ADD COLUMN copro_status TEXT"
            )
            self.connection.commit()
        if "data_origin" not in columns:
            self.connection.execute(
                "ALTER TABLE normalized_listings ADD COLUMN data_origin TEXT"
            )
            self.connection.commit()
        self.connection.execute(
            """
            UPDATE normalized_listings
            SET copro_status = 'true'
            WHERE copro_status IS NULL AND is_copro = 1
            """
        )
        self.connection.execute(
            """
            UPDATE normalized_listings
            SET data_origin = 'seed'
            WHERE data_origin IS NULL
              AND source_listing_id = '12345678'
              AND LOWER(COALESCE(title, '')) LIKE '%test local%'
            """
        )
        self.connection.execute(
            """
            UPDATE normalized_listings
            SET data_origin = 'test'
            WHERE data_origin IS NULL
              AND LOWER(COALESCE(source_url, '')) LIKE '%example.test%'
            """
        )
        self.connection.execute(
            """
            UPDATE normalized_listings
            SET data_origin = 'file_feed'
            WHERE data_origin IS NULL
              AND LOWER(COALESCE(source_name, '')) IN ('filefeed', 'manualfeed')
            """
        )
        self.connection.commit()

    def _ensure_observation_history_columns(self) -> None:
        columns = {
            row["name"]
            for row in self.connection.execute(
                "PRAGMA table_info(listing_observation_history)"
            )
        }
        if "observation_status" not in columns:
            self.connection.execute(
                "ALTER TABLE listing_observation_history ADD COLUMN observation_status TEXT NOT NULL DEFAULT 'seen'"
            )
            self.connection.commit()
        if "changed_fields" not in columns:
            self.connection.execute(
                "ALTER TABLE listing_observation_history ADD COLUMN changed_fields TEXT"
            )
            self.connection.commit()
        if "is_price_changed" not in columns:
            self.connection.execute(
                "ALTER TABLE listing_observation_history ADD COLUMN is_price_changed INTEGER NOT NULL DEFAULT 0"
            )
            self.connection.commit()

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
                """
                SELECT id
                FROM import_queue
                WHERE source_name = ? AND source_listing_id = ?
                """,
                (item["source_name"], source_listing_id),
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
                    data_origin,
                    notes,
                    updated_at
                )
                VALUES (?, ?, ?, 1, 1, ?, ?, ?)
                """,
                (
                    item["source_name"],
                    source_listing_id,
                    item["source_url"],
                    "live",
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

    def upsert_listing(self, item: dict[str, Any]) -> ListingUpsertResult:
        source_id = self.get_source_id(item["source_name"])
        existing = self._fetchone(
            """
            SELECT *
            FROM normalized_listings
            WHERE source_name = ? AND source_listing_id = ?
            """,
            (item["source_name"], item["source_listing_id"]),
        )
        payload = build_listing_payload(item, source_id)
        merged_payload = merge_listing_payload(existing, payload)
        observation_status, changed_fields, is_price_changed = (
            compute_observation_change(existing, merged_payload)
        )
        now = utcnow_iso()
        first_seen_at = (existing or {}).get("first_seen_at") or now
        previous_observation_count = int((existing or {}).get("observation_count") or 0)
        merged_payload["first_seen_at"] = first_seen_at
        merged_payload["observation_count"] = previous_observation_count + 1
        merged_payload["last_observation_status"] = observation_status
        merged_payload["last_changed_at"] = (
            now
            if observation_status in {"new", "modified"}
            else (existing or {}).get("last_changed_at")
        )
        merged_payload["last_changed_fields"] = serialize_changed_fields(changed_fields)
        merged_payload["last_price_change_at"] = (
            now
            if is_price_changed
            else (existing or {}).get("last_price_change_at")
        )
        self._execute(
            """
            INSERT INTO normalized_listings (
                source_id,
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
                copro_status,
                is_new_build,
                is_live_data,
                data_origin,
                last_seen_at,
                first_seen_at,
                observation_count,
                last_observation_status,
                last_changed_at,
                last_changed_fields,
                last_price_change_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_name, source_listing_id) DO UPDATE SET
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
                copro_status = excluded.copro_status,
                is_new_build = excluded.is_new_build,
                is_live_data = excluded.is_live_data,
                data_origin = excluded.data_origin,
                last_seen_at = excluded.last_seen_at,
                first_seen_at = excluded.first_seen_at,
                observation_count = excluded.observation_count,
                last_observation_status = excluded.last_observation_status,
                last_changed_at = excluded.last_changed_at,
                last_changed_fields = excluded.last_changed_fields,
                last_price_change_at = excluded.last_price_change_at
            """,
            (
                merged_payload["source_id"],
                merged_payload["source_name"],
                merged_payload["source_listing_id"],
                merged_payload["source_url"],
                merged_payload["title"],
                merged_payload["description"],
                merged_payload["price"],
                merged_payload["postal_code"],
                merged_payload["commune"],
                merged_payload["property_type"],
                merged_payload["transaction_type"],
                merged_payload["existing_units"],
                merged_payload["surface"],
                int(bool(merged_payload["is_copro"])),
                merged_payload.get("copro_status"),
                int(bool(merged_payload["is_new_build"])),
                int(bool(merged_payload["is_live_data"])),
                merged_payload.get("data_origin"),
                merged_payload["last_seen_at"],
                merged_payload["first_seen_at"],
                merged_payload["observation_count"],
                merged_payload["last_observation_status"],
                merged_payload["last_changed_at"],
                merged_payload["last_changed_fields"],
                merged_payload["last_price_change_at"],
            ),
        )
        row = self._fetchone(
            """
            SELECT id
            FROM normalized_listings
            WHERE source_name = ? AND source_listing_id = ?
            """,
            (merged_payload["source_name"], merged_payload["source_listing_id"]),
        )
        if row is None:
            raise RuntimeError("Impossible de retrouver le listing apres upsert SQLite.")
        return ListingUpsertResult(
            listing_id=str(row["id"]),
            observation_status=observation_status,
            changed_fields=changed_fields,
            is_price_changed=is_price_changed,
            effective_item=build_effective_item(item, merged_payload),
        )

    def upsert_analysis(self, analysis_payload: dict[str, Any]) -> None:
        self._execute(
            """
            INSERT INTO listing_analysis (
                listing_id,
                zone_label,
                strategy_compatible,
                strategy_label,
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
                investment_score_label,
                confidence_score,
                confidence_label,
                confidence_reason,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_id) DO UPDATE SET
                zone_label = excluded.zone_label,
                strategy_compatible = excluded.strategy_compatible,
                strategy_label = excluded.strategy_label,
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
                investment_score_label = excluded.investment_score_label,
                confidence_score = excluded.confidence_score,
                confidence_label = excluded.confidence_label,
                confidence_reason = excluded.confidence_reason,
                updated_at = excluded.updated_at
            """,
            (
                int(analysis_payload["listing_id"]),
                analysis_payload["zone_label"],
                int(bool(analysis_payload["strategy_compatible"])),
                analysis_payload.get("strategy_label"),
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
                analysis_payload.get("investment_score_label"),
                analysis_payload.get("confidence_score"),
                analysis_payload.get("confidence_label"),
                analysis_payload.get("confidence_reason"),
                analysis_payload["updated_at"],
            ),
        )

    def insert_observation_history(
        self,
        upsert_result: ListingUpsertResult,
        item: dict[str, Any],
    ) -> None:
        self._execute(
            """
            INSERT INTO listing_observation_history (
                listing_id,
                source_name,
                source_listing_id,
                source_url,
                title,
                price,
                commune,
                postal_code,
                is_active,
                observation_status,
                changed_fields,
                is_price_changed,
                observed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(upsert_result.listing_id),
                item["source_name"],
                item.get("source_listing_id"),
                item["source_url"],
                item.get("title"),
                item.get("price"),
                item.get("commune"),
                item.get("postal_code"),
                int(bool(item.get("is_active", True))),
                upsert_result.observation_status,
                serialize_changed_fields(upsert_result.changed_fields),
                int(bool(upsert_result.is_price_changed)),
                utcnow_iso(),
            ),
        )

    def insert_price_history(self, listing_id: str, price: Any) -> None:
        if price is None:
            return

        latest = self._fetchone(
            """
            SELECT price
            FROM listing_price_history
            WHERE listing_id = ?
            ORDER BY observed_at DESC, id DESC
            LIMIT 1
            """,
            (int(listing_id),),
        )
        if latest and float(latest["price"]) == float(price):
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
        normalized_item = normalize_feed_listing(
            item,
            default_source_name=item.get("source_name") or "FileFeed",
        )
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
                copro_status,
                is_new_build,
                is_live_data,
                data_origin,
                is_active,
                notes,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_name, source_listing_id) DO UPDATE SET
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
                copro_status = excluded.copro_status,
                is_new_build = excluded.is_new_build,
                is_live_data = excluded.is_live_data,
                data_origin = excluded.data_origin,
                is_active = excluded.is_active,
                notes = excluded.notes,
                updated_at = excluded.updated_at
            """,
            (
                normalized_item["source_name"],
                normalized_item["source_listing_id"],
                normalized_item["source_url"],
                normalized_item.get("title"),
                normalized_item.get("description"),
                normalized_item.get("price"),
                normalized_item.get("postal_code"),
                normalized_item.get("commune"),
                normalized_item.get("property_type"),
                normalized_item.get("transaction_type"),
                normalized_item.get("existing_units"),
                normalized_item.get("surface"),
                int(bool(normalized_item.get("is_copro", False))),
                normalized_item.get("copro_status"),
                int(bool(normalized_item.get("is_new_build", False))),
                int(bool(normalized_item.get("is_live_data", True))),
                normalized_item.get("data_origin"),
                int(bool(normalized_item.get("is_active", True))),
                normalized_item.get("notes"),
                normalized_item.get("updated_at") or utcnow_iso(),
            ),
        )
