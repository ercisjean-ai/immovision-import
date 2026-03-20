import json
from pathlib import Path
from typing import Any

from normalization import normalize_feed_listing, resolve_data_origin


def load_listing_feed(
    path: str | Path,
    default_source_name: str = "FileFeed",
) -> list[dict[str, Any]]:
    feed_path = Path(path)
    suffix = feed_path.suffix.lower()
    content = feed_path.read_text(encoding="utf-8")

    if suffix == ".jsonl":
        raw_items = _load_jsonl(content)
    elif suffix == ".json":
        raw_items = _load_json(content)
    else:
        raise ValueError(
            f"Format non supporte pour {feed_path.name}. Utilise .jsonl ou .json."
        )

    normalized_items: list[dict[str, Any]] = []
    for raw_item in raw_items:
        detected_origin = resolve_data_origin(
            raw_item,
            default_source_name=default_source_name,
        )
        normalized_items.append(
            normalize_feed_listing(
                {
                    **raw_item,
                    # Any local file import must stay visibly non-live unless
                    # an upstream connector already tagged the item explicitly.
                    "data_origin": raw_item.get("data_origin")
                    or (
                        detected_origin
                        if detected_origin != "unknown"
                        else "file_feed"
                    ),
                },
                default_source_name=default_source_name,
            )
        )
    return normalized_items


def _load_jsonl(content: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        items.append(json.loads(stripped))
    return items


def _load_json(content: str) -> list[dict[str, Any]]:
    data = json.loads(content)
    if isinstance(data, list):
        return data
    raise ValueError("Le fichier JSON doit contenir une liste d'annonces.")
