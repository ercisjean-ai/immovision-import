from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

CollectFunc = Callable[..., list[dict[str, Any]]]


@dataclass(frozen=True)
class SourceConnector:
    slug: str
    source_name: str
    description: str
    default_output_path: Path
    fixture_path: Path | None
    collect: CollectFunc
