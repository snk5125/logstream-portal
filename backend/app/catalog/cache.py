import json
from pathlib import Path


class SnapshotCache:
    """Last-good catalog snapshot on disk, with an optional bundled seed."""

    def __init__(self, path: Path, seed_path: Path | None = None):
        self._path = Path(path)
        self._seed = Path(seed_path) if seed_path else None

    def save(self, snapshot: dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(snapshot, indent=2))

    def load(self) -> dict | None:
        for candidate in (self._path, self._seed):
            if candidate and candidate.exists():
                try:
                    return json.loads(candidate.read_text())
                except (json.JSONDecodeError, OSError):
                    continue  # corrupt or unreadable; try the next candidate
        return None
