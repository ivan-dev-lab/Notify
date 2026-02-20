from __future__ import annotations

import json
import logging
from pathlib import Path

from auto_eye.models import AutoEyeState

logger = logging.getLogger(__name__)


def resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return Path.cwd() / path


class AutoEyeStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> AutoEyeState:
        if not self.path.exists():
            logger.info("AutoEye state file not found, starting from empty: %s", self.path)
            return AutoEyeState.empty()

        with self.path.open("r", encoding="utf-8") as file:
            raw = json.load(file)

        if not isinstance(raw, dict):
            logger.warning("Invalid AutoEye state format, starting from empty")
            return AutoEyeState.empty()

        state = AutoEyeState.from_dict(raw)
        logger.info("Loaded AutoEye state: elements=%s", len(state.elements))
        return state

    def save(self, state: AutoEyeState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as file:
            json.dump(state.to_dict(), file, ensure_ascii=False, indent=2)
        logger.info("Saved AutoEye state: %s", self.path)
