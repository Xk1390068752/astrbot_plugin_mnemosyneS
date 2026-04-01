from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any


PLACEHOLDER_RE = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")


def render_template(template: str, values: dict[str, Any]) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        value = values.get(key, "")
        if value is None:
            return ""
        return str(value)

    return PLACEHOLDER_RE.sub(repl, template or "")


class PromptStore:
    def __init__(self, template_path: Path, user_path: Path):
        self.template_path = template_path
        self.user_path = user_path
        self._cache: dict[str, Any] | None = None
        self._mtime: float | None = None

    def ensure_user_file(self) -> Path:
        self.user_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.user_path.exists():
            shutil.copyfile(self.template_path, self.user_path)
        return self.user_path

    def load(self) -> dict[str, Any]:
        self.ensure_user_file()
        stat = self.user_path.stat()
        if self._cache is not None and self._mtime == stat.st_mtime:
            return self._cache

        payload = json.loads(self.user_path.read_text(encoding="utf-8"))
        self._cache = payload
        self._mtime = stat.st_mtime
        return payload
