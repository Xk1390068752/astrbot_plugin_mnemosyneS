from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any


PLACEHOLDER_RE = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")


def render_template(template: str, values: dict[str, Any]) -> str:
    # 这里只做最轻量的占位符替换，不引入复杂模板引擎，
    # 方便你直接维护 prompts.json。
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
        # 首次启动时，把插件内置模板复制到 data/plugin_data 供用户长期维护。
        self.user_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.user_path.exists():
            shutil.copyfile(self.template_path, self.user_path)
        return self.user_path

    def load(self) -> dict[str, Any]:
        # prompts.json 支持运行中直接修改，这里按 mtime 做一个轻量缓存。
        self.ensure_user_file()
        stat = self.user_path.stat()
        if self._cache is not None and self._mtime == stat.st_mtime:
            return self._cache

        payload = json.loads(self.user_path.read_text(encoding="utf-8"))
        payload = self._normalize_payload(payload)
        self._cache = payload
        self._mtime = stat.st_mtime
        return payload

    def _normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        # 模板字段既支持字符串，也支持字符串数组；
        # 数组写法更利于人工编辑和版本对比。
        chat = payload.get("chat")
        if isinstance(chat, dict):
            chat["inject_template"] = self._normalize_template_value(
                chat.get("inject_template", "")
            )

        background = payload.get("background")
        if isinstance(background, dict):
            background["journal_template"] = self._normalize_template_value(
                background.get("journal_template", "")
            )
            background["active_push_template"] = self._normalize_template_value(
                background.get("active_push_template", "")
            )

        return payload

    def _normalize_template_value(self, value: Any) -> str:
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            return "\n".join(value)
        if value is None:
            return ""
        return str(value)
