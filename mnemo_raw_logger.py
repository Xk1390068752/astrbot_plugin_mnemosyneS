from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any


class RawLLMLogger:
    def __init__(self, log_path: Path):
        self.log_path = log_path
        self._lock = asyncio.Lock()

    async def append(self, *, stage: str, payload: dict[str, Any]) -> None:
        # 使用 jsonl 追加写入，方便后面直接按时间顺序 grep / tail / 导入分析。
        record = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "stage": stage,
            "payload": payload,
        }
        async with self._lock:
            await asyncio.to_thread(self._append_sync, record)

    def _append_sync(self, record: dict[str, Any]) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
