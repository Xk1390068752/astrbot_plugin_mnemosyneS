from __future__ import annotations

import asyncio

from astrbot.api import logger


class BackgroundScheduler:
    def __init__(self, service):
        self.service = service
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    def start(self) -> None:
        # 只允许存在一个后台轮询任务，防止插件重载时重复启动。
        if self._task and not self._task.done():
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._runner(), name="mnemosyne-scheduler")

    async def stop(self) -> None:
        # 通过 Event + cancel 双保险，让停止过程更稳一些。
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _runner(self) -> None:
        # 调度器本身不做业务决策，只按固定间隔触发 service.scheduler_tick。
        while not self._stopped.is_set():
            try:
                await self.service.scheduler_tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("mnemosyne scheduler failed: %s", exc, exc_info=True)

            try:
                await asyncio.wait_for(
                    self._stopped.wait(),
                    timeout=self.service._poll_seconds(),
                )
            except asyncio.TimeoutError:
                continue
