from __future__ import annotations

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.provider import LLMResponse, ProviderRequest
from astrbot.api.star import Context, Star, register

try:
    from .mnemo_constants import (
        LLM_REQUEST_INJECTION_PRIORITY,
        LLM_REQUEST_OBSERVER_PRIORITY,
        PLUGIN_AUTHOR,
        PLUGIN_NAME,
        PLUGIN_REPO,
        PLUGIN_VERSION,
    )
    from .mnemo_scheduler import BackgroundScheduler
    from .mnemo_service import MnemosyneService
except ImportError:
    from mnemo_constants import (
        LLM_REQUEST_INJECTION_PRIORITY,
        LLM_REQUEST_OBSERVER_PRIORITY,
        PLUGIN_AUTHOR,
        PLUGIN_NAME,
        PLUGIN_REPO,
        PLUGIN_VERSION,
    )
    from mnemo_scheduler import BackgroundScheduler
    from mnemo_service import MnemosyneService


@register(
    PLUGIN_NAME,
    PLUGIN_AUTHOR,
    "一个面向情感追踪、记忆、日记与陪伴场景的 AstrBot 插件。",
    PLUGIN_VERSION,
    PLUGIN_REPO,
)
class MnemosynePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.service = MnemosyneService(context, config)
        self.scheduler = BackgroundScheduler(self.service)

    async def initialize(self):
        await self.service.initialize()
        self.scheduler.start()
        logger.info("%s loaded.", PLUGIN_NAME)

    @filter.on_llm_request(priority=LLM_REQUEST_OBSERVER_PRIORITY)
    async def observe_llm_request(
        self, event: AstrMessageEvent, req: ProviderRequest
    ) -> None:
        await self.service.observe_llm_request(event, req)

    @filter.on_llm_request(priority=LLM_REQUEST_INJECTION_PRIORITY)
    async def on_llm_request(
        self, event: AstrMessageEvent, req: ProviderRequest
    ) -> None:
        await self.service.on_llm_request(event, req)

    @filter.on_llm_response()
    async def on_llm_response(
        self, event: AstrMessageEvent, resp: LLMResponse
    ) -> None:
        await self.service.on_llm_response(event, resp)

    @filter.after_message_sent()
    async def after_message_sent(self, event: AstrMessageEvent) -> None:
        await self.service.after_message_sent(event)

    @filter.command("mnemosyne")
    async def mnemosyne(self, event: AstrMessageEvent):
        lines = await self.service.get_status_lines()
        yield event.plain_result("\n".join(lines))

    @filter.command("mnemo_ping")
    async def mnemo_ping(self, event: AstrMessageEvent):
        yield event.plain_result("mnemosyne 插件运行正常。")

    async def terminate(self):
        await self.scheduler.stop()
        logger.info("%s unloaded.", PLUGIN_NAME)
