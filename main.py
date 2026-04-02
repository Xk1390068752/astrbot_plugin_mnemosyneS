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
        # service 负责核心业务逻辑，scheduler 只负责定时触发后台生成。
        self.config = config
        self.service = MnemosyneService(context, config)
        self.scheduler = BackgroundScheduler(self.service)

    async def initialize(self):
        # 插件加载时先准备提示词文件与数据库，再启动后台轮询。
        await self.service.initialize()
        self.scheduler.start()
        logger.info("%s loaded.", PLUGIN_NAME)

    @filter.on_llm_request(priority=LLM_REQUEST_OBSERVER_PRIORITY)
    async def observe_llm_request(
        self, event: AstrMessageEvent, req: ProviderRequest
    ) -> None:
        # 预留一个超高优先级观察点，方便后续继续补更早期的诊断逻辑。
        await self.service.observe_llm_request(event, req)

    @filter.on_llm_request(priority=LLM_REQUEST_INJECTION_PRIORITY)
    async def on_llm_request(
        self, event: AstrMessageEvent, req: ProviderRequest
    ) -> None:
        # 主入口：匹配人格、接管上下文、注入记忆，并记录用户 turn。
        await self.service.on_llm_request(event, req)

    @filter.on_llm_response()
    async def on_llm_response(
        self, event: AstrMessageEvent, resp: LLMResponse
    ) -> None:
        # 处理模型回复里的隐藏块，并把 assistant 结果暂存到 event.extra。
        await self.service.on_llm_response(event, resp)

    @filter.after_message_sent()
    async def after_message_sent(self, event: AstrMessageEvent) -> None:
        # 等消息真正发出成功后，再补录 assistant turn 和状态更新。
        await self.service.after_message_sent(event)

    @filter.command("mnemosyne")
    async def mnemosyne(self, event: AstrMessageEvent):
        # 最小状态命令：便于在 AstrBot 内快速确认插件是否正常工作。
        lines = await self.service.get_status_lines_v2(event)
        yield event.plain_result("\n".join(lines))

    @filter.command("mnemo_ping")
    async def mnemo_ping(self, event: AstrMessageEvent):
        yield event.plain_result("mnemosyne 插件运行正常。")

    async def terminate(self):
        # 卸载时停掉后台轮询，避免遗留异步任务。
        await self.scheduler.stop()
        logger.info("%s unloaded.", PLUGIN_NAME)
