from __future__ import annotations

import json
import random
import re
import time
from typing import Any

from astrbot.api import logger
from astrbot.api.message_components import Plain
from astrbot.api.platform import MessageType

try:
    from .mnemo_constants import (
        CHARACTER_SCOPE,
        CHARACTER_SCOPE_KEY,
        DEFAULT_DB_FILENAME,
        DEFAULT_PROMPTS_FILENAME,
        DEFAULT_RAW_LOG_FILENAME,
        EXTRA_ENABLED,
        EXTRA_MATCHED_PERSONA,
        EXTRA_PENDING_ASSISTANT,
        EXTRA_PROVIDER_ID,
        EXTRA_SESSION_KEY,
        EXTRA_USER_TURN_ID,
        PLUGIN_NAME,
        SOURCE_BACKGROUND,
        SOURCE_CHAT,
        SOURCE_PUSH,
    )
    from .mnemo_parser import HiddenBlock, has_mnemosyne_meta, parse_mnemosyne_response
    from .mnemo_paths import get_default_prompts_template_path, resolve_user_path
    from .mnemo_prompts import PromptStore, render_template
    from .mnemo_raw_logger import RawLLMLogger
    from .mnemo_storage import MnemoStorage
except ImportError:
    from mnemo_constants import (
        CHARACTER_SCOPE,
        CHARACTER_SCOPE_KEY,
        DEFAULT_DB_FILENAME,
        DEFAULT_PROMPTS_FILENAME,
        DEFAULT_RAW_LOG_FILENAME,
        EXTRA_ENABLED,
        EXTRA_MATCHED_PERSONA,
        EXTRA_PENDING_ASSISTANT,
        EXTRA_PROVIDER_ID,
        EXTRA_SESSION_KEY,
        EXTRA_USER_TURN_ID,
        PLUGIN_NAME,
        SOURCE_BACKGROUND,
        SOURCE_CHAT,
        SOURCE_PUSH,
    )
    from mnemo_parser import HiddenBlock, has_mnemosyne_meta, parse_mnemosyne_response
    from mnemo_paths import get_default_prompts_template_path, resolve_user_path
    from mnemo_prompts import PromptStore, render_template
    from mnemo_raw_logger import RawLLMLogger
    from mnemo_storage import MnemoStorage


def _safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2)


def _to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]
    if hasattr(value, "model_dump"):
        try:
            return _to_jsonable(value.model_dump())
        except Exception:
            pass
    if hasattr(value, "dict"):
        try:
            return _to_jsonable(value.dict())
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        try:
            return _to_jsonable(vars(value))
        except Exception:
            pass
    return repr(value)


def _outline_memories(memories: list[dict[str, Any]]) -> str:
    if not memories:
        return "(none)"
    lines = []
    for item in memories:
        importance = item.get("importance", 0.5)
        memory_type = item.get("memory_type", "note")
        lines.append(f"- [{memory_type}|{importance:.2f}] {item.get('content', '')}")
    return "\n".join(lines)


def _outline_journals(journals: list[dict[str, Any]]) -> str:
    if not journals:
        return "(none)"
    lines = []
    for item in journals:
        content = (item.get("summary") or item.get("content") or "").strip()
        ts = item.get("generated_at", 0)
        lines.append(f"- [{int(ts)}] {content}")
    return "\n".join(lines)


def _outline_relation(relation: dict[str, Any]) -> str:
    cognition = relation.get("cognition", {})
    if not isinstance(cognition, dict):
        cognition = {}
    lines = [
        f"- favorability: {relation.get('favorability', 0)}",
        f"- relation_stage: {relation.get('relation_stage', '') or '(none)'}",
    ]
    for key in ("impression", "labels", "user_traits", "wants_from_user", "risk_flags"):
        value = cognition.get(key)
        if value in (None, "", [], {}):
            continue
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) if lines else "(none)"


def _outline_benefits(relation: dict[str, Any]) -> str:
    benefits = relation.get("benefits", [])
    if not isinstance(benefits, list) or not benefits:
        return "(none)"
    return "\n".join(f"- {str(item).strip()}" for item in benefits if str(item).strip()) or "(none)"


def _outline_turns_for_summary(turns: list[dict[str, Any]]) -> str:
    if not turns:
        return "(none)"
    lines: list[str] = []
    for turn in turns:
        role = str(turn.get("role", "") or "").strip() or "unknown"
        text_key = "visible_text" if role == "assistant" else "raw_text"
        text = str(turn.get(text_key, "") or "").strip()
        if not text:
            continue
        lines.append(f"[{role}] {text}")
    return "\n".join(lines) if lines else "(none)"


def _message_content_to_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                item_type = str(item.get("type", "") or "")
                if item_type == "text":
                    parts.append(str(item.get("text", "") or ""))
                elif item_type == "image_url":
                    image_url = item.get("image_url", {})
                    if isinstance(image_url, dict):
                        parts.append(f"[image] {image_url.get('url', '')}")
                    else:
                        parts.append(f"[image] {image_url}")
                else:
                    parts.append(json.dumps(_to_jsonable(item), ensure_ascii=False))
            else:
                parts.append(str(item))
        return "\n".join(part for part in parts if part)
    if isinstance(content, dict):
        return json.dumps(_to_jsonable(content), ensure_ascii=False, indent=2)
    return str(content)


def _flatten_contexts_text(contexts: Any) -> str:
    if not contexts:
        return ""
    lines: list[str] = []
    for index, item in enumerate(contexts, start=1):
        role = "unknown"
        content = item
        if isinstance(item, dict):
            role = str(item.get("role", "unknown"))
            content = item.get("content")
        else:
            role = getattr(item, "role", "unknown")
            content = getattr(item, "content", item)
        text = _message_content_to_text(content).strip()
        if not text:
            continue
        lines.append(f"[Context {index} | {role}]\n{text}")
    return "\n\n".join(lines)


def _flatten_extra_parts_text(extra_parts: Any) -> str:
    if not extra_parts:
        return ""
    lines: list[str] = []
    for index, part in enumerate(extra_parts, start=1):
        if hasattr(part, "text"):
            lines.append(f"[Extra {index}]\n{getattr(part, 'text', '')}")
        elif hasattr(part, "image_url"):
            image_url = getattr(part, "image_url", None)
            url = getattr(image_url, "url", image_url)
            lines.append(f"[Extra {index}]\n[image] {url}")
        elif isinstance(part, dict):
            lines.append(f"[Extra {index}]\n{json.dumps(_to_jsonable(part), ensure_ascii=False, indent=2)}")
        else:
            lines.append(f"[Extra {index}]\n{part}")
    return "\n\n".join(lines)


def _build_final_prompt_text(req) -> str:
    # 将请求的各个组成部分串成一段完整文本，便于直接落日志排查。
    sections: list[str] = []
    system_prompt = str(getattr(req, "system_prompt", "") or "").strip()
    if system_prompt:
        sections.append(f"[System Prompt]\n{system_prompt}")

    contexts_text = _flatten_contexts_text(getattr(req, "contexts", []))
    if contexts_text:
        sections.append(contexts_text)

    prompt = str(getattr(req, "prompt", "") or "").strip()
    if prompt:
        sections.append(f"[Prompt]\n{prompt}")

    extra_text = _flatten_extra_parts_text(getattr(req, "extra_user_content_parts", []))
    if extra_text:
        sections.append(extra_text)

    image_urls = getattr(req, "image_urls", []) or []
    if image_urls:
        sections.append("[Image URLs]\n" + "\n".join(str(url) for url in image_urls))

    return "\n\n".join(section for section in sections if section).strip()


def _conversation_context_session_key(req, event) -> str:
    # 插件自己的短期上下文优先绑定到 AstrBot 的 conversation_id，
    # 这样 /new、/del 造成的会话切换也会同步反映到插件上下文。
    conversation = getattr(req, "conversation", None)
    if conversation:
        cid = getattr(conversation, "cid", None) or getattr(
            conversation, "conversation_id", None
        )
        if cid:
            return str(cid)
    return str(event.unified_msg_origin)


def _turns_to_contexts(turns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # 一旦命中目标人格，就由插件自己的 mnemo_turn 重建历史上下文。
    # user 保留原文；assistant 只回放用户可见的 visible_text，避免隐藏标签再次污染上下文。
    contexts: list[dict[str, Any]] = []
    for turn in reversed(turns):
        role = str(turn.get("role", "") or "").strip()
        if role not in {"user", "assistant"}:
            continue
        text_key = "visible_text" if role == "assistant" else "raw_text"
        text = str(turn.get(text_key, "") or "").strip()
        if not text:
            continue
        contexts.append({"role": role, "content": text})
    return contexts


def _collect_text_fragments(value: Any, results: list[str]) -> None:
    if value is None:
        return
    if isinstance(value, str):
        text = value.strip()
        if text:
            results.append(text)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"text", "content"} and isinstance(item, str):
                text = item.strip()
                if text:
                    results.append(text)
                    continue
            _collect_text_fragments(item, results)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _collect_text_fragments(item, results)


def _extract_response_text(resp) -> str:
    # 日志优先尝试从 provider 原始回包里提取文本，
    # 提取不到时再退回到 AstrBot 暴露给插件的 completion_text。
    raw_completion = _to_jsonable(getattr(resp, "raw_completion", None))
    parts: list[str] = []
    _collect_text_fragments(raw_completion, parts)
    merged = "\n".join(part for part in parts if part).strip()
    if merged:
        return merged
    completion_text = str(getattr(resp, "completion_text", "") or "").strip()
    if completion_text:
        return completion_text
    result_chain = getattr(resp, "result_chain", None)
    if result_chain:
        try:
            plain = result_chain.get_plain_text().strip()
            if plain:
                return plain
        except Exception:
            pass
    return ""


def _usage_to_dict(resp) -> dict[str, int]:
    usage = getattr(resp, "usage", None)
    if usage is None:
        return {
            "input_tokens_other": 0,
            "input_tokens_cached": 0,
            "output_tokens": 0,
        }
    return {
        "input_tokens_other": max(int(getattr(usage, "input_other", 0) or 0), 0),
        "input_tokens_cached": max(int(getattr(usage, "input_cached", 0) or 0), 0),
        "output_tokens": max(int(getattr(usage, "output", 0) or 0), 0),
    }


def _serialize_blocks(blocks: list[HiddenBlock]) -> dict[str, Any]:
    # 隐藏块统一按名称归档，便于后续写入 turn 的 hidden_payload_json。
    payload: dict[str, list[dict[str, Any]]] = {}
    for block in blocks:
        payload.setdefault(block.name, []).append(
            {
                "target": block.target,
                "raw": block.raw,
                "payload": block.payload,
            }
        )
    return payload


def _filter_character_blocks(blocks: list[HiddenBlock]) -> list[HiddenBlock]:
    # 当前版本只允许角色侧的数据真正进入状态机和存储层。
    allowed_targets = {
        "character_state_patch",
        "character_emotion_patch",
        "user_relation_patch",
        "character_memory_append",
        "journal_entry",
    }
    return [block for block in blocks if block.target in allowed_targets]


def _extract_hidden_block_hits(text: str, specs: list[dict[str, Any]]) -> list[str]:
    hits: list[str] = []
    for spec in specs:
        pattern = str(spec.get("pattern", "") or "")
        name = str(spec.get("name", "") or spec.get("target", "") or "")
        if not pattern or not name:
            continue
        try:
            if re.search(pattern, text):
                hits.append(name)
        except re.error:
            continue
    return hits


def _mnemosyne_protocol_contract() -> str:
    # 这是一层硬协议兜底，用来提醒模型必须输出统一的 Mnemosyne 包装结构。
    return ("")


def _append_protocol_contract(text: str) -> str:
    base = (text or "").strip()
    contract = _mnemosyne_protocol_contract()
    if contract in base:
        return base
    return f"{base}\n\n{contract}".strip()


class MnemosyneService:
    def __init__(self, context, config):
        self.context = context
        self.config = config
        self._background_running = False

        self.db_path = resolve_user_path(
            self.config.get("database_path", ""), DEFAULT_DB_FILENAME
        )
        self.prompt_path = resolve_user_path(
            self.config.get("prompt_json_path", ""), DEFAULT_PROMPTS_FILENAME
        )
        self.raw_log_path = resolve_user_path(
            self.config.get("raw_llm_log_path", ""), DEFAULT_RAW_LOG_FILENAME
        )
        self.storage = MnemoStorage(self.db_path)
        self.prompt_store = PromptStore(
            get_default_prompts_template_path(),
            self.prompt_path,
        )
        self.raw_logger = RawLLMLogger(self.raw_log_path)

    async def initialize(self) -> None:
        # 准备 prompts.json 和 SQLite；如果文件不存在会自动创建。
        self.prompt_store.ensure_user_file()
        await self.storage.initialize()

    def is_enabled(self) -> bool:
        return bool(self.config.get("enabled", True))

    def _private_only(self) -> bool:
        return bool(self.config.get("private_only", True))

    def _scheduler_enabled(self) -> bool:
        return bool(self.config.get("enable_background_journal", True))

    def _raw_logging_enabled(self) -> bool:
        return bool(self.config.get("enable_raw_llm_logging", True))

    def _target_persona_id(self) -> str:
        return str(self.config.get("target_persona_id", "") or "").strip()

    def _memory_limit(self) -> int:
        return max(int(self.config.get("memory_window_size", 12)), 1)

    def _journal_limit(self) -> int:
        return max(int(self.config.get("journal_window_size", 6)), 1)

    def _turn_context_limit(self) -> int:
        return max(int(self.config.get("turn_context_limit", 12)), 1)

    def _summary_enabled(self) -> bool:
        return bool(self.config.get("enable_session_summary", True))

    def _summary_trigger_turns(self) -> int:
        fallback = self._turn_context_limit()
        return max(int(self.config.get("summary_trigger_turns", fallback)), 2)

    def _summary_compact_turns(self) -> int:
        fallback = max(self._turn_context_limit() // 2, 1)
        return max(int(self.config.get("summary_compact_turns", fallback)), 1)

    def _summary_max_chars(self) -> int:
        return max(int(self.config.get("summary_max_chars", 150)), 50)

    def _poll_seconds(self) -> int:
        return max(int(self.config.get("scheduler_poll_seconds", 120)), 30)

    def _idle_threshold_minutes(self) -> int:
        return max(int(self.config.get("idle_threshold_minutes", 90)), 5)

    def _background_cooldown_minutes(self) -> int:
        return max(
            int(self.config.get("background_generation_cooldown_minutes", 120)),
            10,
        )

    def _active_push_probability(self) -> float:
        probability = float(self.config.get("active_push_probability", 0.15))
        return max(0.0, min(probability, 1.0))

    def _active_push_cooldown_minutes(self) -> int:
        return max(int(self.config.get("active_push_cooldown_minutes", 240)), 10)

    def _provider_settings(self, event) -> dict[str, Any]:
        cfg = self.context.get_config(umo=event.unified_msg_origin)
        return cfg.get("provider_settings", {})

    async def _resolve_active_persona(self, event, conversation_persona_id: str | None):
        # 这里复用 AstrBot 自己的人格解析链，避免插件私自猜当前人格。
        provider_settings = self._provider_settings(event)
        return await self.context.persona_manager.resolve_selected_persona(
            umo=event.unified_msg_origin,
            conversation_persona_id=conversation_persona_id,
            platform_name=event.get_platform_name(),
            provider_settings=provider_settings,
        )

    async def _match_target_persona(self, event, conversation_persona_id: str | None):
        # 只有命中目标人格时插件才会介入；否则整个插件链路保持完全静默。
        if not self.is_enabled():
            return None
        if self._private_only() and event.get_message_type() != MessageType.FRIEND_MESSAGE:
            return None

        target_persona_id = self._target_persona_id()
        if not target_persona_id:
            return None

        persona_id, persona, _, _ = await self._resolve_active_persona(
            event, conversation_persona_id
        )
        if persona_id != target_persona_id:
            return None

        return {
            "persona_id": persona_id or "",
            "persona": persona or {},
        }

    async def _resolve_provider_id(self, event) -> str:
        # 优先用 AstrBot 当前会话绑定的 provider；拿不到时再退回旧接口。
        try:
            return await self.context.get_current_chat_provider_id(
                umo=event.unified_msg_origin
            )
        except Exception:
            provider = self.context.get_using_provider(event.unified_msg_origin)
            return provider.meta().id if provider else ""

    def _get_persona_prompt(self, persona_id: str) -> str:
        persona = self.context.persona_manager.get_persona_v3_by_id(persona_id) or {}
        if hasattr(persona, "get"):
            return str(persona.get("prompt", "") or "")
        return str(getattr(persona, "prompt", "") or "")

    async def _build_prompt_context(
        self, session_key: str, persona_id: str
    ) -> dict[str, Any]:
        # 注入给模型的是“角色当前快照 + 最近窗口”，不是全量历史。
        # 这样既能保留连续性，也能避免提示词无限膨胀。
        character_state = await self.storage.get_state(
            CHARACTER_SCOPE, CHARACTER_SCOPE_KEY
        )
        character_memories = await self.storage.list_recent_memories(
            CHARACTER_SCOPE,
            CHARACTER_SCOPE_KEY,
            self._memory_limit(),
        )
        recent_journals = await self.storage.list_recent_journals(self._journal_limit())

        return {
            "target_persona_id": persona_id,
            "current_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "character_state_json": _safe_json(character_state["state"]),
            "character_emotion_json": _safe_json(character_state["emotion"]),
            "user_state_json": "{}",
            "user_emotion_json": "{}",
            "character_memories_text": _outline_memories(character_memories),
            "user_memories_text": "(disabled)",
            "recent_journals_text": _outline_journals(recent_journals),
            "latest_journal_text": recent_journals[0]["content"] if recent_journals else "",
        }

    async def _build_prompt_context_v2(self, session: dict[str, Any]) -> dict[str, Any]:
        # 新版上下文会同时注入滚动摘要和角色对用户的长期认知。
        character_state = await self.storage.get_state(CHARACTER_SCOPE, CHARACTER_SCOPE_KEY)
        character_memories = await self.storage.list_recent_memories(
            CHARACTER_SCOPE,
            CHARACTER_SCOPE_KEY,
            self._memory_limit(),
        )
        recent_journals = await self.storage.list_recent_journals(self._journal_limit())
        session_summary = await self.storage.get_session_summary(session["session_key"])
        relation = await self.storage.get_relation(
            persona_id=str(session.get("persona_id", "") or ""),
            platform_name=str(session.get("platform_name", "") or ""),
            user_id=str(session.get("user_id", "") or ""),
            display_name=str(session.get("display_name", "") or ""),
        )
        return {
            "target_persona_id": str(session.get("persona_id", "") or ""),
            "current_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "session_summary_text": session_summary.get("summary_text") or "(none)",
            "character_state_json": _safe_json(character_state["state"]),
            "character_emotion_json": _safe_json(character_state["emotion"]),
            "character_memories_text": _outline_memories(character_memories),
            "recent_journals_text": _outline_journals(recent_journals),
            "latest_journal_text": recent_journals[0]["content"] if recent_journals else "",
            "user_relation_json": _safe_json(
                {
                    "favorability": relation.get("favorability", 0),
                    "relation_stage": relation.get("relation_stage", ""),
                    "cognition": relation.get("cognition", {}),
                    "benefits": relation.get("benefits", []),
                }
            ),
            "user_relation_text": _outline_relation(relation),
            "recent_benefits_text": _outline_benefits(relation),
        }

    async def _ensure_session(self, event, persona_id: str, provider_id: str) -> None:
        # 会话表记录的是插件自己的线程视角，而不是 AstrBot 默认上下文的副本。
        session_key = event.get_extra(EXTRA_SESSION_KEY) or event.unified_msg_origin
        await self.storage.upsert_session(
            session_key=session_key,
            unified_msg_origin=event.unified_msg_origin,
            platform_name=event.get_platform_name(),
            user_id=event.get_sender_id() or event.session_id,
            display_name=event.get_sender_name() or "",
            persona_id=persona_id,
            provider_id=provider_id,
            user_message_at=time.time(),
        )

    async def _log_raw_event(self, *, stage: str, payload: dict[str, Any]) -> None:
        # 原始日志是调试入口，不参与业务逻辑；关闭后不影响插件主功能。
        if not self._raw_logging_enabled():
            return
        await self.raw_logger.append(stage=stage, payload=payload)

    async def _maybe_rollup_session_summary(self, session: dict[str, Any]) -> None:
        # 摘要层只压缩已经发出的 chat/push 可见对话，不碰后台 journal turn。
        if not self._summary_enabled():
            return

        trigger_turns = self._summary_trigger_turns()
        candidate_turns = await self.storage.list_turns_for_summary(
            session["session_key"],
            trigger_turns + 1,
        )
        if len(candidate_turns) <= trigger_turns:
            return

        compact_turns = min(self._summary_compact_turns(), len(candidate_turns) - 1)
        turns_to_compress = candidate_turns[:compact_turns]
        if not turns_to_compress:
            return

        prompts = self.prompt_store.load()
        template = str(prompts.get("summary", {}).get("rollup_template", "") or "").strip()
        if not template:
            return

        existing_summary = await self.storage.get_session_summary(session["session_key"])
        summary_prompt = render_template(
            template,
            {
                "current_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "summary_max_chars": self._summary_max_chars(),
                "existing_session_summary_text": existing_summary.get("summary_text") or "(none)",
                "turns_for_summary_text": _outline_turns_for_summary(turns_to_compress),
            },
        ).strip()
        if not summary_prompt:
            return

        provider_id = str(session.get("last_provider_id") or "")
        if not provider_id:
            return

        persona_prompt = self._get_persona_prompt(str(session.get("persona_id", "") or ""))
        await self._log_raw_event(
            stage="summary_rollup_request",
            payload={
                "session_key": session["session_key"],
                "persona_id": session.get("persona_id", ""),
                "provider_id": provider_id,
                "final_prompt_text": f"[System Prompt]\n{persona_prompt}\n\n[Prompt]\n{summary_prompt}".strip(),
            },
        )
        response = await self.context.llm_generate(
            chat_provider_id=provider_id,
            prompt=summary_prompt,
            system_prompt=persona_prompt,
        )
        raw_response_text = _extract_response_text(response)
        await self._log_raw_event(
            stage="summary_rollup_response_raw",
            payload={
                "session_key": session["session_key"],
                "persona_id": session.get("persona_id", ""),
                "provider_id": provider_id,
                "raw_response_text": raw_response_text,
            },
        )

        summary_text = str(response.completion_text or raw_response_text or "").strip()
        if not summary_text:
            return

        summary_record = await self.storage.upsert_session_summary(
            session_key=session["session_key"],
            summary_text=summary_text,
            covered_until_turn_id=str(turns_to_compress[-1]["turn_id"]),
            covered_turn_count=len(turns_to_compress),
            provider_id=provider_id,
        )
        await self.storage.mark_turns_compressed(
            [str(turn["turn_id"]) for turn in turns_to_compress],
            summary_record["summary_ref"],
        )

    async def observe_llm_request(self, event, req) -> None:
        # 当前版本这里只做人格命中探测，保留成单独 hook 主要是为了后续扩展。
        matched = await self._match_target_persona(
            event,
            req.conversation.persona_id if getattr(req, "conversation", None) else None,
        )
        if not matched:
            return

    async def on_llm_request(self, event, req) -> None:
        matched = await self._match_target_persona(
            event,
            req.conversation.persona_id if getattr(req, "conversation", None) else None,
        )
        if not matched:
            event.set_extra(EXTRA_ENABLED, False)
            return

        event.set_extra(EXTRA_ENABLED, True)
        event.set_extra(EXTRA_MATCHED_PERSONA, matched["persona_id"])
        session_key = _conversation_context_session_key(req, event)
        event.set_extra(EXTRA_SESSION_KEY, session_key)

        provider_id = await self._resolve_provider_id(event)
        event.set_extra(EXTRA_PROVIDER_ID, provider_id)
        await self._ensure_session(event, matched["persona_id"], provider_id)
        session = await self.storage.get_session(session_key)
        if not session:
            event.set_extra(EXTRA_ENABLED, False)
            return

        prompts = self.prompt_store.load()
        chat_cfg = prompts.get("chat", {})
        prompt_context = await self._build_prompt_context_v2(session)
        prompt_context["astrbot_system_prompt"] = req.system_prompt or ""

        # 命中目标人格后，完全改用插件自己的 turn 历史接管短期上下文。
        req.contexts = _turns_to_contexts(
            await self.storage.list_recent_turns(
                session_key,
                self._turn_context_limit(),
                exclude_compressed=True,
            )
        )

        inject_template = str(chat_cfg.get("inject_template", "") or "")
        rendered_prompt = render_template(inject_template, prompt_context)
        if "{{astrbot_system_prompt}}" not in inject_template:
            req.system_prompt = f"{req.system_prompt or ''}\n\n{rendered_prompt}".strip()
        else:
            req.system_prompt = rendered_prompt
        # 最后再追加一层硬协议，降低提示词写漏时的行为漂移。
        req.system_prompt = _append_protocol_contract(req.system_prompt)
        await self._log_raw_event(
            stage="chat_request_final",
            payload={
                "session_key": session_key,
                "persona_id": matched["persona_id"],
                "provider_id": provider_id,
                "final_prompt_text": _build_final_prompt_text(req),
            },
        )

        if event.get_extra(EXTRA_USER_TURN_ID):
            return

        user_turn_id = await self.storage.insert_turn(
            session_key=session_key,
            role="user",
            source_type=SOURCE_CHAT,
            visible_text=event.get_message_outline(),
            raw_text=event.get_message_outline(),
            hidden_payload={},
            provider_id=provider_id,
            prompt_snapshot={
                "selected_persona_id": matched["persona_id"],
                "system_prompt_after": req.system_prompt,
                "session_key": session_key,
            },
            sent_at=time.time(),
        )
        event.set_extra(EXTRA_USER_TURN_ID, user_turn_id)

    async def on_llm_response(self, event, resp) -> None:
        if not event.get_extra(EXTRA_ENABLED, False):
            return
        if getattr(resp, "is_chunk", False):
            return

        prompts = self.prompt_store.load()
        specs = prompts.get("hidden_blocks", [])
        # 实际解析与入库仍以 AstrBot 当前回传给插件的 completion_text 为准。
        raw_text = resp.completion_text or ""
        raw_response_text = _extract_response_text(resp)
        usage_payload = _usage_to_dict(resp)
        await self._log_raw_event(
            stage="chat_response_raw",
            payload={
                "session_key": event.get_extra(EXTRA_SESSION_KEY, event.unified_msg_origin),
                "persona_id": event.get_extra(EXTRA_MATCHED_PERSONA, ""),
                "provider_id": event.get_extra(EXTRA_PROVIDER_ID, ""),
                "raw_response_text": raw_response_text,
                "usage": usage_payload,
                "mnemosyne_meta_present": has_mnemosyne_meta(raw_response_text),
                "hidden_block_hits": _extract_hidden_block_hits(raw_response_text, specs),
            },
        )

        try:
            parsed = parse_mnemosyne_response(raw_text, specs)
        except Exception as exc:
            logger.warning("mnemosyne hidden block parsing failed: %s", exc)
            return
        # 解析完之后再做一次角色侧白名单过滤，避免无关隐藏块混入存储。
        parsed.blocks = _filter_character_blocks(parsed.blocks)

        if not parsed.meta_present:
            logger.warning(
                "mnemosyne response missing <mnemosyne_meta> wrapper for session %s",
                event.unified_msg_origin,
            )

        resp.completion_text = parsed.visible_text
        event.set_extra(
            EXTRA_PENDING_ASSISTANT,
            {
                "raw_text": raw_text,
                "visible_text": parsed.visible_text,
                "blocks": _serialize_blocks(parsed.blocks),
                "provider_id": event.get_extra(EXTRA_PROVIDER_ID, ""),
                "parsed_blocks": parsed.blocks,
                "meta_present": parsed.meta_present,
                "usage": usage_payload,
            },
        )

    async def after_message_sent(self, event) -> None:
        # assistant turn 必须等平台实际发出成功后再补录，避免出现“写库成功但消息没发出去”的假记录。
        payload = event.get_extra(EXTRA_PENDING_ASSISTANT)
        if not payload:
            return

        session_key = event.get_extra(EXTRA_SESSION_KEY, event.unified_msg_origin)
        usage = payload.get("usage", {})
        turn_id = await self.storage.insert_turn(
            session_key=session_key,
            role="assistant",
            source_type=SOURCE_CHAT,
            visible_text=payload["visible_text"],
            raw_text=payload["raw_text"],
            hidden_payload=payload["blocks"],
            provider_id=payload["provider_id"],
            prompt_snapshot={},
            sent_at=time.time(),
            input_tokens_other=int(usage.get("input_tokens_other", 0) or 0),
            input_tokens_cached=int(usage.get("input_tokens_cached", 0) or 0),
            output_tokens=int(usage.get("output_tokens", 0) or 0),
        )
        await self.storage.upsert_session(
            session_key=session_key,
            unified_msg_origin=event.unified_msg_origin,
            platform_name=event.get_platform_name(),
            user_id=event.get_sender_id() or event.session_id,
            display_name=event.get_sender_name() or "",
            persona_id=event.get_extra(EXTRA_MATCHED_PERSONA, ""),
            provider_id=payload["provider_id"],
            assistant_message_at=time.time(),
        )
        session = await self.storage.get_session(session_key)
        await self._apply_hidden_blocks(
            session=session,
            blocks=payload["parsed_blocks"],
            source_turn_id=turn_id,
            idle_since=None,
        )
        if session:
            await self._maybe_rollup_session_summary(session)
        event.set_extra(EXTRA_PENDING_ASSISTANT, None)

    async def _apply_hidden_blocks(
        self,
        *,
        session: dict[str, Any] | None,
        blocks: list[HiddenBlock],
        source_turn_id: str,
        idle_since: float | None,
    ) -> dict[str, Any]:
        # 这里负责把模型输出的隐藏块真正沉淀为数据库状态：
        # state/emotion 做 merge，memory 做 append，journal 独立写入。
        journal_text = ""
        character_state_patch: dict[str, Any] = {}
        character_emotion_patch: dict[str, Any] = {}
        user_relation_patch: dict[str, Any] = {}

        for block in blocks:
            target = block.target
            payload = block.payload
            if target == "character_state_patch" and isinstance(payload, dict):
                character_state_patch = payload
                await self.storage.merge_state(
                    scope_type=CHARACTER_SCOPE,
                    scope_key=CHARACTER_SCOPE_KEY,
                    state_patch=payload,
                    source_turn_id=source_turn_id,
                )
            elif target == "character_emotion_patch" and isinstance(payload, dict):
                character_emotion_patch = payload
                await self.storage.merge_state(
                    scope_type=CHARACTER_SCOPE,
                    scope_key=CHARACTER_SCOPE_KEY,
                    emotion_patch=payload,
                    source_turn_id=source_turn_id,
                )
            elif target == "user_relation_patch" and isinstance(payload, dict):
                user_relation_patch = payload
                if session:
                    await self.storage.merge_relation(
                        persona_id=str(session.get("persona_id", "") or ""),
                        platform_name=str(session.get("platform_name", "") or ""),
                        user_id=str(session.get("user_id", "") or ""),
                        display_name=str(session.get("display_name", "") or ""),
                        patch=payload,
                        source_turn_id=source_turn_id,
                    )
            elif target == "character_memory_append":
                for item in self._normalize_memory_payload(payload):
                    await self.storage.add_memory(
                        scope_type=CHARACTER_SCOPE,
                        scope_key=CHARACTER_SCOPE_KEY,
                        content=item["content"],
                        memory_type=item["memory_type"],
                        importance=item["importance"],
                        metadata=item["metadata"],
                        source_turn_id=source_turn_id,
                    )
            elif target == "journal_entry":
                journal_text = str(payload).strip()

        if journal_text:
            await self.storage.insert_journal(
                content=journal_text,
                summary=journal_text[:120],
                state_patch={
                    "character_state_patch": character_state_patch,
                    "character_emotion_patch": character_emotion_patch,
                    "user_relation_patch": user_relation_patch,
                },
                source_turn_id=source_turn_id,
                idle_since=idle_since,
            )

        return {
            "journal_text": journal_text,
            "character_state_patch": character_state_patch,
            "character_emotion_patch": character_emotion_patch,
            "user_relation_patch": user_relation_patch,
        }

    def _normalize_memory_payload(self, payload: Any) -> list[dict[str, Any]]:
        # memory_append 允许字符串、对象或对象数组，这里统一收敛成固定结构。
        if isinstance(payload, str):
            return [
                {
                    "content": payload.strip(),
                    "memory_type": "note",
                    "importance": 0.5,
                    "metadata": {},
                }
            ]
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            return []

        result = []
        for item in payload:
            if isinstance(item, str):
                result.append(
                    {
                        "content": item.strip(),
                        "memory_type": "note",
                        "importance": 0.5,
                        "metadata": {},
                    }
                )
                continue
            if not isinstance(item, dict):
                continue
            content = str(item.get("content", "")).strip()
            if not content:
                continue
            result.append(
                {
                    "content": content,
                    "memory_type": str(item.get("memory_type", "note")),
                    "importance": float(item.get("importance", 0.5)),
                    "metadata": item.get("metadata", {})
                    if isinstance(item.get("metadata", {}), dict)
                    else {},
                }
            )
        return result

    def build_status_lines(self, stats: dict[str, Any]) -> list[str]:
        return [
            f"插件名: {PLUGIN_NAME}",
            f"启用状态: {'开启' if self.is_enabled() else '关闭'}",
            f"目标人格: {self._target_persona_id() or '(未设置)'}",
            f"数据库: {self.db_path}",
            f"提示词文件: {self.prompt_path}",
            f"原始日志: {self.raw_log_path}",
            f"会话数: {stats['session_count']}",
            f"对话条目数: {stats['turn_count']}",
            f"记忆数: {stats['memory_count']}",
            f"日记数: {stats['journal_count']}",
        ]

    async def get_status_lines(self) -> list[str]:
        stats = await self.storage.get_stats()
        return self.build_status_lines(stats)

    def build_status_lines_v2(
        self,
        stats: dict[str, Any],
        current_session: dict[str, Any] | None = None,
        current_session_tokens: dict[str, int] | None = None,
        global_tokens: dict[str, int] | None = None,
    ) -> list[str]:
        lines = [
            f"插件名: {PLUGIN_NAME}",
            f"启用状态: {'开启' if self.is_enabled() else '关闭'}",
            f"目标人格: {self._target_persona_id() or '(未设置)'}",
            f"数据库: {self.db_path}",
            f"提示词文件: {self.prompt_path}",
            f"原始日志: {self.raw_log_path}",
            f"会话数: {stats['session_count']}",
            f"对话条目数: {stats['turn_count']}",
            f"记忆数: {stats['memory_count']}",
            f"日志数: {stats['journal_count']}",
            f"滚动摘要数: {stats.get('summary_count', 0)}",
            f"用户关系数: {stats.get('relation_count', 0)}",
        ]
        if current_session:
            lines.append(f"当前插件会话: {current_session.get('session_key', '')}")
        if current_session_tokens:
            lines.append(
                "当前会话 Tokens: "
                f"in_other={current_session_tokens.get('input_other', 0)}, "
                f"in_cached={current_session_tokens.get('input_cached', 0)}, "
                f"out={current_session_tokens.get('output', 0)}, "
                f"total={current_session_tokens.get('total', 0)}"
            )
        if global_tokens:
            lines.append(
                "全局 Tokens: "
                f"in_other={global_tokens.get('input_other', 0)}, "
                f"in_cached={global_tokens.get('input_cached', 0)}, "
                f"out={global_tokens.get('output', 0)}, "
                f"total={global_tokens.get('total', 0)}"
            )
        return lines

    async def get_status_lines_v2(self, event=None) -> list[str]:
        stats = await self.storage.get_stats()
        current_session = None
        current_session_tokens = None
        if event is not None:
            current_session = await self.storage.get_latest_session_for_origin(
                event.unified_msg_origin,
                self._target_persona_id(),
            )
            if current_session:
                current_session_tokens = await self.storage.get_token_totals(
                    current_session.get("session_key")
                )
        global_tokens = await self.storage.get_token_totals()
        return self.build_status_lines_v2(
            stats,
            current_session=current_session,
            current_session_tokens=current_session_tokens,
            global_tokens=global_tokens,
        )

    async def scheduler_tick(self) -> None:
        # service 层负责防重入；外部调度器只管定时触发。
        if not self.is_enabled() or not self._scheduler_enabled():
            return
        if self._background_running:
            return

        self._background_running = True
        try:
            await self._scheduler_tick_impl()
        finally:
            self._background_running = False

    async def _scheduler_tick_impl(self) -> None:
        # 后台生成只跟随最近一个命中过目标人格的会话继续推进幕后轨迹。
        session = await self.storage.get_latest_session()
        if not session:
            return

        last_user_message_at = session.get("last_user_message_at") or 0.0
        if not last_user_message_at:
            return

        now = time.time()
        idle_seconds = now - last_user_message_at
        if idle_seconds < self._idle_threshold_minutes() * 60:
            return

        recent_journals = await self.storage.list_recent_journals(1)
        if recent_journals:
            cooldown = self._background_cooldown_minutes() * 60
            if now - recent_journals[0]["generated_at"] < cooldown:
                return

        provider_id = str(session.get("last_provider_id") or "")
        if not provider_id:
            provider = self.context.get_using_provider()
            provider_id = provider.meta().id if provider else ""
        if not provider_id:
            logger.warning("mnemosyne scheduler skipped: no provider found")
            return

        persona_id = self._target_persona_id()
        persona_prompt = self._get_persona_prompt(persona_id)

        prompts = self.prompt_store.load()
        background_cfg = prompts.get("background", {})
        prompt_context = await self._build_prompt_context_v2(session)
        prompt_context["idle_minutes"] = int(idle_seconds // 60)
        prompt_context["astrbot_system_prompt"] = persona_prompt

        journal_prompt = render_template(
            str(background_cfg.get("journal_template", "") or ""),
            prompt_context,
        )
        journal_prompt = _append_protocol_contract(journal_prompt)
        if not journal_prompt.strip():
            return
        await self._log_raw_event(
            stage="background_journal_request",
            payload={
                "session_key": session["session_key"],
                "persona_id": persona_id,
                "provider_id": provider_id,
                "final_prompt_text": f"[System Prompt]\n{persona_prompt}\n\n[Prompt]\n{journal_prompt}".strip(),
            },
        )

        response = await self.context.llm_generate(
            chat_provider_id=provider_id,
            prompt=journal_prompt,
            system_prompt=persona_prompt,
        )
        response_usage = _usage_to_dict(response)
        await self._log_raw_event(
            stage="background_journal_response_raw",
            payload={
                "session_key": session["session_key"],
                "persona_id": persona_id,
                "provider_id": provider_id,
                "raw_response_text": _extract_response_text(response),
                "usage": response_usage,
                "mnemosyne_meta_present": has_mnemosyne_meta(
                    _extract_response_text(response)
                ),
            },
        )
        parsed = parse_mnemosyne_response(
            response.completion_text or "",
            prompts.get("hidden_blocks", []),
        )
        parsed.blocks = _filter_character_blocks(parsed.blocks)
        if not parsed.meta_present:
            logger.warning(
                "mnemosyne background journal missing <mnemosyne_meta> wrapper for session %s",
                session["session_key"],
            )
        bg_turn_id = await self.storage.insert_turn(
            session_key=session["session_key"],
            role="assistant",
            source_type=SOURCE_BACKGROUND,
            visible_text=parsed.visible_text,
            raw_text=response.completion_text or "",
            hidden_payload=_serialize_blocks(parsed.blocks),
            provider_id=provider_id,
            prompt_snapshot={"kind": "background_journal"},
            sent_at=time.time(),
            input_tokens_other=int(response_usage.get("input_tokens_other", 0) or 0),
            input_tokens_cached=int(response_usage.get("input_tokens_cached", 0) or 0),
            output_tokens=int(response_usage.get("output_tokens", 0) or 0),
        )
        await self._apply_hidden_blocks(
            session=session,
            blocks=parsed.blocks,
            source_turn_id=bg_turn_id,
            idle_since=last_user_message_at,
        )

        probability = self._active_push_probability()
        if probability <= 0:
            return

        last_push_at = session.get("last_active_push_at") or 0.0
        if now - last_push_at < self._active_push_cooldown_minutes() * 60:
            return

        roll = random.random()
        if roll > probability:
            return

        push_prompt = render_template(
            str(background_cfg.get("active_push_template", "") or ""),
            prompt_context,
        )
        push_prompt = _append_protocol_contract(push_prompt)
        if not push_prompt.strip():
            return
        await self._log_raw_event(
            stage="background_push_request",
            payload={
                "session_key": session["session_key"],
                "persona_id": persona_id,
                "provider_id": provider_id,
                "final_prompt_text": f"[System Prompt]\n{persona_prompt}\n\n[Prompt]\n{push_prompt}".strip(),
            },
        )

        push_resp = await self.context.llm_generate(
            chat_provider_id=provider_id,
            prompt=push_prompt,
            system_prompt=persona_prompt,
        )
        push_usage = _usage_to_dict(push_resp)
        await self._log_raw_event(
            stage="background_push_response_raw",
            payload={
                "session_key": session["session_key"],
                "persona_id": persona_id,
                "provider_id": provider_id,
                "raw_response_text": _extract_response_text(push_resp),
                "usage": push_usage,
                "mnemosyne_meta_present": has_mnemosyne_meta(
                    _extract_response_text(push_resp)
                ),
            },
        )
        parsed_push = parse_mnemosyne_response(
            push_resp.completion_text or "",
            prompts.get("hidden_blocks", []),
        )
        parsed_push.blocks = _filter_character_blocks(parsed_push.blocks)
        if not parsed_push.meta_present:
            logger.warning(
                "mnemosyne proactive push missing <mnemosyne_meta> wrapper for session %s",
                session["session_key"],
            )
        visible_text = parsed_push.visible_text.strip()
        if not visible_text:
            logger.warning("mnemosyne scheduler skipped empty proactive visible text")
            return

        sent = await self.context.send_message(
            session["unified_msg_origin"],
            [Plain(visible_text)],
        )
        if not sent:
            logger.warning("mnemosyne proactive message send failed")
            return

        push_turn_id = await self.storage.insert_turn(
            session_key=session["session_key"],
            role="assistant",
            source_type=SOURCE_PUSH,
            visible_text=visible_text,
            raw_text=push_resp.completion_text or "",
            hidden_payload=_serialize_blocks(parsed_push.blocks),
            provider_id=provider_id,
            prompt_snapshot={"kind": "proactive_push", "roll": roll},
            sent_at=time.time(),
            input_tokens_other=int(push_usage.get("input_tokens_other", 0) or 0),
            input_tokens_cached=int(push_usage.get("input_tokens_cached", 0) or 0),
            output_tokens=int(push_usage.get("output_tokens", 0) or 0),
        )
        await self.storage.upsert_session(
            session_key=session["session_key"],
            unified_msg_origin=session["unified_msg_origin"],
            platform_name=session["platform_name"],
            user_id=session["user_id"],
            display_name=session["display_name"],
            persona_id=session.get("persona_id", ""),
            provider_id=provider_id,
            assistant_message_at=time.time(),
            push_message_at=time.time(),
        )
        await self._apply_hidden_blocks(
            session=session,
            blocks=parsed_push.blocks,
            source_turn_id=push_turn_id,
            idle_since=last_user_message_at,
        )
        refreshed_session = await self.storage.get_session(session["session_key"])
        if refreshed_session:
            await self._maybe_rollup_session_summary(refreshed_session)
