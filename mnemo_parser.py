from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


@dataclass
class HiddenBlock:
    name: str
    target: str
    raw: str
    payload: Any


@dataclass
class ParsedResponse:
    visible_text: str
    blocks: list[HiddenBlock]
    meta_present: bool = False


def _extract_content(match: re.Match[str]) -> str:
    if "content" in match.re.groupindex:
        return match.group("content")
    if match.lastindex:
        return match.group(1)
    return match.group(0)


def _parse_payload(mode: str, raw: str) -> Any:
    mode = (mode or "text").strip().lower()
    content = raw.strip()

    if mode == "json":
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {"_parse_error": "json", "_raw": content}
    if mode == "json_list":
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            return {"_parse_error": "json_list", "_raw": content}
        if not isinstance(parsed, list):
            return {"_parse_error": "json_list", "_raw": content}
        return parsed
    return content


def _cleanup_visible_text(text: str) -> str:
    text = text.replace("\r\n", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_hidden_blocks(text: str, specs: list[dict[str, Any]]) -> ParsedResponse:
    # 按配置里的正则逐个提取隐藏块，并把匹配到的片段从可见文本里剥离。
    if not text:
        return ParsedResponse(visible_text="", blocks=[])

    blocks: list[HiddenBlock] = []
    visible_text = text
    for spec in specs or []:
        pattern = spec.get("pattern")
        if not pattern:
            continue

        compiled = re.compile(pattern, re.DOTALL)
        for match in compiled.finditer(text):
            raw = _extract_content(match)
            payload = _parse_payload(spec.get("mode", "text"), raw)
            blocks.append(
                HiddenBlock(
                    name=str(spec.get("name", "hidden_block")),
                    target=str(spec.get("target", "")),
                    raw=raw,
                    payload=payload,
                )
            )

        visible_text = compiled.sub("", visible_text)

    return ParsedResponse(
        visible_text=_cleanup_visible_text(visible_text),
        blocks=blocks,
        meta_present=False,
    )


MNEMOSYNE_META_PATTERN = re.compile(
    r"<mnemosyne_meta>([\s\S]*?)</mnemosyne_meta>",
    re.DOTALL,
)


def has_mnemosyne_meta(text: str) -> bool:
    if not text:
        return False
    return bool(MNEMOSYNE_META_PATTERN.search(text))


def parse_mnemosyne_response(text: str, specs: list[dict[str, Any]]) -> ParsedResponse:
    # 优先要求模型使用统一的 <mnemosyne_meta> 外层包装。
    # 如果没命中包装，则退回到旧版“裸标签”兼容模式。
    if not text:
        return ParsedResponse(visible_text="", blocks=[], meta_present=False)

    matches = list(MNEMOSYNE_META_PATTERN.finditer(text))
    if not matches:
        legacy_parsed = parse_hidden_blocks(text, specs)
        return ParsedResponse(
            visible_text=legacy_parsed.visible_text,
            blocks=legacy_parsed.blocks,
            meta_present=False,
        )

    blocks: list[HiddenBlock] = []
    filtered_specs = [
        spec
        for spec in (specs or [])
        if str(spec.get("target", "") or "") != "mnemosyne_meta"
    ]
    for match in matches:
        inner_text = _extract_content(match)
        inner_parsed = parse_hidden_blocks(inner_text, filtered_specs)
        blocks.extend(inner_parsed.blocks)

    visible_text = MNEMOSYNE_META_PATTERN.sub("", text)
    return ParsedResponse(
        visible_text=_cleanup_visible_text(visible_text),
        blocks=blocks,
        meta_present=True,
    )
