from __future__ import annotations

from pathlib import Path

try:
    from .mnemo_constants import DEFAULT_PROMPTS_TEMPLATE_FILENAME, PLUGIN_NAME
except ImportError:
    from mnemo_constants import DEFAULT_PROMPTS_TEMPLATE_FILENAME, PLUGIN_NAME


def get_plugin_root() -> Path:
    return Path(__file__).resolve().parent


def get_default_prompts_template_path() -> Path:
    # 插件内置模板只负责“首次复制种子文件”，不会直接覆盖用户正在使用的 prompts.json。
    return get_plugin_root() / DEFAULT_PROMPTS_TEMPLATE_FILENAME


def get_plugin_data_dir(plugin_name: str = PLUGIN_NAME) -> Path:
    # 按 AstrBot 官方插件数据目录规范取路径，避免自行拼接工作目录。
    from astrbot.api.star import StarTools

    target = StarTools.get_data_dir(plugin_name)
    target.mkdir(parents=True, exist_ok=True)
    return target


def resolve_user_path(configured_path: str, fallback_filename: str) -> Path:
    # 配置项既支持绝对路径，也支持相对插件数据目录的相对路径。
    configured = (configured_path or "").strip()
    if configured:
        path = Path(configured)
        if not path.is_absolute():
            path = get_plugin_data_dir() / configured
    else:
        path = get_plugin_data_dir() / fallback_filename

    path.parent.mkdir(parents=True, exist_ok=True)
    return path
