from __future__ import annotations

from pathlib import Path

from mnemo_constants import DEFAULT_PROMPTS_TEMPLATE_FILENAME, PLUGIN_NAME


def get_plugin_root() -> Path:
    return Path(__file__).resolve().parent


def get_default_prompts_template_path() -> Path:
    return get_plugin_root() / DEFAULT_PROMPTS_TEMPLATE_FILENAME


def get_plugin_data_dir(plugin_name: str = PLUGIN_NAME) -> Path:
    try:
        from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path

        base = Path(get_astrbot_plugin_data_path())
    except Exception:
        base = Path.cwd() / "data" / "plugin_data"

    target = base / plugin_name
    target.mkdir(parents=True, exist_ok=True)
    return target


def resolve_user_path(configured_path: str, fallback_filename: str) -> Path:
    configured = (configured_path or "").strip()
    if configured:
        path = Path(configured)
        if not path.is_absolute():
            path = get_plugin_data_dir() / configured
    else:
        path = get_plugin_data_dir() / fallback_filename

    path.parent.mkdir(parents=True, exist_ok=True)
    return path
