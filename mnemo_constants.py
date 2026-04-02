PLUGIN_NAME = "astrbot_plugin_mnemosyneS"
PLUGIN_VERSION = "0.6.0"
PLUGIN_AUTHOR = "Xk1390068752"
PLUGIN_REPO = "https://github.com/Xk1390068752/astrbot_plugin_mnemosyneS"

LLM_REQUEST_OBSERVER_PRIORITY = 1000000000
LLM_REQUEST_INJECTION_PRIORITY = -1000000000

CHARACTER_SCOPE = "character"
USER_SCOPE = "user"
CHARACTER_SCOPE_KEY = "global"

SOURCE_CHAT = "chat"
SOURCE_BACKGROUND = "background"
SOURCE_PUSH = "push"

EXTRA_ENABLED = "_mnemo_enabled"
EXTRA_MATCHED_PERSONA = "_mnemo_matched_persona_id"
EXTRA_PROVIDER_ID = "_mnemo_provider_id"
EXTRA_USER_TURN_ID = "_mnemo_user_turn_id"
EXTRA_PENDING_ASSISTANT = "_mnemo_pending_assistant"

DEFAULT_DB_FILENAME = "mnemosyne.sqlite3"
DEFAULT_PROMPTS_FILENAME = "prompts.json"
DEFAULT_PROMPTS_TEMPLATE_FILENAME = "default_prompts.json"
DEFAULT_RAW_LOG_FILENAME = "raw_llm.jsonl"
