# astrbot_plugin_mnemosyneS

一个面向 AstrBot 人设陪伴场景的插件。

当前版本已经具备这些核心能力：

- 命中指定 Persona 时，拦截 LLM 请求并把记忆、状态、日记注入到 AstrBot 原始提示词里
- 每轮对话落库，保存用户/角色聊天记录
- 用 SQLite 持久化角色状态、用户状态、情感、记忆与日记
- 支持空闲时后台生成角色动态轨迹
- 支持按概率主动向最近私聊用户发起消息
- 支持通过隐藏标签从模型回复中提取结构化内容，并在发给用户前剥离
- 支持把发送给 LLM 的完整请求与收到的原始回复记录为 `jsonl` 日志

## 目录说明

- `main.py`：AstrBot 插件入口
- `mnemo_service.py`：前后台主逻辑
- `mnemo_storage.py`：SQLite 存储层
- `mnemo_parser.py`：隐藏标签解析
- `default_prompts.json`：默认提示词模板
- `_conf_schema.json`：WebUI 配置定义

## 使用方式

1. 将插件放到 AstrBot 插件目录。
2. 在 WebUI 中为插件配置 `target_persona_id`。
3. 启动 AstrBot。
4. 首次启动后，插件会自动创建：
   - `data/plugin_data/astrbot_plugin_mnemosyneS/mnemosyne.sqlite3`
   - `data/plugin_data/astrbot_plugin_mnemosyneS/prompts.json`
   - `data/plugin_data/astrbot_plugin_mnemosyneS/raw_llm.jsonl`
5. 修改 `prompts.json`，把你的实际人设提示词和隐藏标签输出规则写进去。
6. 发送 `/mnemosyne` 查看插件状态。

## 提示词约定

插件默认采用类似 SillyTavern 的隐藏标签机制。模型可以在可见回复后追加这些标签：

- `<character_state_patch>{...}</character_state_patch>`
- `<character_emotion_patch>{...}</character_emotion_patch>`
- `<user_state_patch>{...}</user_state_patch>`
- `<user_emotion_patch>{...}</user_emotion_patch>`
- `<character_memory_append>[...]</character_memory_append>`
- `<user_memory_append>[...]</user_memory_append>`
- `<journal_entry>...</journal_entry>`

插件会：

- 捕获这些标签
- 将内容写入数据库
- 在发给用户前把这些标签从正文剥离

## prompts.json 编辑说明

- 模板字段可以继续写成 JSON 字符串，此时换行需要用 `\\n`
- 现在也支持写成字符串数组，插件会自动用换行拼接
- `hidden_blocks` 里的 `pattern` 仍然是正则表达式，里面的反斜杠需要保持 JSON 转义

## 说明

- 当前版本建议只在私聊使用。
- `author` 和 `repo` 仍需要你自行填写到 `metadata.yaml`。
- 提示词内容由你维护，插件只负责读取、注入、解析和持久化。
