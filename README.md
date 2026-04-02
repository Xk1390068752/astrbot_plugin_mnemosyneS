# astrbot_plugin_mnemosyneS

一个面向 AstrBot 人设陪伴场景的插件。

当前版本支持这些核心能力：
- 命中指定 Persona 时，拦截 LLM 请求并把角色状态、角色记忆、角色日志注入到提示词里
- 命中指定 Persona 时，插件完全接管该人格的短期对话上下文，AstrBot 原生 `req.contexts` 不再参与发给 LLM
- 插件短期上下文基于 `mnemo_turn` 和 AstrBot `conversation_id` 重建，因此 `/new` `/del` 的会话切换会自然同步到插件
- 每轮对话落库，保存用户与角色聊天记录
- 使用 SQLite 持久化角色状态、角色记忆与角色日志
- 在空闲时后台生成角色动态轨迹
- 按概率主动向最近私聊用户发起消息
- 通过隐藏标签从模型回复中提取结构化内容，并在发给用户前剥离
- 把发送给 LLM 的最终完整提示词和模型原始回包记录为 `jsonl` 调试日志

## 目录说明

- `main.py`: AstrBot 插件入口
- `mnemo_service.py`: 前后台主逻辑
- `mnemo_storage.py`: SQLite 存储层
- `mnemo_parser.py`: 隐藏标签解析
- `default_prompts.json`: 默认提示词模板
- `_conf_schema.json`: WebUI 配置定义

## 使用方式

1. 将插件安装到 AstrBot 插件目录或通过仓库地址导入。
2. 在 WebUI 中为插件配置 `target_persona_id`。
3. 启动 AstrBot。
4. 首次启动后，插件会自动创建：
   - `data/plugin_data/astrbot_plugin_mnemosyneS/mnemosyne.sqlite3`
   - `data/plugin_data/astrbot_plugin_mnemosyneS/prompts.json`
   - `data/plugin_data/astrbot_plugin_mnemosyneS/raw_llm.jsonl`
5. 修改 `prompts.json`，写入你自己的提示词和隐藏标签输出规则。
6. 发送 `/mnemosyne` 查看插件状态。

## 调试日志

`raw_llm.jsonl` 当前会记录这些阶段：
- `chat_request_final`: 真正发给 LLM 的最终完整提示词文本
- `chat_response_raw`: 模型原始回包文本
- `background_journal_request`: 后台日志生成的最终完整提示词文本
- `background_journal_response_raw`: 后台日志生成的原始回包文本
- `background_push_request`: 主动私聊生成的最终完整提示词文本
- `background_push_response_raw`: 主动私聊生成的原始回包文本

每条日志现在只保留排查最有用的几项，不再输出大量中间结构。

## 提示词约定

插件默认采用类似 SillyTavern 的隐藏标签机制。模型可以在可见回复后追加这些标签：

- 外层必须是 `<mnemosyne_meta>...</mnemosyne_meta>`
- `<character_state_patch>{...}</character_state_patch>`
- `<character_emotion_patch>{...}</character_emotion_patch>`
- `<character_memory_append>[...]</character_memory_append>`
- `<journal_entry>...</journal_entry>`

插件会：
- 捕获这些标签
- 将内容写入数据库
- 在发给用户前把这些标签从正文剥离

如果本轮没有需要写入的内容，模型也必须输出空包装：

- `<mnemosyne_meta></mnemosyne_meta>`

## prompts.json 编辑说明

- 模板字段可以继续写成 JSON 字符串，此时换行需要写成 `\\n`
- 也支持写成字符串数组，插件会自动按换行拼接
- `hidden_blocks` 里的 `pattern` 仍然是正则表达式，里面的反斜杠需要保持 JSON 转义

## 说明

- 当前版本建议只在私聊使用
- 提示词内容由你自己维护，插件只负责读取、注入、解析和持久化
