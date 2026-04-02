# astrbot_plugin_mnemosyneS

一个面向 AstrBot 人设陪伴场景的插件。

当前版本支持这些核心能力：
- 命中指定 Persona 时，拦截 LLM 请求并把记忆、状态、日记注入到 AstrBot 原始提示词里
- 每轮对话落库，保存用户与角色聊天记录
- 使用 SQLite 持久化角色状态、用户状态、情感、记忆与日记
- 在空闲时后台生成角色动态轨迹
- 按概率主动向最近私聊用户发起消息
- 通过隐藏标签从模型回复中提取结构化内容，并在发给用户前剥离
- 把发送给 LLM 的请求和收到的响应记录为 `jsonl` 调试日志
- 采用强制性的 `<mnemosyne_meta>...</mnemosyne_meta>` 隐藏协议包装记忆更新

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
- `chat_request_entry`: 请求刚进入插件链时的快照，尽量接近“其他插件处理前”
- `chat_request_before_injection`: Mnemosyne 注入前的请求快照
- `chat_request_after_injection`: Mnemosyne 注入后的请求快照
- `chat_response_raw`: AstrBot 暴露给插件的响应文本、消息链和 provider 原始响应对象

其中：
- `request_phase` 用来标记当前记录所处的阶段
- `hook_priority` 用来标记触发该记录的 hook 优先级
- `hidden_block_hits_*` 会告诉你隐藏标签到底出现在了哪一层

## 提示词约定

插件默认采用类似 SillyTavern 的隐藏标签机制。模型可以在可见回复后追加这些标签：

- 外层必须是 `<mnemosyne_meta>...</mnemosyne_meta>`

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

如果本轮没有需要写入的内容，模型也必须输出空包装：

- `<mnemosyne_meta></mnemosyne_meta>`

## prompts.json 编辑说明

- 模板字段可以继续写成 JSON 字符串，此时换行需要写成 `\\n`
- 也支持写成字符串数组，插件会自动按换行拼接
- `hidden_blocks` 里的 `pattern` 仍然是正则表达式，里面的反斜杠需要保持 JSON 转义

## 说明

- 当前版本建议只在私聊使用
- 提示词内容由你自己维护，插件只负责读取、注入、解析和持久化
