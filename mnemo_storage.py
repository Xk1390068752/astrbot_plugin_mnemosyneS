from __future__ import annotations

import asyncio
import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

try:
    from .mnemo_constants import CHARACTER_SCOPE_KEY
except ImportError:
    from mnemo_constants import CHARACTER_SCOPE_KEY


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _merge_dict(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    # 递归合并字典，用于把模型输出的 patch 叠到已有状态快照上。
    merged = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def _normalize_text(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _dedupe_unique_tail(items: list[str], limit: int) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in reversed(items):
        key = _normalize_text(item)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(item.strip())
        if len(ordered) >= limit:
            break
    ordered.reverse()
    return ordered


class MnemoStorage:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        # 这里保持最朴素的 sqlite3 连接，靠 asyncio.to_thread 避免阻塞事件循环。
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=DELETE;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    async def initialize(self) -> None:
        # 所有数据库操作都串行经过同一把锁，避免并发写导致状态相互覆盖。
        async with self._lock:
            await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        # SQLite 规模不大，这里用一份轻量 schema 同时承载：
        # 会话、对话 turn、角色状态、记忆和日记。
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS mnemo_meta (
                    meta_key TEXT PRIMARY KEY,
                    meta_value TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mnemo_session (
                    session_key TEXT PRIMARY KEY,
                    unified_msg_origin TEXT NOT NULL,
                    platform_name TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    persona_id TEXT NOT NULL DEFAULT '',
                    last_provider_id TEXT NOT NULL DEFAULT '',
                    last_user_message_at REAL,
                    last_assistant_message_at REAL,
                    last_active_push_at REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mnemo_turn (
                    turn_id TEXT PRIMARY KEY,
                    session_key TEXT NOT NULL,
                    role TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    visible_text TEXT NOT NULL DEFAULT '',
                    raw_text TEXT NOT NULL DEFAULT '',
                    hidden_payload_json TEXT NOT NULL DEFAULT '{}',
                    provider_id TEXT NOT NULL DEFAULT '',
                    prompt_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL,
                    sent_at REAL,
                    FOREIGN KEY(session_key) REFERENCES mnemo_session(session_key)
                );

                CREATE INDEX IF NOT EXISTS idx_mnemo_turn_session_created
                ON mnemo_turn(session_key, created_at DESC);

                CREATE TABLE IF NOT EXISTS mnemo_state (
                    scope_type TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    state_json TEXT NOT NULL DEFAULT '{}',
                    emotion_json TEXT NOT NULL DEFAULT '{}',
                    updated_from_turn_id TEXT NOT NULL DEFAULT '',
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(scope_type, scope_key)
                );

                CREATE TABLE IF NOT EXISTS mnemo_memory_item (
                    memory_id TEXT PRIMARY KEY,
                    scope_type TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    memory_type TEXT NOT NULL DEFAULT 'note',
                    content TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    importance REAL NOT NULL DEFAULT 0.5,
                    source_turn_id TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_mnemo_memory_scope_created
                ON mnemo_memory_item(scope_type, scope_key, created_at DESC);

                CREATE TABLE IF NOT EXISTS mnemo_journal (
                    journal_id TEXT PRIMARY KEY,
                    generated_at REAL NOT NULL,
                    idle_since REAL,
                    content TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    state_patch_json TEXT NOT NULL DEFAULT '{}',
                    push_roll REAL,
                    push_sent_at REAL,
                    source_turn_id TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_mnemo_journal_generated
                ON mnemo_journal(generated_at DESC);

                CREATE TABLE IF NOT EXISTS mnemo_session_summary (
                    session_key TEXT PRIMARY KEY,
                    summary_text TEXT NOT NULL DEFAULT '',
                    covered_until_turn_id TEXT NOT NULL DEFAULT '',
                    covered_turn_count INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL,
                    provider_id TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(session_key) REFERENCES mnemo_session(session_key)
                );

                CREATE TABLE IF NOT EXISTS mnemo_relation (
                    relation_key TEXT PRIMARY KEY,
                    persona_id TEXT NOT NULL,
                    platform_name TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    favorability REAL NOT NULL DEFAULT 0,
                    relation_stage TEXT NOT NULL DEFAULT '',
                    cognition_json TEXT NOT NULL DEFAULT '{}',
                    benefits_json TEXT NOT NULL DEFAULT '[]',
                    updated_from_turn_id TEXT NOT NULL DEFAULT '',
                    updated_at REAL NOT NULL
                );
                """
            )
            self._ensure_column(conn, "mnemo_turn", "compressed_at", "REAL")
            self._ensure_column(
                conn,
                "mnemo_turn",
                "compressed_into_summary_id",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                conn,
                "mnemo_turn",
                "input_tokens_other",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                conn,
                "mnemo_turn",
                "input_tokens_cached",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                conn,
                "mnemo_turn",
                "output_tokens",
                "INTEGER NOT NULL DEFAULT 0",
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_mnemo_turn_session_uncompressed
                ON mnemo_turn(session_key, compressed_at, created_at ASC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_mnemo_relation_lookup
                ON mnemo_relation(persona_id, platform_name, user_id)
                """
            )
            now = time.time()
            conn.execute(
                """
                INSERT INTO mnemo_meta (meta_key, meta_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(meta_key) DO UPDATE SET
                    meta_value = excluded.meta_value,
                    updated_at = excluded.updated_at
                """,
                ("schema_version", "2", now),
            )
            conn.commit()

    def _ensure_column(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_sql: str,
    ) -> None:
        existing = {
            row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name in existing:
            return
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

    async def upsert_session(
        self,
        *,
        session_key: str,
        unified_msg_origin: str,
        platform_name: str,
        user_id: str,
        display_name: str,
        persona_id: str,
        provider_id: str,
        user_message_at: float | None = None,
        assistant_message_at: float | None = None,
        push_message_at: float | None = None,
    ) -> None:
        async with self._lock:
            await asyncio.to_thread(
                self._upsert_session_sync,
                session_key,
                unified_msg_origin,
                platform_name,
                user_id,
                display_name,
                persona_id,
                provider_id,
                user_message_at,
                assistant_message_at,
                push_message_at,
            )

    def _upsert_session_sync(
        self,
        session_key: str,
        unified_msg_origin: str,
        platform_name: str,
        user_id: str,
        display_name: str,
        persona_id: str,
        provider_id: str,
        user_message_at: float | None,
        assistant_message_at: float | None,
        push_message_at: float | None,
    ) -> None:
        # session_key 是插件自己的线程键；
        # 在命中 persona 时，通常会绑定到 AstrBot conversation_id。
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO mnemo_session (
                    session_key, unified_msg_origin, platform_name, user_id, display_name,
                    persona_id, last_provider_id, last_user_message_at,
                    last_assistant_message_at, last_active_push_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_key) DO UPDATE SET
                    unified_msg_origin = excluded.unified_msg_origin,
                    platform_name = excluded.platform_name,
                    user_id = excluded.user_id,
                    display_name = excluded.display_name,
                    persona_id = excluded.persona_id,
                    last_provider_id = CASE
                        WHEN excluded.last_provider_id != '' THEN excluded.last_provider_id
                        ELSE mnemo_session.last_provider_id
                    END,
                    last_user_message_at = COALESCE(excluded.last_user_message_at, mnemo_session.last_user_message_at),
                    last_assistant_message_at = COALESCE(excluded.last_assistant_message_at, mnemo_session.last_assistant_message_at),
                    last_active_push_at = COALESCE(excluded.last_active_push_at, mnemo_session.last_active_push_at),
                    updated_at = excluded.updated_at
                """,
                (
                    session_key,
                    unified_msg_origin,
                    platform_name,
                    user_id,
                    display_name,
                    persona_id,
                    provider_id,
                    user_message_at,
                    assistant_message_at,
                    push_message_at,
                    now,
                    now,
                ),
            )
            conn.commit()

    async def get_session(self, session_key: str) -> dict[str, Any] | None:
        async with self._lock:
            return await asyncio.to_thread(self._get_session_sync, session_key)

    def _get_session_sync(self, session_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM mnemo_session
                WHERE session_key = ?
                """,
                (session_key,),
            ).fetchone()
            return dict(row) if row else None

    async def get_latest_session_for_origin(
        self,
        unified_msg_origin: str,
        persona_id: str = "",
    ) -> dict[str, Any] | None:
        async with self._lock:
            return await asyncio.to_thread(
                self._get_latest_session_for_origin_sync,
                unified_msg_origin,
                persona_id,
            )

    def _get_latest_session_for_origin_sync(
        self,
        unified_msg_origin: str,
        persona_id: str,
    ) -> dict[str, Any] | None:
        query = """
            SELECT *
            FROM mnemo_session
            WHERE unified_msg_origin = ?
        """
        params: list[Any] = [unified_msg_origin]
        if persona_id:
            query += " AND persona_id = ?"
            params.append(persona_id)
        query += " ORDER BY updated_at DESC LIMIT 1"
        with self._connect() as conn:
            row = conn.execute(query, params).fetchone()
            return dict(row) if row else None

    async def insert_turn(
        self,
        *,
        session_key: str,
        role: str,
        source_type: str,
        visible_text: str,
        raw_text: str,
        hidden_payload: dict[str, Any] | None,
        provider_id: str,
        prompt_snapshot: dict[str, Any] | None,
        sent_at: float | None = None,
        input_tokens_other: int = 0,
        input_tokens_cached: int = 0,
        output_tokens: int = 0,
    ) -> str:
        async with self._lock:
            return await asyncio.to_thread(
                self._insert_turn_sync,
                session_key,
                role,
                source_type,
                visible_text,
                raw_text,
                hidden_payload or {},
                provider_id,
                prompt_snapshot or {},
                sent_at,
                input_tokens_other,
                input_tokens_cached,
                output_tokens,
            )

    def _insert_turn_sync(
        self,
        session_key: str,
        role: str,
        source_type: str,
        visible_text: str,
        raw_text: str,
        hidden_payload: dict[str, Any],
        provider_id: str,
        prompt_snapshot: dict[str, Any],
        sent_at: float | None,
        input_tokens_other: int,
        input_tokens_cached: int,
        output_tokens: int,
    ) -> str:
        # turn 表既承担“短期上下文回放”，也承担审计日志的角色。
        turn_id = uuid.uuid4().hex
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO mnemo_turn (
                    turn_id, session_key, role, source_type, visible_text, raw_text,
                    hidden_payload_json, provider_id, prompt_snapshot_json, created_at, sent_at,
                    compressed_at, compressed_into_summary_id,
                    input_tokens_other, input_tokens_cached, output_tokens
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    turn_id,
                    session_key,
                    role,
                    source_type,
                    visible_text,
                    raw_text,
                    _json_dumps(hidden_payload),
                    provider_id,
                    _json_dumps(prompt_snapshot),
                    now,
                    sent_at,
                    None,
                    "",
                    max(int(input_tokens_other or 0), 0),
                    max(int(input_tokens_cached or 0), 0),
                    max(int(output_tokens or 0), 0),
                ),
            )
            conn.commit()
        return turn_id

    async def get_state(self, scope_type: str, scope_key: str) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._get_state_sync, scope_type, scope_key)

    def _get_state_sync(self, scope_type: str, scope_key: str) -> dict[str, Any]:
        # 没有状态时返回空快照，避免上层到处判空。
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT state_json, emotion_json, updated_from_turn_id, updated_at
                FROM mnemo_state
                WHERE scope_type = ? AND scope_key = ?
                """,
                (scope_type, scope_key),
            ).fetchone()
            if not row:
                return {
                    "scope_type": scope_type,
                    "scope_key": scope_key,
                    "state": {},
                    "emotion": {},
                    "updated_from_turn_id": "",
                    "updated_at": 0.0,
                }
            return {
                "scope_type": scope_type,
                "scope_key": scope_key,
                "state": _json_loads(row["state_json"], {}),
                "emotion": _json_loads(row["emotion_json"], {}),
                "updated_from_turn_id": row["updated_from_turn_id"] or "",
                "updated_at": row["updated_at"] or 0.0,
            }

    async def merge_state(
        self,
        *,
        scope_type: str,
        scope_key: str,
        state_patch: dict[str, Any] | None = None,
        emotion_patch: dict[str, Any] | None = None,
        source_turn_id: str = "",
    ) -> None:
        async with self._lock:
            await asyncio.to_thread(
                self._merge_state_sync,
                scope_type,
                scope_key,
                state_patch or {},
                emotion_patch or {},
                source_turn_id,
            )

    def _merge_state_sync(
        self,
        scope_type: str,
        scope_key: str,
        state_patch: dict[str, Any],
        emotion_patch: dict[str, Any],
        source_turn_id: str,
    ) -> None:
        # 状态更新采用 merge patch，而不是整表覆盖，
        # 方便提示词只输出增量字段。
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT state_json, emotion_json
                FROM mnemo_state
                WHERE scope_type = ? AND scope_key = ?
                """,
                (scope_type, scope_key),
            ).fetchone()
            state = _json_loads(row["state_json"], {}) if row else {}
            emotion = _json_loads(row["emotion_json"], {}) if row else {}
            state = _merge_dict(state, state_patch)
            emotion = _merge_dict(emotion, emotion_patch)
            now = time.time()
            conn.execute(
                """
                INSERT INTO mnemo_state (
                    scope_type, scope_key, state_json, emotion_json, updated_from_turn_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope_type, scope_key) DO UPDATE SET
                    state_json = excluded.state_json,
                    emotion_json = excluded.emotion_json,
                    updated_from_turn_id = excluded.updated_from_turn_id,
                    updated_at = excluded.updated_at
                """,
                (
                    scope_type,
                    scope_key,
                    _json_dumps(state),
                    _json_dumps(emotion),
                    source_turn_id,
                    now,
                ),
            )
            conn.commit()

    async def add_memory(
        self,
        *,
        scope_type: str,
        scope_key: str,
        content: str,
        memory_type: str = "note",
        importance: float = 0.5,
        metadata: dict[str, Any] | None = None,
        source_turn_id: str = "",
        dedupe_window: int = 20,
    ) -> str:
        async with self._lock:
            return await asyncio.to_thread(
                self._add_memory_sync,
                scope_type,
                scope_key,
                content,
                memory_type,
                importance,
                metadata or {},
                source_turn_id,
                dedupe_window,
            )

    def _add_memory_sync(
        self,
        scope_type: str,
        scope_key: str,
        content: str,
        memory_type: str,
        importance: float,
        metadata: dict[str, Any],
        source_turn_id: str,
        dedupe_window: int,
    ) -> str:
        with self._connect() as conn:
            normalized = _normalize_text(content)
            if normalized and dedupe_window > 0:
                rows = conn.execute(
                    """
                    SELECT memory_id, content
                    FROM mnemo_memory_item
                    WHERE scope_type = ? AND scope_key = ? AND memory_type = ?
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT ?
                    """,
                    (scope_type, scope_key, memory_type, dedupe_window),
                ).fetchall()
                for row in rows:
                    if _normalize_text(row["content"]) != normalized:
                        continue
                    conn.execute(
                        """
                        UPDATE mnemo_memory_item
                        SET updated_at = ?, importance = MAX(importance, ?)
                        WHERE memory_id = ?
                        """,
                        (time.time(), float(importance), row["memory_id"]),
                    )
                    conn.commit()
                    return str(row["memory_id"])

            memory_id = uuid.uuid4().hex
            now = time.time()
            conn.execute(
                """
                INSERT INTO mnemo_memory_item (
                    memory_id, scope_type, scope_key, memory_type, content,
                    metadata_json, importance, source_turn_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    memory_id,
                    scope_type,
                    scope_key,
                    memory_type,
                    content,
                    _json_dumps(metadata),
                    importance,
                    source_turn_id,
                    now,
                    now,
                ),
            )
            conn.commit()
        return memory_id

    async def list_recent_memories(
        self, scope_type: str, scope_key: str, limit: int
    ) -> list[dict[str, Any]]:
        async with self._lock:
            return await asyncio.to_thread(
                self._list_recent_memories_sync, scope_type, scope_key, limit
            )

    def _list_recent_memories_sync(
        self, scope_type: str, scope_key: str, limit: int
    ) -> list[dict[str, Any]]:
        # 记忆默认按创建时间倒序取最近窗口，交给提示词层自己决定如何使用。
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT memory_id, memory_type, content, metadata_json, importance,
                       source_turn_id, created_at, updated_at
                FROM mnemo_memory_item
                WHERE scope_type = ? AND scope_key = ?
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ?
                """,
                (scope_type, scope_key, limit),
            ).fetchall()
            return [
                {
                    "memory_id": row["memory_id"],
                    "memory_type": row["memory_type"],
                    "content": row["content"],
                    "metadata": _json_loads(row["metadata_json"], {}),
                    "importance": row["importance"],
                    "source_turn_id": row["source_turn_id"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]

    async def list_recent_turns(
        self,
        session_key: str,
        limit: int,
        include_source_types: tuple[str, ...] = ("chat", "push"),
        exclude_compressed: bool = False,
    ) -> list[dict[str, Any]]:
        async with self._lock:
            return await asyncio.to_thread(
                self._list_recent_turns_sync,
                session_key,
                limit,
                include_source_types,
                exclude_compressed,
            )

    def _list_recent_turns_sync(
        self,
        session_key: str,
        limit: int,
        include_source_types: tuple[str, ...],
        exclude_compressed: bool,
    ) -> list[dict[str, Any]]:
        # 这里返回的是“用于重建短期上下文”的原始 turn 记录。
        placeholders = ",".join("?" for _ in include_source_types)
        compressed_filter = "AND compressed_at IS NULL" if exclude_compressed else ""
        query = f"""
            SELECT turn_id, session_key, role, source_type, visible_text, raw_text,
                   hidden_payload_json, provider_id, prompt_snapshot_json, created_at, sent_at,
                   compressed_at, compressed_into_summary_id,
                   input_tokens_other, input_tokens_cached, output_tokens
            FROM mnemo_turn
            WHERE session_key = ? AND source_type IN ({placeholders}) {compressed_filter}
            ORDER BY created_at DESC
            LIMIT ?
        """
        params: list[Any] = [session_key, *include_source_types, limit]
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_turn(row) for row in rows]

    async def list_turns_for_summary(
        self,
        session_key: str,
        limit: int,
        include_source_types: tuple[str, ...] = ("chat", "push"),
    ) -> list[dict[str, Any]]:
        async with self._lock:
            return await asyncio.to_thread(
                self._list_turns_for_summary_sync,
                session_key,
                limit,
                include_source_types,
            )

    def _list_turns_for_summary_sync(
        self,
        session_key: str,
        limit: int,
        include_source_types: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in include_source_types)
        query = f"""
            SELECT turn_id, session_key, role, source_type, visible_text, raw_text,
                   hidden_payload_json, provider_id, prompt_snapshot_json, created_at, sent_at,
                   compressed_at, compressed_into_summary_id,
                   input_tokens_other, input_tokens_cached, output_tokens
            FROM mnemo_turn
            WHERE session_key = ? AND source_type IN ({placeholders}) AND compressed_at IS NULL
            ORDER BY created_at ASC
            LIMIT ?
        """
        params: list[Any] = [session_key, *include_source_types, limit]
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_turn(row) for row in rows]

    def _row_to_turn(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "turn_id": row["turn_id"],
            "session_key": row["session_key"],
            "role": row["role"],
            "source_type": row["source_type"],
            "visible_text": row["visible_text"],
            "raw_text": row["raw_text"],
            "hidden_payload": _json_loads(row["hidden_payload_json"], {}),
            "provider_id": row["provider_id"],
            "prompt_snapshot": _json_loads(row["prompt_snapshot_json"], {}),
            "created_at": row["created_at"],
            "sent_at": row["sent_at"],
            "compressed_at": row["compressed_at"],
            "compressed_into_summary_id": row["compressed_into_summary_id"],
            "input_tokens_other": int(row["input_tokens_other"] or 0),
            "input_tokens_cached": int(row["input_tokens_cached"] or 0),
            "output_tokens": int(row["output_tokens"] or 0),
        }

    async def get_session_summary(self, session_key: str) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._get_session_summary_sync, session_key)

    def _get_session_summary_sync(self, session_key: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT session_key, summary_text, covered_until_turn_id, covered_turn_count,
                       revision, updated_at, provider_id
                FROM mnemo_session_summary
                WHERE session_key = ?
                """,
                (session_key,),
            ).fetchone()
            if not row:
                return {
                    "session_key": session_key,
                    "summary_text": "",
                    "covered_until_turn_id": "",
                    "covered_turn_count": 0,
                    "revision": 0,
                    "updated_at": 0.0,
                    "provider_id": "",
                }
            return dict(row)

    async def upsert_session_summary(
        self,
        *,
        session_key: str,
        summary_text: str,
        covered_until_turn_id: str,
        covered_turn_count: int,
        provider_id: str,
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(
                self._upsert_session_summary_sync,
                session_key,
                summary_text,
                covered_until_turn_id,
                covered_turn_count,
                provider_id,
            )

    def _upsert_session_summary_sync(
        self,
        session_key: str,
        summary_text: str,
        covered_until_turn_id: str,
        covered_turn_count: int,
        provider_id: str,
    ) -> dict[str, Any]:
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT revision
                FROM mnemo_session_summary
                WHERE session_key = ?
                """,
                (session_key,),
            ).fetchone()
            revision = int(existing["revision"] or 0) + 1 if existing else 1
            now = time.time()
            conn.execute(
                """
                INSERT INTO mnemo_session_summary (
                    session_key, summary_text, covered_until_turn_id, covered_turn_count,
                    revision, updated_at, provider_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_key) DO UPDATE SET
                    summary_text = excluded.summary_text,
                    covered_until_turn_id = excluded.covered_until_turn_id,
                    covered_turn_count = excluded.covered_turn_count,
                    revision = excluded.revision,
                    updated_at = excluded.updated_at,
                    provider_id = excluded.provider_id
                """,
                (
                    session_key,
                    summary_text,
                    covered_until_turn_id,
                    int(covered_turn_count),
                    revision,
                    now,
                    provider_id,
                ),
            )
            conn.commit()
            return {
                "session_key": session_key,
                "summary_text": summary_text,
                "covered_until_turn_id": covered_until_turn_id,
                "covered_turn_count": int(covered_turn_count),
                "revision": revision,
                "updated_at": now,
                "provider_id": provider_id,
                "summary_ref": f"{session_key}:r{revision}",
            }

    async def mark_turns_compressed(
        self,
        turn_ids: list[str],
        compressed_into_summary_id: str,
    ) -> None:
        if not turn_ids:
            return
        async with self._lock:
            await asyncio.to_thread(
                self._mark_turns_compressed_sync,
                turn_ids,
                compressed_into_summary_id,
            )

    def _mark_turns_compressed_sync(
        self,
        turn_ids: list[str],
        compressed_into_summary_id: str,
    ) -> None:
        placeholders = ",".join("?" for _ in turn_ids)
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE mnemo_turn
                SET compressed_at = ?, compressed_into_summary_id = ?
                WHERE turn_id IN ({placeholders})
                """,
                (now, compressed_into_summary_id, *turn_ids),
            )
            conn.commit()

    async def get_relation(
        self,
        persona_id: str,
        platform_name: str,
        user_id: str,
        display_name: str = "",
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(
                self._get_relation_sync,
                persona_id,
                platform_name,
                user_id,
                display_name,
            )

    def _get_relation_sync(
        self,
        persona_id: str,
        platform_name: str,
        user_id: str,
        display_name: str,
    ) -> dict[str, Any]:
        relation_key = self.build_relation_key(persona_id, platform_name, user_id)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM mnemo_relation
                WHERE relation_key = ?
                """,
                (relation_key,),
            ).fetchone()
            if not row:
                return {
                    "relation_key": relation_key,
                    "persona_id": persona_id,
                    "platform_name": platform_name,
                    "user_id": user_id,
                    "display_name": display_name,
                    "favorability": 0.0,
                    "relation_stage": "",
                    "cognition": {},
                    "benefits": [],
                    "updated_from_turn_id": "",
                    "updated_at": 0.0,
                }
            return {
                "relation_key": row["relation_key"],
                "persona_id": row["persona_id"],
                "platform_name": row["platform_name"],
                "user_id": row["user_id"],
                "display_name": row["display_name"],
                "favorability": float(row["favorability"] or 0.0),
                "relation_stage": row["relation_stage"] or "",
                "cognition": _json_loads(row["cognition_json"], {}),
                "benefits": _json_loads(row["benefits_json"], []),
                "updated_from_turn_id": row["updated_from_turn_id"] or "",
                "updated_at": row["updated_at"] or 0.0,
            }

    async def merge_relation(
        self,
        *,
        persona_id: str,
        platform_name: str,
        user_id: str,
        display_name: str,
        patch: dict[str, Any],
        source_turn_id: str,
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(
                self._merge_relation_sync,
                persona_id,
                platform_name,
                user_id,
                display_name,
                patch,
                source_turn_id,
            )

    def _merge_relation_sync(
        self,
        persona_id: str,
        platform_name: str,
        user_id: str,
        display_name: str,
        patch: dict[str, Any],
        source_turn_id: str,
    ) -> dict[str, Any]:
        relation = self._get_relation_sync(
            persona_id=persona_id,
            platform_name=platform_name,
            user_id=user_id,
            display_name=display_name,
        )
        cognition = relation["cognition"] if isinstance(relation["cognition"], dict) else {}
        benefits = relation["benefits"] if isinstance(relation["benefits"], list) else []
        next_favorability = relation["favorability"]
        if "favorability" in patch:
            try:
                next_favorability = max(0.0, min(float(patch.get("favorability", 0.0)), 100.0))
            except (TypeError, ValueError):
                next_favorability = relation["favorability"]
        next_relation_stage = relation["relation_stage"]
        if "relation_stage" in patch and patch.get("relation_stage") is not None:
            next_relation_stage = str(patch.get("relation_stage") or "").strip()
        cognition_patch: dict[str, Any] = {}
        for key in ("labels", "impression", "user_traits", "wants_from_user", "risk_flags"):
            if key in patch:
                cognition_patch[key] = patch[key]
        cognition = _merge_dict(cognition, cognition_patch)
        if isinstance(patch.get("benefits"), list):
            appended = [str(item).strip() for item in patch["benefits"] if str(item).strip()]
            benefits = _dedupe_unique_tail([*benefits, *appended], 20)
        relation_key = self.build_relation_key(persona_id, platform_name, user_id)
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO mnemo_relation (
                    relation_key, persona_id, platform_name, user_id, display_name,
                    favorability, relation_stage, cognition_json, benefits_json,
                    updated_from_turn_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(relation_key) DO UPDATE SET
                    display_name = excluded.display_name,
                    favorability = excluded.favorability,
                    relation_stage = excluded.relation_stage,
                    cognition_json = excluded.cognition_json,
                    benefits_json = excluded.benefits_json,
                    updated_from_turn_id = excluded.updated_from_turn_id,
                    updated_at = excluded.updated_at
                """,
                (
                    relation_key,
                    persona_id,
                    platform_name,
                    user_id,
                    display_name,
                    next_favorability,
                    next_relation_stage,
                    _json_dumps(cognition),
                    _json_dumps(benefits),
                    source_turn_id,
                    now,
                ),
            )
            conn.commit()
        return {
            "relation_key": relation_key,
            "persona_id": persona_id,
            "platform_name": platform_name,
            "user_id": user_id,
            "display_name": display_name,
            "favorability": next_favorability,
            "relation_stage": next_relation_stage,
            "cognition": cognition,
            "benefits": benefits,
            "updated_from_turn_id": source_turn_id,
            "updated_at": now,
        }

    @staticmethod
    def build_relation_key(persona_id: str, platform_name: str, user_id: str) -> str:
        return f"{persona_id}:{platform_name}:{user_id}"

    async def insert_journal(
        self,
        *,
        content: str,
        summary: str,
        state_patch: dict[str, Any] | None,
        source_turn_id: str,
        idle_since: float | None,
        push_roll: float | None = None,
        push_sent_at: float | None = None,
    ) -> str:
        async with self._lock:
            return await asyncio.to_thread(
                self._insert_journal_sync,
                content,
                summary,
                state_patch or {},
                source_turn_id,
                idle_since,
                push_roll,
                push_sent_at,
            )

    def _insert_journal_sync(
        self,
        content: str,
        summary: str,
        state_patch: dict[str, Any],
        source_turn_id: str,
        idle_since: float | None,
        push_roll: float | None,
        push_sent_at: float | None,
    ) -> str:
        # journal 代表角色在对话间隙沉淀下来的幕后轨迹，不等同于聊天 turn。
        journal_id = uuid.uuid4().hex
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO mnemo_journal (
                    journal_id, generated_at, idle_since, content, summary,
                    state_patch_json, push_roll, push_sent_at, source_turn_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    journal_id,
                    now,
                    idle_since,
                    content,
                    summary,
                    _json_dumps(state_patch),
                    push_roll,
                    push_sent_at,
                    source_turn_id,
                ),
            )
            conn.commit()
        return journal_id

    async def list_recent_journals(self, limit: int) -> list[dict[str, Any]]:
        async with self._lock:
            return await asyncio.to_thread(self._list_recent_journals_sync, limit)

    def _list_recent_journals_sync(self, limit: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT journal_id, generated_at, idle_since, content, summary,
                       state_patch_json, push_roll, push_sent_at, source_turn_id
                FROM mnemo_journal
                ORDER BY generated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [
                {
                    "journal_id": row["journal_id"],
                    "generated_at": row["generated_at"],
                    "idle_since": row["idle_since"],
                    "content": row["content"],
                    "summary": row["summary"],
                    "state_patch": _json_loads(row["state_patch_json"], {}),
                    "push_roll": row["push_roll"],
                    "push_sent_at": row["push_sent_at"],
                    "source_turn_id": row["source_turn_id"],
                }
                for row in rows
            ]

    async def get_latest_session(self) -> dict[str, Any] | None:
        async with self._lock:
            return await asyncio.to_thread(self._get_latest_session_sync)

    def _get_latest_session_sync(self) -> dict[str, Any] | None:
        # 后台调度默认跟随最近活跃的那个会话推进。
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM mnemo_session
                ORDER BY COALESCE(last_user_message_at, 0) DESC, updated_at DESC
                LIMIT 1
                """
            ).fetchone()
            return dict(row) if row else None

    async def get_token_totals(self, session_key: str | None = None) -> dict[str, int]:
        async with self._lock:
            return await asyncio.to_thread(self._get_token_totals_sync, session_key)

    def _get_token_totals_sync(self, session_key: str | None = None) -> dict[str, int]:
        where = "WHERE role = 'assistant'"
        params: list[Any] = []
        if session_key:
            where += " AND session_key = ?"
            params.append(session_key)
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT
                    COALESCE(SUM(input_tokens_other), 0) AS input_other,
                    COALESCE(SUM(input_tokens_cached), 0) AS input_cached,
                    COALESCE(SUM(output_tokens), 0) AS output_tokens
                FROM mnemo_turn
                {where}
                """,
                params,
            ).fetchone()
            input_other = int(row["input_other"] or 0)
            input_cached = int(row["input_cached"] or 0)
            output_tokens = int(row["output_tokens"] or 0)
            return {
                "input_other": input_other,
                "input_cached": input_cached,
                "output": output_tokens,
                "input_total": input_other + input_cached,
                "total": input_other + input_cached + output_tokens,
            }

    async def get_stats(self) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._get_stats_sync)

    def _get_stats_sync(self) -> dict[str, Any]:
        # 状态命令只关心几个最小统计值，不在这里做复杂聚合。
        with self._connect() as conn:
            session_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM mnemo_session"
            ).fetchone()["cnt"]
            turn_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM mnemo_turn"
            ).fetchone()["cnt"]
            memory_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM mnemo_memory_item"
            ).fetchone()["cnt"]
            journal_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM mnemo_journal"
            ).fetchone()["cnt"]
            summary_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM mnemo_session_summary"
            ).fetchone()["cnt"]
            relation_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM mnemo_relation"
            ).fetchone()["cnt"]
            character_state = conn.execute(
                """
                SELECT state_json, emotion_json
                FROM mnemo_state
                WHERE scope_type = 'character' AND scope_key = ?
                """,
                (CHARACTER_SCOPE_KEY,),
            ).fetchone()
            token_row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(input_tokens_other), 0) AS input_other,
                    COALESCE(SUM(input_tokens_cached), 0) AS input_cached,
                    COALESCE(SUM(output_tokens), 0) AS output_tokens
                FROM mnemo_turn
                WHERE role = 'assistant'
                """
            ).fetchone()
            input_other = int(token_row["input_other"] or 0)
            input_cached = int(token_row["input_cached"] or 0)
            output_tokens = int(token_row["output_tokens"] or 0)
            return {
                "session_count": session_count,
                "turn_count": turn_count,
                "memory_count": memory_count,
                "journal_count": journal_count,
                "summary_count": summary_count,
                "relation_count": relation_count,
                "has_character_state": bool(character_state),
                "token_input_other": input_other,
                "token_input_cached": input_cached,
                "token_output": output_tokens,
                "token_total": input_other + input_cached + output_tokens,
            }
