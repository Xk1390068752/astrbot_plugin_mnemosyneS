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
    merged = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


class MnemoStorage:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=DELETE;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    async def initialize(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
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
                ("schema_version", "1", now),
            )
            conn.commit()

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
    ) -> str:
        turn_id = uuid.uuid4().hex
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO mnemo_turn (
                    turn_id, session_key, role, source_type, visible_text, raw_text,
                    hidden_payload_json, provider_id, prompt_snapshot_json, created_at, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )
            conn.commit()
        return turn_id

    async def get_state(self, scope_type: str, scope_key: str) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._get_state_sync, scope_type, scope_key)

    def _get_state_sync(self, scope_type: str, scope_key: str) -> dict[str, Any]:
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
    ) -> str:
        memory_id = uuid.uuid4().hex
        now = time.time()
        with self._connect() as conn:
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
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT memory_id, memory_type, content, metadata_json, importance,
                       source_turn_id, created_at, updated_at
                FROM mnemo_memory_item
                WHERE scope_type = ? AND scope_key = ?
                ORDER BY created_at DESC
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

    async def get_stats(self) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._get_stats_sync)

    def _get_stats_sync(self) -> dict[str, Any]:
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
            character_state = conn.execute(
                """
                SELECT state_json, emotion_json
                FROM mnemo_state
                WHERE scope_type = 'character' AND scope_key = ?
                """,
                (CHARACTER_SCOPE_KEY,),
            ).fetchone()
            return {
                "session_count": session_count,
                "turn_count": turn_count,
                "memory_count": memory_count,
                "journal_count": journal_count,
                "has_character_state": bool(character_state),
            }
