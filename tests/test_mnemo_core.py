import shutil
import tempfile
import unittest
from pathlib import Path

from mnemo_parser import has_mnemosyne_meta, parse_hidden_blocks, parse_mnemosyne_response
from mnemo_prompts import PromptStore, render_template
from mnemo_storage import MnemoStorage


class PromptTests(unittest.TestCase):
    def test_render_template(self):
        rendered = render_template(
            "hello {{ name }} - {{missing}}",
            {"name": "mnemo"},
        )
        self.assertEqual(rendered, "hello mnemo - ")

    def test_prompt_store_bootstrap(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            template_path = tmp_path / "template.json"
            user_path = tmp_path / "user.json"
            template_path.write_text('{"ok": true}', encoding="utf-8")
            store = PromptStore(template_path, user_path)
            payload = store.load()
            self.assertTrue(user_path.exists())
            self.assertTrue(payload["ok"])

    def test_prompt_store_joins_template_arrays(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            template_path = tmp_path / "template.json"
            user_path = tmp_path / "user.json"
            template_path.write_text(
                '{"chat":{"inject_template":["a","b"]},"background":{"journal_template":["c","d"],"active_push_template":["e","f"]}}',
                encoding="utf-8",
            )
            store = PromptStore(template_path, user_path)
            payload = store.load()
            self.assertEqual(payload["chat"]["inject_template"], "a\nb")
            self.assertEqual(payload["background"]["journal_template"], "c\nd")
            self.assertEqual(payload["background"]["active_push_template"], "e\nf")


class ParserTests(unittest.TestCase):
    def test_parse_hidden_blocks(self):
        text = (
            "可见内容\n"
            "<character_state_patch>{\"mood\": \"warm\"}</character_state_patch>\n"
            "<journal_entry>今天散步了</journal_entry>"
        )
        parsed = parse_hidden_blocks(
            text,
            [
                {
                    "name": "character_state_patch",
                    "target": "character_state_patch",
                    "mode": "json",
                    "pattern": "<character_state_patch>([\\s\\S]*?)</character_state_patch>",
                },
                {
                    "name": "journal_entry",
                    "target": "journal_entry",
                    "mode": "text",
                    "pattern": "<journal_entry>([\\s\\S]*?)</journal_entry>",
                },
            ],
        )
        self.assertEqual(parsed.visible_text, "可见内容")
        self.assertEqual(parsed.blocks[0].payload["mood"], "warm")
        self.assertEqual(parsed.blocks[1].payload, "今天散步了")

    def test_parse_hidden_blocks_tolerates_bad_json(self):
        parsed = parse_hidden_blocks(
            "<character_state_patch>{bad json}</character_state_patch>",
            [
                {
                    "name": "character_state_patch",
                    "target": "character_state_patch",
                    "mode": "json",
                    "pattern": "<character_state_patch>([\\s\\S]*?)</character_state_patch>",
                }
            ],
        )
        self.assertEqual(parsed.visible_text, "")
        self.assertEqual(parsed.blocks[0].payload["_parse_error"], "json")

    def test_parse_mnemosyne_response_requires_outer_wrapper(self):
        parsed = parse_mnemosyne_response(
            "visible\n<mnemosyne_meta><character_state_patch>{\"mood\":\"warm\"}</character_state_patch><journal_entry>note</journal_entry></mnemosyne_meta>",
            [
                {
                    "name": "mnemosyne_meta",
                    "target": "mnemosyne_meta",
                    "mode": "text",
                    "pattern": "<mnemosyne_meta>([\\s\\S]*?)</mnemosyne_meta>",
                },
                {
                    "name": "character_state_patch",
                    "target": "character_state_patch",
                    "mode": "json",
                    "pattern": "<character_state_patch>([\\s\\S]*?)</character_state_patch>",
                },
                {
                    "name": "journal_entry",
                    "target": "journal_entry",
                    "mode": "text",
                    "pattern": "<journal_entry>([\\s\\S]*?)</journal_entry>",
                },
            ],
        )
        self.assertTrue(parsed.meta_present)
        self.assertEqual(parsed.visible_text, "visible")
        self.assertEqual(parsed.blocks[0].target, "character_state_patch")
        self.assertEqual(parsed.blocks[0].payload["mood"], "warm")
        self.assertEqual(parsed.blocks[1].target, "journal_entry")
        self.assertEqual(parsed.blocks[1].payload, "note")

    def test_parse_mnemosyne_response_without_wrapper_keeps_legacy_fallback(self):
        parsed = parse_mnemosyne_response(
            "visible\n<character_state_patch>{\"mood\":\"warm\"}</character_state_patch>",
            [
                {
                    "name": "character_state_patch",
                    "target": "character_state_patch",
                    "mode": "json",
                    "pattern": "<character_state_patch>([\\s\\S]*?)</character_state_patch>",
                }
            ],
        )
        self.assertFalse(parsed.meta_present)
        self.assertEqual(parsed.visible_text, "visible")
        self.assertEqual(parsed.blocks[0].payload["mood"], "warm")
        self.assertFalse(has_mnemosyne_meta(parsed.visible_text))


class StorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_storage_roundtrip(self):
        tmp = tempfile.mkdtemp()
        try:
            db_path = Path(tmp) / "mnemo.sqlite3"
            storage = MnemoStorage(db_path)
            await storage.initialize()
            await storage.upsert_session(
                session_key="demo",
                unified_msg_origin="demo",
                platform_name="test",
                user_id="u1",
                display_name="tester",
                persona_id="persona-a",
                provider_id="provider-a",
                user_message_at=1.0,
            )
            turn_id = await storage.insert_turn(
                session_key="demo",
                role="assistant",
                source_type="chat",
                visible_text="hello",
                raw_text="hello",
                hidden_payload={"a": 1},
                provider_id="provider-a",
                prompt_snapshot={"x": 1},
                sent_at=2.0,
            )
            await storage.merge_state(
                scope_type="user",
                scope_key="demo",
                state_patch={"trust": 3},
                emotion_patch={"mood": "calm"},
                source_turn_id=turn_id,
            )
            await storage.add_memory(
                scope_type="user",
                scope_key="demo",
                content="喜欢夜聊",
                memory_type="preference",
                importance=0.8,
                metadata={"source": "test"},
                source_turn_id=turn_id,
            )
            await storage.insert_journal(
                content="今天看见下雨了",
                summary="今天看见下雨了",
                state_patch={"weather": "rain"},
                source_turn_id=turn_id,
                idle_since=0.0,
            )

            state = await storage.get_state("user", "demo")
            memories = await storage.list_recent_memories("user", "demo", 5)
            journals = await storage.list_recent_journals(5)
            turns = await storage.list_recent_turns("demo", 5)
            stats = await storage.get_stats()

            self.assertEqual(state["state"]["trust"], 3)
            self.assertEqual(state["emotion"]["mood"], "calm")
            self.assertEqual(memories[0]["memory_type"], "preference")
            self.assertEqual(journals[0]["content"], "今天看见下雨了")
            self.assertEqual(turns[0]["role"], "assistant")
            self.assertEqual(stats["session_count"], 1)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
