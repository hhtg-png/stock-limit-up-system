"""Interpreter tests use fake HTTP, real schemas and no persistence boundary."""
import importlib.util
import json
import unittest
from types import SimpleNamespace
from pathlib import Path

import httpx
from fastapi import FastAPI

if importlib.util.find_spec("app.services.dynamic_group_interpreter_service"):
    from app.services.dynamic_group_interpreter_service import DynamicGroupInterpreterService
else:
    DynamicGroupInterpreterService = None


PROMPT = "排除ST，涨幅至少5%"


def proposal(prompt=PROMPT):
    return {
        "status": "ready", "message": "已理解筛选条件", "assumptions": [],
        "questions": [], "unsupported": [],
        "definition": {"name": "强势观察", "rules": [
            {"field": "is_st", "op": "eq", "value": False},
            {"field": "change_pct", "op": "gte", "value": 5}],
            "strategy": {"prompt": prompt, "summary": "排除ST且涨幅至少5%", "assumptions": []}},
        "clauses": [
            {"text": "排除ST", "status": "implemented", "references": ["is_st"]},
            {"text": "涨幅至少5%", "status": "implemented", "references": ["change_pct"]}],
    }


class InterpreterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(DynamicGroupInterpreterService, "natural-language interpreter is missing")
        self.runtime = SimpleNamespace(DEEPSEEK_API_KEY="private-test-secret",
            DEEPSEEK_BASE_URL="https://api.deepseek.com", DEEPSEEK_MODEL="deepseek-flash",
            DEEPSEEK_REQUEST_TIMEOUT=20)
        self.payload = proposal()
        self.calls = []
        self.respond = None
        async def respond(request):
            self.calls.append(json.loads(request.content))
            if self.respond is not None:
                return await self.respond(request)
            return httpx.Response(200, json={"choices": [{"finish_reason": "stop",
                "message": {"content": json.dumps(self.payload, ensure_ascii=False)}}]})
        self.http = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        self.service = DynamicGroupInterpreterService(settings_provider=lambda: self.runtime,
                                                       http_client=self.http)

    async def asyncTearDown(self):
        if hasattr(self, "http"):
            await self.http.aclose()

    async def test_supported_definition_uses_current_settings_and_strict_local_validation(self):
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["definition"]["rules"][1]["value"], 5)
        self.assertEqual(result["definition"]["strategy"]["prompt"], PROMPT)
        self.assertNotIn("clauses", result)
        self.assertEqual(self.calls[0]["thinking"], {"type": "disabled"})
        self.runtime.DEEPSEEK_MODEL = "replacement-model"
        self.runtime.DEEPSEEK_BASE_URL = "https://compatible.invalid/v1"
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["model"], "replacement-model")
        self.assertEqual(self.calls[-1]["model"], "replacement-model")
        self.assertNotIn("thinking", self.calls[-1])

    async def test_refinement_supplies_current_definition_and_retains_original_input(self):
        current = proposal()["definition"]
        prompt = "把涨幅改为8%，其他不变"
        self.payload = proposal(prompt)
        self.payload["definition"]["rules"][1]["value"] = 8
        self.payload["clauses"] = [{"text": prompt, "status": "implemented",
                                      "references": ["change_pct", "current_definition"]}]
        result = await self.service.interpret(prompt, current_definition=current)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["definition"]["rules"][1]["value"], 8)
        self.assertFalse(result["definition"]["rules"][0]["value"])
        sent = json.loads(self.calls[0]["messages"][1]["content"])
        self.assertEqual(sent["current_definition"]["rules"], current["rules"])
        self.assertEqual(sent["prompt"], prompt)

    async def test_nested_recent_session_rules_and_explicit_historical_ranking_days(self):
        prompt = "近5个交易日涨停至少2次，且今日涨幅大于5%或当前封板，按近5日涨停次数取前20只"
        self.payload = proposal(prompt)
        self.payload["definition"]["rules"] = [{"all": [
            {"field": "recent_limit_up_count", "days": 5, "op": "gte", "value": 2},
            {"any": [{"field": "change_pct", "op": "gt", "value": 5},
                       {"field": "is_sealed", "op": "eq", "value": True}]}]}]
        self.payload["definition"]["strategy"]["ranking"] = {
            "field": "recent_limit_up_count", "days": 5, "direction": "desc", "limit": 20}
        self.payload["clauses"] = [{"text": prompt, "status": "implemented",
            "references": ["recent_limit_up_count", "change_pct", "is_sealed", "strategy.ranking"]}]
        result = await self.service.interpret(prompt)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["definition"]["strategy"]["ranking"]["days"], 5)
        self.assertIn("已完成", " ".join(result["assumptions"]))

    async def test_unknown_field_cannot_be_an_executable_ready_plan(self):
        self.payload["definition"]["rules"].append({"field": "invented_indicator", "op": "gte", "value": 1})
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "unsupported")
        self.assertIsNone(result["definition"])
        self.assertTrue(result["unsupported"])

    async def test_unsupported_clause_cannot_be_omitted_by_a_fluent_ready_reply(self):
        result = await self.service.interpret("排除ST，涨幅至少5%，MACD金叉")
        self.assertEqual(result["status"], "unsupported")
        self.assertIsNone(result["definition"])
        self.assertTrue(result["unsupported"])

    async def test_unaccounted_prompt_text_requires_clarification(self):
        result = await self.service.interpret(PROMPT + "，此外波动要小")
        self.assertEqual(result["status"], "needs_clarification")
        self.assertIsNone(result["definition"])
        self.assertTrue(result["questions"])

    async def test_model_ambiguity_and_unsupported_statuses_never_expose_definition(self):
        for status, key in (("needs_clarification", "questions"), ("unsupported", "unsupported")):
            self.payload = {"status": status, "message": "需要补充", "definition": None,
                "assumptions": [], "questions": [], "unsupported": [], "clauses": []}
            self.payload[key] = ["请明确强势的指标" if key == "questions" else "暂不支持该指标"]
            result = await self.service.interpret("选强势股")
            self.assertEqual(result["status"], status)
            self.assertIsNone(result["definition"])

    async def test_missing_api_key_and_provider_failures_are_safe_and_never_default_rules(self):
        self.runtime.DEEPSEEK_API_KEY = None
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "error")
        self.assertEqual(len(self.calls), 0)
        self.runtime.DEEPSEEK_API_KEY = "private-test-secret"
        async def bad(request):
            return httpx.Response(400, json={"error": {"message": "private-test-secret provider internals"}})
        self.respond = bad
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "error")
        self.assertNotIn("private-test-secret", json.dumps(result))
        self.assertNotIn("provider internals", json.dumps(result))
        self.assertIsNone(result["definition"])
        self.assertEqual(len(self.calls), 1)

    async def test_timeout_is_reported_without_raw_exception_or_secret(self):
        async def bad(request):
            raise httpx.ReadTimeout("private-test-secret timeout", request=request)
        self.respond = bad
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "error")
        self.assertIn("超时", result["message"])
        self.assertNotIn("private-test-secret", json.dumps(result))

    async def test_malformed_and_truncated_json_fail_closed_with_bounded_attempts(self):
        for content, finish in (("{broken", "stop"), ('{"status":"ready"', "length"), ("", "stop")):
            self.calls.clear()
            async def bad(request):
                return httpx.Response(200, json={"choices": [{"finish_reason": finish,
                    "message": {"content": content}}]})
            self.respond = bad
            result = await self.service.interpret(PROMPT)
            self.assertEqual(result["status"], "error")
            self.assertIsNone(result["definition"])
            self.assertLessEqual(len(self.calls), 2)

    async def test_model_code_and_secret_echo_cannot_escape_local_validation(self):
        self.payload["definition"]["sql"] = "select * from stock"
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "error")
        self.assertIsNone(result["definition"])
        self.payload = proposal()
        self.payload["message"] = "private-test-secret"
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "error")
        self.assertNotIn("private-test-secret", json.dumps(result))

    async def test_model_cannot_mark_unresolved_clauses_ready(self):
        self.payload["clauses"][1]["status"] = "needs_clarification"
        self.payload["clauses"][1]["reason"] = "需要确认涨幅阈值"
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "needs_clarification")
        self.assertIsNone(result["definition"])

    async def test_new_smart_groups_default_to_dynamic_and_explicit_keep_uses_retain(self):
        self.payload["definition"]["membership_mode"] = "retain"
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["definition"]["membership_mode"], "dynamic")
        prompt = PROMPT + "，入选后一直跟踪保留"
        self.payload = proposal(prompt)
        self.payload["clauses"].append({"text": "入选后一直跟踪保留", "status": "implemented",
                                           "references": ["membership_mode"]})
        result = await self.service.interpret(prompt)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["definition"]["membership_mode"], "retain")

    async def test_refinement_preserves_current_membership_unless_explicitly_changed(self):
        current = proposal()["definition"]
        current["membership_mode"] = "retain"
        self.payload["definition"]["membership_mode"] = "dynamic"
        result = await self.service.interpret(PROMPT, current_definition=current)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["definition"]["membership_mode"], "retain")

    async def test_quoting_entire_prompt_does_not_excuse_omitting_explicit_st_filter(self):
        self.payload["definition"]["rules"] = [{"field": "change_pct", "op": "gte", "value": 5}]
        self.payload["clauses"] = [{"text": PROMPT, "status": "implemented", "references": ["change_pct"]}]
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "needs_clarification")
        self.assertIsNone(result["definition"])

    async def test_generic_dynamic_group_label_does_not_override_explicit_keep_intent(self):
        prompt = "创建动态分组，排除ST，涨幅至少5%，入选后一直跟踪保留"
        self.payload = proposal(prompt)
        self.payload["clauses"] = [{"text": prompt, "status": "implemented",
            "references": ["is_st", "change_pct", "membership_mode"]}]
        result = await self.service.interpret(prompt)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["definition"]["membership_mode"], "retain")

    async def test_single_compound_root_is_wrapped_without_changing_meaning(self):
        rules = self.payload["definition"]["rules"]
        self.payload["definition"]["rules"] = {"all": rules}
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["definition"]["rules"], [{"all": rules}])

    async def test_ranking_field_can_be_referenced_without_a_filter_on_that_field(self):
        prompt = PROMPT + "，按成交额取前20只"
        self.payload["definition"]["strategy"]["ranking"] = {"field": "amount_yi", "direction": "desc", "limit": 20}
        self.payload["clauses"].append({"text": "按成交额取前20只", "status": "implemented",
            "references": ["amount_yi", "strategy.ranking"]})
        result = await self.service.interpret(prompt)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["definition"]["strategy"]["ranking"]["field"], "amount_yi")

    async def test_explicitly_cancelled_unsupported_conditions_do_not_block_supported_query(self):
        cases = [
            ("只按今日涨幅大于5%筛选，均线不做要求", [
                {"field": "change_pct", "op": "gt", "value": 5}],
                [{"text": "只按今日涨幅大于5%筛选", "status": "implemented", "references": ["change_pct"]},
                 {"text": "均线不做要求", "status": "removed", "references": []}]),
            ("取消均线条件，只保留非ST且高开2%-5%", [
                {"field": "is_st", "op": "eq", "value": False},
                {"field": "today_open_change_pct", "op": "between", "value": 2, "value2": 5}],
                [{"text": "取消均线条件", "status": "removed", "references": []},
                 {"text": "只保留非ST且高开2%-5%", "status": "implemented", "references": ["is_st", "today_open_change_pct"]}]),
        ]
        for prompt, rules, clauses in cases:
            self.payload = proposal(prompt)
            self.payload["definition"]["rules"] = rules
            self.payload["clauses"] = clauses
            result = await self.service.interpret(prompt)
            self.assertEqual(result["status"], "ready", result)
        self.assertEqual(len(self.calls), 2)

    async def test_comparison_negation_is_not_mistaken_for_cancelling_a_feature(self):
        for prompt in ("不要低于5日均线", "不要取消均线条件，涨幅至少5%"):
            result = await self.service.interpret(prompt)
            self.assertEqual(result["status"], "unsupported")
            self.assertIsNone(result["definition"])
        self.assertEqual(len(self.calls), 0)

    async def test_negative_tracking_instruction_does_not_force_retain(self):
        prompt = "不要持续跟踪，涨幅至少5%，不达标就退出"
        self.payload = proposal(prompt)
        self.payload["definition"]["membership_mode"] = "dynamic"
        self.payload["definition"]["rules"] = [{"field": "change_pct", "op": "gte", "value": 5}]
        self.payload["clauses"] = [{"text": prompt, "status": "implemented",
            "references": ["change_pct", "membership_mode"]}]
        result = await self.service.interpret(prompt)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["definition"]["membership_mode"], "dynamic")

    async def test_equivalent_st_negation_in_any_tree_is_valid(self):
        prompt = "排除ST，或今日涨幅大于5%"
        self.payload = proposal(prompt)
        self.payload["definition"]["rules"] = [{"any": [
            {"field": "is_st", "op": "ne", "value": True},
            {"field": "change_pct", "op": "gt", "value": 5}]}]
        self.payload["clauses"] = [{"text": prompt, "status": "implemented", "references": ["is_st", "change_pct"]}]
        result = await self.service.interpret(prompt)
        self.assertEqual(result["status"], "ready", result)

    async def test_quoted_numeric_conditions_require_the_corresponding_executable_fields(self):
        for phrase in ("股价低于10元", "换手率大于3%", "成交额至少1亿元", "流通市值少于100亿元"):
            prompt = "排除ST，" + phrase
            self.payload = proposal(prompt)
            self.payload["definition"]["rules"] = [{"field": "is_st", "op": "eq", "value": False}]
            self.payload["clauses"] = [{"text": prompt, "status": "implemented", "references": ["is_st"]}]
            result = await self.service.interpret(prompt)
            self.assertEqual(result["status"], "needs_clarification", result)
            self.assertIsNone(result["definition"])

    async def test_metric_audit_respects_explicitly_removed_metric(self):
        prompt = "不考虑成交额，排除ST，股价低于10元"
        self.payload = proposal(prompt)
        self.payload["definition"]["rules"] = [{"field": "is_st", "op": "eq", "value": False},
            {"field": "current_price", "op": "lt", "value": 10}]
        self.payload["clauses"] = [
            {"text": "不考虑成交额", "status": "removed", "references": []},
            {"text": "排除ST，股价低于10元", "status": "implemented", "references": ["is_st", "current_price"]}]
        result = await self.service.interpret(prompt)
        self.assertEqual(result["status"], "ready", result)

    async def test_total_market_value_is_unsupported_and_unspecified_market_value_is_ambiguous(self):
        result = await self.service.interpret("总市值至少100亿元")
        self.assertEqual(result["status"], "unsupported")
        self.assertIsNone(result["definition"])
        result = await self.service.interpret("市值至少100亿元")
        self.assertEqual(result["status"], "needs_clarification")
        self.assertIsNone(result["definition"])

    async def test_cancelled_supported_metric_cannot_remain_in_rule_tree_or_ranking(self):
        prompt = "不考虑成交额，排除ST，股价低于10元"
        for mode in ("rule", "ranking"):
            self.payload = proposal(prompt)
            self.payload["definition"]["rules"] = [{"all": [
                {"field": "is_st", "op": "eq", "value": False},
                {"field": "current_price", "op": "lt", "value": 10}]}]
            if mode == "rule":
                self.payload["definition"]["rules"][0]["all"].append({"field": "amount_yi", "op": "gt", "value": 5})
            else:
                self.payload["definition"]["strategy"]["ranking"] = {"field": "amount_yi", "direction": "desc", "limit": 20}
            self.payload["clauses"] = [
                {"text": "不考虑成交额", "status": "removed", "references": []},
                {"text": "排除ST，股价低于10元", "status": "implemented", "references": ["is_st", "current_price"]}]
            result = await self.service.interpret(prompt)
            self.assertEqual(result["status"], "needs_clarification", result)
            self.assertIsNone(result["definition"])

    async def test_model_format_example_does_not_request_duplicate_strategy_metadata(self):
        result = await self.service.interpret(PROMPT)
        self.assertEqual(result["status"], "ready")
        system = self.calls[0]["messages"][0]["content"]
        example_start = system.index("完整结构示例")
        example, _ = json.JSONDecoder().raw_decode(system[system.index("{", example_start):])
        strategy = example["definition"]["strategy"]
        self.assertIn("summary", strategy)
        self.assertNotIn("prompt", strategy)
        self.assertNotIn("assumptions", strategy)
        self.assertIn("assumptions", example)

    async def test_real_bad_strategy_reply_is_sent_back_once_with_safe_error_location(self):
        prompt = "主板非ST，近5个已完成交易日有过收盘涨停，今日竞价高开2%到5%，按成交额从高到低取前20只。"
        bad = (Path(__file__).parent / "fixtures" / "dynamic_group_interpreter_bad_strategy.txt").read_text(encoding="utf-8").strip()
        self.payload = proposal(prompt)
        self.payload["definition"]["rules"] = [
            {"field": "board_type", "op": "eq", "value": "main"},
            {"field": "is_st", "op": "eq", "value": False},
            {"field": "recent_limit_up_count", "days": 5, "op": "gte", "value": 1},
            {"field": "today_open_change_pct", "op": "between", "value": 2, "value2": 5}]
        self.payload["definition"]["strategy"] = {"summary": "主板非ST、近5日涨停、竞价2%-5%、成交额前20",
            "ranking": {"field": "amount_yi", "direction": "desc", "limit": 20}}
        self.payload["clauses"] = [{"text": prompt, "status": "implemented",
            "references": ["board_type", "is_st", "recent_limit_up_count", "today_open_change_pct", "amount_yi", "strategy.ranking"]}]
        async def reply(request):
            content = bad if len(self.calls) == 1 else json.dumps(self.payload, ensure_ascii=False)
            return httpx.Response(200, json={"choices": [{"finish_reason": "stop", "message": {"content": content}}]})
        self.respond = reply
        result = await self.service.interpret(prompt)
        self.assertEqual(result["status"], "ready", result)
        self.assertEqual(len(self.calls), 2)
        repair = self.calls[1]["messages"]
        self.assertEqual(repair[-2], {"role": "assistant", "content": bad})
        self.assertIn("行", repair[-1]["content"])
        self.assertIn("列", repair[-1]["content"])
        self.assertIn("字符", repair[-1]["content"])
        definition = result["definition"]
        self.assertEqual(definition["strategy"]["prompt"], prompt)
        self.assertEqual(definition["rules"], self.payload["definition"]["rules"])
        self.assertEqual(definition["strategy"]["ranking"]["limit"], 20)
        self.assertTrue(definition["strategy"]["assumptions"])

    async def test_repair_does_not_echo_secret_truncated_or_oversized_content(self):
        for content, finish in (("{private-test-secret", "stop"), ("{incomplete", "length"),
                                ("{" + "x" * 13000, "stop")):
            self.calls.clear()
            async def reply(request):
                return httpx.Response(200, json={"choices": [{"finish_reason": finish,
                    "message": {"content": content}}]})
            self.respond = reply
            result = await self.service.interpret(PROMPT)
            self.assertEqual(result["status"], "error")
            self.assertIsNone(result["definition"])
            self.assertEqual(len(self.calls), 2)
            self.assertFalse(any(message["role"] == "assistant" for message in self.calls[1]["messages"]))
            self.assertNotIn("private-test-secret", json.dumps(self.calls[1]))
            self.assertNotIn(content, json.dumps(self.calls[1]))

    async def test_duplicate_keys_and_nonfinite_values_are_never_locally_repaired(self):
        for content in ('{"status":"ready","status":"error"}', '{"status":"ready","definition":{"n":NaN}}'):
            self.calls.clear()
            async def reply(request):
                return httpx.Response(200, json={"choices": [{"finish_reason": "stop",
                    "message": {"content": content}}]})
            self.respond = reply
            result = await self.service.interpret(PROMPT)
            self.assertEqual(result["status"], "error")
            self.assertIsNone(result["definition"])
            self.assertEqual(len(self.calls), 2)


class InterpreterAPI(unittest.IsolatedAsyncioTestCase):
    async def test_interpret_is_static_bounded_and_does_not_create_a_group(self):
        self.assertIsNotNone(DynamicGroupInterpreterService, "interpreter is missing")
        from app.api.v1 import dynamic_groups
        from unittest.mock import AsyncMock, patch
        app = FastAPI()
        app.include_router(dynamic_groups.router, prefix="/api/v1/dynamic-groups")
        result = {"status": "needs_clarification", "message": "请明确", "definition": None,
            "assumptions": [], "questions": ["涨幅至少多少？"], "unsupported": [], "model": None}
        with patch.object(dynamic_groups.dynamic_group_interpreter_service, "interpret", AsyncMock(return_value=result)), \
                patch.object(dynamic_groups.dynamic_group_service, "create_group", AsyncMock(side_effect=AssertionError("interpret must not create"))):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
                response = await client.post("/api/v1/dynamic-groups/interpret", json={"prompt": "选强势股"})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), result)
                for prompt in ("", " " * 5, "选" * 2001):
                    response = await client.post("/api/v1/dynamic-groups/interpret", json={"prompt": prompt})
                    self.assertEqual(response.status_code, 422)
