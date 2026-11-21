import json
from unittest import TestCase

from compare_reasoning import compare_pairs, summarize, validate_response
from followtrade.config import AIConfig


class ReasoningComparisonTest(TestCase):
    config = AIConfig("https://example.test", "test", "not-in-prompt", 90, "profile", False, "low")
    context = {"account_equity_usdt": "1000", "available_balance_usdt": "200",
               "minimum_order_notional_usdt": "10", "max_leverage": 10}
    assumptions = {"existingContractLeverage": 5, "takerFeeFraction": ".0005"}

    def result(self, **changes):
        decision = {"action": "follow", "order_notional_fraction": ".5", "leverage": 10, "reason": "test"}
        decision.update(changes)
        return {"choices": [{"message": {"content": json.dumps(decision),
                                           "reasoning_content": "must not be retained"}, "finish_reason": "stop"}]}

    def test_schema_and_parser_are_independent_and_reasoning_is_not_saved(self):
        result = self.result()
        parsed = validate_response(result, self.config, self.context, self.assumptions)
        self.assertTrue(parsed["schemaValid"])
        self.assertTrue(parsed["modelParserValid"])
        self.assertNotIn("must not be retained", json.dumps(parsed))
        result["choices"][0]["message"]["content"] = '{"action":"skip","reason":"insufficient evidence"}'
        parsed = validate_response(result, self.config, self.context, self.assumptions)
        self.assertTrue(parsed["modelParserValid"])
        self.assertFalse(parsed["schemaValid"])

    def test_margin_check_uses_retained_contract_leverage_and_includes_fee(self):
        parsed = validate_response(self.result(order_notional_fraction="1"), self.config, self.context, self.assumptions)
        self.assertEqual(parsed["requestedLeverage"], 10)
        self.assertEqual(parsed["effectiveExecutionLeverage"], 5)
        self.assertIn("suggested_increment_exceeds_available_margin_before_execution_clipping", parsed["violations"])

    def test_invalid_numeric_or_boolean_outputs_are_not_counted_as_valid(self):
        for changed in ({"order_notional_fraction": "NaN"}, {"leverage": True}, {"leverage": 20}):
            with self.subTest(changed=changed), self.assertLogs(level="WARNING"):
                parsed = validate_response(self.result(**changed), self.config, self.context, self.assumptions)
            self.assertFalse(parsed["modelParserValid"])

    def test_summary_keeps_timeouts_errors_and_late_answers_in_denominator(self):
        runs = [
            {"caseId": "a", "repeat": 1, "effort": "low", "status": "completed", "elapsedSeconds": 8,
             "modelParserValid": True, "normalizedFraction": ".5", "requestedLeverage": 5,
             "withinProductionDeadline": True, "schemaValid": True, "jsonValid": True},
            {"caseId": "a", "repeat": 1, "effort": "high", "status": "completed", "elapsedSeconds": 65,
             "modelParserValid": True, "normalizedFraction": "0.50", "requestedLeverage": 5,
             "withinProductionDeadline": False, "schemaValid": True, "jsonValid": True},
            {"caseId": "b", "repeat": 1, "effort": "high", "status": "wall_timeout", "elapsedSeconds": 92},
            {"caseId": "c", "repeat": 1, "effort": "high", "status": "api_error", "elapsedSeconds": 2},
        ]
        summary = summarize(runs)
        self.assertEqual(summary["byEffort"]["high"]["requests"], 3)
        self.assertEqual(summary["byEffort"]["high"]["observedDeadlineMisses"], 2)
        self.assertEqual(summary["byEffort"]["high"]["apiErrors"], 1)
        self.assertEqual(summary["byEffort"]["high"]["validWithin60Seconds"], 0)
        self.assertEqual(summary["lowVsHigh"]["exactValuesExcludingReason"], 1)

    def test_pair_comparison_uses_percentage_points_and_excludes_failed_runs(self):
        def row(fraction):
            return {"modelParserValid": True, "normalizedFraction": fraction, "requestedLeverage": 5}
        result = compare_pairs([(row(".4"), row(".5")), (row(".3"), {"status": "wall_timeout"})])
        self.assertEqual(result["comparablePairs"], 1)
        self.assertEqual(result["absoluteNotionalFractionDifferencePercentagePoints"]["mean"], 10)
