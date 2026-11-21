"""Compare reasoning settings on frozen cases. Calls the model API only; never places orders."""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys
import time
from unittest.mock import patch

from followtrade.clients import AI_SYSTEM_PROMPT, Model, ServiceError, request_json
from followtrade.config import AIConfig


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def valid_schema(decision):
    try:
        fraction = Decimal(str(decision.get("order_notional_fraction")))
        leverage = decision.get("leverage")
        return (set(decision) == {"action", "order_notional_fraction", "leverage", "reason"}
                and decision.get("action") in ("follow", "skip")
                and isinstance(decision.get("reason"), str)
                and isinstance(decision.get("order_notional_fraction"), str)
                and fraction.is_finite() and fraction >= 0
                and type(leverage) is int and leverage >= 1
                and (decision["action"] != "follow" or fraction > 0)
                and (decision["action"] != "skip" or fraction == 0))
    except (InvalidOperation, TypeError):
        return False


def validate_response(result, config, context, assumptions):
    """Keep final JSON and usage, but never retain the provider's private reasoning text."""
    output = {"jsonValid": False, "schemaValid": False, "modelParserValid": False,
              "decision": None, "violations": [], "usage": result.get("usage") if isinstance(result, dict) else None}
    try:
        choice = result["choices"][0]
        output["finishReason"] = choice.get("finish_reason")
        decision = json.loads(choice["message"]["content"])
        output["jsonValid"] = isinstance(decision, dict)
        if not isinstance(decision, dict):
            return output
        output["decision"] = decision
        output["schemaValid"] = valid_schema(decision)
        with patch("followtrade.clients.request_json", return_value=result):
            normalized, requested_leverage, _ = Model(config).decide(context)
        output["modelParserValid"] = True
        output["normalizedFraction"] = str(normalized)
        output["requestedLeverage"] = requested_leverage
        if normalized:
            effective_leverage = assumptions.get("existingContractLeverage") or requested_leverage
            output["effectiveExecutionLeverage"] = effective_leverage
            equity = Decimal(str(context["account_equity_usdt"]))
            fee = Decimal(str(assumptions["takerFeeFraction"]))
            estimated_margin_and_fee = normalized * equity * (Decimal(1) / effective_leverage + fee)
            output["estimatedMarginPlusFeeUsdt"] = str(estimated_margin_and_fee)
            if estimated_margin_and_fee > Decimal(str(context["available_balance_usdt"])):
                output["violations"].append("suggested_increment_exceeds_available_margin_before_execution_clipping")
            if normalized * equity < Decimal(str(context["minimum_order_notional_usdt"])):
                output["violations"].append("suggested_increment_below_minimum_notional")
    except (ValueError, TypeError, KeyError, IndexError, InvalidOperation, ServiceError):
        pass
    return output


def worker():
    payload = json.load(sys.stdin)
    config = AIConfig(payload["baseUrl"], payload["model"], os.environ["FOLLOWTRADE_AI_KEY"],
                      payload["timeout"], payload["traderProfile"], False, payload["effort"])
    context = payload["context"]
    started = time.monotonic()
    body = {"model": config.model, "reasoning_effort": config.reasoning_effort, "messages": [
        {"role": "system", "content": AI_SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(
            {"trader_profile": config.profile, **context}, ensure_ascii=False)},
    ]}
    try:
        result = request_json("model API", config.base_url + "/chat/completions", body=body,
                              headers={"Authorization": "Bearer " + config.token}, timeout=config.timeout)
        elapsed = time.monotonic() - started
        outcome = validate_response(result, config, context, payload["assumptions"])
        outcome.update(status="completed", elapsedSeconds=round(elapsed, 4),
                       withinProductionDeadline=elapsed < payload["productionTimeout"])
    except ServiceError as error:
        outcome = {"status": "api_error", "error": str(error),
                   "elapsedSeconds": round(time.monotonic() - started, 4)}
    print(json.dumps(outcome, ensure_ascii=False, default=str))


def distribution(values):
    return {"n": len(values), "mean": statistics.mean(values), "median": statistics.median(values),
            "min": min(values), "max": max(values)} if values else {"n": 0}


def compare_pairs(pairs):
    comparable = [(a, b) for a, b in pairs if a.get("modelParserValid") and b.get("modelParserValid")]
    followed = [(a, b) for a, b in comparable if Decimal(a["normalizedFraction"]) and Decimal(b["normalizedFraction"])]
    return {
        "comparablePairs": len(comparable),
        "sameFollowSkip": sum(bool(Decimal(a["normalizedFraction"])) == bool(Decimal(b["normalizedFraction"])) for a, b in comparable),
        "bothFollowPairs": len(followed),
        "absoluteNotionalFractionDifferencePercentagePoints": distribution([
            float(abs(Decimal(a["normalizedFraction"]) - Decimal(b["normalizedFraction"])) * 100) for a, b in followed]),
        "absoluteRequestedLeverageDifference": distribution([abs(a["requestedLeverage"] - b["requestedLeverage"]) for a, b in followed]),
        "sameRequestedLeverage": sum(a["requestedLeverage"] == b["requestedLeverage"] for a, b in followed),
        "exactValuesExcludingReason": sum(
            (Decimal(a["normalizedFraction"]), a["requestedLeverage"]) == (Decimal(b["normalizedFraction"]), b["requestedLeverage"])
            for a, b in comparable),
    }


def summarize(runs):
    by_effort = {}
    for effort in ("low", "high"):
        group = [r for r in runs if r["effort"] == effort]
        completed = [r for r in group if r["status"] == "completed"]
        valid = [r for r in group if r.get("modelParserValid")]
        by_effort[effort] = {
            "requests": len(group), "completed": len(completed),
            "validJson": sum(r.get("jsonValid", False) for r in group),
            "validSchema": sum(r.get("schemaValid", False) for r in group),
            "validModelDecisions": len(valid),
            "validWithin60Seconds": sum(r.get("withinProductionDeadline", False) for r in valid),
            "observedDeadlineMisses": sum(not r.get("withinProductionDeadline", False) for r in completed)
                + sum(r["status"] == "wall_timeout" for r in group),
            "apiErrors": sum(r["status"] == "api_error" for r in group),
            "violations": sum(bool(r.get("violations")) for r in valid),
            "completedLatencySeconds": distribution([r["elapsedSeconds"] for r in completed]),
            "reasoningTokens": distribution([r["usage"]["completion_tokens_details"]["reasoning_tokens"] for r in completed
                if isinstance(r.get("usage"), dict) and isinstance(r["usage"].get("completion_tokens_details"), dict)
                and type(r["usage"]["completion_tokens_details"].get("reasoning_tokens")) is int]),
        }
    lookup = {(r["caseId"], r["repeat"], r["effort"]): r for r in runs}
    cross = [(r, lookup[(r["caseId"], r["repeat"], "high")]) for r in runs if r["effort"] == "low"
             and (r["caseId"], r["repeat"], "high") in lookup]
    repeats = {effort: compare_pairs([(r, lookup[(r["caseId"], 2, effort)]) for r in runs
        if r["effort"] == effort and r["repeat"] == 1 and (r["caseId"], 2, effort) in lookup]) for effort in ("low", "high")}
    return {"byEffort": by_effort, "lowVsHigh": compare_pairs(cross), "repeatVariability": repeats}


def evaluate(payload, case_id, repeat):
    started = time.monotonic()
    try:
        process = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--worker"],
                                 input=json.dumps(payload), text=True, encoding="utf-8", capture_output=True,
                                 timeout=payload["timeout"] + 2, check=True)
        result = json.loads(process.stdout)
    except subprocess.TimeoutExpired:
        result = {"status": "wall_timeout", "elapsedSeconds": round(time.monotonic() - started, 4), "censored": True}
    except (subprocess.CalledProcessError, ValueError):
        result = {"status": "probe_error", "elapsedSeconds": round(time.monotonic() - started, 4)}
    result.update(caseId=case_id, repeat=repeat, effort=payload["effort"], finishedAt=utc_now())
    return result


def main():
    if sys.argv[1:] == ["--worker"]:
        worker()
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, choices=(1, 2), default=2)
    parser.add_argument("--timeout", type=int, choices=range(60, 121), default=90)
    args = parser.parse_args()
    if not os.environ.get("FOLLOWTRADE_AI_KEY"):
        parser.error("FOLLOWTRADE_AI_KEY is required")
    encoded = args.cases.read_bytes()
    data = json.loads(encoded)
    assert 1 <= len(data["cases"]) <= 10
    tasks = []
    for repeat in range(1, args.repeats + 1):
        round_tasks = [(case, repeat, effort) for case in data["cases"] for effort in ("low", "high")]
        random.Random(20261121 + repeat).shuffle(round_tasks)
        tasks.extend(round_tasks)
    report = {"startedAt": utc_now(), "evaluationOnly": True, "ordersSubmitted": 0,
              "casesSha256": hashlib.sha256(encoded).hexdigest(), "model": data["model"],
              "systemPromptSha256": hashlib.sha256(AI_SYSTEM_PROMPT.encode()).hexdigest(),
              "repeats": args.repeats, "workers": 2, "wallTimeoutSeconds": args.timeout + 2,
              "productionTimeoutSeconds": data["productionTimeoutSeconds"], "methodology": data["methodology"], "runs": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        report["summary"] = summarize(report["runs"])
        temporary = args.output.with_suffix(".tmp")
        temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(args.output)

    save()
    with ThreadPoolExecutor(max_workers=2) as pool:
        submitted = []
        for case, repeat, effort in tasks:
            context = {**data["sharedContext"], **case["context"]}
            payload = {"baseUrl": data["baseUrl"], "model": data["model"], "traderProfile": data["traderProfile"],
                       "context": context, "assumptions": case["assumptions"], "effort": effort,
                       "timeout": args.timeout, "productionTimeout": data["productionTimeoutSeconds"]}
            submitted.append(pool.submit(evaluate, payload, case["id"], repeat))
        for future in as_completed(submitted):
            result = future.result()
            report["runs"].append(result)
            save()
            print(json.dumps({"progress": f'{len(report["runs"])}/{len(tasks)}', "case": result["caseId"],
                              "effort": result["effort"], "status": result["status"],
                              "seconds": result["elapsedSeconds"]}), flush=True)
    report["completedAt"] = utc_now()
    save()


if __name__ == "__main__":
    main()
