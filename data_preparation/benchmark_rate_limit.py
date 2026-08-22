"""Probe DPP's scrape rate limits with an escalating-concurrency benchmark.

Sends journey-planning requests to spojeni.dpp.cz exactly the way the
scraper does (same URL, params, cookies, headers) at increasing concurrency,
records the achieved request rate per tier, and reports the highest
sustainable rate before the service starts throttling.

Throttling signals:
  * HTTP 401/403/429
  * HTTP 5xx
  * HTTP 200 pages that look like a bot-check / challenge page
  * transport-level errors (timeout, connection reset)

The benchmark runs each concurrency tier for a short fixed window, backs off
between tiers, and stops escalating as soon as the tier failure fraction
exceeds --failure-threshold. It never touches the live app's database or
search endpoints.

Usage:
    python -m data_preparation benchmark-rate
"""

import argparse
import json
import random
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import requests

from backend.dpp import ROUTE_URL, route_request_kwargs
from backend.utils import get_next_meetup_time

CHALLENGE_MARKERS = (
    "captcha",
    "cf-chl",
    "verify you are human",
    "are you a human",
    "are you human",
    "access denied",
    "unusual traffic",
    "checking your browser",
    "attention required",
    "just a moment",
    "enable javascript",
)


def classify(status: int | None, text: str, error: Exception | None) -> str:
    if error is not None:
        return "transport_error"
    if status == 200:
        low = (text or "").lower()
        if "nepodařilo se vyhledat vhodné spojení" in low:
            return "no_connection"
        if any(marker in low for marker in CHALLENGE_MARKERS):
            return "challenge"
        if "box-ticket" in low:
            return "ok"
        return "odd_200"
    if status in (401, 403, 429):
        return f"http_{status}"
    if status and status >= 500:
        return "http_5xx"
    return f"http_{status}"


FAILURE_KINDS = {"transport_error", "challenge", "http_401", "http_403", "http_429", "http_5xx"}


def run_tier(
    pairs: list[tuple[str, str]],
    concurrency: int,
    duration_s: float,
    date_str: str,
    time_str: str,
    timeout_s: int,
) -> list[dict]:
    deadline = time.monotonic() + duration_s
    stop = threading.Event()

    def next_pair(idx):
        return pairs[idx % len(pairs)]

    def worker(idx):
        seq = idx
        while not stop.is_set():
            if time.monotonic() >= deadline:
                break
            a, b = next_pair(seq)
            seq += 1
            t0 = time.monotonic()
            status = None
            text = ""
            error = None
            try:
                resp = requests.get(
                    ROUTE_URL, timeout=timeout_s, **route_request_kwargs(a, b, date_str, time_str)
                )
                status = resp.status_code
                text = resp.text
            except Exception as e:  # noqa: BLE001
                error = e
            elapsed = time.monotonic() - t0
            yield {
                "from": a,
                "to": b,
                "status": status,
                "kind": classify(status, text, error),
                "elapsed_s": elapsed,
            }

    results = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = list(pool.map(worker, range(concurrency)))
    for f in futures:
        results.extend(f)
    return results


def summarize(results: list[dict]) -> dict:
    counts = Counter(r["kind"] for r in results)
    failures = [r for r in results if r["kind"] in FAILURE_KINDS]
    latencies = sorted(r["elapsed_s"] for r in results)
    if latencies:
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))]
    else:
        p50 = p95 = 0.0
    return {
        "total_requests": len(results),
        "kinds": dict(counts),
        "failures": len(failures),
        "failure_fraction": len(failures) / len(results) if results else 0.0,
        "p50_latency_s": round(p50, 3),
        "p95_latency_s": round(p95, 3),
        "max_latency_s": round(max(latencies, default=0.0), 3),
    }


def run(
    stops_file: str,
    out_file: str,
    max_concurrency: int,
    tier_seconds: float,
    grace_seconds: float,
    failure_threshold: float,
    timeout_s: int,
    seed: int,
):
    with open(stops_file, "r", encoding="utf-8") as f:
        stops = [line.strip() for line in f if line.strip()]
    random.seed(seed)
    pairs = [(a, b) for a, b in zip(stops, stops[1:] + stops[:1]) if a != b]
    random.shuffle(pairs)
    pairs = pairs[: max_concurrency * 20]

    dt = get_next_meetup_time(4, 18)
    date_str = f"{dt.day}.{dt.month}.{dt.year}"
    time_str = dt.strftime("%H:%M")
    print(f"Benchmark date/time: {date_str} {time_str}")
    print(f"Stop pairs available: {len(pairs)}")

    tiers = []
    for concurrency in range(1, max_concurrency + 1):
        print(f"\n=== Tier: {concurrency} concurrent workers ({tier_seconds:g}s window) ===")
        results = run_tier(pairs, concurrency, tier_seconds, date_str, time_str, timeout_s)
        stats = summarize(results)
        tiers.append({"concurrency": concurrency, **stats})
        print(f"  requests: {stats['total_requests']}")
        print(f"  breakdown: {stats['kinds']}")
        print(f"  failures: {stats['failures']} ({stats['failure_fraction']:.1%})")
        print(
            f"  latency: p50={stats['p50_latency_s']}s p95={stats['p95_latency_s']}s "
            f"max={stats['max_latency_s']}s"
        )
        if stats["total_requests"]:
            print(f"  achieved rate: ~{stats['total_requests'] / tier_seconds:.2f} req/s")
        if stats["failure_fraction"] >= failure_threshold:
            print(f"  failure fraction >= {failure_threshold:.1%}; stopping escalation")
            break
        if concurrency < max_concurrency and grace_seconds > 0:
            print(f"  cooling down {grace_seconds:g}s before next tier...")
            time.sleep(grace_seconds)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(
            {"date_str": date_str, "time_str": time_str, "tiers": tiers},
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"\nReport written to {out_file}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark DPP rate limits")
    parser.add_argument("--stops-file", default="data/Prague_stops.txt")
    parser.add_argument("--out", default="benchmark_report.json")
    parser.add_argument("--max-concurrency", type=int, default=6)
    parser.add_argument("--tier-seconds", type=float, default=20.0)
    parser.add_argument("--grace-seconds", type=float, default=4.0)
    parser.add_argument("--failure-threshold", type=float, default=0.2)
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    run(
        stops_file=args.stops_file,
        out_file=args.out,
        max_concurrency=args.max_concurrency,
        tier_seconds=args.tier_seconds,
        grace_seconds=args.grace_seconds,
        failure_threshold=args.failure_threshold,
        timeout_s=args.timeout,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
