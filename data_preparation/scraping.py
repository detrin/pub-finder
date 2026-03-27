import argparse
import json
import os
import random
from itertools import product
from multiprocessing import Pool

from tqdm import tqdm

from backend.utils import get_total_minutes_with_retries, get_next_meetup_time


def process_pair(args):
    from_stop, to_stop, meetup_dt = args
    if from_stop == to_stop:
        return None

    total_minutes = get_total_minutes_with_retries(
        from_stop, to_stop, meetup_dt, max_retries=1
    )

    if total_minutes is not None:
        return {"from": from_stop, "to": to_stop, "total_minutes": total_minutes}
    else:
        return {"from": from_stop, "to": to_stop, "error": "Failed to retrieve data."}


def run(stops_file="data/Prague_stops.txt", results_file="results.json",
        num_processes=5, num_tasks=None):
    """Core scraping logic — callable from CLI or from manager."""
    raw_results = []
    if os.path.exists(results_file):
        with open(results_file, "r", encoding="utf-8") as f:
            raw_results = json.load(f)

    with open(stops_file, "r", encoding="utf-8") as f:
        stops = [line.strip() for line in f if line.strip()]

    meetup_dt = get_next_meetup_time(4, 18)
    print(f"Next meetup: {meetup_dt}")

    unique_pairs = [(a, b) for a, b in product(stops, stops) if a != b]
    print(f"Total unique pairs to process: {len(unique_pairs)}")

    processed_pairs = set()
    correct_entries = []
    error_entries_to_process = []
    for entry in raw_results:
        key = (entry["from"], entry["to"])
        processed_pairs.add(key)
        if "error" not in entry:
            correct_entries.append(entry)
        else:
            error_entries_to_process.append((entry["from"], entry["to"], meetup_dt))

    missing_entries_to_process = [
        (a, b, meetup_dt)
        for a, b in tqdm(unique_pairs, desc="Checking")
        if (a, b) not in processed_pairs
    ]

    print(f"Total correct entries: {len(correct_entries)}")
    print(f"Total entries with errors to retry: {len(error_entries_to_process)}")
    print(f"Total missing entries to process: {len(missing_entries_to_process)}")

    args_to_process = error_entries_to_process + missing_entries_to_process
    random.shuffle(args_to_process)

    if num_tasks is not None:
        args_to_process = args_to_process[:num_tasks]
        print(f"Limiting to the first {num_tasks} tasks as specified by --num-tasks.")

    if not args_to_process:
        print("No entries to process.")
        return

    new_results = []
    with Pool(processes=num_processes) as pool:
        for result in tqdm(
            pool.imap_unordered(process_pair, args_to_process),
            total=len(args_to_process),
            desc="Processing",
        ):
            if result is not None:
                new_results.append(result)

    combined_results = correct_entries + new_results
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(combined_results, f, ensure_ascii=False, indent=4)

    failed_results = [entry for entry in new_results if "error" in entry]
    print(f"Total failed results: {len(failed_results)}")


def main():
    parser = argparse.ArgumentParser(description="Scrape transit times for stop pairs")
    parser.add_argument("--stops_file", default="Prague_stops.txt", help="Path to stops file")
    parser.add_argument("--results", default="results.json", help="Path to results JSON")
    parser.add_argument("--num-processes", type=int, default=5, help="Number of parallel processes")
    parser.add_argument("--num-tasks", type=int, default=None, help="Limit number of tasks")
    args = parser.parse_args()
    run(stops_file=args.stops_file, results_file=args.results,
        num_processes=args.num_processes, num_tasks=args.num_tasks)


if __name__ == "__main__":
    main()
