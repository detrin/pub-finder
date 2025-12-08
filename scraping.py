import os
import datetime
import json
import random
import argparse
from tqdm import tqdm
from multiprocessing import Pool
from itertools import product
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


def main():
    parser = argparse.ArgumentParser(description="Scraping and Correcting Script")

    parser.add_argument(
        "--stops_file",
        type=str,
        default="Prague_stops.txt",
        help="Path to the stops file.",
    )
    parser.add_argument(
        "--results",
        type=str,
        default="results.json",
        help="Path to the final results file.",
    )
    parser.add_argument(
        "--num-processes", type=int, default=5, help="Number of parallel processes."
    )
    # Adding the --num-tasks argument
    parser.add_argument(
        "--num-tasks",
        type=int,
        default=None,
        help="Number of tasks to process. If not set, all tasks will be processed.",
    )

    args = parser.parse_args()
    results_file = args.results
    stops_file = args.stops_file
    num_processes = args.num_processes
    num_tasks = args.num_tasks  # Retrieve the num_tasks value

    raw_results = []
    if os.path.exists(results_file):
        with open(results_file, "r", encoding="utf-8") as f:
            raw_results = json.load(f)

    with open(stops_file, "r", encoding="utf-8") as f:
        stops = [line.strip() for line in f if line.strip()]

    meetup_dt = get_next_meetup_time(4, 18)
    # meetup_dt = datetime.datetime(2025, 2, 28, 20, 0)
    print(f"Next meetup: {meetup_dt}")

    all_pairs = list(product(stops, stops))
    unique_pairs = [pair for pair in all_pairs if pair[0] != pair[1]]
    print(f"Total unique pairs to process: {len(unique_pairs)}")

    processed_pairs = set()
    correct_entries = []
    error_entries = []
    error_entries_to_process = []
    for entry in raw_results:
        key = (entry["from"], entry["to"])
        processed_pairs.add(key)  # Faster than dictionary updates
        if "error" not in entry:
            correct_entries.append(entry)
        else:
            error_entries.append(entry)
            error_entries_to_process.append((entry["from"], entry["to"], meetup_dt))

    # Use set for O(1) lookups instead of dictionary keys
    missing_entries_to_process = [
        (entry[0], entry[1], meetup_dt)
        for entry in tqdm(unique_pairs, desc="Checking")
        if entry not in processed_pairs
    ]

    print(f"Total correct entries: {len(correct_entries)}")
    print(f"Total entries with errors to retry: {len(error_entries_to_process)}")
    print(f"Total missing entries to process: {len(missing_entries_to_process)}")

    # Combine error retries and missing entries
    args_to_process = error_entries_to_process + missing_entries_to_process

    random.shuffle(args_to_process)

    if num_tasks is not None:
        args_to_process = args_to_process[:num_tasks]
        print(f"Limiting to the first {num_tasks} tasks as specified by --num-tasks.")

    if not args_to_process:
        print("No entries to process.")
        return

    combined_results = correct_entries 
    new_results = []
    # with Pool(processes=num_processes) as pool:
    #     for result in tqdm(
    #         pool.imap_unordered(process_pair, args_to_process),
    #         total=len(args_to_process),
    #         desc="Processing",
    #     ):
    #         if result is not None:
    #             new_results.append(result)
                
    with Pool(processes=num_processes) as pool:
        for result in tqdm(
            pool.imap_unordered(process_pair, args_to_process),
            total=len(args_to_process),
            desc="Processing",
        ):
            if result is not None:
                new_results.append(result)

    # Final save after processing all tasks
    combined_results += new_results
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(combined_results, f, ensure_ascii=False, indent=4)

    failed_results = [entry for entry in new_results if "error" in entry]
    print(f"Total failed results: {len(failed_results)}")


if __name__ == "__main__":
    main()
