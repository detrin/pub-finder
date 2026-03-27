"""
Unified CLI for data preparation tasks.

Usage:
    python -m data_preparation scrape     -- Scrape transit times for stop pairs
    python -m data_preparation manage     -- Adaptive scraping with bandit-based scheduling
    python -m data_preparation prepare    -- Prepare geographic stop data from GPS JSON files
    python -m data_preparation bandit-sim -- Run a bandit simulation (for testing algorithms)
"""

import argparse


def cmd_scrape(args):
    from .scraping import run
    run(stops_file=args.stops_file, results_file=args.results,
        num_processes=args.num_processes, num_tasks=args.num_tasks)


def cmd_manage(args):
    from .manager import run
    run(
        threshold_error_rate=args.threshold_error_rate,
        default_wait_time=args.default_wait_time,
        extra_wait_time=args.extra_wait_time,
        waiting_num_tasks=args.waiting_num_tasks,
        num_processes=args.num_processes,
    )


def cmd_prepare(args):
    from .prepare_geo_data import main
    main(json_dir=args.json_dir, stops_file=args.stops_file, output_file=args.output)


def cmd_bandit_sim(args):
    from .bandit import (
        ThompsonSamplingBandit, EpsilonDecreasingBandit, UCB1Bandit,
        deploy_bandit, testing_simulation_function,
    )

    bandit_classes = {
        "thompson": ThompsonSamplingBandit,
        "epsilon-decreasing": EpsilonDecreasingBandit,
        "ucb1": UCB1Bandit,
    }
    cls = bandit_classes[args.algorithm]
    arm_range = list(range(args.arm_min, args.arm_max + 1, args.arm_step))

    if args.algorithm == "epsilon-decreasing":
        bandit = cls(arms=arm_range, initial_epsilon=1.0, limit_epsilon=0.05, half_decay_steps=200)
    else:
        bandit = cls(arms=arm_range)

    deploy_bandit(
        bandit,
        testing_simulation_function,
        failure_threshold=0.1,
        default_wait_time=0.005,
        extra_wait_time=0.01,
        waiting_args=args.arm_min,
        max_steps=args.max_steps,
        verbose=True,
        reward_factor=1.0,
    )


def main():
    parser = argparse.ArgumentParser(
        prog="data_preparation",
        description="Unified CLI for pub-finder data preparation tasks.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- scrape ---
    sp_scrape = subparsers.add_parser("scrape", help="Scrape transit times for stop pairs")
    sp_scrape.add_argument("--stops-file", default="Prague_stops.txt", help="Path to stops file")
    sp_scrape.add_argument("--results", default="results.json", help="Path to results JSON")
    sp_scrape.add_argument("--num-processes", type=int, default=5, help="Number of parallel processes")
    sp_scrape.add_argument("--num-tasks", type=int, default=None, help="Limit number of tasks")
    sp_scrape.set_defaults(func=cmd_scrape)

    # --- manage ---
    sp_manage = subparsers.add_parser("manage", help="Adaptive scraping with bandit-based scheduling")
    sp_manage.add_argument("--threshold-error-rate", type=float, default=0.10, help="Error rate threshold")
    sp_manage.add_argument("--default-wait-time", type=int, default=5, help="Wait time between runs (seconds)")
    sp_manage.add_argument("--extra-wait-time", type=int, default=0, help="Extra wait on consecutive errors")
    sp_manage.add_argument("--waiting-num-tasks", type=int, default=10, help="Tasks during wait/retry")
    sp_manage.add_argument("--num-processes", type=int, default=30, help="Number of processes")
    sp_manage.set_defaults(func=cmd_manage)

    # --- prepare ---
    sp_prepare = subparsers.add_parser("prepare", help="Prepare geographic stop data from GPS JSON files")
    sp_prepare.add_argument("--json-dir", default="data", help="Directory with GPS JSON files")
    sp_prepare.add_argument("--stops-file", default="Prague_stops.txt", help="Path to stops list")
    sp_prepare.add_argument("--output", default="Prague_stops_geo.csv", help="Output CSV path")
    sp_prepare.set_defaults(func=cmd_prepare)

    # --- bandit-sim ---
    sp_sim = subparsers.add_parser("bandit-sim", help="Run a bandit algorithm simulation")
    sp_sim.add_argument("--algorithm", choices=["thompson", "epsilon-decreasing", "ucb1"], default="thompson")
    sp_sim.add_argument("--arm-min", type=int, default=10, help="Minimum arm value")
    sp_sim.add_argument("--arm-max", type=int, default=200, help="Maximum arm value")
    sp_sim.add_argument("--arm-step", type=int, default=10, help="Step between arm values")
    sp_sim.add_argument("--max-steps", type=int, default=1000, help="Maximum simulation steps")
    sp_sim.set_defaults(func=cmd_bandit_sim)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
