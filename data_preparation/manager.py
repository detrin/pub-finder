import argparse
import re
import subprocess
import sys

from .bandit import EpsilonDecreasingBandit, deploy_bandit


def run_scraping(num_processes, num_tasks):
    """
    Runs the scraping module as a subprocess.
    Returns (successful_tasks, failed_tasks).
    """
    cmd = [
        sys.executable,
        "-m", "data_preparation.scraping",
        "--num-processes", str(num_processes),
        "--num-tasks", str(num_tasks),
    ]
    try:
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        failed = 0
        while True:
            output = process.stdout.readline()
            error_output = process.stderr.readline()

            if output:
                print("[STDOUT]:", output.strip())
                match = re.search(r"Total failed results:\s+(\d+)", output)
                if match:
                    failed = int(match.group(1))

            if error_output:
                print("[STDERR]:", error_output.strip())

            if output == "" and error_output == "" and process.poll() is not None:
                break

        print(f"[INFO] Completed scraping with {failed} failed tasks out of {num_tasks}.")
        return num_tasks - failed, failed

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Scraping script failed with error:\n{e.stderr}")
        return 0, num_tasks


def run(threshold_error_rate=0.10, default_wait_time=5, extra_wait_time=0,
        waiting_num_tasks=10, num_processes=30):
    """Core manager logic — callable from CLI."""
    num_tasks_options = [5000, 10000, 25000, 50000]
    bandit = EpsilonDecreasingBandit(
        arms=num_tasks_options,
        initial_epsilon=1.0,
        limit_epsilon=0.05,
        half_decay_steps=300,
    )
    deploy_bandit(
        bandit,
        lambda num_tasks: run_scraping(num_processes, num_tasks),
        failure_threshold=threshold_error_rate,
        default_wait_time=default_wait_time,
        extra_wait_time=extra_wait_time,
        waiting_args=waiting_num_tasks,
        max_steps=2000,
        reward_factor=1.0,
        verbose=True,
    )


def main():
    parser = argparse.ArgumentParser(description="Adaptive scraping with bandit scheduling")
    parser.add_argument("--threshold-error-rate", type=float, default=0.10)
    parser.add_argument("--default-wait-time", type=int, default=5)
    parser.add_argument("--extra-wait-time", type=int, default=0)
    parser.add_argument("--waiting-num-tasks", type=int, default=10)
    parser.add_argument("--num-processes", type=int, default=30)
    args = parser.parse_args()
    run(
        threshold_error_rate=args.threshold_error_rate,
        default_wait_time=args.default_wait_time,
        extra_wait_time=args.extra_wait_time,
        waiting_num_tasks=args.waiting_num_tasks,
        num_processes=args.num_processes,
    )


if __name__ == "__main__":
    main()
