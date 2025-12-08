import argparse
import subprocess
import re
import sys
from bandit import EpsilonDecreasingBandit, deploy_bandit


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Manage scraping tasks based on failure rates."
    )

    # Threshold for error rate (e.g., 0.10 for 10%)
    parser.add_argument(
        "--threshold-error-rate",
        type=float,
        default=0.10,
        help="Threshold for error rate (default: 0.10 for 10%)",
    )

    # Default wait time in seconds
    parser.add_argument(
        "--default-wait-time",
        type=int,
        default=5,
        help="Default wait time in seconds before retrying (default: 5)",
    )

    # Extra wait time if last try also had error
    parser.add_argument(
        "--extra-wait-time",
        type=int,
        default=0,
        help="Extra wait time in seconds if the last retry also had errors (default: 0)",
    )

    # Number of tasks on error
    parser.add_argument(
        "--waiting-num-tasks",
        type=int,
        default=10,
        help="Number of tasks to run when retrying due to high error rate (default: 10)",
    )

    # Number of processes
    parser.add_argument(
        "--num-processes",
        type=int,
        default=30,
        help="Number of processes to use (default: 30)",
    )

    return parser.parse_args()


def run_scraping(num_processes, num_tasks):
    """
    Runs the scraping.py script with specified number of processes and tasks.
    Returns the total number of failed results and total tasks.
    """
    cmd = [
        sys.executable,
        "scraping.py",
        "--num-processes",
        str(num_processes),
        "--num-tasks",
        str(num_tasks),
    ]
    try:
        # Use Popen instead of run to capture live output
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        failed = 0
        while True:
            # Poll process for new output until finished
            output = process.stdout.readline()
            error_output = process.stderr.readline()

            if output:
                print("[STDOUT]:", output.strip())
                # Check if the output contains "Total failed results: X"
                match = re.search(r"Total failed results:\s+(\d+)", output)
                if match:
                    failed = int(match.group(1))

            if error_output:
                print("[STDERR]:", error_output.strip())

            # Exit when the script is done
            if output == "" and error_output == "" and process.poll() is not None:
                break

        if failed is not None:
            print(
                f"[INFO] Completed scraping with {failed} failed tasks out of {num_tasks}."
            )
            return num_tasks - failed, failed
        else:
            print("[WARNING] Could not find 'Total failed results' in the output.")
            return 0, num_tasks

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Scraping script failed with error:\n{e.stderr}")
        return 0, num_tasks


def main():
    args = parse_arguments()

    threshold_error_rate = args.threshold_error_rate
    default_wait = args.default_wait_time
    extra_wait = args.extra_wait_time
    waiting_num_tasks = args.waiting_num_tasks
    num_processes = args.num_processes

    # initial_num_tasks = 50
    # failed_cnt = 0
    # while failed_cnt < 10:
    #     print(f"[INFO] Running initial scraping with {initial_num_tasks} tasks.")
    #     _, failed_cnt = run_scraping(num_processes, initial_num_tasks)

    # num_tasks_options = list(range(1500, 5000, 500))
    # num_tasks_options = list(range(350, 450, 10))
    num_tasks_options = [5000, 10000, 25000, 50000]
    bandit = EpsilonDecreasingBandit(
        arms=num_tasks_options,
        initial_epsilon=1.0,
        limit_epsilon=0.05,
        half_decay_steps=300,
    )
    # bandit = EpsilonGreedyBandit(arms=num_tasks_options, epsilon=0.1)

    deploy_bandit(
        bandit,
        lambda num_tasks: run_scraping(num_processes, num_tasks),
        failure_threshold=threshold_error_rate,
        default_wait_time=default_wait,
        extra_wait_time=extra_wait,
        waiting_args=waiting_num_tasks,
        max_steps=2000,
        reward_factor=1.0,
        verbose=True,
    )


if __name__ == "__main__":
    main()
