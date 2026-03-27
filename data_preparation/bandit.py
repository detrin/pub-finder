import math
import random
import time

from tqdm import tqdm


class EpsilonGreedyBandit:
    def __init__(self, arms, epsilon=0.1):
        self.arms = arms
        self.epsilon = epsilon
        self.q_values = [0.0] * len(arms)
        self.counts = [0] * len(arms)

    def select_arm(self):
        if random.random() < self.epsilon:
            return random.randint(0, len(self.arms) - 1)
        max_q = max(self.q_values)
        candidates = [i for i, q in enumerate(self.q_values) if q == max_q]
        return random.choice(candidates)

    def update(self, arm_index, reward, **kwargs):
        self.counts[arm_index] += 1
        n = self.counts[arm_index]
        old_q = self.q_values[arm_index]
        self.q_values[arm_index] = ((n - 1) * old_q + reward) / n

    def __repr__(self):
        return f"EpsilonGreedyBandit(arms={self.arms}, epsilon={self.epsilon})"

    def report(self):
        print("Q-values per arm:")
        for arm, q, cnt in zip(self.arms, self.q_values, self.counts):
            print(f"  num_tasks={arm}: avg_reward={q:.5f}, count={cnt}")


class EpsilonFirstBandit:
    def __init__(self, arms, exploration_steps=100, epsilon=0.1):
        self.arms = arms
        self.exploration_steps = exploration_steps
        self.q_values = [0.0] * len(arms)
        self.counts = [0] * len(arms)
        self.epsilon = epsilon
        self.step = 0

    def select_arm(self):
        if self.step < self.exploration_steps or random.random() < self.epsilon:
            return random.randint(0, len(self.arms) - 1)
        max_q = max(self.q_values)
        candidates = [i for i, q in enumerate(self.q_values) if q == max_q]

        self.step += 1
        return random.choice(candidates)

    def update(self, arm_index, reward, **kwargs):
        self.counts[arm_index] += 1
        n = self.counts[arm_index]
        old_q = self.q_values[arm_index]
        self.q_values[arm_index] = ((n - 1) * old_q + reward) / n

    def __repr__(self):
        return f"EpsilonFirstBandit(arms={self.arms}, exploration_steps={self.exploration_steps})"

    def report(self):
        print("Q-values per arm:")
        for arm, q, cnt in zip(self.arms, self.q_values, self.counts):
            print(f"  num_tasks={arm}: avg_reward={q:.5f}, count={cnt}")


class EpsilonDecreasingBandit:
    def __init__(
        self, arms, initial_epsilon=1.0, limit_epsilon=0.1, half_decay_steps=100
    ):
        self.arms = arms
        self.initial_epsilon = initial_epsilon
        self.epsilon = initial_epsilon
        self.limit_epsilon = limit_epsilon
        self.half_decay_steps = half_decay_steps
        self.q_values = [0.0] * len(arms)
        self.counts = [0] * len(arms)
        self.step = 0

    def select_arm(self):
        self.step += 1
        self.update_epsilon()

        if random.random() < self.epsilon:
            return random.randint(0, len(self.arms) - 1)

        max_q = max(self.q_values)
        candidates = [i for i, q in enumerate(self.q_values) if q == max_q]
        return random.choice(candidates)

    def update(self, arm_index, reward, **kwargs):
        self.counts[arm_index] += 1
        n = self.counts[arm_index]
        old_q = self.q_values[arm_index]
        self.q_values[arm_index] = ((n - 1) * old_q + reward) / n

    def update_epsilon(self):
        self.epsilon = self.limit_epsilon + (
            self.initial_epsilon - self.limit_epsilon
        ) * (0.5 ** (self.step / self.half_decay_steps))

    def __repr__(self):
        return f"EpsilonDecreasingBandit(arms={self.arms}, initial_epsilon={self.initial_epsilon}, limit_epsilon={self.limit_epsilon}, half_decay_steps={self.half_decay_steps})"

    def report(self):
        print("Q-values per arm:")
        for arm, q, cnt in zip(self.arms, self.q_values, self.counts):
            print(f"  num_tasks={arm}: avg_reward={q:.5f}, count={cnt}")


class UCB1Bandit:
    def __init__(self, arms):
        self.arms = arms
        self.total_count = 0
        self.q_values = [0.0] * len(arms)
        self.counts = [0] * len(arms)

    def select_arm(self):
        for arm_index in range(len(self.arms)):
            if self.counts[arm_index] == 0:
                return arm_index

        ucb_values = [
            self.q_values[i]
            + math.sqrt((2 * math.log(self.total_count)) / self.counts[i])
            for i in range(len(self.arms))
        ]
        return ucb_values.index(max(ucb_values))

    def update(self, arm_index, reward, **kwargs):
        if not (0 <= reward <= 1):
            raise ValueError("Reward must be in the range [0, 1].")
        self.counts[arm_index] += 1
        self.total_count += 1
        n = self.counts[arm_index]
        old_q = self.q_values[arm_index]
        self.q_values[arm_index] = ((n - 1) * old_q + reward) / n

    def __repr__(self):
        return f"UCB1Bandit(arms={self.arms})"

    def report(self):
        print("Q-values per arm:")
        for arm, q, cnt in zip(self.arms, self.q_values, self.counts):
            print(f"  num_tasks={arm}: avg_reward={q:.5f}, count={cnt}")


class GreedyBanditWithHistory:
    def __init__(self, arms, history_length=100):
        self.arms = arms
        self.history_length = history_length
        self.q_values = [0.0] * len(arms)
        self.counts = [0] * len(arms)
        self.history = [[] for _ in range(len(arms))]

    def select_arm(self):
        if any(len(history) < self.history_length for history in self.history):
            candidates = [
                i
                for i, history in enumerate(self.history)
                if len(history) < self.history_length
            ]
            return random.choice(candidates)

        max_q = max(self.q_values)
        candidates = [i for i, q in enumerate(self.q_values) if q == max_q]
        return random.choice(candidates)

    def update(self, arm_index, reward, **kwargs):
        if len(self.history[arm_index]) >= self.history_length:
            self.history[arm_index].pop(0)
        self.history[arm_index].append(reward)

        self.counts[arm_index] = len(self.history[arm_index])
        self.q_values[arm_index] = sum(self.history[arm_index]) / self.counts[arm_index]

    def __repr__(self):
        return f"GreedyBanditWithHistory(arms={self.arms}, history_length={self.history_length})"

    def report(self):
        print("Q-values per arm:")
        for arm, q, cnt, history in zip(
            self.arms, self.q_values, self.counts, self.history
        ):
            print(
                f"  num_tasks={arm}: avg_reward={q:.5f}, count={cnt}, history={history}"
            )


class WilsonSamplingBandit:
    def __init__(
        self, arms, z_score=1.96
    ):  # 1.96 corresponds to ~95% confidence interval
        self.arms = arms
        self.q_values = [0] * len(arms)
        self.counts = [0] * len(arms)
        self.z_score = z_score

    def select_arm(self):
        if sum(self.counts) == 0:
            return random.choice(range(len(self.arms)))

        scores = [
            self._wilson_score(i, self.q_values[i]) for i in range(len(self.arms))
        ]
        return scores.index(max(scores))

    def update(self, arm_index, reward, **kwargs):
        if not (0 <= reward <= 1):
            raise ValueError("Reward must be in the range [0, 1].")
        self.counts[arm_index] += 1
        n = self.counts[arm_index]
        old_q = self.q_values[arm_index]
        self.q_values[arm_index] = ((n - 1) * old_q + reward) / n

    def _wilson_score(self, arm_index, q_val):
        n = self.counts[arm_index]
        if n == 0:
            return 1.0
        proportion = q_val
        factor = self.z_score**2 / (2 * n)
        adj_factor = (
            self.z_score
            / 2
            * math.sqrt((proportion * (1 - proportion) + self.z_score**2 / (4 * n)) / n)
        )
        return (proportion + factor - adj_factor) / (1 + self.z_score**2 / n)

    def __repr__(self):
        return f"WilsonSamplingBandit(arms={self.arms}, z_score={self.z_score})"

    def report(self):
        print("Success and Failure counts per arm:")
        for arm, q, cnt in zip(self.arms, self.q_values, self.counts):
            print(f"  num_tasks={arm}: avg_reward={q:.5f}, count={cnt}")


class ThompsonSamplingBandit:
    def __init__(self, arms):
        self.arms = arms
        self.alpha = [1] * len(arms)  # Number of successes + 1 (beta prior parameter)
        self.beta = [1] * len(arms)  # Number of failures + 1 (beta prior parameter)

    def select_arm(self):
        sampled_means = [
            random.betavariate(self.alpha[i], self.beta[i])
            for i in range(len(self.arms))
        ]
        return sampled_means.index(max(sampled_means))

    def update(self, arm_index, reward, success=1, failure=0, **kwargs):
        self.alpha[arm_index] += success
        self.beta[arm_index] += failure

    def __repr__(self):
        return f"ThompsonSamplingBandit(arms={self.arms})"

    def report(self):
        print("Alpha and Beta values per arm (Beta distribution parameters):")
        for arm, a, b in zip(self.arms, self.alpha, self.beta):
            expected_reward = a / (a + b)
            print(
                f"  num_tasks={arm}: alpha={a}, beta={b}, expected_reward={expected_reward:.5f}"
            )


def simulate_fail_fraction(num_tasks):
    p = 0.07 + num_tasks / 300
    return p + random.uniform(-0.05, 0.05)


def testing_simulation_function(num_tasks):
    successful_tasks = num_tasks * (1 - simulate_fail_fraction(num_tasks))
    failed_tasks = num_tasks - successful_tasks
    return successful_tasks, failed_tasks


def deploy_bandit(
    bandit,
    fun,
    failure_threshold=0.1,
    default_wait_time=5,
    extra_wait_time=10,
    waiting_args=None,
    max_steps=500,
    verbose=False,
    reward_factor=1e-6,
):
    if waiting_args is None:
        raise ValueError("waiting_args must be provided")
    if not isinstance(waiting_args, (tuple, list)):
        waiting_args = (waiting_args,)

    state = "ALIVE"
    last_alive_successes = 0.0
    last_arm_index = None
    waiting_time = 0.0

    iterator = range(max_steps)
    if verbose:
        iterator = tqdm(range(max_steps))

    for _ in iterator:
        if verbose:
            bandit.report()

        if state == "ALIVE":
            current_arm_index = bandit.select_arm()

            fun_args = bandit.arms[current_arm_index]
            if not isinstance(fun_args, (tuple, list)):
                fun_args = (fun_args,)
            successful_tasks, failed_tasks = fun(*fun_args)
            fail_fraction = failed_tasks / (successful_tasks + failed_tasks)

            time.sleep(default_wait_time)
            waiting_time += default_wait_time

            if fail_fraction >= failure_threshold:
                last_alive_successes = successful_tasks
                last_arm_index = current_arm_index
                state = "WAITING"
                waiting_steps = 0
            else:
                reward = successful_tasks / waiting_time * reward_factor
                bandit.update(current_arm_index, reward)
                waiting_time = 0.0

        else:
            successful_tasks, failed_tasks = fun(*waiting_args)
            fail_fraction = failed_tasks / (successful_tasks + failed_tasks)

            time.sleep(default_wait_time + extra_wait_time)
            waiting_time += default_wait_time + extra_wait_time

            if fail_fraction < failure_threshold:
                reward = last_alive_successes / waiting_time * reward_factor
                bandit.update(last_arm_index, reward)
                waiting_time = 0.0
                state = "ALIVE"

    if verbose:
        bandit.report()


