import asyncio
import threading

import pytest

from backend.search_registry import SearchRegistry


class FakeClock:
    def __init__(self, value: float):
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.mark.asyncio
async def test_registry_tracks_and_releases_completed_task():
    registry = SearchRegistry()
    registry.create("search-1", "session-1")
    release = asyncio.Event()

    async def work():
        await release.wait()

    task = registry.start("search-1", work())
    assert registry.task_count == 1

    release.set()
    await task
    await asyncio.sleep(0)

    assert registry.task_count == 0


def test_registry_rejects_progress_access_from_another_session():
    registry = SearchRegistry()
    registry.create("search-1", "session-1")

    assert registry.get("search-1", "session-2") is None
    assert registry.pop("search-1", "session-2") is None
    assert registry.get("search-1", "session-1") is not None


def test_registry_prunes_only_expired_completed_results():
    clock = FakeClock(100.0)
    registry = SearchRegistry(result_ttl_seconds=30, clock=clock)
    registry.create("completed", "session-1")
    registry.update("completed", done=True, result_html="done")
    registry.create("active", "session-1")

    clock.advance(31)

    assert registry.prune() == 1
    assert registry.get("completed", "session-1") is None
    assert registry.get("active", "session-1") is not None


@pytest.mark.asyncio
async def test_registry_shutdown_cancels_outstanding_tasks():
    registry = SearchRegistry()
    registry.create("search-1", "session-1")
    task = registry.start("search-1", asyncio.Event().wait())

    await registry.shutdown()

    assert task.cancelled()
    assert registry.task_count == 0
    assert registry.get("search-1", "session-1") is None


@pytest.mark.asyncio
async def test_registry_shutdown_waits_for_owned_blocking_worker():
    registry = SearchRegistry()
    registry.create("search-1", "session-1")
    started = threading.Event()
    release = threading.Event()
    exited = threading.Event()

    def blocking_work():
        started.set()
        release.wait(timeout=2)
        exited.set()

    async def work():
        await registry.run_blocking(blocking_work)

    registry.start("search-1", work())
    assert await asyncio.to_thread(started.wait, 1)

    shutdown_task = asyncio.create_task(registry.shutdown())
    await asyncio.sleep(0)

    assert not shutdown_task.done()
    assert not exited.is_set()

    release.set()
    await shutdown_task

    assert exited.is_set()
