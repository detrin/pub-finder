import asyncio
import threading

import pytest

from backend.search_registry import SearchRegistry
from routers.search import search_progress_stream


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


@pytest.mark.asyncio
async def test_newer_search_supersedes_and_cancels_the_older_session_search():
    registry = SearchRegistry()
    registry.create("old", "session-1")
    old_task = registry.start("old", asyncio.Event().wait())
    registry.create("new", "session-1")

    await asyncio.sleep(0)

    old = registry.get("old", "session-1")
    assert old is not None and old.done and old.cancelled
    assert old_task.cancelled()
    assert registry.is_current("new", "session-1")
    assert not registry.is_current("old", "session-1")


@pytest.mark.asyncio
async def test_cancelled_search_stream_is_silent_while_latest_search_completes():
    registry = SearchRegistry()
    registry.create("old", "session1")
    registry.create("new", "session1")
    registry.update("new", done=True, result_html="<p>new result</p>")
    request = type(
        "Request",
        (),
        {
            "app": type("App", (), {"state": type("State", (), {"search_registry": registry})()})(),
            "is_disconnected": lambda self: asyncio.sleep(0, result=False),
        },
    )()

    old_response = await search_progress_stream(request, "session1", "old")
    old_events = [event async for event in old_response.body_iterator]
    new_response = await search_progress_stream(request, "session1", "new")
    new_events = [event async for event in new_response.body_iterator]

    assert old_events == []
    assert registry.get("old", "session1") is None
    assert len(new_events) == 1
    assert "<p>new result</p>" in new_events[0]
    assert "event: complete" in new_events[0]
    assert not registry.is_current("new", "session1")


def test_registry_rejects_progress_access_from_another_session():
    registry = SearchRegistry()
    registry.create("search-1", "session-1")

    assert registry.get("search-1", "session-2") is None
    assert registry.pop("search-1", "session-2") is None
    assert registry.get("search-1", "session-1") is not None


def test_registry_keeps_selected_place_type_labels():
    """SSE rendering can use the stable venue types chosen when a search started."""
    registry = SearchRegistry()
    registry.create("search-1", "session-1", place_type_labels=("Coffee", "Food"))

    progress = registry.get("search-1", "session-1")
    assert progress is not None
    assert progress.place_type_labels == ("Coffee", "Food")


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


def test_pop_and_prune_clear_only_the_matching_active_search_mapping():
    clock = FakeClock(100.0)
    registry = SearchRegistry(result_ttl_seconds=30, clock=clock)
    registry.create("old", "session-1")
    registry.create("current", "session-1")

    registry.pop("old", "session-1")
    assert registry.is_current("current", "session-1")

    registry.update("current", done=True, result_html="done")
    clock.advance(31)
    assert registry.prune() == 1
    assert not registry.is_current("current", "session-1")

    registry.create("popped", "session-2")
    registry.pop("popped", "session-2")
    assert not registry.is_current("popped", "session-2")


@pytest.mark.asyncio
async def test_registry_shutdown_cancels_outstanding_tasks():
    registry = SearchRegistry()
    registry.create("search-1", "session-1")
    task = registry.start("search-1", asyncio.Event().wait())

    await registry.shutdown()

    assert task.cancelled()
    assert registry.task_count == 0
    assert registry.get("search-1", "session-1") is None
    assert not registry.is_current("search-1", "session-1")


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
