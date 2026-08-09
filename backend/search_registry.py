from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from functools import partial
from threading import Lock
from typing import Any, TypeVar

T = TypeVar("T")


@dataclass
class SearchProgress:
    session_code: str
    stage: str
    current: int
    total: int
    place_type_labels: tuple[str, ...]
    done: bool
    cancelled: bool
    result_html: str | None
    updated_at: float
    locale: str = "en"


class SearchRegistry:
    """Own in-process search progress and task lifecycle for one app instance."""

    def __init__(
        self,
        result_ttl_seconds: float = 900.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._result_ttl_seconds = result_ttl_seconds
        self._clock = clock
        self._progress: dict[str, SearchProgress] = {}
        self._progress_lock = Lock()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._active_searches: dict[str, str] = {}
        self._worker_pool = ThreadPoolExecutor(thread_name_prefix="pub-finder-search")
        self._worker_futures: set[Future[Any]] = set()
        self._worker_lock = Lock()
        self._accepting_work = True

    @property
    def task_count(self) -> int:
        return len(self._tasks)

    def create(
        self,
        search_id: str,
        session_code: str,
        place_type_labels: tuple[str, ...] = (),
        locale: str = "en",
    ) -> SearchProgress:
        progress = SearchProgress(
            session_code=session_code,
            stage="starting",
            current=0,
            total=0,
            place_type_labels=place_type_labels,
            done=False,
            cancelled=False,
            result_html=None,
            updated_at=self._clock(),
            locale=locale,
        )
        with self._progress_lock:
            previous_id = self._active_searches.get(session_code)
            if previous_id and previous_id != search_id:
                previous = self._progress.get(previous_id)
                if previous is not None:
                    self._progress[previous_id] = replace(
                        previous,
                        done=True,
                        cancelled=True,
                        result_html=None,
                        updated_at=self._clock(),
                    )
                previous_task = self._tasks.get(previous_id)
                if previous_task is not None and not previous_task.done():
                    previous_task.cancel()
            self._progress[search_id] = progress
            self._active_searches[session_code] = search_id
        return replace(progress)

    def is_current(self, search_id: str, session_code: str) -> bool:
        with self._progress_lock:
            return (
                self._active_searches.get(session_code) == search_id and search_id in self._progress
            )

    def update(self, search_id: str, **changes: Any) -> bool:
        with self._progress_lock:
            progress = self._progress.get(search_id)
            if progress is None or progress.cancelled:
                return False
            self._progress[search_id] = replace(
                progress,
                updated_at=self._clock(),
                **changes,
            )
        return True

    def get(self, search_id: str, session_code: str) -> SearchProgress | None:
        with self._progress_lock:
            progress = self._progress.get(search_id)
            if progress is None or progress.session_code != session_code:
                return None
            return replace(progress)

    def pop(self, search_id: str, session_code: str) -> SearchProgress | None:
        with self._progress_lock:
            progress = self._progress.get(search_id)
            if progress is None or progress.session_code != session_code:
                return None
            popped = self._progress.pop(search_id)
            if self._active_searches.get(session_code) == search_id:
                self._active_searches.pop(session_code, None)
            return popped

    def prune(self) -> int:
        now = self._clock()
        with self._progress_lock:
            expired = [
                search_id
                for search_id, progress in self._progress.items()
                if progress.done and now - progress.updated_at > self._result_ttl_seconds
            ]
            for search_id in expired:
                progress = self._progress.pop(search_id, None)
                if (
                    progress is not None
                    and self._active_searches.get(progress.session_code) == search_id
                ):
                    self._active_searches.pop(progress.session_code, None)
        return len(expired)

    def start(
        self,
        search_id: str,
        coroutine: Coroutine[Any, Any, None],
    ) -> asyncio.Task[None]:
        with self._progress_lock:
            if search_id not in self._progress:
                raise KeyError(f"Unknown search ID: {search_id}")
        if search_id in self._tasks:
            raise RuntimeError(f"Search already running: {search_id}")

        task = asyncio.create_task(coroutine)
        self._tasks[search_id] = task

        def release(completed: asyncio.Task[None]) -> None:
            if self._tasks.get(search_id) is completed:
                self._tasks.pop(search_id, None)

        task.add_done_callback(release)
        return task

    async def run_blocking(
        self,
        function: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Run blocking search work in the registry-owned executor."""
        with self._worker_lock:
            if not self._accepting_work:
                raise RuntimeError("Search registry is shutting down")
            worker = self._worker_pool.submit(partial(function, *args, **kwargs))
            self._worker_futures.add(worker)

        def release(completed: Future[Any]) -> None:
            with self._worker_lock:
                self._worker_futures.discard(completed)

        worker.add_done_callback(release)
        return await asyncio.wrap_future(worker)

    async def wait_all(self) -> None:
        tasks = list(self._tasks.values())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        with self._worker_lock:
            self._accepting_work = False

        tasks = list(self._tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()

        with self._worker_lock:
            workers = list(self._worker_futures)
        self._worker_pool.shutdown(wait=False, cancel_futures=True)
        if workers:
            await asyncio.gather(
                *(asyncio.wrap_future(worker) for worker in workers),
                return_exceptions=True,
            )
        self._worker_pool.shutdown(wait=True, cancel_futures=True)

        with self._progress_lock:
            self._progress.clear()
            self._active_searches.clear()
