from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, replace
from threading import Lock
from typing import Any


@dataclass
class SearchProgress:
    session_code: str
    stage: str
    current: int
    total: int
    done: bool
    result_html: str | None
    updated_at: float


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

    @property
    def task_count(self) -> int:
        return len(self._tasks)

    def create(self, search_id: str, session_code: str) -> SearchProgress:
        progress = SearchProgress(
            session_code=session_code,
            stage="starting",
            current=0,
            total=0,
            done=False,
            result_html=None,
            updated_at=self._clock(),
        )
        with self._progress_lock:
            self._progress[search_id] = progress
        return replace(progress)

    def update(self, search_id: str, **changes: Any) -> bool:
        with self._progress_lock:
            progress = self._progress.get(search_id)
            if progress is None:
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
            return self._progress.pop(search_id)

    def prune(self) -> int:
        now = self._clock()
        with self._progress_lock:
            expired = [
                search_id
                for search_id, progress in self._progress.items()
                if progress.done and now - progress.updated_at > self._result_ttl_seconds
            ]
            for search_id in expired:
                self._progress.pop(search_id, None)
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

    async def wait_all(self) -> None:
        tasks = list(self._tasks.values())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        tasks = list(self._tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        with self._progress_lock:
            self._progress.clear()
