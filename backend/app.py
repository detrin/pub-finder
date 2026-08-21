import logging
import re
from contextlib import asynccontextmanager

import aiosqlite
import polars as pl
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from routers.home import router as home_router
from routers.reachability import router as reachability_router
from routers.search import router as search_router
from routers.session import router as session_router
from routers.track import router as track_router

from .analytics import (
    USER_ID_COOKIE,
    USER_ID_COOKIE_MAX_AGE,
    drain_analytics_tasks,
    get_client_ip,
    is_valid_user_id,
    new_user_id,
    page_context,
    page_view_events,
    record_visit,
    schedule_analytics_task,
    send_events,
    tool_used_event,
)
from .config import DATABASE_PATH, HOST, PORT
from .db import cleanup_old_sessions, init_db
from .i18n import DEFAULT_LOCALE, SUPPORTED_LOCALES, reset_current_locale, set_current_locale
from .search_registry import SearchRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    distance_table = pl.read_parquet("data/Prague_stops_combinations.parquet")
    from_stops = distance_table["from"].unique().sort().to_list()
    to_stops = distance_table["to"].unique().sort().to_list()
    all_stops = sorted(list(set(from_stops) & set(to_stops)))

    stop_geo = pl.read_parquet("data/Prague_stops_geo.parquet")

    app.state.distance_table = distance_table
    app.state.all_stops = all_stops
    app.state.stop_geo = stop_geo

    db = await aiosqlite.connect(DATABASE_PATH)
    await init_db(db)
    await cleanup_old_sessions(db)
    app.state.db = db
    search_registry = SearchRegistry()
    app.state.search_registry = search_registry
    app.state.analytics_tasks = set()

    yield

    await search_registry.shutdown()
    await drain_analytics_tasks(app)
    await db.close()


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' https://unpkg.com; "
            "script-src-attr 'none'; "
            "style-src 'self' 'unsafe-inline' https://unpkg.com; "
            "img-src 'self' data: https://*.tile.openstreetmap.org https://maps.google.com; "
            "connect-src 'self'; "
            "font-src 'self'; "
            "frame-src https://docs.google.com; "
            "frame-ancestors 'none'"
        )
        return response


class LocaleMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        locale = request.cookies.get("language", DEFAULT_LOCALE)
        request.state.locale = locale if locale in SUPPORTED_LOCALES else DEFAULT_LOCALE
        token = set_current_locale(request.state.locale)
        try:
            return await call_next(request)
        finally:
            reset_current_locale(token)


_TOOL_ROUTES = (
    (re.compile(r"^/session/[^/]+/search$"), "search"),
    (re.compile(r"^/session/[^/]+/venues$"), "load_venues"),
    (re.compile(r"^/session/create$"), "create_session"),
)

# GET routes that render a full HTML page a person actually looks at. Everything
# else under /session/* is a partial, an SSE stream, or a JSON endpoint fetched
# in the background and must not be counted as a page view.
_PAGE_VIEW_ROUTES = re.compile(
    r"^(?:/|/how-it-works|/feedback|/session/join|/session/[^/]+(?:/results)?)$"
)


class AnalyticsMiddleware(BaseHTTPMiddleware):
    """Server-side GA4 tracking keyed off a single first-party user id cookie."""

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method == "POST" and request.url.path == "/reachability/preview":
            return await call_next(request)
        response = await call_next(request)

        if not request.url.path.startswith("/static") and request.url.path != "/e":
            cookie_user_id = request.cookies.get(USER_ID_COOKIE)
            user_id = cookie_user_id if is_valid_user_id(cookie_user_id) else new_user_id()
            if user_id != cookie_user_id:
                response.set_cookie(
                    USER_ID_COOKIE,
                    user_id,
                    max_age=USER_ID_COOKIE_MAX_AGE,
                    httponly=True,
                    secure=True,
                    samesite="lax",
                )
            schedule_analytics_task(
                request.app,
                self._track(request, user_id, response.status_code),
            )

        return response

    @staticmethod
    async def _track(request: Request, user_id: str, response_status: int) -> None:
        try:
            await AnalyticsMiddleware._track_unsafe(request, user_id, response_status)
        except Exception:
            logger.warning("Analytics tracking failed", exc_info=True)

    @staticmethod
    async def _track_unsafe(request: Request, user_id: str, response_status: int) -> None:
        db = getattr(request.app.state, "db", None)
        if db is None or response_status >= 400:
            return
        tool_name = None
        if request.method == "POST":
            tool_name = next(
                (name for pattern, name in _TOOL_ROUTES if pattern.fullmatch(request.url.path)),
                None,
            )
        is_page_view = (
            request.method == "GET"
            and "hx-request" not in request.headers
            and _PAGE_VIEW_ROUTES.match(request.url.path)
        )
        if not tool_name and not is_page_view:
            return

        visit = await record_visit(db, user_id)

        if tool_name:
            events = tool_used_event(
                tool_name=tool_name,
                session_id=visit.session_id,
                session_number=visit.session_number,
            )
        else:
            context = page_context(request)
            events = page_view_events(
                page_location=context.page_location,
                page_title=request.url.path,
                page_referrer=context.page_referrer,
                campaign=context.campaign,
                session_id=visit.session_id,
                session_number=visit.session_number,
                is_new_user=visit.is_new_user,
            )

        await send_events(user_id, events, ip_address=get_client_ip(request))


app = FastAPI(lifespan=lifespan)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(LocaleMiddleware)
app.add_middleware(AnalyticsMiddleware)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(home_router)
app.include_router(reachability_router)
app.include_router(search_router)
app.include_router(session_router)
app.include_router(track_router)

if __name__ == "__main__":
    uvicorn.run("backend.app:app", host=HOST, port=PORT, reload=True)
