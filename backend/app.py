import asyncio
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
    get_client_ip,
    lookup_country,
    new_user_id,
    page_view_events,
    record_visit,
    send_events,
    session_id_for_today,
    tool_used_event,
)
from .config import DATABASE_PATH, HOST, PORT
from .db import cleanup_old_sessions, get_visitor_country, init_db, set_visitor_country
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

    yield

    await search_registry.shutdown()
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


_TOOL_ROUTES = {
    ("POST", "search"): "search",
    ("POST", "venues"): "load_venues",
    ("POST", "create"): "create_session",
}

# GET routes that render a full HTML page a person actually looks at. Everything
# else under /session/* is a partial, an SSE stream, or a JSON endpoint fetched
# in the background and must not be counted as a page view.
_PAGE_VIEW_ROUTES = re.compile(
    r"^(?:/|/how-it-works|/feedback|/session/join|/session/[^/]+(?:/results)?)$"
)


class AnalyticsMiddleware(BaseHTTPMiddleware):
    """Server-side GA4 tracking keyed off a single first-party user id cookie."""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)

        if not request.url.path.startswith("/static") and request.url.path != "/e":
            user_id = request.cookies.get(USER_ID_COOKIE) or new_user_id()
            is_new_cookie = USER_ID_COOKIE not in request.cookies
            if is_new_cookie:
                response.set_cookie(
                    USER_ID_COOKIE,
                    user_id,
                    max_age=USER_ID_COOKIE_MAX_AGE,
                    httponly=True,
                    samesite="lax",
                )
            asyncio.create_task(self._track(request, user_id))

        return response

    @staticmethod
    async def _track(request: Request, user_id: str) -> None:
        try:
            await AnalyticsMiddleware._track_unsafe(request, user_id)
        except Exception:
            logger.warning("Analytics tracking failed", exc_info=True)

    @staticmethod
    async def _track_unsafe(request: Request, user_id: str) -> None:
        db = getattr(request.app.state, "db", None)
        if db is None:
            return
        is_new_user, is_new_session, session_number = await record_visit(db, user_id)
        session_id = session_id_for_today(user_id)
        tool_name = _TOOL_ROUTES.get((request.method, request.url.path.rsplit("/", 1)[-1]))

        if tool_name:
            events = tool_used_event(
                tool_name=tool_name, session_id=session_id, session_number=session_number
            )
        elif (
            request.method == "GET"
            and "hx-request" not in request.headers
            and _PAGE_VIEW_ROUTES.match(request.url.path)
        ):
            events = page_view_events(
                page_path=request.url.path,
                page_title=request.url.path,
                session_id=session_id,
                session_number=session_number,
                is_new_user=is_new_user,
                is_new_session=is_new_session,
            )
        else:
            return

        country = await get_visitor_country(db, user_id)
        if country is None:
            ip = get_client_ip(request)
            if ip:
                country = await lookup_country(ip)
                if country:
                    await set_visitor_country(db, user_id, country)

        await send_events(user_id, events, country=country)


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
