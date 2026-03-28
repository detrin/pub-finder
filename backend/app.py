import logging
from contextlib import asynccontextmanager

import aiosqlite
import polars as pl
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .config import DATABASE_PATH, HOST, PORT
from .db import init_db, cleanup_old_sessions
from routers.home import router as home_router
from routers.search import router as search_router
from routers.session import router as session_router

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

    yield

    await db.close()


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://unpkg.com; "
            "img-src 'self' data: https://*.tile.openstreetmap.org https://maps.google.com; "
            "connect-src 'self'; "
            "font-src 'self' https://cdn.jsdelivr.net; "
            "frame-ancestors 'none'"
        )
        return response


app = FastAPI(lifespan=lifespan)
app.add_middleware(SecurityHeadersMiddleware)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(home_router)
app.include_router(search_router)
app.include_router(session_router)

if __name__ == "__main__":
    uvicorn.run("backend.app:app", host=HOST, port=PORT, reload=True)
