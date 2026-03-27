import logging
from contextlib import asynccontextmanager

import aiosqlite
import polars as pl
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

import config
from config import DATABASE_PATH
from db import init_db, cleanup_old_sessions
from routers.home import router as home_router
from routers.search import router as search_router
from routers.session import router as session_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

app_state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    distance_table = pl.read_parquet("data/Prague_stops_combinations.parquet")
    from_stops = distance_table["from"].unique().sort().to_list()
    to_stops = distance_table["to"].unique().sort().to_list()
    all_stops = sorted(list(set(from_stops) & set(to_stops)))

    stop_geo = pl.read_parquet("data/Prague_stops_geo.parquet")

    app_state["distance_table"] = distance_table
    app_state["all_stops"] = all_stops
    app_state["stop_geo"] = stop_geo

    db = await aiosqlite.connect(DATABASE_PATH)
    await init_db(db)
    await cleanup_old_sessions(db)
    app.state.db = db
    app_state["db"] = db

    yield

    await db.close()
    app_state.clear()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(home_router)
app.include_router(search_router)
app.include_router(session_router)

if __name__ == "__main__":
    uvicorn.run("main:app", host=config.HOST, port=config.PORT, reload=True)
