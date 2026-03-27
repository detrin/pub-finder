from contextlib import asynccontextmanager

import aiosqlite
import polars as pl
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

import config
from config import DATABASE_PATH
from db import init_db
from routers.home import router as home_router
from routers.session import router as session_router

app_state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    distance_table = pl.read_parquet("Prague_stops_combinations.parquet")
    from_stops = distance_table["from"].unique().sort().to_list()
    to_stops = distance_table["to"].unique().sort().to_list()
    all_stops = sorted(list(set(from_stops) & set(to_stops)))

    app_state["distance_table"] = distance_table
    app_state["all_stops"] = all_stops

    db = await aiosqlite.connect(DATABASE_PATH)
    await init_db(db)
    app.state.db = db
    app_state["db"] = db

    yield

    await db.close()
    app_state.clear()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(home_router)
app.include_router(session_router)

if __name__ == "__main__":
    uvicorn.run("main:app", host=config.HOST, port=config.PORT, reload=True)
