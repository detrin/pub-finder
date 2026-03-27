from contextlib import asynccontextmanager

import polars as pl
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

import config
from routers.home import router as home_router

app_state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    distance_table = pl.read_parquet("Prague_stops_combinations.parquet")
    from_stops = distance_table["from"].unique().sort().to_list()
    to_stops = distance_table["to"].unique().sort().to_list()
    all_stops = sorted(list(set(from_stops) & set(to_stops)))

    app_state["distance_table"] = distance_table
    app_state["all_stops"] = all_stops

    yield

    app_state.clear()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(home_router)

if __name__ == "__main__":
    uvicorn.run("main:app", host=config.HOST, port=config.PORT, reload=True)
