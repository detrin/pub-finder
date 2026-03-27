# Pub Finder

Find the optimal pub to meet with your friends in Prague, using public transit optimization.

Try the live demo at https://pub-finder.hermandaniel.com.

## How does it work?

We have 1,400+ transit stops in Prague. Given a set of `k` stops where friends are starting from (e.g. Krymska, Andel, Muzeum), we find the target stop that is closest to everyone. "Closest" can mean:

1. **Minimize worst-case** -- minimize the maximum travel time from any friend's starting stop.
2. **Minimize total** -- minimize the sum of all travel times.

The naive approach uses geographic (Haversine) distance between stops. But public transit speeds vary by route, so we scraped ~2.1M stop-pair travel times from DPP to use actual transit minutes as the distance metric.

The search works in stages: first, we select the top 10 stops by geographic distance and top 25 by pre-computed transit time. Then we scrape real-time travel times for the selected date/time and re-rank. The top 15 target stops are returned, each with nearby pubs discovered via the Google Places API (cached for 90 days).

## Features

- **Session-based** -- create a session, share the code, friends join and pick their stops
- **Real-time updates** -- participant list updates live via Server-Sent Events
- **Interactive map** -- Leaflet.js map showing stops and recommended pubs
- **Pub discovery** -- Google Places API integration with rating, price level, and walking directions
- **Shareable results** -- permanent link to search results for each session
- **Round-trip support** -- optionally set a different return stop

## Quick start

### Local development

Requires Python 3.12+.

```bash
pip install -e ".[dev]"
cp .env.example .env
# Edit .env and add your GOOGLE_PLACES_API_KEY
python -m backend
```

Visit http://localhost:3000.

### Docker

```bash
cp .env.example .env
# Edit .env and add your GOOGLE_PLACES_API_KEY
docker compose up --build
```

Visit http://localhost:3000.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GOOGLE_PLACES_API_KEY` | _(empty)_ | Required for pub search |
| `DATABASE_PATH` | `pub_finder.db` | SQLite database path |
| `HOST` | `0.0.0.0` | Server bind address |
| `PORT` | `3000` | Server port |

## Project structure

```
backend/          FastAPI app, config, DB, optimization, Places API client
routers/          Route handlers (home, session, search)
templates/        Jinja2 templates with HTMX partials
static/           CSS, JS, favicon
data/             Pre-computed transit data (parquet) and stop lists
data_preparation/ CLI tools for scraping and preparing transit data
tests/            Pytest test suite
```

## Testing

```bash
pip install -e ".[dev]"
pytest
```

## Data preparation

The `data_preparation` module provides a CLI for scraping transit times and preparing stop data.

```bash
pip install -e ".[data-prep]"
python -m data_preparation --help
```

Subcommands:

- `scrape` -- scrape travel times between stop pairs from DPP
- `manage` -- filter errors and manage scrape results
- `prepare` -- generate geo data from raw GPS JSON files
- `bandit-sim` -- run multi-armed bandit simulation for adaptive scraping

## Sources

- https://spojeni.dpp.cz/ -- DPP transit journey planner
- https://pid.cz/zastavky-pid/zastavky-v-praze -- PID stop listings
- https://mapa.pid.cz/ -- PID transit map

## License

MIT
