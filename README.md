# Meet Somewhere

Meet Somewhere ranks Prague meeting points using public transport journey times for every person in a group. It can minimize the longest individual journey or total group travel time, then show nearby pubs, bars, cafes, and restaurants.

The optional reachability layer is derived from 2,083,035 precomputed directional stop pairs. It is approximate. Ranked results use live DPP queries for the selected departure and return times.

Try the live demo at https://pub-finder.hermandaniel.com.

## How does it work?

We have 1,400+ transit stops in Prague. Given a set of `k` stops where friends are starting from (e.g. Krymska, Andel, Muzeum), we find the target stop that is closest to everyone. "Closest" can mean:

1. **Minimize worst-case** -- minimize the maximum travel time from any friend's starting stop.
2. **Minimize total** -- minimize the sum of all travel times.

The naive approach uses geographic (Haversine) distance between stops. But public transit speeds vary by route, so we scraped ~2.1M stop-pair travel times from DPP to use actual transit minutes as the distance metric.

The search works in stages. For each active direction, it selects up to 20 stops by geographic distance and up to 20 by precomputed transit time. Round-trip searches take the union of the outbound and return-direction candidate sets. It then queries live DPP journey times for the selected departure and return times, reranks the candidates, and returns the top 20 meeting points. For the top five transit-ranked stops, type-balanced Google Places discovery checks the selected venue types (pub, bar, cafe, and optionally restaurant) within 500m; results are cached for 90 days.

## Features

- **Session-based** -- create a session, share the code, friends join and pick their stops
- **Real-time updates** -- participant list updates live via Server-Sent Events
- **Interactive map** -- Leaflet.js map showing participants, ranked stops, and nearby venues
- **Approximate reachability** -- participant and group views derived from precomputed typical transit times
- **Venue discovery** -- type-balanced Google Places API integration for the top five results, with ratings, price level, and walking directions
- **Shareable results** -- permanent link to search results for each session
- **Round-trip support** -- optionally set a different return stop

## Quick start

### Local development

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync --locked --extra dev
cp .env.example .env
# Edit .env and add your GOOGLE_PLACES_API_KEY
uv run python -m backend
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
| `GOOGLE_PLACES_API_KEY` | _(empty)_ | Required for nearby venue search |
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
uv sync --locked --extra dev
uv run pytest
npm run test:js
```

## Data preparation

The `data_preparation` module provides a CLI for scraping transit times and preparing stop data.

```bash
uv sync --locked --extra data-prep
uv run python -m data_preparation --help
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
