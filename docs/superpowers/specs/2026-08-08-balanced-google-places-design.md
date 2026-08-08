# Balanced Google Places Retrieval

## Decision

Keep Google Places and the existing Leaflet UI. Improve pub coverage by issuing one
Nearby Search request per user-selected place type for only the top five ranked
meeting stops. This is explicitly a minimal retrieval fix: it does not change the
map provider, Google attribution, or the existing 90-day cache policy.

## Problem

The current implementation makes one `searchNearby` request with all selected
types combined. Nearby Search returns at most 20 places and has no page token, so
one abundant type can crowd out pubs or bars. It also queries every ranked stop
(up to 20), treats any cached row as proof that all requested types are cached,
and stops all later uncached lookups after the first Places API failure.

## Scope

- Search only the first five transit-ranked stops for pub suggestions.
- Query each selected type (`pub`, `bar`, `cafe`, or `restaurant`) separately,
  with a 500 m radius, 20 results, and distance ranking.
- Bound concurrent Google calls to four across the whole search.
- Merge and deduplicate places by place ID, then use distance to the stop,
  rating, and review count for deterministic display order.
- Record which `(stop, type, radius)` queries completed, including queries that
  found no places.
- Keep partial results if a single type or stop fails; show one generic warning
  rather than skipping all later lookups.

## Out of Scope

- Switching from Leaflet to a Google map or Places UI Kit.
- Changing the existing 90-day Google-content cache policy.
- Paginating Google Text Search, changing providers, or adding a user-facing
  "search more" control.
- Adding pub suggestions to rows below the top five results.

## Data Model

Keep `pub_cache` as the place payload table. Add two tables:

- `pub_cache_queries(stop_name, place_type, radius, cached_at)`: one row means
  the precise query completed successfully. Its presence also represents a
  successful empty result.
- `pub_cache_matches(stop_name, place_type, radius, place_id)`: maps each
  completed query to every returned place. A place may appear under several
  types and radii without duplicating its cached payload.

Both tables use composite primary keys and indexes that support cache lookup by
stop, type, and radius. A cache read is valid only when the query row is within
the existing 90-day TTL. A refresh replaces that query's match rows and updates
its query timestamp atomically with the payload upserts.

## Retrieval Flow

1. Take the first five rows from the transit-ranked results.
2. For every selected `(stop, type)` pair, first attempt the query-aware cache
   lookup. `None` means a miss; an empty list means a valid cached empty result.
3. Submit misses through a shared semaphore of four Google calls. Each request
   uses exactly one `includedTypes` value and `rankPreference: "DISTANCE"`.
4. Save a successful response, including an empty response, to all three cache
   structures in one database transaction.
5. Keep failures local to their `(stop, type)` pair. Do not write coverage for a
   failed call, so the next search retries it.
6. Merge each stop's type-specific results, deduplicate by place ID, apply the
   existing opening-hours filter, and order by distance, rating, then review
   count. The existing cross-stop de-duplication remains in effect.
7. Pass the set of searched stop names to the template. Only those five rows
   render a pub section; lower-ranked rows do not claim that no pubs exist.

## Error Handling and Observability

- A 4xx/5xx or timeout for one query logs the stop and type, adds the existing
  incomplete-data warning, and allows the other calls to finish.
- Progress counts query pairs, rather than stops, so it accurately reflects the
  new work.
- A cache hit, valid empty result, and API failure remain distinguishable in
  tests and logs.

## Tests

- Request construction includes one type, 20 results, and distance ranking.
- A response with 20 cafes cannot suppress a subsequent pub query for the same
  stop.
- A completed empty query is cached and does not refetch.
- Refreshing a query replaces only that query's match rows; same-type queries
  with different radii retain independent payloads.
- One failed type does not prevent remaining types or stops from returning.
- Only the top five result rows receive pub suggestions; the rendered UI labels
  this behavior accurately.
- Existing opening-hours filtering and cross-stop place-ID de-duplication still
  pass.
