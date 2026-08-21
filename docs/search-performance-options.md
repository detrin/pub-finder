# Search Performance Options

Last updated: 2026-08-21

## Context

Search latency is dominated by live journey-time lookups against the DPP HTML
journey planner. The original flow generated up to 20 geographic and 20
precomputed-transit candidates for every active direction, then queried every
candidate live for every participant and required trip leg.

For a representative three-person round trip, candidate discovery produced 41
stops and therefore 246 live requests (`41 × 3 × 2`). A single observed DPP
request took about 0.86 seconds. With five candidate workers and sequential
participant lookups inside each worker, a 40–50 second uncached search is an
expected outcome rather than an isolated provider slowdown.

This document records the three considered paths. Option 1 is implemented.
Options 2 and 3 remain available if we need lower latency, better scaling, or a
supported replacement for HTML scraping.

## Option 1: Limit live reranking to ten finalists

Status: **implemented**

The application still forms the broad geographic and precomputed-transit
candidate union. Before making live requests, it now scores that union with the
local transit matrix using the same inputs as the final search:

- `there-only`: start stop to candidate;
- `back-only`: candidate to end stop;
- `round-trip`: both legs summed per participant;
- `minimize-worst-case`: lowest maximum participant score;
- `minimize-total`: lowest sum of participant scores.

Duplicate matrix rows are collapsed to their shortest recorded time. A missing
precomputed leg receives the same 999-minute penalty used by final reranking,
so sparse matrix coverage lowers a candidate's priority without removing every
finalist before the authoritative live lookup.

Only the best ten matrix-scored candidates proceed to live DPP reranking. This
is deliberately not `candidates[:10]`: the union is assembled geographic-first
and its input order is not a quality ranking.

The final results now contain at most ten live-ranked stops instead of up to
twenty. This is the accepted accuracy/latency tradeoff: a candidate outside the
matrix top ten can no longer jump into the live results because of timetable
conditions that differ from the precomputed matrix.

### Local validation

The implementation was checked against the production Prague matrix. For every
scenario below, an independent Python calculation of all candidate scores
matched the application's selected top ten.

| Scenario | Candidates before | Live requests before | Live requests after |
| --- | ---: | ---: | ---: |
| Three-person round trip, worst case | 41 | 246 | 60 |
| Four-person round trip, total | 35 | 280 | 80 |
| Asymmetric outbound, worst case | 38 | 114 | 30 |
| Asymmetric return, total | 40 | 120 | 30 |
| Asymmetric round trip, worst case | 70 | 420 | 60 |
| Two-person round trip including sparse `Škola Poštovka` coverage | 37 | 148 | 40 |

Automated coverage also exercises both optimization objectives across all three
directions, rejects a naive input-order slice, and verifies that the search
progress and persisted result set contain only the selected ten candidates.

## Option 2: Google Routes Compute Route Matrix

Status: **not implemented**

Google Routes can calculate public-transit durations for a matrix of origins
and destinations with a future departure or arrival time. This could replace
dozens of individual HTML requests with a few supported, batched requests.

Important constraints:

- Transit matrices allow at most 100 origin-destination elements per request.
- The default quota is 3,000 matrix elements per minute.
- Billing is per returned element, not per HTTP request.
- The current global Essentials allowance is 10,000 free monthly events, then
  starts at USD 5 per 1,000 events.
- Google Maps attribution and the applicable EEA service terms must be reviewed
  before integrating the results with the existing Leaflet UI.

With Option 1 already reducing a three-person round trip to 60 elements, common
searches would fit in one outbound and one return matrix request. One hundred
such searches per month would also remain below the present 10,000-event free
allowance.

Before adoption, run a comparison spike over saved test scenarios and check:

1. duration and ranking agreement with the DPP planner;
2. latency for outbound, return, and round-trip searches;
3. behavior around service disruptions and walking transfers;
4. attribution, caching, and EEA contract requirements;
5. cost at expected monthly search volume.

References:

- [Compute Route Matrix](https://developers.google.com/maps/documentation/routes/compute_route_matrix)
- [Routes API limits and billing behavior](https://developers.google.com/maps/documentation/routes/usage-and-billing)
- [Google Maps Platform pricing](https://developers.google.com/maps/billing-and-pricing/pricing)
- [Routes API policies and attribution](https://developers.google.com/maps/documentation/routes/policies)

## Option 3: Official PID data with OpenTripPlanner

Status: **not implemented; preferred independent long-term path**

PID publishes a daily GTFS feed under CC-BY. It contains the PID timetable,
routes, stops, transfers, and related schedule data. OpenTripPlanner can build a
local routing graph from GTFS and OpenStreetMap, expose a routing API, and apply
GTFS-Realtime updates.

Advantages:

- no per-search routing fee;
- no dependency on undocumented HTML structure;
- routing remains under our operational control;
- official open timetable data and optional real-time updates.

Costs and constraints:

- OpenTripPlanner is a separate Java service with meaningful memory needs;
- the graph and PID feed need an automated daily refresh;
- production monitoring and fallback behavior are required;
- PID's main GTFS horizon is approximately 12–14 days, while this application
  currently permits dates up to 31 days ahead;
- Prague-specific stop matching, transfers, disruptions, and route-duration
  agreement must be benchmarked before replacing DPP results.

A sensible future spike is to run OpenTripPlanner locally with a Prague-only
OpenStreetMap extract and the current PID GTFS feed, then compare its durations
and rankings against the same scenarios used for Option 2.

References:

- [PID open data and licensing](https://pid.cz/en/opendata/)
- [PID GTFS download](https://data.pid.cz/PID_GTFS.zip)
- [Golemio Public Transport API](https://api.golemio.cz/pid/docs/openapi/)
- [OpenTripPlanner documentation](https://docs.opentripplanner.org/)
- [OpenTripPlanner real-time updaters](https://docs.opentripplanner.org/en/latest/Realtime-Updaters/)

## PID journey-planner API outreach

The public Golemio API documents GTFS, GTFS-Realtime, departure boards,
vehicle positions, and lookup endpoints, but no public point-to-point journey
planner or route matrix. Operátor ICT procurement documents confirm that a web
journey-planning service exists for PID Lítačka, so supported partner access is
worth requesting from `opendata@pid.cz` and `golemio@operatorict.cz`.

The request should ask about:

- point-to-point and batch/matrix journey-time endpoints;
- partner or public API eligibility;
- licensing, attribution, caching, and redistribution rules;
- request and element rate limits;
- future timetable horizon and real-time disruption handling;
- the supported alternative if direct planner access is unavailable.

Reference: [PID Lítačka journey-planning service procurement](https://zakazky.operatorict.cz/vz00000255)
