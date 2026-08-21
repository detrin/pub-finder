# Server-side GA4 analytics

Meet Somewhere uses the GA4 Measurement Protocol without loading `gtag.js` in
the browser. A secure, HTTP-only `_uid` cookie is reused as GA4's `client_id`.
New identifiers use GA4's documented `positive-number.positive-number` web
client-ID format. Older 22-character identifiers are retained in the browser
and database, then deterministically mapped to that format before transport so
returning-user state remains stable inside the application. The browser only
runs the first-party `static/engagement.js` helper, which sends active-page time
back to the application.

## Event and session behavior

- A session receives a positive numeric `session_id` based on its start time.
- A new session starts after 30 minutes without a tracked event.
- `page_view`, `tool_used`, `campaign_details`, and `srv_engagement` are sent.
- Reserved Measurement Protocol events such as `first_visit`, `session_start`,
  and `user_engagement` are never sent directly.
- `visitor_type` on `page_view` records `new` or `returning` according to the
  application's persistent `_uid` record.
- Engagement beacons extend the current session instead of creating a new one.
- In-flight analytics tasks are retained and drained before the database closes
  during application shutdown.

Create an event-scoped custom dimension named `Visitor type` for the
`visitor_type` event parameter in GA4 Admin. This provides the server-side
new-versus-returning breakdown; Measurement Protocol-only collection does not
populate GA4's automatically collected `first_visit` event.

The legacy client-ID mapping creates a one-time GA identity boundary for
browsers that already have the former 22-character `_uid`: their historical
application visitor record remains returning, but GA cannot retroactively join
events previously sent under the undocumented raw identifier format.

## Attribution and privacy boundaries

Page locations are full URLs, but only the following query parameters are
forwarded:

- `utm_id`
- `utm_campaign`
- `utm_source`
- `utm_medium`
- `utm_term`
- `utm_content`

All other query values are removed. Dynamic session codes are replaced with
`_` in page locations and same-site referrers. External referrers are reduced
to their origin so their query strings and paths are not copied into GA4.

The application uses Starlette's validated `request.client` address for GA4's
`ip_override`; raw `X-Forwarded-For` headers are not trusted by application
code. In production, configure Uvicorn's `FORWARDED_ALLOW_IPS` to the exact
reverse-proxy address or network so Uvicorn can safely populate
`request.client`. Private, loopback, reserved, and malformed addresses are not
sent to GA4.

## Notice and consent

Configuring the GA credentials enables collection for eligible requests. This
repository does not currently provide a consent or opt-out interface. Before
enabling analytics in production, provide the required user notice and choose
an appropriate consent or opt-out gate for the jurisdictions where the service
operates. Server-side collection does not remove that obligation.

## Local verification

Run the analytics and request-flow tests with:

```bash
.venv/bin/pytest -q tests/test_analytics.py tests/test_db.py tests/test_routers.py
```

These tests cover the 30-minute boundary, concurrent requests, schema migration,
cookie reuse, campaign/referrer filtering, dynamic-code redaction, engagement,
public-IP validation, and secret-safe transport error logging.

Before a production rollout, validate a representative payload with GA4's
Measurement Protocol validation server using credentials supplied through the
existing `GA4_MEASUREMENT_ID` and `GA4_API_SECRET` environment variables. Never
place those credentials in source code or documentation.
