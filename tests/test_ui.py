import base64
import subprocess
from pathlib import Path

import aiosqlite
import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport

from backend.app import app
from backend.db import create_session, init_db, save_search_results


def saved_result_fixture():
    return {
        "rows": [
            {
                "Target Stop": "B",
                "Worst Case Minutes": 15,
                "Total Minutes": 25,
                "To (Daniel)": 10,
                "From (Daniel)": 11,
                "Round trip (Daniel)": 21,
            },
            {
                "Target Stop": "C",
                "Worst Case Minutes": 18,
                "Total Minutes": 29,
                "To (Daniel)": 13,
                "From (Daniel)": 15,
                "Round trip (Daniel)": 28,
            },
        ],
        "columns": [
            "Target Stop",
            "Worst Case Minutes",
            "Total Minutes",
            "To (Daniel)",
            "From (Daniel)",
            "Round trip (Daniel)",
        ],
        "pubs_by_stop": {"B": [], "C": []},
        "pub_search_stop_names": ["B"],
        "place_types": ["pub", "bar"],
        "stops_geo": [
            {"name": "B", "lat": 50.1, "lon": 14.1},
            {"name": "C", "lat": 50.2, "lon": 14.2},
        ],
        "pubs_flat": [],
        "participants_geo": [],
        "participant_snapshot": [
            {
                "id": 1,
                "name": "Daniel",
                "color": "#ff6658",
                "start_stop": "A",
                "end_stop": "A",
            }
        ],
        "search_direction": "round-trip",
        "warning": None,
    }


@pytest_asyncio.fixture(autouse=True)
async def ui_app_state():
    db = await aiosqlite.connect(":memory:")
    await init_db(db)
    app.state.db = db
    app.state.all_stops = ["A", "B"]
    yield
    await db.close()


@pytest.mark.asyncio
async def test_home_uses_meet_somewhere_shell():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    assert response.status_code == 200
    assert "Meet Somewhere" in response.text
    assert 'class="brand"' in response.text
    assert "/static/theme-init.js" in response.text
    assert "oat.min.css" not in response.text
    assert "oat.min.js" not in response.text


@pytest.mark.asyncio
async def test_home_has_one_primary_start_form_and_secondary_join_path():
    """Catch a regression to the competing create and join card layout."""
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    assert "Pick a place that works for everyone." in response.text
    assert 'name="session_name"' in response.text
    assert 'name="creator_name"' in response.text
    assert 'data-join-disclosure' in response.text
    assert 'data-session-history' in response.text


@pytest.mark.asyncio
async def test_direct_invite_names_the_session_and_requires_only_name():
    """Catch an invite page that omits its plan name or asks for setup fields."""
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/join?code={session['code']}")

    assert "You’re invited to Friday crew." in response.text
    assert 'name="code"' in response.text
    assert 'name="name"' in response.text
    assert 'name="session_name"' not in response.text


@pytest.mark.asyncio
async def test_session_workspace_exposes_autosave_and_dialog_hooks():
    """The session page keeps its HTMX workspace and native dialog hooks."""
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}")

    assert 'class="session-workspace"' in response.text
    assert "data-stop-dialog" in response.text
    assert "data-remove-dialog" in response.text
    assert 'aria-live="polite"' in response.text
    assert "Find somewhere" in response.text


@pytest.mark.asyncio
async def test_session_dialogs_have_labels_and_live_regions():
    session = await create_session(app.state.db, "Test", "Daniel")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}")

    assert 'aria-labelledby="stop-picker-title"' in response.text
    assert 'aria-labelledby="remove-participant-title"' in response.text
    assert 'role="status"' in response.text
    assert 'aria-live="polite"' in response.text


def test_dialog_focus_restoration_only_targets_a_connected_invoker():
    """Catch dialogs losing focus or focusing controls removed by an HTMX swap."""
    session_module = Path("static/session.js").resolve().as_uri()
    script = """
globalThis.document = {
  readyState: "loading",
  addEventListener() {},
};
const { restoreDialogFocus, showModalWithFocusReturn } = await import(process.argv[1]);
let focusCount = 0;
const invoker = {
  isConnected: true,
  focus() { focusCount += 1; },
};
const dialog = {
  returnValue: "stale",
  showModalCount: 0,
  showModal() { this.showModalCount += 1; },
};
showModalWithFocusReturn(dialog, invoker);
if (dialog.returnValue !== "" || dialog.showModalCount !== 1) {
  throw new Error("Dialog did not reset its return value before opening");
}
restoreDialogFocus(dialog);
if (focusCount !== 1) throw new Error(`Expected one focus restoration, got ${focusCount}`);
showModalWithFocusReturn(dialog, invoker);
invoker.isConnected = false;
restoreDialogFocus(dialog);
if (focusCount !== 1) throw new Error("A detached invoker received focus");
"""

    result = subprocess.run(
        ["node", "--input-type=module", "--eval", script, session_module],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_session_workspace_disables_search_until_every_participant_has_stops():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}")

    assert 'data-session-readiness' in response.text
    assert 'data-search-submit' in response.text
    assert 'data-search-submit disabled' in response.text
    assert "Daniel needs start and end stops." in response.text


@pytest.mark.asyncio
async def test_saved_results_render_split_workspace_and_reachability_url():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    await save_search_results(app.state.db, session["code"], saved_result_fixture())
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/results")

    assert 'class="results-workspace"' in response.text
    assert f'data-reachability-url="/session/{session["code"]}/reachability"' in response.text
    assert 'data-selected-rank="1"' in response.text
    assert "data-map-data" in response.text
    assert "data-stops=" in response.text
    assert "data-venues=" in response.text
    assert "data-participants=" in response.text
    assert "Longest journey" in response.text
    assert "Approximate from typical transit times" in response.text


@pytest.mark.asyncio
async def test_results_rank_controls_expand_the_top_result_on_the_server():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    await save_search_results(app.state.db, session["code"], saved_result_fixture())
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/results")

    assert 'data-rank="1"' in response.text
    assert 'aria-controls="result-detail-1"' in response.text
    assert 'aria-expanded="true"' in response.text
    assert 'id="result-detail-1"' in response.text
    assert 'id="result-detail-2"' in response.text
    second_detail = response.text.split('id="result-detail-2"', 1)[1].split(">", 1)[0]
    assert "hidden" in second_detail
    assert "data-mobile-view=\"map\"" in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("direction", "to_minutes", "from_minutes", "round_minutes", "shown_labels"),
    [
        ("there-only", 10, 999, 10, ("to",)),
        ("back-only", 999, 11, 11, ("from",)),
        ("round-trip", 10, 11, 21, ("to", "from", "round trip")),
    ],
)
async def test_results_show_only_journey_legs_used_by_the_saved_direction(
    direction, to_minutes, from_minutes, round_minutes, shown_labels
):
    fixture = saved_result_fixture()
    fixture["search_direction"] = direction
    fixture["rows"] = [fixture["rows"][0]]
    fixture["stops_geo"] = [fixture["stops_geo"][0]]
    fixture["rows"][0]["To (Daniel)"] = to_minutes
    fixture["rows"][0]["From (Daniel)"] = from_minutes
    fixture["rows"][0]["Round trip (Daniel)"] = round_minutes
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    await save_search_results(app.state.db, session["code"], fixture)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/results")

    assert "999 min" not in response.text
    assert ">15</strong>" in response.text
    assert ">25</strong>" in response.text
    for label in {"to", "from", "round trip"}:
        marker = f"<small>{label}</small>"
        assert (marker in response.text) is (label in shown_labels)


@pytest.mark.asyncio
async def test_participant_reachability_selector_is_a_pressed_button_group():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    await save_search_results(app.state.db, session["code"], saved_result_fixture())
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/results")

    assert 'data-participant-tabs role="group"' in response.text
    assert 'data-participant-id="" aria-pressed="true"' in response.text
    assert 'role="tablist"' not in response.text
    assert 'role="tab"' not in response.text


@pytest.mark.asyncio
async def test_results_threshold_keeps_a_44px_pointer_target():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/static/app.css")

    threshold_rule = response.text.split(".threshold-control input {", 1)[1].split("}", 1)[0]
    assert "min-height: 44px;" in threshold_rule


@pytest.mark.asyncio
async def test_results_json_attributes_escape_names_without_losing_visible_text():
    fixture = saved_result_fixture()
    unsafe_name = "O'Reilly \"Stop\" <script>alert(1)</script>"
    fixture["rows"][0]["Target Stop"] = unsafe_name
    fixture["stops_geo"][0]["name"] = unsafe_name
    fixture["pubs_by_stop"] = {unsafe_name: [], "C": []}
    fixture["pub_search_stop_names"] = [unsafe_name]
    fixture["participant_snapshot"][0]["name"] = 'Daniel "<script>"'
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    await save_search_results(app.state.db, session["code"], fixture)
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/results")

    assert unsafe_name not in response.text
    assert "O&#39;Reilly" in response.text or "O&#x27;Reilly" in response.text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in response.text


@pytest.mark.asyncio
async def test_shell_routes_use_meet_somewhere_titles():
    session = await create_session(app.state.db, "Friday drinks", "Daniel")
    expected_titles = {
        "/": "Meet Somewhere",
        "/how-it-works": "How it works - Meet Somewhere",
        "/feedback": "Feedback - Meet Somewhere",
        f"/session/join?code={session['code']}": "Join Session - Meet Somewhere",
        f"/session/{session['code']}": "Friday drinks - Meet Somewhere",
        f"/session/{session['code']}/results": "Results - Friday drinks - Meet Somewhere",
    }

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        for path, title in expected_titles.items():
            response = await client.get(path)
            assert response.status_code == 200
            assert f"<title>{title}</title>" in response.text


@pytest.mark.asyncio
async def test_how_it_works_uses_verified_dataset_facts():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/how-it-works")

    assert "1,444" in response.text
    assert "2,083,035" in response.text
    assert "precomputed typical times" in response.text
    assert "live DPP" in response.text
    assert "1,463" not in response.text


@pytest.mark.asyncio
async def test_feedback_keeps_google_form_and_gives_factual_report_instructions():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/feedback")

    assert (
        "https://docs.google.com/forms/d/e/1FAIpQLSfAjnlpuEmWlHQj9sGgSqjgKx0DFjj_jr3hOwMx5-laZrJG3w/viewform?embedded=true"
        in response.text
    )
    assert "what you expected, what happened, your browser and device, and the session code" in response.text


@pytest.mark.asyncio
async def test_results_without_saved_search_uses_empty_system_message():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/results")

    assert 'data-system-message="empty"' in response.text
    assert "No saved results" in response.text
    message = response.text.split('data-system-message="empty"', 1)[1].split("</section>", 1)[0]
    assert message.count("Open plan") == 1


@pytest.mark.asyncio
async def test_results_reachability_warning_keeps_the_results_js_hook():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    await save_search_results(app.state.db, session["code"], saved_result_fixture())
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/results")

    warning = response.text.split('data-system-message="warning"', 1)[1].split("</section>", 1)[0]
    assert "data-reachability-error" in warning
    assert "hidden" in warning


@pytest.mark.asyncio
async def test_mobile_compact_controls_have_44px_minimum_width():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/static/app.css")

    assert response.status_code == 200
    mobile_css = response.text.split("@media (max-width: 720px)", maxsplit=1)[1]
    assert "button," in mobile_css
    assert "min-width: 44px;" in mobile_css


def test_theme_toggle_announces_the_action_for_the_initial_theme():
    source = Path("static/theme.js").read_bytes()
    theme_module = f"data:text/javascript;base64,{base64.b64encode(source).decode()}"
    script = """
const button = {
  dataset: {},
  attributes: {},
  addEventListener(_event, handler) { this.handler = handler; },
  setAttribute(name, value) { this.attributes[name] = value; },
};
globalThis.document = {
  documentElement: { dataset: { theme: "dark" }, style: {} },
  querySelector() { return button; },
};
globalThis.localStorage = { setItem() {} };
const { initThemeToggle } = await import(process.argv[1]);
initThemeToggle();
if (button.attributes["aria-label"] !== "Use light theme") {
  throw new Error(`Unexpected initial label: ${button.attributes["aria-label"]}`);
}
button.handler();
if (button.attributes["aria-label"] !== "Use dark theme") {
  throw new Error(`Unexpected toggled label: ${button.attributes["aria-label"]}`);
}
"""

    result = subprocess.run(
        ["node", "--input-type=module", "--eval", script, theme_module],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_recent_session_storage_recovers_from_bad_data_and_uses_safe_link_text():
    """Catch unbounded or unsafe recent-session rendering from local storage."""
    history_module = Path("static/history.js").resolve().as_uri()
    script = """
const store = new Map([["meet_somewhere_recent_sessions", "not-json"]]);
const root = {
  hidden: true,
  children: [],
  replaceChildren(...children) { this.children = children; },
};
globalThis.localStorage = {
  getItem(key) { return store.get(key) ?? null; },
  setItem(key, value) { store.set(key, value); },
};
globalThis.document = {
  querySelector(selector) {
    return selector === "[data-session-history]" ? root : null;
  },
  createElement(tag) {
    if (tag !== "a") throw new Error(`Unexpected element: ${tag}`);
    return { href: "", textContent: "" };
  },
};
const { rememberSession, renderRecentSessions } = await import(process.argv[1]);
for (const code of ["a", "b", "c", "d", "e", "f"]) {
  rememberSession({ code, name: `Plan ${code}` });
}
const saved = JSON.parse(store.get("meet_somewhere_recent_sessions"));
if (saved.length !== 5 || saved[0].code !== "f" || saved[4].code !== "b") {
  throw new Error(`Unexpected bounded history: ${JSON.stringify(saved)}`);
}
store.set(
  "meet_somewhere_recent_sessions",
  JSON.stringify([
    { code: "safe code", name: "<img src=x>" },
    { code: "two", name: "Plan two" },
    { code: "three", name: "Plan three" },
    { code: "four", name: "Plan four" },
    { code: "five", name: "Plan five" },
    { code: "six", name: "Plan six" },
    null,
    { code: 7, name: "bad" },
  ]),
);
renderRecentSessions();
if (root.hidden || root.children.length !== 5) {
  throw new Error("Expected five valid recent session links");
}
if (root.children[0].textContent !== "<img src=x>") {
  throw new Error("Recent session name was not rendered as text");
}
if (root.children[0].href !== "/session/safe%20code") {
  throw new Error(`Unexpected recent session link: ${root.children[0].href}`);
}
if (root.children[4].href !== "/session/five") {
  throw new Error(`Expected existing history to be capped: ${root.children[4].href}`);
}
"""

    result = subprocess.run(
        ["node", "--input-type=module", "--eval", script, history_module],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
