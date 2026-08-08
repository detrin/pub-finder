import base64
import subprocess
from pathlib import Path

import aiosqlite
import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport

from backend.app import app
from backend.db import create_session, init_db


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
async def test_mobile_compact_controls_have_44px_minimum_width():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/static/app.css")

    assert response.status_code == 200
    mobile_css = response.text.split("@media (max-width: 640px)", maxsplit=1)[1]
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
