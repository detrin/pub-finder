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
