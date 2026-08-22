import base64
import re
import subprocess
from pathlib import Path

import aiosqlite
import httpx
import pytest
import pytest_asyncio
from bs4 import BeautifulSoup
from httpx import ASGITransport

from backend.app import app
from backend.db import (
    add_participant,
    add_participant_stops,
    begin_search,
    create_session,
    get_participants,
    get_search_results,
    init_db,
    save_search_results,
    update_search_results_if_current,
)


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
    assert "/static/theme.js" in response.text
    assert "/static/history.js" in response.text
    assert "/static/app.js" not in response.text
    assert "oat.min.css" not in response.text
    assert "oat.min.js" not in response.text


@pytest.mark.asyncio
async def test_navigation_and_favicon_use_the_same_canonical_mark():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    page = BeautifulSoup(response.text, "html.parser")
    favicon = page.find("link", rel="icon")
    brand_mark = page.select_one(".brand-mark")

    assert favicon is not None
    assert brand_mark is not None
    assert favicon["href"] == "/static/meet-somewhere-mark.svg"
    assert brand_mark.name == "img"
    assert brand_mark["src"] == favicon["href"]


def test_canonical_mark_contains_the_approved_convergence_geometry():
    mark = Path("static/meet-somewhere-mark.svg").read_text()

    assert 'viewBox="0 0 64 64"' in mark
    assert mark.count("<path") == 6
    assert 'fill="#4DC694"' in mark
    assert "#2458DF" in mark
    assert "#FF6658" in mark
    assert "#FFD447" in mark


@pytest.mark.asyncio
async def test_home_has_one_primary_start_form_and_secondary_join_path():
    """Catch a regression to the competing create and join card layout."""
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    assert "Find a place that works for everyone." in response.text
    assert "Quick estimate" in response.text
    assert "Approximate · one way" in response.text
    assert 'name="session_name"' in response.text
    create_form = BeautifulSoup(response.text, "html.parser").select_one(
        'form[action="/session/create"][method="post"]'
    )
    assert create_form is not None
    assert not create_form.has_attr("hx-post")
    assert not create_form.has_attr("hx-target")
    assert create_form.select_one('[name="creator_name"]') is None
    assert "data-join-disclosure" in response.text
    assert "data-session-history" in response.text


@pytest.mark.asyncio
async def test_home_renders_sessionless_preview_before_the_single_create_form():
    """Catch regressions to the approved hero-first planning flow."""
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    page = BeautifulSoup(response.text, "html.parser")
    hero = page.select_one(".home-hero")
    convergence = hero.select_one(".home-convergence")
    planning_flow = page.select_one(".home-planning-flow")
    preview = page.select_one("[data-home-preview]")
    create_forms = page.select('form[action="/session/create"][method="post"]')

    assert convergence is not None
    assert convergence.select_one('svg[aria-hidden="true"]') is not None
    assert preview is not None
    assert preview.find_parent(class_="home-hero") is None
    assert preview.find_parent(class_="home-planning-flow") is planning_flow
    assert preview.select_one("[data-preview-map]") is not None
    assert preview.select_one('[role="combobox"]') is not None
    assert preview.get("data-stops") == '["A", "B"]'
    assert len(create_forms) == 1
    main_elements = list(page.select_one("main").descendants)
    assert main_elements.index(hero) < main_elements.index(preview)
    assert main_elements.index(preview) < main_elements.index(create_forms[0])
    assert planning_flow.select_one(":scope > .start-plan") is not None
    assert "Approximate · one way" in preview.get_text(" ", strip=True)
    assert "No selected date" in preview.get_text(" ", strip=True)


@pytest.mark.asyncio
async def test_home_preview_exposes_accessible_recovery_and_handoff_structure():
    """Catch a visual-only map that loses keyboard and no-JavaScript affordances."""
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        home_response = await client.get("/")
        other_response = await client.get("/how-it-works")

    page = BeautifulSoup(home_response.text, "html.parser")
    preview = page.select_one("[data-home-preview]")
    search = preview.select_one("[data-preview-search]")
    options = preview.select_one("[data-preview-options]")
    selections = preview.select_one("ul[data-preview-selections]")
    status = preview.select_one('[data-preview-status][aria-live="polite"]')
    legend = preview.select_one("[data-preview-legend]")
    map_root = preview.select_one("[data-preview-map]")
    handoff = preview.select_one('[data-preview-handoff][href="#session-name"]')
    disclosure = preview.select_one(
        '.home-estimate__disclosure a[href="/how-it-works#homepage-quick-estimate"]'
    )
    canonical_options = page.select("#home-stop-suggestions option")
    create_form = page.select_one('form[action="/session/create"]')

    assert search["aria-controls"] == options["id"]
    assert options["role"] == "listbox"
    assert selections is not None
    assert status is not None
    assert legend.name == "ul"
    assert [swatch.get("class", [])[-1] for swatch in legend.select(".travel-time-swatch")] == [
        "travel-time-swatch--fastest",
        "travel-time-swatch--short",
        "travel-time-swatch--medium",
        "travel-time-swatch--long",
        "travel-time-swatch--longest",
        "travel-time-swatch--missing",
    ]
    assert "20 min" in legend.get_text(" ", strip=True)
    assert "no estimate" in legend.get_text(" ", strip=True)
    assert map_root.get("role") == "region"
    assert map_root.get("aria-label") == "Interactive approximate reach map"
    assert not map_root.has_attr("aria-hidden")
    assert preview.select_one("[data-preview-attribution]") is None
    assert "OpenStreetMap contributors" not in preview.get_text(" ", strip=True)
    assert handoff is not None
    assert disclosure is not None
    assert "home-estimate__method-link" in disclosure.get("class", [])
    assert "estimated walking from the nearest stop" in disclosure.get_text(" ", strip=True)
    assert page.select_one("#session-name") is not None
    assert create_form.select_one("[data-preview-hidden-fields]") is not None
    assert create_form.select_one("[data-preview-carry-status]") is not None
    assert [option["value"] for option in canonical_options] == ["A", "B"]
    home_modules = [script.get("src") for script in page.select('script[type="module"]')]
    assert "/static/home-preview.js?v=6" in home_modules
    assert "/static/home-preview.js" not in other_response.text
    assert page.select_one('link[rel="stylesheet"][href="/static/app.css?v=52"]') is not None
    assert (
        page.select_one('link[rel="modulepreload"][href="/static/reachability-map.js?v=8"]')
        is not None
    )
    assert preview["data-limit"] == (
        "The quick estimate supports up to six starting stops. For larger groups, start a plan."
    )


def test_home_preview_legend_styles_match_the_canvas_encoding():
    css = Path("static/app.css").read_text()

    for selector, value in (
        ("fastest", "var(--blue)"),
        ("short", "var(--mint)"),
        ("medium", "var(--yellow)"),
        ("long", "var(--coral)"),
        ("longest", "var(--sky-surface)"),
    ):
        assert f".travel-time-swatch--{selector} {{ background: {value}; }}" in css
    assert re.search(
        r"\.travel-time-swatch--missing\s*\{[^}]*repeating-linear-gradient\(",
        css,
        re.DOTALL,
    )
    map_rule = re.search(r"\.home-estimate__map\s*\{(?P<body>[^}]*)}", css, re.DOTALL)
    assert map_rule is not None
    assert "pointer-events: none" not in map_rule.group("body")
    assert re.search(r"\.home-estimate__map \.leaflet-control-attribution\s*\{", css)


def test_home_preview_marker_letters_center_inside_the_leaflet_dots():
    css = Path("static/app.css").read_text()
    marker_rule = re.search(
        r"\.home-estimate__map \.participant-marker-label\s*\{(?P<body>[^}]*)}",
        css,
        re.DOTALL,
    )

    assert marker_rule is not None
    declarations = marker_rule.group("body")
    assert "display: flex;" in declarations
    assert "align-items: center;" in declarations
    assert "justify-content: center;" in declarations
    assert "width: 16px;" in declarations
    assert "height: 16px;" in declarations
    assert "padding: 0;" in declarations
    assert "line-height: 1;" in declarations


def test_home_and_results_use_the_same_travel_time_legend():
    home = Path("templates/home.html").read_text()
    results = Path("templates/partials/results_table.html").read_text()
    shared = Path("templates/partials/travel_time_legend.html").read_text()

    assert '{% include "partials/travel_time_legend.html" %}' in home
    assert '{% include "partials/travel_time_legend.html" %}' in results
    assert shared.count('class="travel-time-swatch ') == 6
    assert 't("home.legend_20"' in shared


def test_home_cards_and_stop_pills_use_complete_shared_frames():
    css = Path("static/app.css").read_text()

    assert re.search(
        r"\.home-planning-flow\s*\{[^}]*width:\s*100%;",
        css,
        re.DOTALL,
    )
    assert re.search(
        r"\.home-estimate__selections li\s*\{[^}]*box-shadow:\s*var\(--control-shadow\);",
        css,
        re.DOTALL,
    )
    remove_rule = re.search(
        r"\.home-estimate__selections button\s*\{(?P<body>[^}]*)}",
        css,
        re.DOTALL,
    )
    assert remove_rule is not None
    assert "box-shadow: none" in remove_rule.group("body")


def test_home_preview_disclosure_wraps_without_expanding_the_handoff():
    css = Path("static/app.css").read_text()

    assert re.search(
        r"\.home-estimate__method-link\s*\{[^}]*overflow-wrap:\s*anywhere;",
        css,
        re.DOTALL,
    )
    assert re.search(
        r"\.home-estimate__disclosure\s*>\s*\[data-preview-handoff\]\s*\{"
        r"[^}]*white-space:\s*nowrap;",
        css,
        re.DOTALL,
    )
    assert not re.search(
        r"\.home-estimate__disclosure\s+a\s*\{[^}]*white-space:\s*nowrap;",
        css,
        re.DOTALL,
    )


@pytest.mark.asyncio
async def test_home_preview_and_plan_support_copy_is_localized_in_czech():
    """Catch English fallback copy leaking into the Czech preview experience."""
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("language", "cs")
        response = await client.get("/")

    page = BeautifulSoup(response.text, "html.parser")
    preview = page.select_one("[data-home-preview]")
    visible = page.select_one("main").get_text(" ", strip=True)

    for text in (
        "Sejděme se",
        "Někde tady",
        "Rychlý odhad",
        "Přibližně · jedním směrem",
        "Výchozí zastávky",
        "Přidejte výchozí zastávku a zobrazí se její dosah.",
        "bez zvoleného data",
        "bez odhadu",
        "Plánovat podle aktuálních časů DPP ↓",
        "Zvolte datum a čas, seřaďte místa setkání a zahrňte cestu zpět.",
    ):
        assert text in visible

    assert preview["data-updating"] == "Aktualizuji odhad…"
    assert preview.select_one("[data-preview-map]")["aria-label"] == (
        "Interaktivní mapa přibližného dosahu"
    )
    assert preview["data-duplicate"] == "Tato zastávka už je vybraná."
    assert preview["data-limit"] == (
        "Rychlý odhad podporuje nejvýše šest výchozích zastávek. "
        "Větší skupiny mohou pokračovat vytvořením plánu."
    )
    assert preview["data-failure"] == ("Rychlý odhad není dostupný. Plán můžete přesto vytvořit.")
    assert preview["data-coverage"] == (
        "Pro zastávku {stop} není odhad dostupný. Odeberte ji a pokračujte."
    )
    assert "Add a Prague stop" not in visible


@pytest.mark.asyncio
async def test_home_explains_invalid_preview_carry_over():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/?error=preview_stops_invalid")

    assert "Choose up to six Prague stops from the list." in response.text
    assert "No plan was created." in response.text


@pytest.mark.asyncio
async def test_new_session_starts_with_two_editable_empty_participant_slots():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        create_response = await client.post(
            "/session/create",
            data={"session_name": "Friday crew"},
            follow_redirects=False,
        )
        page = await client.get(create_response.headers["location"])

    workspace = BeautifulSoup(page.text, "html.parser")
    name_inputs = workspace.select("[data-participant-name-input]")
    assert [input_element.get("value") for input_element in name_inputs] == ["", ""]
    assert all(
        input_element.find_parent("form")["hx-trigger"]
        == "change target:[data-participant-name-input]"
        for input_element in name_inputs
    )
    assert workspace.select_one("[data-search-submit]").has_attr("disabled")
    assert "Name each participant." in workspace.get_text(" ", strip=True)


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
    assert 'hx-swap="innerHTML focus-scroll:false"' in response.text


@pytest.mark.asyncio
async def test_invite_button_copies_the_direct_session_url():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}")

    page = BeautifulSoup(response.text, "html.parser")
    invite_button = page.select_one("[data-invite-copy]")

    assert invite_button is not None
    assert invite_button["data-invite-url"] == f"/session/{session['code']}"


def test_mobile_stop_picker_stays_centered_in_the_viewport():
    css = Path("static/app.css").read_text()
    mobile_stop_picker = css.rsplit(".stop-picker {", 1)[1].split("}", 1)[0]

    assert "width: min(520px, calc(100vw - 1.5rem));" in mobile_stop_picker
    assert "margin: auto;" in mobile_stop_picker
    assert "margin: auto 0 0;" not in mobile_stop_picker


@pytest.mark.asyncio
async def test_participant_card_groups_status_and_remove_in_header_actions():
    """Keep card-level status and actions together instead of in the stop grid."""
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    await add_participant(app.state.db, session["code"], "Alice")
    await add_participant(app.state.db, session["code"], "Bob")
    participant = (await get_participants(app.state.db, session["code"]))[0]
    await add_participant_stops(
        app.state.db,
        session["code"],
        participant["id"],
        "A",
        "A",
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}")

    soup = BeautifulSoup(response.text, "html.parser")
    card = soup.select_one(".participant-row")
    actions = card.select_one(".participant-row__actions")

    assert actions is not None
    assert actions.select_one(".stop-save-status") is not None
    assert actions.select_one(".save-state--saved") is not None
    assert actions.select_one(".participant-remove") is not None
    assert card.select_one(".stop-form-row .save-state") is None


@pytest.mark.asyncio
async def test_session_search_requires_two_participants():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}")
    assert "data-search-submit disabled" in response.text
    assert "Add one more participant." in response.text


@pytest.mark.asyncio
async def test_stale_venue_expansion_cannot_overwrite_a_new_search_generation():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    data = saved_result_fixture()
    data["search_id"] = "old"
    await save_search_results(app.state.db, session["code"], data)
    saved = await get_search_results(app.state.db, session["code"])
    assert saved is not None
    await begin_search(app.state.db, session["code"], "new")
    data["pubs_by_stop"]["B"] = [{"name": "Stale venue"}]

    updated = await update_search_results_if_current(
        app.state.db,
        session["code"],
        data,
        search_id="old",
        created_at=saved["created_at"],
    )
    current = await get_search_results(app.state.db, session["code"])

    assert not updated
    assert current is not None
    assert current["data"]["pubs_by_stop"]["B"] == []


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

    assert "data-session-readiness" in response.text
    assert "data-search-submit" in response.text
    assert "data-search-submit disabled" in response.text
    assert "Add one more participant." in response.text


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
    assert "Each map location uses its nearest stop and adds an estimated walk" in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "label"),
    [
        ("minimize-worst-case", "Minimize longest journey"),
        ("minimize-total", "Minimize total journey"),
    ],
)
async def test_saved_results_render_the_persisted_search_method(method, label):
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    data = saved_result_fixture()
    data["search_method"] = method
    await save_search_results(app.state.db, session["code"], data)
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/results")
    assert label in response.text


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
    assert 'data-mobile-view="map"' in response.text


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
async def test_results_map_places_fixed_travel_time_legend_below_the_map():
    session = await create_session(app.state.db, "Friday crew", "Daniel")
    await save_search_results(app.state.db, session["code"], saved_result_fixture())
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/session/{session['code']}/results")

    page = BeautifulSoup(response.text, "html.parser")
    map_panel = page.select_one(".results-map-panel")
    map_root = map_panel.select_one("[data-results-map]")
    time_legend = map_panel.select_one(".travel-time-legend")
    marker_legend = map_panel.select_one(".map-marker-legend")

    assert page.select_one("[data-threshold]") is None
    assert time_legend.get_text(" ", strip=True).split() == [
        "up", "to", "20", "min", "21–35", "min", "36–50", "min",
        "51–65", "min", "over", "65", "min", "no", "estimate",
    ]
    panel_elements = list(map_panel.descendants)
    assert panel_elements.index(map_root) < panel_elements.index(time_legend)
    assert panel_elements.index(time_legend) < panel_elements.index(marker_legend)


@pytest.mark.asyncio
async def test_completed_search_results_are_not_constrained_by_the_progress_wrapper():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/static/app.css")

    progress_panel = response.text.split(".search-progress-panel {", 1)[1].split("}", 1)[0]
    progress_box = response.text.split(".progress-box {", 1)[1].split("}", 1)[0]

    assert "width: 100%;" in progress_panel
    assert "width: min(100%, 640px);" in progress_box
    assert "margin: 0 auto;" in progress_box


def test_each_persons_journey_details_use_a_separate_row():
    css = Path("static/app.css").read_text()
    travel_grid = css.split(".travel-grid {", 1)[1].split("}", 1)[0]
    travel_times = css.rsplit(".travel-times {", 1)[1].split("}", 1)[0]

    assert "grid-template-columns: 1fr;" in travel_grid
    assert "flex-wrap: wrap;" in travel_times
    assert "overflow-x: visible;" in travel_times


def test_ranked_stop_participant_summaries_wrap_without_clipping():
    css = Path("static/app.css").read_text()
    time_strip = css.split(".participant-time-strip {", 1)[1].split("}", 1)[0]

    assert "flex-wrap: wrap;" in time_strip
    assert "overflow-x: visible;" in time_strip


def test_mobile_results_stack_the_map_above_stop_details():
    css = Path("static/app.css").read_text()
    mobile_results = css.split("@media (max-width: 720px) {\n    #results-section", 1)[1].split(
        "/* Session history */", 1
    )[0]

    assert ".results-workspace {\n        display: flex;" in mobile_results
    assert ".results-mobile-views {\n        display: none;" in mobile_results
    assert ".results-map-panel {\n        order: 1;" in mobile_results
    assert ".results-rail {\n        order: 2;" in mobile_results
    assert "position: absolute;" not in mobile_results


def test_mobile_map_fills_the_results_frame_width():
    css = Path("static/app.css").read_text()
    mobile_results = css.split("@media (max-width: 720px) {\n    #results-section", 1)[1].split(
        "/* Session history */", 1
    )[0]

    assert ".results-map-panel {\n        order: 1;\n        width: 100%;" in mobile_results
    assert ".results-map-panel #map {\n        width: 100%;" in mobile_results


def test_home_headline_uses_readable_display_tracking():
    css = Path("static/app.css").read_text()
    hero_heading = css.split(".home-hero h1 {", 1)[1].split("}", 1)[0]

    assert "letter-spacing: -0.035em;" in hero_heading


def test_results_map_explains_every_marker_type():
    template = Path("templates/partials/results_table.html").read_text()

    assert 'class="map-marker-legend"' in template
    assert "Meeting points considered" in template
    assert "Nearby place" in template
    assert "Start stops" in template
    assert "Return stops" in template


def test_results_map_uses_role_colours_not_participant_identity_colours():
    script = Path("static/reachability-map.js").read_text()

    assert 'fillColor: "#fffefa"' in script
    assert '"#ff6658", "#17191c", 2' in script
    assert '"#2458df", "#17191c", 2' in script


@pytest.mark.asyncio
async def test_results_json_attributes_escape_names_without_losing_visible_text():
    fixture = saved_result_fixture()
    unsafe_name = 'O\'Reilly "Stop" <script>alert(1)</script>'
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
async def test_footer_does_not_link_to_the_private_repository():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    soup = BeautifulSoup(response.text, "html.parser")
    footer = soup.select_one(".site-footer")
    navigation = soup.select_one(".site-nav")

    assert footer is not None
    assert navigation is not None
    assert footer.find("a", href="https://www.hermandaniel.com") is not None
    assert footer.find("a", href="https://github.com/detrin/pub-finder") is None
    assert navigation.find("a", href="https://github.com/detrin/pub-finder") is None


@pytest.mark.asyncio
async def test_desktop_navigation_uses_a_non_disclosure_link_group():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")

    page = BeautifulSoup(response.text, "html.parser")
    desktop_links = page.select(".site-nav__links--desktop a")

    assert [link.get_text(strip=True) for link in desktop_links] == [
        "Home",
        "How it works",
        "Feedback",
    ]
    assert ".site-nav__links--desktop" in Path("static/app.css").read_text()


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
    assert "nearest stop" in response.text
    assert "5 km/h" in response.text
    assert "1,463" not in response.text
    content = BeautifulSoup(response.text, "html.parser").select_one(".technical-page__content")
    assert content is not None
    assert content.find("a", href="https://github.com/detrin/pub-finder") is None
    assert "Source:" not in content.get_text()


@pytest.mark.asyncio
async def test_how_it_works_separates_the_homepage_estimate_from_live_planning():
    """Catch documentation that makes the sessionless estimate sound live or date-specific."""
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/how-it-works")

    page = BeautifulSoup(response.text, "html.parser")
    section = page.select_one("#homepage-quick-estimate")
    assert section is not None
    assert page.select_one('.technical-page__toc a[href="#homepage-quick-estimate"]') is not None
    assert section.get_text(" ", strip=True) == (
        "Homepage quick estimate The homepage quick estimate uses precomputed typical one-way "
        "transit times and adds estimated walking from the nearest stop. It does not use a "
        "selected date, account for service changes, include a return trip, or call live DPP or "
        "Google services. Create a plan for date-specific journey queries and ranked meeting "
        "points."
    )


@pytest.mark.asyncio
async def test_how_it_works_localizes_the_homepage_method_note_in_czech():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("language", "cs")
        response = await client.get("/how-it-works")

    page = BeautifulSoup(response.text, "html.parser")
    section = page.select_one("#homepage-quick-estimate")
    toc_link = page.select_one('.technical-page__toc a[href="#homepage-quick-estimate"]')
    assert section is not None
    assert toc_link is not None
    assert toc_link.get_text(" ", strip=True) == "Rychlý odhad na úvodní stránce"
    assert section.get_text(" ", strip=True) == (
        "Rychlý odhad na úvodní stránce Rychlý odhad na úvodní stránce používá předem "
        "vypočítané obvyklé doby cest veřejnou dopravou jedním směrem a přidává odhad chůze "
        "od nejbližší zastávky. Nevyužívá zvolené datum, nezohledňuje změny v provozu, "
        "nezahrnuje cestu zpět ani nevolá aktuální služby DPP či Googlu. Pro dotazy na cesty "
        "k určitému datu a seřazená místa setkání vytvořte plán."
    )
    assert "Homepage quick estimate" not in section.get_text(" ", strip=True)


@pytest.mark.asyncio
async def test_feedback_uses_accessible_native_formspree_form():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/feedback")

    page = BeautifulSoup(response.text, "html.parser")
    form = page.select_one(
        'form.feedback-form[action="https://formspree.io/f/myegvedj"][method="post"]'
    )
    assert form is not None
    assert page.select_one("iframe") is None
    assert "docs.google.com/forms" not in response.text

    assert form.select_one("select") is None

    email = form.select_one('input[type="email"][name="email"]')
    assert email is not None
    assert not email.has_attr("required")

    session_url = form.select_one('input[type="url"][name="session_url"][maxlength="2048"]')
    assert session_url is not None
    assert not session_url.has_attr("required")

    rating_group = form.select_one('fieldset[aria-describedby="feedback-rating-hint"]')
    assert rating_group is not None
    rating_inputs = rating_group.select('input[type="radio"][name="rating"]')
    assert [radio.get("value") for radio in rating_inputs] == [
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
    ]
    assert all(not radio.has_attr("required") for radio in rating_inputs)
    assert all(not radio.has_attr("checked") for radio in rating_inputs)
    rating_options = rating_group.select(".feedback-form__rating-option")
    assert len(rating_options) == 6
    for radio, option in zip(rating_inputs, rating_options, strict=True):
        assert radio in option.select('input[type="radio"]')
        assert "visually-hidden" not in radio.get("class", [])
        assert option.select_one(f'label[for="{radio["id"]}"]') is not None

    assert form.select_one('textarea[name="message"][required][maxlength="4000"]') is not None
    assert form.select_one('button[type="submit"]') is not None

    for control_id in ("feedback-email", "feedback-session", "feedback-message"):
        assert form.select_one(f'label[for="{control_id}"]') is not None
        assert form.select_one(f"#{control_id}") is not None

    assert "sent to Formspree" in page.get_text(" ", strip=True)
    assert "sensitive personal information" in page.get_text(" ", strip=True)


@pytest.mark.asyncio
async def test_feedback_form_copy_is_localized_in_czech():
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("language", "cs")
        response = await client.get("/feedback")

    page = BeautifulSoup(response.text, "html.parser")
    assert page.select_one("h1").get_text(" ", strip=True) == "Napište, co se stalo"
    assert page.select_one('label[for="feedback-message"]').get_text(" ", strip=True) == (
        "Co se stalo nebo co by mělo fungovat lépe?"
    )
    assert page.select_one('label[for="feedback-session"]').get_text(" ", strip=True) == (
        "URL relace Nepovinné"
    )
    assert page.select_one("fieldset legend").get_text(" ", strip=True) == ("Hodnocení Nepovinné")
    assert page.select_one('button[type="submit"]').get_text(" ", strip=True) == (
        "Odeslat zpětnou vazbu"
    )
    assert "Google" not in page.get_text(" ", strip=True)


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


def test_dark_theme_accent_surfaces_use_contrasting_foregrounds():
    """Catch dark-theme ink leaking onto light accent surfaces."""
    css = Path("static/app.css").read_text()

    def luminance(value: str) -> float:
        channels = [int(value[index : index + 2], 16) / 255 for index in (1, 3, 5)]
        linear = [
            channel / 12.92 if channel <= 0.04045 else ((channel + 0.055) / 1.055) ** 2.4
            for channel in channels
        ]
        return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]

    def contrast(foreground: str, background: str) -> float:
        values = sorted((luminance(foreground), luminance(background)), reverse=True)
        return (values[0] + 0.05) / (values[1] + 0.05)

    for background in ("#FF7869", "#FFE071", "#6FD5A7", "#B7DDED"):
        assert contrast("#17191C", background) >= 4.5

    def declarations(selector: str) -> str:
        matches = list(re.finditer(rf"(?m)^[ \t]*{re.escape(selector)} \{{", css))
        assert matches, selector
        start = matches[-1].start()
        opening = css.index("{", start)
        return css[opening + 1 : css.index("}", opening)]

    accent_ink_selectors = (
        ".system-message--warning",
        ".system-message--warning h2,\n.system-message--warning p",
        ".system-message__action",
        ".system-message__action:hover",
        "button.outline",
        ".ranked-stop--selected",
        ".ranked-stop--selected .ranked-stop__toggle",
        ".venue-action",
        '.results-mobile-views button[aria-pressed="true"]',
        ".sticker",
        ".invite-participants li",
        ".workspace-error",
        ".participant-remove:hover",
        '.occasion-presets button[aria-pressed="true"]',
        ".home-estimate__badge",
        ".stop-picker__close",
        ".stop-picker__item:hover,\n.stop-picker__item:focus-visible",
        ".badge-ready",
        ".badge-waiting",
        ".rank-badge--gold",
    )
    for selector in accent_ink_selectors:
        assert "color: var(--accent-ink);" in declarations(selector), selector

    for selector in ('.participant-tabs button[aria-pressed="true"]', ".participant-initial"):
        assert "color: var(--participant-ink, var(--accent-ink));" in declarations(selector)


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
  addEventListener() {},
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
