import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import test from "node:test";

class FakeElement {
    constructor(dataset = {}) {
        this.dataset = { ...dataset };
        this.attributes = {};
        this.hidden = false;
        this.listeners = new Map();
        this.parent = null;
        this.selectorMap = new Map();
        this.selectorLists = new Map();
        this.textContent = "";
    }

    addEventListener(type, handler) {
        const handlers = this.listeners.get(type) ?? [];
        handlers.push(handler);
        this.listeners.set(type, handlers);
    }

    dispatch(type, target = this) {
        for (const handler of this.listeners.get(type) ?? []) {
            handler({ type, target, detail: { target, elt: target } });
        }
    }

    matches(selector) {
        return selector === "[data-results-root]" && this.dataset.resultsRoot === "true";
    }

    closest(selector) {
        if (this.matches(selector)) return this;
        return this.parent?.closest(selector) ?? null;
    }

    querySelector(selector) {
        return this.selectorMap.get(selector) ?? null;
    }

    querySelectorAll(selector) {
        return this.selectorLists.get(selector) ?? [];
    }

    setAttribute(name, value) {
        this.attributes[name] = String(value);
    }

    getAttribute(name) {
        return this.attributes[name] ?? null;
    }
}

function makeRoot(name = "B") {
    const root = new FakeElement({
        resultsRoot: "true",
        reachabilityUrl: "/session/code/reachability",
        selectedRank: "1",
        mobileView: "map",
    });
    const map = new FakeElement();
    const mapData = new FakeElement({
        stops: JSON.stringify([
            { name: "B", lat: 50.1, lon: 14.1 },
            { name: "C", lat: 50.2, lon: 14.2 },
        ]),
        venues: "[]",
        participants: "[]",
    });
    const error = new FakeElement();
    error.hidden = true;
    map.parent = root;
    mapData.parent = root;
    error.parent = root;

    const firstButton = new FakeElement({ rank: "1", stopName: name });
    const secondButton = new FakeElement({ rank: "2", stopName: "C" });
    firstButton.setAttribute("aria-expanded", "true");
    secondButton.setAttribute("aria-expanded", "false");
    firstButton.parent = root;
    secondButton.parent = root;
    const firstDetail = new FakeElement({ resultDetail: "1" });
    const secondDetail = new FakeElement({ resultDetail: "2" });
    firstDetail.parent = root;
    secondDetail.parent = root;
    secondDetail.hidden = true;
    const everyone = new FakeElement({ participantId: "" });
    const daniel = new FakeElement({ participantId: "7" });
    everyone.setAttribute("aria-pressed", "true");
    daniel.setAttribute("aria-pressed", "false");
    everyone.parent = root;
    daniel.parent = root;
    const threshold = new FakeElement();
    threshold.value = "35";
    threshold.parent = root;
    const output = new FakeElement();
    output.parent = root;
    const mapView = new FakeElement({ mobileViewTarget: "map" });
    const listView = new FakeElement({ mobileViewTarget: "list" });
    mapView.setAttribute("aria-pressed", "true");
    listView.setAttribute("aria-pressed", "false");
    mapView.parent = root;
    listView.parent = root;

    root.selectorMap.set("[data-results-map]", map);
    root.selectorMap.set("[data-map-data]", mapData);
    root.selectorMap.set("[data-reachability-error]", error);
    root.selectorMap.set('[data-result-detail="1"]', firstDetail);
    root.selectorMap.set('[data-result-detail="2"]', secondDetail);
    root.selectorMap.set("[data-threshold]", threshold);
    root.selectorMap.set("[data-threshold-value]", output);
    root.selectorLists.set("[data-rank]", [firstButton, secondButton]);
    root.selectorLists.set("[data-result-detail]", [firstDetail, secondDetail]);
    root.selectorLists.set("[data-participant-id]", [everyone, daniel]);
    root.selectorLists.set("[data-mobile-view-target]", [mapView, listView]);
    return {
        root,
        map,
        mapData,
        error,
        firstButton,
        secondButton,
        firstDetail,
        secondDetail,
        everyone,
        daniel,
        threshold,
        output,
        mapView,
        listView,
    };
}

async function loadResultsModule(createReachabilityMap) {
    const listeners = new Map();
    globalThis.document = {
        readyState: "loading",
        addEventListener(type, handler) {
            const handlers = listeners.get(type) ?? [];
            handlers.push(handler);
            listeners.set(type, handlers);
        },
        querySelector() { return null; },
    };
    globalThis.__createReachabilityMap = createReachabilityMap;
    const source = await readFile(new URL("../../static/results.js", import.meta.url), "utf8");
    const testableSource = source.replace(
        'import { createReachabilityMap } from "./reachability-map.js?v=2";',
        "const createReachabilityMap = globalThis.__createReachabilityMap;",
    );
    const moduleUrl = `data:text/javascript;base64,${Buffer.from(testableSource).toString("base64")}#${Math.random()}`;
    return { module: await import(moduleUrl), listeners };
}

function deferred() {
    let resolve;
    const promise = new Promise((done) => { resolve = done; });
    return { promise, resolve };
}

function fakeController() {
    return {
        destroyed: 0,
        participantCalls: [],
        resultCalls: [],
        thresholdCalls: [],
        venueCalls: [],
        destroy() { this.destroyed += 1; },
        setParticipant(value) { this.participantCalls.push(value); },
        setResults(value) { this.resultCalls.push(value); },
        setThreshold(value) { this.thresholdCalls.push(value); },
        setVenues(value) { this.venueCalls.push(value); },
    };
}

function renderedReachabilityWarningTag() {
    const script = String.raw`
from types import SimpleNamespace
from routers.session import templates

results = SimpleNamespace(
    columns=["Target Stop", "Worst Case Minutes", "Total Minutes"],
    rows=lambda **_kwargs: [],
)
print(templates.env.get_template("partials/results_table.html").render(
    results=results,
    session_code="code",
    participant_snapshot=[],
    search_direction="round-trip",
    pubs_by_stop={},
    pub_search_stop_names=set(),
    stops_json="[]",
    pubs_json="[]",
    participant_snapshot_json="[]",
    warning=None,
))
`;
    const html = execFileSync("uv", ["run", "python", "-c", script], {
        cwd: process.cwd(),
        encoding: "utf8",
    });
    return html.match(/<section[^>]*data-system-message="warning"[^>]*>/)?.[0] ?? "";
}

test("new result initialization aborts and destroys a stale asynchronous controller", async () => {
    const first = deferred();
    const second = deferred();
    const calls = [];
    const { module } = await loadResultsModule((map, options) => {
        calls.push({ map, options });
        return calls.length === 1 ? first.promise : second.promise;
    });
    const firstRoot = makeRoot("First");
    const secondRoot = makeRoot("Second");
    const firstInit = module.initResultsUi(firstRoot.root);
    const secondInit = module.initResultsUi(secondRoot.root);

    assert.equal(calls[0].options.signal.aborted, true);
    const newestController = fakeController();
    second.resolve(newestController);
    await secondInit;
    const staleController = fakeController();
    first.resolve(staleController);
    await firstInit;

    assert.equal(staleController.destroyed, 1);
    assert.equal(newestController.destroyed, 0);
    const resultCallsBeforeStaleClick = newestController.resultCalls.length;
    firstRoot.secondButton.dispatch("click");
    assert.equal(newestController.resultCalls.length, resultCallsBeforeStaleClick);
});

test("rank, participant, threshold, and mobile handlers use the assigned controller", async () => {
    const assigned = fakeController();
    let createCalls = 0;
    const { module } = await loadResultsModule(async () => {
        createCalls += 1;
        return assigned;
    });
    const fixture = makeRoot();
    await module.initResultsUi(fixture.root);
    await module.initResultsUi(fixture.root);

    assert.equal(createCalls, 1);
    fixture.secondButton.dispatch("click");
    assert.equal(fixture.root.dataset.selectedRank, "2");
    assert.equal(fixture.firstButton.getAttribute("aria-expanded"), "false");
    assert.equal(fixture.secondButton.getAttribute("aria-expanded"), "true");
    assert.equal(fixture.firstDetail.hidden, true);
    assert.equal(fixture.secondDetail.hidden, false);
    assert.equal(assigned.resultCalls.at(-1)[1].selected, true);

    fixture.daniel.dispatch("click");
    assert.equal(assigned.participantCalls.at(-1), 7);
    assert.equal(fixture.daniel.getAttribute("aria-pressed"), "true");
    fixture.threshold.value = "50";
    fixture.threshold.dispatch("input");
    assert.equal(assigned.thresholdCalls.at(-1), 50);
    assert.equal(fixture.output.textContent, "50 min");
    fixture.listView.dispatch("click");
    assert.equal(fixture.root.dataset.mobileView, "list");
    assert.equal(fixture.listView.getAttribute("aria-pressed"), "true");
});

test("rendered reachability warning is revealed on a reachability error", async () => {
    const warningTag = renderedReachabilityWarningTag();
    assert.match(warningTag, /data-reachability-error/);
    assert.match(warningTag, /hidden/);

    const assigned = fakeController();
    const { module } = await loadResultsModule(async () => assigned);
    const fixture = makeRoot();
    fixture.root.selectorMap.delete("[data-reachability-error]");
    if (/data-reachability-error/.test(warningTag)) {
        fixture.root.selectorMap.set("[data-reachability-error]", fixture.error);
    }

    await module.initResultsUi(fixture.root);
    fixture.map.dispatch("reachability:error");

    assert.equal(fixture.error.hidden, false);
});

test("state changed while map creation is pending is synchronized after assignment", async () => {
    const creation = deferred();
    const { module } = await loadResultsModule(() => creation.promise);
    const fixture = makeRoot();
    const initialization = module.initResultsUi(fixture.root);

    fixture.mapData.dataset.venues = JSON.stringify([
        { name: "Late Cafe", lat: 50.15, lon: 14.15 },
    ]);
    fixture.secondButton.dispatch("click");
    fixture.daniel.dispatch("click");
    fixture.threshold.value = "55";
    fixture.threshold.dispatch("input");

    const assigned = fakeController();
    creation.resolve(assigned);
    await initialization;

    assert.equal(assigned.resultCalls.at(-1)[1].selected, true);
    assert.deepEqual(assigned.venueCalls.at(-1), [
        { name: "Late Cafe", lat: 50.15, lon: 14.15 },
    ]);
    assert.equal(assigned.participantCalls.at(-1), 7);
    assert.equal(assigned.thresholdCalls.at(-1), 55);
});

test("venue out-of-band data updates markers without rebuilding the map", async () => {
    const assigned = fakeController();
    let createCalls = 0;
    const { module, listeners } = await loadResultsModule(async () => {
        createCalls += 1;
        return assigned;
    });
    const fixture = makeRoot();
    await module.initResultsUi(fixture.root);
    fixture.mapData.dataset.venues = JSON.stringify([
        { name: 'Cafe "<script>"', lat: 50.15, lon: 14.15 },
    ]);
    for (const handler of listeners.get("htmx:oobAfterSwap") ?? []) {
        handler({ detail: { target: fixture.mapData, elt: fixture.mapData } });
    }

    assert.equal(createCalls, 1);
    assert.deepEqual(assigned.venueCalls.at(-1), [
        { name: 'Cafe "<script>"', lat: 50.15, lon: 14.15 },
    ]);
});

test("replacing results with a non-map state destroys the detached controller", async () => {
    const assigned = fakeController();
    const { module, listeners } = await loadResultsModule(async () => assigned);
    const fixture = makeRoot();
    fixture.root.isConnected = true;
    await module.initResultsUi(fixture.root);
    fixture.root.isConnected = false;
    const replacement = new FakeElement();
    replacement.id = "results-section";
    for (const handler of listeners.get("htmx:afterSwap") ?? []) {
        handler({ detail: { target: replacement } });
    }
    await Promise.resolve();

    assert.equal(assigned.destroyed, 1);
});
