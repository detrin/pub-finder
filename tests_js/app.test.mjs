import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const appSource = readFileSync(new URL("../static/app.js", import.meta.url), "utf8");

function element() {
    return {
        appendChild() {},
        removeAttribute() {},
        setAttribute() {},
        style: {},
    };
}

test("SSE results initialize a replaced map even when the data is unchanged", () => {
    const documentEvents = new Map();
    let mapElement = element();
    let mapInitializations = 0;
    let mapRemovals = 0;
    let markerCreations = 0;

    const mapData = {
        dataset: {
            stops: JSON.stringify([{ lat: 50.08, lon: 14.43, name: "Muzeum" }]),
            pubs: "[]",
            participants: "[]",
        },
    };
    const shareLink = { style: { display: "none" } };
    const resultsSection = {
        querySelector(selector) {
            return selector === "#map-data" ? mapData : null;
        },
    };
    const searchProgress = {
        id: "search-progress",
        closest(selector) {
            return selector === "#results-section" ? resultsSection : null;
        },
    };

    const document = {
        activeElement: null,
        body: { addEventListener() {} },
        documentElement: {
            style: {},
            setAttribute() {},
        },
        addEventListener(name, handler) {
            documentEvents.set(name, handler);
        },
        createElement: element,
        createTextNode(text) {
            return { textContent: text };
        },
        getElementById(id) {
            return {
                map: mapElement,
                "map-data": mapData,
                "share-results-link": shareLink,
            }[id] ?? null;
        },
        querySelector() {
            return null;
        },
        querySelectorAll() {
            return [];
        },
    };

    const L = {
        divIcon(options) {
            return options;
        },
        layerGroup() {
            return {
                addLayer() {},
                addTo() {
                    return this;
                },
            };
        },
        map() {
            mapInitializations += 1;
            const instance = {
                fitBounds() {},
                remove() {
                    mapRemovals += 1;
                },
                setView() {
                    return instance;
                },
            };
            return instance;
        },
        marker() {
            markerCreations += 1;
            return { bindPopup() {} };
        },
        tileLayer() {
            return { addTo() {} };
        },
    };

    const context = vm.createContext({
        L,
        URL,
        document,
        localStorage: {
            getItem() {
                return null;
            },
            setItem() {},
        },
        navigator: { clipboard: { writeText: async () => {} } },
        setTimeout,
        window: {
            location: { origin: "https://pub-finder.example" },
            matchMedia() {
                return { matches: false };
            },
        },
    });
    vm.runInContext(appSource, context);

    const onSseMessage = documentEvents.get("htmx:sseMessage");
    assert.equal(typeof onSseMessage, "function");

    onSseMessage({ detail: { elt: searchProgress } });
    assert.equal(mapInitializations, 1);
    assert.equal(markerCreations, 1);
    assert.equal(shareLink.style.display, "");

    onSseMessage({ detail: { elt: searchProgress } });
    assert.equal(mapInitializations, 1, "a duplicate event must not recreate the same map");

    mapElement = element();
    onSseMessage({ detail: { elt: searchProgress } });
    assert.equal(mapInitializations, 2, "replaced result markup must receive a new map");
    assert.equal(markerCreations, 2);
    assert.equal(mapRemovals, 1);
});
