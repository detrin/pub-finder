import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const appSource = readFileSync(new URL("../static/app.js", import.meta.url), "utf8");

function element(tagName = "div") {
    return {
        children: [],
        tagName: tagName.toUpperCase(),
        appendChild(child) {
            this.children.push(child);
        },
        removeAttribute() {},
        setAttribute() {},
        style: {},
    };
}

test("disabling return includes the existing destination in the autosave request", () => {
    const documentEvents = new Map();
    const endStop = { disabled: true, name: "end_stop", value: "Florenc" };
    const form = {
        querySelector(selector) {
            return selector === "[name=end_stop]" ? endStop : null;
        },
    };
    const checkbox = {
        checked: false,
        closest(selector) {
            if (selector === "[data-same-start-end]") return this;
            if (selector === "form") return form;
            return null;
        },
    };
    const document = {
        activeElement: null,
        body: { addEventListener() {} },
        documentElement: { style: {}, setAttribute() {} },
        addEventListener(name, handler, capture = false) {
            const handlers = documentEvents.get(name) ?? [];
            handlers.push({ capture, handler });
            documentEvents.set(name, handlers);
        },
        getElementById() {
            return null;
        },
        querySelector() {
            return null;
        },
        querySelectorAll() {
            return [];
        },
    };
    const context = vm.createContext({
        URL,
        document,
        localStorage: { getItem() { return null; }, setItem() {} },
        navigator: { clipboard: { writeText: async () => {} } },
        setTimeout,
        window: { location: { origin: "https://pub-finder.example" }, matchMedia() { return { matches: false }; } },
    });
    vm.runInContext(appSource, context);

    for (const listener of (documentEvents.get("change") ?? []).filter(({ capture }) => capture)) {
        listener.handler({ target: checkbox });
    }
    const requestData = endStop.disabled ? {} : { [endStop.name]: endStop.value };

    assert.deepEqual(requestData, { end_stop: "Florenc" });
});

test("stop selection targets the replacement participant form after an HTMX swap", () => {
    const documentEvents = new Map();
    const dialogEvents = new Map();
    let currentForms = [];
    let replacementChanges = 0;

    const searchInput = {
        addEventListener() {},
        focus() {},
        value: "",
    };
    const stopList = {
        children: [],
        set innerHTML(value) {
            this.children = [];
        },
        appendChild(child) {
            this.children.push(child);
        },
        querySelector(selector) {
            return selector === ".stop-picker__item" ? this.children[0] ?? null : null;
        },
    };
    const dialog = {
        addEventListener(name, handler) {
            dialogEvents.set(name, handler);
        },
        close() {
            dialogEvents.get("close")?.();
        },
        querySelector(selector) {
            return {
                ".stop-picker__search": searchInput,
                ".stop-picker__list": stopList,
            }[selector] ?? null;
        },
        showModal() {},
    };

    function participantForm(input) {
        const participantId = { value: "participant-1" };
        return {
            querySelector(selector) {
                if (selector === "[name=participant_id]") return participantId;
                if (selector === "[name=start_stop]") return input;
                return null;
            },
        };
    }

    const originalInput = {
        disabled: false,
        isConnected: true,
        name: "start_stop",
        value: "",
        closest(selector) {
            if (selector === "[data-stop-input]") return this;
            if (selector === "form") return originalForm;
            return null;
        },
        dispatchEvent() {
            throw new Error("selection was dispatched to the detached input");
        },
    };
    const originalForm = participantForm(originalInput);
    const replacementInput = {
        disabled: false,
        isConnected: true,
        name: "start_stop",
        value: "",
        dispatchEvent(event) {
            assert.equal(event.type, "change");
            assert.equal(event.bubbles, true);
            replacementChanges += 1;
        },
    };
    const replacementForm = participantForm(replacementInput);
    currentForms = [originalForm];

    const document = {
        activeElement: null,
        body: { addEventListener() {}, style: {} },
        documentElement: { style: {}, setAttribute() {} },
        addEventListener(name, handler) {
            const handlers = documentEvents.get(name) ?? [];
            handlers.push(handler);
            documentEvents.set(name, handlers);
        },
        createElement() {
            return {
                addEventListener(name, handler) {
                    this[name] = handler;
                },
            };
        },
        getElementById(id) {
            return {
                "all-stops-data": { dataset: { stops: '["Muzeum"]' } },
                "stop-picker-dialog": dialog,
            }[id] ?? null;
        },
        querySelector() {
            return null;
        },
        querySelectorAll(selector) {
            return selector === "form.stop-form" ? currentForms : [];
        },
    };
    const context = vm.createContext({
        Event,
        URL,
        document,
        localStorage: { getItem() { return null; }, setItem() {} },
        navigator: { clipboard: { writeText: async () => {} } },
        setTimeout,
        window: {
            location: { origin: "https://pub-finder.example" },
            matchMedia() { return { matches: false }; },
        },
    });
    vm.runInContext(appSource, context);

    for (const onClick of documentEvents.get("click") ?? []) {
        onClick({ preventDefault() {}, target: originalInput });
    }

    originalInput.isConnected = false;
    currentForms = [replacementForm];
    stopList.children[0].click();

    assert.equal(originalInput.value, "");
    assert.equal(replacementInput.value, "Muzeum");
    assert.equal(replacementChanges, 1);
});

test("SSE results initialize a replaced map even when the data is unchanged", () => {
    const documentEvents = new Map();
    let mapElement = element();
    let mapInitializations = 0;
    let mapRemovals = 0;
    let markerCreations = 0;
    const popupContents = [];
    const unsafeName = '<img src=x onerror="globalThis.compromised=true">';

    const mapData = {
        dataset: {
            stops: JSON.stringify([{ lat: 50.08, lon: 14.43, name: "Muzeum" }]),
            pubs: JSON.stringify([
                {
                    lat: 50.081,
                    lon: 14.431,
                    name: unsafeName,
                    rating: 4.5,
                    rating_count: 10,
                    stop: "Muzeum",
                    url: "javascript:alert(1)",
                },
            ]),
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
        createElement(tagName) {
            return element(tagName);
        },
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
            return {
                bindPopup(content) {
                    popupContents.push(content);
                },
            };
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
    assert.equal(markerCreations, 2);
    assert.equal(shareLink.style.display, "");
    const unsafePopup = popupContents[1];
    assert.equal(unsafePopup.children[0].textContent, unsafeName);
    assert.equal(unsafePopup.children.some((child) => child.tagName === "A"), false);
    assert.equal(context.compromised, undefined);

    onSseMessage({ detail: { elt: searchProgress } });
    assert.equal(mapInitializations, 1, "a duplicate event must not recreate the same map");

    mapElement = element();
    onSseMessage({ detail: { elt: searchProgress } });
    assert.equal(mapInitializations, 2, "replaced result markup must receive a new map");
    assert.equal(markerCreations, 4);
    assert.equal(mapRemovals, 1);
});
