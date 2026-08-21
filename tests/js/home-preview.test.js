import assert from "node:assert/strict";
import test from "node:test";

class FakeElement {
    constructor(tagName, ownerDocument) {
        this.tagName = tagName.toUpperCase();
        this.ownerDocument = ownerDocument;
        this.attributes = new Map();
        this.children = [];
        this.dataset = {};
        this.events = new Map();
        this.hidden = false;
        this.parentNode = null;
        this.textContent = "";
        this.value = "";
    }

    addEventListener(type, handler) {
        const handlers = this.events.get(type) ?? [];
        handlers.push(handler);
        this.events.set(type, handlers);
    }

    appendChild(child) {
        child.parentNode = this;
        this.children.push(child);
        return child;
    }

    dispatch(type, event = {}) {
        const dispatched = {
            preventDefault() { this.defaultPrevented = true; },
            target: this,
            ...event,
        };
        for (const handler of this.events.get(type) ?? []) handler(dispatched);
        return dispatched;
    }

    focus() {
        this.ownerDocument.activeElement = this;
    }

    remove() {
        if (!this.parentNode) return;
        this.parentNode.children = this.parentNode.children.filter((child) => child !== this);
        this.parentNode = null;
    }

    removeAttribute(name) {
        this.attributes.delete(name);
    }

    removeEventListener(type, handler) {
        this.events.set(type, (this.events.get(type) ?? []).filter((item) => item !== handler));
    }

    replaceChildren(...children) {
        for (const child of this.children) child.parentNode = null;
        this.children = [];
        for (const child of children) this.appendChild(child);
    }

    setAttribute(name, value) {
        this.attributes.set(name, String(value));
    }

    getAttribute(name) {
        return this.attributes.get(name) ?? null;
    }
}

function completePayload(origins) {
    return {
        direction: "there-only",
        participants: origins.map((origin, index) => ({
            id: index + 1,
            name: origin,
            color: "#ff6658",
            start_stop: origin,
            end_stop: "",
        })),
        stops: origins.map((origin, index) => ({
            name: origin,
            lat: 50 + index / 100,
            lon: 14 + index / 100,
            participant_minutes: origins.map((_, minuteIndex) => 20 + minuteIndex),
            group_max_minutes: 20 + origins.length,
        })),
    };
}

function createPreviewHarness(stops) {
    const hooks = new Map();
    const document = {
        activeElement: null,
        createElement(tagName) {
            return new FakeElement(tagName, document);
        },
        querySelector(selector) {
            return hooks.get(selector) ?? null;
        },
    };
    const root = new FakeElement("section", document);
    root.dataset = {
        stops: JSON.stringify(stops),
        emptyHeading: "Add a starting stop to see its reach.",
        oneHeading: "Approximate reach from {stop}",
        groupHeading: "Shared reach for {count} starting points",
        onePrompt: "Add another stop to see where everyone can reach.",
        groupPrompt: "Colour shows the longest estimated journey among the selected starts.",
        updating: "Updating estimate…",
        failure: "The quick estimate is unavailable. You can still create a plan.",
        duplicate: "That stop is already selected.",
        limit: "The quick estimate supports up to six starting stops.",
        invalid: "Choose a stop from the Prague stop list.",
        remove: "Remove {stop}",
        carry: "{count} selected starts will be added to this plan.",
    };
    const search = document.createElement("input");
    const options = document.createElement("ul");
    const selections = document.createElement("ul");
    const status = document.createElement("p");
    const heading = document.createElement("h2");
    const prompt = document.createElement("p");
    const mapRoot = document.createElement("div");
    const hiddenFields = document.createElement("div");
    const carryStatus = document.createElement("p");
    const planName = document.createElement("input");
    const handoff = document.createElement("a");
    const rootHooks = new Map([
        ["[data-preview-search]", search],
        ["[data-preview-options]", options],
        ["[data-preview-selections]", selections],
        ["[data-preview-status]", status],
        ["[data-preview-heading]", heading],
        ["[data-preview-prompt]", prompt],
        ["[data-preview-map]", mapRoot],
        ["[data-preview-handoff]", handoff],
    ]);
    root.querySelector = (selector) => rootHooks.get(selector) ?? null;
    root.matches = (selector) => selector === "[data-home-preview]";
    hooks.set("[data-preview-hidden-fields]", hiddenFields);
    hooks.set("[data-preview-carry-status]", carryStatus);
    hooks.set("#session-name", planName);

    const map = {
        clearCount: 0,
        destroyCount: 0,
        payloads: [],
        clearPayload() { this.clearCount += 1; },
        destroy() { this.destroyCount += 1; },
        setPayload(payload) { this.payloads.push(payload); },
    };
    let createMapCount = 0;
    const createMap = async (target, optionsArgument) => {
        createMapCount += 1;
        assert.equal(target, mapRoot);
        assert.deepEqual(optionsArgument.payload, { participants: [], stops: [] });
        return map;
    };

    return {
        carryStatus,
        createMap,
        document,
        handoff,
        heading,
        hiddenFields,
        map,
        mapRoot,
        options,
        planName,
        prompt,
        root,
        search,
        selections,
        status,
        get createMapCount() { return createMapCount; },
        get hiddenInputs() { return hiddenFields.children; },
        async flush() {
            await new Promise((resolve) => setImmediate(resolve));
        },
    };
}

function deferredFetches() {
    const requests = [];
    return {
        requests,
        fetch(url, options) {
            return new Promise((resolve, reject) => {
                requests.push({ options, reject, resolve, url });
            });
        },
        reject(index, error) {
            requests[index].reject(error);
        },
        resolve(index, payload, { ok = true, status = 200 } = {}) {
            requests[index].resolve({ ok, status, json: async () => payload });
        },
    };
}

globalThis.document = {
    readyState: "loading",
    addEventListener() {},
};

const { createHomePreview, initHomePreview } = await import("../../static/home-preview.js");

test("first stop renders individual reach and second stop renders shared reach", async () => {
    const harness = createPreviewHarness(["Anděl", "Dejvická"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    assert.equal(controller.addOrigin("Anděl"), true);
    requests.resolve(0, completePayload(["Anděl"]));
    await harness.flush();
    assert.equal(harness.heading.textContent, "Approximate reach from Anděl");
    assert.equal(harness.prompt.textContent, "Add another stop to see where everyone can reach.");
    assert.equal(harness.root.dataset.previewState, "ready");

    assert.equal(controller.addOrigin("Dejvická"), true);
    assert.equal(harness.root.dataset.previewState, "updating");
    assert.equal(harness.status.textContent, "Updating estimate…");
    requests.resolve(1, completePayload(["Anděl", "Dejvická"]));
    await harness.flush();
    assert.equal(harness.heading.textContent, "Shared reach for 2 starting points");
    assert.equal(harness.prompt.textContent, "Colour shows the longest estimated journey among the selected starts.");
});

test("only canonical unique stops are selected and duplicates issue no request", async () => {
    const harness = createPreviewHarness(["Anděl"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    assert.equal(controller.addOrigin(" Anděl "), false);
    assert.equal(harness.status.textContent, "Choose a stop from the Prague stop list.");
    assert.equal(controller.addOrigin("Anděl"), true);
    assert.equal(controller.addOrigin("Anděl"), false);
    assert.equal(harness.status.textContent, "That stop is already selected.");
    assert.equal(requests.requests.length, 1);
});

test("a seventh unique stop is rejected without recalculation", async () => {
    const stops = ["A", "B", "C", "D", "E", "F", "G"];
    const harness = createPreviewHarness(stops);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    for (const stop of stops.slice(0, 6)) assert.equal(controller.addOrigin(stop), true);
    assert.equal(controller.addOrigin("G"), false);
    assert.equal(harness.status.textContent, "The quick estimate supports up to six starting stops.");
    assert.equal(requests.requests.length, 6);
    assert.equal(JSON.parse(requests.requests.at(-1).options.body).origins.length, 6);
});

test("chips use participant letters, removal recalculates, and hidden fields repeat preview_stops", async () => {
    const harness = createPreviewHarness(["Anděl", "Dejvická"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    controller.addOrigin("Anděl");
    controller.addOrigin("Dejvická");
    assert.equal(harness.selections.children[0].children[0].textContent, "A");
    assert.equal(harness.selections.children[1].children[0].textContent, "B");
    assert.deepEqual(
        harness.hiddenInputs.map((input) => [input.getAttribute("name"), input.value]),
        [["preview_stops", "Anděl"], ["preview_stops", "Dejvická"]],
    );
    assert.equal(harness.carryStatus.hidden, false);
    assert.equal(harness.carryStatus.textContent, "2 selected starts will be added to this plan.");

    const removeAndel = harness.selections.children[0].children.at(-1);
    assert.equal(removeAndel.getAttribute("aria-label"), "Remove Anděl");
    removeAndel.dispatch("click");
    assert.deepEqual(JSON.parse(requests.requests.at(-1).options.body), { origins: ["Dejvická"] });
    assert.deepEqual(harness.hiddenInputs.map((input) => input.value), ["Dejvická"]);
    assert.equal(harness.selections.children[0].children[0].textContent, "A");
});

test("combobox prioritizes prefix matches and supports keyboard selection", async () => {
    const matching = Array.from({ length: 55 }, (_, index) => `Elsewhere ${index} Dej`);
    const harness = createPreviewHarness(["Nádraží Dejvice", ...matching, "Dejvická"]);
    const requests = deferredFetches();
    await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    harness.search.value = "dej";
    harness.search.dispatch("input");
    assert.equal(harness.options.hidden, false);
    assert.equal(harness.options.children.length, 50);
    assert.equal(harness.options.children[0].textContent, "Dejvická");
    assert.equal(harness.search.getAttribute("aria-expanded"), "true");

    const arrowEvent = harness.search.dispatch("keydown", { key: "ArrowDown" });
    assert.equal(arrowEvent.defaultPrevented, true);
    assert.equal(harness.options.children[0].getAttribute("aria-selected"), "true");
    harness.search.dispatch("keydown", { key: "Enter" });
    assert.equal(harness.selections.children[0].children[1].textContent, "Dejvická");
    assert.equal(requests.requests.length, 1);
    assert.equal(harness.options.hidden, true);
});

test("Escape dismisses combobox results without changing selections", async () => {
    const harness = createPreviewHarness(["Anděl"]);
    const requests = deferredFetches();
    await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    harness.search.value = "a";
    harness.search.dispatch("input");
    const event = harness.search.dispatch("keydown", { key: "Escape" });
    assert.equal(event.defaultPrevented, true);
    assert.equal(harness.options.hidden, true);
    assert.equal(harness.search.getAttribute("aria-expanded"), "false");
    assert.equal(requests.requests.length, 0);
});

test("newest response wins even when an aborted fetch still resolves", async () => {
    const harness = createPreviewHarness(["Anděl", "Dejvická"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    controller.addOrigin("Anděl");
    controller.addOrigin("Dejvická");
    assert.equal(requests.requests[0].options.signal.aborted, true);
    requests.resolve(1, completePayload(["Anděl", "Dejvická"]));
    await harness.flush();
    requests.resolve(0, completePayload(["Anděl"]));
    await harness.flush();

    assert.equal(harness.map.payloads.length, 1);
    assert.deepEqual(harness.map.payloads[0].participants.map((person) => person.marker_label), ["A", "B"]);
    assert.equal(harness.heading.textContent, "Shared reach for 2 starting points");
});

test("aborted requests are silent", async () => {
    const harness = createPreviewHarness(["Anděl", "Dejvická"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    controller.addOrigin("Anděl");
    controller.addOrigin("Dejvická");
    const abortError = new Error("aborted");
    abortError.name = "AbortError";
    requests.reject(0, abortError);
    await harness.flush();

    assert.equal(harness.status.textContent, "Updating estimate…");
    assert.equal(harness.map.clearCount, 0);
});

test("a genuine failure clears stale map data but keeps selections and plan fields", async () => {
    const harness = createPreviewHarness(["Anděl", "Dejvická"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    controller.addOrigin("Anděl");
    requests.resolve(0, completePayload(["Anděl"]));
    await harness.flush();
    controller.addOrigin("Dejvická");
    requests.resolve(1, {}, { ok: false, status: 503 });
    await harness.flush();

    assert.equal(harness.map.clearCount, 1);
    assert.equal(harness.root.dataset.previewState, "failure");
    assert.equal(harness.status.textContent, "The quick estimate is unavailable. You can still create a plan.");
    assert.equal(harness.selections.children.length, 2);
    assert.equal(harness.hiddenInputs.length, 2);
});

test("handoff focuses the existing plan field", async () => {
    const harness = createPreviewHarness(["Anděl"]);
    await createHomePreview(harness.root, {
        fetch: async () => ({ ok: true, json: async () => completePayload(["Anděl"]) }),
        createMap: harness.createMap,
    });

    const event = harness.handoff.dispatch("click");
    assert.equal(event.defaultPrevented, true);
    assert.equal(harness.document.activeElement, harness.planName);
});

test("page initialization is idempotent for the same preview root", async () => {
    const harness = createPreviewHarness(["Anděl"]);
    const target = {
        querySelectorAll(selector) {
            return selector === "[data-home-preview]" ? [harness.root] : [];
        },
    };
    const dependencies = {
        fetch: async () => ({ ok: true, json: async () => completePayload(["Anděl"]) }),
        createMap: harness.createMap,
    };

    await Promise.all([
        initHomePreview(target, dependencies),
        initHomePreview(target, dependencies),
    ]);

    assert.equal(harness.createMapCount, 1);
    assert.equal(harness.search.events.get("input").length, 1);
});
