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
        this._textContent = "";
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
            currentTarget: this,
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
        this._textContent = "";
        for (const child of children) this.appendChild(child);
    }

    setAttribute(name, value) {
        this.attributes.set(name, String(value));
    }

    getAttribute(name) {
        return this.attributes.get(name) ?? null;
    }

    get textContent() {
        return this._textContent + this.children.map((child) => child.textContent).join("");
    }

    set textContent(value) {
        for (const child of this.children) child.parentNode = null;
        this.children = [];
        this._textContent = String(value);
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
        coverage: "No estimate is available from {stop}. Remove it to continue.",
        duplicate: "That stop is already selected.",
        limit: "The quick estimate supports up to six starting stops. For larger groups, start a plan.",
        invalid: "Choose a stop from the Prague stop list.",
        remove: "Remove {stop}",
        carry: "{count} selected starts will be added to this plan.",
    };
    const search = document.createElement("input");
    search.setAttribute("list", "home-stop-suggestions");
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
        clearFieldCount: 0,
        clearPayloadCount: 0,
        destroyCount: 0,
        markers: [],
        payloads: [],
        clearField() { this.clearFieldCount += 1; },
        clearPayload() {
            this.clearPayloadCount += 1;
            this.markers = [];
        },
        destroy() { this.destroyCount += 1; },
        setPayload(payload) {
            this.payloads.push(payload);
            const renderedStops = new Set(payload.stops.map((stop) => stop.name));
            this.markers = payload.participants
                .filter((participant) => renderedStops.has(participant.start_stop))
                .map((participant) => [participant.marker_label, participant.start_stop]);
        },
    };
    let createMapCount = 0;
    let mapOptions = null;
    const createMap = async (target, optionsArgument) => {
        createMapCount += 1;
        assert.equal(target, mapRoot);
        assert.deepEqual(optionsArgument.payload, { participants: [], stops: [] });
        mapOptions = optionsArgument;
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
        get mapOptions() { return mapOptions; },
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

test("successful takeover disables native suggestions and creates a decorative map", async () => {
    const harness = createPreviewHarness(["Anděl"]);
    await createHomePreview(harness.root, {
        fetch: async () => {},
        createMap: harness.createMap,
    });

    assert.equal(harness.search.getAttribute("list"), null);
    assert.equal(harness.mapOptions.interactive, false);
});

test("failed takeover leaves native stop suggestions available", async () => {
    const harness = createPreviewHarness(["Anděl"]);

    await assert.rejects(
        createHomePreview(harness.root, {
            fetch: async () => {},
            createMap: async () => { throw new Error("map unavailable"); },
        }),
        { message: "map unavailable" },
    );

    assert.equal(harness.search.getAttribute("list"), "home-stop-suggestions");
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
    assert.equal(
        harness.status.textContent,
        "The quick estimate supports up to six starting stops. For larger groups, start a plan.",
    );
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
    assert.equal(harness.map.clearFieldCount, 0);
    assert.equal(harness.map.clearPayloadCount, 0);
});

test("a genuine failure clears only stale field data and keeps selections and origin markers", async () => {
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

    assert.equal(harness.map.clearFieldCount, 1);
    assert.equal(harness.map.clearPayloadCount, 0);
    assert.equal(harness.root.dataset.previewState, "failure");
    assert.equal(harness.status.textContent, "The quick estimate is unavailable. You can still create a plan.");
    assert.equal(harness.selections.children.length, 2);
    assert.equal(harness.hiddenInputs.length, 2);
});

test("an add failure redraws markers and heading for the current A and B selection", async () => {
    const harness = createPreviewHarness(["Anděl", "Dejvická"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });
    const oneOriginPayload = completePayload(["Anděl"]);
    oneOriginPayload.stops.push({
        name: "Dejvická",
        lat: 50.1,
        lon: 14.1,
        participant_minutes: [25],
        group_max_minutes: 25,
    });

    controller.addOrigin("Anděl");
    requests.resolve(0, oneOriginPayload);
    await harness.flush();
    controller.addOrigin("Dejvická");
    requests.resolve(1, {}, { ok: false, status: 503 });
    await harness.flush();

    assert.deepEqual(harness.map.markers, [["A", "Anděl"], ["B", "Dejvická"]]);
    assert.equal(harness.heading.textContent, "Shared reach for 2 starting points");
    assert.equal(harness.prompt.textContent, "");
    assert.equal(harness.status.textContent, "The quick estimate is unavailable. You can still create a plan.");
});

test("a removal failure relabels the remaining origin and updates the heading", async () => {
    const harness = createPreviewHarness(["Anděl", "Dejvická"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    controller.addOrigin("Anděl");
    controller.addOrigin("Dejvická");
    requests.resolve(1, completePayload(["Anděl", "Dejvická"]));
    await harness.flush();
    controller.removeOrigin("Anděl");
    requests.resolve(2, {}, { ok: false, status: 503 });
    await harness.flush();

    assert.deepEqual(harness.map.markers, [["A", "Dejvická"]]);
    assert.equal(harness.heading.textContent, "Approximate reach from Dejvická");
    assert.equal(harness.prompt.textContent, "");
    assert.equal(harness.status.textContent, "The quick estimate is unavailable. You can still create a plan.");
});

test("six current origins render stable A through F marker labels", async () => {
    const origins = ["A", "B", "C", "D", "E", "F"];
    const harness = createPreviewHarness(origins);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });

    origins.forEach((origin) => controller.addOrigin(origin));
    requests.resolve(5, completePayload(origins));
    await harness.flush();

    assert.deepEqual(harness.map.markers, origins.map((origin) => [origin, origin]));
    assert.equal(harness.heading.textContent, "Shared reach for 6 starting points");
});

test("preview interactions do not write browser storage", async () => {
    const previousStorage = Object.getOwnPropertyDescriptor(globalThis, "localStorage");
    const writes = [];
    Object.defineProperty(globalThis, "localStorage", {
        configurable: true,
        value: { setItem(key, value) { writes.push([key, value]); } },
    });
    try {
        const harness = createPreviewHarness(["Anděl"]);
        const requests = deferredFetches();
        const controller = await createHomePreview(harness.root, {
            fetch: requests.fetch,
            createMap: harness.createMap,
        });

        controller.addOrigin("Anděl");
        requests.resolve(0, completePayload(["Anděl"]));
        await harness.flush();
        controller.removeOrigin("Anděl");

        assert.deepEqual(writes, []);
    } finally {
        if (previousStorage) Object.defineProperty(globalThis, "localStorage", previousStorage);
        else delete globalThis.localStorage;
    }
});

test("empty or reordered participants fail before ready state or payload replacement", async () => {
    for (const invalidPayload of [
        { direction: "there-only", participants: [], stops: [] },
        {
            ...completePayload(["Anděl", "Dejvická"]),
            participants: completePayload(["Dejvická", "Anděl"]).participants,
        },
    ]) {
        const harness = createPreviewHarness(["Anděl", "Dejvická"]);
        const requests = deferredFetches();
        const controller = await createHomePreview(harness.root, {
            fetch: requests.fetch,
            createMap: harness.createMap,
        });
        controller.addOrigin("Anděl");
        controller.addOrigin("Dejvická");

        requests.resolve(1, invalidPayload);
        await harness.flush();

        assert.equal(harness.root.dataset.previewState, "failure");
        assert.equal(harness.map.payloads.length, 0);
        assert.equal(harness.map.clearFieldCount, 1);
        assert.equal(harness.map.clearPayloadCount, 0);
    }
});

test("insufficient participant coverage identifies the stop and offers removal", async () => {
    const harness = createPreviewHarness(["Anděl", "Dejvická"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });
    controller.addOrigin("Anděl");
    controller.addOrigin("Dejvická");
    const sparsePayload = completePayload(["Anděl", "Dejvická"]);
    sparsePayload.stops.forEach((stop) => {
        stop.participant_minutes[1] = null;
        stop.group_max_minutes = stop.participant_minutes[0];
    });

    requests.resolve(1, sparsePayload);
    await harness.flush();

    assert.equal(harness.root.dataset.previewState, "coverage");
    assert.match(harness.status.textContent, /No estimate is available from Dejvická/);
    const remove = harness.status.children.at(-1);
    assert.equal(remove.getAttribute("aria-label"), "Remove Dejvická");
    assert.equal(harness.map.payloads.length, 1);
    assert.equal(harness.map.clearFieldCount, 1);

    remove.dispatch("click");
    assert.deepEqual(JSON.parse(requests.requests.at(-1).options.body), { origins: ["Anděl"] });
    assert.equal(harness.selections.children.length, 1);
});

test("chip removal focuses the next button, then previous, then combobox", async () => {
    const harness = createPreviewHarness(["Anděl", "Dejvická", "Florenc"]);
    const requests = deferredFetches();
    const controller = await createHomePreview(harness.root, {
        fetch: requests.fetch,
        createMap: harness.createMap,
    });
    controller.addOrigin("Anděl");
    controller.addOrigin("Dejvická");
    controller.addOrigin("Florenc");

    const middleRemove = harness.selections.children[1].children.at(-1);
    middleRemove.focus();
    middleRemove.dispatch("click");
    assert.equal(harness.document.activeElement.getAttribute("aria-label"), "Remove Florenc");

    harness.document.activeElement.dispatch("click");
    assert.equal(harness.document.activeElement.getAttribute("aria-label"), "Remove Anděl");

    harness.document.activeElement.dispatch("click");
    assert.equal(harness.document.activeElement, harness.search);
});

test("combobox option IDs are unique across preview roots", async () => {
    const first = createPreviewHarness(["Anděl"]);
    const second = createPreviewHarness(["Anděl"]);
    await createHomePreview(first.root, {
        fetch: async () => ({ ok: true, json: async () => completePayload(["Anděl"]) }),
        createMap: first.createMap,
    });
    await createHomePreview(second.root, {
        fetch: async () => ({ ok: true, json: async () => completePayload(["Anděl"]) }),
        createMap: second.createMap,
    });

    for (const harness of [first, second]) {
        harness.search.value = "and";
        harness.search.dispatch("input");
        harness.search.dispatch("keydown", { key: "ArrowDown" });
    }

    assert.notEqual(
        first.search.getAttribute("aria-activedescendant"),
        second.search.getAttribute("aria-activedescendant"),
    );
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

test("destroy evicts initialization so the same root can initialize again", async () => {
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

    const [first] = await initHomePreview(target, dependencies);
    first.destroy();
    assert.equal(harness.search.getAttribute("list"), "home-stop-suggestions");
    const [second] = await initHomePreview(target, dependencies);

    assert.notEqual(first, second);
    assert.equal(harness.createMapCount, 2);
    assert.equal(harness.search.getAttribute("list"), null);
    assert.equal(harness.map.destroyCount, 1);
});
