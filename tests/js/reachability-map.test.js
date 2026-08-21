import assert from "node:assert/strict";
import test from "node:test";

import { interpolateGrid } from "../../static/reachability-core.js";
import { createReachabilityMap, validateReachabilityPayload } from "../../static/reachability-map.js";

function createHarness({ width = 4, height = 1, pixelRatio = 1 } = {}) {
    const events = [];
    const frameQueue = [];
    const canvasOperations = [];
    const createdGroups = [];
    const documentListeners = new Map();
    const mapListeners = new Map();
    const mapOptions = [];
    const themeColors = new Map();
    let mapRemoveCount = 0;
    let cancelledFrames = 0;
    let failSizeRead = false;

    const context = {
        clearRect() {
            canvasOperations.push({ type: "clear" });
        },
        setTransform() {},
        fillRect(x, y, width, height) {
            canvasOperations.push({
                type: "fillRect",
                alpha: this.globalAlpha,
                fillStyle: this.fillStyle,
                x,
                y,
                width,
                height,
            });
        },
        beginPath() {},
        arc(x, y, radius) {
            canvasOperations.push({ type: "arc", x, y, radius });
        },
        fill() {
            canvasOperations.push({ type: "dot", fillStyle: this.fillStyle });
        },
        createPattern(source, repetition) {
            canvasOperations.push({ type: "pattern", repetition, source });
            return "missing-hatch";
        },
        globalAlpha: 1,
        fillStyle: "",
    };
    const canvas = {
        className: "",
        height: 0,
        parentNode: null,
        style: {},
        width: 0,
        hidden: false,
        getContext() {
            return context;
        },
        remove() {
            this.parentNode = null;
        },
    };
    const overlayPane = {
        appendChild(element) {
            element.parentNode = this;
        },
    };
    const document = {
        canvasCount: 0,
        addEventListener(type, handler) {
            const handlers = documentListeners.get(type) ?? new Set();
            handlers.add(handler);
            documentListeners.set(type, handlers);
        },
        createElement(tagName) {
            if (tagName === "canvas") {
                this.canvasCount += 1;
                if (this.canvasCount === 1) return canvas;
                return {
                    height: 0,
                    width: 0,
                    getContext() {
                        return {
                            beginPath() {},
                            fillRect() {},
                            lineTo() {},
                            moveTo() {},
                            stroke() {},
                        };
                    },
                };
            }
            return {
                children: [],
                append(...children) {
                    this.children.push(...children);
                },
                appendChild(child) {
                    this.children.push(child);
                },
                setAttribute() {},
                textContent: "",
            };
        },
        createTextNode(text) {
            return { textContent: text };
        },
        defaultView: {
            CustomEvent,
            devicePixelRatio: pixelRatio,
            getComputedStyle() {
                return {
                    getPropertyValue(property) {
                        return themeColors.get(property) ?? "";
                    },
                };
            },
        },
        dispatchEvent(event) {
            for (const handler of documentListeners.get(event.type) ?? []) handler(event);
            return true;
        },
        removeEventListener(type, handler) {
            const handlers = documentListeners.get(type);
            handlers?.delete(handler);
            if (handlers?.size === 0) documentListeners.delete(type);
        },
    };
    const root = {
        ownerDocument: document,
        dispatchEvent(event) {
            events.push(event);
            return true;
        },
    };
    const map = {
        containerPointToLayerPoint(point) {
            return point;
        },
        fitBounds() {},
        getPanes() {
            return { overlayPane };
        },
        getSize() {
            if (failSizeRead) {
                failSizeRead = false;
                throw new Error("forced canvas redraw failure");
            }
            return { x: width, y: height };
        },
        latLngToContainerPoint([lat, lon]) {
            return { x: lon, y: lat };
        },
        off(names, handler) {
            assert.equal(mapListeners.get(names), handler);
            mapListeners.delete(names);
        },
        on(names, handler) {
            mapListeners.set(names, handler);
        },
        remove() {
            mapRemoveCount += 1;
        },
        setView() {
            return map;
        },
    };
    const leaflet = {
        DomUtil: {
            setPosition(element, point) {
                element.position = point;
            },
        },
        circleMarker(latLng, options) {
            return {
                latLng,
                options,
                bindPopup(content) {
                    this.popup = content;
                    return this;
                },
                bindTooltip(content, tooltipOptions) {
                    this.tooltip = content;
                    this.tooltipOptions = tooltipOptions;
                    return this;
                },
                bringToFront() {
                    this.isFront = true;
                    return this;
                },
            };
        },
        latLngBounds(points) {
            return points;
        },
        layerGroup() {
            const group = {
                layers: [],
                addLayer(layer) {
                    this.layers.push(layer);
                    return this;
                },
                addTo() {
                    return this;
                },
                clearLayers() {
                    this.layers.length = 0;
                },
            };
            createdGroups.push(group);
            return group;
        },
        map(_root, options) {
            mapOptions.push(options ?? {});
            return map;
        },
        tileLayer() {
            return { addTo() {} };
        },
    };

    return {
        canvas,
        canvasOperations,
        createdGroups,
        events,
        frameQueue,
        leaflet,
        map,
        mapListeners,
        mapOptions,
        root,
        dispatchThemeChange() {
            document.dispatchEvent(new CustomEvent("themechange"));
        },
        cancelAnimationFrame() {
            cancelledFrames += 1;
        },
        get cancelledFrames() {
            return cancelledFrames;
        },
        get mapRemoveCount() {
            return mapRemoveCount;
        },
        get themeListenerCount() {
            return documentListeners.get("themechange")?.size ?? 0;
        },
        failNextSizeRead() {
            failSizeRead = true;
        },
        requestAnimationFrame(callback) {
            frameQueue.push(callback);
            return frameQueue.length;
        },
        runFrame() {
            const callback = frameQueue.shift();
            assert.equal(typeof callback, "function");
            callback();
        },
        setThemeColor(property, value) {
            themeColors.set(property, value);
        },
    };
}

const payload = {
    participants: [{
        id: 7,
        name: "Daniel <script>",
        color: "#ff6658",
        start_stop: "Start",
        end_stop: "Start",
    }],
    stops: [
        { name: "Start", lat: 0.5, lon: 0.5, participant_minutes: [20], group_max_minutes: 20 },
        { name: "B", lat: 0.5, lon: 1.5, participant_minutes: [50], group_max_minutes: 50 },
        { name: "C", lat: 0.5, lon: 2.5, participant_minutes: [65], group_max_minutes: 65 },
        { name: "D", lat: 0.5, lon: 3.5, participant_minutes: [80], group_max_minutes: 80 },
    ],
};

test("payload validator rejects non-plain participant records", () => {
    assert.equal(typeof validateReachabilityPayload, "function");
    const inheritedParticipant = Object.create({ id: 7 });

    assert.throws(
        () => validateReachabilityPayload({
            participants: [inheritedParticipant],
            stops: payload.stops,
        }),
        { name: "TypeError", message: "Reachability response is invalid" },
    );
});

test("payload mode creates a reusable map without fetching", async () => {
    const harness = createHarness();
    let fetchCount = 0;
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        payload,
        fetch: async () => {
            fetchCount += 1;
        },
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });

    assert.equal(fetchCount, 0);
    const participantLayer = harness.createdGroups[0];
    controller.setPayload({ participants: [], stops: [] });
    assert.equal(participantLayer.layers.length, 0);
    assert.equal(harness.createdGroups[0], participantLayer);
});

test("decorative mode disables every Leaflet interaction and built-in control", async () => {
    const harness = createHarness();
    await createReachabilityMap(harness.root, {
        interactive: false,
        leaflet: harness.leaflet,
        payload,
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });

    assert.deepEqual(harness.mapOptions, [{
        attributionControl: false,
        boxZoom: false,
        doubleClickZoom: false,
        dragging: false,
        keyboard: false,
        scrollWheelZoom: false,
        touchZoom: false,
        zoomControl: false,
    }]);
});

test("malformed replacement payloads leave the current map state intact", async () => {
    const harness = createHarness();
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        payload,
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });
    const participantLayer = harness.createdGroups[0];
    const currentPayload = controller.payload;
    const currentMarkers = [...participantLayer.layers];

    assert.throws(
        () => controller.setPayload({
            participants: [{ id: 7 }],
            stops: [{
                name: "Start",
                lat: 0.5,
                lon: 0.5,
                participant_minutes: [],
                group_max_minutes: 20,
            }],
        }),
        { name: "TypeError", message: "Reachability response is invalid" },
    );

    assert.equal(controller.payload, currentPayload);
    assert.deepEqual(participantLayer.layers, currentMarkers);
});

test("clearPayload hides the field and clears participant markers without destroying the map", async () => {
    const harness = createHarness();
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        payload,
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });
    harness.runFrame();
    assert.equal(harness.canvas.hidden, false);
    assert.equal(harness.createdGroups[0].layers.length, 1);

    controller.clearPayload();

    assert.equal(harness.canvas.hidden, true);
    assert.equal(harness.createdGroups[0].layers.length, 0);
    assert.equal(harness.mapRemoveCount, 0);
});

test("participant marker labels render as safe permanent text", async () => {
    const harness = createHarness();
    const labelledPayload = {
        ...payload,
        participants: [{ ...payload.participants[0], marker_label: "A" }],
    };
    await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        payload: labelledPayload,
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });

    const marker = harness.createdGroups[0].layers[0];
    assert.equal(marker.tooltip.textContent, "A");
    assert.deepEqual(marker.tooltipOptions, {
        className: "participant-marker-label",
        direction: "center",
        interactive: false,
        opacity: 1,
        permanent: true,
    });
});

test("clearField hides stale heat values while retaining origin markers", async () => {
    const harness = createHarness();
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        payload,
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });
    harness.runFrame();
    const participantMarkers = [...harness.createdGroups[0].layers];

    controller.clearField();

    assert.equal(harness.canvas.hidden, true);
    assert.deepEqual(harness.createdGroups[0].layers, participantMarkers);
    assert.equal(controller.payload, payload);
    harness.mapListeners.get("move zoom resize")();
    harness.runFrame();
    assert.equal(harness.canvas.hidden, true);
    assert.deepEqual(harness.createdGroups[0].layers, participantMarkers);
});

test("controller renders four fixed travel bands and hatches explicit missing estimates", async () => {
    const harness = createHarness({ width: 5 });
    const fetchCalls = [];
    const completeBandPayload = {
        ...payload,
        stops: [
            ...payload.stops,
            {
                name: "Missing",
                lat: 0.5,
                lon: 4.5,
                participant_minutes: [null],
                group_max_minutes: null,
            },
        ],
    };
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        reachabilityUrl: "/session/code/reachability",
        fetch: async (url, options) => {
            fetchCalls.push({ url, options });
            return { ok: true, json: async () => completeBandPayload };
        },
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
        stops: [{ name: "Ranked", lat: 0.5, lon: 1.5, rank: 1 }],
        venues: [{ name: "Cafe", lat: 0.5, lon: 2.5 }],
        threshold: 35,
    });

    assert.equal(typeof controller.setParticipant, "function");
    assert.equal(typeof controller.setThreshold, "function");
    assert.equal(typeof controller.setResults, "function");
    assert.equal(typeof controller.setVenues, "function");
    assert.equal(typeof controller.destroy, "function");
    assert.equal(harness.createdGroups.length, 4);
    assert.equal(harness.createdGroups[3].layers.length, 1);
    assert.equal(fetchCalls.length, 1);
    assert.equal(fetchCalls[0].url, "/session/code/reachability");
    assert.deepEqual(fetchCalls[0].options.headers, { Accept: "application/json" });
    assert.ok(fetchCalls[0].options.signal instanceof AbortSignal);

    harness.runFrame();
    const fieldPixels = harness.canvasOperations.filter((operation) => operation.type === "fillRect");
    assert.deepEqual([...new Set(fieldPixels.map((operation) => operation.fillStyle))], [
        "#4dc694",
        "#ffd447",
        "#ff6658",
        "#dff0ff",
        "missing-hatch",
    ]);
    assert.deepEqual([...new Set(fieldPixels.map((operation) => operation.alpha))], [0.48]);
    assert.equal(harness.canvasOperations.filter((operation) => operation.type === "pattern").length, 1);
    assert.equal(harness.canvasOperations.filter((operation) => operation.type === "dot").length, 4);

    controller.setParticipant(7);
    controller.setThreshold(50);
    controller.setResults([{ name: "Next", lat: 0.5, lon: 3.5, rank: 2 }]);
    controller.setVenues([{ name: "Venue", lat: 0.5, lon: 0.5 }]);
    assert.equal(harness.createdGroups[1].layers.length, 1);
    assert.equal(harness.createdGroups[2].layers.length, 1);
});

test("non-square maps render circular observation dots at scaled output coordinates", async () => {
    const harness = createHarness({ width: 340, height: 654, pixelRatio: 2 });
    const tallPayload = {
        participants: [],
        stops: [
            { name: "Northwest", lat: 10, lon: 10, participant_minutes: [], group_max_minutes: 20 },
            { name: "Center", lat: 327, lon: 170, participant_minutes: [], group_max_minutes: 35 },
            { name: "Southeast", lat: 644, lon: 330, participant_minutes: [], group_max_minutes: 50 },
        ],
    };
    await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        reachabilityUrl: "/reachability",
        fetch: async () => ({ ok: true, json: async () => tallPayload }),
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
        pixelRatio: 2,
    });

    harness.runFrame();

    assert.equal(harness.canvas.width, 680);
    assert.equal(harness.canvas.height, 1308);
    assert.equal(harness.canvas.style.width, "340px");
    assert.equal(harness.canvas.style.height, "654px");
    const centerDot = harness.canvasOperations.filter(({ type }) => type === "arc")[1];
    assert.deepEqual(centerDot, { type: "arc", x: 340, y: 654, radius: 2.3 });
    const fieldCell = harness.canvasOperations.find(({ type }) => type === "fillRect");
    assert.ok(Math.abs(fieldCell.width - fieldCell.height) <= 1);

    const boundedGrid = interpolateGrid([{ x: 1, y: 1, value: 20 }], 340, 654);
    assert.deepEqual([boundedGrid.width, boundedGrid.height], [96, 96]);
});

test("selected result controls marker emphasis without changing its factual rank", async () => {
    const harness = createHarness();
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        reachabilityUrl: "/reachability",
        fetch: async () => ({ ok: true, json: async () => payload }),
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });

    controller.setResults([
        { name: "First", lat: 0.5, lon: 1.5, rank: 1, selected: false },
        { name: "Second", lat: 0.5, lon: 2.5, rank: 2, selected: true },
    ]);

    const [first, second] = harness.createdGroups[1].layers;
    assert.ok(second.options.radius > first.options.radius);
    assert.ok(second.options.weight > first.options.weight);
    assert.equal(second.popup.children.at(-1).textContent, "Rank 2");
});

test("input stop markers stay above meeting point markers", async () => {
    const harness = createHarness();
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        reachabilityUrl: "/reachability",
        fetch: async () => ({ ok: true, json: async () => payload }),
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });

    controller.setResults([{ name: "First", lat: 0.5, lon: 1.5, rank: 1 }]);

    assert.ok(harness.createdGroups[0].layers.every((marker) => marker.isFront));
});

test("map movement coalesces redraws and destroy cleans up once", async () => {
    const harness = createHarness();
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        reachabilityUrl: "/reachability",
        fetch: async () => ({ ok: true, json: async () => payload }),
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });
    harness.runFrame();

    const redraw = harness.mapListeners.get("move zoom resize");
    redraw();
    redraw();
    assert.equal(harness.frameQueue.length, 1);

    controller.destroy();
    controller.destroy();
    assert.equal(harness.mapListeners.size, 0);
    assert.equal(harness.cancelledFrames, 1);
    assert.equal(harness.mapRemoveCount, 1);
    assert.equal(harness.canvas.parentNode, null);
});

test("theme changes redraw homepage and results maps with fresh CSS colors without leaks", async () => {
    for (const mode of ["homepage", "results"]) {
        const harness = createHarness();
        const controller = await createReachabilityMap(harness.root, {
            leaflet: harness.leaflet,
            ...(mode === "homepage"
                ? { payload }
                : {
                    reachabilityUrl: "/reachability",
                    fetch: async () => ({ ok: true, json: async () => payload }),
                }),
            requestAnimationFrame: harness.requestAnimationFrame,
            cancelAnimationFrame: harness.cancelAnimationFrame,
        });
        harness.runFrame();
        assert.equal(harness.themeListenerCount, 1, mode);

        harness.setThemeColor("--mint", "#123456");
        harness.dispatchThemeChange();
        harness.dispatchThemeChange();
        assert.equal(harness.frameQueue.length, 1, mode);
        harness.runFrame();
        assert.ok(
            harness.canvasOperations.some((operation) => (
                operation.type === "fillRect" && operation.fillStyle === "#123456"
            )),
            mode,
        );

        controller.destroy();
        controller.destroy();
        assert.equal(harness.themeListenerCount, 0, mode);
        harness.dispatchThemeChange();
        assert.equal(harness.frameQueue.length, 0, mode);
    }
});

test("redraw errors hide stale field pixels without changing markers and later redraw restores them", async () => {
    const harness = createHarness();
    await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        reachabilityUrl: "/reachability",
        fetch: async () => ({ ok: true, json: async () => payload }),
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
        stops: [{ name: "Ranked", lat: 0.5, lon: 1.5 }],
        venues: [{ name: "Venue", lat: 0.5, lon: 2.5 }],
    });
    harness.runFrame();
    assert.equal(harness.canvas.hidden, false);

    const rankedLayers = [...harness.createdGroups[1].layers];
    const venueLayers = [...harness.createdGroups[2].layers];
    const clearsAfterSuccess = harness.canvasOperations.filter(({ type }) => type === "clear").length;
    const redraw = harness.mapListeners.get("move zoom resize");
    harness.failNextSizeRead();
    redraw();
    harness.runFrame();

    assert.equal(harness.events.at(-1).type, "reachability:error");
    assert.equal(harness.events.at(-1).detail.message, "forced canvas redraw failure");
    assert.equal(harness.canvas.hidden, true);
    assert.equal(
        harness.canvasOperations.filter(({ type }) => type === "clear").length,
        clearsAfterSuccess + 1,
    );
    assert.deepEqual(harness.createdGroups[1].layers, rankedLayers);
    assert.deepEqual(harness.createdGroups[2].layers, venueLayers);

    redraw();
    harness.runFrame();
    assert.equal(harness.canvas.hidden, false);
    assert.deepEqual(harness.createdGroups[1].layers, rankedLayers);
    assert.deepEqual(harness.createdGroups[2].layers, venueLayers);
});

test("reachability fetch errors emit a safe event and preserve ranked and venue markers", async () => {
    const harness = createHarness();
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        reachabilityUrl: "/reachability",
        fetch: async () => ({ ok: false, status: 503 }),
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
        stops: [{ name: "Ranked", lat: 0.5, lon: 1.5 }],
        venues: [{ name: "Venue", lat: 0.5, lon: 2.5 }],
    });

    assert.equal(harness.events.length, 1);
    assert.equal(harness.events[0].type, "reachability:error");
    assert.equal(harness.events[0].detail.message, "Reachability request failed: 503");
    assert.equal(harness.createdGroups[1].layers.length, 1);
    assert.equal(harness.createdGroups[2].layers.length, 1);
    assert.equal(typeof controller.destroy, "function");
});

test("malformed reachability JSON emits an error instead of rendering untrusted data", async () => {
    const harness = createHarness();
    await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        reachabilityUrl: "/reachability",
        fetch: async () => ({ ok: true, json: async () => ({ stops: "invalid" }) }),
        requestAnimationFrame: harness.requestAnimationFrame,
        cancelAnimationFrame: harness.cancelAnimationFrame,
    });

    assert.equal(harness.events.length, 1);
    assert.equal(harness.events[0].detail.message, "Reachability response is invalid");
});
