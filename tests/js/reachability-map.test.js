import assert from "node:assert/strict";
import test from "node:test";

import { interpolateGrid } from "../../static/reachability-core.js";
import { createReachabilityMap } from "../../static/reachability-map.js";

function createHarness({ width = 4, height = 1, pixelRatio = 1 } = {}) {
    const events = [];
    const frameQueue = [];
    const canvasOperations = [];
    const createdGroups = [];
    const mapListeners = new Map();
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
        createElement(tagName) {
            if (tagName === "canvas") return canvas;
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
        defaultView: { CustomEvent, devicePixelRatio: pixelRatio },
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
                bindTooltip(content) {
                    this.tooltip = content;
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
        map() {
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
        root,
        cancelAnimationFrame() {
            cancelledFrames += 1;
        },
        get cancelledFrames() {
            return cancelledFrames;
        },
        get mapRemoveCount() {
            return mapRemoveCount;
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

test("controller owns separate marker and canvas layers and draws four travel-time bands", async () => {
    const harness = createHarness();
    const fetchCalls = [];
    const controller = await createReachabilityMap(harness.root, {
        leaflet: harness.leaflet,
        reachabilityUrl: "/session/code/reachability",
        fetch: async (url, options) => {
            fetchCalls.push({ url, options });
            return { ok: true, json: async () => payload };
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
    assert.deepEqual([...new Set(fieldPixels.map((operation) => operation.alpha))], [0.52, 0.36, 0.22, 0.1]);
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
