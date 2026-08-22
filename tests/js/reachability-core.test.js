import assert from "node:assert/strict";
import test from "node:test";

import {
    classifyTime,
    estimateNearestStopGrid,
    selectLayerValues,
} from "../../static/reachability-core.js";

const payload = {
    participants: [{ id: 1 }, { id: 2 }],
    stops: [
        { group_max_minutes: 35, participant_minutes: [20, 35] },
        { group_max_minutes: null, participant_minutes: [40, null] },
    ],
};

test("Everyone uses group maximum values", () => {
    assert.deepEqual(selectLayerValues(payload, null), [35, null]);
});

test("participant view uses the matching array index", () => {
    assert.deepEqual(selectLayerValues(payload, 1), [20, 40]);
    assert.deepEqual(selectLayerValues(payload, 2), [35, null]);
});

test("participant view rejects an ID outside the saved snapshot", () => {
    assert.throws(() => selectLayerValues(payload, 3), {
        name: "RangeError",
        message: "Unknown participant 3",
    });
});

test("time bands add a distinct fastest band below the threshold", () => {
    assert.equal(classifyTime(20, 35, 15), 0);
    assert.equal(classifyTime(21, 35, 15), 1);
    assert.equal(classifyTime(35, 35, 15), 1);
    assert.equal(classifyTime(36, 35, 15), 2);
    assert.equal(classifyTime(50, 35, 15), 2);
    assert.equal(classifyTime(51, 35, 15), 3);
    assert.equal(classifyTime(65, 35, 15), 3);
    assert.equal(classifyTime(66, 35, 15), 4);
    assert.equal(classifyTime(null, 35, 15), null);
});

test("time bands reject invalid observations and band settings", () => {
    assert.equal(classifyTime(Number.NaN, 35, 15), null);
    assert.equal(classifyTime(Number.POSITIVE_INFINITY, 35, 15), null);
    assert.equal(classifyTime(20, Number.NaN, 15), null);
    assert.equal(classifyTime(20, 35, 0), null);
});

test("map cells use the nearest stop time plus walking instead of blending stops", () => {
    const grid = estimateNearestStopGrid([
        { x: 0, y: 0.5, value: 10 },
        { x: 1, y: 0.5, value: 30 },
    ], 1, 1, 12);

    assert.equal(grid.width, 1);
    assert.equal(grid.height, 1);
    assert.equal(grid.values[0], 16);
});

test("map cells stay unavailable when their nearest stop has no estimate", () => {
    const grid = estimateNearestStopGrid([
        { x: 0.5, y: 0.5, value: null },
        { x: 10, y: 0.5, value: 20 },
    ], 1, 1, 1);

    assert.ok(Number.isNaN(grid.values[0]));
});

test("nearest-stop estimation returns the exact travel time at a stop", () => {
    const grid = estimateNearestStopGrid([
        { x: 0.5, y: 0.5, value: 27 },
        { x: 10, y: 10, value: 90 },
    ], 1, 1, 5);

    assert.equal(grid.values[0], 27);
});

test("nearest-stop estimation caps the working grid at 96 by 96", () => {
    const grid = estimateNearestStopGrid([{ x: 0.5, y: 0.5, value: 27 }], 160, 120, 1);

    assert.equal(grid.width, 96);
    assert.equal(grid.height, 96);
    assert.equal(grid.values.length, 96 * 96);
});

test("nearest-stop estimation marks cells unavailable when no stops are observed", () => {
    const grid = estimateNearestStopGrid([], 1, 1, 1);

    assert.ok(Number.isNaN(grid.values[0]));
});
