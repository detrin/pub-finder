import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

import { interpolateGrid } from "../../static/reachability-core.js";

const stops = Array.from({ length: 1444 }, (_, index) => ({
    x: (index % 38) * (95 / 37),
    y: Math.floor(index / 38) * (95 / 37),
    value: 12 + ((index * 17) % 79),
}));

interpolateGrid(stops, 96, 96);
interpolateGrid(stops, 96, 96);

let grid;
const samples = Array.from({ length: 3 }, () => {
    const started = performance.now();
    grid = interpolateGrid(stops, 96, 96);
    return performance.now() - started;
});
const elapsed = Math.min(...samples);

assert.equal(grid.values.length, 96 * 96);
assert.ok(elapsed < 150, `96x96 grid took ${elapsed.toFixed(2)}ms, expected under 150ms`);
console.log(
    `Reachability benchmark: ${elapsed.toFixed(2)}ms best of ${samples.length} `
    + `for 1,444 stops on a 96x96 grid`,
);
