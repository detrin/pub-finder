import assert from "node:assert/strict";
import test from "node:test";

test("session UI exposes a stable participant colour and an idempotent initializer", async () => {
    const root = {
        dataset: {},
        addEventListener() {},
        querySelector() { return null; },
        querySelectorAll() { return []; },
    };
    globalThis.document = {
        querySelector(selector) {
            return selector === "[data-session-workspace]" ? root : null;
        },
    };

    const module = await import(new URL("../static/session.js", import.meta.url));

    assert.equal(module.participantColor(0), "#ff6658");
    assert.equal(module.participantColor(6), "#ff6658");
    assert.equal(module.participantColor(-1), "#dff0ff");
    module.initSessionUi();
    module.initSessionUi();
    assert.equal(root.dataset.bound, "true");
});
