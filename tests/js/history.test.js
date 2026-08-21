import assert from "node:assert/strict";
import test from "node:test";

test("history module remembers the current plan and renders recent plans on load", async () => {
    const store = new Map();
    const historyRoot = {
        hidden: true,
        children: [],
        replaceChildren(...children) { this.children = children; },
    };
    const sessionRoot = {
        dataset: { sessionCode: "friday-crew", sessionName: "Friday crew" },
    };
    globalThis.localStorage = {
        getItem(key) { return store.get(key) ?? null; },
        setItem(key, value) { store.set(key, value); },
    };
    globalThis.document = {
        readyState: "complete",
        querySelector(selector) {
            if (selector === "[data-session-code]") return sessionRoot;
            if (selector === "[data-session-history]") return historyRoot;
            return null;
        },
        createElement(tag) {
            assert.equal(tag, "a");
            return { href: "", textContent: "" };
        },
    };

    await import(new URL(`../../static/history.js?test=${Math.random()}`, import.meta.url));

    assert.deepEqual(JSON.parse(store.get("meet_somewhere_recent_sessions")), [
        { code: "friday-crew", name: "Friday crew" },
    ]);
    assert.equal(historyRoot.hidden, false);
    assert.equal(historyRoot.children.length, 1);
    assert.equal(historyRoot.children[0].href, "/session/friday-crew");
    assert.equal(historyRoot.children[0].textContent, "Friday crew");
});

test("history migrates legacy pubfinder sessions once without replacing current history", async () => {
    const store = new Map([
        ["pubfinder_sessions", JSON.stringify([
            { code: "old", name: "Old plan" }, { code: "old", name: "Duplicate" }, { code: "bad" },
        ])],
        ["meet_somewhere_recent_sessions", JSON.stringify([{ code: "new", name: "New plan" }])],
    ]);
    globalThis.localStorage = {
        getItem(key) { return store.get(key) ?? null; },
        setItem(key, value) { store.set(key, value); },
        removeItem(key) { store.delete(key); },
    };
    globalThis.document = {
        readyState: "complete",
        querySelector() { return null; },
        createElement() { return { href: "", textContent: "" }; },
    };
    await import(new URL(`../../static/history.js?test=${Math.random()}`, import.meta.url));
    assert.deepEqual(JSON.parse(store.get("meet_somewhere_recent_sessions")), [
        { code: "new", name: "New plan" }, { code: "old", name: "Old plan" },
    ]);
    assert.equal(store.has("pubfinder_sessions"), false);
});

test("history initialization without legacy state performs no storage writes", async () => {
    const store = new Map();
    const writes = [];
    const removals = [];
    globalThis.localStorage = {
        getItem(key) { return store.get(key) ?? null; },
        setItem(key, value) { writes.push([key, value]); },
        removeItem(key) { removals.push(key); },
    };
    globalThis.document = {
        readyState: "complete",
        querySelector() { return null; },
        createElement() { return { href: "", textContent: "" }; },
    };

    await import(new URL(`../../static/history.js?test=${Math.random()}`, import.meta.url));

    assert.deepEqual(writes, []);
    assert.deepEqual(removals, []);
});

test("current history renders without being rewritten when there is no legacy state", async () => {
    const saved = JSON.stringify([{ code: "current", name: "Current plan" }]);
    const store = new Map([["meet_somewhere_recent_sessions", saved]]);
    const writes = [];
    const historyRoot = {
        hidden: true,
        children: [],
        replaceChildren(...children) { this.children = children; },
    };
    globalThis.localStorage = {
        getItem(key) { return store.get(key) ?? null; },
        setItem(key, value) { writes.push([key, value]); },
        removeItem() { throw new Error("No legacy key should be removed"); },
    };
    globalThis.document = {
        readyState: "complete",
        querySelector(selector) {
            return selector === "[data-session-history]" ? historyRoot : null;
        },
        createElement() { return { href: "", textContent: "" }; },
    };

    await import(new URL(`../../static/history.js?test=${Math.random()}`, import.meta.url));

    assert.deepEqual(writes, []);
    assert.equal(store.get("meet_somewhere_recent_sessions"), saved);
    assert.equal(historyRoot.children[0].href, "/session/current");
});

test("history migration normalizes, deduplicates, bounds, and is idempotent", async () => {
    const store = new Map([
        ["meet_somewhere_recent_sessions", JSON.stringify([
            { code: "a", name: "A", extra: true },
            { code: "a", name: "A duplicate" },
            { code: "c", name: "C" },
            { code: "d", name: "D" },
            { code: "e", name: "E" },
        ])],
        ["pubfinder_sessions", JSON.stringify([
            { code: "b", name: "B", legacy: true },
            { code: "f", name: "F" },
            { code: "g", name: "G" },
        ])],
    ]);
    globalThis.localStorage = {
        getItem(key) { return store.get(key) ?? null; },
        setItem(key, value) { store.set(key, value); },
        removeItem(key) { store.delete(key); },
    };
    globalThis.document = {
        readyState: "complete",
        querySelector() { return null; },
        createElement() { return { href: "", textContent: "" }; },
    };

    await import(new URL(`../../static/history.js?test=${Math.random()}`, import.meta.url));
    const once = store.get("meet_somewhere_recent_sessions");
    assert.deepEqual(JSON.parse(once), [
        { code: "a", name: "A" },
        { code: "c", name: "C" },
        { code: "d", name: "D" },
        { code: "e", name: "E" },
        { code: "b", name: "B" },
    ]);
    assert.equal(store.has("pubfinder_sessions"), false);

    await import(new URL(`../../static/history.js?test=${Math.random()}`, import.meta.url));
    assert.equal(store.get("meet_somewhere_recent_sessions"), once);
});

test("history migration safely recovers malformed current and legacy stores", async () => {
    const store = new Map([
        ["meet_somewhere_recent_sessions", "not-json"],
        ["pubfinder_sessions", JSON.stringify([{ code: "legacy", name: "Legacy", extra: 1 }])],
    ]);
    globalThis.localStorage = {
        getItem(key) { return store.get(key) ?? null; },
        setItem(key, value) { store.set(key, value); },
        removeItem(key) { store.delete(key); },
    };
    globalThis.document = {
        readyState: "complete",
        querySelector() { return null; },
        createElement() { return { href: "", textContent: "" }; },
    };

    await import(new URL(`../../static/history.js?test=${Math.random()}`, import.meta.url));
    assert.deepEqual(JSON.parse(store.get("meet_somewhere_recent_sessions")), [
        { code: "legacy", name: "Legacy" },
    ]);
    assert.equal(store.has("pubfinder_sessions"), false);

    store.set("pubfinder_sessions", "not-json");
    const current = store.get("meet_somewhere_recent_sessions");
    await import(new URL(`../../static/history.js?test=${Math.random()}`, import.meta.url));
    assert.equal(store.get("meet_somewhere_recent_sessions"), current);
    assert.equal(store.get("pubfinder_sessions"), "not-json");
});
