import assert from "node:assert/strict";
import test from "node:test";

class FakeButton {
    constructor() {
        this.dataset = {};
        this.attributes = {};
        this.listeners = new Map();
    }

    addEventListener(type, handler) {
        const handlers = this.listeners.get(type) ?? [];
        handlers.push(handler);
        this.listeners.set(type, handlers);
    }

    dispatch(type) {
        for (const handler of this.listeners.get(type) ?? []) {
            handler({ type, target: this });
        }
    }

    closest(selector) {
        return selector === "[data-theme-toggle]" ? this : null;
    }

    setAttribute(name, value) {
        this.attributes[name] = String(value);
    }
}

class FakeDocument {
    constructor(button) {
        this.button = button;
        this.defaultView = { CustomEvent };
        this.documentElement = { dataset: { theme: "light" }, style: {} };
        this.events = [];
        this.listeners = new Map();
    }

    addEventListener(type, handler) {
        const handlers = this.listeners.get(type) ?? [];
        handlers.push(handler);
        this.listeners.set(type, handlers);
    }

    querySelector(selector) {
        return selector === "[data-theme-toggle]" ? this.button : null;
    }

    querySelectorAll(selector) {
        return selector === "[data-theme-toggle]" ? [this.button] : [];
    }

    dispatch(type, target = this.button) {
        for (const handler of this.listeners.get(type) ?? []) {
            handler({ type, target, detail: { target } });
        }
    }

    dispatchEvent(event) {
        this.events.push(event);
        for (const handler of this.listeners.get(event.type) ?? []) handler(event);
        return true;
    }
}

test("theme toggle survives an HTMX replacement shell without duplicate handlers", async () => {
    const initialButton = new FakeButton();
    const documentFixture = new FakeDocument(initialButton);
    const savedThemes = [];
    globalThis.document = documentFixture;
    globalThis.localStorage = {
        setItem(key, value) { savedThemes.push([key, value]); },
    };

    const { initThemeToggle } = await import(
        new URL(`../../static/theme.js?test=${Math.random()}`, import.meta.url)
    );
    assert.equal(initialButton.attributes["aria-label"], "Use dark theme");
    initThemeToggle();
    initThemeToggle();

    assert.equal(initialButton.attributes["aria-label"], "Use dark theme");
    assert.equal(initialButton.listeners.get("click")?.length, 1);
    assert.equal(documentFixture.listeners.get("htmx:afterSwap")?.length, 1);

    const replacementButton = new FakeButton();
    documentFixture.button = replacementButton;
    documentFixture.dispatch("htmx:afterSwap", replacementButton);
    documentFixture.dispatch("htmx:afterSwap", replacementButton);
    assert.equal(replacementButton.attributes["aria-label"], "Use dark theme");
    assert.equal(replacementButton.listeners.get("click")?.length, 1);

    replacementButton.dispatch("click");
    assert.equal(documentFixture.documentElement.dataset.theme, "dark");
    assert.equal(documentFixture.documentElement.style.colorScheme, "dark");
    assert.equal(replacementButton.attributes["aria-label"], "Use light theme");
    assert.equal(documentFixture.events.length, 1);
    assert.equal(documentFixture.events[0].type, "themechange");
    assert.deepEqual(documentFixture.events[0].detail, { theme: "dark" });

    replacementButton.dispatch("click");
    assert.equal(documentFixture.documentElement.dataset.theme, "light");
    assert.equal(replacementButton.attributes["aria-label"], "Use dark theme");
    assert.deepEqual(savedThemes, [
        ["pubfinder_theme", "dark"],
        ["pubfinder_theme", "light"],
    ]);
    assert.equal(documentFixture.events.length, 2);
    assert.deepEqual(documentFixture.events[1].detail, { theme: "light" });
});
