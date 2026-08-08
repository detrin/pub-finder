import assert from "node:assert/strict";
import test from "node:test";

let moduleNumber = 0;

async function loadSessionModule(document, window = {}) {
    globalThis.document = document;
    globalThis.window = {
        location: { origin: "https://meet.example" },
        requestAnimationFrame(callback) { callback(); },
        setTimeout(callback) { callback(); },
        ...window,
    };
    return import(new URL(`../../static/session.js?test=${moduleNumber++}`, import.meta.url));
}

function eventTarget(properties = {}) {
    return {
        dataset: {},
        isConnected: true,
        addEventListener(name, handler) { (this.events ??= {})[name] = [...(this.events[name] ?? []), handler]; },
        closest(selector) { return selector === this.selector ? this : null; },
        focus() { this.focusCount = (this.focusCount || 0) + 1; },
        setAttribute(name, value) { this.attributes ??= {}; this.attributes[name] = value; },
        ...properties,
    };
}

function fire(target, name, event = {}) {
    for (const handler of target.events?.[name] ?? []) handler(event);
}

function createDocument(root, dialogs = {}) {
    const events = {};
    return {
        readyState: "complete",
        events,
        addEventListener(name, handler) { (events[name] ??= []).push(handler); },
        querySelector(selector) {
            if (selector === "[data-session-workspace]") return root;
            return dialogs[selector] ?? null;
        },
        createElement() { return eventTarget(); },
        createTextNode(text) { return { textContent: text }; },
    };
}

function emit(document, name, detail, event = {}) {
    for (const handler of document.events[name] ?? []) handler({ ...event, detail });
}

test("session UI exposes a stable participant colour and idempotent initializer", async () => {
    const root = eventTarget({
        querySelector() { return null; },
        querySelectorAll() { return []; },
        contains() { return true; },
    });
    const document = createDocument(root);
    const module = await loadSessionModule(document);

    assert.equal(module.participantColor(0), "#ff6658");
    assert.equal(module.participantColor(6), "#ff6658");
    assert.equal(module.participantColor(-1), "#dff0ff");
    module.initSessionUi();
    assert.equal(root.dataset.bound, "true");
});

test("readiness updates after an SSE participant swap", async () => {
    const start = eventTarget({ value: "", selector: "[data-participant-name]", dataset: { participantName: "Daniel" } });
    const end = eventTarget({ value: "", selector: "[name=end_stop]" });
    const same = eventTarget({ checked: false, selector: "[data-same-start-end]" });
    const form = {
        querySelector(selector) {
            return { "[name=start_stop]": start, "[name=end_stop]": end, "[data-same-start-end]": same }[selector] ?? null;
        },
    };
    const submit = eventTarget({ disabled: true });
    const status = eventTarget({ textContent: "Daniel needs start and end stops." });
    const root = eventTarget({
        querySelector(selector) {
            return { "[data-search-submit]": submit, "[data-session-readiness]": status }[selector] ?? null;
        },
        querySelectorAll(selector) { return selector === "form.stop-form" ? [form, form] : []; },
        contains() { return true; },
    });
    const document = createDocument(root);
    await loadSessionModule(document);

    start.value = "Anděl";
    end.value = "Florenc";
    emit(document, "htmx:sseMessage", { elt: root });

    assert.equal(submit.disabled, false);
    assert.equal(status.textContent, "Everyone is ready.");
});

test("readiness requires two participants after a participant swap", async () => {
    const submit = eventTarget({ disabled: false });
    const status = eventTarget({ textContent: "Everyone is ready." });
    const root = eventTarget({
        querySelector(selector) { return { "[data-search-submit]": submit, "[data-session-readiness]": status }[selector] ?? null; },
        querySelectorAll(selector) { return selector === "form.stop-form" ? [{}] : []; },
        contains() { return true; },
    });
    const document = createDocument(root);
    await loadSessionModule(document);
    assert.equal(submit.disabled, true);
    assert.equal(status.textContent, "Add one more participant.");
});

test("same-stop changes disable the end field before autosave", async () => {
    const end = eventTarget({ disabled: false, selector: "[name=end_stop]", value: "Florenc" });
    const form = { querySelector(selector) { return selector === "[name=end_stop]" ? end : null; } };
    const checkbox = eventTarget({
        checked: true,
        selector: "[data-same-start-end]",
        matches() { return false; },
        closest(selector) { return selector === "form" ? form : (selector === "[data-same-start-end]" ? this : null); },
    });
    const root = eventTarget({
        querySelector() { return null; }, querySelectorAll() { return []; }, contains() { return true; },
    });
    const document = createDocument(root);
    await loadSessionModule(document);
    fire(root, "change", { target: checkbox });
    assert.equal(end.disabled, true);

    checkbox.checked = false;
    fire(root, "change", { target: checkbox });
    assert.equal(end.disabled, false);
    assert.equal(end.value, "Florenc");
});

test("SSE participant swaps wait for autosave and focused form inputs", async () => {
    const input = eventTarget({ tagName: "INPUT" });
    const request = {};
    const participantStream = eventTarget({ id: "session-participants", contains(candidate) { return candidate === input; } });
    const root = eventTarget({
        querySelector(selector) {
            return selector === "#session-participants" ? participantStream : null;
        },
        querySelectorAll() { return []; },
        contains(candidate) {
            return candidate === input || candidate === request || candidate === participantStream;
        },
    });
    const document = createDocument(root);
    document.activeElement = null;
    await loadSessionModule(document);

    emit(document, "htmx:beforeRequest", { elt: request, xhr: request });
    let prevented = 0;
    emit(document, "htmx:sseBeforeMessage", { type: "participants" }, {
        target: participantStream,
        preventDefault() { prevented += 1; },
    });
    assert.equal(prevented, 1);

    emit(document, "htmx:afterRequest", { elt: request, xhr: request });
    emit(document, "htmx:sseBeforeMessage", { type: "participants" }, {
        target: participantStream,
        preventDefault() { prevented += 1; },
    });
    assert.equal(prevented, 1);

    document.activeElement = input;
    emit(document, "htmx:sseBeforeMessage", { type: "participants" }, {
        target: participantStream,
        preventDefault() { prevented += 1; },
    });
    assert.equal(prevented, 2);
});

test("search progress SSE messages remain independent from participant editing", async () => {
    const input = eventTarget({ tagName: "INPUT" });
    const request = {};
    const participantStream = eventTarget({ id: "session-participants" });
    const searchProgress = eventTarget({ id: "search-progress" });
    const root = eventTarget({
        querySelector(selector) {
            return selector === "#session-participants" ? participantStream : null;
        },
        querySelectorAll() { return []; },
        contains(candidate) {
            return candidate === input || candidate === request || candidate === participantStream;
        },
    });
    const document = createDocument(root);
    document.activeElement = input;
    await loadSessionModule(document);

    emit(document, "htmx:beforeRequest", { elt: request, xhr: request });
    let prevented = 0;
    for (const type of ["progress", "complete"]) {
        emit(document, "htmx:sseBeforeMessage", { type }, {
            target: searchProgress,
            preventDefault() { prevented += 1; },
        });
    }

    assert.equal(prevented, 0);
});

test("participant SSE defers only participant edits then refreshes after focus leaves", async () => {
    const participantInput = eventTarget({ tagName: "INPUT" });
    const planInput = eventTarget({ tagName: "INPUT" });
    const participantStream = eventTarget({ contains(candidate) { return candidate === participantInput; } });
    const root = eventTarget({
        dataset: { sessionCode: "code" },
        querySelector(selector) { return selector === "#session-participants" ? participantStream : null; },
        querySelectorAll() { return []; },
        contains() { return true; },
    });
    const document = createDocument(root);
    const requests = [];
    globalThis.htmx = { ajax(...args) { requests.push(args); } };
    document.activeElement = planInput;
    await loadSessionModule(document);
    let prevented = 0;
    emit(document, "htmx:sseBeforeMessage", { type: "participants" }, {
        target: participantStream, preventDefault() { prevented += 1; },
    });
    assert.equal(prevented, 0);

    document.activeElement = participantInput;
    emit(document, "htmx:sseBeforeMessage", { type: "participants" }, {
        target: participantStream, preventDefault() { prevented += 1; },
    });
    assert.equal(prevented, 1);
    document.activeElement = null;
    emit(document, "focusout", {});
    assert.deepEqual(requests[0], ["GET", "/session/code/participants", {
        target: "#session-participants-inner", swap: "innerHTML",
    }]);
});

test("occasion presets update source checkboxes and resync after manual changes", async () => {
    const inputs = ["pub", "bar", "cafe", "restaurant"].map((value) => eventTarget({ value, checked: false }));
    const drinks = eventTarget({ dataset: { occasion: "drinks" }, selector: "[data-occasion]" });
    const coffee = eventTarget({ dataset: { occasion: "coffee" }, selector: "[data-occasion]" });
    const food = eventTarget({ dataset: { occasion: "food" }, selector: "[data-occasion]" });
    const anything = eventTarget({ dataset: { occasion: "anything" }, selector: "[data-occasion]" });
    const root = eventTarget({
        querySelector() { return null; },
        querySelectorAll(selector) {
            if (selector === "input[name=place_types]") return inputs;
            if (selector === "[data-occasion]") return [drinks, coffee, food, anything];
            return [];
        },
        contains() { return true; },
    });
    const document = createDocument(root);
    await loadSessionModule(document);
    for (const [button, expected] of [
        [drinks, ["pub", "bar"]],
        [coffee, ["cafe"]],
        [food, ["restaurant"]],
        [anything, ["pub", "bar", "cafe", "restaurant"]],
    ]) {
        fire(root, "click", { target: button });
        assert.deepEqual(inputs.filter((input) => input.checked).map((input) => input.value), expected);
        assert.equal(button.attributes["aria-pressed"], "true");
    }

    inputs[1].checked = false;
    fire(root, "change", { target: { closest() { return null; }, matches(selector) { return selector === "input[name=place_types]"; } } });
    assert.equal(drinks.attributes["aria-pressed"], "false");
});

test("stop selection resolves the latest replacement field once", async () => {
    let forms = [];
    const search = eventTarget({ value: "" });
    const list = eventTarget({
        replaceChildren(...children) { this.children = children; },
        querySelectorAll(selector) { return selector === ".stop-picker__item" ? this.children ?? [] : []; },
    });
    const dialog = eventTarget({
        querySelector(selector) { return { ".stop-picker__search": search, ".stop-picker__list": list, "[data-stop-picker-context]": eventTarget() }[selector] ?? null; },
        showModal() { this.open = true; },
        close() { this.open = false; fire(this, "close"); },
    });
    const original = eventTarget({ name: "start_stop", value: "", selector: "[data-stop-input]" });
    const replacement = eventTarget({ name: "start_stop", value: "", selector: "[name=start_stop]" });
    let changes = 0;
    replacement.dispatchEvent = (event) => {
        assert.equal(event.type, "change");
        assert.equal(event.bubbles, true);
        changes += 1;
    };
    function participantForm(input) {
        const id = eventTarget({ value: "1" });
        return {
            querySelector(selector) {
                return { "[name=participant_id]": id, "[name=start_stop]": input }[selector] ?? null;
            },
        };
    }
    const originalForm = participantForm(original);
    original.closest = (selector) => selector === "form" ? originalForm : (selector === "[data-stop-input]" ? original : null);
    forms = [originalForm];
    const root = eventTarget({
        dataset: { stops: '["Muzeum"]' },
        querySelector() { return null; },
        querySelectorAll(selector) { return selector === "form.stop-form" ? forms : []; },
        contains() { return true; },
    });
    const document = createDocument(root, { "[data-stop-dialog]": dialog });
    await loadSessionModule(document);

    fire(root, "click", { target: original, preventDefault() {} });
    original.isConnected = false;
    forms = [participantForm(replacement)];
    fire(list.children[0], "click");

    assert.equal(replacement.value, "Muzeum");
    assert.equal(changes, 1);
});

test("stop focus intent survives an unrelated participant swap", async () => {
    let forms = [];
    const search = eventTarget({ value: "" });
    const list = eventTarget({
        replaceChildren(...children) { this.children = children; },
        querySelectorAll(selector) { return selector === ".stop-picker__item" ? this.children ?? [] : []; },
    });
    const dialog = eventTarget({
        querySelector(selector) { return { ".stop-picker__search": search, ".stop-picker__list": list, "[data-stop-picker-context]": eventTarget() }[selector] ?? null; },
        showModal() {},
        close() { fire(this, "close"); },
    });
    const original = eventTarget({ name: "start_stop", value: "", selector: "[data-stop-input]" });
    const replacement = eventTarget({ name: "start_stop", value: "Muzeum", selector: "[name=start_stop]" });
    const autosaveXhr = {};
    const unrelatedXhr = {};
    function participantForm(idValue, input) {
        const id = eventTarget({ value: idValue });
        return {
            querySelector(selector) {
                return { "[name=participant_id]": id, "[name=start_stop]": input }[selector] ?? null;
            },
        };
    }
    const originalForm = participantForm("1", original);
    original.closest = (selector) => selector === "form" ? originalForm : (selector === "[data-stop-input]" ? original : null);
    forms = [originalForm];
    const root = eventTarget({
        dataset: { stops: '["Muzeum"]' },
        querySelector() { return null; },
        querySelectorAll(selector) { return selector === "form.stop-form" ? forms : []; },
        contains() { return true; },
    });
    const document = createDocument(root, { "[data-stop-dialog]": dialog });
    await loadSessionModule(document);
    original.dispatchEvent = () => {
        emit(document, "htmx:beforeRequest", { elt: originalForm, xhr: autosaveXhr });
    };

    fire(root, "click", { target: original, preventDefault() {} });
    fire(list.children[0], "click");
    original.isConnected = false;
    const unrelatedForm = participantForm("2", eventTarget({ name: "start_stop" }));
    forms = [unrelatedForm];
    emit(document, "htmx:afterSwap", { target: root, elt: root, xhr: unrelatedXhr });
    forms = [participantForm("1", replacement)];
    emit(document, "htmx:afterSwap", { target: root, elt: root, xhr: autosaveXhr });

    assert.equal(replacement.focusCount, 1);
});

test("an SSE message does not close an open stop picker", async () => {
    const search = eventTarget({ value: "" });
    const list = eventTarget({ replaceChildren(...children) { this.children = children; }, querySelectorAll() { return []; } });
    const dialog = eventTarget({
        querySelector(selector) { return { ".stop-picker__search": search, ".stop-picker__list": list, "[data-stop-picker-context]": eventTarget() }[selector] ?? null; },
        showModal() { this.open = true; },
        close() { this.open = false; fire(this, "close"); },
    });
    const input = eventTarget({ name: "start_stop", selector: "[data-stop-input]" });
    const form = { querySelector(selector) { return selector === "[name=participant_id]" ? eventTarget({ value: "1" }) : null; } };
    input.closest = (selector) => selector === "form" ? form : (selector === "[data-stop-input]" ? input : null);
    const root = eventTarget({
        dataset: { stops: '["Muzeum"]' }, querySelector() { return null; }, querySelectorAll(selector) { return selector === "form.stop-form" ? [form] : []; }, contains() { return true; },
    });
    const document = createDocument(root, { "[data-stop-dialog]": dialog });
    await loadSessionModule(document);

    fire(root, "click", { target: input, preventDefault() {} });
    emit(document, "htmx:sseMessage", { elt: root });

    assert.equal(dialog.open, true);
    assert.equal(search.focusCount, 1);
});

test("removal confirmation names the participant and focuses a stable replacement control", async () => {
    let controls = [];
    const form = eventTarget();
    const name = eventTarget({ textContent: "" });
    const id = eventTarget({ value: "" });
    const cancel = eventTarget();
    const dialog = eventTarget({
        querySelector(selector) {
            return {
                "[data-remove-form]": form,
                "[data-remove-participant-name]": name,
                "[data-remove-participant-id]": id,
                "[data-dialog-cancel]": cancel,
            }[selector] ?? null;
        },
        showModal() { this.open = true; },
        close() { this.open = false; fire(this, "close"); },
    });
    const remove = eventTarget({
        selector: "[data-remove-participant]",
        dataset: { participantId: "1", participantName: "Daniel" },
    });
    const nextRemove = eventTarget({
        selector: "[data-remove-participant]",
        dataset: { participantId: "2", participantName: "Petra" },
    });
    controls = [remove];
    const root = eventTarget({
        querySelector(selector) {
            if (selector === "[data-remove-participant]") return controls[0] ?? null;
            return selector === ".add-participant-form input" ? eventTarget() : null;
        },
        querySelectorAll(selector) { return selector === "[data-remove-participant]" ? controls : []; },
        contains() { return true; },
    });
    const document = createDocument(root, { "[data-remove-dialog]": dialog });
    await loadSessionModule(document);

    fire(root, "click", { target: remove });
    assert.equal(dialog.open, true);
    assert.equal(name.textContent, "Daniel");
    assert.equal(id.value, "1");

    fire(form, "submit");
    controls = [nextRemove];
    fire(form, "htmx:afterRequest");

    assert.equal(dialog.open, false);
    assert.equal(nextRemove.focusCount, 1);
});

test("failed removal focuses the replacement control for the same participant", async () => {
    let controls = [];
    const form = eventTarget();
    const name = eventTarget();
    const id = eventTarget({ value: "" });
    const cancel = eventTarget();
    const dialog = eventTarget({
        querySelector(selector) {
            return {
                "[data-remove-form]": form,
                "[data-remove-participant-name]": name,
                "[data-remove-participant-id]": id,
                "[data-dialog-cancel]": cancel,
            }[selector] ?? null;
        },
        showModal() {},
        close() { fire(this, "close"); },
    });
    const remove = eventTarget({
        selector: "[data-remove-participant]",
        dataset: { participantId: "1", participantName: "Daniel" },
    });
    const replacement = eventTarget({
        selector: "[data-remove-participant]",
        dataset: { participantId: "1", participantName: "Daniel" },
    });
    controls = [remove];
    const root = eventTarget({
        querySelector(selector) {
            if (selector === "[data-remove-participant]") return controls[0] ?? null;
            return null;
        },
        querySelectorAll(selector) { return selector === "[data-remove-participant]" ? controls : []; },
        contains() { return true; },
    });
    const document = createDocument(root, { "[data-remove-dialog]": dialog });
    await loadSessionModule(document);

    fire(root, "click", { target: remove });
    fire(form, "submit");
    controls = [replacement];
    fire(form, "htmx:afterRequest");

    assert.equal(replacement.focusCount, 1);
});
