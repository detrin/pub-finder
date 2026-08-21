import {
    createReachabilityMap,
    validateReachabilityPayload,
} from "./reachability-map.js?v=5";

const EMPTY_PAYLOAD = Object.freeze({
    participants: Object.freeze([]),
    stops: Object.freeze([]),
});
const MAX_ORIGINS = 6;
const MAX_OPTIONS = 50;
const initializations = new WeakMap();
let previewInstanceId = 0;

const DEFAULT_COPY = Object.freeze({
    carry: "{count} selected starts will be added to this plan.",
    coverage: "No estimate is available from {stop}. Remove it to continue.",
    duplicate: "That stop is already selected.",
    emptyHeading: "Add a starting stop to see its reach.",
    failure: "The quick estimate is unavailable. You can still create a plan.",
    groupHeading: "Shared reach for {count} starting points",
    groupPrompt: "Colour shows the longest estimated journey among the selected starts.",
    invalid: "Choose a stop from the Prague stop list.",
    limit: "The quick estimate supports up to six starting stops. For larger groups, start a plan.",
    oneHeading: "Approximate reach from {stop}",
    onePrompt: "Add another stop to see where everyone can reach.",
    remove: "Remove {stop}",
    updatedGroup: "Estimate updated for {count} starting stops.",
    updatedOne: "Estimate updated for one starting stop.",
    updating: "Updating estimate…",
});

function copy(root, key) {
    const value = root.dataset?.[key];
    return typeof value === "string" && value.length > 0 ? value : DEFAULT_COPY[key];
}

function format(template, values) {
    let result = template;
    for (const [key, value] of Object.entries(values)) {
        result = result.replaceAll(`{${key}}`, String(value));
    }
    return result;
}

function parseStops(serialized) {
    try {
        const parsed = JSON.parse(serialized ?? "[]");
        if (!Array.isArray(parsed)) return [];
        const seen = new Set();
        return parsed.filter((stop) => {
            if (typeof stop !== "string" || stop.length === 0 || seen.has(stop)) return false;
            seen.add(stop);
            return true;
        });
    } catch (_) {
        return [];
    }
}

function requiredHook(root, selector) {
    const element = root.querySelector?.(selector);
    if (!element) throw new TypeError(`Missing homepage preview hook: ${selector}`);
    return element;
}

function participantLetter(index) {
    return String.fromCharCode(65 + index);
}

function preparePayload(payload, origins) {
    const validated = validateReachabilityPayload(payload);
    if (
        validated.participants.length !== origins.length
        || validated.participants.some((participant, index) => participant.start_stop !== origins[index])
    ) {
        throw new TypeError("Preview response does not match the request");
    }
    const missingIndex = validated.participants.findIndex((_, participantIndex) => (
        !validated.stops.some((stop) => Number.isFinite(stop.participant_minutes[participantIndex]))
    ));
    const labelled = {
        ...validated,
        participants: validated.participants.map((participant, index) => {
            const markerLabel = participantLetter(index);
            const stop = origins[index] ?? participant.start_stop ?? participant.name ?? "";
            return {
                ...participant,
                marker_label: markerLabel,
                name: `${markerLabel} · ${stop}`,
            };
        }),
    };
    return {
        missingOrigin: missingIndex === -1 ? null : origins[missingIndex],
        payload: labelled,
    };
}

function findPreviewRoots(target) {
    if (!target) return [];
    if (target.matches?.("[data-home-preview]")) return [target];
    return [...(target.querySelectorAll?.("[data-home-preview]") ?? [])];
}

export async function createHomePreview(root, dependencies = {}) {
    if (!root || typeof root.querySelector !== "function") {
        throw new TypeError("A homepage preview root is required");
    }
    const fetchRequest = dependencies.fetch ?? globalThis.fetch;
    const createMap = dependencies.createMap ?? createReachabilityMap;
    if (typeof fetchRequest !== "function") throw new Error("Fetch is unavailable");
    if (typeof createMap !== "function") throw new Error("Map creation is unavailable");

    const document = root.ownerDocument ?? globalThis.document;
    const search = requiredHook(root, "[data-preview-search]");
    const options = requiredHook(root, "[data-preview-options]");
    const selections = requiredHook(root, "[data-preview-selections]");
    const status = requiredHook(root, "[data-preview-status]");
    const heading = requiredHook(root, "[data-preview-heading]");
    const prompt = requiredHook(root, "[data-preview-prompt]");
    const mapRoot = requiredHook(root, "[data-preview-map]");
    const hiddenFields = document?.querySelector?.("[data-preview-hidden-fields]") ?? null;
    const carryStatus = document?.querySelector?.("[data-preview-carry-status]") ?? null;
    const handoff = root.querySelector("[data-preview-handoff]");
    const planName = document?.querySelector?.("#session-name") ?? null;
    const stops = parseStops(root.dataset?.stops);
    // Keep every request value inside the server-rendered canonical stop allowlist.
    const canonicalStops = new Set(stops);
    const selected = [];
    const listeners = [];
    const optionListeners = [];
    const selectionListeners = [];
    const statusListeners = [];
    const optionIdPrefix = `home-stop-option-${++previewInstanceId}`;
    let removeButtons = [];
    let requestVersion = 0;
    let activeRequest = null;
    let filteredStops = [];
    let activeOption = -1;
    let destroyed = false;
    const nativeSuggestionList = search.getAttribute?.("list");
    const map = await createMap(mapRoot, { interactive: false, payload: EMPTY_PAYLOAD });

    function listen(element, type, handler, registry = listeners) {
        element?.addEventListener?.(type, handler);
        registry.push(() => element?.removeEventListener?.(type, handler));
    }

    function detachAll(registry) {
        for (const detach of registry.splice(0)) detach();
    }

    function setState(state) {
        root.dataset.previewState = state;
        mapRoot.setAttribute?.("aria-busy", state === "updating" ? "true" : "false");
    }

    function setStatus(text) {
        detachAll(statusListeners);
        status.textContent = text;
    }

    function dismissOptions() {
        detachAll(optionListeners);
        filteredStops = [];
        activeOption = -1;
        options.replaceChildren();
        options.hidden = true;
        search.setAttribute("aria-expanded", "false");
        search.removeAttribute?.("aria-activedescendant");
    }

    function updateActiveOption() {
        [...options.children].forEach((option, index) => {
            const active = index === activeOption;
            option.setAttribute("aria-selected", active ? "true" : "false");
            if (active) search.setAttribute("aria-activedescendant", option.getAttribute("id"));
        });
    }

    function renderOptions() {
        detachAll(optionListeners);
        const query = search.value.trim().toLocaleLowerCase();
        if (!query) {
            dismissOptions();
            return;
        }
        const matches = stops.filter((stop) => (
            !selected.includes(stop) && stop.toLocaleLowerCase().includes(query)
        ));
        matches.sort((left, right) => {
            const leftPrefix = left.toLocaleLowerCase().startsWith(query);
            const rightPrefix = right.toLocaleLowerCase().startsWith(query);
            if (leftPrefix !== rightPrefix) return leftPrefix ? -1 : 1;
            return left.localeCompare(right);
        });
        filteredStops = matches.slice(0, MAX_OPTIONS);
        activeOption = -1;
        const nodes = filteredStops.map((stop, index) => {
            const option = document.createElement("li");
            option.dataset.stop = stop;
            option.setAttribute("id", `${optionIdPrefix}-${index}`);
            option.setAttribute("role", "option");
            option.setAttribute("aria-selected", "false");
            option.textContent = stop;
            listen(option, "mousedown", (event) => event.preventDefault?.(), optionListeners);
            listen(option, "click", () => addOrigin(stop), optionListeners);
            return option;
        });
        options.replaceChildren(...nodes);
        options.hidden = nodes.length === 0;
        search.setAttribute("aria-expanded", nodes.length > 0 ? "true" : "false");
        search.removeAttribute?.("aria-activedescendant");
    }

    function renderSelections() {
        detachAll(selectionListeners);
        removeButtons = [];
        const chips = selected.map((stop, index) => {
            const item = document.createElement("li");
            const letter = document.createElement("span");
            letter.textContent = participantLetter(index);
            const name = document.createElement("span");
            name.textContent = stop;
            const remove = document.createElement("button");
            remove.setAttribute("type", "button");
            remove.setAttribute("aria-label", format(copy(root, "remove"), { stop }));
            remove.textContent = "×";
            listen(remove, "click", () => removeOrigin(stop, true), selectionListeners);
            removeButtons.push(remove);
            item.appendChild(letter);
            item.appendChild(name);
            item.appendChild(remove);
            return item;
        });
        selections.replaceChildren(...chips);

        if (hiddenFields) {
            const inputs = selected.map((stop) => {
                const input = document.createElement("input");
                input.setAttribute("type", "hidden");
                input.setAttribute("name", "preview_stops");
                input.value = stop;
                return input;
            });
            hiddenFields.replaceChildren(...inputs);
        }
        if (carryStatus) {
            carryStatus.hidden = selected.length === 0;
            carryStatus.textContent = selected.length
                ? format(copy(root, "carry"), { count: selected.length })
                : "";
        }
    }

    function renderEmpty() {
        setState("empty");
        heading.textContent = copy(root, "emptyHeading");
        prompt.textContent = "";
        setStatus("");
    }

    function renderUpdating() {
        setState("updating");
        setStatus(copy(root, "updating"));
    }

    function renderReady() {
        setState("ready");
        if (selected.length === 1) {
            heading.textContent = format(copy(root, "oneHeading"), { stop: selected[0] });
            prompt.textContent = copy(root, "onePrompt");
            setStatus(copy(root, "updatedOne"));
            return;
        }
        heading.textContent = format(copy(root, "groupHeading"), { count: selected.length });
        prompt.textContent = copy(root, "groupPrompt");
        setStatus(format(copy(root, "updatedGroup"), { count: selected.length }));
    }

    function renderFailure() {
        setState("failure");
        prompt.textContent = "";
        setStatus(copy(root, "failure"));
    }

    function renderCoverage(stop) {
        setState("coverage");
        prompt.textContent = "";
        setStatus(format(copy(root, "coverage"), { stop }));
        const remove = document.createElement("button");
        remove.setAttribute("type", "button");
        remove.setAttribute("aria-label", format(copy(root, "remove"), { stop }));
        remove.textContent = format(copy(root, "remove"), { stop });
        listen(remove, "click", () => removeOrigin(stop, true), statusListeners);
        status.appendChild(remove);
    }

    async function refresh() {
        const version = ++requestVersion;
        activeRequest?.abort();
        activeRequest = null;
        if (!selected.length) {
            map.clearPayload();
            renderEmpty();
            return;
        }

        const origins = [...selected];
        const controller = new AbortController();
        activeRequest = controller;
        renderUpdating();
        try {
            const response = await fetchRequest("/reachability/preview", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Accept: "application/json",
                },
                body: JSON.stringify({ origins }),
                signal: controller.signal,
            });
            if (!response?.ok) throw new Error("Preview request failed");
            const payload = await response.json();
            if (destroyed || version !== requestVersion) return;
            const prepared = preparePayload(payload, origins);
            map.setPayload(prepared.payload);
            if (prepared.missingOrigin !== null) {
                map.clearField();
                renderCoverage(prepared.missingOrigin);
                return;
            }
            renderReady();
        } catch (error) {
            if (error?.name === "AbortError" || destroyed || version !== requestVersion) return;
            map.clearField();
            renderFailure();
        } finally {
            if (version === requestVersion) activeRequest = null;
        }
    }

    function addOrigin(stop) {
        if (destroyed) return false;
        if (typeof stop !== "string" || !canonicalStops.has(stop)) {
            setStatus(copy(root, "invalid"));
            return false;
        }
        if (selected.includes(stop)) {
            setStatus(copy(root, "duplicate"));
            return false;
        }
        if (selected.length >= MAX_ORIGINS) {
            setStatus(copy(root, "limit"));
            return false;
        }
        selected.push(stop);
        search.value = "";
        dismissOptions();
        renderSelections();
        void refresh();
        return true;
    }

    function removeOrigin(stop, recoverFocus = false) {
        if (destroyed || typeof stop !== "string") return false;
        const index = selected.indexOf(stop);
        if (index === -1) return false;
        selected.splice(index, 1);
        dismissOptions();
        renderSelections();
        if (recoverFocus) {
            (removeButtons[Math.min(index, removeButtons.length - 1)] ?? search).focus?.();
        }
        void refresh();
        return true;
    }

    function destroy() {
        if (destroyed) return;
        destroyed = true;
        requestVersion += 1;
        activeRequest?.abort();
        activeRequest = null;
        detachAll(optionListeners);
        detachAll(selectionListeners);
        detachAll(statusListeners);
        detachAll(listeners);
        if (nativeSuggestionList) search.setAttribute("list", nativeSuggestionList);
        map.destroy?.();
        initializations.delete(root);
    }

    listen(search, "input", renderOptions);
    listen(search, "keydown", (event) => {
        if (event.key === "Escape") {
            if (!options.hidden) event.preventDefault?.();
            dismissOptions();
            return;
        }
        if (!filteredStops.length || !["ArrowDown", "ArrowUp", "Enter"].includes(event.key)) return;
        event.preventDefault?.();
        if (event.key === "ArrowDown") {
            activeOption = (activeOption + 1) % filteredStops.length;
            updateActiveOption();
        } else if (event.key === "ArrowUp") {
            activeOption = activeOption <= 0 ? filteredStops.length - 1 : activeOption - 1;
            updateActiveOption();
        } else if (activeOption >= 0) {
            addOrigin(filteredStops[activeOption]);
        }
    });
    if (handoff && planName) {
        listen(handoff, "click", (event) => {
            event.preventDefault?.();
            planName.scrollIntoView?.({ block: "center" });
            planName.focus?.();
        });
    }

    search.setAttribute("aria-expanded", "false");
    options.hidden = true;
    renderSelections();
    renderEmpty();
    search.removeAttribute?.("list");
    return { addOrigin, destroy, removeOrigin };
}

export function initHomePreview(target = globalThis.document, dependencies = {}) {
    const promises = findPreviewRoots(target).map((root) => {
        const existing = initializations.get(root);
        if (existing) return existing;
        const initialization = Promise.resolve()
            .then(() => createHomePreview(root, dependencies))
            .catch((error) => {
                initializations.delete(root);
                throw error;
            });
        initializations.set(root, initialization);
        return initialization;
    });
    return Promise.all(promises);
}

function initializePage() {
    void initHomePreview().catch(() => {});
}

if (typeof document !== "undefined") {
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initializePage, { once: true });
    } else {
        initializePage();
    }
}
