import { createReachabilityMap } from "./reachability-map.js";

let controller = null;
let activeRoot = null;
let pendingAbort = null;
let initialization = 0;

function findRoot(target) {
    if (!target) return null;
    if (target.matches?.("[data-results-root]")) return target;
    return target.querySelector?.("[data-results-root]")
        ?? target.closest?.("[data-results-root]")
        ?? null;
}

function readJson(value) {
    const parsed = JSON.parse(value || "[]");
    if (!Array.isArray(parsed)) throw new TypeError("Results map data must be an array");
    return parsed;
}

function readMapData(root) {
    const data = root.querySelector("[data-map-data]");
    if (!data) throw new TypeError("Results map data is unavailable");
    return {
        participants: readJson(data.dataset.participants),
        stops: readJson(data.dataset.stops),
        venues: readJson(data.dataset.venues),
    };
}

function rankedStops(root, selectedRank = root.dataset.selectedRank || "1") {
    return readMapData(root).stops.map((stop, index) => {
        const rank = Number.isInteger(stop.rank) && stop.rank > 0 ? stop.rank : index + 1;
        return { ...stop, rank, selected: String(rank) === String(selectedRank) };
    });
}

function updateRankedStopState(root, selectedRank) {
    root.dataset.selectedRank = String(selectedRank);
    root.querySelectorAll("[data-rank]").forEach((button) => {
        const selected = button.dataset.rank === String(selectedRank);
        button.setAttribute("aria-expanded", String(selected));
        const card = button.closest?.("[data-ranked-stop]");
        card?.classList?.toggle("ranked-stop--selected", selected);
    });
    root.querySelectorAll("[data-result-detail]").forEach((detail) => {
        detail.hidden = detail.dataset.resultDetail !== String(selectedRank);
    });
}

function bindRankSelection(root) {
    root.querySelectorAll("[data-rank]").forEach((button) => {
        button.addEventListener("click", () => {
            if (activeRoot !== root) return;
            const selectedRank = button.dataset.rank;
            updateRankedStopState(root, selectedRank);
            controller?.setResults(rankedStops(root, selectedRank));
        });
    });
}

function bindMobileViews(root) {
    root.querySelectorAll("[data-mobile-view-target]").forEach((button) => {
        button.addEventListener("click", () => {
            const view = button.dataset.mobileViewTarget;
            if (view !== "map" && view !== "list") return;
            root.dataset.mobileView = view;
            root.querySelectorAll("[data-mobile-view-target]").forEach((candidate) => {
                candidate.setAttribute(
                    "aria-pressed",
                    String(candidate.dataset.mobileViewTarget === view),
                );
            });
        });
    });
}

function bindReachabilityControls(root) {
    root.querySelectorAll("[data-participant-id]").forEach((button) => {
        button.addEventListener("click", () => {
            if (activeRoot !== root) return;
            const rawId = button.dataset.participantId;
            const participantId = rawId === ""
                ? null
                : (/^-?\d+$/.test(rawId) ? Number(rawId) : rawId);
            root.querySelectorAll("[data-participant-id]").forEach((candidate) => {
                candidate.setAttribute("aria-pressed", String(candidate === button));
            });
            controller?.setParticipant(participantId);
        });
    });

    const threshold = root.querySelector("[data-threshold]");
    const output = root.querySelector("[data-threshold-value]");
    threshold?.addEventListener("input", () => {
        if (activeRoot !== root) return;
        const minutes = Number(threshold.value);
        if (!Number.isFinite(minutes)) return;
        if (output) output.textContent = `${minutes} min`;
        controller?.setThreshold(minutes);
    });
}

function selectedParticipant(root) {
    const selected = [...root.querySelectorAll("[data-participant-id]")]
        .find((button) => button.getAttribute("aria-pressed") === "true");
    const rawId = selected?.dataset.participantId ?? "";
    return rawId === "" ? null : (/^-?\d+$/.test(rawId) ? Number(rawId) : rawId);
}

function synchronizeController(root) {
    if (!controller || activeRoot !== root) return;
    const data = readMapData(root);
    const threshold = Number(root.querySelector("[data-threshold]")?.value);
    controller.setResults(rankedStops(root));
    controller.setVenues(data.venues);
    controller.setParticipant(selectedParticipant(root));
    if (Number.isFinite(threshold)) controller.setThreshold(threshold);
}

function showReachabilityError(root) {
    const message = root.querySelector("[data-reachability-error]");
    if (message) message.hidden = false;
}

function updateVenueMarkers(target) {
    const data = target?.dataset && Object.hasOwn(target.dataset, "venues")
        ? target
        : target?.querySelector?.("[data-map-data]");
    const root = findRoot(data ?? target);
    if (!data || !root || root !== activeRoot || !controller) return;
    try {
        controller.setVenues(readJson(data.dataset.venues));
    } catch (_) {
        showReachabilityError(root);
    }
}

function destroyDetachedController() {
    if (!activeRoot || activeRoot.isConnected !== false) return;
    initialization += 1;
    pendingAbort?.abort();
    pendingAbort = null;
    controller?.destroy();
    controller = null;
    activeRoot = null;
}

export async function initResultsUi(target = document) {
    const root = findRoot(target);
    if (!root) {
        destroyDetachedController();
        return;
    }
    if (root.dataset.bound === "true") {
        updateVenueMarkers(target);
        return;
    }

    root.dataset.bound = "true";
    bindRankSelection(root);
    bindMobileViews(root);
    bindReachabilityControls(root);

    const mapRoot = root.querySelector("[data-results-map]");
    mapRoot?.addEventListener("reachability:error", () => {
        if (activeRoot === root) showReachabilityError(root);
    });

    initialization += 1;
    const ownInitialization = initialization;
    pendingAbort?.abort();
    const ownAbort = new AbortController();
    pendingAbort = ownAbort;

    controller?.destroy();
    controller = null;
    activeRoot = root;

    try {
        const data = readMapData(root);
        const createdController = await createReachabilityMap(mapRoot, {
            reachabilityUrl: root.dataset.reachabilityUrl,
            signal: ownAbort.signal,
            stops: rankedStops(root),
            venues: data.venues,
        });
        if (
            ownAbort.signal.aborted
            || ownInitialization !== initialization
            || activeRoot !== root
        ) {
            createdController?.destroy();
            return;
        }
        controller = createdController;
        synchronizeController(root);
    } catch (_) {
        if (ownInitialization === initialization && activeRoot === root) {
            showReachabilityError(root);
        }
    } finally {
        if (pendingAbort === ownAbort) pendingAbort = null;
    }
}

function eventTarget(event) {
    return event?.detail?.target ?? event?.detail?.elt ?? event?.target ?? document;
}

function initializeFromEvent(event) {
    void initResultsUi(eventTarget(event));
}

document.addEventListener("htmx:afterSwap", initializeFromEvent);
document.addEventListener("htmx:sseMessage", initializeFromEvent);
document.addEventListener("htmx:oobAfterSwap", (event) => {
    const target = eventTarget(event);
    updateVenueMarkers(target);
    void initResultsUi(target);
});

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => { void initResultsUi(document); });
} else {
    void initResultsUi(document);
}
