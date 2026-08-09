import { classifyTime, interpolateGrid, selectLayerValues } from "./reachability-core.js";

const DEFAULT_CENTER = [50.0755, 14.4378];
const DEFAULT_ZOOM = 12;
const DEFAULT_THRESHOLD = 35;
const DEFAULT_STEP = 15;
const MAX_RENDER_GRID_SIZE = 96;
const BAND_COLORS = ["#4dc694", "#ffd447", "#ff8a47"];
const BAND_OPACITY = 0.48;
const TILE_URL = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";

function errorMessage(error) {
    return error instanceof Error ? error.message : "Reachability layer unavailable";
}

function dispatchReachabilityError(root, error) {
    const detail = { message: errorMessage(error) };
    const EventConstructor = root.ownerDocument?.defaultView?.CustomEvent ?? globalThis.CustomEvent;
    const event = typeof EventConstructor === "function"
        ? new EventConstructor("reachability:error", { detail })
        : { type: "reachability:error", detail };
    root.dispatchEvent(event);
}

function isNullableFinite(value) {
    return value == null || Number.isFinite(value);
}

function validatePayload(payload) {
    if (
        payload == null
        || typeof payload !== "object"
        || !Array.isArray(payload.participants)
        || !Array.isArray(payload.stops)
    ) {
        throw new TypeError("Reachability response is invalid");
    }

    const participantIds = new Set();
    for (const participant of payload.participants) {
        if (
            participant == null
            || typeof participant !== "object"
            || (typeof participant.id !== "number" && typeof participant.id !== "string")
            || participantIds.has(String(participant.id))
        ) {
            throw new TypeError("Reachability response is invalid");
        }
        participantIds.add(String(participant.id));
    }

    for (const stop of payload.stops) {
        if (
            stop == null
            || typeof stop !== "object"
            || typeof stop.name !== "string"
            || !Number.isFinite(stop.lat)
            || !Number.isFinite(stop.lon)
            || !Array.isArray(stop.participant_minutes)
            || stop.participant_minutes.length !== payload.participants.length
            || !stop.participant_minutes.every(isNullableFinite)
            || !isNullableFinite(stop.group_max_minutes)
        ) {
            throw new TypeError("Reachability response is invalid");
        }
    }
    return payload;
}

function validLocation(item) {
    return item != null && Number.isFinite(item.lat) && Number.isFinite(item.lon);
}

function popupContent(document, title, details = []) {
    const wrapper = document.createElement("div");
    const heading = document.createElement("strong");
    heading.textContent = String(title ?? "");
    wrapper.appendChild(heading);
    for (const detail of details) {
        wrapper.appendChild(document.createElement("br"));
        wrapper.appendChild(document.createTextNode(String(detail)));
    }
    return wrapper;
}

function addCircle(leaflet, group, document, item, options, details = []) {
    if (!validLocation(item)) return null;
    const marker = leaflet.circleMarker([item.lat, item.lon], options);
    marker.bindPopup(popupContent(document, item.name, details));
    group.addLayer(marker);
    return marker;
}

export class ReachabilityMapController {
    constructor(map, payload, options = {}) {
        this.map = map;
        this.payload = payload;
        this.options = options;
        this.root = options.root;
        this.leaflet = options.leaflet;
        this.document = this.root.ownerDocument ?? globalThis.document;
        this.requestFrame = options.requestAnimationFrame;
        this.cancelFrame = options.cancelAnimationFrame;
        this.abortController = options.abortController;
        this.threshold = Number.isFinite(options.threshold) ? options.threshold : DEFAULT_THRESHOLD;
        this.step = Number.isFinite(options.step) && options.step > 0 ? options.step : DEFAULT_STEP;
        this.participantId = null;
        this.participantMarkers = [];
        this.layerValues = [];
        this.results = [];
        this.venues = [];
        this.frameId = null;
        this.destroyed = false;

        this.participantLayer = this.leaflet.layerGroup().addTo(this.map);
        this.rankedStopLayer = this.leaflet.layerGroup().addTo(this.map);
        this.venueLayer = this.leaflet.layerGroup().addTo(this.map);
        this.canvasLayer = this.leaflet.layerGroup().addTo(this.map);

        this.canvas = this.document.createElement("canvas");
        this.canvas.className = "reachability-field";
        this.canvas.setAttribute?.("aria-hidden", "true");
        this.context = this.canvas.getContext?.("2d") ?? null;
        if (this.leaflet.Layer?.extend) {
            const canvas = this.canvas;
            const CanvasFieldLayer = this.leaflet.Layer.extend({
                onAdd(ownerMap) {
                    ownerMap.getPanes().overlayPane.appendChild(canvas);
                },
                onRemove() {
                    canvas.remove();
                },
            });
            this.canvasLeafletLayer = new CanvasFieldLayer();
            this.canvasLayer.addLayer(this.canvasLeafletLayer);
        } else {
            this.map.getPanes().overlayPane.appendChild(this.canvas);
            this.canvasLeafletLayer = { canvas: this.canvas };
            this.canvasLayer.addLayer(this.canvasLeafletLayer);
        }
        this.redrawListener = () => this.scheduleRedraw();
        this.map.on("move zoom resize", this.redrawListener);

        this.setResults(options.stops ?? []);
        this.setVenues(options.venues ?? []);
        this.setPayload(payload);
    }

    setPayload(payload) {
        this.payload = payload;
        this.participantId = null;
        this.layerValues = selectLayerValues(payload, null);
        this.renderParticipants();
        this.scheduleRedraw();
    }

    render() {
        this.renderParticipants();
        this.setResults(this.results);
        this.setVenues(this.venues);
        this.scheduleRedraw();
    }

    setParticipant(id) {
        if (id == null) {
            this.participantId = null;
        } else {
            const participant = this.payload.participants.find((person) => String(person.id) === String(id));
            if (!participant) {
                throw new RangeError(`Unknown participant ${id}`);
            }
            this.participantId = participant.id;
        }
        this.layerValues = selectLayerValues(this.payload, this.participantId);
        this.scheduleRedraw();
    }

    setThreshold(minutes) {
        if (!Number.isFinite(minutes)) {
            throw new RangeError("Reachability threshold must be finite");
        }
        this.threshold = minutes;
        this.scheduleRedraw();
    }

    setResults(stops) {
        this.results = Array.isArray(stops) ? stops : [];
        this.rankedStopLayer.clearLayers();
        this.results.forEach((stop, index) => {
            const rank = Number.isInteger(stop.rank) && stop.rank > 0 ? stop.rank : index + 1;
            const selected = typeof stop.selected === "boolean" ? stop.selected : rank === 1;
            addCircle(
                this.leaflet,
                this.rankedStopLayer,
                this.document,
                stop,
                {
                    className: "ranked-stop-marker",
                    color: "#17191c",
                    fillColor: "#fffefa",
                    fillOpacity: 1,
                    radius: selected ? 10 : 7,
                    weight: selected ? 3 : 2,
                },
                [`Rank ${rank}`],
            );
        });
        this._bringParticipantsToFront();
    }

    setVenues(venues) {
        this.venues = Array.isArray(venues) ? venues : [];
        this.venueLayer.clearLayers();
        this.venues.forEach((venue) => {
            const details = [];
            if (Number.isFinite(venue.rating)) details.push(`Rating ${venue.rating.toFixed(1)}`);
            addCircle(
                this.leaflet,
                this.venueLayer,
                this.document,
                venue,
                {
                    className: "venue-marker",
                    color: "#17191c",
                    fillColor: "#ff6658",
                    fillOpacity: 1,
                    radius: 6,
                    weight: 2,
                },
                details,
            );
        });
    }

    renderParticipants() {
        this.participantLayer.clearLayers();
        this.participantMarkers = [];
        const stopsByName = new Map(this.payload.stops.map((stop) => [stop.name, stop]));
        for (const participant of this.payload.participants) {
            const locations = [
                [participant.start_stop, "Starting stop"],
                [participant.end_stop, "Return stop"],
            ];
            const seen = new Set();
            for (const [stopName, label] of locations) {
                if (typeof stopName !== "string" || seen.has(stopName)) continue;
                seen.add(stopName);
                const stop = stopsByName.get(stopName);
                if (!stop) continue;
                const marker = addCircle(
                    this.leaflet,
                    this.participantLayer,
                    this.document,
                    { ...stop, name: participant.name ?? stopName },
                    {
                        className: "participant-marker",
                        color: "#17191c",
                        fillColor: "#2458df",
                        fillOpacity: 1,
                        radius: 8,
                        weight: 2,
                    },
                    [`${label}: ${stopName}`],
                );
                if (marker) this.participantMarkers.push(marker);
            }
        }
        this._bringParticipantsToFront();
    }

    _bringParticipantsToFront() {
        this.participantMarkers.forEach((marker) => marker.bringToFront?.());
    }

    scheduleRedraw() {
        if (this.destroyed || this.frameId != null || !this.context) return;
        this.frameId = this.requestFrame(() => {
            this.frameId = null;
            if (this.destroyed) return;
            try {
                this.drawField();
            } catch (error) {
                this.hideField();
                dispatchReachabilityError(this.root, error);
            }
        });
    }

    hideField() {
        this.canvas.hidden = true;
        try {
            this.context.clearRect(0, 0, this.canvas.width, this.canvas.height);
        } catch (_) {
            // Hiding the canvas still prevents stale field pixels from being shown.
        }
    }

    drawField() {
        const size = this.map.getSize();
        const cssWidth = Math.max(1, Math.round(size.x));
        const cssHeight = Math.max(1, Math.round(size.y));
        const rawPixelRatio = this.options.pixelRatio
            ?? this.document.defaultView?.devicePixelRatio
            ?? 1;
        const pixelRatio = Number.isFinite(rawPixelRatio)
            ? Math.max(1, Math.min(2, rawPixelRatio))
            : 1;
        const outputWidth = Math.max(1, Math.round(cssWidth * pixelRatio));
        const outputHeight = Math.max(1, Math.round(cssHeight * pixelRatio));
        const gridScale = Math.min(1, MAX_RENDER_GRID_SIZE / Math.max(cssWidth, cssHeight));
        const gridWidth = Math.max(1, Math.round(cssWidth * gridScale));
        const gridHeight = Math.max(1, Math.round(cssHeight * gridScale));
        const scaleX = gridWidth / cssWidth;
        const scaleY = gridHeight / cssHeight;
        const observedPoints = [];

        this.payload.stops.forEach((stop, index) => {
            const value = this.layerValues[index];
            if (!Number.isFinite(value)) return;
            const point = this.map.latLngToContainerPoint([stop.lat, stop.lon]);
            observedPoints.push({
                x: point.x * scaleX,
                y: point.y * scaleY,
                outputX: point.x * pixelRatio,
                outputY: point.y * pixelRatio,
                value,
            });
        });

        this.canvas.width = outputWidth;
        this.canvas.height = outputHeight;
        this.canvas.style.width = `${cssWidth}px`;
        this.canvas.style.height = `${cssHeight}px`;
        this.leaflet.DomUtil.setPosition(
            this.canvas,
            this.map.containerPointToLayerPoint([0, 0]),
        );
        this.context.clearRect(0, 0, outputWidth, outputHeight);
        if (observedPoints.length === 0) {
            this.canvas.hidden = true;
            return;
        }

        const grid = interpolateGrid(observedPoints, gridWidth, gridHeight);
        const minX = Math.min(...observedPoints.map((point) => point.x));
        const maxX = Math.max(...observedPoints.map((point) => point.x));
        const minY = Math.min(...observedPoints.map((point) => point.y));
        const maxY = Math.max(...observedPoints.map((point) => point.y));
        for (let y = 0; y < grid.height; y += 1) {
            for (let x = 0; x < grid.width; x += 1) {
                const centerX = x + 0.5;
                const centerY = y + 0.5;
                if (centerX < minX || centerX > maxX || centerY < minY || centerY > maxY) continue;
                const band = classifyTime(grid.values[y * grid.width + x], this.threshold, this.step);
                if (band == null || band >= BAND_COLORS.length) continue;
                this.context.fillStyle = BAND_COLORS[band];
                this.context.globalAlpha = BAND_OPACITY;
                const left = Math.floor(x * outputWidth / grid.width);
                const top = Math.floor(y * outputHeight / grid.height);
                const right = Math.ceil((x + 1) * outputWidth / grid.width);
                const bottom = Math.ceil((y + 1) * outputHeight / grid.height);
                this.context.fillRect(left, top, right - left, bottom - top);
            }
        }

        this.context.globalAlpha = 0.9;
        this.context.fillStyle = this.inkColor();
        for (const point of observedPoints) {
            this.context.beginPath();
            this.context.arc(
                point.outputX,
                point.outputY,
                1.15 * pixelRatio,
                0,
                Math.PI * 2,
            );
            this.context.fill();
        }
        this.context.globalAlpha = 1;
        this.canvas.hidden = false;
    }

    inkColor() {
        const getStyle = this.document.defaultView?.getComputedStyle ?? globalThis.getComputedStyle;
        const value = typeof getStyle === "function"
            ? getStyle(this.root).getPropertyValue("--ink").trim()
            : "";
        return value || "#17191c";
    }

    destroy() {
        if (this.destroyed) return;
        this.destroyed = true;
        this.map.off("move zoom resize", this.redrawListener);
        if (this.frameId != null) {
            this.cancelFrame(this.frameId);
            this.frameId = null;
        }
        this.abortController.abort();
        this.canvas.remove();
        this.map.remove();
    }
}

export async function createReachabilityMap(root, options = {}) {
    if (!root || typeof root.dispatchEvent !== "function") {
        throw new TypeError("A map root element is required");
    }
    if (typeof options.reachabilityUrl !== "string" || options.reachabilityUrl.length === 0) {
        throw new TypeError("A reachability URL is required");
    }

    const leaflet = options.leaflet ?? globalThis.L;
    if (!leaflet) throw new Error("Leaflet is unavailable");
    const fetchRequest = options.fetch ?? globalThis.fetch;
    if (typeof fetchRequest !== "function") throw new Error("Fetch is unavailable");
    const requestFrame = options.requestAnimationFrame
        ?? globalThis.requestAnimationFrame?.bind(globalThis)
        ?? ((callback) => setTimeout(callback, 0));
    const cancelFrame = options.cancelAnimationFrame
        ?? globalThis.cancelAnimationFrame?.bind(globalThis)
        ?? clearTimeout;
    const abortController = new AbortController();
    let detachAbort = () => {};
    if (options.signal) {
        const abort = () => abortController.abort(options.signal.reason);
        if (options.signal.aborted) abort();
        else {
            options.signal.addEventListener("abort", abort, { once: true });
            detachAbort = () => options.signal.removeEventListener("abort", abort);
        }
    }

    const map = leaflet.map(root).setView(options.center ?? DEFAULT_CENTER, options.zoom ?? DEFAULT_ZOOM);
    leaflet.tileLayer(options.tileUrl ?? TILE_URL, {
        attribution: "&copy; OpenStreetMap contributors",
        maxZoom: 19,
    }).addTo(map);

    const controllerOptions = {
        ...options,
        abortController,
        cancelAnimationFrame: cancelFrame,
        leaflet,
        requestAnimationFrame: requestFrame,
        root,
    };
    const controller = new ReachabilityMapController(
        map,
        { participants: [], stops: [] },
        controllerOptions,
    );

    try {
        const response = await fetchRequest(options.reachabilityUrl, {
            headers: { Accept: "application/json" },
            signal: abortController.signal,
        });
        if (!response.ok) {
            throw new Error(`Reachability request failed: ${response.status}`);
        }
        const payload = validatePayload(await response.json());
        controller.setPayload(payload);
    } catch (error) {
        if (error?.name !== "AbortError") dispatchReachabilityError(root, error);
    } finally {
        detachAbort();
    }

    return controller;
}
