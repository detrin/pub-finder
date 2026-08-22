import { classifyTime, interpolateGrid, selectLayerValues } from "./reachability-core.js";

const DEFAULT_CENTER = [50.0755, 14.4378];
const DEFAULT_ZOOM = 12;
const DEFAULT_THRESHOLD = 35;
const DEFAULT_STEP = 15;
const MAX_RENDER_GRID_SIZE = 96;
const BAND_COLORS = [
    ["--blue", "#2458df"],
    ["--mint", "#4dc694"],
    ["--yellow", "#ffd447"],
    ["--coral", "#ff6658"],
    ["--sky-surface", "#dff0ff"],
];
const BAND_OPACITY = 0.48;
const MISSING_PATTERN_SIZE = 6;
const MISSING_MARKER_SIZE = 12;
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
    return value === null || Number.isFinite(value);
}

function invalidPayload() {
    throw new TypeError("Reachability response is invalid");
}

function isPlainRecord(value) {
    return value != null
        && typeof value === "object"
        && Object.getPrototypeOf(value) === Object.prototype;
}

function isPlainArray(value) {
    return Array.isArray(value) && Object.getPrototypeOf(value) === Array.prototype;
}

function hasDangerousKeys(value) {
    return Object.keys(value).some((key) => (
        key === "__proto__" || key === "constructor" || key === "prototype"
    ));
}

function hasOwn(value, key) {
    return Object.prototype.hasOwnProperty.call(value, key);
}

export function validateReachabilityPayload(payload) {
    try {
        if (
            !isPlainRecord(payload)
            || hasDangerousKeys(payload)
            || !hasOwn(payload, "participants")
            || !hasOwn(payload, "stops")
            || !isPlainArray(payload.participants)
            || !isPlainArray(payload.stops)
            || hasDangerousKeys(payload.participants)
            || hasDangerousKeys(payload.stops)
        ) invalidPayload();

        const participantIds = new Set();
        for (let index = 0; index < payload.participants.length; index += 1) {
            if (!hasOwn(payload.participants, index)) invalidPayload();
            const participant = payload.participants[index];
            if (
                !isPlainRecord(participant)
                || hasDangerousKeys(participant)
                || !hasOwn(participant, "id")
                || !["number", "string"].includes(typeof participant.id)
                || (hasOwn(participant, "marker_label")
                    && (typeof participant.marker_label !== "string"
                        || !/^[A-F]$/.test(participant.marker_label)))
                || participantIds.has(String(participant.id))
            ) invalidPayload();
            participantIds.add(String(participant.id));
        }

        for (let index = 0; index < payload.stops.length; index += 1) {
            if (!hasOwn(payload.stops, index)) invalidPayload();
            const stop = payload.stops[index];
            if (
                !isPlainRecord(stop)
                || hasDangerousKeys(stop)
                || !hasOwn(stop, "name")
                || !hasOwn(stop, "lat")
                || !hasOwn(stop, "lon")
                || !hasOwn(stop, "participant_minutes")
                || !hasOwn(stop, "group_max_minutes")
                || typeof stop.name !== "string"
                || !Number.isFinite(stop.lat)
                || !Number.isFinite(stop.lon)
                || !isPlainArray(stop.participant_minutes)
                || hasDangerousKeys(stop.participant_minutes)
                || stop.participant_minutes.length !== payload.participants.length
                || !isNullableFinite(stop.group_max_minutes)
            ) invalidPayload();
            for (let minuteIndex = 0; minuteIndex < stop.participant_minutes.length; minuteIndex += 1) {
                if (
                    !hasOwn(stop.participant_minutes, minuteIndex)
                    || !isNullableFinite(stop.participant_minutes[minuteIndex])
                ) invalidPayload();
            }
        }
        return payload;
    } catch (error) {
        if (error instanceof TypeError && error.message === "Reachability response is invalid") {
            throw error;
        }
        invalidPayload();
    }
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
        const validatedPayload = validateReachabilityPayload(payload);
        this.map = map;
        this.payload = validatedPayload;
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
        this.themeListener = () => this.scheduleRedraw();
        this.document.addEventListener?.("themechange", this.themeListener);

        this.setResults(options.stops ?? []);
        this.setVenues(options.venues ?? []);
        this.setPayload(validatedPayload);
    }

    setPayload(payload) {
        const validatedPayload = validateReachabilityPayload(payload);
        this.payload = validatedPayload;
        this.participantId = null;
        this.layerValues = selectLayerValues(validatedPayload, null);
        this.renderParticipants();
        this.scheduleRedraw();
    }

    clearPayload() {
        this.setPayload({ participants: [], stops: [] });
        this.hideField();
    }

    clearField() {
        this.layerValues = [];
        this.hideField();
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
            const startStop = participant.start_stop;
            const endStop = participant.end_stop;
            const locations = startStop === endStop
                ? [[startStop, "Start and return stop", "#2458df", "#ff6658", 4]]
                : [
                    [startStop, "Starting stop", "#ff6658", "#17191c", 2],
                    [endStop, "Return stop", "#2458df", "#17191c", 2],
                ];
            for (const [stopName, label, fillColor, borderColor, weight] of locations) {
                if (typeof stopName !== "string") continue;
                const stop = stopsByName.get(stopName);
                if (!stop) continue;
                const marker = addCircle(
                    this.leaflet,
                    this.participantLayer,
                    this.document,
                    { ...stop, name: participant.name ?? stopName },
                    {
                        className: "participant-marker",
                        color: borderColor,
                        fillColor,
                        fillOpacity: 1,
                        radius: 8,
                        weight,
                    },
                    [`${label}: ${stopName}`],
                );
                if (marker) {
                    if (typeof participant.marker_label === "string") {
                        const markerLabel = this.document.createElement("span");
                        markerLabel.textContent = participant.marker_label;
                        marker.bindTooltip(markerLabel, {
                            className: "participant-marker-label",
                            direction: "center",
                            interactive: false,
                            opacity: 1,
                            permanent: true,
                        });
                    }
                    this.participantMarkers.push(marker);
                }
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
        const missingPoints = [];

        this.payload.stops.forEach((stop, index) => {
            const value = this.layerValues[index];
            const point = this.map.latLngToContainerPoint([stop.lat, stop.lon]);
            if (value === null) {
                missingPoints.push({
                    outputX: point.x * pixelRatio,
                    outputY: point.y * pixelRatio,
                });
                return;
            }
            if (!Number.isFinite(value)) return;
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
        if (observedPoints.length === 0 && missingPoints.length === 0) {
            this.canvas.hidden = true;
            return;
        }

        if (observedPoints.length > 0) {
            const grid = interpolateGrid(observedPoints, gridWidth, gridHeight);
            const minX = Math.min(...observedPoints.map((point) => point.x));
            const maxX = Math.max(...observedPoints.map((point) => point.x));
            const minY = Math.min(...observedPoints.map((point) => point.y));
            const maxY = Math.max(...observedPoints.map((point) => point.y));
            const bandColors = BAND_COLORS.map(([property, fallback]) => (
                this.styleColor(property, fallback)
            ));
            for (let y = 0; y < grid.height; y += 1) {
                for (let x = 0; x < grid.width; x += 1) {
                    const centerX = x + 0.5;
                    const centerY = y + 0.5;
                    if (centerX < minX || centerX > maxX || centerY < minY || centerY > maxY) continue;
                    const band = classifyTime(grid.values[y * grid.width + x], this.threshold, this.step);
                    if (band == null || band >= bandColors.length) continue;
                    this.context.fillStyle = bandColors[band];
                    this.context.globalAlpha = BAND_OPACITY;
                    const left = Math.floor(x * outputWidth / grid.width);
                    const top = Math.floor(y * outputHeight / grid.height);
                    const right = Math.ceil((x + 1) * outputWidth / grid.width);
                    const bottom = Math.ceil((y + 1) * outputHeight / grid.height);
                    this.context.fillRect(left, top, right - left, bottom - top);
                }
            }
        }

        if (missingPoints.length > 0) {
            this.context.globalAlpha = BAND_OPACITY;
            this.context.fillStyle = this.missingPattern(pixelRatio);
            const missingSize = MISSING_MARKER_SIZE * pixelRatio;
            for (const point of missingPoints) {
                this.context.fillRect(
                    point.outputX - missingSize / 2,
                    point.outputY - missingSize / 2,
                    missingSize,
                    missingSize,
                );
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
        return this.styleColor("--ink", "#17191c");
    }

    styleColor(property, fallback) {
        const getStyle = this.document.defaultView?.getComputedStyle ?? globalThis.getComputedStyle;
        const value = typeof getStyle === "function"
            ? getStyle(this.root).getPropertyValue(property).trim()
            : "";
        return value || fallback;
    }

    missingPattern(pixelRatio) {
        const size = MISSING_PATTERN_SIZE * pixelRatio;
        const tile = this.document.createElement("canvas");
        tile.width = size;
        tile.height = size;
        const context = tile.getContext?.("2d");
        if (!context || typeof this.context.createPattern !== "function") {
            return this.styleColor("--muted", "#686d71");
        }
        context.fillStyle = this.styleColor("--paper", "#fffefa");
        context.fillRect(0, 0, size, size);
        context.strokeStyle = this.styleColor("--muted", "#686d71");
        context.lineWidth = Math.max(1, pixelRatio);
        context.beginPath();
        context.moveTo(-size / 2, size / 2);
        context.lineTo(size / 2, -size / 2);
        context.moveTo(0, size);
        context.lineTo(size, 0);
        context.moveTo(size / 2, size * 1.5);
        context.lineTo(size * 1.5, size / 2);
        context.stroke();
        return this.context.createPattern(tile, "repeat")
            ?? this.styleColor("--muted", "#686d71");
    }

    destroy() {
        if (this.destroyed) return;
        this.destroyed = true;
        this.map.off("move zoom resize", this.redrawListener);
        this.document.removeEventListener?.("themechange", this.themeListener);
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
    const hasPayload = hasOwn(options, "payload");
    const hasReachabilityUrl = hasOwn(options, "reachabilityUrl");
    if (hasPayload && hasReachabilityUrl) {
        throw new TypeError("Provide either a reachability URL or payload");
    }
    if (
        !hasPayload
        && (typeof options.reachabilityUrl !== "string" || options.reachabilityUrl.length === 0)
    ) {
        throw new TypeError("A reachability URL is required");
    }
    const initialPayload = hasPayload
        ? validateReachabilityPayload(options.payload)
        : { participants: [], stops: [] };

    const leaflet = options.leaflet ?? globalThis.L;
    if (!leaflet) throw new Error("Leaflet is unavailable");
    const fetchRequest = hasPayload ? null : options.fetch ?? globalThis.fetch;
    if (!hasPayload && typeof fetchRequest !== "function") throw new Error("Fetch is unavailable");
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

    const interactive = options.interactive !== false;
    const mapOptions = interactive ? undefined : {
        attributionControl: false,
        boxZoom: false,
        doubleClickZoom: false,
        dragging: false,
        keyboard: false,
        scrollWheelZoom: false,
        touchZoom: false,
        zoomControl: false,
    };
    const map = leaflet.map(root, mapOptions)
        .setView(options.center ?? DEFAULT_CENTER, options.zoom ?? DEFAULT_ZOOM);
    if (!interactive) root.removeAttribute?.("tabindex");
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
        initialPayload,
        controllerOptions,
    );

    if (hasPayload) {
        detachAbort();
        return controller;
    }

    try {
        const response = await fetchRequest(options.reachabilityUrl, {
            headers: { Accept: "application/json" },
            signal: abortController.signal,
        });
        if (!response.ok) {
            throw new Error(`Reachability request failed: ${response.status}`);
        }
        controller.setPayload(await response.json());
    } catch (error) {
        if (error?.name !== "AbortError") dispatchReachabilityError(root, error);
    } finally {
        detachAbort();
    }

    return controller;
}
