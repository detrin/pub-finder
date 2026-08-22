const MAX_GRID_SIZE = 96;
const NEIGHBOUR_COUNT = 8;

export function selectLayerValues(payload, participantId) {
    if (participantId == null) {
        return payload.stops.map((stop) => stop.group_max_minutes);
    }

    const index = payload.participants.findIndex((person) => person.id === participantId);
    if (index < 0) {
        throw new RangeError(`Unknown participant ${participantId}`);
    }
    return payload.stops.map((stop) => stop.participant_minutes[index] ?? null);
}

export function classifyTime(value, threshold, step = 15) {
    if (
        value == null
        || !Number.isFinite(value)
        || !Number.isFinite(threshold)
        || !Number.isFinite(step)
        || step <= 0
    ) {
        return null;
    }
    return Math.max(0, Math.min(4, Math.ceil((value - threshold) / step) + 1));
}

export function interpolateGrid(points, width, height, power = 2) {
    const gridWidth = Math.max(1, Math.min(MAX_GRID_SIZE, Math.floor(width)));
    const gridHeight = Math.max(1, Math.min(MAX_GRID_SIZE, Math.floor(height)));
    const values = new Float64Array(gridWidth * gridHeight);
    values.fill(Number.NaN);

    const validPoints = points.filter((point) => (
        Number.isFinite(point.x)
        && Number.isFinite(point.y)
        && Number.isFinite(point.value)
    ));
    if (validPoints.length === 0) {
        return { width: gridWidth, height: gridHeight, values };
    }

    const nearestDistances = new Float64Array(NEIGHBOUR_COUNT);
    const nearestValues = new Float64Array(NEIGHBOUR_COUNT);
    const exponent = power / 2;

    for (let y = 0; y < gridHeight; y += 1) {
        const cellY = y + 0.5;
        for (let x = 0; x < gridWidth; x += 1) {
            const cellX = x + 0.5;
            let nearestCount = 0;
            let exactValue = null;

            for (const point of validPoints) {
                const dx = point.x - cellX;
                const dy = point.y - cellY;
                const distanceSquared = dx * dx + dy * dy;
                if (distanceSquared === 0) {
                    exactValue = point.value;
                    break;
                }
                if (
                    nearestCount === NEIGHBOUR_COUNT
                    && distanceSquared >= nearestDistances[nearestCount - 1]
                ) {
                    continue;
                }

                let insertAt = Math.min(nearestCount, NEIGHBOUR_COUNT - 1);
                while (insertAt > 0 && distanceSquared < nearestDistances[insertAt - 1]) {
                    if (insertAt < NEIGHBOUR_COUNT) {
                        nearestDistances[insertAt] = nearestDistances[insertAt - 1];
                        nearestValues[insertAt] = nearestValues[insertAt - 1];
                    }
                    insertAt -= 1;
                }
                nearestDistances[insertAt] = distanceSquared;
                nearestValues[insertAt] = point.value;
                nearestCount = Math.min(NEIGHBOUR_COUNT, nearestCount + 1);
            }

            const gridIndex = y * gridWidth + x;
            if (exactValue != null) {
                values[gridIndex] = exactValue;
                continue;
            }

            let weightedTotal = 0;
            let totalWeight = 0;
            for (let index = 0; index < nearestCount; index += 1) {
                const weight = 1 / (nearestDistances[index] ** exponent);
                weightedTotal += nearestValues[index] * weight;
                totalWeight += weight;
            }
            values[gridIndex] = weightedTotal / totalWeight;
        }
    }

    return { width: gridWidth, height: gridHeight, values };
}
