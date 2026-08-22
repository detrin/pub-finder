const MAX_GRID_SIZE = 96;

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

export function estimateNearestStopGrid(points, width, height, walkingMinutesPerUnit) {
    const gridWidth = Math.max(1, Math.min(MAX_GRID_SIZE, Math.floor(width)));
    const gridHeight = Math.max(1, Math.min(MAX_GRID_SIZE, Math.floor(height)));
    const values = new Float64Array(gridWidth * gridHeight);
    values.fill(Number.NaN);
    if (!Number.isFinite(walkingMinutesPerUnit) || walkingMinutesPerUnit < 0) {
        return { width: gridWidth, height: gridHeight, values };
    }

    const validPoints = points.filter((point) => (
        Number.isFinite(point.x)
        && Number.isFinite(point.y)
    ));
    for (let y = 0; y < gridHeight; y += 1) {
        for (let x = 0; x < gridWidth; x += 1) {
            let nearestPoint = null;
            let nearestDistanceSquared = Number.POSITIVE_INFINITY;
            for (const point of validPoints) {
                const dx = point.x - (x + 0.5);
                const dy = point.y - (y + 0.5);
                const distanceSquared = dx * dx + dy * dy;
                if (distanceSquared < nearestDistanceSquared) {
                    nearestPoint = point;
                    nearestDistanceSquared = distanceSquared;
                }
            }
            if (nearestPoint && Number.isFinite(nearestPoint.value)) {
                values[y * gridWidth + x] = nearestPoint.value
                    + Math.sqrt(nearestDistanceSquared) * walkingMinutesPerUnit;
            }
        }
    }
    return { width: gridWidth, height: gridHeight, values };
}
