const KEY = "meet_somewhere_recent_sessions";
const LEGACY_KEY = "pubfinder_sessions";
const LIMIT = 5;

function normalizeSession(value) {
    if (!(
        value
        && typeof value === "object"
        && typeof value.code === "string"
        && value.code.length > 0
        && typeof value.name === "string"
    )) return null;
    return { code: value.code, name: value.name };
}

function readSessions(key) {
    try {
        const stored = JSON.parse(localStorage.getItem(key) || "[]");
        return Array.isArray(stored)
            ? stored.map(normalizeSession).filter((session) => session !== null)
            : [];
    } catch (_) {
        return [];
    }
}

function uniqueSessions(sessions) {
    const seen = new Set();
    return sessions.filter((session) => {
        if (seen.has(session.code)) return false;
        seen.add(session.code);
        return true;
    });
}

function getSessions() {
    return uniqueSessions(readSessions(KEY)).slice(0, LIMIT);
}

function migrateLegacySessions() {
    const merged = uniqueSessions([
        ...readSessions(KEY),
        ...readSessions(LEGACY_KEY),
    ]).slice(0, LIMIT);
    try {
        localStorage.setItem(KEY, JSON.stringify(merged));
        localStorage.removeItem?.(LEGACY_KEY);
    } catch (_) {
        // A failed migration is harmless and can be retried on a later load.
    }
}

export function rememberSession(session) {
    const normalized = normalizeSession(session);
    if (!normalized) return;

    const next = [normalized, ...getSessions().filter((item) => item.code !== normalized.code)]
        .slice(0, LIMIT);

    try {
        localStorage.setItem(KEY, JSON.stringify(next));
    } catch (_) {
        // Storage can be unavailable or full. Session use does not depend on it.
    }
}

export function renderRecentSessions() {
    const root = document.querySelector("[data-session-history]");
    if (!root) return;

    const sessions = getSessions();
    root.hidden = sessions.length === 0;
    root.replaceChildren(
        ...sessions.map(({ code, name }) => {
            const link = document.createElement("a");
            link.href = `/session/${encodeURIComponent(code)}`;
            link.textContent = name;
            return link;
        }),
    );
}

export function initSessionHistory() {
    migrateLegacySessions();
    const session = document.querySelector("[data-session-code]");
    if (session) {
        rememberSession({
            code: session.dataset.sessionCode,
            name: session.dataset.sessionName,
        });
    }
    renderRecentSessions();
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initSessionHistory, { once: true });
} else {
    initSessionHistory();
}
