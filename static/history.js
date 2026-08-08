const KEY = "meet_somewhere_recent_sessions";
const LEGACY_KEY = "pubfinder_sessions";
const LIMIT = 5;

function validSession(value) {
    return (
        value
        && typeof value === "object"
        && typeof value.code === "string"
        && value.code.length > 0
        && typeof value.name === "string"
    );
}

function getSessions() {
    try {
        const stored = JSON.parse(localStorage.getItem(KEY) || "[]");
        return Array.isArray(stored) ? stored.filter(validSession).slice(0, LIMIT) : [];
    } catch (_) {
        return [];
    }
}

function migrateLegacySessions() {
    let legacy = [];
    try {
        const stored = JSON.parse(localStorage.getItem(LEGACY_KEY) || "[]");
        legacy = Array.isArray(stored) ? stored.filter(validSession) : [];
    } catch (_) {
        legacy = [];
    }
    if (!legacy.length) return;
    const current = getSessions();
    const seen = new Set(current.map((item) => item.code));
    const migrated = legacy.filter((item) => {
        if (seen.has(item.code)) return false;
        seen.add(item.code);
        return true;
    });
    const merged = [...current, ...migrated].slice(0, LIMIT);
    try {
        localStorage.setItem(KEY, JSON.stringify(merged));
        localStorage.removeItem(LEGACY_KEY);
    } catch (_) {
        // A failed migration is harmless and can be retried on a later load.
    }
}

export function rememberSession(session) {
    if (!validSession(session)) return;

    const next = [session, ...getSessions().filter((item) => item.code !== session.code)]
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
