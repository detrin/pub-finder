const KEY = "meet_somewhere_recent_sessions";
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
