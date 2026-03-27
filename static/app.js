let map = null;
let markersLayer = null;
let currentMapHash = null;

function initMap() {
    const dataEl = document.getElementById("map-data");
    if (!dataEl) return;

    const stopsRaw = dataEl.dataset.stops || "[]";
    const pubsRaw = dataEl.dataset.pubs || "[]";
    const participantsRaw = dataEl.dataset.participants || "[]";
    const dataHash = stopsRaw + pubsRaw + participantsRaw;

    // Skip if map already shows this data
    if (map && dataHash === currentMapHash) return;

    const stops = JSON.parse(stopsRaw);
    const pubs = JSON.parse(pubsRaw);
    const participants = JSON.parse(participantsRaw);

    if (stops.length === 0) return;

    const mapEl = document.getElementById("map");
    if (!mapEl) return;

    if (map) {
        map.remove();
        map = null;
    }

    currentMapHash = dataHash;

    map = L.map("map").setView([stops[0].lat, stops[0].lon], 13);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: "&copy; OpenStreetMap contributors",
    }).addTo(map);

    markersLayer = L.layerGroup().addTo(map);

    const stopIcon = L.divIcon({
        html: '<div style="background:#2563eb;color:white;border-radius:50%;width:28px;height:28px;display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:12px;border:2px solid white;box-shadow:0 2px 4px rgba(0,0,0,0.3);">S</div>',
        iconSize: [28, 28],
        iconAnchor: [14, 14],
        className: "",
    });

    const pubIcon = L.divIcon({
        html: '<div style="background:#dc2626;color:white;border-radius:50%;width:22px;height:22px;display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:10px;border:2px solid white;box-shadow:0 2px 4px rgba(0,0,0,0.3);">P</div>',
        iconSize: [22, 22],
        iconAnchor: [11, 11],
        className: "",
    });

    const bounds = [];

    stops.forEach(function (stop, i) {
        const marker = L.marker([stop.lat, stop.lon], { icon: stopIcon });
        marker.bindPopup("<strong>" + stop.name + "</strong><br>Stop #" + (i + 1));
        markersLayer.addLayer(marker);
        bounds.push([stop.lat, stop.lon]);
    });

    pubs.forEach(function (pub) {
        const marker = L.marker([pub.lat, pub.lon], { icon: pubIcon });
        let popup = "<strong>" + pub.name + "</strong>";
        if (pub.rating) popup += "<br>Rating: " + pub.rating + "/5 (" + pub.rating_count + ")";
        popup += "<br>Near: " + pub.stop;
        if (pub.url) popup += '<br><a href="' + pub.url + '" target="_blank">Google Maps</a>';
        marker.bindPopup(popup);
        markersLayer.addLayer(marker);
        bounds.push([pub.lat, pub.lon]);
    });

    // Participant colors for distinguishing people
    const colors = ["#7c3aed", "#0891b2", "#c026d3", "#ea580c", "#4f46e5", "#059669"];

    participants.forEach(function (p, i) {
        // Find color index by unique participant name
        const nameIndex = [...new Set(participants.map(x => x.name))].indexOf(p.name);
        const color = colors[nameIndex % colors.length];
        const label = p.type === "from" ? "F" : "T";
        const icon = L.divIcon({
            html: '<div style="background:' + color + ';color:white;border-radius:50%;width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:10px;border:2px solid white;box-shadow:0 2px 4px rgba(0,0,0,0.3);">' + label + '</div>',
            iconSize: [24, 24],
            iconAnchor: [12, 12],
            className: "",
        });
        const marker = L.marker([p.lat, p.lon], { icon: icon });
        marker.bindPopup("<strong>" + p.name + "</strong><br>" + (p.type === "from" ? "From: " : "To: ") + p.stop);
        markersLayer.addLayer(marker);
        bounds.push([p.lat, p.lon]);
    });

    if (bounds.length > 0) {
        map.fitBounds(bounds, { padding: [30, 30] });
    }
}

document.addEventListener("htmx:afterSwap", function (event) {
    if (event.detail.target && event.detail.target.id === "results-section") {
        initMap();
    }
});

// ── Session history (localStorage) ──────────────────────────

function saveSessionToHistory(code, name) {
    if (!code) return;
    var history = JSON.parse(localStorage.getItem("pubfinder_sessions") || "[]");
    // Remove existing entry for this code
    history = history.filter(function (s) { return s.code !== code; });
    // Add to front
    history.unshift({ code: code, name: name || code, ts: Date.now() });
    // Keep last 10
    history = history.slice(0, 10);
    localStorage.setItem("pubfinder_sessions", JSON.stringify(history));
}

function renderSessionHistory() {
    var container = document.getElementById("session-history");
    if (!container) return;
    var history = JSON.parse(localStorage.getItem("pubfinder_sessions") || "[]");
    if (history.length === 0) {
        container.style.display = "none";
        return;
    }
    container.style.display = "";
    var list = container.querySelector(".session-history-list");
    list.innerHTML = "";
    history.forEach(function (s) {
        var li = document.createElement("li");
        var a = document.createElement("a");
        a.href = "/session/" + s.code;
        a.textContent = s.name;
        li.appendChild(a);
        list.appendChild(li);
    });
}

// Auto-save current session if on session page
(function () {
    var sessionEl = document.querySelector("[data-session-code]");
    if (sessionEl) {
        saveSessionToHistory(
            sessionEl.dataset.sessionCode,
            sessionEl.dataset.sessionName
        );
    }
    renderSessionHistory();
})();
