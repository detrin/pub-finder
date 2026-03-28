var map = null;
var markersLayer = null;
var currentMapHash = null;

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

// ── Stop autocomplete ──────────────────────────────────────

(function () {
    var stops = window.__ALL_STOPS || [];
    if (!stops.length) return;

    // Build ASCII-normalized lookup: [{name: "Anděl", norm: "andel"}, ...]
    var stopData = stops.map(function (s) {
        return { name: s, norm: toAscii(s).toLowerCase() };
    });

    function toAscii(str) {
        return str.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
    }

    function matchStops(query) {
        if (!query || query.length < 1) return [];
        var q = toAscii(query).toLowerCase();
        var starts = [];
        var contains = [];
        for (var i = 0; i < stopData.length; i++) {
            var idx = stopData[i].norm.indexOf(q);
            if (idx === 0) starts.push(stopData[i]);
            else if (idx > 0) contains.push(stopData[i]);
            if (starts.length + contains.length >= 30) break;
        }
        return starts.concat(contains);
    }

    function highlightMatch(name, query) {
        var q = toAscii(query).toLowerCase();
        var norm = toAscii(name).toLowerCase();
        var idx = norm.indexOf(q);
        if (idx < 0) return document.createTextNode(name);
        var frag = document.createDocumentFragment();
        if (idx > 0) frag.appendChild(document.createTextNode(name.slice(0, idx)));
        var mark = document.createElement("mark");
        mark.textContent = name.slice(idx, idx + query.length);
        frag.appendChild(mark);
        if (idx + query.length < name.length)
            frag.appendChild(document.createTextNode(name.slice(idx + query.length)));
        return frag;
    }

    function initAutocomplete(input) {
        var list = input.nextElementSibling;
        if (!list || !list.classList.contains("stop-ac-list")) return;
        var activeIdx = -1;
        var items = [];

        function show(matches, query) {
            list.innerHTML = "";
            items = [];
            activeIdx = -1;
            if (!matches.length) { list.hidden = true; return; }
            matches.forEach(function (m, i) {
                var li = document.createElement("li");
                li.className = "stop-ac-item";
                li.appendChild(highlightMatch(m.name, query));
                li.addEventListener("mousedown", function (e) {
                    e.preventDefault();
                    select(m.name);
                });
                list.appendChild(li);
                items.push(li);
            });
            list.hidden = false;
        }

        function hide() {
            list.hidden = true;
            items = [];
            activeIdx = -1;
        }

        function select(name) {
            input.value = name;
            hide();
            input.dispatchEvent(new Event("change", { bubbles: true }));
        }

        function setActive(idx) {
            if (items[activeIdx]) items[activeIdx].removeAttribute("data-active");
            activeIdx = idx;
            if (items[activeIdx]) {
                items[activeIdx].setAttribute("data-active", "");
                items[activeIdx].scrollIntoView({ block: "nearest" });
            }
        }

        input.addEventListener("input", function () {
            var val = input.value.trim();
            if (val.length < 1) { hide(); return; }
            show(matchStops(val), val);
        });

        input.addEventListener("focus", function () {
            var val = input.value.trim();
            if (val.length >= 1) show(matchStops(val), val);
        });

        input.addEventListener("blur", function () {
            // Delay to allow mousedown on list items
            setTimeout(hide, 150);
        });

        input.addEventListener("keydown", function (e) {
            if (list.hidden) return;
            if (e.key === "ArrowDown") {
                e.preventDefault();
                setActive(Math.min(activeIdx + 1, items.length - 1));
            } else if (e.key === "ArrowUp") {
                e.preventDefault();
                setActive(Math.max(activeIdx - 1, 0));
            } else if (e.key === "Enter" && activeIdx >= 0) {
                e.preventDefault();
                var name = items[activeIdx].textContent;
                select(name);
            } else if (e.key === "Escape") {
                hide();
            }
        });
    }

    // Init all existing stop inputs
    document.querySelectorAll("[data-stop-input]").forEach(initAutocomplete);

    // Re-init after HTMX swaps (participant list gets re-rendered)
    document.addEventListener("htmx:afterSwap", function () {
        document.querySelectorAll("[data-stop-input]").forEach(function (input) {
            // Only init if not already initialized
            if (!input._acInit) {
                initAutocomplete(input);
                input._acInit = true;
            }
        });
    });

    // Mark initially bound inputs
    document.querySelectorAll("[data-stop-input]").forEach(function (input) {
        input._acInit = true;
    });
})();

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
