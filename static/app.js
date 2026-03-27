let map = null;
let markersLayer = null;

function initMap() {
    const dataEl = document.getElementById("map-data");
    if (!dataEl) return;

    const stops = JSON.parse(dataEl.dataset.stops || "[]");
    const pubs = JSON.parse(dataEl.dataset.pubs || "[]");

    if (stops.length === 0) return;

    const mapEl = document.getElementById("map");
    if (!mapEl) return;

    if (map) {
        map.remove();
        map = null;
    }

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

    if (bounds.length > 0) {
        map.fitBounds(bounds, { padding: [30, 30] });
    }
}

document.addEventListener("htmx:afterSwap", function () {
    if (document.getElementById("map-data")) {
        initMap();
    }
});
