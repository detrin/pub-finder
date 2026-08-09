let activeSince = document.hasFocus() ? performance.now() : null;
let accumulatedMsec = 0;

function flush(useBeacon) {
  if (activeSince !== null) {
    accumulatedMsec += performance.now() - activeSince;
    activeSince = document.visibilityState === "visible" ? performance.now() : null;
  }
  const msec = Math.round(accumulatedMsec);
  if (msec < 1000) return;
  accumulatedMsec = 0;

  const body = new URLSearchParams({
    path: location.pathname,
    engagement_time_msec: String(msec),
  });
  if (useBeacon && navigator.sendBeacon) {
    navigator.sendBeacon("/e", body);
  } else {
    fetch("/e", { method: "POST", body, keepalive: true });
  }
}

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "hidden") {
    flush(true);
  } else {
    activeSince = performance.now();
  }
});

window.addEventListener("pagehide", () => flush(true));
