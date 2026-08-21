let lifecycleBound = false;

function dispatchThemeChange(theme) {
  const EventConstructor = document.defaultView?.CustomEvent ?? globalThis.CustomEvent;
  const event = typeof EventConstructor === "function"
    ? new EventConstructor("themechange", { detail: { theme } })
    : { type: "themechange", detail: { theme } };
  document.dispatchEvent?.(event);
}

export function initThemeToggle() {
  const button = document.querySelector("[data-theme-toggle]");
  if (button && button.dataset.bound !== "true") {
    button.dataset.bound = "true";
    button.setAttribute(
      "aria-label",
      document.documentElement.dataset.theme === "dark" ? "Use light theme" : "Use dark theme"
    );
    button.addEventListener("click", () => {
      const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
      document.documentElement.dataset.theme = next;
      document.documentElement.style.colorScheme = next;
      localStorage.setItem("pubfinder_theme", next);
      button.setAttribute("aria-label", next === "dark" ? "Use light theme" : "Use dark theme");
      dispatchThemeChange(next);
    });
  }
  if (lifecycleBound) return;
  lifecycleBound = true;
  document.addEventListener("htmx:afterSwap", initThemeToggle);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initThemeToggle, { once: true });
} else {
  initThemeToggle();
}
