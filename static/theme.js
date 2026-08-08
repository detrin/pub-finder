export function initThemeToggle() {
  const button = document.querySelector("[data-theme-toggle]");
  if (!button || button.dataset.bound === "true") return;
  button.dataset.bound = "true";
  button.addEventListener("click", () => {
    const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    document.documentElement.style.colorScheme = next;
    localStorage.setItem("pubfinder_theme", next);
    button.setAttribute("aria-label", next === "dark" ? "Use light theme" : "Use dark theme");
  });
}
