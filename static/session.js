const palette = ["#ff6658", "#dff0ff", "#ffd447", "#4dc694", "#2458df", "#b9a8ff"];
const dialogInvokers = new WeakMap();

export function showModalWithFocusReturn(dialog, invoker) {
    dialog.returnValue = "";
    dialogInvokers.set(dialog, invoker);
    dialog.showModal();
}

export function restoreDialogFocus(dialog, shouldFocus = true) {
    const invoker = dialogInvokers.get(dialog);
    dialogInvokers.delete(dialog);
    if (!shouldFocus || !invoker?.isConnected) return false;
    invoker.focus();
    return true;
}

export function participantColor(id) {
    return palette[Math.abs(Number(id) || 0) % palette.length];
}

function bindInviteCopy(root) {
    root.addEventListener("click", (event) => {
        const button = event.target.closest("[data-invite-copy]");
        if (!button || !root.contains(button) || !navigator.clipboard) return;
        const url = new URL(button.dataset.inviteUrl, window.location.origin);
        navigator.clipboard.writeText(url.href).then(() => {
            button.dataset.copied = "true";
            window.setTimeout(() => delete button.dataset.copied, 2000);
        }).catch(() => {});
    });
}

function bindStopPicker(root) {
    const dialog = document.querySelector("[data-stop-dialog]");
    const stops = parseStops(root.dataset.stops);
    if (!dialog || !stops.length) return;

    const searchInput = dialog.querySelector(".stop-picker__search");
    const list = dialog.querySelector(".stop-picker__list");
    const context = dialog.querySelector("[data-stop-picker-context]");
    let activeInput = null;
    let activeParticipantId = null;
    let activeFieldName = null;
    let activeInvoker = null;
    let pendingFocus = null;

    function resolveActiveInput() {
        if (activeParticipantId && activeFieldName) {
            const forms = root.querySelectorAll("form.stop-form");
            for (const form of forms) {
                const id = form.querySelector("[name=participant_id]");
                if (id && id.value === activeParticipantId) {
                    const input = form.querySelector(`[name=${activeFieldName}]`);
                    return input && !input.disabled ? input : null;
                }
            }
        }
        return activeInput && activeInput.isConnected ? activeInput : null;
    }

    function matches(query) {
        const normalized = normalize(query);
        const starts = [];
        const contains = [];
        for (const stop of stops) {
            const value = normalize(stop);
            if (!normalized || value.startsWith(normalized)) starts.push(stop);
            else if (value.includes(normalized)) contains.push(stop);
            if (starts.length + contains.length >= 50) break;
        }
        return starts.concat(contains);
    }

    function selectStop(stop) {
        const input = resolveActiveInput();
        if (input) {
            input.value = stop;
            pendingFocus = {
                participantId: activeParticipantId,
                fieldName: activeFieldName,
                stop,
                sourceForm: input.closest("form"),
            };
            input.dispatchEvent(new Event("change", { bubbles: true }));
        }
        dialog.close();
    }

    function render(query) {
        const items = matches(query);
        const children = items.map((stop) => {
            const item = document.createElement("button");
            item.type = "button";
            item.className = "stop-picker__item";
            item.setAttribute("role", "option");
            item.textContent = stop;
            item.addEventListener("click", () => selectStop(stop));
            return item;
        });
        if (!children.length) {
            const empty = document.createElement("li");
            empty.className = "stop-picker__empty";
            empty.textContent = "No stops found";
            children.push(empty);
        }
        list.replaceChildren(...children);
    }

    function openPicker(input) {
        if (input.disabled || typeof dialog.showModal !== "function") return;
        activeInput = input;
        activeInvoker = input;
        const form = input.closest("form");
        activeParticipantId = form?.querySelector("[name=participant_id]")?.value || null;
        activeFieldName = input.name || null;
        context.textContent = `${input.dataset.participantName || "Participant"} · ${input.dataset.stopDirection || ""}`;
        searchInput.value = "";
        render("");
        showModalWithFocusReturn(dialog, activeInvoker);
        searchInput.focus();
    }

    root.addEventListener("click", (event) => {
        const input = event.target.closest("[data-stop-input]");
        if (!input || !root.contains(input)) return;
        event.preventDefault();
        openPicker(input);
    });
    root.addEventListener("keydown", (event) => {
        const input = event.target.closest("[data-stop-input]");
        if (input && (event.key === "Enter" || event.key === " ")) {
            event.preventDefault();
            openPicker(input);
        }
    });
    searchInput.addEventListener("input", () => render(searchInput.value.trim()));
    searchInput.addEventListener("keydown", (event) => {
        const items = [...list.querySelectorAll(".stop-picker__item")];
        if (event.key === "Enter" && items[0]) {
            event.preventDefault();
            items[0].click();
        } else if (event.key === "ArrowDown" && items[0]) {
            event.preventDefault();
            items[0].focus();
        } else if (event.key === "ArrowUp" && items.length) {
            event.preventDefault();
            items[items.length - 1].focus();
        }
    });
    list.addEventListener("keydown", (event) => {
        const items = [...list.querySelectorAll(".stop-picker__item")];
        const index = items.indexOf(event.target);
        if (index < 0) return;
        if (event.key === "ArrowDown" && items[index + 1]) {
            event.preventDefault();
            items[index + 1].focus();
        } else if (event.key === "ArrowUp") {
            event.preventDefault();
            (items[index - 1] || searchInput).focus();
        }
    });
    dialog.addEventListener("click", (event) => {
        if (event.target === dialog) dialog.close();
    });
    dialog.addEventListener("close", () => {
        const focusTarget = resolveActiveInput();
        const restoredInvoker = restoreDialogFocus(dialog);
        activeInput = null;
        activeParticipantId = null;
        activeFieldName = null;
        activeInvoker = null;
        if (!restoredInvoker && focusTarget?.isConnected) focusTarget.focus();
    });
    document.addEventListener("htmx:beforeRequest", (event) => {
        if (pendingFocus && event.detail.elt === pendingFocus.sourceForm) {
            pendingFocus.xhr = event.detail.xhr;
        }
    });
    document.addEventListener("htmx:afterSwap", (event) => {
        if (!pendingFocus || !root.contains(event.detail.target)) return;
        if (event.detail.xhr !== pendingFocus.xhr) return;
        const participantId = pendingFocus.participantId;
        const fieldName = pendingFocus.fieldName;
        const forms = root.querySelectorAll("form.stop-form");
        for (const form of forms) {
            if (form.querySelector("[name=participant_id]")?.value !== participantId) continue;
            const input = form.querySelector(`[name=${fieldName}]`);
            if (input && !input.disabled && input.value === pendingFocus.stop) {
                pendingFocus = null;
                window.requestAnimationFrame(() => input.focus());
            }
            break;
        }
    });
}

function bindReturnCheckboxes(root) {
    root.addEventListener("change", (event) => {
        const checkbox = event.target.closest("[data-same-start-end]");
        if (!checkbox || !root.contains(checkbox)) return;
        const endStop = checkbox.closest("form")?.querySelector("[name=end_stop]");
        if (endStop) endStop.disabled = checkbox.checked;
    }, true);
}

function bindSseProtection(root) {
    const participantStream = root.querySelector("#session-participants");
    const pendingRequests = new Set();
    let deferred = false;
    const requestKey = (event) => event.detail?.xhr ?? event.detail?.elt;

    function focusedParticipantInput() {
        const active = document.activeElement;
        return active?.tagName === "INPUT" && participantStream?.contains?.(active);
    }

    function refreshDeferred() {
        if (!deferred || pendingRequests.size || focusedParticipantInput()) return;
        deferred = false;
        const code = root.dataset.sessionCode;
        const htmx = globalThis.htmx;
        if (code && htmx?.ajax) {
            htmx.ajax("GET", `/session/${encodeURIComponent(code)}/participants`, {
                target: "#session-participants-inner",
                swap: "innerHTML",
            });
        }
    }

    document.addEventListener("htmx:beforeRequest", (event) => {
        if (root.contains(event.detail?.elt)) pendingRequests.add(requestKey(event));
    });
    for (const eventName of ["htmx:afterRequest", "htmx:responseError", "htmx:sendError"]) {
        document.addEventListener(eventName, (event) => {
            pendingRequests.delete(requestKey(event));
            refreshDeferred();
        });
    }
    document.addEventListener("htmx:sseBeforeMessage", (event) => {
        if (event.target !== participantStream) return;
        if (pendingRequests.size || focusedParticipantInput()) {
            deferred = true;
            event.preventDefault();
        }
    });
    document.addEventListener("focusout", () => window.setTimeout(refreshDeferred, 0));
}

function bindRemoveConfirmation(root) {
    const dialog = document.querySelector("[data-remove-dialog]");
    if (!dialog) return;
    const form = dialog.querySelector("[data-remove-form]");
    const name = dialog.querySelector("[data-remove-participant-name]");
    const id = dialog.querySelector("[data-remove-participant-id]");
    let invoker = null;
    let pendingRemovalId = null;

    function findRemovalControl(participantId) {
        return [...root.querySelectorAll("[data-remove-participant]")].find(
            (button) => button.dataset.participantId === participantId
        );
    }

    function restoreRemovalFocus() {
        const sameParticipant = findRemovalControl(pendingRemovalId);
        const nextControl = sameParticipant
            || root.querySelector("[data-remove-participant]")
            || root.querySelector(".add-participant-form input");
        pendingRemovalId = null;
        if (nextControl?.isConnected) nextControl.focus();
    }

    root.addEventListener("click", (event) => {
        const button = event.target.closest("[data-remove-participant]");
        if (!button || !root.contains(button) || typeof dialog.showModal !== "function") return;
        invoker = button;
        name.textContent = button.dataset.participantName || "this participant";
        id.value = button.dataset.participantId || "";
        showModalWithFocusReturn(dialog, invoker);
    });
    dialog.querySelector("[data-dialog-cancel]").addEventListener("click", () => dialog.close());
    form.addEventListener("submit", () => {
        pendingRemovalId = id.value;
    });
    form.addEventListener("htmx:afterRequest", () => {
        dialog.close();
        window.setTimeout(restoreRemovalFocus, 0);
    });
    dialog.addEventListener("close", () => {
        restoreDialogFocus(dialog, !pendingRemovalId);
        invoker = null;
    });
}

function bindOccasionPresets(root) {
    const presets = {
        drinks: ["pub", "bar"],
        coffee: ["cafe"],
        food: ["restaurant"],
        anything: ["pub", "bar", "cafe", "restaurant"],
    };
    root.addEventListener("click", (event) => {
        const button = event.target.closest("[data-occasion]");
        if (!button || !root.contains(button)) return;
        const types = presets[button.dataset.occasion];
        if (!types) return;
        root.querySelectorAll("input[name=place_types]").forEach((input) => {
            input.checked = types.includes(input.value);
        });
        syncOccasionPresets(root, presets);
    });
    root.addEventListener("change", (event) => {
        if (event.target.matches("input[name=place_types]")) syncOccasionPresets(root, presets);
    });
    syncOccasionPresets(root, presets);
}

function bindStyledSelects(root) {
    const controls = () => [...root.querySelectorAll("[data-select]")];

    function elements(control) {
        return {
            trigger: control.querySelector("[data-select-trigger]"),
            input: control.querySelector("[data-select-input]"),
            menu: control.querySelector("[data-select-menu]"),
            options: [...control.querySelectorAll("[data-select-option]")],
        };
    }

    function close(control) {
        const { trigger, menu } = elements(control);
        if (trigger) trigger.setAttribute("aria-expanded", "false");
        if (menu) menu.hidden = true;
    }

    function open(control) {
        controls().forEach((candidate) => {
            if (candidate !== control) close(candidate);
        });
        const { trigger, menu } = elements(control);
        if (trigger) trigger.setAttribute("aria-expanded", "true");
        if (menu) menu.hidden = false;
    }

    function choose(control, option) {
        const { trigger, input, options } = elements(control);
        if (input) input.value = option.dataset.value || "";
        if (trigger) trigger.textContent = option.textContent;
        options.forEach((candidate) => {
            candidate.setAttribute("aria-selected", String(candidate === option));
        });
        close(control);
    }

    root.addEventListener("click", (event) => {
        const option = event.target.closest("[data-select-option]");
        if (option && root.contains(option)) {
            event.preventDefault();
            choose(option.closest("[data-select]"), option);
            return;
        }
        const selectTrigger = event.target.closest("[data-select-trigger]");
        if (!selectTrigger || !root.contains(selectTrigger)) return;
        event.preventDefault();
        const control = selectTrigger.closest("[data-select]");
        const { menu } = elements(control);
        if (menu?.hidden) open(control);
        else close(control);
    });

    root.addEventListener("keydown", (event) => {
        const trigger = event.target.closest("[data-select-trigger]");
        if (trigger && root.contains(trigger)) {
            const control = trigger.closest("[data-select]");
            const { menu, options } = elements(control);
            if (["Enter", " ", "ArrowDown", "ArrowUp"].includes(event.key)) {
                event.preventDefault();
                if (menu?.hidden) open(control);
                const selected = options.find((option) => option.getAttribute("aria-selected") === "true");
                const target = event.key === "ArrowUp" ? options.at(-1) : selected || options[0];
                target?.focus();
            } else if (event.key === "Escape") {
                close(control);
            }
            return;
        }

        const option = event.target.closest("[data-select-option]");
        if (!option || !root.contains(option)) return;
        const control = option.closest("[data-select]");
        const { trigger: controlTrigger, options } = elements(control);
        const index = options.indexOf(option);
        if (event.key === "Escape") {
            event.preventDefault();
            close(control);
            controlTrigger?.focus();
        } else if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            choose(control, option);
            controlTrigger?.focus();
        } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
            event.preventDefault();
            const offset = event.key === "ArrowDown" ? 1 : -1;
            (options[index + offset] || options[(index + offset + options.length) % options.length])?.focus();
        } else if (event.key === "Home") {
            event.preventDefault();
            options[0]?.focus();
        } else if (event.key === "End") {
            event.preventDefault();
            options.at(-1)?.focus();
        }
    });

    document.addEventListener("click", (event) => {
        if (!root.contains(event.target)) controls().forEach(close);
    });
}

function syncOccasionPresets(root, presets) {
    const selected = [...root.querySelectorAll("input[name=place_types]")]
        .filter((input) => input.checked)
        .map((input) => input.value)
        .sort();
    root.querySelectorAll("[data-occasion]").forEach((button) => {
        const types = [...presets[button.dataset.occasion]].sort();
        button.setAttribute("aria-pressed", String(
            types.length === selected.length && types.every((type, index) => type === selected[index])
        ));
    });
}

function updateReadiness(root) {
    const submit = root.querySelector("[data-search-submit]");
    const status = root.querySelector("[data-session-readiness]");
    if (!submit || !status) return;
    const forms = [...root.querySelectorAll("form.stop-form")];
    if (forms.length < 2) {
        submit.disabled = true;
        status.textContent = root.dataset.readinessAdd || "Add one more participant.";
        return;
    }
    const incomplete = forms.find((form) => {
        const start = form.querySelector("[name=start_stop]")?.value.trim();
        const end = form.querySelector("[name=end_stop]")?.value.trim();
        const same = form.querySelector("[data-same-start-end]")?.checked;
        return !start || (!same && !end);
    });
    if (!incomplete) {
        submit.disabled = false;
        status.textContent = root.dataset.readinessReady || "Everyone is ready.";
        return;
    }
    const start = incomplete.querySelector("[name=start_stop]")?.value.trim();
    const name = incomplete.querySelector("[data-participant-name]")?.dataset.participantName
        || root.dataset.readinessParticipant
        || "A participant";
    submit.disabled = true;
    const message = start
        ? root.dataset.readinessEnd || "{name} needs an end stop."
        : root.dataset.readinessStartAndEnd || "{name} needs start and end stops.";
    status.textContent = message.replace("{name}", name);
}

function normalize(value) {
    return String(value).normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
}

function parseStops(value) {
    try {
        const stops = JSON.parse(value || "[]");
        return Array.isArray(stops) ? stops.filter((stop) => typeof stop === "string") : [];
    } catch (_) {
        return [];
    }
}

export function initSessionUi() {
    const root = document.querySelector("[data-session-workspace]");
    if (!root || root.dataset.bound === "true") return;
    root.dataset.bound = "true";
    bindInviteCopy(root);
    bindStopPicker(root);
    bindReturnCheckboxes(root);
    bindSseProtection(root);
    bindRemoveConfirmation(root);
    bindOccasionPresets(root);
    bindStyledSelects(root);
    updateReadiness(root);
    document.addEventListener("htmx:afterSwap", (event) => {
        if (root.contains(event.detail.target)) updateReadiness(root);
    });
    document.addEventListener("htmx:sseMessage", (event) => {
        if (root.contains(event.detail.elt)) updateReadiness(root);
    });
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initSessionUi, { once: true });
} else {
    initSessionUi();
}
