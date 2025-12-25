function _closest(el, selector) {
  if (!el) return null;
  if (el.closest) return el.closest(selector);
  return null;
}

async function copyToClipboard(text) {
  const value = String(text || "");
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(value);
      return true;
    }
  } catch (_) {}

  try {
    const ta = document.createElement("textarea");
    ta.value = value;
    ta.setAttribute("readonly", "readonly");
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    ta.style.top = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch (_) {
    return false;
  }
}

function openModalById(modalId) {
  const el = document.getElementById(modalId);
  if (!el) return;
  el.classList.add("open");
  document.body.classList.add("modal-open");
}

function closeModalEl(el) {
  if (!el) return;
  el.classList.remove("open");
  document.body.classList.remove("modal-open");
}

function switchTab(tabButton) {
  const tabName = tabButton.getAttribute("data-tab");
  if (!tabName) return;

  const modal = _closest(tabButton, ".modal") || document;
  const tabs = modal.querySelectorAll(".tab[data-tab]");
  tabs.forEach((t) => t.classList.remove("active"));
  tabButton.classList.add("active");

  const panels = modal.querySelectorAll(".tab-panel[data-tab-panel]");
  panels.forEach((p) => {
    const name = p.getAttribute("data-tab-panel");
    p.style.display = name === tabName ? "" : "none";
  });
}

document.addEventListener("click", async (e) => {
  const openBtn = _closest(e.target, "[data-open-modal]");
  if (openBtn) {
    openModalById(openBtn.getAttribute("data-open-modal"));
    return;
  }

  const closeBtn = _closest(e.target, "[data-close-modal]");
  if (closeBtn) {
    closeModalEl(_closest(closeBtn, "[data-modal]"));
    return;
  }

  if (e.target && e.target.classList && e.target.classList.contains("modal-backdrop")) {
    closeModalEl(e.target);
    return;
  }

  const tabBtn = _closest(e.target, ".tab[data-tab]");
  if (tabBtn) {
    switchTab(tabBtn);
    return;
  }

  const copyBtn = _closest(e.target, "[data-copy]");
  if (copyBtn) {
    const text = copyBtn.getAttribute("data-copy") || "";
    const ok = await copyToClipboard(text);
    const old = copyBtn.textContent;
    copyBtn.textContent = ok ? "Copied" : "Copy failed";
    setTimeout(() => {
      copyBtn.textContent = old;
    }, 1200);
  }
});

document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("[data-modal][data-auto-open='1']").forEach((el) => {
    if (el.id) openModalById(el.id);
  });
});

