const banner = document.getElementById("freshness-banner");
const bannerText = document.getElementById("freshness-text");
const lightbox = document.getElementById("lightbox");
const lightboxImage = document.getElementById("lightbox-image");
const lightboxCaption = document.getElementById("lightbox-caption");
const lightboxTitle = document.getElementById("lightbox-title");
const lightboxMeta = document.getElementById("lightbox-meta");
const lightboxCaptionText = document.getElementById("lightbox-caption-text");
const lightboxDescription = document.getElementById("lightbox-description");
const lightboxPrev = document.getElementById("lightbox-prev");
const lightboxNext = document.getElementById("lightbox-next");
const confirmModal = document.getElementById("confirm-modal");
const confirmModalMessage = document.getElementById("confirm-modal-message");
const confirmModalConfirm = document.getElementById("confirm-modal-confirm");
const confirmModalCancel = document.getElementById("confirm-modal-cancel");

function setBanner(data) {
  if (!banner || !data) return;
  const dirty = data.repo_dirty;
  const pullFailed = data.pull_result === "error";
  const state = pullFailed ? "error" : dirty ? "warning" : "ok";
  banner.dataset.state = state;
  if (bannerText) bannerText.textContent = data.message || "Workspace status unavailable.";
  // Hide banner after a moment if everything is fine
  if (state === "ok") {
    setTimeout(() => { banner.style.display = "none"; }, 2500);
  }
}

async function fetchFreshness(url, options = {}) {
  try {
    const response = await fetch(url, {headers: {"Accept": "application/json"}, ...options});
    setBanner(await response.json());
  } catch (_) {
    if (bannerText) bannerText.textContent = "Workspace status unavailable.";
    if (banner) banner.dataset.state = "error";
  }
}

if (banner) {
  const button = banner.querySelector("button");
  if (button) {
    button.addEventListener("click", () => {
      if (bannerText) bannerText.textContent = "Updating…";
      fetchFreshness(button.dataset.updateUrl, {method: "POST"});
    });
  }
  if (!sessionStorage.getItem("arquimedes-freshness-checked")) {
    sessionStorage.setItem("arquimedes-freshness-checked", "1");
    fetchFreshness("/api/freshness");
  } else {
    // Already checked this session — hide banner immediately
    banner.style.display = "none";
  }
}

// ── Confirm modal + scroll restore ───────────────

if (confirmModal && confirmModalMessage && confirmModalConfirm && confirmModalCancel) {
  let pendingAction = null;

  const restoreY = sessionStorage.getItem("arquimedes-restore-scroll-y");
  if (restoreY !== null) {
    sessionStorage.removeItem("arquimedes-restore-scroll-y");
    window.addEventListener("load", () => {
      window.scrollTo({top: Number(restoreY) || 0, behavior: "instant"});
    }, {once: true});
  }

  function closeConfirmModal() {
    confirmModal.setAttribute("hidden", "");
    document.body.classList.remove("lightbox-open");
    pendingAction = null;
  }

  function openConfirmModal(message, action) {
    pendingAction = action;
    confirmModalMessage.textContent = message || "¿Confirmar esta acción?";
    confirmModal.removeAttribute("hidden");
    document.body.classList.add("lightbox-open");
  }

  function preserveScroll(node) {
    if (node?.dataset?.preserveScroll === "true" || node?.form?.dataset?.preserveScroll === "true") {
      sessionStorage.setItem("arquimedes-restore-scroll-y", String(window.scrollY || window.pageYOffset || 0));
    }
  }

  document.addEventListener("submit", (event) => {
    const form = event.target instanceof HTMLFormElement ? event.target : null;
    if (!form || form.dataset.confirmBypass === "true") return;
    const submitter = event.submitter instanceof HTMLElement ? event.submitter : null;
    const message = submitter?.dataset.confirmMessage || form.dataset.confirmMessage;
    if (!message) return;
    event.preventDefault();
    openConfirmModal(message, () => {
      preserveScroll(submitter || form);
      form.dataset.confirmBypass = "true";
      if (submitter instanceof HTMLButtonElement || submitter instanceof HTMLInputElement) {
        form.requestSubmit(submitter);
      } else {
        form.requestSubmit();
      }
      delete form.dataset.confirmBypass;
    });
  }, true);

  confirmModalConfirm.addEventListener("click", () => {
    const action = pendingAction;
    closeConfirmModal();
    if (action) action();
  });

  confirmModalCancel.addEventListener("click", closeConfirmModal);
  confirmModal.addEventListener("click", (event) => {
    if (event.target === confirmModal) closeConfirmModal();
  });
  document.addEventListener("keydown", (event) => {
    if (confirmModal.hasAttribute("hidden")) return;
    if (event.key === "Escape") closeConfirmModal();
  });
}

// ── Lightbox ──────────────────────────────────────

if (lightbox && lightboxImage) {
  let zoomItems = [];
  let zoomIndex = -1;

  function setLightboxText(node) {
    const title = node.dataset.zoomTitle || "";
    const meta = node.dataset.zoomMeta || "";
    const caption = node.dataset.zoomCaption || "";
    const description = node.dataset.zoomDescription || "";
    if (lightboxTitle) lightboxTitle.textContent = title;
    if (lightboxMeta) {
      lightboxMeta.textContent = meta;
      lightboxMeta.hidden = !meta;
    }
    if (lightboxCaptionText) {
      lightboxCaptionText.textContent = caption;
      lightboxCaptionText.hidden = !caption;
    }
    if (lightboxDescription) {
      lightboxDescription.textContent = description;
      lightboxDescription.hidden = !description;
    }
    if (lightboxCaption) lightboxCaption.hidden = !(title || meta || caption || description);
  }

  function openLightbox(node) {
    const group = node.dataset.zoomGroup || "";
    zoomItems = [...document.querySelectorAll(`[data-zoom-src]${group ? `[data-zoom-group="${group}"]` : ""}`)];
    zoomIndex = Math.max(0, zoomItems.indexOf(node));
    lightboxImage.src = node.dataset.zoomSrc || "";
    lightboxImage.alt = node.dataset.zoomAlt || "";
    setLightboxText(node);
    if (lightboxPrev) lightboxPrev.hidden = zoomItems.length < 2;
    if (lightboxNext) lightboxNext.hidden = zoomItems.length < 2;
    lightbox.removeAttribute("hidden");
    document.body.classList.add("lightbox-open");
  }

  function moveLightbox(step) {
    if (zoomItems.length < 2) return;
    zoomIndex = (zoomIndex + step + zoomItems.length) % zoomItems.length;
    openLightbox(zoomItems[zoomIndex]);
  }

  function closeLightbox() {
    lightbox.setAttribute("hidden", "");
    lightboxImage.removeAttribute("src");
    if (lightboxCaption) lightboxCaption.hidden = true;
    document.body.classList.remove("lightbox-open");
  }

  document.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const node = target ? target.closest("[data-zoom-src]") : null;
    if (!node) return;
    event.preventDefault();
    openLightbox(node);
  });

  lightbox.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (target?.closest(".lightbox-prev")) return moveLightbox(-1);
    if (target?.closest(".lightbox-next")) return moveLightbox(1);
    if (event.target === lightbox || target?.closest(".lightbox-close")) closeLightbox();
  });

  document.addEventListener("keydown", (event) => {
    if (lightbox.hasAttribute("hidden")) return;
    if (event.key === "Escape") closeLightbox();
    if (event.key === "ArrowLeft") moveLightbox(-1);
    if (event.key === "ArrowRight") moveLightbox(1);
  });
}
