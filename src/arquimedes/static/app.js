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
  banner.dataset.state = data.compiled_at ? "ok" : "warning";
  if (bannerText) bannerText.textContent = data.message || "";
}

async function fetchFreshness(url) {
  try {
    const response = await fetch(url, {headers: {"Accept": "application/json"}});
    setBanner(await response.json());
  } catch (_) {
    if (bannerText) bannerText.textContent = "Freshness status unavailable.";
    if (banner) banner.dataset.state = "error";
  }
}

if (banner) {
  fetchFreshness("/api/freshness");
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

// ── Figure delete mode ────────────────────────────

function setFigureDeleteMode(form, enabled) {
  if (!(form instanceof HTMLFormElement)) return;
  form.dataset.deleteMode = enabled ? "true" : "false";
  form.querySelectorAll("[data-figure-delete-controls], [data-figure-delete-pick]").forEach((node) => {
    if (!(node instanceof HTMLElement)) return;
    node.hidden = !enabled;
  });
  if (!enabled) {
    form.querySelectorAll('input[type="checkbox"][name="figure_sidecar"]').forEach((input) => {
      if (input instanceof HTMLInputElement) input.checked = false;
    });
  }
}

document.addEventListener("click", (event) => {
  const target = event.target instanceof Element ? event.target : null;
  const toggle = target?.closest("[data-figure-delete-toggle]");
  if (toggle instanceof HTMLElement) {
    const selector = toggle.dataset.figureDeleteTarget || "";
    const form = selector ? document.querySelector(selector) : null;
    if (form instanceof HTMLFormElement) {
      setFigureDeleteMode(form, true);
      form.scrollIntoView({block: "nearest", behavior: "smooth"});
    }
    return;
  }
  const cancel = target?.closest("[data-figure-delete-cancel]");
  if (cancel instanceof HTMLElement) {
    const form = cancel.closest("form");
    if (form instanceof HTMLFormElement) setFigureDeleteMode(form, false);
  }
});

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
