const banner = document.getElementById("freshness-banner");

function setBanner(data) {
  if (!banner || !data) return;
  const state = data.pull_result === "error" ? "error" : (data.repo_dirty ? "warning" : "ok");
  banner.dataset.state = state;
  banner.querySelector("span").textContent = data.message || "Workspace status unavailable.";
}

async function fetchFreshness(url, options = {}) {
  const response = await fetch(url, {headers: {"Accept": "application/json"}, ...options});
  setBanner(await response.json());
}

if (banner) {
  const button = banner.querySelector("button");
  if (button) {
    button.addEventListener("click", () => fetchFreshness(button.dataset.updateUrl, {method: "POST"}));
  }
  if (!sessionStorage.getItem("arquimedes-freshness-checked")) {
    sessionStorage.setItem("arquimedes-freshness-checked", "1");
    fetchFreshness("/api/freshness").catch(() => setBanner({message: "Workspace status unavailable.", pull_result: "error"}));
  }
}
