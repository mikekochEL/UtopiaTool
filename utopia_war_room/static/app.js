const CATEGORY_COLORS = {
  attack: "rgba(83, 177, 255, 0.75)",
  aid: "rgba(63, 212, 152, 0.75)",
  diplomacy: "rgba(255, 200, 87, 0.78)",
  dragon: "rgba(255, 116, 116, 0.78)",
  thievery: "rgba(197, 142, 255, 0.76)",
  magic: "rgba(100, 220, 233, 0.78)",
  other: "rgba(168, 183, 206, 0.74)",
};
const STATUS_POLL_MS = 15000;
const MONTH_INDEX = {
  january: 1,
  february: 2,
  march: 3,
  april: 4,
  may: 5,
  june: 6,
  july: 7,
  august: 8,
  september: 9,
  october: 10,
  november: 11,
  december: 12,
};

let kingdomTrendChart = null;
let kingdomCompareChart = null;
let provinceTimelineChart = null;

function asInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function colorForCategory(category) {
  return CATEGORY_COLORS[category] || "rgba(168, 183, 206, 0.74)";
}

function categorySort(categories) {
  const order = ["attack", "aid", "diplomacy", "dragon", "thievery", "magic", "other"];
  const rank = new Map(order.map((name, i) => [name, i]));
  return [...categories].sort((a, b) => {
    const ra = rank.has(a) ? rank.get(a) : 999;
    const rb = rank.has(b) ? rank.get(b) : 999;
    if (ra !== rb) return ra - rb;
    return a.localeCompare(b);
  });
}

function activeFilterParams() {
  const source = new URLSearchParams(window.location.search);
  const keys = ["war", "start", "end", "kingdom", "compare"];
  const out = new URLSearchParams();
  keys.forEach((key) => {
    const value = (source.get(key) || "").trim();
    if (value) out.set(key, value);
  });
  return out;
}

function apiUrl(path) {
  const url = new URL(path, window.location.origin);
  const params = activeFilterParams();
  params.forEach((value, key) => url.searchParams.set(key, value));
  return url.pathname + url.search;
}

function apiUrlWithParams(path, params = {}) {
  const url = new URL(apiUrl(path), window.location.origin);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== null && value !== undefined && String(value).trim() !== "") {
      url.searchParams.set(key, String(value));
    }
  });
  return url.pathname + url.search;
}

function setupInitialScrollPosition() {
  if ("scrollRestoration" in window.history) {
    window.history.scrollRestoration = "manual";
  }
  window.scrollTo({ top: 0, left: 0, behavior: "auto" });
}

function focusCard(cardId, doScroll = true) {
  const card = document.getElementById(cardId);
  if (!card) return;
  card.classList.remove("spotlight");
  void card.offsetWidth;
  card.classList.add("spotlight");
  if (doScroll) {
    card.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function parseUtopiaDay(value) {
  const text = (value || "").trim();
  const match = text.match(
    /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d+)\s+of\s+YR(\d+)$/i
  );
  if (!match) return null;
  const month = MONTH_INDEX[match[1].toLowerCase()] || 0;
  const day = Number(match[2]) || 0;
  const year = Number(match[3]) || 0;
  return year * 10000 + month * 100 + day;
}

function parseSortValue(text) {
  const raw = (text || "").trim();
  if (!raw || raw === "-") return { kind: "empty", value: "" };

  const utopiaDay = parseUtopiaDay(raw);
  if (utopiaDay !== null) return { kind: "number", value: utopiaDay };

  const normalizedNumber = raw.replace(/,/g, "").replace(/%$/, "");
  if (/^-?\d+(\.\d+)?$/.test(normalizedNumber)) {
    return { kind: "number", value: Number(normalizedNumber) };
  }

  return { kind: "text", value: raw.toLowerCase() };
}

function compareSortValues(a, b) {
  if (a.kind === "empty" && b.kind !== "empty") return 1;
  if (b.kind === "empty" && a.kind !== "empty") return -1;
  if (a.kind === "number" && b.kind === "number") return a.value - b.value;
  return String(a.value).localeCompare(String(b.value));
}

function setupSortableTables() {
  const tables = [...document.querySelectorAll("table.sortable")];
  tables.forEach((table) => {
    const headers = [...table.querySelectorAll("tr:first-child th")];
    if (!headers.length) return;

    headers.forEach((th, index) => {
      if (th.dataset.sortBound === "1") return;
      th.dataset.sortBound = "1";
      th.addEventListener("click", () => {
        const current = th.dataset.sortDir || "";
        const dir = current === "asc" ? "desc" : "asc";
        headers.forEach((header) => {
          header.classList.remove("sorted-asc", "sorted-desc");
          header.dataset.sortDir = "";
        });
        th.classList.add(dir === "asc" ? "sorted-asc" : "sorted-desc");
        th.dataset.sortDir = dir;

        const rows = [...table.querySelectorAll("tr")].slice(1);
        rows.sort((rowA, rowB) => {
          const aText = rowA.children[index] ? rowA.children[index].textContent : "";
          const bText = rowB.children[index] ? rowB.children[index].textContent : "";
          const cmp = compareSortValues(parseSortValue(aText), parseSortValue(bText));
          return dir === "asc" ? cmp : -cmp;
        });
        rows.forEach((row) => table.appendChild(row));
      });
    });
  });
}

function applyViewVisibility(view) {
  const current = (view || "overview").trim().toLowerCase();
  const blocks = [...document.querySelectorAll("[data-view-block]")];
  blocks.forEach((el) => {
    const allowed = (el.dataset.viewBlock || "").split(/\s+/).filter(Boolean);
    const visible = allowed.includes(current);
    el.classList.toggle("hidden", !visible);
  });

  const tabs = [...document.querySelectorAll(".tab[data-view]")];
  tabs.forEach((tab) => {
    tab.classList.toggle("active", (tab.dataset.view || "") === current);
  });

  const hidden = document.getElementById("viewModeInput");
  if (hidden) hidden.value = current;
  document.body.dataset.selectedView = current;
}

function applyGlobalSearch(query) {
  const text = (query || "").trim().toLowerCase();
  const tables = [...document.querySelectorAll("table")];
  tables.forEach((table) => {
    const rows = [...table.querySelectorAll("tr")].slice(1);
    rows.forEach((row) => {
      if (!text) {
        row.classList.remove("hidden");
        return;
      }
      const hit = (row.textContent || "").toLowerCase().includes(text);
      row.classList.toggle("hidden", !hit);
    });
  });
}

function setupFilterControls() {
  const resetBtn = document.getElementById("resetFilters");
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      window.location.href = "/";
    });
  }

  const form = document.getElementById("filterForm");
  const autoSubmitIds = ["warSelect", "startDay", "endDay", "kingdomSelect", "compareSelect"];
  autoSubmitIds.forEach((id) => {
    const input = document.getElementById(id);
    if (!input || !form) return;
    input.addEventListener("change", () => form.submit());
  });

  const savedView = localStorage.getItem("utopia_view");
  const selectedView = (document.body.dataset.selectedView || savedView || "overview").toLowerCase();
  applyViewVisibility(selectedView);

  const tabs = [...document.querySelectorAll(".tab[data-view]")];
  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      const view = tab.dataset.view || "overview";
      localStorage.setItem("utopia_view", view);
      applyViewVisibility(view);
    });
  });

  const search = document.getElementById("globalSearch");
  if (search) {
    search.addEventListener("input", () => applyGlobalSearch(search.value || ""));
  }

  document.addEventListener("keydown", (event) => {
    if (event.key === "/" && !event.ctrlKey && !event.metaKey && !event.altKey) {
      const target = event.target;
      const isTyping = target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable);
      if (!isTyping) {
        event.preventDefault();
        if (search) search.focus();
      }
    }
  });
}

function pushToast(message) {
  const stack = document.getElementById("toastStack");
  if (!stack || !message) return;
  const item = document.createElement("div");
  item.className = "toast";
  item.textContent = message;
  stack.appendChild(item);
  setTimeout(() => {
    item.remove();
  }, 5000);
}

function showToastsFromSnapshot() {
  const snap = window.__snapshot || {};
  const alerts = Array.isArray(snap.alerts) ? snap.alerts : [];
  alerts.slice(0, 4).forEach((message, index) => {
    setTimeout(() => pushToast(message), 450 * index);
  });
}

function setupEventDetails() {
  const rows = [...document.querySelectorAll("tr.event-row")];
  if (!rows.length) return;

  const meta = document.getElementById("eventDetailMeta");
  const summary = document.getElementById("eventDetailSummary");
  const hint = document.getElementById("eventDetailHint");
  if (!meta || !summary || !hint) return;

  const renderRow = (row) => {
    rows.forEach((r) => r.classList.remove("active"));
    row.classList.add("active");
    const d = row.dataset;
    hint.textContent = `Event #${d.eventId || "-"}`;
    meta.innerHTML = `
      <div><strong>Fetched:</strong> ${d.fetched || "-"}</div>
      <div><strong>Day:</strong> ${d.day || "-"}</div>
      <div><strong>Category:</strong> ${d.category || "-"}</div>
      <div><strong>Attack Type:</strong> ${d.attackType || "-"}</div>
      <div><strong>Outcome:</strong> ${d.outcome || "-"}</div>
      <div><strong>Acres:</strong> ${d.acres || "-"}</div>
      <div><strong>Actor -> Target:</strong> ${(d.actor || "-")} -> ${(d.target || "-")}</div>
    `;
    summary.textContent = d.summary || "No summary";
    focusCard("eventDetail", false);
  };

  rows.forEach((row) => {
    row.addEventListener("click", () => renderRow(row));
  });
  renderRow(rows[0]);
}

function renderProvinceHistory(payload) {
  const title = document.getElementById("provinceHistoryTitle");
  const hint = document.getElementById("provinceHistoryHint");
  const meta = document.getElementById("provinceHistoryMeta");
  const table = document.getElementById("provinceHistoryTable");
  const card = document.getElementById("provinceHistory");
  if (!title || !hint || !meta || !table) return;

  if (payload.error) {
    title.textContent = "Province History";
    hint.textContent = `Unable to load province history (${payload.error}).`;
    meta.innerHTML = "";
    if (card) card.classList.remove("has-data");
    return;
  }

  const events = Array.isArray(payload.events) ? payload.events : [];
  title.textContent = `Province History: ${payload.province}${payload.kingdom ? ` (${payload.kingdom})` : ""}`;
  hint.textContent = `${events.length} matching events in current scope.`;

  const stats = payload.stats || {};
  const extraOps = `
    <div><span>Ops Sent</span><strong>${stats.ops_sent || 0}</strong></div>
    <div><span>Ops Received</span><strong>${stats.ops_received || 0}</strong></div>
    <div><span>Magic Sent</span><strong>${stats.magic_sent || 0}</strong></div>
    <div><span>Magic Received</span><strong>${stats.magic_received || 0}</strong></div>
  `;
  meta.innerHTML = `
    <div><span>Attacks Sent</span><strong>${stats.attacks_sent || 0}</strong></div>
    <div><span>Attacks Received</span><strong>${stats.attacks_received || 0}</strong></div>
    <div><span>Aid Sent</span><strong>${stats.aid_sent || 0}</strong></div>
    <div><span>Aid Received</span><strong>${stats.aid_received || 0}</strong></div>
    ${extraOps}
    <div><span>Gains</span><strong>${stats.gains || 0}</strong></div>
    <div><span>Losses</span><strong>${stats.losses || 0}</strong></div>
    <div><span>Net</span><strong>${stats.net || 0}</strong></div>
  `;

  const header = table.querySelector("tr");
  table.innerHTML = "";
  if (header) table.appendChild(header);

  events.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.fetched_at_utc || "-"}</td>
      <td>${row.event_time_text || "-"}</td>
      <td>${row.category || "-"}</td>
      <td>${row.attack_type || "-"}</td>
      <td>${row.role || "-"}</td>
      <td>${row.outcome || "-"}</td>
      <td>${row.acres || "-"}</td>
      <td>${row.actor || "-"}</td>
      <td>${row.target || "-"}</td>
      <td class="summary">${row.summary || "-"}</td>
    `;
    table.appendChild(tr);
  });

  if (card) card.classList.toggle("has-data", events.length > 0);
  setupSortableTables();
  focusCard("provinceHistory", true);
}

function renderProvinceSnapshotTimeline(rows, provinceName) {
  const hint = document.getElementById("provinceTimelineHint");
  const canvas = document.getElementById("provinceTimelineChart");
  if (!canvas || !hint) return;

  if (provinceTimelineChart) {
    provinceTimelineChart.destroy();
    provinceTimelineChart = null;
  }

  if (!rows.length) {
    hint.textContent = `No kingdom snapshot timeline found for ${provinceName}.`;
    return;
  }

  hint.textContent = `Snapshot timeline for ${provinceName}.`;
  const labels = rows.map((row) => row.day || (row.fetched_at_utc || "").slice(0, 10));
  const land = rows.map((row) => asInt(row.land));
  const nwpa = rows.map((row) => Number(row.nwpa || 0));

  provinceTimelineChart = new Chart(canvas.getContext("2d"), {
    data: {
      labels,
      datasets: [
        {
          type: "line",
          label: "Land",
          data: land,
          borderColor: "rgba(83, 177, 255, 0.92)",
          backgroundColor: "rgba(83, 177, 255, 0.22)",
          yAxisID: "y",
          tension: 0.25,
          pointRadius: 2,
        },
        {
          type: "line",
          label: "NW/A",
          data: nwpa,
          borderColor: "rgba(255, 200, 87, 0.95)",
          backgroundColor: "rgba(255, 200, 87, 0.25)",
          yAxisID: "y1",
          tension: 0.25,
          pointRadius: 2,
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      scales: {
        x: { ticks: { color: "rgba(217, 231, 252, 0.86)" }, grid: { color: "rgba(159,186,223,0.12)" } },
        y: { ticks: { color: "rgba(217, 231, 252, 0.86)" }, grid: { color: "rgba(159,186,223,0.12)" } },
        y1: { position: "right", ticks: { color: "rgba(255, 220, 156, 0.85)" }, grid: { drawOnChartArea: false } },
      },
      plugins: { legend: { labels: { color: "rgba(231, 241, 255, 0.9)" } } },
    },
  });
}

async function loadProvinceSnapshotTimeline(name, kingdom = "") {
  if (!name || !kingdom) return;
  try {
    const resp = await fetch(apiUrlWithParams("/api/province_snapshot_timeline", { name, kingdom }));
    if (!resp.ok) return;
    const payload = await resp.json();
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    renderProvinceSnapshotTimeline(rows, name);
  } catch (_err) {
    // Ignore single-request errors.
  }
}

async function loadProvinceHistory(name, kingdom = "") {
  if (!name) return;
  const hint = document.getElementById("provinceHistoryHint");
  const card = document.getElementById("provinceHistory");
  if (hint) hint.textContent = `Loading ${name}${kingdom ? ` (${kingdom})` : ""}...`;
  if (card) card.classList.add("loading");
  try {
    const resp = await fetch(apiUrlWithParams("/api/province_history", { name, kingdom }));
    if (!resp.ok) {
      renderProvinceHistory({ error: `http_${resp.status}` });
      return;
    }
    const payload = await resp.json();
    renderProvinceHistory(payload);
    await loadProvinceSnapshotTimeline(payload.province || name, payload.kingdom || kingdom);
  } catch (_err) {
    renderProvinceHistory({ error: "request_failed" });
  } finally {
    if (card) card.classList.remove("loading");
  }
}

function setupProvinceLinks() {
  const links = [...document.querySelectorAll(".province-link")];
  links.forEach((link) => {
    if (link.dataset.bound === "1") return;
    link.dataset.bound = "1";
    link.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const province = link.dataset.province || "";
      const kingdom = link.dataset.kingdom || "";
      if (!province) return;
      const url = new URL("/province", window.location.origin);
      const source = new URLSearchParams(window.location.search);
      ["war", "start", "end", "view"].forEach((key) => {
        const value = (source.get(key) || "").trim();
        if (value) url.searchParams.set(key, value);
      });
      url.searchParams.set("name", province);
      if (kingdom) url.searchParams.set("kingdom", kingdom);
      window.location.href = url.pathname + url.search;
    });
  });
}

function renderFactDetail(payload, focus = true) {
  const title = document.getElementById("factDetailTitle");
  const hint = document.getElementById("factDetailHint");
  const summary = document.getElementById("factDetailSummary");
  const rows = document.getElementById("factDetailRows");
  const events = document.getElementById("factDetailEvents");
  if (!title || !hint || !summary || !rows || !events) return;

  title.textContent = payload.title || "Fact Detail";
  hint.textContent = "Source detail for selected metric.";
  summary.textContent = payload.summary || "No detail";
  rows.innerHTML = (payload.rows || [])
    .map((row) => `<div><span>${row.label || "-"}</span><strong>${row.value || "-"}</strong></div>`)
    .join("");
  events.innerHTML = (payload.events || [])
    .map(
      (event) => `<div class="event">
        <div><strong>${event.event_time_text || "-"}</strong> | ${event.category || "-"}</div>
        <div>${event.summary || "-"}</div>
      </div>`
    )
    .join("");
  if (focus) focusCard("factDetail", true);
}

async function loadFactDetail(fact, key = "", focus = true) {
  if (!fact) return;
  try {
    const resp = await fetch(apiUrlWithParams("/api/fact_detail", { fact, key }));
    if (!resp.ok) return;
    const payload = await resp.json();
    renderFactDetail(payload, focus);
  } catch (_err) {
    // Ignore.
  }
}

function setupFactInspectors() {
  const kpis = [...document.querySelectorAll(".kpi[data-fact]")];
  kpis.forEach((kpi) => {
    if (kpi.dataset.bound === "1") return;
    kpi.dataset.bound = "1";
    kpi.addEventListener("click", () => {
      loadFactDetail(kpi.dataset.fact || "", "", true);
    });
  });

  const factRows = [...document.querySelectorAll(".fact-row[data-fact]")];
  factRows.forEach((row) => {
    if (row.dataset.bound === "1") return;
    row.dataset.bound = "1";
    row.addEventListener("click", () => {
      loadFactDetail(row.dataset.fact || "", row.dataset.key || "", true);
    });
  });
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value;
}

function renderIngestStatus(status) {
  setText("statusEnabled", status.enabled ? "enabled" : "disabled");
  setText("statusRunning", status.running ? "running" : "idle");
  setText("statusLastSuccess", status.last_success_utc || "n/a");
  setText(
    "statusLastParsed",
    status.last_parsed_events === null || status.last_parsed_events === undefined
      ? "n/a"
      : String(status.last_parsed_events)
  );
  setText("statusLastError", status.last_error || "none");
}

async function fetchIngestStatus() {
  const resp = await fetch("/api/status", { cache: "no-store" });
  if (!resp.ok) return null;
  return resp.json();
}

async function watchForLiveUpdates() {
  let knownLastSuccess = (document.body.dataset.lastSuccess || "").trim();
  while (true) {
    try {
      const status = await fetchIngestStatus();
      if (status) {
        renderIngestStatus(status);
        const latestSuccess = (status.last_success_utc || "").trim();
        if (latestSuccess && latestSuccess !== knownLastSuccess) {
          pushToast("New ingest cycle completed. Refreshing view.");
          setTimeout(() => window.location.reload(), 700);
          return;
        }
        if (latestSuccess) knownLastSuccess = latestSuccess;
      }
    } catch (_err) {
      // Keep polling.
    }
    await new Promise((resolve) => setTimeout(resolve, STATUS_POLL_MS));
  }
}

async function loadMomentumChart() {
  const canvas = document.getElementById("momentumChart");
  if (!canvas) return;
  const resp = await fetch(apiUrl("/api/momentum"));
  if (!resp.ok) return;
  const rows = await resp.json();
  if (!Array.isArray(rows) || !rows.length) return;

  const days = [...new Set(rows.map((row) => row.day))];
  const categories = categorySort(new Set(rows.map((row) => row.category)));
  const valueMap = new Map(rows.map((row) => [`${row.day}__${row.category}`, asInt(row.cnt)]));

  const datasets = categories.map((category) => ({
    label: category,
    data: days.map((day) => valueMap.get(`${day}__${category}`) || 0),
    backgroundColor: colorForCategory(category),
    borderColor: colorForCategory(category).replace("0.7", "1"),
    borderWidth: 1,
  }));

  new Chart(canvas.getContext("2d"), {
    type: "bar",
    data: { labels: days, datasets },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: { stacked: true, ticks: { color: "rgba(217, 231, 252, 0.86)" }, grid: { color: "rgba(159,186,223,0.15)" } },
        y: { stacked: true, beginAtZero: true, ticks: { color: "rgba(217, 231, 252, 0.86)" }, grid: { color: "rgba(159,186,223,0.12)" } },
      },
      plugins: { legend: { labels: { color: "rgba(231, 241, 255, 0.9)" } } },
    },
  });
}

async function loadLandSwingChart() {
  const canvas = document.getElementById("landSwingChart");
  if (!canvas) return;
  const resp = await fetch(apiUrl("/api/land_swing"));
  if (!resp.ok) return;

  const payload = await resp.json();
  const rows = payload.rows || [];
  const meta = document.getElementById("landSwingMeta");
  if (!rows.length) {
    if (meta) meta.textContent = "No home-kingdom land swing rows yet.";
    return;
  }

  const labels = rows.map((row) => row.day);
  const gained = rows.map((row) => asInt(row.gained));
  const lostNegative = rows.map((row) => asInt(row.lost) * -1);
  const net = rows.map((row) => asInt(row.net));
  if (meta && payload.home_kingdom) {
    meta.textContent = `Gained vs lost acres for inferred home kingdom ${payload.home_kingdom}.`;
  }

  new Chart(canvas.getContext("2d"), {
    data: {
      labels,
      datasets: [
        { type: "bar", label: "Gained Acres", data: gained, backgroundColor: "rgba(51,199,127,0.66)", borderColor: "rgba(51,199,127,0.96)", borderWidth: 1 },
        { type: "bar", label: "Lost Acres", data: lostNegative, backgroundColor: "rgba(255,116,116,0.63)", borderColor: "rgba(255,116,116,0.93)", borderWidth: 1 },
        { type: "line", label: "Net Acres", data: net, borderColor: "rgba(83,177,255,0.95)", backgroundColor: "rgba(83,177,255,0.24)", fill: false, tension: 0.2, pointRadius: 2, yAxisID: "y" },
      ],
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: { ticks: { color: "rgba(217, 231, 252, 0.86)" }, grid: { color: "rgba(159,186,223,0.15)" } },
        y: { ticks: { color: "rgba(217, 231, 252, 0.86)", callback: (value) => Math.abs(value) }, grid: { color: "rgba(159,186,223,0.12)" } },
      },
      plugins: { legend: { labels: { color: "rgba(231, 241, 255, 0.9)" } } },
    },
  });
}

async function loadNwSwingChart() {
  const canvas = document.getElementById("nwSwingChart");
  if (!canvas) return;
  const resp = await fetch(apiUrl("/api/nw_swing"));
  if (!resp.ok) return;

  const payload = await resp.json();
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  const meta = document.getElementById("nwSwingMeta");
  if (!rows.length) {
    if (meta) meta.textContent = "No networth snapshot deltas available yet.";
    return;
  }

  const labels = rows.map((row) => row.day);
  const totalNw = rows.map((row) => asInt(row.total_networth));
  const deltaNw = rows.map((row) => asInt(row.delta_networth));
  if (meta && payload.home_kingdom) {
    meta.textContent = `Daily networth swing for home kingdom ${payload.home_kingdom}.`;
  }

  new Chart(canvas.getContext("2d"), {
    data: {
      labels,
      datasets: [
        {
          type: "bar",
          label: "Delta Networth",
          data: deltaNw,
          backgroundColor: deltaNw.map((value) =>
            value >= 0 ? "rgba(51,199,127,0.66)" : "rgba(255,116,116,0.63)"
          ),
          borderColor: deltaNw.map((value) =>
            value >= 0 ? "rgba(51,199,127,0.96)" : "rgba(255,116,116,0.93)"
          ),
          borderWidth: 1,
          yAxisID: "y",
        },
        {
          type: "line",
          label: "Total Networth",
          data: totalNw,
          borderColor: "rgba(83,177,255,0.95)",
          backgroundColor: "rgba(83,177,255,0.24)",
          fill: false,
          tension: 0.2,
          pointRadius: 2,
          yAxisID: "y1",
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: { ticks: { color: "rgba(217, 231, 252, 0.86)" }, grid: { color: "rgba(159,186,223,0.15)" } },
        y: { ticks: { color: "rgba(217, 231, 252, 0.86)" }, grid: { color: "rgba(159,186,223,0.12)" } },
        y1: { position: "right", ticks: { color: "rgba(139,205,255,0.95)" }, grid: { drawOnChartArea: false } },
      },
      plugins: { legend: { labels: { color: "rgba(231, 241, 255, 0.9)" } } },
    },
  });
}

function loadKingdomTrendChart() {
  const canvas = document.getElementById("kingdomTrendChart");
  const snap = window.__snapshot || {};
  const rows = Array.isArray(snap.focus_trend_rows) ? snap.focus_trend_rows : [];
  if (!canvas || !rows.length) return;

  if (kingdomTrendChart) {
    kingdomTrendChart.destroy();
    kingdomTrendChart = null;
  }

  const labels = rows.map((row) => (row.fetched_at_utc || "").slice(0, 10));
  const land = rows.map((row) => asInt(row.total_land));
  const nw = rows.map((row) => asInt(row.total_networth));
  const honor = rows.map((row) => asInt(row.total_honor));

  kingdomTrendChart = new Chart(canvas.getContext("2d"), {
    data: {
      labels,
      datasets: [
        { type: "line", label: "Land", data: land, borderColor: "rgba(83,177,255,0.96)", backgroundColor: "rgba(83,177,255,0.24)", yAxisID: "y", tension: 0.25, pointRadius: 2 },
        { type: "line", label: "Networth", data: nw, borderColor: "rgba(51,199,127,0.96)", backgroundColor: "rgba(51,199,127,0.22)", yAxisID: "y1", tension: 0.25, pointRadius: 2 },
        { type: "line", label: "Honor", data: honor, borderColor: "rgba(255,200,87,0.96)", backgroundColor: "rgba(255,200,87,0.2)", yAxisID: "y2", tension: 0.25, pointRadius: 2 },
      ],
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: { ticks: { color: "rgba(217,231,252,0.86)" }, grid: { color: "rgba(159,186,223,0.12)" } },
        y: { ticks: { color: "rgba(139,205,255,0.95)" }, grid: { color: "rgba(159,186,223,0.12)" } },
        y1: { position: "right", ticks: { color: "rgba(156,238,191,0.95)" }, grid: { drawOnChartArea: false } },
        y2: { position: "right", ticks: { color: "rgba(255,230,165,0.95)" }, grid: { drawOnChartArea: false } },
      },
      plugins: { legend: { labels: { color: "rgba(231,241,255,0.9)" } } },
    },
  });
}

function loadKingdomCompareChart() {
  const canvas = document.getElementById("kingdomCompareChart");
  const snap = window.__snapshot || {};
  const compare = snap.compare || null;
  const rows = compare && Array.isArray(compare.trend_rows) ? compare.trend_rows : [];
  if (!canvas || !rows.length) return;

  if (kingdomCompareChart) {
    kingdomCompareChart.destroy();
    kingdomCompareChart = null;
  }

  const labels = rows.map((row) => row.day);
  const leftLand = rows.map((row) => asInt(row.left_land));
  const rightLand = rows.map((row) => asInt(row.right_land));
  const leftNW = rows.map((row) => asInt(row.left_nw));
  const rightNW = rows.map((row) => asInt(row.right_nw));

  kingdomCompareChart = new Chart(canvas.getContext("2d"), {
    data: {
      labels,
      datasets: [
        { type: "line", label: `${compare.left.coord} Land`, data: leftLand, borderColor: "rgba(83,177,255,0.95)", backgroundColor: "rgba(83,177,255,0.22)", yAxisID: "y", tension: 0.25, pointRadius: 2 },
        { type: "line", label: `${compare.right.coord} Land`, data: rightLand, borderColor: "rgba(255,116,116,0.95)", backgroundColor: "rgba(255,116,116,0.2)", yAxisID: "y", tension: 0.25, pointRadius: 2 },
        { type: "line", label: `${compare.left.coord} NW`, data: leftNW, borderColor: "rgba(51,199,127,0.95)", borderDash: [5, 3], yAxisID: "y1", tension: 0.25, pointRadius: 1 },
        { type: "line", label: `${compare.right.coord} NW`, data: rightNW, borderColor: "rgba(255,200,87,0.95)", borderDash: [5, 3], yAxisID: "y1", tension: 0.25, pointRadius: 1 },
      ],
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: { ticks: { color: "rgba(217,231,252,0.86)" }, grid: { color: "rgba(159,186,223,0.12)" } },
        y: { ticks: { color: "rgba(139,205,255,0.95)" }, grid: { color: "rgba(159,186,223,0.12)" } },
        y1: { position: "right", ticks: { color: "rgba(177,237,195,0.95)" }, grid: { drawOnChartArea: false } },
      },
      plugins: { legend: { labels: { color: "rgba(231,241,255,0.9)" } } },
    },
  });
}

function signed(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "0";
  const rounded = Math.round(n * 100) / 100;
  return rounded > 0 ? `+${rounded}` : `${rounded}`;
}

function setupReplaySlider() {
  const payload = window.__warCommand || {};
  const rows = Array.isArray(payload.replay_rows) ? payload.replay_rows : [];
  const slider = document.getElementById("replaySlider");
  const dayLabel = document.getElementById("replayDayLabel");
  const landNet = document.getElementById("replayLandNet");
  const landCum = document.getElementById("replayLandCum");
  const opNet = document.getElementById("replayOpNet");
  const opCum = document.getElementById("replayOpCum");
  if (!slider || !dayLabel || !landNet || !landCum || !opNet || !opCum) return;

  if (!rows.length) {
    slider.disabled = true;
    dayLabel.textContent = "No replay rows in current scope.";
    landNet.textContent = "0";
    landCum.textContent = "0";
    opNet.textContent = "0";
    opCum.textContent = "0";
    return;
  }

  slider.min = "0";
  slider.max = String(rows.length - 1);
  slider.value = String(rows.length - 1);

  const render = (index) => {
    const safe = Math.max(0, Math.min(rows.length - 1, index));
    const row = rows[safe];
    dayLabel.textContent = `${row.day || "-"} | hits ${asInt(row.home_hits)} vs ${asInt(row.enemy_hits)}`;
    landNet.textContent = signed(row.home_land_net);
    landCum.textContent = signed(row.cumulative_land_net);
    opNet.textContent = signed(row.op_net_damage);
    opCum.textContent = signed(row.cumulative_op_net_damage);
  };

  slider.addEventListener("input", () => {
    render(asInt(slider.value));
  });
  render(asInt(slider.value));
}

async function bootstrap() {
  setupInitialScrollPosition();
  setupFilterControls();
  setupSortableTables();
  setupProvinceLinks();
  setupFactInspectors();
  setupEventDetails();
  loadFactDetail("total_events", "", false);
  loadKingdomTrendChart();
  loadKingdomCompareChart();
  setupReplaySlider();
  showToastsFromSnapshot();
  await Promise.all([loadMomentumChart(), loadLandSwingChart(), loadNwSwingChart()]);
  watchForLiveUpdates();
}

bootstrap();
