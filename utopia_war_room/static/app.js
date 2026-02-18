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

function colorForCategory(category) {
  return CATEGORY_COLORS[category] || "rgba(168, 183, 206, 0.74)";
}

function asInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
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

function activeWarParam() {
  const params = new URLSearchParams(window.location.search);
  const war = (params.get("war") || "").trim();
  return war && war.toLowerCase() !== "all" ? war : "";
}

function apiUrl(path) {
  const war = activeWarParam();
  if (!war) return path;
  const url = new URL(path, window.location.origin);
  url.searchParams.set("war", war);
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

function setupWarFilter() {
  const select = document.getElementById("warSelect");
  if (!select) return;

  select.addEventListener("change", () => {
    const params = new URLSearchParams(window.location.search);
    const value = (select.value || "all").trim();
    if (!value || value.toLowerCase() === "all") {
      params.delete("war");
    } else {
      params.set("war", value);
    }
    const query = params.toString();
    window.location.href = query ? `/?${query}` : "/";
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
      <div><strong>Outcome:</strong> ${d.outcome || "-"}</div>
      <div><strong>Acres:</strong> ${d.acres || "-"}</div>
      <div><strong>Actor -> Target:</strong> ${(d.actor || "-")} -> ${(d.target || "-")}</div>
    `;
    summary.textContent = d.summary || "No summary";
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
  if (!title || !hint || !meta || !table) return;

  title.textContent = `Province History: ${payload.province}${payload.kingdom ? ` (${payload.kingdom})` : ""}`;
  hint.textContent = `${payload.events.length} matching events in current scope.`;

  const stats = payload.stats || {};
  meta.innerHTML = `
    <div><span>Attacks Sent</span><strong>${stats.attacks_sent || 0}</strong></div>
    <div><span>Attacks Received</span><strong>${stats.attacks_received || 0}</strong></div>
    <div><span>Aid Sent</span><strong>${stats.aid_sent || 0}</strong></div>
    <div><span>Aid Received</span><strong>${stats.aid_received || 0}</strong></div>
    <div><span>Gains</span><strong>${stats.gains || 0}</strong></div>
    <div><span>Losses</span><strong>${stats.losses || 0}</strong></div>
    <div><span>Net</span><strong>${stats.net || 0}</strong></div>
  `;

  const rows = payload.events || [];
  const header = table.querySelector("tr");
  table.innerHTML = "";
  if (header) table.appendChild(header);
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.fetched_at_utc || "-"}</td>
      <td>${row.event_time_text || "-"}</td>
      <td>${row.category || "-"}</td>
      <td>${row.role || "-"}</td>
      <td>${row.outcome || "-"}</td>
      <td>${row.acres || "-"}</td>
      <td>${row.actor || "-"}</td>
      <td>${row.target || "-"}</td>
      <td class="summary">${row.summary || "-"}</td>
    `;
    table.appendChild(tr);
  });
}

async function loadProvinceHistory(name, kingdom = "") {
  if (!name) return;
  const resp = await fetch(apiUrlWithParams("/api/province_history", { name, kingdom }));
  if (!resp.ok) return;
  const payload = await resp.json();
  renderProvinceHistory(payload);
}

function setupProvinceLinks() {
  const links = [...document.querySelectorAll(".province-link")];
  links.forEach((link) => {
    link.addEventListener("click", async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const province = link.dataset.province || "";
      const kingdom = link.dataset.kingdom || "";
      await loadProvinceHistory(province, kingdom);
    });
  });
}

function renderFactDetail(payload) {
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
}

async function loadFactDetail(fact, key = "") {
  if (!fact) return;
  const resp = await fetch(apiUrlWithParams("/api/fact_detail", { fact, key }));
  if (!resp.ok) return;
  const payload = await resp.json();
  renderFactDetail(payload);
}

function setupFactInspectors() {
  const kpis = [...document.querySelectorAll(".kpi[data-fact]")];
  kpis.forEach((kpi) => {
    kpi.addEventListener("click", () => {
      loadFactDetail(kpi.dataset.fact || "");
    });
  });

  const factRows = [...document.querySelectorAll(".fact-row[data-fact]")];
  factRows.forEach((row) => {
    row.addEventListener("click", () => {
      loadFactDetail(row.dataset.fact || "", row.dataset.key || "");
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
          window.location.reload();
          return;
        }

        if (latestSuccess) {
          knownLastSuccess = latestSuccess;
        }
      }
    } catch (_err) {
      // Keep polling even if one request fails.
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
  if (!Array.isArray(rows) || rows.length === 0) return;

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

  const ctx = canvas.getContext("2d");
  new Chart(ctx, {
    type: "bar",
    data: {
      labels: days,
      datasets,
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      interaction: {
        mode: "index",
        intersect: false,
      },
      scales: {
        x: {
          stacked: true,
          ticks: { color: "rgba(217, 231, 252, 0.86)", maxRotation: 45, minRotation: 45 },
          grid: { color: "rgba(159, 186, 223, 0.15)" },
        },
        y: {
          stacked: true,
          beginAtZero: true,
          ticks: { color: "rgba(217, 231, 252, 0.86)" },
          grid: { color: "rgba(159, 186, 223, 0.12)" },
        },
      },
      plugins: {
        legend: {
          labels: { color: "rgba(231, 241, 255, 0.9)" },
        },
      },
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
    if (meta) {
      meta.textContent = "No home-kingdom land swing rows yet. Keep ingest running for another cycle.";
    }
    return;
  }

  const labels = rows.map((row) => row.day);
  const gained = rows.map((row) => asInt(row.gained));
  const lostNegative = rows.map((row) => asInt(row.lost) * -1);
  const net = rows.map((row) => asInt(row.net));

  if (meta && payload.home_kingdom) {
    meta.textContent = `Gained vs lost acres for inferred home kingdom ${payload.home_kingdom}.`;
  }

  const ctx = canvas.getContext("2d");
  new Chart(ctx, {
    data: {
      labels,
      datasets: [
        {
          type: "bar",
          label: "Gained Acres",
          data: gained,
          backgroundColor: "rgba(51, 199, 127, 0.66)",
          borderColor: "rgba(51, 199, 127, 0.96)",
          borderWidth: 1,
        },
        {
          type: "bar",
          label: "Lost Acres",
          data: lostNegative,
          backgroundColor: "rgba(255, 116, 116, 0.63)",
          borderColor: "rgba(255, 116, 116, 0.93)",
          borderWidth: 1,
        },
        {
          type: "line",
          label: "Net Acres",
          data: net,
          borderColor: "rgba(83, 177, 255, 0.95)",
          backgroundColor: "rgba(83, 177, 255, 0.24)",
          fill: false,
          tension: 0.2,
          pointRadius: 2,
          yAxisID: "y",
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      interaction: {
        mode: "index",
        intersect: false,
      },
      scales: {
        x: {
          ticks: { color: "rgba(217, 231, 252, 0.86)", maxRotation: 45, minRotation: 45 },
          grid: { color: "rgba(159, 186, 223, 0.15)" },
        },
        y: {
          ticks: {
            color: "rgba(217, 231, 252, 0.86)",
            callback: (value) => Math.abs(value),
          },
          grid: { color: "rgba(159, 186, 223, 0.12)" },
        },
      },
      plugins: {
        legend: {
          labels: { color: "rgba(231, 241, 255, 0.9)" },
        },
      },
    },
  });
}

async function bootstrap() {
  setupWarFilter();
  setupSortableTables();
  setupProvinceLinks();
  setupFactInspectors();
  setupEventDetails();
  loadFactDetail("total_events");
  await Promise.all([loadMomentumChart(), loadLandSwingChart()]);
  watchForLiveUpdates();
}

bootstrap();
