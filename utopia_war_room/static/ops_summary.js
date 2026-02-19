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
    headers.forEach((th, index) => {
      if (th.dataset.sortBound === "1") return;
      th.dataset.sortBound = "1";
      th.addEventListener("click", () => {
        const current = th.dataset.sortDir || "";
        const dir = current === "asc" ? "desc" : "asc";
        headers.forEach((header) => {
          header.dataset.sortDir = "";
          header.classList.remove("sorted-asc", "sorted-desc");
        });
        th.dataset.sortDir = dir;
        th.classList.add(dir === "asc" ? "sorted-asc" : "sorted-desc");
        const rows = [...table.querySelectorAll("tr")].slice(1);
        rows.sort((a, b) => {
          const aText = a.children[index] ? a.children[index].textContent : "";
          const bText = b.children[index] ? b.children[index].textContent : "";
          const cmp = compareSortValues(parseSortValue(aText), parseSortValue(bText));
          return dir === "asc" ? cmp : -cmp;
        });
        rows.forEach((row) => table.appendChild(row));
      });
    });
  });
}

function scopeParamsFromCurrentUrl() {
  const source = new URLSearchParams(window.location.search);
  const out = new URLSearchParams();
  ["war", "start", "end", "view"].forEach((key) => {
    const value = (source.get(key) || "").trim();
    if (value) out.set(key, value);
  });
  return out;
}

function setupProvinceLinks() {
  const links = [...document.querySelectorAll(".province-link")];
  links.forEach((link) => {
    if (link.dataset.bound === "1") return;
    link.dataset.bound = "1";
    link.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const province = (link.dataset.province || "").trim();
      const kingdom = (link.dataset.kingdom || "").trim();
      if (!province) return;

      const url = new URL("/province", window.location.origin);
      const scope = scopeParamsFromCurrentUrl();
      scope.forEach((value, key) => url.searchParams.set(key, value));
      url.searchParams.set("name", province);
      if (kingdom) url.searchParams.set("kingdom", kingdom);
      window.location.href = url.pathname + url.search;
    });
  });
}

function renderDamageChart() {
  const canvas = document.getElementById("opsDamageChart");
  if (!canvas) return;
  const snap = window.__ops || {};
  const rows = Array.isArray(snap.timeline_rows) ? snap.timeline_rows : [];
  if (!rows.length) return;

  const labels = rows.map((row) => row.day);
  const damageDone = rows.map((row) => Number(row.damage_done || 0));
  const damageTaken = rows.map((row) => Number(row.damage_taken || 0));
  const warDone = rows.map((row) => Number(row.war_damage_done || 0));
  const warTaken = rows.map((row) => Number(row.war_damage_taken || 0));
  const hostileOps = rows.map((row) => Number(row.hostile_ops || 0));

  new Chart(canvas.getContext("2d"), {
    data: {
      labels,
      datasets: [
        {
          type: "line",
          label: "Damage Done",
          data: damageDone,
          borderColor: "rgba(51,199,127,0.95)",
          backgroundColor: "rgba(51,199,127,0.22)",
          yAxisID: "y",
          tension: 0.25,
          pointRadius: 2,
        },
        {
          type: "line",
          label: "Damage Taken",
          data: damageTaken,
          borderColor: "rgba(255,116,116,0.95)",
          backgroundColor: "rgba(255,116,116,0.2)",
          yAxisID: "y",
          tension: 0.25,
          pointRadius: 2,
        },
        {
          type: "line",
          label: "War Damage Done",
          data: warDone,
          borderColor: "rgba(111,231,157,0.9)",
          borderDash: [4, 3],
          yAxisID: "y",
          tension: 0.25,
          pointRadius: 1,
        },
        {
          type: "line",
          label: "War Damage Taken",
          data: warTaken,
          borderColor: "rgba(255,176,176,0.9)",
          borderDash: [4, 3],
          yAxisID: "y",
          tension: 0.25,
          pointRadius: 1,
        },
        {
          type: "bar",
          label: "Hostile Ops",
          data: hostileOps,
          backgroundColor: "rgba(83,177,255,0.28)",
          borderColor: "rgba(83,177,255,0.85)",
          borderWidth: 1,
          yAxisID: "y1",
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      responsive: true,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: { ticks: { color: "rgba(217,231,252,0.86)" }, grid: { color: "rgba(159,186,223,0.12)" } },
        y: { ticks: { color: "rgba(217,231,252,0.86)" }, grid: { color: "rgba(159,186,223,0.12)" } },
        y1: { position: "right", ticks: { color: "rgba(139,205,255,0.95)" }, grid: { drawOnChartArea: false } },
      },
      plugins: { legend: { labels: { color: "rgba(231,241,255,0.9)" } } },
    },
  });
}

setupSortableTables();
setupProvinceLinks();
renderDamageChart();
