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

  const resp = await fetch("/api/momentum");
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

  const resp = await fetch("/api/land_swing");
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
  await Promise.all([loadMomentumChart(), loadLandSwingChart()]);
  watchForLiveUpdates();
}

bootstrap();
