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

setupSortableTables();
setupProvinceLinks();
