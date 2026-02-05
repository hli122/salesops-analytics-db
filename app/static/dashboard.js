async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return await res.json();
}

function money(x) {
  const n = Number(x || 0);
  return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function num(x) {
  const n = Number(x || 0);
  return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function tableFromRows(headers, rows, rowMapper) {
  if (!rows || rows.length === 0) return `<div class="muted">No rows</div>`;
  const thead = `<tr>${headers.map(h => `<th>${h}</th>`).join("")}</tr>`;
  const tbody = rows.map(r => `<tr>${rowMapper(r).map(c => `<td>${c}</td>`).join("")}</tr>`).join("");
  return `<table><thead>${thead}</thead><tbody>${tbody}</tbody></table>`;
}

function setStatus(msg, isError=false) {
  const el = document.getElementById("status");
  el.textContent = msg;
  el.className = isError ? "bad" : "muted";
}

function todayStr() {
  const d = new Date();
  return d.toISOString().slice(0,10);
}

function addDaysStr(iso, delta) {
  const d = new Date(iso);
  d.setDate(d.getDate() + delta);
  return d.toISOString().slice(0,10);
}

async function loadAll() {
  const start = document.getElementById("startDate").value;
  const end = document.getElementById("endDate").value;
  const limit = Number(document.getElementById("limit").value || 12);
  const tol = Number(document.getElementById("tol").value || 0.05);

  if (!start || !end) {
    setStatus("Please select start_date and end_date.", true);
    return;
  }

  const btn = document.getElementById("loadBtn");
  btn.disabled = true;
  setStatus("Loading...");

  try {
    // Your existing APIs
    const q = `start_date=${encodeURIComponent(start)}&end_date=${encodeURIComponent(end)}`;
    const weekly = await fetchJson(`/reports/weekly-summary?${q}`);
    const sellers = await fetchJson(`/reports/seller-ranking?${q}`);
    const topProducts = await fetchJson(`/reports/top-products?${q}&limit=${limit}`);
    const shipping = await fetchJson(`/reports/shipping-breakdown?${q}`);

    // If you have these endpoints, use them; otherwise you can skip/adjust
    let dq = null;
    let dqSamples = null;
    try {
      dq = await fetchJson(`/reports/data-quality?${q}&tol=${tol}`);
    } catch (e) {
      dq = { error: String(e) };
    }
    try {
      dqSamples = await fetchJson(`/reports/data-quality-samples?${q}&tol=${tol}&limit=${Math.min(50, limit)}`);
    } catch (e) {
      dqSamples = { error: String(e) };
    }

    // Weekly summary
    document.getElementById("weeklySummary").innerHTML = `
      <div><b>Revenue</b>: $${money(weekly.revenue)}</div>
      <div><b>Units</b>: ${num(weekly.units)}</div>
      <div><b>Line count</b>: ${Number(weekly.line_count || 0).toLocaleString()}</div>
      <div class="muted">${weekly.start_date} → ${weekly.end_date}</div>
    `;

    // Seller ranking
    document.getElementById("sellerTable").innerHTML = tableFromRows(
      ["Seller", "Revenue", "Units", "Lines"],
      sellers.sellers,
      r => [r.seller_name, `$${money(r.revenue)}`, num(r.units), Number(r.line_count || 0).toLocaleString()]
    );

    // Shipping
    document.getElementById("shipTable").innerHTML = tableFromRows(
      ["Shipping", "Revenue", "Lines"],
      shipping.shipping_companies,
      r => [r.shipping_company, `$${money(r.revenue)}`, Number(r.line_count || 0).toLocaleString()]
    );

    // Top products
    document.getElementById("productTable").innerHTML = tableFromRows(
      ["Product", "Revenue", "Units"],
      topProducts.top_products,
      r => [r.product_code, `$${money(r.revenue)}`, num(r.units)]
    );

    // Data quality summary
    if (dq && !dq.error) {
      const status = (dq.mismatched_total_count || dq.negative_amount_count || dq.nonpositive_units_count) ? "WARN" : "OK";
      document.getElementById("dqSummary").innerHTML = `
        <div>Status: <span class="${status === "OK" ? "ok" : "bad"}">${status}</span></div>
        <div><b>Rows</b>: ${Number(dq.rows_in_range || 0).toLocaleString()}</div>
        <div><b>Mismatched totals</b>: ${Number(dq.mismatched_total_count || 0).toLocaleString()}</div>
        <div><b>Non-positive units</b>: ${Number(dq.nonpositive_units_count || 0).toLocaleString()}</div>
        <div><b>Negative amounts</b>: ${Number(dq.negative_amount_count || 0).toLocaleString()}</div>
        <div><b>Missing shipping</b>: ${Number(dq.missing_shipping_company_count || 0).toLocaleString()}</div>
      `;
    } else {
      document.getElementById("dqSummary").innerHTML = `<div class="bad">DQ endpoint error</div><div class="muted">${dq?.error || ""}</div>`;
    }

    // DQ samples
    if (dqSamples && !dqSamples.error) {
      const rows = dqSamples.samples || dqSamples.rows || dqSamples.data || [];
      document.getElementById("dqSamples").innerHTML = tableFromRows(
        ["Time", "Date", "Product", "Seller", "Ship", "Unit", "Units", "Total", "Expected", "Diff"],
        rows,
        r => [
          r.sale_time, r.sale_date, r.product_code, r.seller_name, r.shipping_company,
          money(r.unit_price), num(r.units), money(r.line_total), money(r.expected_total), money(r.diff)
        ]
      );
    } else {
      document.getElementById("dqSamples").innerHTML = `<div class="bad">DQ samples endpoint not available</div><div class="muted">${dqSamples?.error || ""}</div>`;
    }

    setStatus("Loaded.");
  } catch (err) {
    setStatus(String(err), true);
  } finally {
    btn.disabled = false;
  }
}

function initDefaults() {
  // Default date range: last 7 days ending today
  const end = todayStr();
  const start = addDaysStr(end, -6);
  document.getElementById("startDate").value = start;
  document.getElementById("endDate").value = end;
}

document.getElementById("loadBtn").addEventListener("click", loadAll);
initDefaults();
