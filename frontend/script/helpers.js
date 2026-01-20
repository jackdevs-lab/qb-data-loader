import { showToast } from './utils.js';
import { APP_STATE } from './state.js';

export async function fetchWithAuth(url, options = {}) {
  const headers = new Headers(options.headers || {});
  let fetchOptions = {
    ...options,
    headers,
    credentials: "include"
  };
  if (!Clerk.session) {
    return fetch(url, fetchOptions);
  }
  try {
    const token = await Clerk.session.getToken();
    headers.set('Authorization', `Bearer ${token}`);
    return fetch(url, fetchOptions);
  } catch (err) {
    console.error("Token error:", err);
    return fetch(url, fetchOptions);
  }
}

export async function updateQBOStatus() {
  try {
    const res = await fetchWithAuth('/api/jobs/test-qbo-connection');
    if (res.ok) {
      const data = await res.json();
      APP_STATE.qboConnected = true;
      document.getElementById('connect-qbo-btn').style.display = 'none';
      document.getElementById('qbo-status').innerHTML = `<span class="text-green-600 font-medium">✓ Connected to ${data.company_name}</span>`;
      document.getElementById('qbo-status-mobile').innerHTML = `<span class="text-green-600 font-medium">✓ Connected to ${data.company_name}</span>`;
    } else {
      APP_STATE.qboConnected = false;
      document.getElementById('connect-qbo-btn').style.display = 'inline-block';
      document.getElementById('qbo-status').textContent = 'Not connected';
      document.getElementById('qbo-status-mobile').textContent = 'Not connected';
    }
  } catch (err) {
    APP_STATE.qboConnected = false;
    document.getElementById('connect-qbo-btn').style.display = 'inline-block';
    document.getElementById('qbo-status').textContent = 'Not connected';
    document.getElementById('qbo-status-mobile').textContent = 'Not connected';
  }
}

export function validateCellClientSide(value, mappedPath) {
  const errorIssues = [];
  const infoIssues = [];
  value = (value || "").trim();
  if (!mappedPath) return {errorIssues, infoIssues};
  if (mappedPath === "DisplayName") {
    if (!value) errorIssues.push("Required");
    if (value.length > 500) errorIssues.push("Max 500 characters");
    if (/[:\t\n]/.test(value)) errorIssues.push("Cannot contain : or line breaks");
  }
  else if (mappedPath === "PrimaryEmailAddr.Address") {
    const junk = ["invalid email", "n/a", "none", "-", "no email", ""];
    if (junk.includes(value.toLowerCase())) errorIssues.push("Invalid/junk email");
    if (value && !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(value)) errorIssues.push("Invalid email format");
    if (value.endsWith('.')) errorIssues.push("Domain cannot end with .");
    if (/\.\./.test(value)) errorIssues.push("No double dots in domain");
    if ((value.match(/@/g) || []).length !== 1) errorIssues.push("Must have exactly one @");
  }
  else if (mappedPath.includes("FreeFormNumber")) {
    if (value.length > 30) errorIssues.push("Max 30 characters");
  }
  else if (mappedPath === "WebAddr.URI") {
    const junk = ["invalid website", "n/a", "none", "-", "no website"];
    if (junk.includes(value.toLowerCase())) errorIssues.push("Invalid/junk website");
    if (value && !/^https?:\/\//i.test(value)) {
      infoIssues.push("Server will automatically add https://");
    }
  }
  else if (mappedPath === "Title" || mappedPath === "Suffix") {
    if (value.length > 16) errorIssues.push("Max 16 characters");
  }
  else if (["GivenName", "MiddleName", "FamilyName", "CompanyName"].some(f => mappedPath.includes(f))) {
    if (value.length > 100) errorIssues.push("Max 100 characters");
  }
  return {errorIssues, infoIssues};
}

export function handleCellEdit(inputElement, rowIndex, header) {
  const newValue = inputElement.value.trim();
  const oldValue = (APP_STATE.previewRows[rowIndex][header] || '').trim();
  if (newValue !== oldValue) {
    APP_STATE.previewRows[rowIndex][header] = newValue;
    APP_STATE.hasUnvalidatedChanges = true;
    updateDryRunButton();
    document.getElementById('start-real-import-btn').disabled = true;
  }
  const mappedPath = APP_STATE.mapping[header] || '';
  const {errorIssues, infoIssues} = validateCellClientSide(newValue, mappedPath);
  // Clear previous messages/borders
  const cell = inputElement.parentElement;
  cell.querySelectorAll('.client-error, .client-info').forEach(el => el.remove());
  inputElement.classList.remove('border-red-500', 'ring-red-300', 'border-yellow-500', 'ring-yellow-300');
  // Apply styling and messages
  if (errorIssues.length > 0) {
    inputElement.classList.add('border-red-500', 'ring-2', 'ring-red-300');
    const div = document.createElement('div');
    div.className = 'text-xs text-red-700 font-medium mt-1 client-error';
    div.innerHTML = errorIssues.map(m => `⚠ ${m}`).join('<br>');
    cell.appendChild(div);
  } else if (infoIssues.length > 0) {
    inputElement.classList.add('border-yellow-500', 'ring-2', 'ring-yellow-300');
    const div = document.createElement('div');
    div.className = 'text-xs text-blue-700 font-medium mt-1 client-info';
    div.innerHTML = infoIssues.map(m => `ℹ ${m}`).join('<br>');
    cell.appendChild(div);
  }
}

export function initSalesChart(monthlyData) {
  const ctx = document.getElementById('sales-chart').getContext('2d');
 
  const labels = monthlyData.map(item => item.month);
  const sales = monthlyData.map(item => item.sales || 0);
 
  new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: 'Sales ($)',
        data: sales,
        borderColor: 'rgb(59, 130, 246)',
        backgroundColor: 'rgba(59, 130, 246, 0.1)',
        fill: true,
        tension: 0.4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: true,
          position: 'top',
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback: function(value) {
              return '$' + value.toLocaleString();
            }
          }
        }
      }
    }
  });
}