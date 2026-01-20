import { APP_STATE, QBO_FIELDS } from './state.js';
import { fetchWithAuth } from './helpers.js';
import { validateCellClientSide } from './helpers.js';
import { loadJobsSection } from './loaders/jobs.js';

export async function uploadForPreview() {
  const fileInput = document.getElementById('file');
  const file = fileInput.files[0];
  const type = document.getElementById('object_type').value;
  if (!file) return alert("Please select a CSV file");
  const form = new FormData();
  form.append('file', file);
  try {
    const res = await fetchWithAuth(`/api/import/${type}`, {
      method: 'POST',
      body: form
    });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();
    APP_STATE.currentJobId = data.job_id;
    APP_STATE.csvHeaders = data.headers || [];
    APP_STATE.previewRows = data.preview_rows || [];
    document.getElementById('upload-section').style.display = 'none';
    document.getElementById('mapping-section').style.display = 'block';
    renderMappingFields();
    autoMapColumns();
  } catch (err) {
    alert("Upload failed: " + err.message);
  }
}

export function renderMappingFields() {
  const container = document.getElementById('mapping-fields');
  container.innerHTML = APP_STATE.csvHeaders.map(header => `
    <div class="flex flex-col md:flex-row md:items-center gap-4 bg-gray-50 p-4 rounded-lg mapping-row">
      <div class="w-full md:w-72 font-medium truncate">${header}</div>
      <span class="text-gray-500 hidden md:block">→</span>
      <select class="mapping-select w-full md:flex-1 px-4 py-2 border rounded bg-white" data-header="${header}">
        ${QBO_FIELDS.map(f => `<option value="${f.value}">${f.label}</option>`).join('')}
      </select>
      <button onclick="this.parentElement.remove(); checkRequiredFields()" class="text-red-600 hover:text-red-800 text-sm">Remove</button>
    </div>
  `).join('');
  // Add custom mapping button
  container.innerHTML += `
    <button onclick="addCustomMapping()" class="mt-6 px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
      <i class="fas fa-plus mr-2"></i>Add Custom Mapping
    </button>
  `;
  document.querySelectorAll('.mapping-select').forEach(checkRequiredFields);
  document.getElementById('mapping-fields').addEventListener('change', checkRequiredFields);
}

export function addCustomMapping() {
  const container = document.getElementById('mapping-fields');
  const newRow = document.createElement('div');
  newRow.className = 'flex flex-col md:flex-row md:items-center gap-4 bg-gray-50 p-4 rounded-lg mapping-row';
  newRow.innerHTML = `
    <input type="text" placeholder="New CSV Column Name" class="w-full md:w-72 px-4 py-2 border rounded custom-header">
    <span class="text-gray-500 hidden md:block">→</span>
    <select class="mapping-select w-full md:flex-1 px-4 py-2 border rounded bg-white">
      ${QBO_FIELDS.map(f => `<option value="${f.value}">${f.label}</option>`).join('')}
    </select>
    <button onclick="this.parentElement.remove(); checkRequiredFields()" class="text-red-600 hover:text-red-800 text-sm">Remove</button>
  `;
  container.insertBefore(newRow, container.lastElementChild);
}

export function autoMapColumns() {
  document.querySelectorAll('.mapping-select').forEach(select => {
    const header = select.dataset.header?.toLowerCase() || document.querySelector('.custom-header')?.value.toLowerCase() || '';
    if (!select.value) {
      if (header.includes('display') || header.includes('name') || header.includes('company') || header.includes('customer')) select.value = 'DisplayName';
      else if (header.includes('email')) select.value = 'PrimaryEmailAddr.Address';
      else if (header.includes('phone') && !header.includes('mobile') && !header.includes('fax')) select.value = 'PrimaryPhone.FreeFormNumber';
      else if (header.includes('mobile')) select.value = 'Mobile.FreeFormNumber';
      else if (header.includes('fax')) select.value = 'Fax.FreeFormNumber';
      else if (header.includes('website') || header.includes('web')) select.value = 'WebAddr.URI';
      else if (header.includes('city')) select.value = 'BillAddr.City';
      else if (header.includes('state') || header.includes('province')) select.value = 'BillAddr.CountrySubDivisionCode';
      else if (header.includes('zip') || header.includes('postal')) select.value = 'BillAddr.PostalCode';
    }
  });
  checkRequiredFields();
}

export function checkRequiredFields() {
  const hasDisplayName = [...document.querySelectorAll('.mapping-select')].some(s => s.value === 'DisplayName');
  const btn = document.getElementById('start-import-btn');
  if (hasDisplayName) {
    btn.disabled = false;
    btn.classList.remove('opacity-50', 'cursor-not-allowed');
  } else {
    btn.disabled = true;
    btn.classList.add('opacity-50', 'cursor-not-allowed');
  }
}

export function proceedToPreview() {
  APP_STATE.mapping = {};
  document.querySelectorAll('.mapping-row').forEach(row => {
    const headerInput = row.querySelector('.custom-header');
    const header = headerInput ? headerInput.value.trim() : row.querySelector('.mapping-select').dataset.header;
    const select = row.querySelector('.mapping-select');
    if (header && select.value) {
      APP_STATE.mapping[header] = select.value;
    }
  });
  document.getElementById('mapping-section').style.display = 'none';
  document.getElementById('preview-section').style.display = 'block';
  document.getElementById('validation-summary').classList.add('hidden');
  document.getElementById('dry-run-btn').textContent = "Run Dry Run Simulation";
  document.getElementById('start-real-import-btn').classList.add('hidden');
  APP_STATE.overrideDuplicates = false;
  document.getElementById('override-duplicates').checked = false;
  renderPreviewTableClean();
}

export function backToMapping() {
  document.getElementById('preview-section').style.display = 'none';
  document.getElementById('mapping-section').style.display = 'block';
}

export function cancelPreview() {
  document.getElementById('preview-section').style.display = 'none';
  document.getElementById('mapping-section').style.display = 'none';
  document.getElementById('upload-section').style.display = 'block';
  document.getElementById('file').value = '';
  APP_STATE.currentJobId = null;
  APP_STATE.csvHeaders = [];
  APP_STATE.previewRows = [];
  APP_STATE.mapping = {};
}

export function renderPreviewTableClean() {
  const thead = document.querySelector('#preview-table thead');
  const tbody = document.querySelector('#preview-table tbody');
  thead.innerHTML = `
    <tr>
      <th class="px-4 md:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">#</th>
      <th class="px-4 md:px-6 py-3 text-left text-xs font-medium text-red-700 uppercase">Issues</th>
      ${APP_STATE.csvHeaders.map(h => `<th class="px-4 md:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">${h}</th>`).join('')}
    </tr>`;
  tbody.innerHTML = APP_STATE.previewRows.map((row, idx) => `
    <tr class="hover:bg-gray-50 transition-colors">
      <td class="px-4 md:px-6 py-4 whitespace-nowrap text-sm text-gray-700 font-medium">
        ${idx + 1}
      </td>
      <td class="px-4 md:px-6 py-4 text-sm text-gray-400 italic">
        None
      </td>
      ${APP_STATE.csvHeaders.map(header => {
        const mappedPath = APP_STATE.mapping[header] || '';
        return `<td class="px-4 md:px-6 py-4 text-sm">
          <input type="text"
                 value="${(row[header] || '').replace(/"/g, '&quot;')}"
                 class="w-full px-3 py-2 border border-gray-300 rounded bg-white focus:outline-none focus:ring-2 focus:ring-indigo-500 transition-shadow"
                 oninput="handleCellEdit(this, ${idx}, '${header}')">
          ${mappedPath ? `<div class="text-xs text-green-600 mt-1 font-medium">→ ${mappedPath}</div>` : ''}
        </td>`;
      }).join('')}
    </tr>
  `).join('');
}

export function updateDryRunButton() {
  const btn = document.getElementById('dry-run-btn');
  if (APP_STATE.hasUnvalidatedChanges) {
    btn.textContent = "Validate Changes ⚡";
    btn.classList.add('animate-pulse', 'ring-4', 'ring-indigo-300', 'bg-indigo-700');
  } else {
    btn.textContent = APP_STATE.currentValidation ? "Re-run Dry Run" : "Run Dry Run Simulation";
    btn.classList.remove('animate-pulse', 'ring-4', 'ring-indigo-300', 'bg-indigo-700');
  }
}

export function updateSummaryDisplay() {
  if (!APP_STATE.lastDryRunSummary) return;
  const s = APP_STATE.lastDryRunSummary;
  const override = APP_STATE.overrideDuplicates;
  const effectiveSucceed = override ? s.will_succeed + APP_STATE.qboDuplicateOnlyRows : s.will_succeed;
  const effectiveFail = s.total_rows - effectiveSucceed;
  document.getElementById('success-count').textContent = effectiveSucceed;
  document.getElementById('fail-count').textContent = effectiveFail;
  const effectiveZeroFail = effectiveFail === 0;
  if (effectiveZeroFail) {
    document.getElementById('summary-text').textContent = "🎉 Ready to import!";
    document.getElementById('validation-summary').className = "mb-8 p-6 rounded-lg border bg-green-50 border-green-300";
    document.getElementById('start-real-import-btn').textContent = "Confirm & Start Import";
    document.getElementById('start-real-import-btn').className = "px-8 py-4 bg-green-600 text-white text-lg rounded-lg hover:bg-green-700";
  } else {
    document.getElementById('summary-text').textContent = "⚠️ Some rows have issues. Edit or override QBO duplicates.";
    document.getElementById('validation-summary').className = "mb-8 p-6 rounded-lg border bg-orange-50 border-orange-300";
    document.getElementById('start-real-import-btn').textContent = "Import Safe Rows Only";
    document.getElementById('start-real-import-btn').className = "px-8 py-4 bg-orange-600 text-white text-lg rounded-lg hover:bg-orange-700";
  }
  document.getElementById('start-real-import-btn').disabled = !effectiveZeroFail;
}

export function renderPreviewTableWithValidation(validationRows) {
  APP_STATE.lastDryRunRows = validationRows;
  APP_STATE.currentValidation = {};
  validationRows.forEach(r => {
    APP_STATE.currentValidation[r.row_number] = {
      status: r.status,
      issues: r.issues || []
    };
  });
  APP_STATE.qboDuplicateOnlyRows = 0;
  validationRows.forEach(r => {
    if (r.status === "error") {
      const hasNonQboDup = r.issues.some(iss => iss.code !== "qbo_duplicate_displayname");
      const hasQboDup = r.issues.some(iss => iss.code === "qbo_duplicate_displayname");
      if (!hasNonQboDup && hasQboDup) APP_STATE.qboDuplicateOnlyRows++;
    }
  });
  APP_STATE.hasUnvalidatedChanges = false;
  updateDryRunButton();
  updateSummaryDisplay();
  const thead = document.querySelector('#preview-table thead');
  const tbody = document.querySelector('#preview-table tbody');
  thead.innerHTML = `
    <tr>
      <th class="px-4 md:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">#</th>
      <th class="px-4 md:px-6 py-3 text-left text-xs font-medium text-red-700 uppercase">Issues</th>
      ${APP_STATE.csvHeaders.map(h => `<th class="px-4 md:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">${h}</th>`).join('')}
    </tr>`;
  let html = '';
  APP_STATE.previewRows.forEach((row, idx) => {
    const rowNum = idx + 2;
    const val = APP_STATE.currentValidation[rowNum] || { status: "valid", issues: [] };
    let filteredIssues = val.issues;
    if (APP_STATE.overrideDuplicates) {
      filteredIssues = filteredIssues.filter(iss => iss.code !== "qbo_duplicate_displayname");
    }
    const effectiveStatus = filteredIssues.length === 0 ? "valid" : "error";
    // Collect all issues for Issues column
    const allIssues = filteredIssues.map(iss => {
      let field = iss.field || "General";
      let niceField = field
        .replace("PrimaryEmailAddr.Address", "Email")
        .replace("PrimaryPhone.FreeFormNumber", "Phone")
        .replace("Mobile.FreeFormNumber", "Mobile")
        .replace("WebAddr.URI", "Website")
        .replace("BillAddr.", "Billing ")
        .replace("ShipAddr.", "Shipping ");
      return `<div class="text-xs text-red-700 font-medium">⚠ ${niceField}: ${iss.message}</div>`;
    }).join('');
    const issuesDisplay = allIssues || '<span class="text-gray-400 text-xs italic">None</span>';
    const rowClass = effectiveStatus === "valid"
      ? "bg-white hover:bg-green-50 border-l-4 border-green-500"
      : "bg-white hover:bg-red-50 border-l-4 border-red-500";
    html += `<tr class="${rowClass}">
      <td class="px-4 md:px-6 py-4 whitespace-nowrap text-sm font-bold ${effectiveStatus === 'valid' ? 'text-green-800' : 'text-red-800'}">
        ${idx + 1}
        ${effectiveStatus === 'valid' ? '<span class="ml-2 text-green-600 text-lg">✓</span>' : '<span class="ml-2 text-red-600 text-lg">✗</span>'}
      </td>
      <td class="px-4 md:px-6 py-4 text-sm align-top max-w-xs">
        ${issuesDisplay}
      </td>
      ${APP_STATE.csvHeaders.map(header => {
        const mappedPath = APP_STATE.mapping[header] || '';
        const fieldIssues = filteredIssues.filter(iss =>
          iss.field && (
            iss.field === mappedPath ||
            iss.field.endsWith("." + mappedPath.split(".").pop()) ||
            mappedPath.includes(iss.field)
          )
        );
        const hasError = fieldIssues.length > 0;
        const cellBg = hasError ? "bg-red-100" : "bg-green-100";
        const icon = hasError ? "⚠" : "✓";
        const msg = hasError ? fieldIssues.map(i => i.message).join("<br>") : "Valid";
        return `<td class="px-4 md:px-6 py-4 ${cellBg}">
          <input type="text"
                 value="${(row[header] || '').replace(/"/g, '&quot;')}"
                 class="w-full px-3 py-2 border rounded-md focus:ring-2 focus:ring-indigo-500 bg-white"
                 oninput="handleCellEdit(this, ${idx}, '${header}')">
          ${mappedPath ? `<div class="text-xs text-green-700 mt-1 font-medium">→ ${mappedPath}</div>` : ''}
          <div class="mt-2 text-xs font-medium ${hasError ? 'text-red-700' : 'text-green-700'}">
            <span class="text-lg">${icon}</span> ${msg}
          </div>
        </td>`;
      }).join('')}
    </tr>`;
  });
  tbody.innerHTML = html;
}

export async function startDryRun() {
  if (!APP_STATE.mapping || Object.keys(APP_STATE.mapping).length === 0) {
    alert("Please complete mapping first");
    return;
  }
  const payload = {
    mapping: APP_STATE.mapping,
    rows: APP_STATE.previewRows
  };
  try {
    const res = await fetchWithAuth(`/api/import/customer/${APP_STATE.currentJobId}/dry-run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!res.ok) throw new Error(await res.text() || "Dry run failed");
    const data = await res.json();
    const s = data.summary;
    APP_STATE.lastDryRunSummary = s;
    APP_STATE.lastDryRunRows = data.rows;
    document.getElementById('validation-summary').classList.remove('hidden');
    document.getElementById('dry-run-btn').textContent = "Re-run Dry Run";
    document.getElementById('start-real-import-btn').classList.remove('hidden');
    // Fixed counts (unchanged by override)
    document.getElementById('total-count').textContent = s.total_rows;
    document.getElementById('warning-count').textContent = s.warnings;
    // Initial override state
    APP_STATE.overrideDuplicates = document.getElementById('override-duplicates').checked;
    renderPreviewTableWithValidation(data.rows);
    // Live update on checkbox toggle
    document.getElementById('override-duplicates').onchange = () => {
      APP_STATE.overrideDuplicates = document.getElementById('override-duplicates').checked;
      if (APP_STATE.lastDryRunRows) {
        renderPreviewTableWithValidation(APP_STATE.lastDryRunRows);
      }
    };
  } catch (err) {
    alert("Dry run failed: " + err.message);
  }
}

export async function startRealImport() {
  if (!confirm("This will import data into QuickBooks. Continue?")) return;
  const payload = {
    mapping: APP_STATE.mapping,
    rows: APP_STATE.previewRows,
    override_existing: document.getElementById('override-duplicates').checked
  };
  try {
    const res = await fetchWithAuth(`/api/import/customer/${APP_STATE.currentJobId}/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!res.ok) throw new Error(await res.text());
    alert("Import started successfully! Check 'Recent Jobs' for progress.");
    cancelPreview();
    loadJobsSection();
  } catch (err) {
    alert("Import failed to start: " + err.message);
  }
}