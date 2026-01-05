const jobsEl = document.getElementById('jobs');
  let currentJobId = null;
  let hasUnvalidatedChanges = false;
  let csvHeaders = [];
  let previewRows = [];
  let mapping = {};
  let currentValidation = {};
  let overrideDuplicates = false;
  let qboDuplicateOnlyRows = 0;
  let lastDryRunSummary = null;
  let lastDryRunRows = null;

  const QBO_FIELDS = [ 
    { value: "", label: "-- Not mapped --" },
    { value: "DisplayName", label: "DisplayName * (Required)" },
    { value: "CompanyName", label: "Company Name" },
    { value: "Title", label: "Title (Mr/Mrs)" },
    { value: "GivenName", label: "First Name" },
    { value: "MiddleName", label: "Middle Name" },
    { value: "FamilyName", label: "Last Name" },
    { value: "Suffix", label: "Suffix (Jr/Sr)" },
    { value: "PrintOnCheckName", label: "Print on Check As" },
    { value: "PrimaryEmailAddr.Address", label: "Primary Email" },
    { value: "WebAddr.URI", label: "Website" },
    { value: "PrimaryPhone.FreeFormNumber", label: "Primary Phone" },
    { value: "Mobile.FreeFormNumber", label: "Mobile Phone" },
    { value: "Fax.FreeFormNumber", label: "Fax" },
    { value: "AlternatePhone.FreeFormNumber", label: "Alternate Phone" },
    { value: "BillAddr.Line1", label: "Billing Address Line 1" },
    { value: "BillAddr.Line2", label: "Billing Address Line 2" },
    { value: "BillAddr.Line3", label: "Billing Address Line 3" },
    { value: "BillAddr.City", label: "Billing City" },
    { value: "BillAddr.CountrySubDivisionCode", label: "Billing State/Province" },
    { value: "BillAddr.PostalCode", label: "Billing ZIP/Postal" },
    { value: "BillAddr.Country", label: "Billing Country" },
    { value: "ShipAddr.Line1", label: "Shipping Address Line 1" },
    { value: "ShipAddr.Line2", label: "Shipping Address Line 2" },
    { value: "ShipAddr.City", label: "Shipping City" },
    { value: "ShipAddr.CountrySubDivisionCode", label: "Shipping State/Province" },
    { value: "ShipAddr.PostalCode", label: "Shipping ZIP/Postal" },
    { value: "ShipAddr.Country", label: "Shipping Country" },
    { value: "Notes", label: "Notes" },
    { value: "Taxable", label: "Taxable (true/false)" },
    { value: "Active", label: "Active (true/false)" },
    { value: "CurrencyRef.value", label: "Currency Code (e.g. USD)" }
  ];
  function validateCellClientSide(value, mappedPath) {
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
  function handleCellEdit(inputElement, rowIndex, header) {
    const newValue = inputElement.value.trim();
    const oldValue = (previewRows[rowIndex][header] || '').trim();

    if (newValue !== oldValue) {
      previewRows[rowIndex][header] = newValue;
      hasUnvalidatedChanges = true;
      updateDryRunButton();
      document.getElementById('start-real-import-btn').disabled = true;
    }

    const mappedPath = mapping[header] || '';
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
  // === AUTH & STATUS (unchanged) ===
  async function startQBOAuth() {
    const btn = document.getElementById('connect-qbo-btn');
    btn.disabled = true;
    btn.textContent = 'Opening QuickBooks...';

    try {
      const res = await fetchWithAuth('/api/auth_qbo/qbo/login');
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();

      const width = 600, height = 700;
      const left = screen.width / 2 - width / 2;
      const top = screen.height / 2 - height / 2;
      const popup = window.open(
        data.redirect_url,
        'qbo_auth',
        `width=${width},height=${height},left=${left},top=${top},resizable=yes,scrollbars=yes`
      );

      if (!popup) {
        alert('Popup blocked! Please allow popups for this site.');
        btn.disabled = false;
        btn.textContent = 'Connect to QuickBooks';
        return;
      }

      const handleSuccess = async (event) => {
        if (event.data === 'qbo_connected') {
          window.removeEventListener('message', handleSuccess);
          await updateQBOStatus();
          await loadJobs();
          btn.disabled = false;
          btn.textContent = 'Connect to QuickBooks';
        }
      };
      window.addEventListener('message', handleSuccess);

      const checkClosed = setInterval(async () => {
        if (popup.closed) {
          clearInterval(checkClosed);
          window.removeEventListener('message', handleSuccess);
          await updateQBOStatus();
          await loadJobs();
          btn.disabled = false;
          btn.textContent = 'Connect to QuickBooks';
        }
      }, 500);

    } catch (err) {
      alert('Error: ' + err.message);
      btn.disabled = false;
      btn.textContent = 'Connect to QuickBooks';
    }
  }

async function fetchWithAuth(url, options = {}) {
  // Create a proper Headers object to avoid overwriting
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
async function updateQBOStatus() {
    try {
      const res = await fetchWithAuth('/api/jobs/test-qbo-connection');
      if (res.ok) {
        const data = await res.json();
        document.getElementById('connect-qbo-btn').style.display = 'none';
        document.getElementById('qbo-status').innerHTML = `<span class="text-green-600 font-medium">✓ Connected to ${data.company_name}</span>`;
      } else {
        document.getElementById('connect-qbo-btn').style.display = 'inline-block';
        document.getElementById('qbo-status').textContent = 'Not connected';
      }
    } catch (err) {
      document.getElementById('connect-qbo-btn').style.display = 'inline-block';
      document.getElementById('qbo-status').textContent = 'Not connected';
    }
  }
async function loadJobs() {
    try {
      const res = await fetchWithAuth('/api/jobs');
      if (!res.ok) {
        if (res.status === 401) {
          alert("Session expired — please sign in again.");
          Clerk.openSignIn();
          return;
        } else {
          jobsEl.innerHTML = '<p class="text-red-600">Failed to load jobs.</p>';
        }
        return;
      }
      const jobs = await res.json();
      if (jobs.length === 0) {
        jobsEl.innerHTML = '<p class="text-gray-600">No jobs yet. Upload a file to get started!</p>';
        return;
      }
      jobsEl.innerHTML = jobs.map(job => `
        <div class="bg-white p-6 rounded-lg shadow border-l-4 ${job.status.includes('error') || job.status.includes('fail') ? 'border-red-500' : 'border-green-500'}">
          <div class="flex justify-between">
            <div>
              <div class="font-semibold text-lg">${job.filename || 'Unknown file'}</div>
              <div class="text-sm text-gray-600">${job.object_type} • ${new Date(job.created_at).toLocaleString()}</div>
            </div>
            <div class="text-right">
              <div class="text-2xl font-bold">${job.status.replace(/_/g, ' ')}</div>
            </div>
          </div>
          <div class="mt-4 text-sm">
            Rows: ${job.total_rows || '?'}
            ${job.failed_rows !== undefined ? ` | Failed: ${job.failed_rows}` : ''}
          </div>
        </div>
      `).join('');
    } catch (err) {
      console.error("loadJobs error:", err);
      jobsEl.innerHTML = '<p class="text-red-600">Error loading jobs.</p>';
    }
  }
  // === UPLOAD & MAPPING ===
async function uploadForPreview() {
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
    currentJobId = data.job_id;
    csvHeaders = data.headers || [];
    previewRows = data.preview_rows || [];  // ← Now used!

    document.getElementById('upload-section').style.display = 'none';
    document.getElementById('mapping-section').style.display = 'block';

    renderMappingFields();     // ← Use the new dynamic renderer
    autoMapColumns();          // ← Use the improved auto-mapping
  } catch (err) {
    alert("Upload failed: " + err.message);
  }
}
function renderMappingFields() {
  const container = document.getElementById('mapping-fields');
  container.innerHTML = csvHeaders.map(header => `
    <div class="flex items-center gap-6 bg-gray-50 p-4 rounded-lg mapping-row">
      <div class="w-72 font-medium truncate">${header}</div>
      <span class="text-gray-500">→</span>
      <select class="mapping-select flex-1 px-4 py-2 border rounded bg-white" data-header="${header}">
        ${QBO_FIELDS.map(f => `<option value="${f.value}">${f.label}</option>`).join('')}
      </select>
      <button onclick="this.parentElement.remove(); checkRequiredFields()" class="text-red-600 hover:text-red-800">Remove</button>
    </div>
  `).join('');

  // Add custom mapping button
  container.innerHTML += `
    <button onclick="addCustomMapping()" class="mt-6 px-6 py-3 bg-blue-600 text-white rounded hover:bg-blue-700">
      + Add Custom Mapping
    </button>
  `;

  document.querySelectorAll('.mapping-select').forEach(checkRequiredFields);
  document.getElementById('mapping-fields').addEventListener('change', checkRequiredFields);
}
function addCustomMapping() {
  const container = document.getElementById('mapping-fields');
  const newRow = document.createElement('div');
  newRow.className = 'flex items-center gap-6 bg-gray-50 p-4 rounded-lg mapping-row';
  newRow.innerHTML = `
    <input type="text" placeholder="New CSV Column Name" class="w-72 px-4 py-2 border rounded custom-header">
    <span class="text-gray-500">→</span>
    <select class="mapping-select flex-1 px-4 py-2 border rounded bg-white">
      ${QBO_FIELDS.map(f => `<option value="${f.value}">${f.label}</option>`).join('')}
    </select>
    <button onclick="this.parentElement.remove(); checkRequiredFields()" class="text-red-600 hover:text-red-800">Remove</button>
  `;
  container.insertBefore(newRow, container.lastElementChild);
}
function autoMapColumns() {
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
function checkRequiredFields() {
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
  async function proceedToPreview() {
    mapping = {};
    document.querySelectorAll('.mapping-row').forEach(row => {
      const headerInput = row.querySelector('.custom-header');
      const header = headerInput ? headerInput.value.trim() : row.querySelector('.mapping-select').dataset.header;
      const select = row.querySelector('.mapping-select');
      if (header && select.value) {
        mapping[header] = select.value;
      }
    });

    document.getElementById('mapping-section').style.display = 'none';
    document.getElementById('preview-section').style.display = 'block';

    document.getElementById('validation-summary').classList.add('hidden');
    document.getElementById('dry-run-btn').textContent = "Run Dry Run Simulation";
    document.getElementById('start-real-import-btn').classList.add('hidden');
    document.getElementById('override-duplicates').checked = false;

    renderPreviewTableClean();
  }

  function backToMapping() {
    document.getElementById('preview-section').style.display = 'none';
    document.getElementById('mapping-section').style.display = 'block';
  }

  function cancelPreview() {
    document.getElementById('preview-section').style.display = 'none';
    document.getElementById('mapping-section').style.display = 'none';
    document.getElementById('upload-section').style.display = 'block';
    document.getElementById('file').value = '';
    currentJobId = null;
    csvHeaders = [];
    previewRows = [];
    mapping = {};
  }

  function renderPreviewTableClean() {
    const thead = document.querySelector('#preview-table thead');
    const tbody = document.querySelector('#preview-table tbody');

    thead.innerHTML = `
      <tr>
        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">#</th>
        <th class="px-6 py-3 text-left text-xs font-medium text-red-700 uppercase tracking-wider">Issues</th>
        ${csvHeaders.map(h => `<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">${h}</th>`).join('')}
      </tr>`;

    tbody.innerHTML = previewRows.map((row, idx) => `
    <tr class="hover:bg-gray-50 transition-colors">
      <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-700 font-medium">
        ${idx + 1}
      </td>
      <td class="px-6 py-4 text-sm text-gray-400 italic">
        None
      </td>
      ${csvHeaders.map(header => {
        const mappedPath = mapping[header] || '';
        return `<td class="px-6 py-4 text-sm">
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


function updateDryRunButton() {
    const btn = document.getElementById('dry-run-btn');
    if (hasUnvalidatedChanges) {
      btn.textContent = "Validate Changes ⚡";
      btn.classList.add('animate-pulse', 'ring-4', 'ring-indigo-300', 'bg-indigo-700');
    } else {
      btn.textContent = currentValidation ? "Re-run Dry Run" : "Run Dry Run Simulation";
      btn.classList.remove('animate-pulse', 'ring-4', 'ring-indigo-300', 'bg-indigo-700');
    }
  }
  function updateSummaryDisplay() {
    if (!lastDryRunSummary) return;

    const s = lastDryRunSummary;
    const override = overrideDuplicates;

    const effectiveSucceed = override ? s.will_succeed + qboDuplicateOnlyRows : s.will_succeed;
    const effectiveFail = s.total_rows - effectiveSucceed;

    document.getElementById('success-count').textContent = effectiveSucceed;
    document.getElementById('fail-count').textContent = effectiveFail;

    const effectiveZeroFail = effectiveFail === 0;

    if (effectiveZeroFail) {
      document.getElementById('summary-text').textContent = "🎉 Ready to import!";
      document.getElementById('validation-summary').className = "mb-8 p-6 rounded-lg border bg-green-50 border-green-300";
      document.getElementById('start-real-import-btn').textContent = "Confirm & Start Import";
      document.getElementById('start-real-import-btn').className = "px-8 py-4 bg-green-600 text-white text-lg rounded hover:bg-green-700";
    } else {
      document.getElementById('summary-text').textContent = "⚠️ Some rows have issues. Edit or override QBO duplicates.";
      document.getElementById('validation-summary').className = "mb-8 p-6 rounded-lg border bg-orange-50 border-orange-300";
      document.getElementById('start-real-import-btn').textContent = "Import Safe Rows Only";
      document.getElementById('start-real-import-btn').className = "px-8 py-4 bg-orange-600 text-white text-lg rounded hover:bg-orange-700";
    }

    document.getElementById('start-real-import-btn').disabled = !effectiveZeroFail;
  }


function handleCellEdit(inputElement, rowIndex, header) {
  const newValue = inputElement.value.trim();
  const oldValue = (previewRows[rowIndex][header] || '').trim();

  if (newValue !== oldValue) {
    previewRows[rowIndex][header] = newValue;
    hasUnvalidatedChanges = true;
    updateDryRunButton();

    // Immediately disable import until re-validated
    document.getElementById('start-real-import-btn').disabled = true;
  }
}

function renderPreviewTableWithValidation(validationRows) {
  lastDryRunRows = validationRows;
  currentValidation = {};
  validationRows.forEach(r => {
    currentValidation[r.row_number] = {
      status: r.status,
      issues: r.issues || []
    };
  });

  qboDuplicateOnlyRows = 0;
  validationRows.forEach(r => {
    if (r.status === "error") {
      const hasNonQboDup = r.issues.some(iss => iss.code !== "qbo_duplicate_displayname");
      const hasQboDup = r.issues.some(iss => iss.code === "qbo_duplicate_displayname");
      if (!hasNonQboDup && hasQboDup) qboDuplicateOnlyRows++;
    }
  });

  hasUnvalidatedChanges = false;
  updateDryRunButton();
  updateSummaryDisplay();

  const thead = document.querySelector('#preview-table thead');
  const tbody = document.querySelector('#preview-table tbody');

  thead.innerHTML = `
    <tr>
      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">#</th>
      <th class="px-6 py-3 text-left text-xs font-medium text-red-700 uppercase">Issues</th>
      ${csvHeaders.map(h => `<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">${h}</th>`).join('')}
    </tr>`;

  let html = '';

  previewRows.forEach((row, idx) => {
    const rowNum = idx + 2;
    const val = currentValidation[rowNum] || { status: "valid", issues: [] };

    let filteredIssues = val.issues;
    if (overrideDuplicates) {
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
      <td class="px-6 py-4 whitespace-nowrap text-sm font-bold ${effectiveStatus === 'valid' ? 'text-green-800' : 'text-red-800'}">
        ${idx + 1}
        ${effectiveStatus === 'valid' ? '<span class="ml-4 text-green-600 text-lg">✓</span>' : '<span class="ml-4 text-red-600 text-lg">✗</span>'}
      </td>
      <td class="px-6 py-4 text-sm align-top max-w-xs">
        ${issuesDisplay}
      </td>
      ${csvHeaders.map(header => {
        const mappedPath = mapping[header] || '';
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

        return `<td class="px-6 py-4 ${cellBg}">
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
async function startDryRun() {
    if (!mapping || Object.keys(mapping).length === 0) {
      alert("Please complete mapping first");
      return;
    }

    const payload = {
      mapping: mapping,
      rows: previewRows
    };

    try {
      const res = await fetchWithAuth(`/api/import/customer/${currentJobId}/dry-run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!res.ok) throw new Error(await res.text() || "Dry run failed");

      const data = await res.json();
      const s = data.summary;

      lastDryRunSummary = s;
      lastDryRunRows = data.rows;

      document.getElementById('validation-summary').classList.remove('hidden');
      document.getElementById('dry-run-btn').textContent = "Re-run Dry Run";
      document.getElementById('start-real-import-btn').classList.remove('hidden');

      // Fixed counts (unchanged by override)
      document.getElementById('total-count').textContent = s.total_rows;
      document.getElementById('warning-count').textContent = s.warnings;

      // Initial override state
      overrideDuplicates = document.getElementById('override-duplicates').checked;

      renderPreviewTableWithValidation(data.rows);

      // Live update on checkbox toggle
      document.getElementById('override-duplicates').onchange = () => {
        overrideDuplicates = document.getElementById('override-duplicates').checked;
        if (lastDryRunRows) {
          renderPreviewTableWithValidation(lastDryRunRows);
        }
      };

    } catch (err) {
      alert("Dry run failed: " + err.message);
    }
  }
  async function startRealImport() {
    if (!confirm("This will import data into QuickBooks. Continue?")) return;

    const payload = {
      mapping: mapping,
      rows: previewRows,
      override_existing: document.getElementById('override-duplicates').checked
    };

    try {
      const res = await fetchWithAuth(`/api/import/customer/${currentJobId}/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!res.ok) throw new Error(await res.text());

      alert("Import started successfully! Check 'Recent Jobs' for progress.");
      cancelPreview();
      loadJobs();
    } catch (err) {
      alert("Import failed to start: " + err.message);
    }
  }

  // === CLERK INIT ===
  window.addEventListener('load', async () => {
    if (typeof Clerk === 'undefined') return alert("Clerk failed to load");

    await Clerk.load();

    const mountDiv = document.getElementById('clerk-mount');
    if (Clerk.user) {
      Clerk.mountUserButton(mountDiv);
    } else {
      const btn = document.createElement('button');
      btn.textContent = 'Sign In';
      btn.className = 'px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 text-sm';
      btn.onclick = () => Clerk.openSignIn();
      mountDiv.appendChild(btn);
    } 

    updateQBOStatus();
    loadJobs();
    setInterval(updateQBOStatus, 30000);
    setInterval(loadJobs, 5000);
  });