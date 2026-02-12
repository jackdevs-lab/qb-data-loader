import { APP_STATE } from '../state.js';
import { fetchWithAuth } from '../helpers.js';
import { showToast } from '../utils.js';

let editingCustomerId = null;
let editingCustomerSyncToken = null;
let currentFilter = 'active';
let currentSearch = '';

function renderCustomerTable(customers) {
  if (customers.length === 0) {
    return `
      <div class="text-center py-12">
        <div class="text-gray-400 text-5xl mb-4">
          <i class="fas fa-users"></i>
        </div>
        <h3 class="text-lg font-medium text-gray-900 mb-2">No Customers Found</h3>
        <p class="text-gray-500 mb-6">Get started by adding your first customer or importing a CSV file.</p>
        <div class="space-x-4">
          <button onclick="loadSection('customers', 'create')" class="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
            Add Customer
          </button>
          <button onclick="loadSection('customers', 'import')" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
            Import CSV
          </button>
        </div>
      </div>
    `;
  } else {
    return `
      <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Display Name</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Company</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Email</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Phone</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            ${customers.map(customer => `
              <tr class="hover:bg-gray-50">
                <td class="px-6 py-4 whitespace-nowrap">
                  <div class="font-medium text-gray-900">${customer.DisplayName || 'Unnamed'}</div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <div class="text-gray-900">${customer.CompanyName || '—'}</div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <div class="text-gray-900">${customer.PrimaryEmailAddr?.Address || '—'}</div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <div class="text-gray-900">${customer.PrimaryPhone?.FreeFormNumber || '—'}</div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <span class="px-3 py-1 text-xs rounded-full ${customer.Active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'}">
                    ${customer.Active ? 'Active' : 'Inactive'}
                  </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                  <button onclick="editCustomer('${customer.Id}')" class="text-blue-600 hover:text-blue-900 mr-4">
                    <i class="fas fa-edit"></i>
                  </button>
                  <button onclick="deleteCustomer('${customer.Id}', '${customer.DisplayName || 'Customer'}')" class="text-red-600 hover:text-red-900">
                    <i class="fas fa-trash"></i>
                  </button>
                </td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    `;
  }
}

function updateCustomerTable() {
  const customers = APP_STATE.customers || [];
  const filtered = customers.filter(customer => {
    let matchesFilter = true;
    if (currentFilter === 'active') {
      matchesFilter = customer.Active;
    } else if (currentFilter === 'inactive') {
      matchesFilter = !customer.Active;
    }
    if (!matchesFilter) return false;

    if (currentSearch) {
      const searchLower = currentSearch.toLowerCase();
      const displayName = (customer.DisplayName || '').toLowerCase();
      const companyName = (customer.CompanyName || '').toLowerCase();
      return displayName.includes(searchLower) || companyName.includes(searchLower);
    }
    return true;
  });

  const tableContainer = document.getElementById('customers-table-container');
  if (tableContainer) {
    tableContainer.innerHTML = renderCustomerTable(filtered);
    renderPagination();
  }
}

function renderPagination() {
  const container = document.getElementById('pagination-container');
  if (!container) return;

  const { totalCount, limit, offset } = APP_STATE.customerPagination;
  const currentPage = Math.floor((offset - 1) / limit) + 1;
  const totalPages = Math.ceil(totalCount / limit) || 1;

  if (totalPages <= 1 && totalCount < limit) {
    container.innerHTML = '';
    return;
  }

  container.innerHTML = `
    <div class="flex items-center justify-between px-6 py-4 bg-gray-50 border-t">
      <div class="text-sm text-gray-700">
        Showing <span class="font-medium">${offset}</span> to <span class="font-medium">${Math.min(offset + limit - 1, totalCount)}</span> of <span class="font-medium">${totalCount}</span> results
      </div>
      <div class="flex space-x-2">
        <button onclick="changePage(-1)" ${offset === 1 ? 'disabled' : ''} 
                class="px-4 py-2 border rounded bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
          Previous
        </button>
        <button onclick="changePage(1)" ${offset + limit > totalCount ? 'disabled' : ''} 
                class="px-4 py-2 border rounded bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
          Next
        </button>
      </div>
    </div>
  `;
}

window.changePage = async (direction) => {
  const { limit, offset } = APP_STATE.customerPagination;
  const newOffset = Math.max(1, offset + (direction * limit));

  if (newOffset === offset) return;

  APP_STATE.customerPagination.offset = newOffset;
  APP_STATE.customers = []; // Clear current to trigger reload
  await loadCustomersSection('list');
};

export async function loadCustomersSection(subSection) {
  try {
    let html = '';

    if (subSection === 'list' || !subSection) {
      // Load customer list if not already loaded
      if (!APP_STATE.customers || APP_STATE.customers.length === 0) {
        const { limit, offset } = APP_STATE.customerPagination;
        const res = await fetchWithAuth(`/api/customers?limit=${limit}&offset=${offset}`);
        if (res.ok) {
          const data = await res.json();
          APP_STATE.customers = data.customers || [];
          APP_STATE.customerPagination.totalCount = data.totalCount || 0;
        }
      }

      const initialCustomers = (APP_STATE.customers || []).filter(c => c.Active);

      html = `
        <div class="fade-in">
          <div class="flex justify-between items-center mb-8">
            <h2 class="text-2xl font-bold text-gray-800">Customer List</h2>
            <div class="flex space-x-4">
              <button onclick="loadSection('customers', 'create')" class="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
                <i class="fas fa-plus mr-2"></i>Add Customer
              </button>
              <button onclick="loadSection('customers', 'import')" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
                <i class="fas fa-file-import mr-2"></i>Import CSV
              </button>
            </div>
          </div>
         
          <!-- Search and Filters -->
          <div class="bg-white rounded-xl shadow p-4 mb-6">
            <div class="flex flex-col md:flex-row md:items-center justify-between gap-4">
              <div class="flex-1">
                <div class="relative">
                  <input type="text" id="customer-search" placeholder="Search customers..." class="w-full pl-10 pr-4 py-2 border rounded-lg">
                  <i class="fas fa-search absolute left-3 top-3 text-gray-400"></i>
                </div>
              </div>
              <div class="flex space-x-4">
                <select id="customer-filter" class="px-4 py-2 border rounded-lg">
                  <option value="all">All Customers</option>
                  <option value="active" selected>Active Only</option>
                  <option value="inactive">Inactive Only</option>
                </select>
                <button onclick="exportCustomers()" class="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50">
                  <i class="fas fa-download mr-2"></i>Export
                </button>
              </div>
            </div>
          </div>
         
          <!-- Customers Table -->
          <div class="bg-white rounded-xl shadow overflow-hidden">
            <div id="customers-table-container">
              ${renderCustomerTable(initialCustomers)}
            </div>
            <div id="pagination-container">
              ${renderPagination() || ''}
            </div>
          </div>
        </div>
      `;

      document.getElementById('content-container').innerHTML = html;

      // Attach event listeners
      document.getElementById('customer-search').addEventListener('input', (e) => {
        currentSearch = e.target.value;
        updateCustomerTable();
      });

      document.getElementById('customer-filter').addEventListener('change', (e) => {
        currentFilter = e.target.value;
        updateCustomerTable();
      });

    } else if (subSection === 'create' || subSection === 'edit') {
      const isEditMode = subSection === 'edit';
      let customer = null;

      if (isEditMode) {
        const id = new URLSearchParams(window.location.search).get('id') || editingCustomerId;
        if (!id) {
          showToast('No customer ID provided for editing', 'error');
          loadSection('customers', 'list');
          return;
        }
        try {
          const res = await fetchWithAuth(`/api/customers/${id}`);
          if (!res.ok) throw new Error('Failed to fetch customer');
          customer = await res.json();
          editingCustomerId = customer.Id;
          editingCustomerSyncToken = customer.SyncToken;
        } catch (err) {
          showToast('Error loading customer: ' + err.message, 'error');
          loadSection('customers', 'list');
          return;
        }
      }

      const title = isEditMode ? 'Edit Customer' : 'Add New Customer';
      const buttonText = isEditMode ? 'Update Customer' : 'Save Customer';

      html = `
      <div class="fade-in max-w-4xl">
        <div class="flex items-center mb-8">
          <button onclick="loadSection('customers', 'list')" class="text-blue-600 hover:text-blue-800 mr-4">
            <i class="fas fa-arrow-left"></i>
          </button>
          <h2 class="text-2xl font-bold text-gray-800">${title}</h2>
        </div>
        
        <div class="bg-white rounded-xl shadow p-8">
          <form id="customer-form" onsubmit="saveCustomer(event)">
            <!-- Hidden fields for edit mode -->
            ${isEditMode ? `
              <input type="hidden" name="Id" value="${customer.Id}">
              <input type="hidden" name="SyncToken" value="${customer.SyncToken}">
            ` : ''}

            <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
              <div>
                <label class="block text-sm font-medium text-gray-700 mb-2">Display Name *</label>
                <input type="text" name="DisplayName" required class="w-full px-4 py-2 border rounded-lg" 
                       value="${customer?.DisplayName || ''}">
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 mb-2">Company Name</label>
                <input type="text" name="CompanyName" class="w-full px-4 py-2 border rounded-lg" 
                       value="${customer?.CompanyName || ''}">
              </div>
              
              <div>
                <label class="block text-sm font-medium text-gray-700 mb-2">Primary Email</label>
                <input type="email" name="PrimaryEmailAddr.Address" class="w-full px-4 py-2 border rounded-lg" 
                       value="${customer?.PrimaryEmailAddr?.Address || ''}">
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 mb-2">Primary Phone</label>
                <input type="text" name="PrimaryPhone.FreeFormNumber" class="w-full px-4 py-2 border rounded-lg" 
                       value="${customer?.PrimaryPhone?.FreeFormNumber || ''}">
              </div>

              <!-- Add more fields as needed (Billing Address, etc.) -->
              <div class="md:col-span-2">
                <label class="block text-sm font-medium text-gray-700 mb-2">Billing Address</label>
                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <input type="text" name="BillAddr.Line1" placeholder="Street Address" class="px-4 py-2 border rounded-lg" 
                         value="${customer?.BillAddr?.Line1 || ''}">
                  <input type="text" name="BillAddr.City" placeholder="City" class="px-4 py-2 border rounded-lg" 
                         value="${customer?.BillAddr?.City || ''}">
                  <input type="text" name="BillAddr.PostalCode" placeholder="ZIP Code" class="px-4 py-2 border rounded-lg" 
                         value="${customer?.BillAddr?.PostalCode || ''}">
                </div>
              </div>

              <div class="md:col-span-2">
                <div class="flex items-center space-x-6">
                  <label class="flex items-center">
                    <input type="checkbox" name="Taxable" class="mr-2" ${customer?.Taxable ? 'checked' : ''}>
                    <span class="text-sm text-gray-700">Taxable</span>
                  </label>
                  <label class="flex items-center">
                    <input type="checkbox" name="Active" class="mr-2" ${customer?.Active !== false ? 'checked' : ''}>
                    <span class="text-sm text-gray-700">Active</span>
                  </label>
                </div>
              </div>
            </div>
            
            <div class="flex justify-end space-x-4">
              <button type="button" onclick="loadSection('customers', 'list')" class="px-6 py-3 border border-gray-300 rounded-lg hover:bg-gray-50">
                Cancel
              </button>
              <button type="submit" class="px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700">
                ${buttonText}
              </button>
            </div>
          </form>
        </div>
      </div>
    `;

    } else if (subSection === 'import') {
      // Load the existing import functionality
      html = `
        <div class="fade-in">
          <!-- Upload Section -->
          <div id="upload-section">
            <div class="bg-white rounded-xl shadow p-8">
              <div class="flex items-center mb-6">
                <button onclick="loadSection('customers', 'list')" class="text-blue-600 hover:text-blue-800 mr-4">
                  <i class="fas fa-arrow-left"></i>
                </button>
                <h2 class="text-2xl font-semibold">Import Customers from CSV</h2>
              </div>
             
              <div class="bg-blue-50 border-l-4 border-blue-500 p-6 rounded mb-8">
                <div class="flex">
                  <div class="flex-shrink-0">
                    <i class="fas fa-info-circle text-blue-500 text-xl"></i>
                  </div>
                  <div class="ml-4">
                    <h3 class="text-lg font-medium text-blue-800">CSV Import Instructions</h3>
                    <p class="mt-2 text-blue-700">Upload a CSV file with customer data. The system will help you map columns to QuickBooks fields. Ensure your CSV includes a header row.</p>
                  </div>
                </div>
              </div>
             
              <input type="file" id="file" accept=".csv" class="block w-full text-sm text-gray-500 file:mr-4 file:py-3 file:px-4 file:rounded-lg file:border-0 file:bg-blue-600 file:text-white hover:file:bg-blue-700 mb-6">
             
              <div class="mb-6">
                <label class="block text-sm font-medium text-gray-700 mb-2">Import Type</label>
                <select id="object_type" class="w-full px-4 py-2 border rounded-lg">
                  <option value="customer">Customer</option>
                  <option value="invoice" disabled>Invoice (coming soon)</option>
                </select>
              </div>
             
              <button onclick="uploadForPreview()" class="px-8 py-4 bg-indigo-600 text-white text-lg rounded-lg hover:bg-indigo-700 w-full">
                <i class="fas fa-upload mr-2"></i>Upload & Preview Mapping
              </button>
            </div>
          </div>
          <!-- Mapping Section -->
          <div id="mapping-section" style="display:none" class="bg-white rounded-xl shadow p-8 mt-8">
            <h2 class="text-2xl font-semibold mb-6">Map Columns to QuickBooks Fields</h2>
            <p class="text-gray-600 mb-8">
              Map your CSV columns to QuickBooks fields. <span class="text-red-600 font-bold">DisplayName is required*</span>
            </p>
            <div id="mapping-fields" class="space-y-6 mb-10"></div>
            <div class="flex gap-4">
              <button id="start-import-btn" onclick="proceedToPreview()" disabled class="px-8 py-4 bg-green-600 text-white text-lg rounded-lg opacity-50 cursor-not-allowed">
                Proceed to Preview
              </button>
              <button onclick="cancelPreview()" class="px-6 py-4 bg-gray-400 text-white rounded-lg hover:bg-gray-500">
                Cancel
              </button>
            </div>
          </div>
          <!-- Preview Section -->
          <div id="preview-section" style="display:none" class="bg-white rounded-xl shadow p-8 mt-8">
            <h2 class="text-2xl font-semibold mb-6">Preview & Edit Data</h2>
            <p class="text-gray-600 mb-8">
              Review how your data will appear in QuickBooks. Edit any cell directly.
            </p>
            <!-- Validation Summary -->
            <div id="validation-summary" class="hidden mb-8 p-6 rounded-lg border">
              <p id="summary-text" class="text-lg font-medium"></p>
              <div class="mt-4 flex flex-wrap gap-8 text-sm items-center">
                <div><span class="font-bold text-green-700">Will Succeed:</span> <span id="success-count">0</span></div>
                <div><span class="font-bold text-red-700">Will Fail:</span> <span id="fail-count">0</span></div>
                <div><span class="font-bold text-yellow-700">Warnings:</span> <span id="warning-count">0</span></div>
                <div><span class="font-bold">Total:</span> <span id="total-count">0</span></div>
               
                <!-- Override Duplicates Checkbox -->
                <label class="flex items-center gap-2 cursor-pointer">
                  <input type="checkbox" id="override-duplicates" class="w-5 h-5 text-indigo-600 rounded focus:ring-indigo-500">
                  <span class="text-sm font-medium text-gray-700">Override existing customers in QuickBooks</span>
                </label>
              </div>
            </div>
            <div class="overflow-x-auto mb-8 border rounded-lg">
              <table id="preview-table" class="min-w-full divide-y divide-gray-200">
                <thead class="bg-gray-50"></thead>
                <tbody class="bg-white divide-y divide-gray-200"></tbody>
              </table>
            </div>
            <div id="preview-actions" class="flex flex-col md:flex-row gap-4 items-center">
              <button id="dry-run-btn" onclick="startDryRun()" class="px-8 py-4 bg-indigo-600 text-white text-lg rounded-lg hover:bg-indigo-700">
                Run Dry Run Simulation
              </button>
              <button onclick="backToMapping()" class="px-6 py-4 bg-gray-400 text-white rounded-lg hover:bg-gray-500">
                Back to Mapping
              </button>
              <button id="start-real-import-btn" onclick="startRealImport()"
                      class="px-8 py-4 bg-orange-600 text-white text-lg rounded-lg hover:bg-orange-700 hidden"
                      disabled>
                Import Anyway (Safe Rows Only)
              </button>
            </div>
          </div>
        </div>
      `;
    }

    if (subSection !== 'list' && subSection) {
      document.getElementById('content-container').innerHTML = html;
    }

    // If we're in import mode, we need to reinitialize the import functionality
    if (subSection === 'import') {
      // Reset import state
      APP_STATE.currentJobId = null;
      APP_STATE.hasUnvalidatedChanges = false;
      APP_STATE.csvHeaders = [];
      APP_STATE.previewRows = [];
      APP_STATE.mapping = {};
      APP_STATE.currentValidation = {};
      APP_STATE.overrideDuplicates = false;
      APP_STATE.qboDuplicateOnlyRows = 0;
      APP_STATE.lastDryRunSummary = null;
      APP_STATE.lastDryRunRows = null;
    }
  } catch (error) {
    console.error('Error loading customers section:', error);
    document.getElementById('content-container').innerHTML = `
      <div class="bg-red-50 border-l-4 border-red-500 p-6 rounded">
        <div class="flex items-center">
          <div class="text-red-500 text-2xl mr-3">
            <i class="fas fa-exclamation-triangle"></i>
          </div>
          <div>
            <h3 class="text-lg font-medium text-red-800">Error Loading Customers</h3>
            <p class="text-red-700 mt-1">${error.message || 'Please try again later.'}</p>
          </div>
        </div>
      </div>
    `;
  }
}

export async function editCustomer(id) {
  editingCustomerId = id;
  loadSection('customers', 'edit');
}

export async function saveCustomer(event) {
  event.preventDefault();
  const form = event.target;
  const formData = new FormData(form);
  const data = Object.fromEntries(formData.entries());

  // Convert checkboxes
  data.Taxable = formData.has('Taxable');
  data.Active = formData.has('Active');

  const isUpdate = !!data.Id;

  try {
    const url = isUpdate ? `/api/customers/${data.Id}` : '/api/customers';
    const method = isUpdate ? 'PUT' : 'POST';

    // For updates, remove Id/SyncToken from main payload – backend handles it
    if (isUpdate) {
      delete data.Id;
      delete data.SyncToken;
    }

    const res = await fetchWithAuth(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });

    if (res.ok) {
      showToast(isUpdate ? 'Customer updated successfully!' : 'Customer created successfully!', 'success');
      editingCustomerId = null;
      editingCustomerSyncToken = null;
      APP_STATE.customers = []; // Invalidate cache to refetch on next load
      loadSection('customers', 'list');
    } else {
      const err = await res.text();
      throw new Error(err || 'Failed to save customer');
    }
  } catch (error) {
    showToast('Error: ' + error.message, 'error');
  }
}

export async function deleteCustomer(id, name) {
  if (confirm(`Are you sure you want to delete ${name}?`)) {
    try {
      const res = await fetchWithAuth(`/api/customers/${id}`, {
        method: 'DELETE'
      });

      if (res.ok) {
        showToast('Customer deleted successfully!', 'success');
        APP_STATE.customers = []; // Invalidate cache to refetch on next load
        loadSection('customers', 'list');
      } else {
        throw new Error('Failed to delete customer');
      }
    } catch (error) {
      showToast('Error deleting customer: ' + error.message, 'error');
    }
  }
}

export function exportCustomers() {
  showToast('Export functionality would download customer data', 'info');
}