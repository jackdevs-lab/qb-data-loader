// script/loaders/customers.js
import { APP_STATE } from '../state.js';
import { fetchWithAuth, showToast } from '../utils.js';
import { saveCustomer, editCustomer, deleteCustomer } from '../helpers.js';

export async function loadCustomersSection(subSection) {
  try {
    let html = '';

    if (!subSection || subSection === 'list') {
      const res = await fetchWithAuth('/api/customers');
      if (res.ok) {
        APP_STATE.customers = await res.json();
      }
      html = renderCustomerList(APP_STATE.customers);
    } else if (subSection === 'create') {
      html = renderCustomerForm();
    } else if (subSection === 'import') {
      html = renderImportSection();
      // Reset import state when entering import view
      resetImportState();
    }

    document.getElementById('content-container').innerHTML = html;

    // Re-attach event listeners
    if (subSection === 'create') {
      document.getElementById('customer-form')?.addEventListener('submit', saveCustomer);
    } else if (subSection === 'import') {
      document.getElementById('file')?.addEventListener('change', uploadForPreview);
      // ... attach other import buttons ...
    }
  } catch (err) {
    console.error(err);
    showToast('Failed to load customers section', 'error');
  }
}

function renderCustomerList(customers) {
  if (!customers?.length) {
    return `
      <div class="text-center py-12">
        <div class="text-gray-400 text-5xl mb-4"><i class="fas fa-users"></i></div>
        <h3 class="text-lg font-medium text-gray-900 mb-2">No Customers Found</h3>
        <p class="text-gray-500 mb-6">Get started by adding or importing customers.</p>
        <div class="space-x-4">
          <button onclick="loadSection('customers', 'create')" class="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">Add Customer</button>
          <button onclick="loadSection('customers', 'import')" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">Import CSV</button>
        </div>
      </div>
    `;
  }

  return `
    <div class="fade-in">
      <!-- header, search, filters ... -->
      <div class="bg-white rounded-xl shadow overflow-hidden">
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Display Name</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Company</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Email</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Phone</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              ${customers.map(c => `
                <tr class="hover:bg-gray-50">
                  <td class="px-6 py-4 font-medium">${c.DisplayName || 'Unnamed'}</td>
                  <td>${c.CompanyName || '—'}</td>
                  <td>${c.PrimaryEmailAddr?.Address || '—'}</td>
                  <td>${c.PrimaryPhone?.FreeFormNumber || '—'}</td>
                  <td>
                    <span class="px-3 py-1 text-xs rounded-full ${c.Active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'}">
                      ${c.Active ? 'Active' : 'Inactive'}
                    </span>
                  </td>
                  <td>
                    <button onclick="editCustomer('${c.Id}')" class="text-blue-600 hover:text-blue-900 mr-4"><i class="fas fa-edit"></i></button>
                    <button onclick="deleteCustomer('${c.Id}', '${c.DisplayName || 'Customer'}')" class="text-red-600 hover:text-red-900"><i class="fas fa-trash"></i></button>
                  </td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  `;
}

function renderCustomerForm() {
  return `
    <div class="fade-in max-w-4xl">
      <div class="flex items-center mb-8">
        <button onclick="loadSection('customers', 'list')" class="text-blue-600 hover:text-blue-800 mr-4">
          <i class="fas fa-arrow-left"></i>
        </button>
        <h2 class="text-2xl font-bold text-gray-800">Add New Customer</h2>
      </div>

      <div class="bg-white rounded-xl shadow p-8">
        <form id="customer-form">
          <!-- your full form fields here - unchanged -->
          <div class="flex justify-end space-x-4">
            <button type="button" onclick="loadSection('customers', 'list')" class="px-6 py-3 border border-gray-300 rounded-lg hover:bg-gray-50">Cancel</button>
            <button type="submit" class="px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700">Save Customer</button>
          </div>
        </form>
      </div>
    </div>
  `;
}

function renderImportSection() {
  return `
    <div class="fade-in">
      <div id="upload-section">
        <!-- your full upload section HTML here -->
      </div>

      <div id="mapping-section" style="display:none">
        <!-- your mapping section HTML here -->
        <button id="ai-suggest-btn" class="px-6 py-3 bg-purple-600 text-white rounded-lg hover:bg-purple-700">
          <i class="fas fa-magic mr-2"></i>AI Suggest Mapping
        </button>
        <!-- ... rest of mapping buttons ... -->
      </div>

      <div id="preview-section" style="display:none">
        <!-- your preview section HTML here -->
      </div>
    </div>
  `;
}

export function resetImportState() {
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