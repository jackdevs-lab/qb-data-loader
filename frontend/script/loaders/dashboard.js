// script/loaders/dashboard.js
import { APP_STATE } from '../state.js';
import { fetchWithAuth, showToast } from '../utils.js';

export async function loadDashboard() {
  try {
    const [customersRes, jobsRes, reportsRes] = await Promise.allSettled([
      fetchWithAuth('/api/customers?limit=5'),
      fetchWithAuth('/api/jobs?limit=5'),
      fetchWithAuth('/api/reports/sales-summary')
    ]);

    let customers = [];
    let jobs = [];
    let salesData = { total: 0, monthly: [] };

    if (customersRes.status === 'fulfilled' && customersRes.value.ok) {
      customers = await customersRes.value.json();
      APP_STATE.customers = customers;
    }

    if (jobsRes.status === 'fulfilled' && jobsRes.value.ok) {
      jobs = await jobsRes.value.json();
      APP_STATE.jobs = jobs;
    }

    if (reportsRes.status === 'fulfilled' && reportsRes.value.ok) {
      salesData = await reportsRes.value.json();
    }

    document.getElementById('content-container').innerHTML = renderDashboard(customers, jobs, salesData);
  } catch (error) {
    console.error('Dashboard load error:', error);
    showToast('Failed to load dashboard: ' + error.message, 'error');
  }
}

function renderDashboard(customers, jobs, salesData) {
  return `
    <div class="fade-in">
      <div class="flex justify-between items-center mb-8">
        <h2 class="text-2xl font-bold text-gray-800">Dashboard</h2>
        <div class="flex space-x-4">
          <button onclick="import('/script/loaders/customers.js').then(m => m.loadCustomersSection('import'))"
                  class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
            <i class="fas fa-file-import mr-2"></i>Import Data
          </button>
          <button onclick="import('/script/loaders/customers.js').then(m => m.loadCustomersSection('create'))"
                  class="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
            <i class="fas fa-plus mr-2"></i>Add Customer
          </button>
        </div>
      </div>

      <!-- Stats Cards -->
      <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <div class="bg-white p-6 rounded-xl shadow">
          <div class="flex items-center">
            <div class="bg-blue-100 p-3 rounded-lg mr-4">
              <i class="fas fa-users text-blue-600 text-2xl"></i>
            </div>
            <div>
              <p class="text-gray-500 text-sm">Total Customers</p>
              <p class="text-3xl font-bold">${customers.length || '0'}</p>
            </div>
          </div>
        </div>

        <div class="bg-white p-6 rounded-xl shadow">
          <div class="flex items-center">
            <div class="bg-green-100 p-3 rounded-lg mr-4">
              <i class="fas fa-chart-line text-green-600 text-2xl"></i>
            </div>
            <div>
              <p class="text-gray-500 text-sm">Monthly Sales</p>
              <p class="text-3xl font-bold">$${salesData.total?.toLocaleString() || '0'}</p>
            </div>
          </div>
        </div>

        <div class="bg-white p-6 rounded-xl shadow">
          <div class="flex items-center">
            <div class="bg-purple-100 p-3 rounded-lg mr-4">
              <i class="fas fa-tasks text-purple-600 text-2xl"></i>
            </div>
            <div>
              <p class="text-gray-500 text-sm">Recent Jobs</p>
              <p class="text-3xl font-bold">${jobs.length || '0'}</p>
            </div>
          </div>
        </div>
      </div>

      <!-- Recent Customers + Recent Jobs (same as before) -->
      <!-- ... paste your original recent customers and recent jobs blocks here ... -->

      <!-- Quick Links -->
      <!-- ... paste your original quick links grid here ... -->
    </div>
  `;
}