import { APP_STATE } from '../state.js';
import { fetchWithAuth } from '../helpers.js';
import { showToast } from '../utils.js';

console.log("dashboard.js module loaded");

export async function loadDashboard() {
  console.log("Starting loadDashboard()");

  try {
    console.log("Initiating parallel fetches...");
    
    const promises = [
      fetchWithAuth('/api/customers?limit=5'),
      fetchWithAuth('/api/jobs?limit=5'),
      fetchWithAuth('/api/reports/sales-summary')
    ];

    const [customersRes, jobsRes, reportsRes] = await Promise.allSettled(promises);

    console.log("All fetches settled. Results status:");
    console.log("- Customers:", customersRes.status, customersRes.status === 'fulfilled' ? customersRes.value?.status : '—');
    console.log("- Jobs:     ", jobsRes.status,     jobsRes.status === 'fulfilled' ? jobsRes.value?.status : '—');
    console.log("- Reports:  ", reportsRes.status,  reportsRes.status === 'fulfilled' ? reportsRes.value?.status : '—');

    let customers = [];
    let jobs = [];
    let salesData = { total: 0, monthly: [] };

    // Process customers
    if (customersRes.status === 'fulfilled') {
      const res = customersRes.value;
      console.log("Customers response status:", res.status);
      if (res.ok) {
        try {
          customers = await res.json();
          console.log(`Loaded ${customers.length} customers`);
        } catch (jsonErr) {
          console.error("Failed to parse customers JSON:", jsonErr);
        }
      } else {
        try {
          const errorText = await res.text();
          console.warn("Customers endpoint error:", res.status, errorText);
        } catch {}
      }
    } else {
      console.error("Customers fetch rejected:", customersRes.reason);
    }

    // Process jobs (similar pattern)
    if (jobsRes.status === 'fulfilled') {
      const res = jobsRes.value;
      console.log("Jobs response status:", res.status);
      if (res.ok) {
        try {
          jobs = await res.json();
          console.log(`Loaded ${jobs.length} jobs`);
        } catch (jsonErr) {
          console.error("Failed to parse jobs JSON:", jsonErr);
        }
      } else {
        try {
          const errorText = await res.text();
          console.warn("Jobs endpoint error:", res.status, errorText);
        } catch {}
      }
    } else {
      console.error("Jobs fetch rejected:", jobsRes.reason);
    }

    // Process reports (similar pattern)
    if (reportsRes.status === 'fulfilled') {
      const res = reportsRes.value;
      console.log("Reports response status:", res.status);
      if (res.ok) {
        try {
          salesData = await res.json();
          console.log("Sales summary loaded:", salesData);
        } catch (jsonErr) {
          console.error("Failed to parse sales-summary JSON:", jsonErr);
        }
      } else {
        try {
          const errorText = await res.text();
          console.warn("Sales-summary endpoint error:", res.status, errorText);
        } catch {}
      }
    } else {
      console.error("Reports fetch rejected:", reportsRes.reason);
    }

    console.log("Data ready for rendering:");
    console.log(`- Customers count: ${customers.length}`);
    console.log(`- Jobs count: ${jobs.length}`);
    console.log(`- Sales total: ${salesData.total || 'N/A'}`);

    // Build dashboard HTML
    const html = `
      <div class="fade-in">
        <div class="flex justify-between items-center mb-8">
          <h2 class="text-2xl font-bold text-gray-800">Dashboard</h2>
          <div class="flex space-x-4">
            <button onclick="loadSection('customers', 'import')" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
              <i class="fas fa-file-import mr-2"></i>Import Data
            </button>
            <button onclick="loadSection('customers', 'create')" class="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
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
                <p class="text-3xl font-bold">$${salesData.total?.toLocaleString?.() || '0'}</p>
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

        <!-- Quick Stats & Charts -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <!-- Recent Customers -->
          <div class="bg-white rounded-xl shadow p-6">
            <h3 class="text-lg font-bold mb-4">Recent Customers</h3>
            ${customers.length > 0 ? `
              <div class="space-y-4">
                ${customers.slice(0, 5).map(customer => `
                  <div class="flex items-center justify-between border-b pb-3">
                    <div>
                      <p class="font-medium">${customer.DisplayName || 'Unnamed'}</p>
                      <p class="text-sm text-gray-500">${customer.PrimaryEmailAddr?.Address || 'No email'}</p>
                    </div>
                    <span class="px-3 py-1 text-xs rounded-full ${customer.Active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'}">
                      ${customer.Active ? 'Active' : 'Inactive'}
                    </span>
                  </div>
                `).join('')}
              </div>
              <div class="mt-4 text-center">
                <button onclick="loadSection('customers', 'list')" class="text-blue-600 hover:text-blue-800 font-medium">
                  View All Customers <i class="fas fa-arrow-right ml-1"></i>
                </button>
              </div>
            ` : '<p class="text-gray-500 text-center py-8">No customers found</p>'}
          </div>
         
          <!-- Recent Jobs -->
          <div class="bg-white rounded-xl shadow p-6">
            <h3 class="text-lg font-bold mb-4">Recent Import Jobs</h3>
            ${jobs.length > 0 ? `
              <div class="space-y-4">
                ${jobs.slice(0, 5).map(job => `
                  <div class="flex items-center justify-between border-b pb-3">
                    <div>
                      <p class="font-medium">${job.filename || 'Unknown file'}</p>
                      <p class="text-sm text-gray-500">${new Date(job.created_at).toLocaleDateString()}</p>
                    </div>
                    <span class="px-3 py-1 text-xs rounded-full ${job.status === 'completed' ? 'bg-green-100 text-green-800' : job.status === 'failed' ? 'bg-red-100 text-red-800' : 'bg-yellow-100 text-yellow-800'}">
                      ${job.status}
                    </span>
                  </div>
                `).join('')}
              </div>
              <div class="mt-4 text-center">
                <button onclick="loadSection('jobs')" class="text-blue-600 hover:text-blue-800 font-medium">
                  View All Jobs <i class="fas fa-arrow-right ml-1"></i>
                </button>
              </div>
            ` : '<p class="text-gray-500 text-center py-8">No jobs yet</p>'}
          </div>
        </div>
       
        <!-- Quick Links -->
        <div class="mt-8">
          <h3 class="text-lg font-bold mb-4">Quick Actions</h3>
          <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
            <button onclick="loadSection('customers', 'import')" class="bg-white p-6 rounded-xl shadow text-center hover:shadow-md transition-shadow">
              <div class="text-blue-600 text-3xl mb-3">
                <i class="fas fa-file-import"></i>
              </div>
              <p class="font-medium">Import CSV</p>
            </button>
           
            <button onclick="loadSection('reports', 'sales')" class="bg-white p-6 rounded-xl shadow text-center hover:shadow-md transition-shadow">
              <div class="text-green-600 text-3xl mb-3">
                <i class="fas fa-chart-line"></i>
              </div>
              <p class="font-medium">Sales Report</p>
            </button>
           
            <button onclick="loadSection('products', 'list')" class="bg-white p-6 rounded-xl shadow text-center hover:shadow-md transition-shadow">
              <div class="text-purple-600 text-3xl mb-3">
                <i class="fas fa-boxes"></i>
              </div>
              <p class="font-medium">Manage Products</p>
            </button>
           
            <button onclick="window.open('https://quickbooks.intuit.com', '_blank')" class="bg-white p-6 rounded-xl shadow text-center hover:shadow-md transition-shadow">
              <div class="text-red-600 text-3xl mb-3">
                <i class="fas fa-external-link-alt"></i>
              </div>
              <p class="font-medium">Go to QuickBooks</p>
            </button>
          </div>
        </div>
      </div>
    `;
   
    console.log("Dashboard HTML generated (length:", html.length, "chars)");
    const container = document.getElementById('content-container');
    if (!container) {
      console.error("CRITICAL: #content-container element not found in DOM");
      return;
    }
    
    console.log("Replacing #content-container content...");
    container.innerHTML = html;
    console.log("Dashboard content set successfully");

  } catch (error) {
    console.error('Error in loadDashboard():', error);
    const container = document.getElementById('content-container');
    if (container) {
      container.innerHTML = `
        <div class="bg-red-50 border-l-4 border-red-500 p-6 rounded">
          <div class="flex items-center">
            <div class="text-red-500 text-2xl mr-3">
              <i class="fas fa-exclamation-triangle"></i>
            </div>
            <div>
              <h3 class="text-lg font-medium text-red-800">Error Loading Dashboard</h3>
              <p class="text-red-700 mt-1">${error.message || 'Please try again later.'}</p>
            </div>
          </div>
        </div>
      `;
      console.log("Error message displayed in UI");
    } else {
      console.error("Cannot display error — #content-container missing");
    }
  }
}