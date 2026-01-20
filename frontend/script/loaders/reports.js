import { APP_STATE } from '../state.js';
import { fetchWithAuth, initSalesChart } from '../helpers.js';
import { showToast } from '../utils.js';

export async function loadReportsSection(subSection) {
  try {
    let html = '';
   
    if (subSection === 'customers' || !subSection) {
      // Customer Report
      const res = await fetchWithAuth('/api/reports/customers');
      let customers = [];
      if (res.ok) {
        customers = await res.json();
      }
     
      html = `
        <div class="fade-in">
          <div class="flex justify-between items-center mb-8">
            <h2 class="text-2xl font-bold text-gray-800">Customer Report</h2>
            <div class="flex space-x-4">
              <button onclick="printReport()" class="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50">
                <i class="fas fa-print mr-2"></i>Print
              </button>
              <button onclick="exportReport('customer')" class="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50">
                <i class="fas fa-download mr-2"></i>Export
              </button>
            </div>
          </div>
         
          <div class="bg-white rounded-xl shadow overflow-hidden mb-8">
            <div class="p-6 border-b">
              <h3 class="text-lg font-bold">Customer Summary</h3>
              <p class="text-gray-600">Total: ${customers.length} customers</p>
            </div>
           
            <div class="overflow-x-auto">
              <table class="min-w-full divide-y divide-gray-200">
                <thead class="bg-gray-50">
                  <tr>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Display Name</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Company</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Email</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Phone</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                  </tr>
                </thead>
                <tbody class="bg-white divide-y divide-gray-200">
                  ${customers.map(customer => `
                    <tr>
                      <td class="px-6 py-4 whitespace-nowrap">${customer.DisplayName || '—'}</td>
                      <td class="px-6 py-4 whitespace-nowrap">${customer.CompanyName || '—'}</td>
                      <td class="px-6 py-4 whitespace-nowrap">${customer.PrimaryEmailAddr?.Address || '—'}</td>
                      <td class="px-6 py-4 whitespace-nowrap">${customer.PrimaryPhone?.FreeFormNumber || '—'}</td>
                      <td class="px-6 py-4 whitespace-nowrap">
                        <span class="px-2 py-1 text-xs rounded-full ${customer.Active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'}">
                          ${customer.Active ? 'Active' : 'Inactive'}
                        </span>
                      </td>
                    </tr>
                  `).join('')}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      `;
    } else if (subSection === 'sales') {
      // Sales Report with chart
      const res = await fetchWithAuth('/api/reports/sales');
      let salesData = { summary: {}, monthly: [] };
      if (res.ok) {
        salesData = await res.json();
      }
     
      html = `
        <div class="fade-in">
          <div class="flex justify-between items-center mb-8">
            <h2 class="text-2xl font-bold text-gray-800">Sales Report</h2>
            <div class="flex space-x-4">
              <select id="sales-period" class="px-4 py-2 border rounded-lg">
                <option value="monthly">Last 30 Days</option>
                <option value="quarterly">Last Quarter</option>
                <option value="yearly">Last Year</option>
              </select>
              <button onclick="exportReport('sales')" class="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50">
                <i class="fas fa-download mr-2"></i>Export
              </button>
            </div>
          </div>
         
          <!-- Sales Summary Cards -->
          <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
            <div class="bg-white p-6 rounded-xl shadow">
              <p class="text-gray-500 text-sm">Total Sales</p>
              <p class="text-3xl font-bold text-green-600">$${salesData.summary?.totalSales?.toLocaleString() || '0'}</p>
            </div>
            <div class="bg-white p-6 rounded-xl shadow">
              <p class="text-gray-500 text-sm">Total Orders</p>
              <p class="text-3xl font-bold text-blue-600">${salesData.summary?.totalOrders || '0'}</p>
            </div>
            <div class="bg-white p-6 rounded-xl shadow">
              <p class="text-gray-500 text-sm">Average Order</p>
              <p class="text-3xl font-bold text-purple-600">$${salesData.summary?.averageOrder?.toFixed(2) || '0.00'}</p>
            </div>
            <div class="bg-white p-6 rounded-xl shadow">
              <p class="text-gray-500 text-sm">Top Customer</p>
              <p class="text-3xl font-bold text-orange-600">${salesData.summary?.topCustomer || '—'}</p>
            </div>
          </div>
         
          <!-- Chart Container -->
          <div class="bg-white rounded-xl shadow p-6 mb-8">
            <h3 class="text-lg font-bold mb-6">Sales Trend</h3>
            <div class="h-80">
              <canvas id="sales-chart"></canvas>
            </div>
          </div>
         
          <!-- Sales Data Table -->
          <div class="bg-white rounded-xl shadow overflow-hidden">
            <div class="p-6 border-b">
              <h3 class="text-lg font-bold">Monthly Sales Details</h3>
            </div>
            <div class="overflow-x-auto">
              <table class="min-w-full divide-y divide-gray-200">
                <thead class="bg-gray-50">
                  <tr>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Month</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Sales</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Orders</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Average Order</th>
                  </tr>
                </thead>
                <tbody class="bg-white divide-y divide-gray-200">
                  ${salesData.monthly.map(month => `
                    <tr>
                      <td class="px-6 py-4 whitespace-nowrap">${month.month}</td>
                      <td class="px-6 py-4 whitespace-nowrap">$${month.sales?.toLocaleString() || '0'}</td>
                      <td class="px-6 py-4 whitespace-nowrap">${month.orders || '0'}</td>
                      <td class="px-6 py-4 whitespace-nowrap">$${month.averageOrder?.toFixed(2) || '0.00'}</td>
                    </tr>
                  `).join('')}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      `;
    } else if (subSection === 'profit-loss') {
      // Profit & Loss Report
      const res = await fetchWithAuth('/api/reports/profit-loss');
      let reportData = { income: [], expenses: [], summary: {} };
      if (res.ok) {
        reportData = await res.json();
      }
     
      html = `
        <div class="fade-in">
          <div class="flex justify-between items-center mb-8">
            <h2 class="text-2xl font-bold text-gray-800">Profit & Loss Statement</h2>
            <div class="flex space-x-4">
              <select id="pl-period" class="px-4 py-2 border rounded-lg">
                <option value="current-month">Current Month</option>
                <option value="last-month">Last Month</option>
                <option value="quarter">Current Quarter</option>
                <option value="year">Current Year</option>
              </select>
              <button onclick="exportReport('profit-loss')" class="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50">
                <i class="fas fa-download mr-2"></i>Export
              </button>
            </div>
          </div>
         
          <!-- Summary Card -->
          <div class="bg-white rounded-xl shadow p-8 mb-8">
            <div class="grid grid-cols-1 md:grid-cols-3 gap-8">
              <div class="text-center">
                <p class="text-gray-500 mb-2">Total Income</p>
                <p class="text-4xl font-bold text-green-600">$${reportData.summary?.totalIncome?.toLocaleString() || '0'}</p>
              </div>
              <div class="text-center">
                <p class="text-gray-500 mb-2">Total Expenses</p>
                <p class="text-4xl font-bold text-red-600">$${reportData.summary?.totalExpenses?.toLocaleString() || '0'}</p>
              </div>
              <div class="text-center">
                <p class="text-gray-500 mb-2">Net Profit</p>
                <p class="text-4xl font-bold ${(reportData.summary?.netProfit || 0) >= 0 ? 'text-blue-600' : 'text-red-600'}">
                  $${reportData.summary?.netProfit?.toLocaleString() || '0'}
                </p>
              </div>
            </div>
          </div>
         
          <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <!-- Income -->
            <div class="bg-white rounded-xl shadow overflow-hidden">
              <div class="bg-green-50 p-6 border-b">
                <h3 class="text-lg font-bold text-green-800">Income</h3>
              </div>
              <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-gray-200">
                  <thead class="bg-gray-50">
                    <tr>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Account</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Amount</th>
                    </tr>
                  </thead>
                  <tbody class="bg-white divide-y divide-gray-200">
                    ${reportData.income.map(item => `
                      <tr>
                        <td class="px-6 py-4 whitespace-nowrap">${item.account}</td>
                        <td class="px-6 py-4 whitespace-nowrap text-green-600">$${item.amount?.toLocaleString() || '0'}</td>
                      </tr>
                    `).join('')}
                  </tbody>
                </table>
              </div>
            </div>
           
            <!-- Expenses -->
            <div class="bg-white rounded-xl shadow overflow-hidden">
              <div class="bg-red-50 p-6 border-b">
                <h3 class="text-lg font-bold text-red-800">Expenses</h3>
              </div>
              <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-gray-200">
                  <thead class="bg-gray-50">
                    <tr>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Account</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Amount</th>
                    </tr>
                  </thead>
                  <tbody class="bg-white divide-y divide-gray-200">
                    ${reportData.expenses.map(item => `
                      <tr>
                        <td class="px-6 py-4 whitespace-nowrap">${item.account}</td>
                        <td class="px-6 py-4 whitespace-nowrap text-red-600">$${item.amount?.toLocaleString() || '0'}</td>
                      </tr>
                    `).join('')}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      `;
    }
   
    document.getElementById('content-container').innerHTML = html;
   
    // Initialize chart for sales report
    if (subSection === 'sales' && window.Chart) {
      setTimeout(() => {
        initSalesChart(salesData.monthly);
      }, 100);
    }
  } catch (error) {
    console.error('Error loading reports section:', error);
    showToast('Error loading report: ' + error.message, 'error');
  }
}

export function exportReport(type) {
  showToast(`${type} report export would download data`, 'info');
}

export function printReport() {
  window.print();
}