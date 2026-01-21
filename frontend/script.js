const APP_STATE = {
  currentSection: 'dashboard',
  currentSubSection: null,
  isAuthenticated: false,
  qboConnected: false,
  currentCustomer: null,
  currentProduct: null,
  customers: [],
  products: [],
  jobs: [],
  reports: {},
  // Existing import state (from original code)
  currentJobId: null,
  hasUnvalidatedChanges: false,
  csvHeaders: [],
  previewRows: [],
  mapping: {},
  currentValidation: {},
  overrideDuplicates: false,
  qboDuplicateOnlyRows: 0,
  lastDryRunSummary: null,
  lastDryRunRows: null
};

// QBO Fields (from original code, expanded)
const AI_FIELDS = QBO_FIELDS;
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

// Product fields
const PRODUCT_FIELDS = [
  { value: "Name", label: "Product/Service Name *" },
  { value: "Description", label: "Description" },
  { value: "UnitPrice", label: "Unit Price" },
  { value: "IncomeAccountRef", label: "Income Account" },
  { value: "ExpenseAccountRef", label: "Expense Account" },
  { value: "Type", label: "Type (Inventory/Service)" },
  { value: "PurchaseCost", label: "Purchase Cost" },
  { value: "QtyOnHand", label: "Quantity On Hand" },
  { value: "Taxable", label: "Taxable" },
  { value: "Active", label: "Active" }
];

// ============================================
// UTILITY FUNCTIONS
// ============================================

// Toast notification system
function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  const toastId = 'toast-' + Date.now();
  
  const colors = {
    success: 'bg-green-500',
    error: 'bg-red-500',
    warning: 'bg-yellow-500',
    info: 'bg-blue-500'
  };
  
  const toast = document.createElement('div');
  toast.id = toastId;
  toast.className = `toast ${colors[type]} text-white p-4 rounded-lg shadow-lg max-w-sm`;
  toast.innerHTML = `
    <div class="flex items-center justify-between">
      <div class="flex items-center">
        <i class="fas ${type === 'success' ? 'fa-check-circle' : type === 'error' ? 'fa-exclamation-circle' : type === 'warning' ? 'fa-exclamation-triangle' : 'fa-info-circle'} mr-3"></i>
        <span>${message}</span>
      </div>
      <button onclick="document.getElementById('${toastId}').remove()" class="ml-4 text-white hover:text-gray-200">
        <i class="fas fa-times"></i>
      </button>
    </div>
  `;
  
  container.appendChild(toast);
  
  // Show toast
  setTimeout(() => {
    toast.classList.add('show');
  }, 10);
  
  // Auto-remove after 5 seconds
  setTimeout(() => {
    if (toast.parentNode) {
      toast.classList.remove('show');
      setTimeout(() => {
        if (toast.parentNode) toast.remove();
      }, 300);
    }
  }, 5000);
}

// Show loading spinner
function showLoading() {
  const container = document.getElementById('content-container');
  container.innerHTML = `
    <div class="flex justify-center items-center h-64">
      <div class="spinner"></div>
    </div>
  `;
}

// Fetch with authentication (from original code, enhanced)
async function fetchWithAuth(url, options = {}) {
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

// Update QBO connection status
async function updateQBOStatus() {
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

// ============================================
// NAVIGATION & ROUTING
// ============================================

// Load section content
async function loadSection(section, subSection = null) {
  APP_STATE.currentSection = section;
  APP_STATE.currentSubSection = subSection;
  
  // Update active navigation
  document.querySelectorAll('.nav-link').forEach(link => {
    link.classList.remove('text-blue-600', 'font-bold');
    if (link.dataset.section === section) {
      link.classList.add('text-blue-600', 'font-bold');
    }
  });
  
  // Update sidebar
  updateSidebar(section, subSection);
  
  // Load content
  showLoading();
  
  try {
    switch(section) {
      case 'dashboard':
        await loadDashboard();
        break;
      case 'customers':
        await loadCustomersSection(subSection);
        break;
      case 'products':
        await loadProductsSection(subSection);
        break;
      case 'reports':
        await loadReportsSection(subSection);
        break;
      case 'jobs':
        await loadJobsSection();
        break;
      default:
        await loadDashboard();
    }
  } catch (error) {
    console.error(`Error loading ${section}:`, error);
    document.getElementById('content-container').innerHTML = `
      <div class="bg-red-50 border-l-4 border-red-500 p-6 rounded">
        <div class="flex items-center">
          <div class="text-red-500 text-2xl mr-3">
            <i class="fas fa-exclamation-triangle"></i>
          </div>
          <div>
            <h3 class="text-lg font-medium text-red-800">Error Loading Content</h3>
            <p class="text-red-700 mt-1">${error.message || 'Please try again later.'}</p>
          </div>
        </div>
      </div>
    `;
  }
}

// Update sidebar navigation
function updateSidebar(section, subSection) {
  const sidebarNav = document.getElementById('sidebar-nav');
  let sidebarHTML = '';
  
  switch(section) {
    case 'dashboard':
      sidebarHTML = `
        <h3 class="font-bold text-gray-700 mb-4">Dashboard</h3>
        <a href="#" data-subsection="overview" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'overview' || !subSection ? 'active' : ''}">
          <i class="fas fa-tachometer-alt mr-3"></i>Overview
        </a>
        <a href="#" data-subsection="quick-actions" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'quick-actions' ? 'active' : ''}">
          <i class="fas fa-bolt mr-3"></i>Quick Actions
        </a>
      `;
      break;
      
    case 'customers':
      sidebarHTML = `
        <h3 class="font-bold text-gray-700 mb-4">Customers</h3>
        <a href="#" data-subsection="list" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'list' || !subSection ? 'active' : ''}">
          <i class="fas fa-list mr-3"></i>Customer List
        </a>
        <a href="#" data-subsection="create" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'create' ? 'active' : ''}">
          <i class="fas fa-plus-circle mr-3"></i>Add Customer
        </a>
        <a href="#" data-subsection="import" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'import' ? 'active' : ''}">
          <i class="fas fa-file-import mr-3"></i>Import CSV
        </a>
      `;
      break;
      
    case 'products':
      sidebarHTML = `
        <h3 class="font-bold text-gray-700 mb-4">Products & Services</h3>
        <a href="#" data-subsection="list" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'list' || !subSection ? 'active' : ''}">
          <i class="fas fa-boxes mr-3"></i>Product List
        </a>
        <a href="#" data-subsection="create" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'create' ? 'active' : ''}">
          <i class="fas fa-plus-circle mr-3"></i>Add Product
        </a>
      `;
      break;
      
    case 'reports':
      sidebarHTML = `
        <h3 class="font-bold text-gray-700 mb-4">Reports</h3>
        <a href="#" data-subsection="customers" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'customers' || !subSection ? 'active' : ''}">
          <i class="fas fa-users mr-3"></i>Customer Report
        </a>
        <a href="#" data-subsection="sales" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'sales' ? 'active' : ''}">
          <i class="fas fa-chart-line mr-3"></i>Sales Report
        </a>
        <a href="#" data-subsection="profit-loss" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'profit-loss' ? 'active' : ''}">
          <i class="fas fa-money-bill-wave mr-3"></i>Profit & Loss
        </a>
      `;
      break;
      
    case 'jobs':
      sidebarHTML = `
        <h3 class="font-bold text-gray-700 mb-4">Import Jobs</h3>
        <a href="#" data-subsection="recent" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'recent' || !subSection ? 'active' : ''}">
          <i class="fas fa-history mr-3"></i>Recent Jobs
        </a>
        <a href="#" data-subsection="logs" class="block sidebar-link py-3 px-4 rounded hover:bg-blue-50 ${subSection === 'logs' ? 'active' : ''}">
          <i class="fas fa-clipboard-list mr-3"></i>Import Logs
        </a>
      `;
      break;
  }
  
  sidebarNav.innerHTML = sidebarHTML;
  
  // Add event listeners to sidebar links
  sidebarNav.querySelectorAll('a').forEach(link => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      const subsection = link.dataset.subsection;
      loadSection(section, subsection);
    });
  });
}

// ============================================
// SECTION LOADERS
// ============================================

// Load Dashboard
async function loadDashboard() {
  try {
    // Fetch dashboard data
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
    }
    
    if (jobsRes.status === 'fulfilled' && jobsRes.value.ok) {
      jobs = await jobsRes.value.json();
    }
    
    if (reportsRes.status === 'fulfilled' && reportsRes.value.ok) {
      salesData = await reportsRes.value.json();
    }
    
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
    
    document.getElementById('content-container').innerHTML = html;
  } catch (error) {
    console.error('Error loading dashboard:', error);
    document.getElementById('content-container').innerHTML = `
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
  }
}

// Load Customers Section
async function loadCustomersSection(subSection) {
  try {
    let html = '';
    
    if (subSection === 'list' || !subSection) {
      // Load customer list
      const res = await fetchWithAuth('/api/customers');
      if (res.ok) {
        APP_STATE.customers = await res.json();
      }
      
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
                  <option value="active">Active Only</option>
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
            ${APP_STATE.customers.length > 0 ? `
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
                    ${APP_STATE.customers.map(customer => `
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
            ` : `
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
            `}
          </div>
        </div>
      `;
    } else if (subSection === 'create') {
      // Create customer form
      html = `
        <div class="fade-in max-w-4xl">
          <div class="flex items-center mb-8">
            <button onclick="loadSection('customers', 'list')" class="text-blue-600 hover:text-blue-800 mr-4">
              <i class="fas fa-arrow-left"></i>
            </button>
            <h2 class="text-2xl font-bold text-gray-800">Add New Customer</h2>
          </div>
          
          <div class="bg-white rounded-xl shadow p-8">
            <form id="customer-form" onsubmit="saveCustomer(event)">
              <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Display Name *</label>
                  <input type="text" name="DisplayName" required class="w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Company Name</label>
                  <input type="text" name="CompanyName" class="w-full px-4 py-2 border rounded-lg">
                </div>
                
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Primary Email</label>
                  <input type="email" name="PrimaryEmailAddr.Address" class="w-full px-4 py-2 border rounded-lg">
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Primary Phone</label>
                  <input type="text" name="PrimaryPhone.FreeFormNumber" class="w-full px-4 py-2 border rounded-lg">
                </div>
                
                <div class="md:col-span-2">
                  <label class="block text-sm font-medium text-gray-700 mb-2">Billing Address</label>
                  <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <input type="text" name="BillAddr.Line1" placeholder="Street Address" class="px-4 py-2 border rounded-lg">
                    <input type="text" name="BillAddr.City" placeholder="City" class="px-4 py-2 border rounded-lg">
                    <input type="text" name="BillAddr.PostalCode" placeholder="ZIP Code" class="px-4 py-2 border rounded-lg">
                  </div>
                </div>
                
                <div class="md:col-span-2">
                  <div class="flex items-center space-x-6">
                    <label class="flex items-center">
                      <input type="checkbox" name="Taxable" class="mr-2">
                      <span class="text-sm text-gray-700">Taxable</span>
                    </label>
                    <label class="flex items-center">
                      <input type="checkbox" name="Active" checked class="mr-2">
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
                  Save Customer
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
    
    document.getElementById('content-container').innerHTML = html;
    
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

// Load Products Section
async function loadProductsSection(subSection) {
  try {
    let html = '';
    
    if (subSection === 'list' || !subSection) {
      // Load product list
      const res = await fetchWithAuth('/api/products');
      if (res.ok) {
        APP_STATE.products = await res.json();
      }
      
      html = `
        <div class="fade-in">
          <div class="flex justify-between items-center mb-8">
            <h2 class="text-2xl font-bold text-gray-800">Products & Services</h2>
            <button onclick="loadSection('products', 'create')" class="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
              <i class="fas fa-plus mr-2"></i>Add Product
            </button>
          </div>
          
          <!-- Products Table -->
          <div class="bg-white rounded-xl shadow overflow-hidden">
            ${APP_STATE.products.length > 0 ? `
              <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-gray-200">
                  <thead class="bg-gray-50">
                    <tr>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Description</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Price</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                    </tr>
                  </thead>
                  <tbody class="bg-white divide-y divide-gray-200">
                    ${APP_STATE.products.map(product => `
                      <tr class="hover:bg-gray-50">
                        <td class="px-6 py-4 whitespace-nowrap">
                          <div class="font-medium text-gray-900">${product.Name || 'Unnamed'}</div>
                        </td>
                        <td class="px-6 py-4">
                          <div class="text-gray-900 max-w-xs truncate">${product.Description || '—'}</div>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap">
                          <div class="text-gray-900">${product.Type || '—'}</div>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap">
                          <div class="text-gray-900">$${product.UnitPrice?.toFixed(2) || '0.00'}</div>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap">
                          <span class="px-3 py-1 text-xs rounded-full ${product.Active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'}">
                            ${product.Active ? 'Active' : 'Inactive'}
                          </span>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                          <button onclick="editProduct('${product.Id}')" class="text-blue-600 hover:text-blue-900 mr-4">
                            <i class="fas fa-edit"></i>
                          </button>
                          <button onclick="deleteProduct('${product.Id}', '${product.Name || 'Product'}')" class="text-red-600 hover:text-red-900">
                            <i class="fas fa-trash"></i>
                          </button>
                        </td>
                      </tr>
                    `).join('')}
                  </tbody>
                </table>
              </div>
            ` : `
              <div class="text-center py-12">
                <div class="text-gray-400 text-5xl mb-4">
                  <i class="fas fa-boxes"></i>
                </div>
                <h3 class="text-lg font-medium text-gray-900 mb-2">No Products Found</h3>
                <p class="text-gray-500 mb-6">Get started by adding your first product or service.</p>
                <button onclick="loadSection('products', 'create')" class="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
                  Add Product
                </button>
              </div>
            `}
          </div>
        </div>
      `;
    } else if (subSection === 'create') {
      // Create product form
      html = `
        <div class="fade-in max-w-4xl">
          <div class="flex items-center mb-8">
            <button onclick="loadSection('products', 'list')" class="text-blue-600 hover:text-blue-800 mr-4">
              <i class="fas fa-arrow-left"></i>
            </button>
            <h2 class="text-2xl font-bold text-gray-800">Add New Product/Service</h2>
          </div>
          
          <div class="bg-white rounded-xl shadow p-8">
            <form id="product-form" onsubmit="saveProduct(event)">
              <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Product/Service Name *</label>
                  <input type="text" name="Name" required class="w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Type</label>
                  <select name="Type" class="w-full px-4 py-2 border rounded-lg">
                    <option value="Service">Service</option>
                    <option value="Inventory">Inventory</option>
                    <option value="NonInventory">Non-Inventory</option>
                  </select>
                </div>
                
                <div class="md:col-span-2">
                  <label class="block text-sm font-medium text-gray-700 mb-2">Description</label>
                  <textarea name="Description" rows="3" class="w-full px-4 py-2 border rounded-lg"></textarea>
                </div>
                
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Unit Price ($)</label>
                  <input type="number" step="0.01" name="UnitPrice" class="w-full px-4 py-2 border rounded-lg">
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Purchase Cost ($)</label>
                  <input type="number" step="0.01" name="PurchaseCost" class="w-full px-4 py-2 border rounded-lg">
                </div>
                
                <div class="md:col-span-2">
                  <div class="flex items-center space-x-6">
                    <label class="flex items-center">
                      <input type="checkbox" name="Taxable" class="mr-2">
                      <span class="text-sm text-gray-700">Taxable</span>
                    </label>
                    <label class="flex items-center">
                      <input type="checkbox" name="Active" checked class="mr-2">
                      <span class="text-sm text-gray-700">Active</span>
                    </label>
                  </div>
                </div>
              </div>
              
              <div class="flex justify-end space-x-4">
                <button type="button" onclick="loadSection('products', 'list')" class="px-6 py-3 border border-gray-300 rounded-lg hover:bg-gray-50">
                  Cancel
                </button>
                <button type="submit" class="px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700">
                  Save Product
                </button>
              </div>
            </form>
          </div>
        </div>
      `;
    }
    
    document.getElementById('content-container').innerHTML = html;
  } catch (error) {
    console.error('Error loading products section:', error);
    showToast('Error loading products: ' + error.message, 'error');
  }
}

// Load Reports Section
async function loadReportsSection(subSection) {
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

// Load Jobs Section
async function loadJobsSection() {
  try {
    const res = await fetchWithAuth('/api/jobs');
    let jobs = [];
    if (res.ok) {
      jobs = await res.json();
    }
    
    const html = `
      <div class="fade-in">
        <div class="flex justify-between items-center mb-8">
          <h2 class="text-2xl font-bold text-gray-800">Import Jobs</h2>
          <div class="flex space-x-4">
            <button onclick="refreshJobs()" class="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50">
              <i class="fas fa-sync-alt mr-2"></i>Refresh
            </button>
          </div>
        </div>
        
        <!-- Jobs Summary -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <div class="bg-white p-6 rounded-xl shadow">
            <p class="text-gray-500 text-sm">Total Jobs</p>
            <p class="text-3xl font-bold">${jobs.length}</p>
          </div>
          <div class="bg-white p-6 rounded-xl shadow">
            <p class="text-gray-500 text-sm">Successful</p>
            <p class="text-3xl font-bold text-green-600">${jobs.filter(j => j.status === 'completed').length}</p>
          </div>
          <div class="bg-white p-6 rounded-xl shadow">
            <p class="text-gray-500 text-sm">Failed</p>
            <p class="text-3xl font-bold text-red-600">${jobs.filter(j => j.status === 'failed').length}</p>
          </div>
          <div class="bg-white p-6 rounded-xl shadow">
            <p class="text-gray-500 text-sm">In Progress</p>
            <p class="text-3xl font-bold text-blue-600">${jobs.filter(j => j.status === 'processing').length}</p>
          </div>
        </div>
        
        <!-- Jobs Table -->
        <div class="bg-white rounded-xl shadow overflow-hidden">
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Filename</th>
                  <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                  <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Date</th>
                  <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                  <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Rows</th>
                  <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                ${jobs.map(job => `
                  <tr class="hover:bg-gray-50">
                    <td class="px-6 py-4">
                      <div class="font-medium text-gray-900">${job.filename || 'Unknown file'}</div>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                      <div class="text-gray-900">${job.object_type || 'customer'}</div>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                      <div class="text-gray-900">${new Date(job.created_at).toLocaleDateString()}</div>
                      <div class="text-sm text-gray-500">${new Date(job.created_at).toLocaleTimeString()}</div>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                      <span class="px-3 py-1 text-xs rounded-full ${
                        job.status === 'completed' ? 'bg-green-100 text-green-800' : 
                        job.status === 'failed' ? 'bg-red-100 text-red-800' : 
                        job.status === 'processing' ? 'bg-blue-100 text-blue-800' : 
                        'bg-yellow-100 text-yellow-800'
                      }">
                        ${job.status}
                      </span>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                      <div class="text-gray-900">Total: ${job.total_rows || '?'}</div>
                      ${job.failed_rows !== undefined ? `<div class="text-sm text-red-600">Failed: ${job.failed_rows}</div>` : ''}
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                      <button onclick="viewJobDetails('${job.id}')" class="text-blue-600 hover:text-blue-900 mr-4">
                        <i class="fas fa-eye"></i>
                      </button>
                      ${job.status === 'completed' ? `
                        <button onclick="downloadJobResults('${job.id}')" class="text-green-600 hover:text-green-900">
                          <i class="fas fa-download"></i>
                        </button>
                      ` : ''}
                    </td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    `;
    
    document.getElementById('content-container').innerHTML = html;
  } catch (error) {
    console.error('Error loading jobs section:', error);
    showToast('Error loading jobs: ' + error.message, 'error');
  }
}

// ============================================
// EXISTING IMPORT FUNCTIONALITY (from original code)
// ============================================

// All original functions from the MVP are preserved below
// (Only modified to work with the new state management)

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

// QBO Authentication (from original code)
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
        await loadJobsSection();
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
        await loadJobsSection();
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

// Upload for preview (modified to use APP_STATE)
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

function renderMappingFields() {
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

function addCustomMapping() {
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

function proceedToPreview() {
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

function backToMapping() {
  document.getElementById('preview-section').style.display = 'none';
  document.getElementById('mapping-section').style.display = 'block';
}

function cancelPreview() {
  document.getElementById('preview-section').style.display = 'none';
  document.getElementById('mapping-section').style.display = 'none';
  document.getElementById('upload-section').style.display = 'block';
  document.getElementById('file').value = '';
  APP_STATE.currentJobId = null;
  APP_STATE.csvHeaders = [];
  APP_STATE.previewRows = [];
  APP_STATE.mapping = {};
}

function renderPreviewTableClean() {
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

function updateDryRunButton() {
  const btn = document.getElementById('dry-run-btn');
  if (APP_STATE.hasUnvalidatedChanges) {
    btn.textContent = "Validate Changes ⚡";
    btn.classList.add('animate-pulse', 'ring-4', 'ring-indigo-300', 'bg-indigo-700');
  } else {
    btn.textContent = APP_STATE.currentValidation ? "Re-run Dry Run" : "Run Dry Run Simulation";
    btn.classList.remove('animate-pulse', 'ring-4', 'ring-indigo-300', 'bg-indigo-700');
  }
}

function updateSummaryDisplay() {
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

function renderPreviewTableWithValidation(validationRows) {
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

async function startDryRun() {
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

async function startRealImport() {
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

// ============================================
// HELPER FUNCTIONS
// ============================================

// Initialize sales chart
function initSalesChart(monthlyData) {
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

// CRUD operations for customers
async function saveCustomer(event) {
  event.preventDefault();
  const form = event.target;
  const formData = new FormData(form);
  const data = Object.fromEntries(formData.entries());
  
  // Convert checkbox values
  data.Taxable = formData.has('Taxable');
  data.Active = formData.has('Active');
  
  try {
    const res = await fetchWithAuth('/api/customers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });
    
    if (res.ok) {
      showToast('Customer saved successfully!', 'success');
      loadSection('customers', 'list');
    } else {
      throw new Error('Failed to save customer');
    }
  } catch (error) {
    showToast('Error saving customer: ' + error.message, 'error');
  }
}

async function editCustomer(id) {
  // In a real app, you would fetch the customer details
  // and populate a form for editing
  showToast('Edit functionality would fetch customer details', 'info');
}

async function deleteCustomer(id, name) {
  if (confirm(`Are you sure you want to delete ${name}?`)) {
    try {
      const res = await fetchWithAuth(`/api/customers/${id}`, {
        method: 'DELETE'
      });
      
      if (res.ok) {
        showToast('Customer deleted successfully!', 'success');
        loadSection('customers', 'list');
      } else {
        throw new Error('Failed to delete customer');
      }
    } catch (error) {
      showToast('Error deleting customer: ' + error.message, 'error');
    }
  }
}

// CRUD operations for products
async function saveProduct(event) {
  event.preventDefault();
  const form = event.target;
  const formData = new FormData(form);
  const data = Object.fromEntries(formData.entries());
  
  // Convert checkbox values
  data.Taxable = formData.has('Taxable');
  data.Active = formData.has('Active');
  
  try {
    const res = await fetchWithAuth('/api/products', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });
    
    if (res.ok) {
      showToast('Product saved successfully!', 'success');
      loadSection('products', 'list');
    } else {
      throw new Error('Failed to save product');
    }
  } catch (error) {
    showToast('Error saving product: ' + error.message, 'error');
  }
}

async function editProduct(id) {
  showToast('Edit functionality would fetch product details', 'info');
}

async function deleteProduct(id, name) {
  if (confirm(`Are you sure you want to delete ${name}?`)) {
    try {
      const res = await fetchWithAuth(`/api/products/${id}`, {
        method: 'DELETE'
      });
      
      if (res.ok) {
        showToast('Product deleted successfully!', 'success');
        loadSection('products', 'list');
      } else {
        throw new Error('Failed to delete product');
      }
    } catch (error) {
      showToast('Error deleting product: ' + error.message, 'error');
    }
  }
}

// Export functions (placeholder)
function exportCustomers() {
  showToast('Export functionality would download customer data', 'info');
}

function exportReport(type) {
  showToast(`${type} report export would download data`, 'info');
}

function printReport() {
  window.print();
}

function refreshJobs() {
  loadJobsSection();
}

function viewJobDetails(id) {
  showToast(`Job details for ${id} would be displayed`, 'info');
}

function downloadJobResults(id) {
  showToast(`Downloading results for job ${id}`, 'info');
}

// ============================================
// INITIALIZATION
// ============================================

// Clerk initialization
window.addEventListener('load', async () => {
  if (typeof Clerk === 'undefined') {
    console.error("Clerk failed to load");
    return;
  }

  await Clerk.load();

  // Setup authentication state change listener
  Clerk.addListener(({ user }) => {
    APP_STATE.isAuthenticated = !!user;
    updateUIForAuthState();
  });

  // Initial UI update
  updateUIForAuthState();
  
  // Mobile menu toggle
  document.getElementById('mobile-menu-btn').addEventListener('click', () => {
    const menu = document.getElementById('mobile-menu');
    menu.classList.toggle('hidden');
  });
});

// Update UI based on authentication state
function updateUIForAuthState() {
  const isAuthenticated = !!Clerk.user;
  
  if (isAuthenticated) {
    // User is logged in - show app
    document.getElementById('landing-page').classList.add('hidden');
    document.getElementById('app-container').classList.remove('hidden');
    document.getElementById('sidebar').classList.remove('hidden');
    
    // Initialize Clerk user button
    const mountDiv = document.getElementById('clerk-mount');
    mountDiv.innerHTML = '';
    Clerk.mountUserButton(mountDiv);
    
    // Load initial section
    loadSection('dashboard');
    
    // Update QBO status and load jobs
    updateQBOStatus();
    setInterval(updateQBOStatus, 30000);
    
  } else {
    // User is not logged in - show landing page
    document.getElementById('landing-page').classList.remove('hidden');
    document.getElementById('app-container').classList.add('hidden');
    
    // Setup landing page buttons
    document.getElementById('landing-signin-btn').onclick = () => Clerk.openSignIn();
    document.getElementById('landing-signup-btn').onclick = () => Clerk.openSignUp();
    document.getElementById('landing-get-started').onclick = () => Clerk.openSignUp();
  }
}

// Setup navigation links
document.addEventListener('click', (e) => {
  // Handle main navigation clicks
  if (e.target.matches('.nav-link')) {
    e.preventDefault();
    const section = e.target.dataset.section;
    loadSection(section);
    
    // Close mobile menu if open
    document.getElementById('mobile-menu').classList.add('hidden');
  }
  
  // Handle sidebar clicks (delegated)
  if (e.target.closest('.sidebar-link')) {
    e.preventDefault();
    const link = e.target.closest('.sidebar-link');
    const subsection = link.dataset.subsection;
    loadSection(APP_STATE.currentSection, subsection);
  }
});

// Initialize the app
window.addEventListener('DOMContentLoaded', () => {
  console.log('Quickbooks Lite initialized');
});