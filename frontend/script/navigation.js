import { APP_STATE } from './state.js';
import { showLoading, showError } from './utils.js';
import * as loaders from './loaders/index.js';  // Assume index.js exports all loaders

export function loadSection(section, subSection = null) {
  APP_STATE.currentSection = section;
  APP_STATE.currentSubSection = subSection;

  document.querySelectorAll('.nav-link').forEach(link => {
    link.classList.toggle('text-blue-600 font-bold', link.dataset.section === section);
  });

  updateSidebar(section, subSection);
  showLoading();

  const loaderMap = {
    dashboard: loaders.loadDashboard,
    customers: () => loaders.loadCustomersSection(subSection),
    products: () => loaders.loadProductsSection(subSection),
    reports: () => loaders.loadReportsSection(subSection),
    jobs: loaders.loadJobsSection
  };

  (loaderMap[section] || loaders.loadDashboard)()
    .catch(error => {
      console.error(`Error loading ${section}:`, error);
      showError('content-container', error.message || 'Please try again later.');
    });
}
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