import { APP_STATE } from './state.js';
import { showLoading, showToast } from './utils.js';
import { updateQBOStatus } from './helpers.js';
import * as loaders from './loaders/index.js';

export async function loadSection(section, subSection = null) {
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
        await loaders.loadDashboard();
        break;
      case 'customers':
        await loaders.loadCustomersSection(subSection);
        break;
      case 'products':
        await loaders.loadProductsSection(subSection);
        break;
      case 'reports':
        await loaders.loadReportsSection(subSection);
        break;
      case 'jobs':
        await loaders.loadJobsSection();
        break;
      default:
        await loaders.loadDashboard();
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

export function updateSidebar(section, subSection) {
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

export function updateUIForAuthState() {
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
window.dispatchEvent(new CustomEvent('navigation-ready'));