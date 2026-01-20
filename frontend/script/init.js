
import { APP_STATE } from './state.js';
import { loadSection } from './navigation.js';
import { uploadForPreview, proceedToPreview, backToMapping, cancelPreview, startDryRun, startRealImport, updateDryRunButton } from './import.js'; // updateDryRunButton called indirectly
import { saveCustomer, editCustomer, deleteCustomer, exportCustomers } from './loaders/customers.js';
import { saveProduct, editProduct, deleteProduct } from './loaders/products.js';
import { exportReport, printReport } from './loaders/reports.js';
import { refreshJobs, viewJobDetails, downloadJobResults, startQBOAuth } from './loaders/jobs.js';
import { handleCellEdit } from './helpers.js';

// Clerk initialization
window.addEventListener('load', async () => {
  if (typeof Clerk === 'undefined') {
    console.error("Clerk failed to load");
    return;
  }

  await Clerk.load();

  // Wait until the navigation module (which defines updateUIForAuthState) is ready
  // Small delay is usually enough in practice – or use a better signal (see alternative below)
  window.addEventListener('navigation-ready', () => {
  Clerk.addListener(({ user }) => {
    APP_STATE.isAuthenticated = !!user;
    updateUIForAuthState();
  });

  // Also run once immediately
  updateUIForAuthState();
}, { once: true });;   // 400–800 ms is usually plenty; increase if still failing

  // Mobile menu toggle (can stay outside)
  document.getElementById('mobile-menu-btn').addEventListener('click', () => {
    document.getElementById('mobile-menu').classList.toggle('hidden');
  });
});
// Global event listeners
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
window.loadSection = loadSection;
window.uploadForPreview = uploadForPreview;
window.proceedToPreview = proceedToPreview;
window.backToMapping = backToMapping;
window.cancelPreview = cancelPreview;
window.startDryRun = startDryRun;
window.startRealImport = startRealImport;
window.updateDryRunButton = updateDryRunButton; // If needed elsewhere
window.saveCustomer = saveCustomer;
window.editCustomer = editCustomer;
window.deleteCustomer = deleteCustomer;
window.exportCustomers = exportCustomers;
window.saveProduct = saveProduct;
window.editProduct = editProduct;
window.deleteProduct = deleteProduct;
window.exportReport = exportReport;
window.printReport = printReport;
window.refreshJobs = refreshJobs;
window.viewJobDetails = viewJobDetails;
window.downloadJobResults = downloadJobResults;
window.startQBOAuth = startQBOAuth;
window.handleCellEdit = handleCellEdit;

console.log('QBO Solutions Superapp initialized');