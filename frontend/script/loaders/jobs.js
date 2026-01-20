import { APP_STATE } from '../state.js';
import { fetchWithAuth, updateQBOStatus } from '../helpers.js';
import { showToast } from '../utils.js';

export async function loadJobsSection() {
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

export function refreshJobs() {
  loadJobsSection();
}

export function viewJobDetails(id) {
  showToast(`Job details for ${id} would be displayed`, 'info');
}

export function downloadJobResults(id) {
  showToast(`Downloading results for job ${id}`, 'info');
}

export async function startQBOAuth() {
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