export function showToast(message, type = 'info') {
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
export function showLoading() {
  const container = document.getElementById('content-container');
  container.innerHTML = `
    <div class="flex justify-center items-center h-64">
      <div class="spinner"></div>
    </div>
  `;
}
export function showError(containerId, message) {
  document.getElementById(containerId).innerHTML = `
    <div class="bg-red-50 border-l-4 border-red-500 p-6 rounded">
      <div class="flex items-center">
        <i class="fas fa-exclamation-triangle text-red-500 text-2xl mr-3"></i>
        <div>
          <h3 class="text-lg font-medium text-red-800">Error</h3>
          <p class="text-red-700 mt-1">${message}</p>
        </div>
      </div>
    </div>
  `;
}
export async function fetchWithAuth(url, options = {}) {
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
export function debounce(fn, ms = 300) {
  let timeout;
  return (...args) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => fn(...args), ms);
  };
}