// frontend/utils.js

// ────────────────────────────────────────────────
// Authenticated fetch with Clerk token
// ────────────────────────────────────────────────
export async function authenticatedFetch(url, options = {}) {
  let token;
  try {
    if (window.Clerk && window.Clerk.session) {
      token = await window.Clerk.session.getToken();
    } else {
      throw new Error("Clerk session not available");
    }
  } catch (err) {
    console.error("Failed to get Clerk token:", err);
    throw new Error("Authentication failed – please sign in again");
  }

  if (!token) {
    throw new Error("No authentication token");
  }

  const headers = {
    ...options.headers,
    'Authorization': `Bearer ${token}`,
    'Content-Type': 'application/json',
  };

  console.log(`Sending authenticated request to: ${url}`); // debug

  return fetch(url, {
    ...options,
    headers,
    credentials: 'include',
  });
}

// ────────────────────────────────────────────────
// Toast (your original code – unchanged)
// ────────────────────────────────────────────────
export function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  if (!container) return;

  const toastId = 'toast-' + Date.now();
  const colors = { success: 'bg-green-500', error: 'bg-red-500', warning: 'bg-yellow-500', info: 'bg-blue-500' };
  const icons = { success: 'fa-check-circle', error: 'fa-exclamation-circle', warning: 'fa-exclamation-triangle', info: 'fa-info-circle' };

  const toast = document.createElement('div');
  toast.id = toastId;
  toast.className = `toast ${colors[type]} text-white p-4 rounded-lg shadow-lg max-w-sm fixed top-5 right-5 z-50 transform translate-x-full transition-transform duration-300`;

  toast.innerHTML = `
    <div class="flex items-center justify-between">
      <div class="flex items-center">
        <i class="fas ${icons[type]} mr-3"></i>
        <span>${message}</span>
      </div>
      <button onclick="document.getElementById('${toastId}').remove()" class="ml-4 text-white hover:text-gray-200">
        <i class="fas fa-times"></i>
      </button>
    </div>
  `;

  container.appendChild(toast);
  setTimeout(() => toast.classList.remove('translate-x-full'), 10);
  setTimeout(() => {
    toast.classList.add('translate-x-full');
    setTimeout(() => toast.remove(), 300);
  }, 5000);
}

// ────────────────────────────────────────────────
// Loading spinner (unchanged)
// ────────────────────────────────────────────────
export function showLoading() {
  const container = document.getElementById('content-container');
  if (container) {
    container.innerHTML = `
      <div class="flex justify-center items-center h-64">
        <div class="spinner"></div>
      </div>
    `;
  }
}