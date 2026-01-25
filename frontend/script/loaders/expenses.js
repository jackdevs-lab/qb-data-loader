import { showLoading, showToast } from '../utils.js';
import { authenticatedFetch } from '../utils.js';

export async function loadExpensesSection(subSection = 'expenses-list') {
  console.log('🚀 loadExpensesSection called with subSection:', subSection);

  const content = document.getElementById('content-container');
  showLoading();

  try {
    if (subSection.includes('expense')) {
      if (subSection.includes('create')) {
        await renderExpenseCreateForm(content);
      } else {
        await renderExpensesList(content);
      }
    } else if (subSection.includes('bill')) {
      if (subSection.includes('create')) {
        await renderBillCreateForm(content);
      } else {
        await renderBillsList(content);
      }
    }
  } catch (error) {
    console.error('Expenses section error:', error);
    content.innerHTML = `
      <div class="bg-red-50 border border-red-200 p-8 rounded-lg text-center mt-8">
        <div class="text-red-600 text-5xl mb-4">⚠️</div>
        <h3 class="text-xl font-semibold text-red-800 mb-2">Failed to load</h3>
        <p class="text-red-700">${error.message || 'An unexpected error occurred'}</p>
      </div>
    `;
    showToast(error.message, 'error');
  }
}

async function renderExpensesList(content) {
  content.innerHTML = `
    <div class="flex justify-between items-center mb-6">
      <h2 class="text-2xl font-bold text-gray-800">Expenses & Bills</h2>
      <div class="space-x-3">
        <button id="btn-create-expense" class="px-5 py-2.5 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 transition">
          + Record Expense
        </button>
        <button id="btn-create-bill" class="px-5 py-2.5 bg-purple-600 text-white font-medium rounded-lg hover:bg-purple-700 transition">
          + Create Bill
        </button>
      </div>
    </div>
    <div class="overflow-x-auto bg-white rounded-lg shadow">
      <table class="min-w-full divide-y divide-gray-200">
        <thead class="bg-gray-50">
          <tr>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Payee / Vendor</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Amount</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Date</th>
          </tr>
        </thead>
        <tbody id="expenses-tbody" class="bg-white divide-y divide-gray-200"></tbody>
      </table>
    </div>
  `;

  try {
    const resp = await authenticatedFetch('/api/expenses?limit=10');
    if (resp.status === 401) throw new Error('Unauthorized – please reconnect to QuickBooks');
    if (!resp.ok) throw new Error(`Server error: ${resp.status}`);

    const data = await resp.json();
    const tbody = document.getElementById('expenses-tbody');

    const items = [
      ...(data.expenses || []).map(e => ({ ...e, type: 'Expense' })),
      ...(data.bills || []).map(b => ({ ...b, type: 'Bill' }))
    ];

    if (items.length === 0) {
      tbody.innerHTML = '<tr><td colspan="4" class="px-6 py-10 text-center text-gray-500">No records found</td></tr>';
    } else {
      tbody.innerHTML = items.map(item => `
        <tr class="hover:bg-gray-50 transition-colors">
          <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">${item.type}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${item.EntityRef?.name || item.VendorRef?.name || '—'}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">$${Number(item.TotalAmt || 0).toFixed(2)}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${item.TxnDate || '—'}</td>
        </tr>
      `).join('');
    }
  } catch (err) {
    showToast(err.message, 'error');
    document.getElementById('expenses-tbody').innerHTML = `
      <tr><td colspan="4" class="px-6 py-10 text-center text-red-600">${err.message}</td></tr>
    `;
  }

  setTimeout(() => {
    const expBtn = document.getElementById('btn-create-expense');
    const billBtn = document.getElementById('btn-create-bill');
    if (expBtn) expBtn.addEventListener('click', () => loadExpensesSection('expenses-create'));
    if (billBtn) billBtn.addEventListener('click', () => loadExpensesSection('bills-create'));
  }, 0);
}

async function renderExpenseCreateForm(content) {
  content.innerHTML = `<div class="p-10 text-center text-gray-600">Record Expense form – under construction</div>`;
  // You can paste your original renderExpenseCreate logic here when ready
}

async function renderBillsList(content) {
  content.innerHTML = `<div class="p-10 text-center text-gray-600">Bills list – under construction</div>`;
}

async function renderBillCreateForm(content) {
  content.innerHTML = `<div class="p-10 text-center text-gray-600">Create Bill form – under construction</div>`;
  // Paste your original renderBillCreate logic here when ready
}