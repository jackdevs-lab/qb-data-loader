// sales.js (full updated version with forms, edit, and void)
import { showLoading, showToast } from '../utils.js';
import { authenticatedFetch } from '../utils.js';

export async function loadSalesSection(subSection = 'invoices-list') {
  console.log('🚀 loadSalesSection called with subSection:', subSection);

  const content = document.getElementById('content-container');
  showLoading();

  try {
    if (subSection.includes('invoice')) {
      if (subSection.includes('create')) {
        await renderInvoiceForm(content, 'create');
      } else {
        await renderInvoicesList(content);
      }
    } else if (subSection.includes('receipt')) {
      if (subSection.includes('create')) {
        await renderReceiptForm(content, 'create');
      } else {
        await renderReceiptsList(content);
      }
    }
  } catch (error) {
    console.error('Sales section error:', error);
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

async function fetchCustomers() {
  const resp = await authenticatedFetch('/api/customers?limit=50');
  if (!resp.ok) throw new Error('Failed to fetch customers');
  return await resp.json();
}

async function fetchItems() {
  const resp = await authenticatedFetch('/api/products?limit=50');
  if (!resp.ok) throw new Error('Failed to fetch items');
  return await resp.json();
}

async function renderInvoicesList(content) {
  content.innerHTML = `
    <div class="flex justify-between items-center mb-6">
      <h2 class="text-2xl font-bold text-gray-800">Recent Invoices</h2>
      <button id="btn-create-invoice" class="px-5 py-2.5 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 transition">
        + Create Invoice
      </button>
    </div>
    <div class="overflow-x-auto bg-white rounded-lg shadow">
      <table class="min-w-full divide-y divide-gray-200">
        <thead class="bg-gray-50">
          <tr>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Customer</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Total</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Balance</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Date</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
          </tr>
        </thead>
        <tbody id="invoices-tbody" class="bg-white divide-y divide-gray-200"></tbody>
      </table>
    </div>
  `;

  try {
    const resp = await authenticatedFetch('/api/invoices?limit=10');
    if (resp.status === 401) throw new Error('Unauthorized – please reconnect to QuickBooks');
    if (!resp.ok) throw new Error(`Server error: ${resp.status}`);

    const invoices = await resp.json();
    const tbody = document.getElementById('invoices-tbody');

    if (!invoices || invoices.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="px-6 py-10 text-center text-gray-500">No invoices found</td></tr>';
    } else {
      tbody.innerHTML = invoices.map(inv => `
        <tr class="hover:bg-gray-50 transition-colors">
          <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">${inv.Id}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${inv.CustomerRef?.name || '—'}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">$${Number(inv.TotalAmt || 0).toFixed(2)}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">$${Number(inv.Balance || 0).toFixed(2)}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${inv.TxnDate || '—'}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
            <button class="text-blue-600 hover:text-blue-900 mr-3 edit-btn" data-id="${inv.Id}">Edit</button>
            <button class="text-red-600 hover:text-red-900 void-btn" data-id="${inv.Id}">Void</button>
          </td>
        </tr>
      `).join('');
    }
  } catch (err) {
    showToast(err.message, 'error');
    document.getElementById('invoices-tbody').innerHTML = `
      <tr><td colspan="6" class="px-6 py-10 text-center text-red-600">${err.message}</td></tr>
    `;
  }

  // Event listeners for create, edit, void
  setTimeout(() => {
    const createBtn = document.getElementById('btn-create-invoice');
    if (createBtn) createBtn.addEventListener('click', () => loadSalesSection('invoices-create'));

    document.querySelectorAll('.edit-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        const id = btn.dataset.id;
        showLoading();
        try {
          const resp = await authenticatedFetch(`/api/invoices/${id}`);
          if (!resp.ok) throw new Error('Failed to load invoice');
          const data = await resp.json();
          await renderInvoiceForm(document.getElementById('content-container'), 'update', data);
        } catch (e) {
          showToast(e.message, 'error');
        }
      });
    });

    document.querySelectorAll('.void-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        if (!confirm('Void this invoice? It will set amounts to $0 and preserve history for reporting.')) return;
        const id = btn.dataset.id;
        try {
          const resp = await authenticatedFetch(`/api/invoices/${id}`, { method: 'DELETE' });
          if (!resp.ok) throw new Error('Void failed');
          showToast('Invoice voided successfully', 'success');
          await renderInvoicesList(document.getElementById('content-container'));
        } catch (e) {
          showToast(e.message, 'error');
        }
      });
    });
  }, 0);
}

async function renderInvoiceForm(content, mode = 'create', invoice = null) {
  try {
    const customers = await fetchCustomers();
    const items = await fetchItems();

    content.innerHTML = `
      <div class="max-w-4xl mx-auto p-6 bg-white rounded-lg shadow">
        <h2 class="text-2xl font-bold mb-6">${mode === 'create' ? 'Create Invoice' : 'Edit Invoice'}</h2>
        <form id="invoice-form">
          <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
            <div>
              <label class="block text-gray-700 font-medium mb-1">Customer *</label>
              <select id="customer" class="w-full p-2 border rounded" required>
                <option value="">Select Customer</option>
                ${customers.map(c => `<option value="${c.Id}">${c.DisplayName || c.name || 'Unnamed'}</option>`).join('')}
              </select>
            </div>
            <div>
              <label class="block text-gray-700 font-medium mb-1">Date</label>
              <input type="date" id="txnDate" class="w-full p-2 border rounded" value="${new Date().toISOString().split('T')[0]}" required>
            </div>
            <div>
              <label class="block text-gray-700 font-medium mb-1">Due Date</label>
              <input type="date" id="dueDate" class="w-full p-2 border rounded" value="${new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]}" required>
            </div>
          </div>

          <h3 class="text-lg font-semibold mb-3">Line Items</h3>
          <div class="mb-4 grid grid-cols-1 sm:grid-cols-5 gap-2">
            <select id="item" class="p-2 border rounded col-span-2">
              <option value="">Select Product/Service</option>
              ${items.map(i => `<option value="${i.Id}" data-rate="${i.SalesPrice || 0}" data-desc="${(i.Description || '').replace(/"/g, '&quot;')}">${i.Name}</option>`).join('')}
            </select>
            <input type="number" id="qty" value="1" min="1" class="p-2 border rounded w-20" placeholder="Qty">
            <input type="number" id="rate" step="0.01" class="p-2 border rounded" placeholder="Rate">
            <input type="text" id="desc" class="p-2 border rounded col-span-2" placeholder="Description">
            <button type="button" id="add-line" class="bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700">Add</button>
          </div>
          <div id="lines-container" class="mb-6"></div>

          <div class="flex justify-end mt-6">
            <button type="button" id="cancel-btn" class="px-6 py-2 bg-gray-300 text-gray-800 rounded mr-4 hover:bg-gray-400">Cancel</button>
            <button type="submit" class="px-6 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">Save Invoice</button>
          </div>
        </form>
      </div>
    `;

    // Populate if editing
    if (mode === 'update' && invoice) {
      document.getElementById('customer').value = invoice.CustomerRef?.value || '';
      document.getElementById('txnDate').value = invoice.TxnDate || '';
      document.getElementById('dueDate').value = invoice.DueDate || '';
      if (invoice.Line) {
        invoice.Line.forEach(line => {
          if (line.DetailType === 'SalesItemLineDetail' && line.SalesItemLineDetail?.ItemRef) {
            const itemId = line.SalesItemLineDetail.ItemRef.value;
            const qty = line.SalesItemLineDetail.Qty;
            const rate = line.SalesItemLineDetail.UnitPrice || (line.Amount / qty);
            const desc = line.Description || '';
            addInvoiceLine(itemId, rate, desc, qty);
          }
        });
      }
    }

    // Auto-fill rate & desc on item select
    document.getElementById('item').addEventListener('change', e => {
      const opt = e.target.options[e.target.selectedIndex];
      document.getElementById('rate').value = opt.dataset.rate || '';
      document.getElementById('desc').value = opt.dataset.desc || '';
    });

    // Add line
    document.getElementById('add-line').addEventListener('click', () => {
      const itemSelect = document.getElementById('item');
      const itemId = itemSelect.value;
      if (!itemId) return showToast('Select an item first', 'error');

      const qty = parseFloat(document.getElementById('qty').value) || 1;
      const rate = parseFloat(document.getElementById('rate').value) || 0;
      const desc = document.getElementById('desc').value.trim();

      addInvoiceLine(itemId, rate, desc, qty);

      // Reset inputs
      itemSelect.value = '';
      document.getElementById('qty').value = '1';
      document.getElementById('rate').value = '';
      document.getElementById('desc').value = '';
    });

    // Form submit
    document.getElementById('invoice-form').addEventListener('submit', async e => {
      e.preventDefault();
      const lines = getInvoiceLines();
      if (lines.length === 0) return showToast('Add at least one line item', 'error');

      const payload = {
        CustomerRef: { value: document.getElementById('customer').value },
        TxnDate: document.getElementById('txnDate').value,
        DueDate: document.getElementById('dueDate').value,
        Line: lines,
      };

      if (mode === 'update') {
        payload.Id = invoice.Id;
        payload.SyncToken = invoice.SyncToken;
        payload.sparse = true;
      }

      try {
        const url = mode === 'create' ? '/api/invoices' : `/api/invoices/${invoice.Id}`;
        const method = mode === 'create' ? 'POST' : 'PUT';
        const resp = await authenticatedFetch(url, {
          method,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        if (!resp.ok) {
          const errData = await resp.json();
          throw new Error(errData.detail || 'Save failed');
        }
        showToast(mode === 'create' ? 'Invoice created' : 'Invoice updated', 'success');
        loadSalesSection('invoices-list');
      } catch (err) {
        showToast(err.message, 'error');
      }
    });

    document.getElementById('cancel-btn').addEventListener('click', () => loadSalesSection('invoices-list'));
  } catch (err) {
    showToast(err.message, 'error');
    content.innerHTML = `<div class="p-10 text-center text-red-600">${err.message}</div>`;
  }
}

function addInvoiceLine(itemId, rate, desc, qty) {
  const container = document.getElementById('lines-container');
  const div = document.createElement('div');
  div.className = 'flex items-center mb-3 p-3 bg-gray-50 rounded border';
  div.innerHTML = `
    <input type="hidden" class="item-id" value="${itemId}">
    <div class="flex-1">
      <strong>${desc || 'Item'}</strong>
    </div>
    <input type="number" class="qty w-20 mx-2 p-1 border rounded text-center" value="${qty}" min="1">
    <input type="number" class="rate w-24 mx-2 p-1 border rounded text-right" value="${rate.toFixed(2)}" step="0.01">
    <span class="mx-4 font-medium total">$${(qty * rate).toFixed(2)}</span>
    <button type="button" class="remove-line text-red-600 hover:text-red-800">Remove</button>
  `;
  container.appendChild(div);

  // Update total on change
  const updateTotal = () => {
    const q = parseFloat(div.querySelector('.qty').value) || 0;
    const r = parseFloat(div.querySelector('.rate').value) || 0;
    div.querySelector('.total').textContent = `$${(q * r).toFixed(2)}`;
  };
  div.querySelector('.qty').addEventListener('input', updateTotal);
  div.querySelector('.rate').addEventListener('input', updateTotal);

  div.querySelector('.remove-line').addEventListener('click', () => div.remove());
}

function getInvoiceLines() {
  const lines = [];
  document.querySelectorAll('#lines-container > div').forEach(div => {
    const itemId = div.querySelector('.item-id').value;
    const qty = parseFloat(div.querySelector('.qty').value) || 1;
    const rate = parseFloat(div.querySelector('.rate').value) || 0;
    const desc = div.querySelector('strong').textContent || '';
    lines.push({
      Amount: qty * rate,
      DetailType: "SalesItemLineDetail",
      SalesItemLineDetail: {
        ItemRef: { value: itemId },
        Qty: qty,
        UnitPrice: rate
      },
      Description: desc
    });
  });
  return lines;
}

// Similar implementation for Sales Receipts (simplified - no DueDate, add PaymentMethod if needed later)
async function renderReceiptsList(content) {
  // Similar to renderInvoicesList, but adjust columns (no Balance)
  content.innerHTML = `
    <div class="flex justify-between items-center mb-6">
      <h2 class="text-2xl font-bold text-gray-800">Recent Sales Receipts</h2>
      <button id="btn-create-receipt" class="px-5 py-2.5 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 transition">
        + Create Receipt
      </button>
    </div>
    <div class="overflow-x-auto bg-white rounded-lg shadow">
      <table class="min-w-full divide-y divide-gray-200">
        <thead class="bg-gray-50">
          <tr>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Customer</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Total</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Date</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
          </tr>
        </thead>
        <tbody id="receipts-tbody" class="bg-white divide-y divide-gray-200"></tbody>
      </table>
    </div>
  `;

  try {
    const resp = await authenticatedFetch('/api/sales_receipts?limit=10');
    if (resp.status === 401) throw new Error('Unauthorized – please reconnect to QuickBooks');
    if (!resp.ok) throw new Error(`Server error: ${resp.status}`);

    const receipts = await resp.json();
    const tbody = document.getElementById('receipts-tbody');

    if (!receipts || receipts.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" class="px-6 py-10 text-center text-gray-500">No sales receipts found</td></tr>';
    } else {
      tbody.innerHTML = receipts.map(rec => `
        <tr class="hover:bg-gray-50 transition-colors">
          <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">${rec.Id}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${rec.CustomerRef?.name || '—'}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">$${Number(rec.TotalAmt || 0).toFixed(2)}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${rec.TxnDate || '—'}</td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
            <button class="text-blue-600 hover:text-blue-900 mr-3 edit-btn" data-id="${rec.Id}">Edit</button>
            <button class="text-red-600 hover:text-red-900 void-btn" data-id="${rec.Id}">Void</button>
          </td>
        </tr>
      `).join('');
    }
  } catch (err) {
    showToast(err.message, 'error');
    document.getElementById('receipts-tbody').innerHTML = `
      <tr><td colspan="5" class="px-6 py-10 text-center text-red-600">${err.message}</td></tr>
    `;
  }

  setTimeout(() => {
    const createBtn = document.getElementById('btn-create-receipt');
    if (createBtn) createBtn.addEventListener('click', () => loadSalesSection('receipts-create'));

    document.querySelectorAll('.edit-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        const id = btn.dataset.id;
        showLoading();
        try {
          const resp = await authenticatedFetch(`/api/sales_receipts/${id}`);
          if (!resp.ok) throw new Error('Failed to load receipt');
          const data = await resp.json();
          await renderReceiptForm(document.getElementById('content-container'), 'update', data);
        } catch (e) {
          showToast(e.message, 'error');
        }
      });
    });

    document.querySelectorAll('.void-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        if (!confirm('Void this sales receipt? It will set amounts to $0 and preserve history.')) return;
        const id = btn.dataset.id;
        try {
          const resp = await authenticatedFetch(`/api/sales_receipts/${id}`, { method: 'DELETE' });
          if (!resp.ok) throw new Error('Void failed');
          showToast('Sales Receipt voided successfully', 'success');
          await renderReceiptsList(document.getElementById('content-container'));
        } catch (e) {
          showToast(e.message, 'error');
        }
      });
    });
  }, 0);
}

async function renderReceiptForm(content, mode = 'create', receipt = null) {
  // Similar to renderInvoiceForm, but remove DueDate and adjust as needed
  // For now, use the same structure minus DueDate
  // You can add PaymentMethodRef, DepositToAccountRef later
  try {
    const customers = await fetchCustomers();
    const items = await fetchItems();

    content.innerHTML = `
      <div class="max-w-4xl mx-auto p-6 bg-white rounded-lg shadow">
        <h2 class="text-2xl font-bold mb-6">${mode === 'create' ? 'Create Sales Receipt' : 'Edit Sales Receipt'}</h2>
        <form id="receipt-form">
          <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
            <div>
              <label class="block text-gray-700 font-medium mb-1">Customer *</label>
              <select id="customer" class="w-full p-2 border rounded" required>
                <option value="">Select Customer</option>
                ${customers.map(c => `<option value="${c.Id}">${c.DisplayName || c.name || 'Unnamed'}</option>`).join('')}
              </select>
            </div>
            <div>
              <label class="block text-gray-700 font-medium mb-1">Date</label>
              <input type="date" id="txnDate" class="w-full p-2 border rounded" value="${new Date().toISOString().split('T')[0]}" required>
            </div>
          </div>

          <h3 class="text-lg font-semibold mb-3">Line Items</h3>
          <div class="mb-4 grid grid-cols-1 sm:grid-cols-5 gap-2">
            <select id="item" class="p-2 border rounded col-span-2">
              <option value="">Select Product/Service</option>
              ${items.map(i => `<option value="${i.Id}" data-rate="${i.SalesPrice || 0}" data-desc="${(i.Description || '').replace(/"/g, '&quot;')}">${i.Name}</option>`).join('')}
            </select>
            <input type="number" id="qty" value="1" min="1" class="p-2 border rounded w-20" placeholder="Qty">
            <input type="number" id="rate" step="0.01" class="p-2 border rounded" placeholder="Rate">
            <input type="text" id="desc" class="p-2 border rounded col-span-2" placeholder="Description">
            <button type="button" id="add-line" class="bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700">Add</button>
          </div>
          <div id="lines-container" class="mb-6"></div>

          <div class="flex justify-end mt-6">
            <button type="button" id="cancel-btn" class="px-6 py-2 bg-gray-300 text-gray-800 rounded mr-4 hover:bg-gray-400">Cancel</button>
            <button type="submit" class="px-6 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">Save Receipt</button>
          </div>
        </form>
      </div>
    `;

    if (mode === 'update' && receipt) {
      document.getElementById('customer').value = receipt.CustomerRef?.value || '';
      document.getElementById('txnDate').value = receipt.TxnDate || '';
      if (receipt.Line) {
        receipt.Line.forEach(line => {
          if (line.DetailType === 'SalesItemLineDetail' && line.SalesItemLineDetail?.ItemRef) {
            const itemId = line.SalesItemLineDetail.ItemRef.value;
            const qty = line.SalesItemLineDetail.Qty;
            const rate = line.SalesItemLineDetail.UnitPrice || (line.Amount / qty);
            const desc = line.Description || '';
            addReceiptLine(itemId, rate, desc, qty);
          }
        });
      }
    }

    document.getElementById('item').addEventListener('change', e => {
      const opt = e.target.options[e.target.selectedIndex];
      document.getElementById('rate').value = opt.dataset.rate || '';
      document.getElementById('desc').value = opt.dataset.desc || '';
    });

    document.getElementById('add-line').addEventListener('click', () => {
      const itemSelect = document.getElementById('item');
      const itemId = itemSelect.value;
      if (!itemId) return showToast('Select an item first', 'error');

      const qty = parseFloat(document.getElementById('qty').value) || 1;
      const rate = parseFloat(document.getElementById('rate').value) || 0;
      const desc = document.getElementById('desc').value.trim();

      addReceiptLine(itemId, rate, desc, qty);

      itemSelect.value = '';
      document.getElementById('qty').value = '1';
      document.getElementById('rate').value = '';
      document.getElementById('desc').value = '';
    });

    document.getElementById('receipt-form').addEventListener('submit', async e => {
      e.preventDefault();
      const lines = getReceiptLines();
      if (lines.length === 0) return showToast('Add at least one line item', 'error');

      const payload = {
        CustomerRef: { value: document.getElementById('customer').value },
        TxnDate: document.getElementById('txnDate').value,
        Line: lines,
      };

      if (mode === 'update') {
        payload.Id = receipt.Id;
        payload.SyncToken = receipt.SyncToken;
        payload.sparse = true;
      }

      try {
        const url = mode === 'create' ? '/api/sales_receipts' : `/api/sales_receipts/${receipt.Id}`;
        const method = mode === 'create' ? 'POST' : 'PUT';
        const resp = await authenticatedFetch(url, {
          method,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        if (!resp.ok) {
          const errData = await resp.json();
          throw new Error(errData.detail || 'Save failed');
        }
        showToast(mode === 'create' ? 'Receipt created' : 'Receipt updated', 'success');
        loadSalesSection('receipts-list');
      } catch (err) {
        showToast(err.message, 'error');
      }
    });

    document.getElementById('cancel-btn').addEventListener('click', () => loadSalesSection('receipts-list'));
  } catch (err) {
    showToast(err.message, 'error');
    content.innerHTML = `<div class="p-10 text-center text-red-600">${err.message}</div>`;
  }
}

function addReceiptLine(itemId, rate, desc, qty) {
  const container = document.getElementById('lines-container');
  const div = document.createElement('div');
  div.className = 'flex items-center mb-3 p-3 bg-gray-50 rounded border';
  div.innerHTML = `
    <input type="hidden" class="item-id" value="${itemId}">
    <div class="flex-1">
      <strong>${desc || 'Item'}</strong>
    </div>
    <input type="number" class="qty w-20 mx-2 p-1 border rounded text-center" value="${qty}" min="1">
    <input type="number" class="rate w-24 mx-2 p-1 border rounded text-right" value="${rate.toFixed(2)}" step="0.01">
    <span class="mx-4 font-medium total">$${(qty * rate).toFixed(2)}</span>
    <button type="button" class="remove-line text-red-600 hover:text-red-800">Remove</button>
  `;
  container.appendChild(div);

  const updateTotal = () => {
    const q = parseFloat(div.querySelector('.qty').value) || 0;
    const r = parseFloat(div.querySelector('.rate').value) || 0;
    div.querySelector('.total').textContent = `$${(q * r).toFixed(2)}`;
  };
  div.querySelector('.qty').addEventListener('input', updateTotal);
  div.querySelector('.rate').addEventListener('input', updateTotal);

  div.querySelector('.remove-line').addEventListener('click', () => div.remove());
}

function getReceiptLines() {
  const lines = [];
  document.querySelectorAll('#lines-container > div').forEach(div => {
    const itemId = div.querySelector('.item-id').value;
    const qty = parseFloat(div.querySelector('.qty').value) || 1;
    const rate = parseFloat(div.querySelector('.rate').value) || 0;
    const desc = div.querySelector('strong').textContent || '';
    lines.push({
      Amount: qty * rate,
      DetailType: "SalesItemLineDetail",
      SalesItemLineDetail: {
        ItemRef: { value: itemId },
        Qty: qty,
        UnitPrice: rate
      },
      Description: desc
    });
  });
  return lines;
}