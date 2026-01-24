// frontend/loaders/sales.js

import { APP_STATE } from '../state.js';
import { showLoading, showToast } from '../utils.js';

export async function loadSalesSection(subSection = 'invoices-list') {
    const content = document.getElementById('content-container');
    content.innerHTML = '';

    if (subSection === 'invoices-list') {
        await renderInvoicesList(content);
    } else if (subSection === 'invoices-create') {
        await renderInvoiceCreate(content);
    } else if (subSection === 'receipts-list') {
        await renderReceiptsList(content);
    } else if (subSection === 'receipts-create') {
        await renderReceiptCreate(content);
    }
}

async function renderInvoicesList(content) {
    content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">Recent Invoices</h2>
    <table id="invoices-table" class="w-full border-collapse mb-6">
      <thead>
        <tr class="bg-gray-100">
          <th class="p-3 text-left">ID</th>
          <th class="p-3 text-left">Customer</th>
          <th class="p-3 text-left">Total</th>
          <th class="p-3 text-left">Balance</th>
          <th class="p-3 text-left">Date</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
    <button id="create-invoice-btn" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
      Create Invoice
    </button>
  `;

    showLoading();
    try {
        const resp = await fetch('/api/invoices');
        if (!resp.ok) throw new Error('Failed to fetch invoices');
        const invoices = await resp.json();
        const tbody = document.querySelector('#invoices-table tbody');
        tbody.innerHTML = '';
        invoices.forEach(inv => {
            const row = `
        <tr class="border-b">
          <td class="p-3">${inv.Id}</td>
          <td class="p-3">${inv.CustomerRef?.name || 'N/A'}</td>
          <td class="p-3">${inv.TotalAmt || 0}</td>
          <td class="p-3">${inv.Balance || 0}</td>
          <td class="p-3">${inv.TxnDate || 'N/A'}</td>
        </tr>
      `;
            tbody.innerHTML += row;
        });
    } catch (e) {
        showToast('Error loading invoices: ' + e.message, 'error');
    }

    document.getElementById('create-invoice-btn').onclick = () => loadSalesSection('invoices-create');
}

async function renderInvoiceCreate(content) {
    const customers = await fetch('/api/customers').then(r => r.json()).catch(() => []);
    const items = await fetch('/api/products').then(r => r.json()).catch(() => []);

    content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">Create Invoice</h2>
    <form id="invoice-form" class="space-y-4 max-w-2xl">
      <div>
        <label class="block text-sm font-medium">Customer *</label>
        <select name="CustomerRef" class="w-full p-2 border rounded" required>
          <option value="">Select Customer</option>
          ${customers.map(c => `<option value="${c.Id}">${c.DisplayName || c.CompanyName}</option>`).join('')}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium">Transaction Date *</label>
        <input type="date" name="TxnDate" class="w-full p-2 border rounded" required value="${new Date().toISOString().split('T')[0]}">
      </div>
      <div>
        <label class="block text-sm font-medium">Due Date *</label>
        <input type="date" name="DueDate" class="w-full p-2 border rounded" required value="${new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0]}">
      </div>
      <div>
        <label class="block text-sm font-medium">Lines *</label>
        <div id="invoice-lines" class="space-y-4"></div>
        <button type="button" id="add-line-btn" class="mt-2 px-3 py-1 bg-gray-200 rounded hover:bg-gray-300">
          Add Line
        </button>
      </div>
      <button type="submit" class="px-6 py-2 bg-green-600 text-white rounded hover:bg-green-700">
        Create Invoice
      </button>
    </form>
  `;

    setupLineItems('invoice-lines', items, 'SalesItemLineDetail');

    document.getElementById('add-line-btn').onclick = () => addLine('invoice-lines', items, 'SalesItemLineDetail');

    document.getElementById('invoice-form').onsubmit = async (e) => {
        e.preventDefault();
        const formData = new FormData(e.target);
        const lines = getLines('invoice-lines', 'SalesItemLineDetail');
        if (lines.length === 0) return showToast('Add at least one line', 'error');

        const payload = {
            CustomerRef: { value: formData.get('CustomerRef') },
            Line: lines,
            TxnDate: formData.get('TxnDate'),
            DueDate: formData.get('DueDate')
        };

        try {
            const resp = await fetch('/api/invoices', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!resp.ok) throw new Error(await resp.text());
            showToast('Invoice created successfully');
            loadSalesSection('invoices-list');
        } catch (err) {
            showToast('Error creating invoice: ' + err.message, 'error');
        }
    };
}

async function renderReceiptsList(content) {
    content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">Recent Sales Receipts</h2>
    <table id="receipts-table" class="w-full border-collapse mb-6">
      <thead>
        <tr class="bg-gray-100">
          <th class="p-3 text-left">ID</th>
          <th class="p-3 text-left">Customer</th>
          <th class="p-3 text-left">Total</th>
          <th class="p-3 text-left">Date</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
    <button id="create-receipt-btn" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
      Create Sales Receipt
    </button>
  `;

    showLoading();
    try {
        const resp = await fetch('/api/sales_receipts');
        if (!resp.ok) throw new Error('Failed to fetch receipts');
        const receipts = await resp.json();
        const tbody = document.querySelector('#receipts-table tbody');
        tbody.innerHTML = '';
        receipts.forEach(rec => {
            const row = `
        <tr class="border-b">
          <td class="p-3">${rec.Id}</td>
          <td class="p-3">${rec.CustomerRef?.name || 'N/A'}</td>
          <td class="p-3">${rec.TotalAmt || 0}</td>
          <td class="p-3">${rec.TxnDate || 'N/A'}</td>
        </tr>
      `;
            tbody.innerHTML += row;
        });
    } catch (e) {
        showToast('Error loading receipts: ' + e.message, 'error');
    }

    document.getElementById('create-receipt-btn').onclick = () => loadSalesSection('receipts-create');
}

async function renderReceiptCreate(content) {
    const customers = await fetch('/api/customers').then(r => r.json()).catch(() => []);
    const items = await fetch('/api/products').then(r => r.json()).catch(() => []);
    const paymentMethods = await fetch('/api/references/paymentmethods').then(r => r.json()).catch(() => []);
    const accounts = await fetch('/api/references/accounts').then(r => r.json()).catch(() => []);

    const bankAccounts = accounts.filter(a => a.AccountType === 'Bank');

    content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">Create Sales Receipt</h2>
    <form id="receipt-form" class="space-y-4 max-w-2xl">
      <div>
        <label class="block text-sm font-medium">Customer *</label>
        <select name="CustomerRef" class="w-full p-2 border rounded" required>
          <option value="">Select Customer</option>
          ${customers.map(c => `<option value="${c.Id}">${c.DisplayName || c.CompanyName}</option>`).join('')}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium">Transaction Date *</label>
        <input type="date" name="TxnDate" class="w-full p-2 border rounded" required value="${new Date().toISOString().split('T')[0]}">
      </div>
      <div>
        <label class="block text-sm font-medium">Payment Method *</label>
        <select name="PaymentMethodRef" class="w-full p-2 border rounded" required>
          <option value="">Select Payment Method</option>
          ${paymentMethods.map(pm => `<option value="${pm.Id}">${pm.Name}</option>`).join('')}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium">Deposit To Account *</label>
        <select name="DepositToAccountRef" class="w-full p-2 border rounded" required>
          <option value="">Select Account</option>
          ${bankAccounts.map(acc => `<option value="${acc.Id}">${acc.Name}</option>`).join('')}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium">Lines *</label>
        <div id="receipt-lines" class="space-y-4"></div>
        <button type="button" id="add-line-btn" class="mt-2 px-3 py-1 bg-gray-200 rounded hover:bg-gray-300">
          Add Line
        </button>
      </div>
      <button type="submit" class="px-6 py-2 bg-green-600 text-white rounded hover:bg-green-700">
        Create Receipt
      </button>
    </form>
  `;

    setupLineItems('receipt-lines', items, 'SalesItemLineDetail');

    document.getElementById('add-line-btn').onclick = () => addLine('receipt-lines', items, 'SalesItemLineDetail');

    document.getElementById('receipt-form').onsubmit = async (e) => {
        e.preventDefault();
        const formData = new FormData(e.target);
        const lines = getLines('receipt-lines', 'SalesItemLineDetail');
        if (lines.length === 0) return showToast('Add at least one line', 'error');

        const payload = {
            CustomerRef: { value: formData.get('CustomerRef') },
            Line: lines,
            TxnDate: formData.get('TxnDate'),
            PaymentMethodRef: { value: formData.get('PaymentMethodRef') },
            DepositToAccountRef: { value: formData.get('DepositToAccountRef') }
        };

        try {
            const resp = await fetch('/api/sales_receipts', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!resp.ok) throw new Error(await resp.text());
            showToast('Sales Receipt created successfully');
            loadSalesSection('receipts-list');
        } catch (err) {
            showToast('Error creating receipt: ' + err.message, 'error');
        }
    };
}

// Shared helper functions
function setupLineItems(containerId, items, detailType) {
    const container = document.getElementById(containerId);
    addLine(containerId, items, detailType); // Add first line by default
}

function addLine(containerId, items, detailType) {
    const container = document.getElementById(containerId);
    const lineDiv = document.createElement('div');
    lineDiv.className = 'flex space-x-2 items-end';
    lineDiv.innerHTML = `
    <div class="flex-1">
      <label class="block text-sm">Item/Service *</label>
      <select name="ItemRef" class="w-full p-2 border rounded" required>
        <option value="">Select Item</option>
        ${items.map(i => `<option value="${i.Id}">${i.Name}</option>`).join('')}
      </select>
    </div>
    <div class="w-24">
      <label class="block text-sm">Qty *</label>
      <input type="number" name="Qty" min="1" value="1" class="w-full p-2 border rounded" required>
    </div>
    <div class="w-32">
      <label class="block text-sm">Unit Price *</label>
      <input type="number" name="UnitPrice" step="0.01" value="0.00" class="w-full p-2 border rounded" required>
    </div>
    <div class="flex-1">
      <label class="block text-sm">Description</label>
      <input type="text" name="Description" class="w-full p-2 border rounded">
    </div>
    <button type="button" class="remove-line px-2 py-1 bg-red-200 rounded text-red-700">Remove</button>
  `;
    container.appendChild(lineDiv);

    lineDiv.querySelector('.remove-line').onclick = () => lineDiv.remove();
}

function getLines(containerId, detailType) {
    const container = document.getElementById(containerId);
    const lines = [];
    container.querySelectorAll('div.flex').forEach(div => {
        const itemRef = div.querySelector('[name="ItemRef"]').value;
        const qty = parseFloat(div.querySelector('[name="Qty"]').value);
        const unitPrice = parseFloat(div.querySelector('[name="UnitPrice"]').value);
        const description = div.querySelector('[name="Description"]').value;
        if (itemRef && qty > 0 && unitPrice > 0) {
            const amount = qty * unitPrice;
            lines.push({
                Description: description,
                Amount: amount,
                DetailType: detailType,
                [detailType]: {
                    ItemRef: { value: itemRef },
                    Qty: qty,
                    UnitPrice: unitPrice
                }
            });
        }
    });
    return lines;
}