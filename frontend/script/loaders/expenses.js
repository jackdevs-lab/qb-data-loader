// frontend/loaders/expenses.js

import { APP_STATE } from '../state.js';
import { showLoading, showToast } from '../utils.js';

export async function loadExpensesSection(subSection = 'expenses-list') {
    const content = document.getElementById('content-container');
    content.innerHTML = '';

    if (subSection === 'expenses-list') {
        await renderExpensesList(content);
    } else if (subSection === 'expenses-create') {
        await renderExpenseCreate(content);
    } else if (subSection === 'bills-list') {
        await renderBillsList(content);
    } else if (subSection === 'bills-create') {
        await renderBillCreate(content);
    }
}

async function renderExpensesList(content) {
    content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">Recent Expenses & Bills</h2>
    <table id="expenses-table" class="w-full border-collapse mb-6">
      <thead>
        <tr class="bg-gray-100">
          <th class="p-3 text-left">Type</th>
          <th class="p-3 text-left">Payee/Vendor</th>
          <th class="p-3 text-left">Amount</th>
          <th class="p-3 text-left">Date</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
    <button id="create-expense-btn" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 mr-2">
      Create Expense
    </button>
    <button id="create-bill-btn" class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
      Create Bill
    </button>
  `;

    showLoading();
    try {
        const resp = await fetch('/api/expenses');
        if (!resp.ok) throw new Error('Failed to fetch expenses');
        const data = await resp.json();
        const tbody = document.querySelector('#expenses-table tbody');
        tbody.innerHTML = '';
        data.expenses.forEach(exp => {
            tbody.innerHTML += `
        <tr class="border-b">
          <td class="p-3">Expense</td>
          <td class="p-3">${exp.EntityRef?.name || 'N/A'}</td>
          <td class="p-3">${exp.TotalAmt || 0}</td>
          <td class="p-3">${exp.TxnDate || 'N/A'}</td>
        </tr>
      `;
        });
        data.bills.forEach(bill => {
            tbody.innerHTML += `
        <tr class="border-b">
          <td class="p-3">Bill</td>
          <td class="p-3">${bill.VendorRef?.name || 'N/A'}</td>
          <td class="p-3">${bill.TotalAmt || 0}</td>
          <td class="p-3">${bill.TxnDate || 'N/A'}</td>
        </tr>
      `;
        });
    } catch (e) {
        showToast('Error loading expenses: ' + e.message, 'error');
    }

    document.getElementById('create-expense-btn').onclick = () => loadExpensesSection('expenses-create');
    document.getElementById('create-bill-btn').onclick = () => loadExpensesSection('bills-create');
}

async function renderExpenseCreate(content) {
    const vendors = await fetch('/api/references/vendors').then(r => r.json()).catch(() => []);
    const accounts = await fetch('/api/references/accounts').then(r => r.json()).catch(() => []);
    const paymentAccounts = accounts.filter(a => ['Bank', 'Credit Card'].includes(a.AccountType));
    const expenseAccounts = accounts.filter(a => a.AccountType === 'Expenses');

    content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">Create Expense</h2>
    <form id="expense-form" class="space-y-4 max-w-2xl">
      <div>
        <label class="block text-sm font-medium">Payment Account *</label>
        <select name="AccountRef" class="w-full p-2 border rounded" required>
          <option value="">Select Account</option>
          ${paymentAccounts.map(a => `<option value="${a.Id}">${a.Name}</option>`).join('')}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium">Payment Type *</label>
        <select name="PaymentType" class="w-full p-2 border rounded" required>
          <option value="">Select Type</option>
          <option value="Cash">Cash</option>
          <option value="Check">Check</option>
          <option value="CreditCard">Credit Card</option>
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium">Transaction Date *</label>
        <input type="date" name="TxnDate" class="w-full p-2 border rounded" required value="${new Date().toISOString().split('T')[0]}">
      </div>
      <div>
        <label class="block text-sm font-medium">Payee (Vendor) *</label>
        <select name="EntityRef" class="w-full p-2 border rounded" required>
          <option value="">Select Vendor</option>
          ${vendors.map(v => `<option value="${v.Id}">${v.DisplayName}</option>`).join('')}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium">Lines *</label>
        <div id="expense-lines" class="space-y-4"></div>
        <button type="button" id="add-line-btn" class="mt-2 px-3 py-1 bg-gray-200 rounded hover:bg-gray-300">
          Add Line
        </button>
      </div>
      <button type="submit" class="px-6 py-2 bg-green-600 text-white rounded hover:bg-green-700">
        Create Expense
      </button>
    </form>
  `;

    setupExpenseLines('expense-lines', expenseAccounts);

    document.getElementById('add-line-btn').onclick = () => addExpenseLine('expense-lines', expenseAccounts);

    document.getElementById('expense-form').onsubmit = async (e) => {
        e.preventDefault();
        const formData = new FormData(e.target);
        const lines = getExpenseLines('expense-lines');
        if (lines.length === 0) return showToast('Add at least one line', 'error');

        const payload = {
            AccountRef: { value: formData.get('AccountRef') },
            PaymentType: formData.get('PaymentType'),
            TxnDate: formData.get('TxnDate'),
            EntityRef: { value: formData.get('EntityRef'), type: 'Vendor' },
            Line: lines
        };

        try {
            const resp = await fetch('/api/expenses', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!resp.ok) throw new Error(await resp.text());
            showToast('Expense created successfully');
            loadExpensesSection('expenses-list');
        } catch (err) {
            showToast('Error creating expense: ' + err.message, 'error');
        }
    };
}

async function renderBillCreate(content) {
    const vendors = await fetch('/api/references/vendors').then(r => r.json()).catch(() => []);
    const expenseAccounts = await fetch('/api/references/accounts').then(r => r.json()).then(accs => accs.filter(a => a.AccountType === 'Expenses'));

    content.innerHTML = `
    <h2 class="text-2xl font-bold mb-6">Create Bill</h2>
    <form id="bill-form" class="space-y-4 max-w-2xl">
      <div>
        <label class="block text-sm font-medium">Vendor *</label>
        <select name="VendorRef" class="w-full p-2 border rounded" required>
          <option value="">Select Vendor</option>
          ${vendors.map(v => `<option value="${v.Id}">${v.DisplayName}</option>`).join('')}
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
        <div id="bill-lines" class="space-y-4"></div>
        <button type="button" id="add-line-btn" class="mt-2 px-3 py-1 bg-gray-200 rounded hover:bg-gray-300">
          Add Line
        </button>
      </div>
      <button type="submit" class="px-6 py-2 bg-green-600 text-white rounded hover:bg-green-700">
        Create Bill
      </button>
    </form>
  `;

    setupExpenseLines('bill-lines', expenseAccounts);

    document.getElementById('add-line-btn').onclick = () => addExpenseLine('bill-lines', expenseAccounts);

    document.getElementById('bill-form').onsubmit = async (e) => {
        e.preventDefault();
        const formData = new FormData(e.target);
        const lines = getExpenseLines('bill-lines');
        if (lines.length === 0) return showToast('Add at least one line', 'error');

        const payload = {
            VendorRef: { value: formData.get('VendorRef') },
            TxnDate: formData.get('TxnDate'),
            DueDate: formData.get('DueDate'),
            Line: lines
        };

        try {
            const resp = await fetch('/api/expenses/bill', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!resp.ok) throw new Error(await resp.text());
            showToast('Bill created successfully');
            loadExpensesSection('expenses-list');
        } catch (err) {
            showToast('Error creating bill: ' + err.message, 'error');
        }
    };
}

// Shared helpers for expense/bill lines
function setupExpenseLines(containerId, accounts) {
    const container = document.getElementById(containerId);
    addExpenseLine(containerId, accounts); // First line
}

function addExpenseLine(containerId, accounts) {
    const container = document.getElementById(containerId);
    const lineDiv = document.createElement('div');
    lineDiv.className = 'flex space-x-2 items-end';
    lineDiv.innerHTML = `
    <div class="flex-1">
      <label class="block text-sm">Category (Account) *</label>
      <select name="AccountRef" class="w-full p-2 border rounded" required>
        <option value="">Select Account</option>
        ${accounts.map(a => `<option value="${a.Id}">${a.Name}</option>`).join('')}
      </select>
    </div>
    <div class="w-32">
      <label class="block text-sm">Amount *</label>
      <input type="number" name="Amount" step="0.01" value="0.00" class="w-full p-2 border rounded" required>
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

function getExpenseLines(containerId) {
    const container = document.getElementById(containerId);
    const lines = [];
    container.querySelectorAll('div.flex').forEach(div => {
        const accountRef = div.querySelector('[name="AccountRef"]').value;
        const amount = parseFloat(div.querySelector('[name="Amount"]').value);
        const description = div.querySelector('[name="Description"]').value;
        if (accountRef && amount > 0) {
            lines.push({
                Description: description,
                Amount: amount,
                DetailType: 'AccountBasedExpenseLineDetail',
                AccountBasedExpenseLineDetail: {
                    AccountRef: { value: accountRef },
                    BillableStatus: 'NotBillable'
                }
            });
        }
    });
    return lines;
}