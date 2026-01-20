import { APP_STATE } from '../state.js';
import { fetchWithAuth } from '../helpers.js';
import { showToast } from '../utils.js';

export async function loadProductsSection(subSection) {
  try {
    let html = '';
   
    if (subSection === 'list' || !subSection) {
      // Load product list
      const res = await fetchWithAuth('/api/products');
      if (res.ok) {
        APP_STATE.products = await res.json();
      }
     
      html = `
        <div class="fade-in">
          <div class="flex justify-between items-center mb-8">
            <h2 class="text-2xl font-bold text-gray-800">Products & Services</h2>
            <button onclick="loadSection('products', 'create')" class="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
              <i class="fas fa-plus mr-2"></i>Add Product
            </button>
          </div>
         
          <!-- Products Table -->
          <div class="bg-white rounded-xl shadow overflow-hidden">
            ${APP_STATE.products.length > 0 ? `
              <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-gray-200">
                  <thead class="bg-gray-50">
                    <tr>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Description</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Price</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                      <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                    </tr>
                  </thead>
                  <tbody class="bg-white divide-y divide-gray-200">
                    ${APP_STATE.products.map(product => `
                      <tr class="hover:bg-gray-50">
                        <td class="px-6 py-4 whitespace-nowrap">
                          <div class="font-medium text-gray-900">${product.Name || 'Unnamed'}</div>
                        </td>
                        <td class="px-6 py-4">
                          <div class="text-gray-900 max-w-xs truncate">${product.Description || '—'}</div>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap">
                          <div class="text-gray-900">${product.Type || '—'}</div>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap">
                          <div class="text-gray-900">$${product.UnitPrice?.toFixed(2) || '0.00'}</div>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap">
                          <span class="px-3 py-1 text-xs rounded-full ${product.Active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'}">
                            ${product.Active ? 'Active' : 'Inactive'}
                          </span>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                          <button onclick="editProduct('${product.Id}')" class="text-blue-600 hover:text-blue-900 mr-4">
                            <i class="fas fa-edit"></i>
                          </button>
                          <button onclick="deleteProduct('${product.Id}', '${product.Name || 'Product'}')" class="text-red-600 hover:text-red-900">
                            <i class="fas fa-trash"></i>
                          </button>
                        </td>
                      </tr>
                    `).join('')}
                  </tbody>
                </table>
              </div>
            ` : `
              <div class="text-center py-12">
                <div class="text-gray-400 text-5xl mb-4">
                  <i class="fas fa-boxes"></i>
                </div>
                <h3 class="text-lg font-medium text-gray-900 mb-2">No Products Found</h3>
                <p class="text-gray-500 mb-6">Get started by adding your first product or service.</p>
                <button onclick="loadSection('products', 'create')" class="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
                  Add Product
                </button>
              </div>
            `}
          </div>
        </div>
      `;
    } else if (subSection === 'create') {
      // Create product form
      html = `
        <div class="fade-in max-w-4xl">
          <div class="flex items-center mb-8">
            <button onclick="loadSection('products', 'list')" class="text-blue-600 hover:text-blue-800 mr-4">
              <i class="fas fa-arrow-left"></i>
            </button>
            <h2 class="text-2xl font-bold text-gray-800">Add New Product/Service</h2>
          </div>
         
          <div class="bg-white rounded-xl shadow p-8">
            <form id="product-form" onsubmit="saveProduct(event)">
              <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Product/Service Name *</label>
                  <input type="text" name="Name" required class="w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Type</label>
                  <select name="Type" class="w-full px-4 py-2 border rounded-lg">
                    <option value="Service">Service</option>
                    <option value="Inventory">Inventory</option>
                    <option value="NonInventory">Non-Inventory</option>
                  </select>
                </div>
               
                <div class="md:col-span-2">
                  <label class="block text-sm font-medium text-gray-700 mb-2">Description</label>
                  <textarea name="Description" rows="3" class="w-full px-4 py-2 border rounded-lg"></textarea>
                </div>
               
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Unit Price ($)</label>
                  <input type="number" step="0.01" name="UnitPrice" class="w-full px-4 py-2 border rounded-lg">
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-2">Purchase Cost ($)</label>
                  <input type="number" step="0.01" name="PurchaseCost" class="w-full px-4 py-2 border rounded-lg">
                </div>
               
                <div class="md:col-span-2">
                  <div class="flex items-center space-x-6">
                    <label class="flex items-center">
                      <input type="checkbox" name="Taxable" class="mr-2">
                      <span class="text-sm text-gray-700">Taxable</span>
                    </label>
                    <label class="flex items-center">
                      <input type="checkbox" name="Active" checked class="mr-2">
                      <span class="text-sm text-gray-700">Active</span>
                    </label>
                  </div>
                </div>
              </div>
             
              <div class="flex justify-end space-x-4">
                <button type="button" onclick="loadSection('products', 'list')" class="px-6 py-3 border border-gray-300 rounded-lg hover:bg-gray-50">
                  Cancel
                </button>
                <button type="submit" class="px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700">
                  Save Product
                </button>
              </div>
            </form>
          </div>
        </div>
      `;
    }
   
    document.getElementById('content-container').innerHTML = html;
  } catch (error) {
    console.error('Error loading products section:', error);
    showToast('Error loading products: ' + error.message, 'error');
  }
}

export async function saveProduct(event) {
  event.preventDefault();
  const form = event.target;
  const formData = new FormData(form);
  const data = Object.fromEntries(formData.entries());
 
  // Convert checkbox values
  data.Taxable = formData.has('Taxable');
  data.Active = formData.has('Active');
 
  try {
    const res = await fetchWithAuth('/api/products', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });
   
    if (res.ok) {
      showToast('Product saved successfully!', 'success');
      loadSection('products', 'list');
    } else {
      throw new Error('Failed to save product');
    }
  } catch (error) {
    showToast('Error saving product: ' + error.message, 'error');
  }
}

export async function editProduct(id) {
  showToast('Edit functionality would fetch product details', 'info');
}

export async function deleteProduct(id, name) {
  if (confirm(`Are you sure you want to delete ${name}?`)) {
    try {
      const res = await fetchWithAuth(`/api/products/${id}`, {
        method: 'DELETE'
      });
     
      if (res.ok) {
        showToast('Product deleted successfully!', 'success');
        loadSection('products', 'list');
      } else {
        throw new Error('Failed to delete product');
      }
    } catch (error) {
      showToast('Error deleting product: ' + error.message, 'error');
    }
  }
}