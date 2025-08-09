// Proxy page JavaScript

let proxies = [];

document.addEventListener('DOMContentLoaded', async () => {
    await loadProxies();
});

async function loadProxies() {
    try {
        showLoading();
        proxies = await api.getProxies();
        renderProxies();
        updateStats();
        hideLoading();
    } catch (error) {
        console.error('Error loading proxies:', error);
        hideLoading();
    }
}

function renderProxies() {
    const tbody = document.getElementById('proxy-table-body');
    
    if (proxies.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" class="text-center py-12">
                    <div class="flex flex-col items-center">
                        <i data-lucide="globe" class="h-12 w-12 text-slate-400 mb-4"></i>
                        <h3 class="text-lg font-medium text-white mb-2">Нет прокси серверов</h3>
                        <p class="text-slate-400 mb-4">Добавьте первый прокси для начала работы</p>
                        <button onclick="openAddProxyModal()" class="bg-blue-600 hover:bg-blue-700 px-4 py-2 rounded-md text-white transition-colors">
                            Добавить прокси
                        </button>
                    </div>
                </td>
            </tr>
        `;
        lucide.createIcons();
        return;
    }

    const html = proxies.map(proxy => `
        <tr class="border-b border-slate-700 hover:bg-slate-700/50 transition-colors">
            <td class="py-4 px-6 text-white font-medium">${proxy.host}</td>
            <td class="py-4 px-6 text-slate-300">${proxy.port}</td>
            <td class="py-4 px-6">
                <span class="inline-block px-2 py-1 text-xs rounded bg-slate-600 text-white uppercase">
                    ${proxy.type || 'HTTP'}
                </span>
            </td>
            <td class="py-4 px-6">
                <span class="inline-flex items-center gap-1 px-2 py-1 text-xs rounded ${
                    proxy.is_active ? 'bg-green-600' : 'bg-red-600'
                } text-white">
                    <i data-lucide="${proxy.is_active ? 'check' : 'x'}" class="h-3 w-3"></i>
                    ${proxy.is_active ? 'Активен' : 'Неактивен'}
                </span>
            </td>
            <td class="py-4 px-6 text-slate-300">
                ${proxy.assigned_to ? `@${proxy.assigned_to}` : 'Не назначен'}
            </td>
            <td class="py-4 px-6 text-right">
                <div class="flex justify-end gap-2">
                    <button onclick="checkProxy(${proxy.id})" class="p-2 text-slate-400 hover:text-blue-400 transition-colors" title="Проверить">
                        <i data-lucide="refresh-cw" class="h-4 w-4"></i>
                    </button>
                    <button onclick="editProxy(${proxy.id})" class="p-2 text-slate-400 hover:text-white transition-colors" title="Редактировать">
                        <i data-lucide="edit" class="h-4 w-4"></i>
                    </button>
                    <button onclick="deleteProxy(${proxy.id})" class="p-2 text-slate-400 hover:text-red-400 transition-colors" title="Удалить">
                        <i data-lucide="trash-2" class="h-4 w-4"></i>
                    </button>
                </div>
            </td>
        </tr>
    `).join('');
    
    tbody.innerHTML = html;
    lucide.createIcons();
}

function updateStats() {
    const total = proxies.length;
    const active = proxies.filter(p => p.is_active).length;
    const inactive = total - active;
    const assigned = proxies.filter(p => p.assigned_to).length;
    
    document.getElementById('total-proxies').textContent = total;
    document.getElementById('active-proxies').textContent = active;
    document.getElementById('inactive-proxies').textContent = inactive;
    document.getElementById('assigned-proxies').textContent = assigned;
}

function openAddProxyModal() {
    document.getElementById('add-proxy-modal').classList.remove('hidden');
}

function closeAddProxyModal() {
    document.getElementById('add-proxy-modal').classList.add('hidden');
    // Reset form
    document.querySelector('#add-proxy-modal form').reset();
}

function openBulkProxyModal() {
    document.getElementById('bulk-proxy-modal').classList.remove('hidden');
}

function closeBulkProxyModal() {
    document.getElementById('bulk-proxy-modal').classList.add('hidden');
    document.querySelector('#bulk-proxy-modal form').reset();
}

async function addProxy(event) {
    event.preventDefault();
    
    const host = document.getElementById('proxy-host').value;
    const port = parseInt(document.getElementById('proxy-port').value);
    const username = document.getElementById('proxy-username').value;
    const password = document.getElementById('proxy-password').value;
    const protocol = document.getElementById('proxy-type').value;
    
    try {
        const proxyData = {
            host: host,
            port: port,
            protocol: protocol,
            username: username || null,
            password: password || null
        };
        
        await api.addProxy(proxyData);
        closeAddProxyModal();
        await loadProxies();
        showNotification('Прокси успешно добавлен!', 'success');
    } catch (error) {
        showNotification('Ошибка при добавлении прокси: ' + error.message, 'error');
    }
}

async function bulkAddProxies(event) {
    event.preventDefault();
    
    const data = document.getElementById('bulk-proxies-data').value.trim();
    const checkProxies = document.getElementById('check-proxies').checked;
    
    if (!data) {
        showNotification('Введите данные прокси', 'error');
        return;
    }
    
    try {
        let proxiesData;
        
        // Try to parse as JSON first
        if (data.startsWith('[') || data.startsWith('{')) {
            proxiesData = JSON.parse(data);
        } else {
            // Parse as line-separated format
            proxiesData = data.split('\n').filter(line => line.trim()).map(line => {
                const parts = line.split(':');
                return {
                    host: parts[0],
                    port: parseInt(parts[1]),
                    protocol: parts[2] || 'http',
                    username: parts[3] || null,
                    password: parts[4] || null
                };
            });
        }
        
        await api.bulkAddProxies(proxiesData, checkProxies);
        closeBulkProxyModal();
        await loadProxies();
        showNotification(`Успешно добавлено ${proxiesData.length} прокси!`, 'success');
    } catch (error) {
        showNotification('Ошибка при массовом добавлении прокси: ' + error.message, 'error');
    }
}

async function checkProxy(proxyId) {
    try {
        const result = await api.checkProxy(proxyId);
        await loadProxies();
        showNotification(result.is_active ? 'Прокси работает!' : 'Прокси не отвечает!', result.is_active ? 'success' : 'error');
    } catch (error) {
        showNotification('Ошибка при проверке прокси: ' + error.message, 'error');
    }
}

async function checkAllProxies() {
    if (!confirm('Проверить все прокси? Это может занять некоторое время.')) return;
    
    try {
        await api.checkAllProxies();
        await loadProxies();
        showNotification('Проверка всех прокси завершена!', 'success');
    } catch (error) {
        showNotification('Ошибка при проверке прокси: ' + error.message, 'error');
    }
}

function editProxy(proxyId) {
    showNotification(`Редактирование прокси ${proxyId} (будет реализовано)`, 'info');
}

async function deleteProxy(proxyId) {
    if (!confirm('Удалить этот прокси?')) return;
    
    try {
        await api.deleteProxy(proxyId);
        await loadProxies();
        showNotification('Прокси удален', 'success');
    } catch (error) {
        showNotification('Ошибка при удалении: ' + error.message, 'error');
    }
}

function autoAssignProxies() {
    showNotification('Автоматическое назначение прокси (будет реализовано)', 'info');
}

function showLoading() {
    document.getElementById('proxy-table-body').innerHTML = `
        <tr>
            <td colspan="6" class="py-8">
                <div class="skeleton h-8 rounded mb-4"></div>
                <div class="skeleton h-8 rounded mb-4"></div>
                <div class="skeleton h-8 rounded"></div>
            </td>
        </tr>
    `;
}

function hideLoading() {
    // Loading will be replaced by renderProxies()
}

// Search functionality
document.getElementById('search-input').addEventListener('input', (e) => {
    const searchTerm = e.target.value.toLowerCase();
    const filtered = proxies.filter(proxy => 
        proxy.host.toLowerCase().includes(searchTerm) ||
        proxy.port.toString().includes(searchTerm) ||
        (proxy.assigned_to && proxy.assigned_to.toLowerCase().includes(searchTerm))
    );
    
    const tbody = document.getElementById('proxy-table-body');
    const html = filtered.map(proxy => `
        <tr class="border-b border-slate-700 hover:bg-slate-700/50 transition-colors">
            <td class="py-4 px-6 text-white font-medium">${proxy.host}</td>
            <td class="py-4 px-6 text-slate-300">${proxy.port}</td>
            <td class="py-4 px-6">
                <span class="inline-block px-2 py-1 text-xs rounded bg-slate-600 text-white uppercase">
                    ${proxy.type || 'HTTP'}
                </span>
            </td>
            <td class="py-4 px-6">
                <span class="inline-flex items-center gap-1 px-2 py-1 text-xs rounded ${
                    proxy.is_active ? 'bg-green-600' : 'bg-red-600'
                } text-white">
                    <i data-lucide="${proxy.is_active ? 'check' : 'x'}" class="h-3 w-3"></i>
                    ${proxy.is_active ? 'Активен' : 'Неактивен'}
                </span>
            </td>
            <td class="py-4 px-6 text-slate-300">
                ${proxy.assigned_to ? `@${proxy.assigned_to}` : 'Не назначен'}
            </td>
            <td class="py-4 px-6 text-right">
                <div class="flex justify-end gap-2">
                    <button onclick="checkProxy(${proxy.id})" class="p-2 text-slate-400 hover:text-blue-400 transition-colors" title="Проверить">
                        <i data-lucide="refresh-cw" class="h-4 w-4"></i>
                    </button>
                    <button onclick="editProxy(${proxy.id})" class="p-2 text-slate-400 hover:text-white transition-colors" title="Редактировать">
                        <i data-lucide="edit" class="h-4 w-4"></i>
                    </button>
                    <button onclick="deleteProxy(${proxy.id})" class="p-2 text-slate-400 hover:text-red-400 transition-colors" title="Удалить">
                        <i data-lucide="trash-2" class="h-4 w-4"></i>
                    </button>
                </div>
            </td>
        </tr>
    `).join('');
    
    tbody.innerHTML = html || '<tr><td colspan="6" class="text-center py-12 text-slate-400">Ничего не найдено</td></tr>';
    lucide.createIcons();
}); 