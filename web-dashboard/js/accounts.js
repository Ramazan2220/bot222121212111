// Accounts page JavaScript

let currentTab = 'all';
let accounts = [];
let groups = [];
let selectedAccounts = [];
let filteredAccounts = [];
let proxies = [];
let currentFilter = 'all';
let currentGroupFilter = 'all';
let currentSearchQuery = '';

document.addEventListener('DOMContentLoaded', async () => {
    await Promise.all([loadInitialData(), loadGroups()]);
    setupEventListeners();
    renderAccounts();
});

async function loadInitialData() {
    try {
        showLoading();
        const [accountsData, proxiesData] = await Promise.all([
            api.getAccounts(),
            api.getProxies()
        ]);
        
        accounts = accountsData;
        proxies = proxiesData;
        filteredAccounts = [...accounts];
        
        hideLoading();
        
        console.log('Loaded accounts:', accounts.length);
        console.log('Loaded proxies:', proxies.length);
        
    } catch (error) {
        console.error('Error loading initial data:', error);
        hideLoading();
        showNotification('Ошибка при загрузке данных: ' + error.message, 'error');
    }
}

async function loadGroups() {
    try {
        // Load groups for filters and selectors
        groups = await getGroups();
        populateGroupSelectors();
        populateGroupsList();
    } catch (error) {
        console.error('Error loading groups:', error);
    }
}

function populateGroupSelectors() {
    const selectors = ['account-group', 'bulk-group', 'group-filter', 'target-group'];
    selectors.forEach(id => {
        const select = document.getElementById(id);
        if (select) {
            const baseOptions = id === 'group-filter' ? 
                '<option value="">Все группы</option>' : 
                id === 'target-group' ? 
                '<option value="">Удалить из группы</option>' :
                '<option value="">Без группы</option>';
            
            select.innerHTML = baseOptions + groups.map(group => 
                `<option value="${group.id}">${group.name}</option>`
            ).join('');
        }
    });
}

function populateAccountSelectors() {
    const select = document.getElementById('accounts-to-assign');
    if (select) {
        select.innerHTML = accounts.map(account => 
            `<option value="${account.id}">@${account.username} ${account.group_name ? `(${account.group_name})` : ''}</option>`
        ).join('');
    }
}

function populateGroupsList() {
    const list = document.getElementById('groups-list');
    if (!list) return;
    
    if (groups.length === 0) {
        list.innerHTML = '<p class="text-slate-400 text-sm">Нет созданных групп</p>';
        return;
    }
    
    list.innerHTML = groups.map(group => `
        <div class="flex items-center justify-between bg-slate-600 p-3 rounded">
            <div>
                <div class="text-white font-medium">${group.name}</div>
                <div class="text-slate-300 text-sm">${group.description || 'Без описания'}</div>
                <div class="text-slate-400 text-xs">${group.accounts_count || 0} аккаунтов</div>
            </div>
            <div class="flex gap-2">
                <button onclick="editGroup(${group.id})" class="p-1 text-slate-400 hover:text-white transition-colors">
                    <i data-lucide="edit" class="h-4 w-4"></i>
                </button>
                <button onclick="deleteGroup(${group.id})" class="p-1 text-slate-400 hover:text-red-400 transition-colors">
                    <i data-lucide="trash-2" class="h-4 w-4"></i>
                </button>
            </div>
        </div>
    `).join('');
    lucide.createIcons();
}

function renderAccounts() {
    const grid = document.getElementById('accounts-grid');
    let filteredAccounts = filterAccountsByTab(accounts, currentTab);
    
    // Apply group filter
    const groupFilter = document.getElementById('group-filter')?.value;
    if (groupFilter) {
        filteredAccounts = filteredAccounts.filter(acc => acc.group_id == groupFilter);
    }
    
    if (filteredAccounts.length === 0) {
        grid.innerHTML = `
            <div class="col-span-full text-center py-12">
                <i data-lucide="users" class="h-12 w-12 text-slate-400 mx-auto mb-4"></i>
                <h3 class="text-lg font-medium text-white mb-2">Нет аккаунтов</h3>
                <p class="text-slate-400 mb-4">Добавьте первый аккаунт для начала работы</p>
                <button onclick="openAddAccountModal()" class="bg-blue-600 hover:bg-blue-700 px-4 py-2 rounded-md text-white transition-colors">
                    Добавить аккаунт
                </button>
            </div>
        `;
        lucide.createIcons();
        return;
    }

    const html = filteredAccounts.map(account => `
        <div class="bg-slate-800/50 border border-slate-700 rounded-lg p-6 card-hover ${selectedAccounts.includes(account.id) ? 'ring-2 ring-blue-500' : ''}">
            <div class="flex items-center justify-between">
                <div class="flex items-center gap-4">
                    <input type="checkbox" ${selectedAccounts.includes(account.id) ? 'checked' : ''} 
                           onchange="toggleAccountSelection(${account.id})"
                           class="text-blue-600">
                    <div class="w-12 h-12 bg-gradient-to-r from-blue-500 to-purple-500 rounded-full flex items-center justify-center">
                        <span class="text-white font-bold">${account.username.charAt(0).toUpperCase()}</span>
                    </div>
                    <div>
                        <h3 class="text-lg font-semibold text-white">@${account.username}</h3>
                        <p class="text-slate-400">${account.email || 'Нет email'}</p>
                        <div class="flex items-center gap-2 mt-1">
                            <span class="inline-block px-2 py-1 text-xs rounded ${
                                account.is_active ? 'bg-green-600' : 'bg-red-600'
                            } text-white">
                                ${account.is_active ? 'Активен' : 'Неактивен'}
                            </span>
                            ${account.group_name ? 
                                `<span class="inline-block px-2 py-1 text-xs rounded bg-blue-600 text-white">
                                    ${account.group_name}
                                </span>` : ''
                            }
                        </div>
                    </div>
                </div>
                
                <div class="flex items-center gap-2">
                    <button onclick="editAccount(${account.id})" class="p-2 text-slate-400 hover:text-white transition-colors">
                        <i data-lucide="edit" class="h-4 w-4"></i>
                    </button>
                    <button onclick="handleDeleteAccount(${account.id})" class="p-2 text-slate-400 hover:text-red-400 transition-colors">
                        <i data-lucide="trash-2" class="h-4 w-4"></i>
                    </button>
                    <button onclick="viewAccountDetails(${account.id})" class="px-3 py-1 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded transition-colors">
                        Подробнее
                    </button>
                </div>
            </div>
        </div>
    `).join('');
    
    grid.innerHTML = html;
    lucide.createIcons();
}

function filterAccountsByTab(accounts, tab) {
    switch (tab) {
        case 'active':
            return accounts.filter(acc => acc.is_active);
        case 'inactive':
            return accounts.filter(acc => !acc.is_active);
        default:
            return accounts;
    }
}

function filterByGroup() {
    renderAccounts();
}

function toggleAccountSelection(accountId) {
    if (selectedAccounts.includes(accountId)) {
        selectedAccounts = selectedAccounts.filter(id => id !== accountId);
    } else {
        selectedAccounts.push(accountId);
    }
    renderAccounts();
}

function selectAllInGroup() {
    const groupFilter = document.getElementById('group-filter')?.value;
    let accountsToSelect = filterAccountsByTab(accounts, currentTab);
    
    if (groupFilter) {
        accountsToSelect = accountsToSelect.filter(acc => acc.group_id == groupFilter);
    }
    
    selectedAccounts = [...new Set([...selectedAccounts, ...accountsToSelect.map(acc => acc.id)])];
    renderAccounts();
}

function clearSelection() {
    selectedAccounts = [];
    renderAccounts();
}

function updateCounts() {
    const total = accounts.length;
    const active = accounts.filter(acc => acc.is_active).length;
    const inactive = total - active;
    
    document.getElementById('total-count').textContent = total;
    document.getElementById('active-count').textContent = active;
    document.getElementById('inactive-count').textContent = inactive;
}

function switchTab(tab) {
    currentTab = tab;
    
    // Update tab buttons
    document.querySelectorAll('button[onclick^="switchTab"]').forEach(btn => {
        btn.className = btn.className.replace('bg-blue-600 text-white', 'text-slate-400 hover:text-white');
    });
    
    const activeBtn = document.querySelector(`button[onclick="switchTab('${tab}')"]`);
    if (activeBtn) {
        activeBtn.className = activeBtn.className.replace('text-slate-400 hover:text-white', 'bg-blue-600 text-white');
    }
    
    renderAccounts();
}

// Close modals
function closeModals() {
    // Закрываем все модальные окна
    const modals = [
        'add-account-modal',
        'bulk-add-modal', 
        'group-manager-modal',
        'importModal'
    ];
    
    modals.forEach(modalId => {
        const modal = document.getElementById(modalId);
        if (modal) {
            modal.classList.add('hidden');
            modal.classList.remove('flex');
        }
    });
}

// Функции для открытия/закрытия конкретных модалей
function openAddAccountModal() {
    const modal = document.getElementById('add-account-modal');
    if (modal) {
        modal.classList.remove('hidden');
        modal.classList.add('flex');
        
        // Reset form
        const form = document.getElementById('add-account-form');
        if (form) {
            form.reset();
        }
    }
}

function closeAddAccountModal() {
    const modal = document.getElementById('add-account-modal');
    if (modal) {
        modal.classList.add('hidden');
        modal.classList.remove('flex');
    }
}

function openBulkAddModal() {
    const modal = document.getElementById('bulk-add-modal');
    if (modal) {
        modal.classList.remove('hidden');
        modal.classList.add('flex');
        
        // Reset form
        const form = document.getElementById('bulk-add-form');
        if (form) {
            form.reset();
        }
    }
}

function closeBulkAddModal() {
    const modal = document.getElementById('bulk-add-modal');
    if (modal) {
        modal.classList.add('hidden');
        modal.classList.remove('flex');
    }
}

function openGroupManagerModal() {
    const modal = document.getElementById('group-manager-modal');
    if (modal) {
        modal.classList.remove('hidden');
        modal.classList.add('flex');
        populateAccountSelectors();
        populateGroupsList();
    }
}

function closeGroupManagerModal() {
    const modal = document.getElementById('group-manager-modal');
    if (modal) {
        modal.classList.add('hidden');
        modal.classList.remove('flex');
    }
}

async function addAccountOld(event) {
    event.preventDefault();
    
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    const email = document.getElementById('email').value;
    const emailPassword = document.getElementById('email-password').value;
    const groupId = document.getElementById('account-group').value;
    
    try {
        // Use the new addAccount function from api.js
        const accountData = {
            username: username,
            password: password,
            email: email || '',
            email_password: emailPassword || '',
            full_name: '',
            biography: ''
        };
        
        await addAccount(accountData);
        closeAddAccountModal();
        await loadInitialData();
        alert('Аккаунт успешно добавлен!');
    } catch (error) {
        alert('Ошибка при добавлении аккаунта: ' + error.message);
    }
}

async function handleBulkAddAccounts(event) {
    event.preventDefault();
    
    const data = document.getElementById('bulk-accounts-data').value.trim();
    const groupId = document.getElementById('bulk-group').value;
    const validateAccounts = document.getElementById('validate-accounts').checked;
    const parallelThreads = parseInt(document.getElementById('parallel-threads').value) || 2;
    
    if (!data) {
        alert('Введите данные аккаунтов');
        return;
    }
    
    try {
        let accountsData;
        
        // Try to parse as JSON first
        if (data.startsWith('[') || data.startsWith('{')) {
            accountsData = JSON.parse(data);
        } else {
            // Parse as line-separated format
            accountsData = data.split('\n').filter(line => line.trim()).map(line => {
                const parts = line.split(':');
                return {
                    username: parts[0],
                    password: parts[1],
                    email: parts[2] || null,
                    email_password: parts[3] || null
                };
            });
        }
        
        // Добавляем параметр количества потоков
        await bulkAddAccounts(accountsData, groupId, validateAccounts, parallelThreads);
        closeBulkAddModal();
        await loadInitialData();
        alert(`Успешно запущено добавление ${accountsData.length} аккаунтов в ${parallelThreads} потоке(ах)!`);
    } catch (error) {
        alert('Ошибка при массовом добавлении: ' + error.message);
    }
}

async function createGroup(event) {
    event.preventDefault();
    
    const name = document.getElementById('new-group-name').value;
    const description = document.getElementById('new-group-description').value;
    
    try {
        await createGroup(name, description);
        document.getElementById('new-group-name').value = '';
        document.getElementById('new-group-description').value = '';
        await loadGroups();
        alert('Группа успешно создана!');
    } catch (error) {
        alert('Ошибка при создании группы: ' + error.message);
    }
}

async function assignToGroup() {
    const accountIds = Array.from(document.getElementById('accounts-to-assign').selectedOptions).map(opt => opt.value);
    const groupId = document.getElementById('target-group').value || null;
    
    if (accountIds.length === 0) {
        alert('Выберите аккаунты для назначения');
        return;
    }
    
    try {
        await assignAccountsToGroup(accountIds, groupId);
        await Promise.all([loadInitialData(), loadGroups()]);
        alert('Аккаунты успешно назначены в группу!');
    } catch (error) {
        alert('Ошибка при назначении в группу: ' + error.message);
    }
}

function editGroup(groupId) {
    alert(`Редактирование группы ${groupId} (будет реализовано)`);
}

async function deleteGroup(groupId) {
    if (!confirm('Удалить эту группу? Аккаунты останутся, но будут удалены из группы.')) return;
    
    try {
        await deleteGroup(groupId);
        await Promise.all([loadInitialData(), loadGroups()]);
        alert('Группа удалена');
    } catch (error) {
        alert('Ошибка при удалении группы: ' + error.message);
    }
}

function editAccount(accountId) {
    alert(`Редактирование аккаунта ${accountId} (будет реализовано)`);
}

async function handleDeleteAccount(accountId) {
    if (!confirm('Удалить этот аккаунт?')) return;
    
    try {
        await deleteAccount(accountId);
        await loadInitialData();
        alert('Аккаунт удален');
    } catch (error) {
        alert('Ошибка при удалении: ' + error.message);
    }
}

function viewAccountDetails(accountId) {
    alert(`Детали аккаунта ${accountId} (будет реализовано)`);
}

function openImportModal() {
    alert('Импорт CSV (будет реализовано)');
}

function showLoading() {
    document.getElementById('accounts-grid').innerHTML = `
        <div class="col-span-full">
            <div class="skeleton h-24 rounded-lg mb-4"></div>
            <div class="skeleton h-24 rounded-lg mb-4"></div>
            <div class="skeleton h-24 rounded-lg"></div>
        </div>
    `;
}

function hideLoading() {
    // Loading will be replaced by renderAccounts()
}

// Search functionality
document.getElementById('search-input').addEventListener('input', (e) => {
    const searchTerm = e.target.value.toLowerCase();
    const filtered = accounts.filter(account => 
        account.username.toLowerCase().includes(searchTerm) ||
        account.id.toString().includes(searchTerm) ||
        (account.email && account.email.toLowerCase().includes(searchTerm)) ||
        (account.group_name && account.group_name.toLowerCase().includes(searchTerm))
    );
    
    const grid = document.getElementById('accounts-grid');
    const html = filtered.map(account => `
        <div class="bg-slate-800/50 border border-slate-700 rounded-lg p-6 card-hover ${selectedAccounts.includes(account.id) ? 'ring-2 ring-blue-500' : ''}">
            <div class="flex items-center justify-between">
                <div class="flex items-center gap-4">
                    <input type="checkbox" ${selectedAccounts.includes(account.id) ? 'checked' : ''} 
                           onchange="toggleAccountSelection(${account.id})"
                           class="text-blue-600">
                    <div class="w-12 h-12 bg-gradient-to-r from-blue-500 to-purple-500 rounded-full flex items-center justify-center">
                        <span class="text-white font-bold">${account.username.charAt(0).toUpperCase()}</span>
                    </div>
                    <div>
                        <h3 class="text-lg font-semibold text-white">@${account.username}</h3>
                        <p class="text-slate-400">${account.email || 'Нет email'}</p>
                        <div class="flex items-center gap-2 mt-1">
                            <span class="inline-block px-2 py-1 text-xs rounded ${
                                account.is_active ? 'bg-green-600' : 'bg-red-600'
                            } text-white">
                                ${account.is_active ? 'Активен' : 'Неактивен'}
                            </span>
                            ${account.group_name ? 
                                `<span class="inline-block px-2 py-1 text-xs rounded bg-blue-600 text-white">
                                    ${account.group_name}
                                </span>` : ''
                            }
                        </div>
                    </div>
                </div>
                
                <div class="flex items-center gap-2">
                    <button onclick="editAccount(${account.id})" class="p-2 text-slate-400 hover:text-white transition-colors">
                        <i data-lucide="edit" class="h-4 w-4"></i>
                    </button>
                    <button onclick="handleDeleteAccount(${account.id})" class="p-2 text-slate-400 hover:text-red-400 transition-colors">
                        <i data-lucide="trash-2" class="h-4 w-4"></i>
                    </button>
                    <button onclick="viewAccountDetails(${account.id})" class="px-3 py-1 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded transition-colors">
                        Подробнее
                    </button>
                </div>
            </div>
        </div>
    `).join('');
    
    grid.innerHTML = html || '<div class="col-span-full text-center py-12 text-slate-400">Ничего не найдено</div>';
    lucide.createIcons();
});

// Setup event listeners
function setupEventListeners() {
    // Search functionality
    const searchInput = document.getElementById('search-accounts');
    if (searchInput) {
        searchInput.addEventListener('input', handleSearch);
    }

    // Filter buttons
    const filterButtons = document.querySelectorAll('[data-filter]');
    filterButtons.forEach(button => {
        button.addEventListener('click', (e) => {
            const filter = e.target.getAttribute('data-filter');
            setFilter(filter);
        });
    });

    // Add account button
    const addAccountBtn = document.getElementById('add-account-btn');
    if (addAccountBtn) {
        addAccountBtn.addEventListener('click', showAddAccountModal);
    }

    // Bulk add button
    const bulkAddBtn = document.getElementById('bulk-add-btn');
    if (bulkAddBtn) {
        bulkAddBtn.addEventListener('click', showBulkAddModal);
    }

    // Modal close buttons
    document.querySelectorAll('[data-modal-close]').forEach(button => {
        button.addEventListener('click', closeModals);
    });

    // Form submissions
    const addAccountForm = document.getElementById('add-account-form');
    if (addAccountForm) {
        addAccountForm.addEventListener('submit', handleAddAccount);
    }

    const bulkAddForm = document.getElementById('bulk-add-form');
    if (bulkAddForm) {
        bulkAddForm.addEventListener('submit', handleBulkAdd);
    }

    // Bulk format toggle
    const formatToggle = document.getElementById('bulk-format');
    if (formatToggle) {
        formatToggle.addEventListener('change', updateBulkFormatExample);
    }
}

// Handle search
function handleSearch(e) {
    currentSearchQuery = e.target.value.toLowerCase();
    applyFilters();
}

// Set filter
function setFilter(filter) {
    currentFilter = filter;
    
    // Update active filter button
    document.querySelectorAll('[data-filter]').forEach(btn => {
        btn.classList.remove('bg-blue-600', 'text-white');
        btn.classList.add('bg-slate-700', 'text-slate-300');
    });
    
    const activeBtn = document.querySelector(`[data-filter="${filter}"]`);
    if (activeBtn) {
        activeBtn.classList.remove('bg-slate-700', 'text-slate-300');
        activeBtn.classList.add('bg-blue-600', 'text-white');
    }
    
    applyFilters();
}

// Apply filters
function applyFilters() {
    filteredAccounts = accounts.filter(account => {
        // Status filter
        let statusMatch = true;
        if (currentFilter === 'active') {
            statusMatch = account.is_active;
        } else if (currentFilter === 'inactive') {
            statusMatch = !account.is_active;
        } else if (currentFilter === 'with-proxy') {
            statusMatch = account.proxy_id !== null;
        } else if (currentFilter === 'without-proxy') {
            statusMatch = account.proxy_id === null;
        }

        // Search filter
        let searchMatch = true;
        if (currentSearchQuery) {
            searchMatch = account.username.toLowerCase().includes(currentSearchQuery) ||
                         (account.email && account.email.toLowerCase().includes(currentSearchQuery)) ||
                         (account.full_name && account.full_name.toLowerCase().includes(currentSearchQuery));
        }

        return statusMatch && searchMatch;
    });

    renderAccounts();
}

// Show add account modal
function showAddAccountModal() {
    const modal = document.getElementById('add-account-modal');
    if (modal) {
        modal.classList.remove('hidden');
        modal.classList.add('flex');
        
        // Reset form
        const form = document.getElementById('add-account-form');
        if (form) {
            form.reset();
        }
    }
}

// Show bulk add modal
function showBulkAddModal() {
    const modal = document.getElementById('bulk-add-modal');
    if (modal) {
        modal.classList.remove('hidden');
        modal.classList.add('flex');
        
        // Reset form
        const form = document.getElementById('bulk-add-form');
        if (form) {
            form.reset();
        }
        
        updateBulkFormatExample();
    }
}

// Handle add account form submission
async function handleAddAccount(e) {
    e.preventDefault();
    
    console.log('handleAddAccount called'); // Отладка
    
    const formData = new FormData(e.target);
    const accountData = {
        username: formData.get('username'),
        password: formData.get('password'),
        email: formData.get('email') || '',
        email_password: formData.get('email_password') || '',
        full_name: formData.get('full_name') || '',
        biography: formData.get('biography') || ''
    };

    console.log('Account data:', accountData); // Отладка

    try {
        // Show loading state
        const submitBtn = e.target.querySelector('button[type="submit"]');
        const originalText = submitBtn.textContent;
        submitBtn.textContent = 'Добавление...';
        submitBtn.disabled = true;

        // Add account
        const result = await addAccount(accountData);
        
        console.log('Add account result:', result); // Отладка
        
        // Если аккаунт обрабатывается асинхронно, показываем дополнительную информацию
        if (result.processing) {
            showNotification('Начата фоновая обработка аккаунта. Следите за уведомлениями!', 'info');
        }
        
        // Reload accounts to show the new account (even if it's still processing)
        await loadInitialData();
        renderAccounts();
        
        // Close modal
        closeModals();
        
        // Reset form
        e.target.reset();
        
    } catch (error) {
        console.error('Error adding account:', error);
        showNotification('Ошибка при добавлении аккаунта: ' + error.message, 'error');
    } finally {
        // Reset button
        const submitBtn = e.target.querySelector('button[type="submit"]');
        if (submitBtn) {
            submitBtn.textContent = 'Добавить';
            submitBtn.disabled = false;
        }
    }
}

// Handle bulk add form submission
async function handleBulkAdd(e) {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const accountsText = formData.get('accounts_text');
    const format = formData.get('format');

    if (!accountsText.trim()) {
        showNotification('Введите данные аккаунтов', 'error');
        return;
    }

    try {
        // Show loading state
        const submitBtn = e.target.querySelector('button[type="submit"]');
        const originalText = submitBtn.textContent;
        submitBtn.textContent = 'Добавление...';
        submitBtn.disabled = true;

        // Parse accounts
        const accountsData = parseAccountsFromText(accountsText, format);
        
        if (accountsData.length === 0) {
            showNotification('Не удалось распознать аккаунты. Проверьте формат данных.', 'error');
            return;
        }

        // Show immediate feedback
        showNotification(`Начинаем добавление ${accountsData.length} аккаунтов...`, 'info');

        // Bulk add accounts
        const result = await bulkAddAccounts(accountsData);
        
        // Если аккаунты обрабатываются асинхронно, показываем дополнительную информацию
        if (result.processing) {
            showNotification('Аккаунты добавляются в фоновом режиме. Вскоре они появятся в панели!', 'info');
        } else {
            // Показываем результаты синхронной обработки
            const results = result.data;
            if (results.success.length > 0) {
                showNotification(`Успешно добавлено ${results.success.length} аккаунтов`, 'success');
            }
            
            if (results.failed.length > 0) {
                showNotification(`Не удалось добавить ${results.failed.length} аккаунтов`, 'warning');
            }
        }
        
        // Reload accounts
        await loadInitialData();
        renderAccounts();
        
        // Close modal
        closeModals();
        
        // Reset form
        e.target.reset();
        
    } catch (error) {
        console.error('Error bulk adding accounts:', error);
        showNotification('Ошибка при массовом добавлении: ' + error.message, 'error');
    } finally {
        // Reset button
        const submitBtn = e.target.querySelector('button[type="submit"]');
        if (submitBtn) {
            submitBtn.textContent = 'Добавить аккаунты';
            submitBtn.disabled = false;
        }
    }
}

// Update bulk format example
function updateBulkFormatExample() {
    const format = document.getElementById('bulk-format')?.value;
    const example = document.getElementById('format-example');
    
    if (!example) return;
    
    if (format === 'colon') {
        example.textContent = 'username1:password1:email1@example.com:emailpass1\nusername2:password2:email2@example.com:emailpass2';
    } else if (format === 'json') {
        example.textContent = '{"username":"user1","password":"pass1","email":"user1@example.com"}\n{"username":"user2","password":"pass2","email":"user2@example.com"}';
    }
}

// Edit account
async function editAccount(accountId) {
    const account = accounts.find(acc => acc.id === accountId);
    if (!account) return;

    // For now, show a simple prompt - later can be replaced with a proper modal
    const newFullName = prompt('Введите новое полное имя:', account.full_name || '');
    if (newFullName === null) return; // User cancelled

    const newBiography = prompt('Введите новое описание:', account.biography || '');
    if (newBiography === null) return; // User cancelled

    try {
        await updateAccount(accountId, {
            full_name: newFullName,
            biography: newBiography
        });
        
        // Reload accounts
        await loadInitialData();
        renderAccounts();
        
    } catch (error) {
        console.error('Error updating account:', error);
    }
}

// Assign proxy to account
async function assignProxy(accountId) {
    if (proxies.length === 0) {
        showNotification('Сначала добавьте прокси', 'warning');
        return;
    }

    // Create proxy selection modal (simple version)
    const proxyOptions = proxies.map(proxy => 
        `<option value="${proxy.id}">${proxy.host}:${proxy.port} (${proxy.protocol})</option>`
    ).join('');

    const proxySelect = document.createElement('select');
    proxySelect.innerHTML = `<option value="">Выберите прокси</option>${proxyOptions}`;
    proxySelect.className = 'w-full p-2 bg-slate-700 border border-slate-600 rounded text-white';

    const modal = document.createElement('div');
    modal.className = 'fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50';
    modal.innerHTML = `
        <div class="bg-slate-800 p-6 rounded-lg max-w-md w-full mx-4">
            <h3 class="text-lg font-semibold text-white mb-4">Назначить прокси</h3>
            <div class="mb-4">
                ${proxySelect.outerHTML}
            </div>
            <div class="flex space-x-3">
                <button onclick="this.closest('.fixed').remove()" class="flex-1 bg-slate-600 hover:bg-slate-700 text-white px-4 py-2 rounded transition-colors">
                    Отмена
                </button>
                <button onclick="confirmAssignProxy(${accountId}, this)" class="flex-1 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded transition-colors">
                    Назначить
                </button>
            </div>
        </div>
    `;

    document.body.appendChild(modal);
}

// Confirm proxy assignment
async function confirmAssignProxy(accountId, button) {
    const modal = button.closest('.fixed');
    const select = modal.querySelector('select');
    const proxyId = select.value;

    if (!proxyId) {
        showNotification('Выберите прокси', 'error');
        return;
    }

    try {
        await assignProxyToAccount(accountId, parseInt(proxyId));
        
        // Reload accounts
        await loadInitialData();
        renderAccounts();
        
        // Close modal
        modal.remove();
        
    } catch (error) {
        console.error('Error assigning proxy:', error);
    }
}

// Export functions for global access
window.editAccount = editAccount;
window.assignProxy = assignProxy;
window.handleDeleteAccount = handleDeleteAccount;
window.confirmAssignProxy = confirmAssignProxy; 