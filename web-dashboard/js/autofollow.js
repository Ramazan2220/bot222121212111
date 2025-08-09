// Autofollow functionality
let followTasks = [];
let availableAccounts = [];
let selectedFollowAccounts = [];
let refreshInterval = null;

// Проверяем, что все необходимые объекты загружены
if (typeof api === 'undefined') {
    console.error('API объект не найден. Убедитесь, что api.js загружен.');
}

// Initialize page
document.addEventListener('DOMContentLoaded', async () => {
    await loadFollowTasks();
    await loadAccounts();
    await loadFollowStats();
    setupEventListeners();
    updateStats();
    
    // Auto-refresh every 10 seconds
    refreshInterval = setInterval(async () => {
        await loadFollowTasks();
        await loadFollowStats();
        await updateStats();
    }, 10000);
});

// Делаем функции глобальными для onclick обработчиков
window.createFollowTask = async function() {
    console.log('createFollowTask вызвана');
    // Сбрасываем выбранные аккаунты при открытии модального окна
    selectedFollowAccounts = [];
    updateSelectedAccountsDisplay();
    
    // Показываем модальное окно
    document.getElementById('createTaskModal').classList.remove('hidden');
    
    // Загружаем аккаунты
    await loadAccounts();
}

window.closeCreateTaskModal = function() {
    document.getElementById('createTaskModal').classList.add('hidden');
    document.getElementById('createTaskForm').reset();
    // Очищаем выбранные аккаунты
    selectedFollowAccounts = [];
    updateSelectedAccountsDisplay();
    // Скрываем dropdown если он открыт
    document.getElementById('account-selection-dropdown').classList.add('hidden');
}

// Alias для обратной совместимости с HTML
window.closeCreateModal = window.closeCreateTaskModal;

async function loadAccounts() {
    try {
        const accounts = await api.getAccounts();
        availableAccounts = accounts.filter(acc => acc.is_active);
        renderAccountsList();
        updateSelectedAccountsDisplay();
    } catch (error) {
        console.error('Error loading accounts:', error);
        availableAccounts = [];
    }
}

function renderAccountsList() {
    const accountsList = document.getElementById('accounts-list');
    if (!accountsList) return;
    
    accountsList.innerHTML = availableAccounts.map(account => `
        <label class="flex items-center gap-2 p-2 hover:bg-slate-600 rounded cursor-pointer">
            <input type="checkbox" 
                   value="${account.id}" 
                   ${selectedFollowAccounts.includes(account.id.toString()) ? 'checked' : ''}
                   onchange="toggleFollowAccount('${account.id}')"
                   class="h-4 w-4 text-blue-600">
            <span class="text-white">@${account.username}</span>
        </label>
    `).join('');
}

window.toggleAccountSelection = function() {
    const dropdown = document.getElementById('account-selection-dropdown');
    dropdown.classList.toggle('hidden');
    
    if (!dropdown.classList.contains('hidden')) {
        // Close dropdown when clicking outside
        setTimeout(() => {
            document.addEventListener('click', closeAccountDropdown);
        }, 100);
    }
}

function closeAccountDropdown(event) {
    const dropdown = document.getElementById('account-selection-dropdown');
    const button = event.target.closest('button[onclick*="toggleAccountSelection"]');
    
    if (!dropdown.classList.contains('hidden') && !dropdown.contains(event.target) && !button) {
        dropdown.classList.add('hidden');
        document.removeEventListener('click', closeAccountDropdown);
    }
}

window.toggleFollowAccount = function(accountId) {
    const idStr = accountId.toString();
    const index = selectedFollowAccounts.indexOf(idStr);
    
    if (index > -1) {
        selectedFollowAccounts.splice(index, 1);
    } else {
        selectedFollowAccounts.push(idStr);
    }
    
    updateSelectedAccountsDisplay();
}

window.selectAllFollowAccounts = function() {
    selectedFollowAccounts = availableAccounts.map(acc => acc.id.toString());
    renderAccountsList();
    updateSelectedAccountsDisplay();
}

window.deselectAllFollowAccounts = function() {
    selectedFollowAccounts = [];
    renderAccountsList();
    updateSelectedAccountsDisplay();
}

function updateSelectedAccountsDisplay() {
    const display = document.getElementById('selected-accounts-display');
    if (!display) return;
    
    if (selectedFollowAccounts.length === 0) {
        display.innerHTML = '<span class="text-slate-400 text-sm">Выберите аккаунты для работы...</span>';
    } else {
        const selectedAccountObjs = availableAccounts.filter(acc => 
            selectedFollowAccounts.includes(acc.id.toString())
        );
        
        display.innerHTML = selectedAccountObjs.map(account => `
            <span class="inline-flex items-center gap-1 px-3 py-1 bg-blue-600 text-white rounded-full text-sm">
                @${account.username}
                <button type="button" onclick="removeFollowAccount('${account.id}')" class="ml-1 hover:text-red-200">
                    <i data-lucide="x" class="h-3 w-3"></i>
                </button>
            </span>
        `).join('');
        
        // Обновляем иконки Lucide
        if (typeof lucide !== 'undefined') {
            lucide.createIcons();
        }
    }
    
    // Обновляем информацию о режиме задач
    if (typeof updateTaskModeInfo === 'function') {
        updateTaskModeInfo();
    }
}

window.removeFollowAccount = function(accountId) {
    const index = selectedFollowAccounts.indexOf(accountId.toString());
    if (index > -1) {
        selectedFollowAccounts.splice(index, 1);
        renderAccountsList();
        updateSelectedAccountsDisplay();
    }
}

window.filterFollowAccounts = function() {
    const searchTerm = document.getElementById('account-search').value.toLowerCase();
    const accountItems = document.querySelectorAll('#accounts-list label');
    
    accountItems.forEach(item => {
        const username = item.textContent.toLowerCase();
        if (username.includes(searchTerm)) {
            item.style.display = '';
        } else {
            item.style.display = 'none';
        }
    });
}

// Account selection functions for source input change
window.toggleSourceInput = function() {
    const sourceType = document.getElementById('source-type').value;
    const sourceInput = document.getElementById('source-input');
    
    if (sourceType) {
        sourceInput.classList.remove('hidden');
        
        const input = document.getElementById('source-value');
        switch(sourceType) {
            case 'followers':
            case 'following':
                input.placeholder = '@username';
                break;
            case 'hashtag':
                input.placeholder = '#hashtag';
                break;
            case 'location':
                input.placeholder = 'Название локации';
                break;
            case 'likers':
            case 'commenters':
                input.placeholder = 'URL поста';
                break;
        }
    } else {
        sourceInput.classList.add('hidden');
    }
}

// Task management functions - делаем глобальными для onclick
window.pauseTask = async function(taskId) {
    try {
        await api.updateFollowTask(taskId, { status: 'paused' });
        showNotification('Задача приостановлена', 'success');
        await loadFollowTasks();
    } catch (error) {
        console.error('Error pausing task:', error);
        showNotification('Ошибка при приостановке задачи', 'error');
    }
}

window.resumeTask = async function(taskId) {
    try {
        await api.updateFollowTask(taskId, { status: 'running' });
        showNotification('Задача возобновлена', 'success');
        await loadFollowTasks();
    } catch (error) {
        console.error('Error resuming task:', error);
        showNotification('Ошибка при возобновлении задачи', 'error');
    }
}

window.stopTask = async function(taskId) {
    if (!confirm('Остановить задачу? Это действие нельзя отменить.')) return;
    
    try {
        await api.updateFollowTask(taskId, { status: 'stopped' });
        showNotification('Задача остановлена', 'success');
        await loadFollowTasks();
    } catch (error) {
        console.error('Error stopping task:', error);
        showNotification('Ошибка при остановке задачи', 'error');
    }
}

window.deleteTask = async function(taskId) {
    if (!confirm('Удалить задачу? Это действие нельзя отменить.')) return;
    
    try {
        await api.deleteFollowTask(taskId);
        showNotification('Задача удалена', 'success');
        await loadFollowTasks();
        await loadFollowStats();
    } catch (error) {
        console.error('Error deleting task:', error);
        showNotification('Ошибка при удалении задачи', 'error');
    }
}

window.stopAllTasks = async function() {
    const activeTasks = followTasks.filter(task => 
        task.status === 'running' || task.status === 'pending' || task.status === 'paused'
    );
    
    if (activeTasks.length === 0) {
        showNotification('Нет активных задач для остановки', 'info');
        return;
    }
    
    if (!confirm(`Остановить все активные задачи (${activeTasks.length} шт.)? Это действие нельзя отменить.`)) return;
    
    try {
        // Показываем уведомление о процессе
        showNotification(`Останавливаем ${activeTasks.length} задач...`, 'info');
        
        // Останавливаем все задачи одним запросом
        const response = await api.stopAllFollowTasks();
        
        if (response.success) {
            showNotification(`Успешно остановлено ${response.stopped_count} задач`, 'success');
        } else {
            showNotification('Ошибка при остановке задач', 'error');
        }
        
        // Обновляем интерфейс
        await loadFollowTasks();
        await loadFollowStats();
        
    } catch (error) {
        console.error('Error stopping all tasks:', error);
        showNotification('Ошибка при остановке задач: ' + (error.message || 'Неизвестная ошибка'), 'error');
    }
}

window.startTask = async function(taskId) {
    try {
        await api.updateFollowTask(taskId, { status: 'running' });
        showNotification('Задача запущена', 'success');
        await loadFollowTasks();
    } catch (error) {
        console.error('Error starting task:', error);
        showNotification('Ошибка при запуске задачи', 'error');
    }
}

async function loadFollowTasks() {
    try {
        const response = await api.getFollowTasks();
        followTasks = response.tasks || [];
        updateTasksTable();
        updateActiveTasksCount();
        
        if (response.error) {
            console.error('Server error:', response.error);
        }
    } catch (error) {
        console.error('Error loading follow tasks:', error);
        showNotification('Ошибка при загрузке задач', 'error');
    }
}

function updateTasksTable() {
    const tbody = document.getElementById('tasks-table-body');
    if (!tbody) return;
    
    if (followTasks.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="px-6 py-12 text-center text-slate-400">
                    <i data-lucide="inbox" class="h-12 w-12 mx-auto mb-3 text-slate-600"></i>
                    <p>Нет активных задач</p>
                    <button onclick="createFollowTask()" class="mt-3 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md transition-colors text-sm">
                        Создать первую задачу
                    </button>
                </td>
            </tr>
        `;
    } else {
        tbody.innerHTML = followTasks.map(task => {
            const statusColors = {
                'pending': 'bg-yellow-500/20 text-yellow-400',
                'running': 'bg-green-500/20 text-green-400',
                'paused': 'bg-blue-500/20 text-blue-400',
                'completed': 'bg-slate-500/20 text-slate-400',
                'failed': 'bg-red-500/20 text-red-400',
                'cancelled': 'bg-orange-500/20 text-orange-400'
            };
            
            const sourceTypeText = {
                'followers': 'Подписчики',
                'following': 'Подписки',
                'hashtag': 'Хештег',
                'location': 'Локация',
                'likers': 'Лайки',
                'commenters': 'Комментарии'
            };
            
            const statusText = {
                'pending': 'Ожидание',
                'running': 'Активна',
                'paused': 'Пауза',
                'completed': 'Завершена',
                'failed': 'Ошибка',
                'cancelled': 'Отменена'
            };
            
            return `
                <tr class="border-b border-slate-700 hover:bg-slate-800 transition-colors">
                    <td class="px-6 py-4">
                        <div class="flex items-center gap-3">
                            <div>
                                <div class="font-medium text-white">${task.name}</div>
                                <div class="text-sm text-slate-400">@${task.account_username}</div>
                            </div>
                        </div>
                    </td>
                    <td class="px-6 py-4">
                        <div class="text-sm">
                            <div class="text-white">${sourceTypeText[task.source_type] || task.source_type}</div>
                            <div class="text-slate-400">${task.source_value}</div>
                        </div>
                    </td>
                    <td class="px-6 py-4">
                        <span class="px-2 py-1 text-xs rounded-full ${statusColors[task.status]}">
                            ${statusText[task.status] || task.status}
                        </span>
                    </td>
                    <td class="px-6 py-4">
                        <div class="flex items-center gap-2">
                            <div class="flex-1 bg-slate-700 rounded-full h-2 overflow-hidden">
                                <div class="bg-blue-600 h-full transition-all duration-300" 
                                     style="width: ${getProgressPercentage(task)}%"></div>
                            </div>
                            <span class="text-xs text-slate-400">
                                ${task.followed_count}/${task.follow_limit}
                            </span>
                        </div>
                    </td>
                    <td class="px-6 py-4 text-sm text-slate-300">
                        ${task.follows_per_hour}/час
                    </td>
                    <td class="px-6 py-4">
                        <div class="text-sm text-slate-400">
                            ${formatDateTime(task.created_at)}
                        </div>
                    </td>
                    <td class="px-6 py-4">
                        <div class="flex items-center gap-2">
                            ${getTaskActions(task)}
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    }
    
    // Update lucide icons
    if (typeof lucide !== 'undefined') {
        lucide.createIcons();
    }
}

async function loadFollowStats() {
    try {
        const stats = await api.getFollowStats();
        updateStatsCards(stats);
    } catch (error) {
        console.error('Error loading follow stats:', error);
    }
}

function updateStatsCards(stats) {
    // Active tasks
    const activeTasksEl = document.getElementById('active-tasks-count');
    if (activeTasksEl) {
        activeTasksEl.textContent = stats.active_tasks || 0;
    }
    
    // Total followed
    const totalFollowedEl = document.getElementById('total-followed');
    if (totalFollowedEl) {
        totalFollowedEl.textContent = formatNumber(stats.total_followed || 0);
    }
    
    // Success rate
    const successRateEl = document.getElementById('success-rate');
    if (successRateEl) {
        const rate = stats.success_rate || 0;
        successRateEl.textContent = `${rate.toFixed(1)}%`;
    }
    
    // Today's follows
    const todayFollowsEl = document.getElementById('today-follows');
    if (todayFollowsEl) {
        todayFollowsEl.textContent = formatNumber(stats.today_follows || 0);
    }
}

function updateActiveTasksCount() {
    const activeTasks = followTasks.filter(task => 
        task.status === 'running' || task.status === 'pending' || task.status === 'paused'
    );
    
    // Обновляем счетчик активных задач в статистике
    const activeTasksEl = document.getElementById('active-tasks-count');
    if (activeTasksEl) {
        activeTasksEl.textContent = activeTasks.length;
    }
    
    // Обновляем кнопку "Остановить все"
    const stopAllBtn = document.getElementById('stopAllTasksBtn');
    const activeCountSpan = document.getElementById('activeTasksCount');
    
    if (stopAllBtn && activeCountSpan) {
        if (activeTasks.length > 0) {
            stopAllBtn.classList.remove('hidden');
            activeCountSpan.textContent = activeTasks.length;
        } else {
            stopAllBtn.classList.add('hidden');
        }
    }
}

function setupEventListeners() {
    const form = document.getElementById('createTaskForm');
    if (form) {
        form.addEventListener('submit', handleCreateTask);
    }
}

// Handle create task form submission
async function handleCreateTask(event) {
    event.preventDefault();
    
    if (selectedFollowAccounts.length === 0) {
        showNotification('Выберите хотя бы один аккаунт', 'error');
        return;
    }
    
    // Get target accounts
    const targetAccountsText = document.getElementById('targetAccounts').value.trim();
    if (!targetAccountsText) {
        showNotification('Введите целевые аккаунты для подписки', 'error');
        return;
    }
    
    const targetAccounts = targetAccountsText.split('\n')
        .map(acc => acc.trim())
        .filter(acc => acc.length > 0)
        .map(acc => acc.startsWith('@') ? acc.substring(1) : acc);
    
    if (targetAccounts.length === 0) {
        showNotification('Введите хотя бы один целевой аккаунт', 'error');
        return;
    }
    
    // Определяем режим создания задач
    const taskMode = document.querySelector('input[name="taskMode"]:checked').value;
    
    const taskData = {
        name: document.getElementById('taskName').value,
        account_ids: selectedFollowAccounts.map(id => parseInt(id)),
        source_type: 'followers',  // Используем followers как тип для прямых подписок
        source_value: targetAccounts[0],  // Первый аккаунт из списка
        follows_per_hour: parseInt(document.getElementById('followsPerHour').value),
        follow_limit: parseInt(document.getElementById('followLimit').value),
        target_accounts: targetAccounts,  // Список целевых аккаунтов
        unique_follows: document.getElementById('uniqueFollows').checked,
        threads: parseInt(document.getElementById('threadsSlider').value),
        delay_min: parseInt(document.getElementById('delayMin').value),
        delay_max: parseInt(document.getElementById('delayMax').value),
        task_mode: taskMode,  // Добавляем режим создания задач
        filters: {
            skip_private: document.getElementById('skipPrivate').checked,
            skip_no_avatar: document.getElementById('skipNoAvatar').checked,
            business_only: document.getElementById('businessOnly').checked
        }
    };
    
    try {
        const response = await api.createFollowTask(taskData);
        
        if (response.success) {
            let message = `Создано задач: ${response.created_tasks.length}`;
            message += `. Целевых аккаунтов: ${targetAccounts.length}`;
            showNotification(message, 'success');
            closeCreateModal();
            await loadFollowTasks();
            await loadFollowStats();
        } else {
            showNotification(response.error || 'Ошибка при создании задачи', 'error');
        }
    } catch (error) {
        console.error('Error creating task:', error);
        showNotification('Ошибка при создании задачи', 'error');
    }
}

function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.className = `fixed top-4 right-4 px-6 py-3 rounded-lg shadow-lg z-50 transition-all transform translate-x-full ${
        type === 'success' ? 'bg-green-600' : 
        type === 'error' ? 'bg-red-600' : 
        type === 'warning' ? 'bg-yellow-600' : 
        'bg-blue-600'
    } text-white`;
    
    notification.innerHTML = `
        <div class="flex items-center gap-3">
            <i data-lucide="${
                type === 'success' ? 'check-circle' : 
                type === 'error' ? 'x-circle' : 
                type === 'warning' ? 'alert-triangle' : 
                'info'
            }" class="h-5 w-5"></i>
            <span>${message}</span>
        </div>
    `;
    
    document.body.appendChild(notification);
    if (typeof lucide !== 'undefined') {
        lucide.createIcons();
    }
    
    // Animate in
    setTimeout(() => {
        notification.classList.remove('translate-x-full');
    }, 10);
    
    // Remove after 3 seconds
    setTimeout(() => {
        notification.classList.add('translate-x-full');
        setTimeout(() => {
            notification.remove();
        }, 300);
    }, 3000);
}

function getProgressPercentage(task) {
    if (task.followed_count === 0 || task.follow_limit === 0) {
        return 0;
    }
    return (task.followed_count / task.follow_limit) * 100;
}

function formatDateTime(timestamp) {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    return date.toLocaleString('ru-RU');
}

function formatNumber(number) {
    return number.toLocaleString('ru-RU');
}

// Get task action buttons based on status
function getTaskActions(task) {
    // Приводим статус к нижнему регистру для совместимости
    const status = task.status.toLowerCase();
    
    switch (status) {
        case 'running':
            return `
                <button onclick="pauseTask(${task.id})" class="p-2 text-yellow-400 hover:bg-slate-700 rounded-lg transition-colors" title="Приостановить">
                    <i data-lucide="pause" class="h-4 w-4"></i>
                </button>
                <button onclick="stopTask(${task.id})" class="p-2 text-red-400 hover:bg-slate-700 rounded-lg transition-colors" title="Остановить">
                    <i data-lucide="square" class="h-4 w-4"></i>
                </button>
            `;
        case 'paused':
            return `
                <button onclick="resumeTask(${task.id})" class="p-2 text-green-400 hover:bg-slate-700 rounded-lg transition-colors" title="Возобновить">
                    <i data-lucide="play" class="h-4 w-4"></i>
                </button>
                <button onclick="stopTask(${task.id})" class="p-2 text-red-400 hover:bg-slate-700 rounded-lg transition-colors" title="Остановить">
                    <i data-lucide="square" class="h-4 w-4"></i>
                </button>
            `;
        case 'completed':
        case 'failed':
        case 'stopped':
        case 'cancelled':
            return `
                <button onclick="deleteTask(${task.id})" class="p-2 text-red-400 hover:bg-slate-700 rounded-lg transition-colors" title="Удалить">
                    <i data-lucide="trash-2" class="h-4 w-4"></i>
                </button>
            `;
        case 'pending':
            return `
                <button onclick="startTask(${task.id})" class="p-2 text-green-400 hover:bg-slate-700 rounded-lg transition-colors" title="Запустить">
                    <i data-lucide="play" class="h-4 w-4"></i>
                </button>
                <button onclick="deleteTask(${task.id})" class="p-2 text-red-400 hover:bg-slate-700 rounded-lg transition-colors" title="Удалить">
                    <i data-lucide="trash-2" class="h-4 w-4"></i>
                </button>
            `;
        default:
            return '';
    }
}

// Utility functions
function getSourceTypeText(type) {
    const types = {
        'followers': 'Подписчики',
        'following': 'Подписки',
        'hashtag': 'Хештег',
        'location': 'Локация',
        'likers': 'Лайкнувшие',
        'commenters': 'Комментаторы'
    };
    return types[type] || type;
}

function getStatusText(status) {
    const statuses = {
        'PENDING': 'Ожидание',
        'RUNNING': 'Активна',
        'PAUSED': 'Пауза',
        'COMPLETED': 'Завершена',
        'FAILED': 'Ошибка',
        'STOPPED': 'Остановлена'
    };
    return statuses[status] || status;
}

function getStatusBadgeClass(status) {
    const classes = {
        'PENDING': 'bg-slate-600 text-slate-300',
        'RUNNING': 'bg-green-600 text-white',
        'PAUSED': 'bg-yellow-600 text-white',
        'COMPLETED': 'bg-blue-600 text-white',
        'FAILED': 'bg-red-600 text-white',
        'STOPPED': 'bg-gray-600 text-white'
    };
    return classes[status] || 'bg-gray-600 text-white';
}

function getProgressPercentage(task) {
    if (task.follow_limit === 0) return 0;
    return Math.min(100, (task.followed_count / task.follow_limit) * 100);
}

function formatDateTime(timestamp) {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    return date.toLocaleString('ru-RU', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function formatNumber(number) {
    return number.toLocaleString('ru-RU');
}

// Функция обновления информации о режиме создания задач
window.updateTaskModeInfo = function() {
    const mode = document.querySelector('input[name="taskMode"]:checked').value;
    const info = document.getElementById('taskModeInfo');
    const selectedCount = selectedFollowAccounts.length || 1;
    const targetCount = document.getElementById('targetAccounts').value.trim().split('\n').filter(a => a.trim()).length || 0;
    
    if (mode === 'single') {
        info.textContent = `Будет создано ${selectedCount} задач (по одной на каждый выбранный аккаунт)`;
    } else {
        const totalTasks = selectedCount * targetCount;
        info.textContent = `Будет создано ${totalTasks} задач (${selectedCount} аккаунтов × ${targetCount} целей)`;
    }
}

// Функции уже экспортированы через window. выше в коде 