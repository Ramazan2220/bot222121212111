// Masslooking functionality
let masslookingTasks = [];
let accounts = [];

// Initialize page
document.addEventListener('DOMContentLoaded', async () => {
    await loadAccounts();
    await loadMasslookingTasks();
    setupEventListeners();
    updateStats();
});

async function loadAccounts() {
    try {
        const response = await api.getAccounts();
        if (response.success) {
            accounts = response.data.filter(acc => acc.is_active);
            populateAccountSelect();
        }
    } catch (error) {
        console.error('Error loading accounts:', error);
        showNotification('Ошибка загрузки аккаунтов', 'error');
    }
}

function populateAccountSelect() {
    const select = document.getElementById('task-account');
    if (!select) return;
    
    select.innerHTML = '<option value="">Выберите аккаунт</option>';
    accounts.forEach(account => {
        const option = document.createElement('option');
        option.value = account.id;
        option.textContent = `@${account.username}`;
        select.appendChild(option);
    });
}

async function loadMasslookingTasks() {
    // TODO: Load tasks from API
    updateTasksTable();
}

function updateTasksTable() {
    const tbody = document.getElementById('masslooking-tasks-table');
    if (!tbody) return;
    
    if (masslookingTasks.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="8" class="px-6 py-12 text-center text-slate-400">
                    <i data-lucide="eye-off" class="h-12 w-12 mx-auto mb-3 text-slate-600"></i>
                    <p>Нет активных задач масслукинга</p>
                    <button onclick="createMasslookingTask()" class="mt-3 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md transition-colors text-sm">
                        Создать первую задачу
                    </button>
                </td>
            </tr>
        `;
    } else {
        tbody.innerHTML = masslookingTasks.map(task => `
            <tr>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-white">${task.name}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">@${task.account}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${task.source}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${task.views}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${task.reactions}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${task.speed}/ч</td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <span class="px-2 py-1 text-xs leading-5 font-semibold rounded-full ${getStatusClass(task.status)}">
                        ${getStatusText(task.status)}
                    </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <button onclick="toggleTask(${task.id})" class="text-blue-400 hover:text-blue-300 mr-3">
                        <i data-lucide="${task.status === 'active' ? 'pause' : 'play'}" class="h-4 w-4"></i>
                    </button>
                    <button onclick="deleteTask(${task.id})" class="text-red-400 hover:text-red-300">
                        <i data-lucide="trash-2" class="h-4 w-4"></i>
                    </button>
                </td>
            </tr>
        `).join('');
    }
    
    lucide.createIcons();
}

function setupEventListeners() {
    const form = document.getElementById('create-task-form');
    if (form) {
        form.addEventListener('submit', handleCreateTask);
    }
    
    // Source type radio buttons
    const sourceTypes = document.querySelectorAll('input[name="source-type"]');
    sourceTypes.forEach(radio => {
        radio.addEventListener('change', toggleSourceInputs);
    });
    
    // Enable reactions toggle
    const enableReactions = document.getElementById('enable-reactions');
    if (enableReactions) {
        enableReactions.addEventListener('change', toggleReactionSettings);
    }
    
    // Reaction percentage slider
    const reactionSlider = document.getElementById('reaction-percentage');
    if (reactionSlider) {
        reactionSlider.addEventListener('input', updateReactionValue);
    }
}

function createMasslookingTask() {
    document.getElementById('create-task-modal').classList.remove('hidden');
}

function closeTaskModal() {
    document.getElementById('create-task-modal').classList.add('hidden');
    document.getElementById('create-task-form').reset();
}

function toggleSourceInputs() {
    const sourceType = document.querySelector('input[name="source-type"]:checked').value;
    
    // Hide all source inputs
    document.getElementById('source-accounts').classList.add('hidden');
    document.getElementById('source-hashtags').classList.add('hidden');
    
    // Show relevant input
    switch(sourceType) {
        case 'followers':
            document.getElementById('source-accounts').classList.remove('hidden');
            break;
        case 'hashtags':
            document.getElementById('source-hashtags').classList.remove('hidden');
            break;
    }
}

function toggleReactionSettings() {
    const enabled = document.getElementById('enable-reactions').checked;
    const settings = document.getElementById('reaction-settings');
    
    if (enabled) {
        settings.classList.remove('hidden');
    } else {
        settings.classList.add('hidden');
    }
}

function updateReactionValue() {
    const value = document.getElementById('reaction-percentage').value;
    document.getElementById('reaction-value').textContent = value + '%';
}

async function handleCreateTask(e) {
    e.preventDefault();
    
    const sourceType = document.querySelector('input[name="source-type"]:checked').value;
    let sources = [];
    
    switch(sourceType) {
        case 'followers':
            sources = document.getElementById('target-accounts').value
                .split('\n')
                .map(s => s.trim())
                .filter(s => s);
            break;
        case 'hashtags':
            sources = document.getElementById('target-hashtags').value
                .split('\n')
                .map(s => s.trim())
                .filter(s => s);
            break;
    }
    
    const taskData = {
        name: document.getElementById('task-name').value,
        account_id: document.getElementById('task-account').value,
        source_type: sourceType,
        sources: sources,
        views_per_hour: parseInt(document.getElementById('views-per-hour').value),
        daily_limit: parseInt(document.getElementById('daily-limit').value),
        enable_reactions: document.getElementById('enable-reactions').checked,
        reaction_percentage: parseInt(document.getElementById('reaction-percentage')?.value || 0),
        reaction_types: Array.from(document.querySelectorAll('#reaction-settings input[type="checkbox"]:checked'))
            .map(cb => cb.nextElementSibling.textContent)
    };
    
    try {
        showNotification('Создание задачи...', 'info');
        
        // TODO: Implement API call
        // const result = await api.createMasslookingTask(taskData);
        
        // For now, just add to local array
        masslookingTasks.push({
            id: Date.now(),
            ...taskData,
            views: 0,
            reactions: 0,
            speed: taskData.views_per_hour,
            status: 'active',
            account: accounts.find(a => a.id == taskData.account_id)?.username || 'unknown',
            source: `${sources.length} ${sourceType === 'followers' ? 'аккаунтов' : 'хештегов'}`
        });
        
        updateTasksTable();
        updateStats();
        closeTaskModal();
        showNotification('Задача создана успешно', 'success');
    } catch (error) {
        console.error('Error creating task:', error);
        showNotification('Ошибка при создании задачи', 'error');
    }
}

function updateStats() {
    // TODO: Update stats from real data
}

function getStatusClass(status) {
    switch(status) {
        case 'active': return 'bg-green-100 text-green-800';
        case 'paused': return 'bg-yellow-100 text-yellow-800';
        case 'completed': return 'bg-blue-100 text-blue-800';
        case 'error': return 'bg-red-100 text-red-800';
        default: return 'bg-gray-100 text-gray-800';
    }
}

function getStatusText(status) {
    switch(status) {
        case 'active': return 'Активна';
        case 'paused': return 'Пауза';
        case 'completed': return 'Завершена';
        case 'error': return 'Ошибка';
        default: return status;
    }
}

function toggleTask(taskId) {
    const task = masslookingTasks.find(t => t.id === taskId);
    if (task) {
        task.status = task.status === 'active' ? 'paused' : 'active';
        updateTasksTable();
        showNotification(`Задача ${task.status === 'active' ? 'запущена' : 'приостановлена'}`, 'info');
    }
}

function deleteTask(taskId) {
    if (confirm('Удалить эту задачу?')) {
        masslookingTasks = masslookingTasks.filter(t => t.id !== taskId);
        updateTasksTable();
        showNotification('Задача удалена', 'success');
    }
}

function showNotification(message, type = 'info', duration = 3000) {
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
    lucide.createIcons();
    
    setTimeout(() => {
        notification.classList.remove('translate-x-full');
    }, 10);
    
    setTimeout(() => {
        notification.classList.add('translate-x-full');
        setTimeout(() => {
            notification.remove();
        }, 300);
    }, duration);
} 