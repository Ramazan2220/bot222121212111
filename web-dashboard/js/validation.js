const API_URL = 'http://localhost:8080/api';
let accounts = [];
let validationResults = {};
let isValidating = false;
let updateInterval = null;
let checkingAccounts = new Set();

// Загрузка начальных данных
document.addEventListener('DOMContentLoaded', () => {
    lucide.createIcons();
    loadAccounts();
    loadValidationStatus();
    startAutoUpdate();
});

// Автообновление каждые 5 секунд
function startAutoUpdate() {
    updateInterval = setInterval(() => {
        if (isValidating || checkingAccounts.size > 0) {
            loadValidationStatus();
            updateAccountsTable();
        }
    }, 5000);
}

// Загрузка аккаунтов
async function loadAccounts() {
    try {
        const response = await fetch(`${API_URL}/accounts`);
        const data = await response.json();

        if (data.success) {
            accounts = data.data;
            updateAccountsTable();
        }
    } catch (error) {
        console.error('Ошибка загрузки аккаунтов:', error);
        addLogEntry(`Ошибка загрузки аккаунтов: ${error.message}`, 'error');
    }
}

// Загрузка статуса валидации
async function loadValidationStatus() {
    try {
        const response = await fetch(`${API_URL}/accounts/validate/status`);
        const data = await response.json();

        if (data.success) {
            updateServiceStatus(data);
            updateStatistics(data.last_results, data.stats);
            
            // Обновляем время следующей проверки
            if (data.is_running && data.check_interval_minutes) {
                const nextCheck = new Date();
                nextCheck.setMinutes(nextCheck.getMinutes() + data.check_interval_minutes);
                document.getElementById('next-check').textContent = nextCheck.toLocaleTimeString('ru-RU');
            }
            
            // Обновляем информацию об очередях
            if (data.stats) {
                const queueInfo = `Проверки: ${data.stats.check_queue_size}, Восстановления: ${data.stats.recovery_queue_size}`;
                addLogEntry(`Очереди: ${queueInfo}`, 'info');
            }
        }
    } catch (error) {
        console.error('Ошибка загрузки статуса:', error);
    }
}

// Обновление статуса сервиса
function updateServiceStatus(data) {
    const statusEl = document.getElementById('service-status');
    const intervalEl = document.getElementById('check-interval');
    const autoRepairEl = document.getElementById('auto-repair');

    statusEl.textContent = data.is_running ? 'Работает' : 'Остановлен';
    statusEl.className = data.is_running ? 'px-3 py-1 bg-green-600/20 text-green-400 rounded-full' : 'px-3 py-1 bg-red-600/20 text-red-400 rounded-full';
    
    intervalEl.value = data.check_interval_minutes;
    autoRepairEl.checked = data.auto_repair;

    isValidating = data.is_running;
}

// Обновление статистики
function updateStatistics(results, stats) {
    if (!results) return;

    document.getElementById('valid-count').textContent = results.valid || 0;
    document.getElementById('invalid-count').textContent = results.invalid || 0;
    document.getElementById('repaired-count').textContent = results.repaired || 0;
    
    // Если есть статистика из умного валидатора
    if (stats) {
        const checking = (stats.status_counts?.checking || 0) + (stats.status_counts?.recovering || 0);
        document.getElementById('checking-count').textContent = checking;
        
        // Обновляем информацию о нагрузке
        if (stats.system_load) {
            const loadInfo = `CPU: ${stats.system_load.cpu.toFixed(1)}%, RAM: ${stats.system_load.memory.toFixed(1)}%`;
            const statusEl = document.getElementById('service-status');
            if (stats.system_load.is_high) {
                statusEl.innerHTML = `Работает (Высокая нагрузка: ${loadInfo})`;
                statusEl.className = 'px-3 py-1 bg-yellow-600/20 text-yellow-400 rounded-full';
            } else {
                statusEl.innerHTML = `Работает (${loadInfo})`;
            }
        }
    } else {
        document.getElementById('checking-count').textContent = checkingAccounts.size;
    }
}

// Запуск валидации
async function startValidation() {
    try {
        addLogEntry('Запуск проверки валидности...', 'info');
        
        const response = await fetch(`${API_URL}/accounts/validate`, {
            method: 'POST'
        });
        const data = await response.json();

        if (data.success) {
            addLogEntry(data.message || 'Проверка запущена успешно', 'success');
            isValidating = true;
            
            // Обновляем статистику сразу
            if (data.stats) {
                updateStatistics(data.last_results, data.stats);
            }
            
            setTimeout(loadValidationStatus, 1000);
        } else {
            addLogEntry(`Ошибка: ${data.error}`, 'error');
        }
    } catch (error) {
        addLogEntry(`Ошибка запуска: ${error.message}`, 'error');
    }
}

// Остановка валидации
async function stopValidation() {
    try {
        // В текущей реализации нет эндпоинта для остановки
        // Но мы можем изменить настройки на очень большой интервал
        const response = await fetch(`${API_URL}/accounts/validate/settings`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                check_interval_minutes: 1440, // 24 часа
                auto_repair: false
            })
        });

        if (response.ok) {
            addLogEntry('Валидация остановлена', 'success');
            isValidating = false;
            checkingAccounts.clear();
            updateAccountsTable();
            loadValidationStatus();
        }
    } catch (error) {
        addLogEntry(`Ошибка: ${error.message}`, 'error');
    }
}

// Сохранение настроек
async function saveSettings() {
    try {
        const interval = document.getElementById('check-interval').value;
        const autoRepair = document.getElementById('auto-repair').checked;

        const response = await fetch(`${API_URL}/accounts/validate/settings`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                check_interval_minutes: parseInt(interval),
                auto_repair: autoRepair
            })
        });

        const data = await response.json();
        if (data.success) {
            addLogEntry('Настройки сохранены', 'success');
            showNotification('Настройки сохранены', 'success');
        } else {
            addLogEntry(`Ошибка: ${data.error}`, 'error');
        }
    } catch (error) {
        addLogEntry(`Ошибка сохранения: ${error.message}`, 'error');
    }
}

// Проверка отдельного аккаунта
async function checkAccount(accountId) {
    try {
        const btn = event.target.closest('button');
        btn.disabled = true;
        btn.innerHTML = '<i data-lucide="loader"></i> Проверка...';
        lucide.createIcons();

        checkingAccounts.add(accountId);
        updateAccountsTable();
        
        addLogEntry(`Проверка аккаунта #${accountId}...`, 'info');
        
        // Вызываем проверку конкретного аккаунта
        const response = await fetch(`${API_URL}/accounts/${accountId}/check`, {
            method: 'POST'
        });
        
        if (response.ok) {
            const data = await response.json();
            if (data.success) {
                if (data.status === 'checking') {
                    addLogEntry(`Аккаунт #${accountId} добавлен в очередь проверки`, 'info');
                } else {
                    const status = data.is_valid ? 'валидный' : 'невалидный';
                    addLogEntry(`Аккаунт #${accountId} - ${status} (${data.status})`, data.is_valid ? 'success' : 'error');
                    
                    // Обновляем статус аккаунта в списке
                    const account = accounts.find(a => a.id === accountId);
                    if (account) {
                        account.is_active = data.is_valid;
                    }
                }
            }
        }
        
        checkingAccounts.delete(accountId);
        btn.disabled = false;
        btn.innerHTML = '<i data-lucide="check"></i> Проверить';
        lucide.createIcons();
        updateAccountsTable();
        
    } catch (error) {
        addLogEntry(`Ошибка проверки: ${error.message}`, 'error');
        checkingAccounts.delete(accountId);
    }
}

// Обновление таблицы аккаунтов
function updateAccountsTable(filter = 'all') {
    const tbody = document.getElementById('accounts-table');
    let filteredAccounts = accounts;

    if (filter !== 'all') {
        filteredAccounts = accounts.filter(account => {
            if (filter === 'valid') return account.is_active;
            if (filter === 'invalid') return !account.is_active;
            if (filter === 'checking') return checkingAccounts.has(account.id);
        });
    }

    tbody.innerHTML = filteredAccounts.map(account => {
        const isChecking = checkingAccounts.has(account.id);
        const statusClass = isChecking ? 'checking' : (account.is_active ? 'valid' : 'invalid');
        const statusText = isChecking ? 'Проверяется' : (account.is_active ? 'Валидный' : 'Невалидный');
        
        return `
            <tr>
                <td>
                    <div class="account-info">
                        <strong>@${account.username}</strong>
                        <small>#${account.id}</small>
                    </div>
                </td>
                <td>${account.email || '-'}</td>
                <td>
                    <div class="status-cell">
                        <span class="status-dot ${statusClass}"></span>
                        <span>${statusText}</span>
                    </div>
                </td>
                <td>${formatDate(account.updated_at)}</td>
                <td class="error-cell">${account.last_error || '-'}</td>
                <td>
                    <button class="px-3 py-1 bg-blue-600 hover:bg-blue-700 rounded text-white text-sm transition-colors" onclick="checkAccount(${account.id})" ${isChecking ? 'disabled' : ''}>
                        ${isChecking ? '<i data-lucide="loader" class="inline h-4 w-4"></i> Проверка...' : '<i data-lucide="check" class="inline h-4 w-4"></i> Проверить'}
                    </button>
                </td>
            </tr>
        `;
    }).join('');

    lucide.createIcons();
}

// Фильтрация аккаунтов
function filterAccounts(filter) {
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.classList.add('active');
    updateAccountsTable(filter);
}

// Добавление записи в лог
function addLogEntry(message, type = 'info') {
    const log = document.getElementById('validation-log');
    const time = new Date().toLocaleTimeString();
    
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    entry.innerHTML = `
        <span class="log-time">[${time}]</span>
        <span class="log-message ${type}">${message}</span>
    `;
    
    log.insertBefore(entry, log.firstChild);
    
    // Ограничиваем количество записей
    while (log.children.length > 100) {
        log.removeChild(log.lastChild);
    }
}

// Очистка лога
function clearLog() {
    document.getElementById('validation-log').innerHTML = '';
    addLogEntry('Лог очищен', 'info');
}

// Форматирование даты
function formatDate(dateString) {
    if (!dateString) return '-';
    const date = new Date(dateString);
    return date.toLocaleString('ru-RU', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

// Показать уведомление
function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.innerHTML = `
        <i data-lucide="${type === 'success' ? 'check-circle' : type === 'error' ? 'x-circle' : 'info'}" class="h-5 w-5"></i>
        <span>${message}</span>
    `;
    
    document.body.appendChild(notification);
    lucide.createIcons();
    
    setTimeout(() => {
        notification.classList.add('show');
    }, 100);
    
    setTimeout(() => {
        notification.classList.remove('show');
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}

// Стили для уведомлений
const style = document.createElement('style');
style.textContent = `
    .notification {
        position: fixed;
        top: 20px;
        right: -300px;
        background: white;
        padding: 1rem 1.5rem;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        display: flex;
        align-items: center;
        gap: 0.75rem;
        transition: right 0.3s ease;
        z-index: 1000;
    }
    
    .notification.show {
        right: 20px;
    }
    
    .notification.success {
        border-left: 4px solid #28a745;
    }
    
    .notification.error {
        border-left: 4px solid #dc3545;
    }
    
    .notification.info {
        border-left: 4px solid #17a2b8;
    }
    
    .account-info {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
    }
    
    .account-info small {
        color: #6c757d;
    }
    
    .error-cell {
        font-size: 0.875rem;
        color: #dc3545;
        max-width: 200px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
`;
document.head.appendChild(style); 