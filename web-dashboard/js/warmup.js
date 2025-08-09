// Warmup page JavaScript

let currentTab = 'active';
let warmupProcesses = [];
let warmupTemplates = [];
let availableAccounts = [];
let selectedWarmupAccounts = [];

// Warmup preset configurations
const WARMUP_PRESETS = {
    SLOW: {
        name: 'Медленный',
        duration: 21,
        description: 'Максимальная безопасность',
        phases: {
            1: { enabled: true, min: 5, max: 15, duration_days: 7, duration_hours: 0 },
            2: { enabled: true, min: 20, max: 50, duration_days: 5, duration_hours: 0 },
            3: { enabled: true, min: 2, max: 8, duration_days: 4, duration_hours: 0 },
            4: { enabled: true, min: 1, max: 2, duration_days: 5, duration_hours: 0 }
        },
        workHours: { start: '09:00', end: '21:00' },
        breaks: { enabled: true, workPeriod: 180, breakDuration: 30 },
        actionInterval: { min: 60, max: 180 }
    },
    NORMAL: {
        name: 'Обычный',
        duration: 14,
        description: 'Оптимальный баланс',
        phases: {
            1: { enabled: true, min: 10, max: 25, duration_days: 5, duration_hours: 0 },
            2: { enabled: true, min: 50, max: 100, duration_days: 4, duration_hours: 0 },
            3: { enabled: true, min: 5, max: 15, duration_days: 3, duration_hours: 0 },
            4: { enabled: true, min: 1, max: 3, duration_days: 2, duration_hours: 0 }
        },
        workHours: { start: '09:00', end: '22:00' },
        breaks: { enabled: true, workPeriod: 120, breakDuration: 15 },
        actionInterval: { min: 30, max: 120 }
    },
    FAST: {
        name: 'Быстрый',
        duration: 7,
        description: 'Для опытных аккаунтов',
        phases: {
            1: { enabled: true, min: 20, max: 40, duration_days: 2, duration_hours: 0 },
            2: { enabled: true, min: 80, max: 150, duration_days: 2, duration_hours: 0 },
            3: { enabled: true, min: 10, max: 25, duration_days: 2, duration_hours: 0 },
            4: { enabled: true, min: 2, max: 5, duration_days: 1, duration_hours: 0 }
        },
        workHours: { start: '08:00', end: '23:00' },
        breaks: { enabled: true, workPeriod: 90, breakDuration: 10 },
        actionInterval: { min: 20, max: 60 }
    },
    SUPER_FAST: {
        name: 'Супер быстрый',
        duration: 3,
        description: 'Агрессивный режим',
        phases: {
            1: { enabled: true, min: 40, max: 80, duration_days: 1, duration_hours: 0 },
            2: { enabled: true, min: 150, max: 300, duration_days: 1, duration_hours: 0 },
            3: { enabled: true, min: 25, max: 50, duration_days: 1, duration_hours: 0 },
            4: { enabled: false, min: 0, max: 0, duration_days: 0, duration_hours: 0 }
        },
        workHours: { start: '06:00', end: '23:59' },
        breaks: { enabled: false, workPeriod: 480, breakDuration: 5 },
        actionInterval: { min: 10, max: 30 }
    },
    DAILY: {
        name: 'Дневной',
        duration: 1,
        description: 'Тестовый режим',
        phases: {
            1: { enabled: true, min: 100, max: 200, duration_days: 0, duration_hours: 8 },
            2: { enabled: true, min: 300, max: 500, duration_days: 0, duration_hours: 8 },
            3: { enabled: true, min: 50, max: 100, duration_days: 0, duration_hours: 4 },
            4: { enabled: true, min: 10, max: 20, duration_days: 0, duration_hours: 4 }
        },
        workHours: { start: '00:00', end: '23:59' },
        breaks: { enabled: false, workPeriod: 720, breakDuration: 5 },
        actionInterval: { min: 5, max: 15 }
    }
};

// Current selected preset
let currentPreset = 'NORMAL';
let advancedSettingsVisible = false;

document.addEventListener('DOMContentLoaded', async () => {
    await Promise.all([loadWarmupProcesses(), loadWarmupTemplates()]);
    setupSearchFilter();
    renderWarmupContent();
    updateCounts();
    
    // Обновляем статус каждые 10 секунд
    setInterval(async () => {
        await loadWarmupProcesses();
    }, 10000);
});

async function loadWarmupProcesses() {
    try {
        // Загружаем статус прогрева из API
        const response = await api.get('/warmup/status');
        
        if (response.success && response.data) {
            // Преобразуем данные из API в формат для отображения
            warmupProcesses = response.data;
            
            // Обновляем отображение
            renderWarmupContent();
            updateCounts();
        } else {
            // Используем моковые данные как запасной вариант
            warmupProcesses = generateMockWarmupData();
            renderWarmupContent();
            updateCounts();
        }
    } catch (error) {
        console.error('Error loading warmup processes:', error);
        // Заглушка с тестовыми данными
        warmupProcesses = generateMockWarmupData();
        renderWarmupContent();
        updateCounts();
    }
}

async function loadWarmupTemplates() {
    try {
        warmupTemplates = await api.getWarmupTemplates();
    } catch (error) {
        console.error('Error loading warmup templates:', error);
        // Заглушка с тестовыми данными
        warmupTemplates = generateMockTemplateData();
    }
}

function generateMockWarmupData() {
    const statuses = ['active', 'queue', 'completed'];
    const phases = ['phase1', 'phase2', 'phase3', 'phase4'];
    const accounts = [];
    
    for (let i = 1; i <= 67; i++) {
        const status = statuses[Math.floor(Math.random() * statuses.length)];
        const phase = phases[Math.floor(Math.random() * phases.length)];
        
        accounts.push({
            id: i,
            username: `account_${i.toString().padStart(3, '0')}`,
            status: status,
            phase: phase,
            progress: Math.floor(Math.random() * 100),
            daily_actions: Math.floor(Math.random() * 50) + 10,
            total_actions: Math.floor(Math.random() * 500) + 100,
            start_date: new Date(Date.now() - Math.random() * 30 * 24 * 60 * 60 * 1000),
            next_action: new Date(Date.now() + Math.random() * 24 * 60 * 60 * 1000),
            template: ['conservative', 'moderate', 'aggressive'][Math.floor(Math.random() * 3)]
        });
    }
    
    return accounts;
}

function generateMockTemplateData() {
    return [
        {
            id: 1,
            name: 'Консервативный прогрев',
            description: 'Медленный и безопасный прогрев для новых аккаунтов',
            phases: [
                { name: 'Подписки', min_daily: 5, max_daily: 15, duration: 10 },
                { name: 'Лайки', min_daily: 20, max_daily: 50, duration: 7 },
                { name: 'Комментарии', min_daily: 5, max_daily: 15, duration: 5 },
                { name: 'Сториз', min_daily: 3, max_daily: 10, duration: 5 }
            ],
            total_duration: 27,
            success_rate: 98.5
        },
        {
            id: 2,
            name: 'Умеренный прогрев',
            description: 'Сбалансированный подход для стабильного роста',
            phases: [
                { name: 'Подписки', min_daily: 10, max_daily: 30, duration: 7 },
                { name: 'Лайки', min_daily: 40, max_daily: 80, duration: 5 },
                { name: 'Комментарии', min_daily: 10, max_daily: 25, duration: 4 },
                { name: 'Сториз', min_daily: 5, max_daily: 15, duration: 4 }
            ],
            total_duration: 20,
            success_rate: 94.2
        },
        {
            id: 3,
            name: 'Агрессивный прогрев',
            description: 'Быстрый прогрев для опытных аккаунтов',
            phases: [
                { name: 'Подписки', min_daily: 20, max_daily: 50, duration: 5 },
                { name: 'Лайки', min_daily: 60, max_daily: 120, duration: 4 },
                { name: 'Комментарии', min_daily: 15, max_daily: 35, duration: 3 },
                { name: 'Сториз', min_daily: 8, max_daily: 20, duration: 3 }
            ],
            total_duration: 15,
            success_rate: 87.8
        }
    ];
}

function renderWarmupContent() {
    const activeTab = document.getElementById('active-tab');
    const queueTab = document.getElementById('queue-tab');
    const completedTab = document.getElementById('completed-tab');
    const templatesTab = document.getElementById('templates-tab');
    
    // Clear all tabs
    activeTab.innerHTML = '';
    queueTab.innerHTML = '';
    completedTab.innerHTML = '';
    templatesTab.innerHTML = '';
    
    // Render based on current tab
    switch (currentTab) {
        case 'active':
            renderActiveWarmup();
            break;
        case 'queue':
            renderQueueWarmup();
            break;
        case 'completed':
            renderCompletedWarmup();
            break;
        case 'templates':
            renderTemplates();
            break;
    }
}

function renderActiveWarmup() {
    const activeProcesses = warmupProcesses.filter(p => 
        p.status === 'RUNNING' || p.status === 'running'
    );
    const container = document.getElementById('active-tab');
    
    if (activeProcesses.length === 0) {
        container.innerHTML = `
            <div class="col-span-full text-center py-12">
                <i data-lucide="activity" class="h-12 w-12 text-slate-400 mx-auto mb-4"></i>
                <h3 class="text-lg font-medium text-white mb-2">Нет активных процессов прогрева</h3>
                <p class="text-slate-400 mb-4">Запустите прогрев для новых аккаунтов</p>
                <button onclick="startWarmup()" class="bg-green-600 hover:bg-green-700 px-4 py-2 rounded-md text-white transition-colors">
                    Запустить прогрев
                </button>
            </div>
        `;
        lucide.createIcons();
        return;
    }
    
    const html = activeProcesses.map(process => renderWarmupCard(process, true)).join('');
    
    container.innerHTML = html;
    lucide.createIcons();
}

function renderQueueWarmup() {
    const queueProcesses = warmupProcesses.filter(p => 
        p.status === 'PENDING' || p.status === 'pending' || p.status === 'queue'
    );
    const container = document.getElementById('queue-tab');
    
    const html = queueProcesses.map(process => renderWarmupCard(process, false)).join('');
    
    container.innerHTML = html || '<div class="col-span-full text-center py-12 text-slate-400">Нет аккаунтов в очереди</div>';
    lucide.createIcons();
}

function renderCompletedWarmup() {
    const completedProcesses = warmupProcesses.filter(p => 
        p.status === 'COMPLETED' || p.status === 'completed' || 
        p.status === 'FAILED' || p.status === 'failed' ||
        p.status === 'CANCELLED' || p.status === 'cancelled'
    );
    const container = document.getElementById('completed-tab');
    
    const html = completedProcesses.map(process => renderWarmupCard(process, false)).join('');
    
    container.innerHTML = html || '<div class="col-span-full text-center py-12 text-slate-400">Нет завершенных процессов</div>';
    lucide.createIcons();
}

function renderTemplates() {
    const container = document.getElementById('templates-tab');
    
    const html = warmupTemplates.map(template => `
        <div class="bg-slate-800/50 border border-slate-700 rounded-lg p-6">
            <div class="flex items-center justify-between mb-4">
                <div>
                    <h3 class="text-lg font-semibold text-white">${template.name}</h3>
                    <p class="text-slate-400 text-sm">${template.description}</p>
                </div>
                <div class="flex items-center gap-2">
                    <span class="inline-block px-2 py-1 text-xs rounded bg-blue-600 text-white">
                        ${template.success_rate}% успех
                    </span>
                    <button onclick="useTemplate(${template.id})" class="px-3 py-1 bg-green-600 hover:bg-green-700 text-white text-sm rounded transition-colors">
                        Использовать
                    </button>
                </div>
            </div>
            
            <div class="space-y-3">
                <div class="text-sm">
                    <span class="text-slate-400">Общая длительность:</span>
                    <span class="text-white ml-2">${template.total_duration} дней</span>
                </div>
                
                <div class="space-y-2">
                    ${template.phases.map((phase, index) => `
                        <div class="flex items-center justify-between bg-slate-700/30 rounded p-2">
                            <span class="text-white text-sm">${phase.name}</span>
                            <span class="text-slate-400 text-xs">${phase.min_daily}-${phase.max_daily}/день • ${phase.duration} дней</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        </div>
    `).join('');
    
    container.innerHTML = html;
    lucide.createIcons();
}

function updateCounts() {
    const active = warmupProcesses.filter(p => 
        p.status === 'RUNNING' || p.status === 'running'
    ).length;
    const queue = warmupProcesses.filter(p => 
        p.status === 'PENDING' || p.status === 'pending' || p.status === 'queue'
    ).length;
    const completed = warmupProcesses.filter(p => 
        p.status === 'COMPLETED' || p.status === 'completed' || 
        p.status === 'FAILED' || p.status === 'failed' ||
        p.status === 'CANCELLED' || p.status === 'cancelled'
    ).length;
    
    document.getElementById('active-count').textContent = active;
    document.getElementById('queue-count').textContent = queue;
    document.getElementById('completed-count').textContent = completed;
    
    document.getElementById('active-warmup').textContent = active;
    document.getElementById('queue-warmup').textContent = queue;
    document.getElementById('completed-warmup').textContent = completed;
}

function updateConcurrentValue(value) {
    document.getElementById('concurrent-value').textContent = value;
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
    
    // Show/hide tabs
    ['active-tab', 'queue-tab', 'completed-tab', 'templates-tab'].forEach(tabId => {
        const element = document.getElementById(tabId);
        if (tabId === `${tab}-tab`) {
            element.classList.remove('hidden');
        } else {
            element.classList.add('hidden');
        }
    });
    
    renderWarmupContent();
}

function getPhaseText(phase) {
    const phases = {
        'phase1': 'Фаза 1: Подписки',
        'phase2': 'Фаза 2: Лайки',
        'phase3': 'Фаза 3: Комментарии',
        'phase4': 'Фаза 4: Сториз'
    };
    return phases[phase] || 'Неизвестная фаза';
}

function getTemplateText(template) {
    const templates = {
        'conservative': 'Консервативный',
        'moderate': 'Умеренный',
        'aggressive': 'Агрессивный'
    };
    return templates[template] || template;
}

function formatTime(date) {
    return new Date(date).toLocaleTimeString('ru-RU', { 
        hour: '2-digit', 
        minute: '2-digit' 
    });
}

function formatDate(date) {
    return new Date(date).toLocaleDateString('ru-RU');
}

function setupSearchFilter() {
    document.getElementById('search-input').addEventListener('input', (e) => {
        const searchTerm = e.target.value.toLowerCase();
        // Implement search filtering
    });
    
    document.getElementById('phase-filter').addEventListener('change', (e) => {
        const phase = e.target.value;
        // Implement phase filtering
    });
}

// Action functions
async function startWarmup() {
    // Open settings modal to configure warmup
    openWarmupSettings();
    
    // Pre-fill with default safe values
    document.getElementById('phase1-min').value = 5;
    document.getElementById('phase1-max').value = 15;
    document.getElementById('phase1-duration-days').value = 7;
    document.getElementById('phase1-duration-hours').value = 0;
    
    document.getElementById('phase2-min').value = 30;
    document.getElementById('phase2-max').value = 60;
    document.getElementById('phase2-duration-days').value = 5;
    document.getElementById('phase2-duration-hours').value = 0;
    
    document.getElementById('work-start').value = '09:00';
    document.getElementById('work-end').value = '21:00';
    
    showNotification('Настройте параметры прогрева и нажмите "Сохранить"', 'info');
}

async function openWarmupSettings() {
    document.getElementById('warmup-settings-modal').classList.remove('hidden');
    
    // Load accounts into select
    await loadAccountsForWarmup();
    
    // Initialize phase states
    togglePhase(1, true);
    togglePhase(2, true);
    
    lucide.createIcons();
}

async function loadAccountsForWarmup() {
    try {
        const accounts = await api.getAccounts();
        availableAccounts = accounts.filter(acc => acc.is_active);
        renderAccountsList();
        updateSelectedAccountsDisplay();
    } catch (error) {
        console.error('Error loading accounts for warmup:', error);
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
                   ${selectedWarmupAccounts.includes(account.id.toString()) ? 'checked' : ''}
                   onchange="toggleWarmupAccount('${account.id}')"
                   class="h-4 w-4 text-blue-600">
            <span class="text-white">@${account.username}</span>
        </label>
    `).join('');
}

function toggleAccountSelection() {
    const dropdown = document.getElementById('account-selection-dropdown');
    dropdown.classList.toggle('hidden');
    
    // Close dropdown when clicking outside
    if (!dropdown.classList.contains('hidden')) {
        setTimeout(() => {
            document.addEventListener('click', closeAccountDropdown);
        }, 100);
    }
}

function closeAccountDropdown(event) {
    const dropdown = document.getElementById('account-selection-dropdown');
    const button = event.target.closest('button[onclick="toggleAccountSelection()"]');
    
    if (!dropdown.contains(event.target) && !button) {
        dropdown.classList.add('hidden');
        document.removeEventListener('click', closeAccountDropdown);
    }
}

function toggleWarmupAccount(accountId) {
    const idStr = accountId.toString();
    const index = selectedWarmupAccounts.indexOf(idStr);
    
    if (index > -1) {
        selectedWarmupAccounts.splice(index, 1);
    } else {
        selectedWarmupAccounts.push(idStr);
    }
    
    updateSelectedAccountsDisplay();
}

function selectAllWarmupAccounts() {
    selectedWarmupAccounts = availableAccounts.map(acc => acc.id.toString());
    renderAccountsList();
    updateSelectedAccountsDisplay();
}

function updateSelectedAccountsDisplay() {
    const display = document.getElementById('selected-accounts-display');
    if (!display) return;
    
    if (selectedWarmupAccounts.length === 0) {
        display.innerHTML = '<span class="text-slate-400 text-sm">Выберите аккаунты для прогрева...</span>';
    } else {
        const selectedAccountObjs = availableAccounts.filter(acc => 
            selectedWarmupAccounts.includes(acc.id.toString())
        );
        
        display.innerHTML = selectedAccountObjs.map(account => `
            <span class="inline-flex items-center gap-1 px-3 py-1 bg-blue-600 text-white rounded-full text-sm">
                @${account.username}
                <button type="button" onclick="removeWarmupAccount('${account.id}')" class="ml-1 hover:text-red-200">
                    <i data-lucide="x" class="h-3 w-3"></i>
                </button>
            </span>
        `).join('');
        
        lucide.createIcons();
    }
    
    // Update distribution info when accounts selection changes
    updateDistributionInfo();
}

function removeWarmupAccount(accountId) {
    const index = selectedWarmupAccounts.indexOf(accountId.toString());
    if (index > -1) {
        selectedWarmupAccounts.splice(index, 1);
        renderAccountsList();
        updateSelectedAccountsDisplay();
    }
}

function filterWarmupAccounts() {
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

function togglePhase(phaseNumber, silent = false) {
    const isEnabled = document.getElementById(`phase${phaseNumber}-enabled`).checked;
    const phaseContent = document.getElementById(`phase${phaseNumber}-content`);
    const disabledMessage = document.getElementById(`phase${phaseNumber}-disabled-message`);
    
    if (phaseContent && disabledMessage) {
        if (isEnabled) {
            phaseContent.classList.remove('hidden');
            disabledMessage.classList.add('hidden');
        } else {
            phaseContent.classList.add('hidden');
            disabledMessage.classList.remove('hidden');
        }
    }
    
    // Show notification only if not silent
    if (!silent) {
        const phaseNames = {
            1: 'Подписки',
            2: 'Лайки', 
            3: 'Комментарии',
            4: 'Истории'
        };
        const phaseName = phaseNames[phaseNumber] || `Фаза ${phaseNumber}`;
        showNotification(`Фаза "${phaseName}" ${isEnabled ? 'включена' : 'отключена'}`, 'info');
    }
}

function closeWarmupSettings() {
    document.getElementById('warmup-settings-modal').classList.add('hidden');
    // Reset selected accounts
    selectedWarmupAccounts = [];
    updateSelectedAccountsDisplay();
    
    // Close dropdown if open
    const dropdown = document.getElementById('account-selection-dropdown');
    if (dropdown) {
        dropdown.classList.add('hidden');
    }
}

async function saveWarmupSettings() {
    // Get selected accounts
    if (selectedWarmupAccounts.length === 0) {
        showNotification('Пожалуйста, выберите хотя бы один аккаунт для прогрева', 'error');
        return;
    }
    
    // Get warmup settings using new system
    let settings = getWarmupSettings();
    
    // Validate settings
    if (!advancedSettingsVisible) {
        // For preset mode, just check if we have a valid preset
        if (!WARMUP_PRESETS[currentPreset]) {
            showNotification('Выбранный режим прогрева недоступен', 'error');
            return;
        }
    } else {
        // For custom mode, validate all settings
        const enabledPhases = Object.keys(settings.phases).filter(key => settings.phases[key].enabled);
        
        if (enabledPhases.length === 0) {
            showNotification('Пожалуйста, включите хотя бы одну фазу прогрева', 'error');
            return;
        }
        
        // Validate each enabled phase
        for (const phaseKey of enabledPhases) {
            const phase = settings.phases[phaseKey];
            const phaseNum = phaseKey.replace('phase', '');
            
            if (phase.min > phase.max) {
                showNotification(`Минимальное значение не может быть больше максимального в Фазе ${phaseNum}`, 'error');
                return;
            }
            
            const totalHours = phase.duration_days * 24 + phase.duration_hours;
            if (totalHours === 0) {
                showNotification(`Пожалуйста, укажите длительность для Фазы ${phaseNum}`, 'error');
                return;
            }
        }
    }
    
    // Convert to API format
    const apiSettings = {
        accounts: selectedWarmupAccounts,
        warmup_speed: settings.preset || currentPreset,
        phases: {},
        working_hours: {
            start: settings.workHours?.start || '09:00',
            end: settings.workHours?.end || '22:00',
            enable_breaks: settings.breaks?.enabled || true,
            work_period: settings.breaks?.workPeriod || 120,
            break_duration: settings.breaks?.breakDuration || 15
        },
        action_intervals: {
            min: settings.actionInterval?.min || 30,
            max: settings.actionInterval?.max || 120
        },
        max_concurrent_accounts: settings.max_concurrent_accounts || 3
    };
    
    // Convert phases to API format
    if (advancedSettingsVisible) {
        // Custom settings
        Object.keys(settings.phases).forEach(key => {
            const phase = settings.phases[key];
            apiSettings.phases[key] = {
                enabled: phase.enabled,
                min_daily: phase.min,
                max_daily: phase.max,
                duration_hours: phase.duration_days * 24 + phase.duration_hours
            };
        });
    } else {
        // Preset settings
        const preset = WARMUP_PRESETS[currentPreset];
        Object.keys(preset.phases).forEach(phaseNum => {
            const phase = preset.phases[phaseNum];
            apiSettings.phases[`phase${phaseNum}`] = {
                enabled: phase.enabled,
                min_daily: phase.min,
                max_daily: phase.max,
                duration_hours: phase.duration_days * 24 + phase.duration_hours
            };
        });
    }
    
    // Check if saving as template
    if (advancedSettingsVisible) {
        const saveAsTemplate = document.getElementById('save-as-template')?.checked;
        if (saveAsTemplate) {
            const templateName = document.getElementById('template-name')?.value;
            const templateDescription = document.getElementById('template-description')?.value;
            
            if (!templateName) {
                showNotification('Пожалуйста, введите название шаблона', 'error');
                return;
            }
            
            const template = {
                name: templateName,
                description: templateDescription,
                settings: apiSettings,
                created_at: new Date()
            };
            
            try {
                // Save template to API
                await api.saveWarmupTemplate(template);
                showNotification('Шаблон сохранен успешно', 'success');
                
                // Add to local templates
                warmupTemplates.push({
                    id: Date.now(),
                    ...template,
                    success_rate: 0 // Will be calculated based on usage
                });
            } catch (error) {
                console.error('Error saving template:', error);
                showNotification('Ошибка при сохранении шаблона', 'error');
            }
        }
    }
    
    // Save settings
    try {
        await api.saveWarmupSettings(apiSettings);
        showNotification('Настройки сохранены успешно', 'success');
        
        // Если были выбраны аккаунты, запускаем прогрев
        if (selectedWarmupAccounts.length > 0) {
            try {
                // Получаем настройки целевых аккаунтов
                const targetAccounts = document.getElementById('target-accounts')?.value || '';
                const uniqueFollows = document.getElementById('unique-follows')?.checked || false;
                
                // Запускаем прогрев для выбранных аккаунтов
                const response = await api.post('/warmup/start', {
                    account_ids: selectedWarmupAccounts.map(id => parseInt(id)),
                    settings: apiSettings,
                    target_accounts: targetAccounts,
                    unique_follows: uniqueFollows
                });
                
                if (response.success) {
                    let message = response.message || 'Прогрев успешно запущен!';
                    if (response.target_accounts_count) {
                        message += ` Целевых аккаунтов: ${response.target_accounts_count}`;
                    }
                    if (response.unique_follows_enabled) {
                        message += ' (уникальные подписки включены)';
                    }
                    showNotification(message, 'success');
                    
                    // Обновляем список процессов
                    await loadWarmupProcesses();
                    
                    // Переключаемся на вкладку активных процессов
                    switchTab('active');
                } else {
                    showNotification(response.error || 'Ошибка при запуске прогрева', 'error');
                }
            } catch (error) {
                console.error('Error starting warmup:', error);
                showNotification('Ошибка при запуске прогрева: ' + error.message, 'error');
            }
        }
        
        closeWarmupSettings();
    } catch (error) {
        console.error('Error saving settings:', error);
        showNotification('Ошибка при сохранении настроек', 'error');
    }
}

// Add notification function
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
    lucide.createIcons();
    
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

async function pauseWarmup(processId) {
    alert(`Приостановка прогрева ${processId}`);
}

async function stopWarmup(processId) {
    if (!confirm('Остановить прогрев? Прогресс будет потерян.')) return;
    
    try {
        // Находим процесс чтобы получить account_id
        const process = warmupProcesses.find(p => p.id === processId);
        if (!process) {
            showNotification('Процесс не найден', 'error');
            return;
        }
        
        // Получаем account_id из задачи
        const taskResponse = await api.get('/warmup/status');
        const task = taskResponse.data.find(t => t.id === processId);
        
        if (!task) {
            showNotification('Задача не найдена', 'error');
            return;
        }
        
        // Отправляем запрос на остановку
        const response = await api.post('/warmup/stop', {
            account_ids: [task.account_id]
        });
        
        if (response.success) {
            showNotification(response.message || 'Прогрев остановлен', 'success');
            // Обновляем список процессов
            await loadWarmupProcesses();
        } else {
            showNotification(response.error || 'Ошибка при остановке прогрева', 'error');
        }
    } catch (error) {
        console.error('Error stopping warmup:', error);
        showNotification('Ошибка при остановке прогрева', 'error');
    }
}

async function stopAllWarmup() {
    if (!confirm('Остановить прогрев всех аккаунтов? Весь прогресс будет потерян.')) return;
    
    try {
        // Получаем все активные задачи
        const taskResponse = await api.get('/warmup/status');
        const activeTasks = taskResponse.data.filter(t => 
            t.status === 'running' || t.status === 'pending' || t.status === 'RUNNING' || t.status === 'PENDING'
        );
        
        if (activeTasks.length === 0) {
            showNotification('Нет активных задач прогрева', 'warning');
            return;
        }
        
        // Собираем все account_ids
        const accountIds = activeTasks.map(t => t.account_id);
        
        // Отправляем запрос на остановку всех
        const response = await api.post('/warmup/stop', {
            account_ids: accountIds
        });
        
        if (response.success) {
            showNotification(`Остановлено ${accountIds.length} задач прогрева`, 'success');
            // Обновляем список процессов
            await loadWarmupProcesses();
        } else {
            showNotification(response.error || 'Ошибка при остановке прогрева', 'error');
        }
    } catch (error) {
        console.error('Error stopping all warmup:', error);
        showNotification('Ошибка при остановке всех задач прогрева', 'error');
    }
}

async function startSingleWarmup(processId) {
    alert(`Запуск прогрева ${processId}`);
}

async function removeFromQueue(processId) {
    if (!confirm('Удалить из очереди?')) return;
    alert(`Удаление из очереди ${processId}`);
}

function viewWarmupReport(processId) {
    alert(`Просмотр отчета ${processId} (будет реализовано)`);
}

function useTemplate(templateId) {
    const template = warmupTemplates.find(t => t.id === templateId);
    if (!template) return;
    
    // Open settings modal
    openWarmupSettings();
    
    // Load template values
    if (template.settings) {
        // Phase 1 settings
        if (template.settings.phases.phase1) {
            document.getElementById('phase1-enabled').checked = template.settings.phases.phase1.enabled !== false;
            togglePhase(1, true);
            
            document.getElementById('phase1-min').value = template.settings.phases.phase1.min_daily || 10;
            document.getElementById('phase1-max').value = template.settings.phases.phase1.max_daily || 25;
            
            // Convert hours to days and hours
            const phase1Hours = template.settings.phases.phase1.duration_hours || 168; // 7 days default
            document.getElementById('phase1-duration-days').value = Math.floor(phase1Hours / 24);
            document.getElementById('phase1-duration-hours').value = phase1Hours % 24;
            
            // Load accounts if available
            if (template.settings.phases.phase1.accounts) {
                document.getElementById('phase1-accounts').value = template.settings.phases.phase1.accounts.join('\n');
            }
        }
        
        // Phase 2 settings
        if (template.settings.phases.phase2) {
            document.getElementById('phase2-enabled').checked = template.settings.phases.phase2.enabled !== false;
            togglePhase(2, true);
            
            document.getElementById('phase2-min').value = template.settings.phases.phase2.min_daily || 50;
            document.getElementById('phase2-max').value = template.settings.phases.phase2.max_daily || 100;
            
            const phase2Hours = template.settings.phases.phase2.duration_hours || 120; // 5 days default
            document.getElementById('phase2-duration-days').value = Math.floor(phase2Hours / 24);
            document.getElementById('phase2-duration-hours').value = phase2Hours % 24;
            
            // Load like sources
            if (template.settings.phases.phase2.sources) {
                document.getElementById('like-following-posts').checked = template.settings.phases.phase2.sources.following_posts !== false;
                document.getElementById('like-explore-reels').checked = template.settings.phases.phase2.sources.explore_reels !== false;
                document.getElementById('like-explore-posts').checked = template.settings.phases.phase2.sources.explore_posts !== false;
            }
        }
        
        // Working hours
        document.getElementById('work-start').value = template.settings.working_hours?.start || '09:00';
        document.getElementById('work-end').value = template.settings.working_hours?.end || '21:00';
        
        // Action intervals
        document.getElementById('action-interval-min').value = template.settings.action_intervals?.min || 30;
        document.getElementById('action-interval-max').value = template.settings.action_intervals?.max || 120;
    } else {
        // Use legacy template format
        const phase1 = template.phases.find(p => p.name === 'Подписки');
        const phase2 = template.phases.find(p => p.name === 'Лайки');
        
        if (phase1) {
            document.getElementById('phase1-min').value = phase1.min_daily;
            document.getElementById('phase1-max').value = phase1.max_daily;
            document.getElementById('phase1-duration-days').value = phase1.duration;
            document.getElementById('phase1-duration-hours').value = 0;
        }
        
        if (phase2) {
            document.getElementById('phase2-min').value = phase2.min_daily;
            document.getElementById('phase2-max').value = phase2.max_daily;
            document.getElementById('phase2-duration-days').value = phase2.duration;
            document.getElementById('phase2-duration-hours').value = 0;
        }
    }
    
    showNotification(`Шаблон "${template.name}" загружен`, 'success');
}

// Select warmup preset
function selectWarmupPreset(presetName) {
    currentPreset = presetName;
    const preset = WARMUP_PRESETS[presetName];
    
    if (!preset) return;
    
    // Apply preset settings to form fields (only if advanced settings are visible)
    if (advancedSettingsVisible) {
        applyPresetToForm(preset);
    }
    
    console.log(`Selected preset: ${preset.name} (${preset.duration} days)`);
}

// Apply preset configuration to form fields
function applyPresetToForm(preset) {
    // Apply phase settings
    Object.keys(preset.phases).forEach(phaseNum => {
        const phase = preset.phases[phaseNum];
        const phaseId = `phase${phaseNum}`;
        
        // Enable/disable phase
        const enabledCheckbox = document.getElementById(`${phaseId}-enabled`);
        if (enabledCheckbox) {
            enabledCheckbox.checked = phase.enabled;
            togglePhase(parseInt(phaseNum));
        }
        
        // Set min/max values
        const minInput = document.getElementById(`${phaseId}-min`);
        const maxInput = document.getElementById(`${phaseId}-max`);
        const daysInput = document.getElementById(`${phaseId}-duration-days`);
        const hoursInput = document.getElementById(`${phaseId}-duration-hours`);
        
        if (minInput) minInput.value = phase.min;
        if (maxInput) maxInput.value = phase.max;
        if (daysInput) daysInput.value = phase.duration_days;
        if (hoursInput) hoursInput.value = phase.duration_hours;
    });
    
    // Apply work hours
    const workStartInput = document.getElementById('work-start');
    const workEndInput = document.getElementById('work-end');
    if (workStartInput) workStartInput.value = preset.workHours.start;
    if (workEndInput) workEndInput.value = preset.workHours.end;
    
    // Apply breaks settings
    const enableBreaksCheckbox = document.getElementById('enable-breaks');
    const workPeriodInput = document.getElementById('work-period');
    const breakDurationInput = document.getElementById('break-duration');
    
    if (enableBreaksCheckbox) enableBreaksCheckbox.checked = preset.breaks.enabled;
    if (workPeriodInput) workPeriodInput.value = preset.breaks.workPeriod;
    if (breakDurationInput) breakDurationInput.value = preset.breaks.breakDuration;
    
    // Apply action intervals
    const actionMinInput = document.getElementById('action-interval-min');
    const actionMaxInput = document.getElementById('action-interval-max');
    
    if (actionMinInput) actionMinInput.value = preset.actionInterval.min;
    if (actionMaxInput) actionMaxInput.value = preset.actionInterval.max;
}

// Toggle advanced settings visibility
function toggleAdvancedSettings() {
    const advancedSettings = document.getElementById('advanced-settings');
    const toggleButton = document.getElementById('toggle-advanced-settings');
    const chevronIcon = document.getElementById('advanced-chevron');
    
    if (!advancedSettings) return;
    
    advancedSettingsVisible = !advancedSettingsVisible;
    
    if (advancedSettingsVisible) {
        advancedSettings.classList.remove('hidden');
        toggleButton.querySelector('span').textContent = 'Скрыть расширенные настройки';
        chevronIcon.style.transform = 'rotate(180deg)';
        
        // Apply current preset to form
        const preset = WARMUP_PRESETS[currentPreset];
        if (preset) {
            applyPresetToForm(preset);
        }
    } else {
        advancedSettings.classList.add('hidden');
        toggleButton.querySelector('span').textContent = 'Расширенные настройки';
        chevronIcon.style.transform = 'rotate(0deg)';
    }
}

// Get warmup settings (updated to use presets)
function getWarmupSettings() {
    if (!advancedSettingsVisible) {
        // Use preset configuration
        const preset = WARMUP_PRESETS[currentPreset];
        return {
            preset: currentPreset,
            ...preset,
            accounts: getSelectedAccounts(),
            max_concurrent_accounts: parseInt(document.getElementById('max-concurrent-accounts')?.value || '3')
        };
    }
    
    // Use custom configuration from form
    return {
        preset: 'CUSTOM',
        name: 'Пользовательские настройки',
        phases: {
            1: getPhaseSettings(1),
            2: getPhaseSettings(2),
            3: getPhaseSettings(3),
            4: getPhaseSettings(4)
        },
        workHours: {
            start: document.getElementById('work-start')?.value || '09:00',
            end: document.getElementById('work-end')?.value || '22:00'
        },
        breaks: {
            enabled: document.getElementById('enable-breaks')?.checked || true,
            workPeriod: parseInt(document.getElementById('work-period')?.value || '120'),
            breakDuration: parseInt(document.getElementById('break-duration')?.value || '15')
        },
        actionInterval: {
            min: parseInt(document.getElementById('action-interval-min')?.value || '30'),
            max: parseInt(document.getElementById('action-interval-max')?.value || '120')
        },
        accounts: getSelectedAccounts(),
        max_concurrent_accounts: parseInt(document.getElementById('max-concurrent-accounts')?.value || '3')
    };
}

// Helper function to get selected accounts
function getSelectedAccounts() {
    return selectedWarmupAccounts;
}

// Helper function to get phase settings
function getPhaseSettings(phaseNumber) {
    const enabledCheckbox = document.getElementById(`phase${phaseNumber}-enabled`);
    const minInput = document.getElementById(`phase${phaseNumber}-min`);
    const maxInput = document.getElementById(`phase${phaseNumber}-max`);
    const daysInput = document.getElementById(`phase${phaseNumber}-duration-days`);
    const hoursInput = document.getElementById(`phase${phaseNumber}-duration-hours`);
    
    return {
        enabled: enabledCheckbox?.checked || false,
        min: parseInt(minInput?.value || '0'),
        max: parseInt(maxInput?.value || '0'),
        duration_days: parseInt(daysInput?.value || '0'),
        duration_hours: parseInt(hoursInput?.value || '0')
    };
}

// Deselect all accounts
function deselectAllWarmupAccounts() {
    // Очищаем массив выбранных аккаунтов
    selectedWarmupAccounts = [];
    
    // Снимаем отметки со всех чекбоксов
    const checkboxes = document.querySelectorAll('.warmup-account-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
    });
    
    // Обновляем отображение
    updateSelectedAccountsDisplay();
}

// Show preset information modal
function showPresetInfo(presetType) {
    const modal = document.getElementById('preset-info-modal');
    const title = document.getElementById('preset-info-title');
    const content = document.getElementById('preset-info-content');
    
    const presetInfo = {
        'SLOW': {
            title: '🐢 Медленный режим (~21 день)',
            content: `
                <div class="space-y-3">
                    <div class="flex items-center gap-2 text-green-400">
                        <i data-lucide="shield-check" class="h-5 w-5"></i>
                        <span class="font-medium">Максимальная безопасность</span>
                    </div>
                    <div class="text-sm space-y-2">
                        <p><strong>Активность:</strong> 1-3 действия в час</p>
                        <p><strong>Длительность:</strong> ~21 день</p>
                        <p><strong>Рекомендуется для:</strong> Новых аккаунтов, аккаунты с ограничениями</p>
                    </div>
                    <div class="bg-slate-700 rounded-lg p-3">
                        <h4 class="text-white font-medium mb-2">Последовательность действий:</h4>
                        <ul class="text-sm text-slate-300 space-y-1">
                            <li>• Просмотр ленты → Лайки (2-5) → Отдых 15-30 мин</li>
                            <li>• Подписка (1-2) → Просмотр профилей → Отдых 20-40 мин</li>
                            <li>• Просмотр историй → Сохранение постов → Отдых 30-60 мин</li>
                            <li>• Просмотр Reels → Лайки → Длинный отдых 1-2 часа</li>
                        </ul>
                    </div>
                    <div class="bg-green-500/10 border border-green-500/20 rounded-lg p-3">
                        <p class="text-green-300 text-sm">
                            <i data-lucide="info" class="h-4 w-4 inline mr-1"></i>
                            Имитирует поведение осторожного пользователя с длинными паузами
                        </p>
                    </div>
                </div>
            `
        },
        'NORMAL': {
            title: '⚡ Обычный режим (~14 дней)',
            content: `
                <div class="space-y-3">
                    <div class="flex items-center gap-2 text-blue-400">
                        <i data-lucide="zap" class="h-5 w-5"></i>
                        <span class="font-medium">Оптимальный баланс</span>
                    </div>
                    <div class="text-sm space-y-2">
                        <p><strong>Активность:</strong> 3-8 действий в час</p>
                        <p><strong>Длительность:</strong> ~14 дней</p>
                        <p><strong>Рекомендуется для:</strong> Большинства аккаунтов</p>
                    </div>
                    <div class="bg-slate-700 rounded-lg p-3">
                        <h4 class="text-white font-medium mb-2">Последовательность действий:</h4>
                        <ul class="text-sm text-slate-300 space-y-1">
                            <li>• Просмотр ленты → Лайки (3-8) → Подписка → Отдых 10-20 мин</li>
                            <li>• Просмотр историй → Комментарий → Отдых 15-25 мин</li>
                            <li>• Публикация истории → Просмотр Reels → Отдых 20-40 мин</li>
                            <li>• Лайки → Сообщения → Сохранения → Отдых 30-60 мин</li>
                        </ul>
                    </div>
                    <div class="bg-blue-500/10 border border-blue-500/20 rounded-lg p-3">
                        <p class="text-blue-300 text-sm">
                            <i data-lucide="info" class="h-4 w-4 inline mr-1"></i>
                            Имитирует поведение обычного активного пользователя
                        </p>
                    </div>
                </div>
            `
        },
        'FAST': {
            title: '🚀 Быстрый режим (~7 дней)',
            content: `
                <div class="space-y-3">
                    <div class="flex items-center gap-2 text-orange-400">
                        <i data-lucide="rocket" class="h-5 w-5"></i>
                        <span class="font-medium">Для опытных аккаунтов</span>
                    </div>
                    <div class="text-sm space-y-2">
                        <p><strong>Активность:</strong> 8-15 действий в час</p>
                        <p><strong>Длительность:</strong> ~7 дней</p>
                        <p><strong>Рекомендуется для:</strong> Аккаунты с историей активности</p>
                    </div>
                    <div class="bg-slate-700 rounded-lg p-3">
                        <h4 class="text-white font-medium mb-2">Последовательность действий:</h4>
                        <ul class="text-sm text-slate-300 space-y-1">
                            <li>• Серия лайков (5-12) → Подписки (2-4) → Отдых 8-15 мин</li>
                            <li>• Комментарии (1-3) → Просмотр историй → Отдых 10-20 мин</li>
                            <li>• Публикация истории → Активные сообщения → Отдых 15-30 мин</li>
                            <li>• Просмотр Reels → Лайки → Сохранения → Отдых 20-40 мин</li>
                        </ul>
                    </div>
                    <div class="bg-orange-500/10 border border-orange-500/20 rounded-lg p-3">
                        <p class="text-orange-300 text-sm">
                            <i data-lucide="alert-triangle" class="h-4 w-4 inline mr-1"></i>
                            Имитирует поведение очень активного пользователя
                        </p>
                    </div>
                </div>
            `
        },
        'SUPER_FAST': {
            title: '🔥 Супер быстрый режим (2-4 дня)',
            content: `
                <div class="space-y-3">
                    <div class="flex items-center gap-2 text-red-400">
                        <i data-lucide="flame" class="h-5 w-5"></i>
                        <span class="font-medium">Агрессивный режим</span>
                    </div>
                    <div class="text-sm space-y-2">
                        <p><strong>Активность:</strong> 15-30 действий в час</p>
                        <p><strong>Длительность:</strong> 2-4 дня</p>
                        <p><strong>Рекомендуется для:</strong> Только опытные пользователи</p>
                    </div>
                    <div class="bg-slate-700 rounded-lg p-3">
                        <h4 class="text-white font-medium mb-2">Последовательность действий:</h4>
                        <ul class="text-sm text-slate-300 space-y-1">
                            <li>• Интенсивные лайки (10-20) → Подписки (3-6) → Отдых 5-10 мин</li>
                            <li>• Комментарии (2-5) → Публикация историй → Отдых 8-15 мин</li>
                            <li>• Максимальная активность → Все действия → Отдых 10-20 мин</li>
                            <li>• ⚠️ Высокий риск обнаружения алгоритмами</li>
                        </ul>
                    </div>
                    <div class="bg-red-500/10 border border-red-500/20 rounded-lg p-3">
                        <p class="text-red-300 text-sm">
                            <i data-lucide="alert-triangle" class="h-4 w-4 inline mr-1"></i>
                            <strong>ВНИМАНИЕ:</strong> Имитирует поведение бота! Используйте только для тестов
                        </p>
                    </div>
                </div>
            `
        },
        'DAILY': {
            title: '⏰ Дневной режим (1 день)',
            content: `
                <div class="space-y-3">
                    <div class="flex items-center gap-2 text-purple-400">
                        <i data-lucide="clock" class="h-5 w-5"></i>
                        <span class="font-medium">Быстрый прогрев</span>
                    </div>
                    <div class="text-sm space-y-2">
                        <p><strong>Активность:</strong> 5-12 действий в час</p>
                        <p><strong>Длительность:</strong> 1 день</p>
                        <p><strong>Рекомендуется для:</strong> Быстрый прогрев за день</p>
                    </div>
                    <div class="bg-slate-700 rounded-lg p-3">
                        <h4 class="text-white font-medium mb-2">Последовательность действий:</h4>
                        <ul class="text-sm text-slate-300 space-y-1">
                            <li>• Утро: Просмотр ленты → Лайки (4-8) → Подписки (2-4)</li>
                            <li>• День: Комментарии → Просмотр историй → Отдых 15-30 мин</li>
                            <li>• Вечер: Публикация истории → Reels → Сообщения</li>
                            <li>• Имитация естественного дневного ритма</li>
                        </ul>
                    </div>
                    <div class="bg-purple-500/10 border border-purple-500/20 rounded-lg p-3">
                        <p class="text-purple-300 text-sm">
                            <i data-lucide="info" class="h-4 w-4 inline mr-1"></i>
                            Имитирует поведение пользователя в течение одного активного дня
                        </p>
                    </div>
                </div>
            `
        }
    };
    
    const info = presetInfo[presetType];
    if (info) {
        title.textContent = info.title;
        content.innerHTML = info.content;
        modal.classList.remove('hidden');
        
        // Re-initialize lucide icons in the modal content
        setTimeout(() => {
            lucide.createIcons();
        }, 100);
    }
}

// Close preset info modal
function closePresetInfo() {
    const modal = document.getElementById('preset-info-modal');
    modal.classList.add('hidden');
}

// Close modal when clicking outside
document.addEventListener('click', function(event) {
    const modal = document.getElementById('preset-info-modal');
    if (event.target === modal) {
        closePresetInfo();
    }
});

// Update distribution info based on target accounts and selected accounts
function updateDistributionInfo() {
    const targetAccountsText = document.getElementById('target-accounts')?.value || '';
    const uniqueFollows = document.getElementById('unique-follows')?.checked || false;
    const distributionText = document.getElementById('distribution-text');
    
    if (!distributionText) return;
    
    // Parse target accounts
    const targetAccounts = targetAccountsText
        .split('\n')
        .map(line => line.trim())
        .filter(line => line.length > 0)
        .map(line => line.startsWith('@') ? line : '@' + line);
    
    const selectedAccountsCount = selectedWarmupAccounts.length;
    
    if (targetAccounts.length === 0) {
        distributionText.textContent = 'Добавьте аккаунты для подписки, чтобы увидеть распределение';
        return;
    }
    
    if (selectedAccountsCount === 0) {
        distributionText.innerHTML = `
            <div class="text-slate-400">
                <div>📊 Целевых аккаунтов: <span class="text-white">${targetAccounts.length}</span></div>
                <div class="mt-1 text-xs">Выберите аккаунты для прогрева, чтобы увидеть распределение</div>
            </div>
        `;
        return;
    }
    
    if (uniqueFollows) {
        const accountsPerTarget = Math.ceil(selectedAccountsCount / targetAccounts.length);
        const targetsPerAccount = Math.ceil(targetAccounts.length / selectedAccountsCount);
        
        distributionText.innerHTML = `
            <div class="text-slate-300">
                <div class="flex items-center gap-4 mb-2">
                    <div>📊 Целевых аккаунтов: <span class="text-white">${targetAccounts.length}</span></div>
                    <div>👥 Выбрано аккаунтов: <span class="text-white">${selectedAccountsCount}</span></div>
                </div>
                <div class="text-xs text-slate-400">
                    <div>🔄 <strong>Уникальное распределение:</strong></div>
                    <div class="mt-1">• Каждый аккаунт подпишется на ~${targetsPerAccount} целевых аккаунтов</div>
                    <div>• На каждый целевой аккаунт подпишется ~${accountsPerTarget} ваших аккаунтов</div>
                </div>
            </div>
        `;
    } else {
        distributionText.innerHTML = `
            <div class="text-slate-300">
                <div class="flex items-center gap-4 mb-2">
                    <div>📊 Целевых аккаунтов: <span class="text-white">${targetAccounts.length}</span></div>
                    <div>👥 Выбрано аккаунтов: <span class="text-white">${selectedAccountsCount}</span></div>
                </div>
                <div class="text-xs text-slate-400">
                    <div>🔁 <strong>Обычное распределение:</strong></div>
                    <div class="mt-1">• Все аккаунты будут подписываться на одни и те же целевые аккаунты</div>
                    <div>• На каждый целевой аккаунт подпишется ${selectedAccountsCount} ваших аккаунтов</div>
                </div>
            </div>
        `;
    }
}

function renderWarmupCard(process, isActive = false) {
    const statusLower = process.status.toLowerCase();
    const statusColor = getStatusColor(process.status);
    const statusText = getStatusText(process.status);
    
    // Проверяем прогресс
    const progress = process.progress || {};
    const currentPhase = progress.current_phase || 'phase1';
    const phaseNames = {
        'phase1': 'Подписки',
        'phase2': 'Лайки', 
        'phase3': 'Комментарии',
        'phase4': 'Истории',
        'completed': 'Завершен'
    };
    
    const phaseName = phaseNames[currentPhase] || currentPhase;
    
    // Статистика действий
    const stats = process.stats || {};
    const totalActions = progress.total_actions || {};
    
    return `
        <div class="bg-slate-800 border border-slate-700 rounded-lg p-4 hover:border-slate-600 transition-colors">
            <div class="flex items-start justify-between mb-3">
                <div class="flex items-center gap-3">
                    <div class="w-10 h-10 bg-slate-700 rounded-full flex items-center justify-center">
                        <span class="text-white font-semibold">${process.username.charAt(0).toUpperCase()}</span>
                    </div>
                    <div>
                        <h4 class="text-white font-medium">@${process.username}</h4>
                        <div class="flex items-center gap-2 mt-1">
                            <span class="px-2 py-1 text-xs rounded-full ${statusColor}">
                                ${statusText}
                            </span>
                            ${isActive ? '<span class="text-xs text-slate-400">• Активен</span>' : ''}
                        </div>
                    </div>
                </div>
                <div class="flex items-center gap-2">
                    ${statusLower === 'running' || statusLower === 'pending' ? `
                        <button onclick="stopWarmup(${process.id})" class="p-2 text-red-400 hover:bg-red-500/20 rounded-md transition-colors">
                            <i data-lucide="square" class="h-4 w-4"></i>
                        </button>
                    ` : ''}
                    <button class="p-2 text-slate-400 hover:bg-slate-700 rounded-md transition-colors">
                        <i data-lucide="more-vertical" class="h-4 w-4"></i>
                    </button>
                </div>
            </div>
            
            <!-- Current Phase -->
            <div class="mb-3">
                <div class="flex items-center justify-between text-sm mb-1">
                    <span class="text-slate-400">Текущая фаза</span>
                    <span class="text-white font-medium">${phaseName}</span>
                </div>
                ${currentPhase !== 'completed' ? `
                    <div class="w-full bg-slate-700 rounded-full h-2">
                        <div class="bg-blue-500 h-2 rounded-full" style="width: ${getPhaseProgress(currentPhase)}%"></div>
                    </div>
                ` : ''}
            </div>
            
            <!-- Statistics -->
            <div class="grid grid-cols-5 gap-2 text-center">
                <div class="bg-slate-700/50 rounded-lg p-2">
                    <div class="text-xs text-slate-400">Подписки</div>
                    <div class="text-sm font-semibold text-white">${totalActions.follow || 0}</div>
                </div>
                <div class="bg-slate-700/50 rounded-lg p-2">
                    <div class="text-xs text-slate-400">Лайки</div>
                    <div class="text-sm font-semibold text-white">${totalActions.like_posts || 0}</div>
                </div>
                <div class="bg-slate-700/50 rounded-lg p-2">
                    <div class="text-xs text-slate-400">Комменты</div>
                    <div class="text-sm font-semibold text-white">${totalActions.comment || 0}</div>
                </div>
                <div class="bg-slate-700/50 rounded-lg p-2">
                    <div class="text-xs text-slate-400">Истории</div>
                    <div class="text-sm font-semibold text-white">${totalActions.view_stories || 0}</div>
                </div>
                <div class="bg-slate-700/50 rounded-lg p-2">
                    <div class="text-xs text-slate-400">Сохранено</div>
                    <div class="text-sm font-semibold text-white">${totalActions.save_posts || 0}</div>
                </div>
            </div>
            
            <!-- Last Activity -->
            ${progress.last_session ? `
                <div class="mt-3 pt-3 border-t border-slate-700">
                    <div class="flex items-center justify-between text-xs">
                        <span class="text-slate-400">Последняя активность</span>
                        <span class="text-slate-300">${formatRelativeTime(progress.last_session)}</span>
                    </div>
                </div>
            ` : ''}
        </div>
    `;
}

function getStatusColor(status) {
    const statusLower = status.toLowerCase();
    const colors = {
        'running': 'bg-green-500/20 text-green-400',
        'pending': 'bg-blue-500/20 text-blue-400',
        'completed': 'bg-slate-500/20 text-slate-400',
        'failed': 'bg-red-500/20 text-red-400',
        'cancelled': 'bg-orange-500/20 text-orange-400'
    };
    return colors[statusLower] || 'bg-slate-500/20 text-slate-400';
}

function getStatusText(status) {
    const statusLower = status.toLowerCase();
    const texts = {
        'running': 'Активен',
        'pending': 'В очереди',
        'completed': 'Завершен',
        'failed': 'Ошибка',
        'cancelled': 'Остановлен'
    };
    return texts[statusLower] || status;
}

function getPhaseProgress(phase) {
    // Простая логика прогресса по фазам
    const progress = {
        'phase1': 25,
        'phase2': 50,
        'phase3': 75,
        'phase4': 90,
        'completed': 100
    };
    return progress[phase] || 0;
}

function formatRelativeTime(dateString) {
    const date = new Date(dateString);
    const now = new Date();
    const diff = now - date;
    
    const minutes = Math.floor(diff / 60000);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    
    if (days > 0) return `${days} дн. назад`;
    if (hours > 0) return `${hours} ч. назад`;
    if (minutes > 0) return `${minutes} мин. назад`;
    return 'Только что';
} 