// Comments functionality
let campaigns = [];
let templates = [];
let accounts = [];

// Initialize page
document.addEventListener('DOMContentLoaded', async () => {
    await loadAccounts();
    await loadTemplates();
    await loadCampaigns();
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
    const select = document.getElementById('campaign-account');
    if (!select) return;
    
    select.innerHTML = '<option value="">Выберите аккаунт</option>';
    accounts.forEach(account => {
        const option = document.createElement('option');
        option.value = account.id;
        option.textContent = `@${account.username}`;
        select.appendChild(option);
    });
}

async function loadTemplates() {
    // TODO: Load templates from API
    // For now, add sample template
    templates = [{
        id: 1,
        name: 'Продающий комментарий',
        text: 'Отличный пост! Кстати, у нас есть {product} который идеально подойдет для {target}. Заходите в профиль 🔥',
        variables: ['product', 'target'],
        usage_count: 0
    }];
    
    updateTemplatesGrid();
}

function updateTemplatesGrid() {
    const grid = document.getElementById('templates-grid');
    if (!grid) return;
    
    grid.innerHTML = templates.map(template => `
        <div class="bg-slate-700/50 rounded-lg p-4 border border-slate-600">
            <div class="flex justify-between items-start mb-2">
                <h4 class="font-medium text-white">${template.name}</h4>
                <div class="flex gap-1">
                    <button onclick="editTemplate(${template.id})" class="text-slate-400 hover:text-white">
                        <i data-lucide="edit-2" class="h-4 w-4"></i>
                    </button>
                    <button onclick="deleteTemplate(${template.id})" class="text-slate-400 hover:text-red-400">
                        <i data-lucide="trash-2" class="h-4 w-4"></i>
                    </button>
                </div>
            </div>
            <p class="text-sm text-slate-300 mb-3">${template.text}</p>
            <div class="flex items-center gap-2 text-xs text-slate-400">
                <span class="bg-slate-600 px-2 py-1 rounded">${template.variables.length} переменные</span>
                <span>Использован ${template.usage_count} раз</span>
            </div>
        </div>
    `).join('') + `
        <div onclick="addTemplate()" class="bg-slate-700/30 rounded-lg p-4 border-2 border-dashed border-slate-600 hover:border-slate-500 cursor-pointer transition-colors flex items-center justify-center min-h-[120px]">
            <div class="text-center">
                <i data-lucide="plus-circle" class="h-8 w-8 text-slate-400 mx-auto mb-2"></i>
                <p class="text-slate-400">Добавить шаблон</p>
            </div>
        </div>
    `;
    
    lucide.createIcons();
}

async function loadCampaigns() {
    // TODO: Load campaigns from API
    updateCampaignsTable();
}

function updateCampaignsTable() {
    const tbody = document.getElementById('campaigns-table');
    if (!tbody) return;
    
    if (campaigns.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="px-6 py-12 text-center text-slate-400">
                    <i data-lucide="message-square-off" class="h-12 w-12 mx-auto mb-3 text-slate-600"></i>
                    <p>Нет активных кампаний</p>
                    <button onclick="createCommentCampaign()" class="mt-3 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md transition-colors text-sm">
                        Создать первую кампанию
                    </button>
                </td>
            </tr>
        `;
    } else {
        tbody.innerHTML = campaigns.map(campaign => `
            <tr>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-white">${campaign.name}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">@${campaign.account}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${campaign.targeting}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${campaign.comments_sent}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${campaign.conversion}%</td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <span class="px-2 py-1 text-xs leading-5 font-semibold rounded-full ${getStatusClass(campaign.status)}">
                        ${getStatusText(campaign.status)}
                    </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <button onclick="toggleCampaign(${campaign.id})" class="text-blue-400 hover:text-blue-300 mr-3">
                        <i data-lucide="${campaign.status === 'active' ? 'pause' : 'play'}" class="h-4 w-4"></i>
                    </button>
                    <button onclick="deleteCampaign(${campaign.id})" class="text-red-400 hover:text-red-300">
                        <i data-lucide="trash-2" class="h-4 w-4"></i>
                    </button>
                </td>
            </tr>
        `).join('');
    }
    
    lucide.createIcons();
}

function setupEventListeners() {
    const form = document.getElementById('create-campaign-form');
    if (form) {
        form.addEventListener('submit', handleCreateCampaign);
    }
    
    // Generation method radio buttons
    const generationMethods = document.querySelectorAll('input[name="generation-method"]');
    generationMethods.forEach(radio => {
        radio.addEventListener('change', toggleAISettings);
    });
}

function createCommentCampaign() {
    document.getElementById('create-campaign-modal').classList.remove('hidden');
}

function closeCampaignModal() {
    document.getElementById('create-campaign-modal').classList.add('hidden');
    document.getElementById('create-campaign-form').reset();
}

function toggleAISettings() {
    const method = document.querySelector('input[name="generation-method"]:checked').value;
    const aiSettings = document.getElementById('ai-settings');
    
    if (method === 'ai' || method === 'mixed') {
        aiSettings.classList.remove('hidden');
    } else {
        aiSettings.classList.add('hidden');
    }
}

async function handleCreateCampaign(e) {
    e.preventDefault();
    
    const campaignData = {
        name: document.getElementById('campaign-name').value,
        account_id: document.getElementById('campaign-account').value,
        targeting: {
            hashtags: document.getElementById('target-hashtags').value.split(',').map(h => h.trim()).filter(h => h),
            accounts: document.getElementById('target-accounts').value.split(',').map(a => a.trim()).filter(a => a),
            keywords: document.getElementById('target-keywords').value.split(',').map(k => k.trim()).filter(k => k)
        },
        generation_method: document.querySelector('input[name="generation-method"]:checked').value,
        ai_settings: {
            context: document.getElementById('ai-context')?.value || '',
            tone: document.getElementById('ai-tone')?.value || 'friendly'
        },
        comments_per_hour: parseInt(document.getElementById('comments-per-hour').value),
        daily_limit: parseInt(document.getElementById('daily-limit').value)
    };
    
    try {
        showNotification('Создание кампании...', 'info');
        
        // TODO: Implement API call
        // const result = await api.createCommentCampaign(campaignData);
        
        // For now, just add to local array
        campaigns.push({
            id: Date.now(),
            ...campaignData,
            comments_sent: 0,
            conversion: 0,
            status: 'active',
            account: accounts.find(a => a.id == campaignData.account_id)?.username || 'unknown',
            targeting: `${campaignData.targeting.hashtags.length} хештегов, ${campaignData.targeting.accounts.length} аккаунтов`
        });
        
        updateCampaignsTable();
        updateStats();
        closeCampaignModal();
        showNotification('Кампания создана успешно', 'success');
    } catch (error) {
        console.error('Error creating campaign:', error);
        showNotification('Ошибка при создании кампании', 'error');
    }
}

function addTemplate() {
    // TODO: Implement template creation modal
    showNotification('Функция создания шаблонов будет доступна в следующей версии', 'info');
}

function editTemplate(templateId) {
    // TODO: Implement template editing
    showNotification('Функция редактирования шаблонов будет доступна в следующей версии', 'info');
}

function deleteTemplate(templateId) {
    if (confirm('Удалить этот шаблон?')) {
        templates = templates.filter(t => t.id !== templateId);
        updateTemplatesGrid();
        showNotification('Шаблон удален', 'success');
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

function toggleCampaign(campaignId) {
    const campaign = campaigns.find(c => c.id === campaignId);
    if (campaign) {
        campaign.status = campaign.status === 'active' ? 'paused' : 'active';
        updateCampaignsTable();
        showNotification(`Кампания ${campaign.status === 'active' ? 'запущена' : 'приостановлена'}`, 'info');
    }
}

function deleteCampaign(campaignId) {
    if (confirm('Удалить эту кампанию?')) {
        campaigns = campaigns.filter(c => c.id !== campaignId);
        updateCampaignsTable();
        showNotification('Кампания удалена', 'success');
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