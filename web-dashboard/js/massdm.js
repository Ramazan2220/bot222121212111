// Mass DM functionality
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
    const select = document.getElementById('sender-account');
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
        name: 'Приветственное сообщение',
        text: 'Привет, {name}! 👋 Заметил(а), что ты интересуешься {interest}. У нас есть отличное предложение...',
        variables: ['name', 'interest'],
        ctr: 0
    }];
    
    updateTemplatesDisplay();
}

function updateTemplatesDisplay() {
    // Update template select
    const select = document.getElementById('message-template');
    if (select) {
        select.innerHTML = '<option value="">Выберите шаблон или создайте новое</option>';
        templates.forEach(template => {
            const option = document.createElement('option');
            option.value = template.id;
            option.textContent = template.name;
            select.appendChild(option);
        });
        select.innerHTML += '<option value="custom">Свой текст</option>';
    }
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
                <td colspan="8" class="px-6 py-12 text-center text-slate-400">
                    <i data-lucide="mail-x" class="h-12 w-12 mx-auto mb-3 text-slate-600"></i>
                    <p>Нет активных рассылок</p>
                    <button onclick="createMassDMCampaign()" class="mt-3 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md transition-colors text-sm">
                        Создать первую рассылку
                    </button>
                </td>
            </tr>
        `;
    } else {
        tbody.innerHTML = campaigns.map(campaign => `
            <tr>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-white">${campaign.name}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">@${campaign.account}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${campaign.audience}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${campaign.sent}/${campaign.total}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${campaign.delivered}%</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-slate-300">${campaign.replies}</td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <span class="px-2 py-1 text-xs leading-5 font-semibold rounded-full ${getStatusClass(campaign.status)}">
                        ${getStatusText(campaign.status)}
                    </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <button onclick="toggleCampaign(${campaign.id})" class="text-blue-400 hover:text-blue-300 mr-3">
                        <i data-lucide="${campaign.status === 'active' ? 'pause' : 'play'}" class="h-4 w-4"></i>
                    </button>
                    <button onclick="viewCampaignDetails(${campaign.id})" class="text-green-400 hover:text-green-300 mr-3">
                        <i data-lucide="eye" class="h-4 w-4"></i>
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
    
    // Audience type select
    const audienceType = document.getElementById('audience-type');
    if (audienceType) {
        audienceType.addEventListener('change', toggleAudienceInput);
    }
    
    // Message template select
    const messageTemplate = document.getElementById('message-template');
    if (messageTemplate) {
        messageTemplate.addEventListener('change', loadTemplate);
    }
    
    // Message text input
    const messageText = document.getElementById('message-text');
    if (messageText) {
        messageText.addEventListener('input', updateMessageLength);
    }
}

function createMassDMCampaign() {
    document.getElementById('create-campaign-modal').classList.remove('hidden');
}

function closeCampaignModal() {
    document.getElementById('create-campaign-modal').classList.add('hidden');
    document.getElementById('create-campaign-form').reset();
}

function toggleAudienceInput() {
    const audienceType = document.getElementById('audience-type').value;
    
    // Hide all audience inputs
    document.getElementById('custom-audience').classList.add('hidden');
    document.getElementById('import-audience').classList.add('hidden');
    
    // Show relevant input
    switch(audienceType) {
        case 'custom':
            document.getElementById('custom-audience').classList.remove('hidden');
            break;
        case 'import':
            document.getElementById('import-audience').classList.remove('hidden');
            break;
    }
}

function loadTemplate() {
    const templateId = document.getElementById('message-template').value;
    const messageText = document.getElementById('message-text');
    
    if (templateId && templateId !== 'custom') {
        const template = templates.find(t => t.id == templateId);
        if (template) {
            messageText.value = template.text;
            updateMessageLength();
        }
    } else if (templateId === 'custom') {
        messageText.value = '';
        messageText.focus();
    }
}

function insertVariable(variable) {
    const messageText = document.getElementById('message-text');
    const cursorPos = messageText.selectionStart;
    const textBefore = messageText.value.substring(0, cursorPos);
    const textAfter = messageText.value.substring(cursorPos);
    
    messageText.value = textBefore + variable + textAfter;
    messageText.focus();
    messageText.setSelectionRange(cursorPos + variable.length, cursorPos + variable.length);
    updateMessageLength();
}

function updateMessageLength() {
    const messageText = document.getElementById('message-text');
    const lengthDisplay = document.getElementById('message-length');
    if (lengthDisplay) {
        lengthDisplay.textContent = messageText.value.length;
    }
}

async function handleCreateCampaign(e) {
    e.preventDefault();
    
    const audienceType = document.getElementById('audience-type').value;
    let recipients = [];
    
    switch(audienceType) {
        case 'followers':
        case 'following':
        case 'engaged':
            // Will be fetched from API
            break;
        case 'custom':
            recipients = document.getElementById('recipients-list').value
                .split('\n')
                .map(r => r.trim())
                .filter(r => r);
            break;
        case 'import':
            // Handle file import
            const file = document.getElementById('import-file').files[0];
            if (file) {
                // TODO: Parse file
            }
            break;
    }
    
    const campaignData = {
        name: document.getElementById('campaign-name').value,
        sender_account_id: document.getElementById('sender-account').value,
        audience_type: audienceType,
        recipients: recipients,
        message: document.getElementById('message-text').value,
        messages_per_hour: parseInt(document.getElementById('messages-per-hour').value),
        total_messages: parseInt(document.getElementById('total-messages').value),
        settings: {
            skip_existing: document.getElementById('skip-existing').checked,
            track_opens: document.getElementById('track-opens').checked,
            stop_on_reply: document.getElementById('stop-on-reply').checked
        }
    };
    
    try {
        showNotification('Создание рассылки...', 'info');
        
        // TODO: Implement API call
        // const result = await api.createMassDMCampaign(campaignData);
        
        // For now, just add to local array
        campaigns.push({
            id: Date.now(),
            ...campaignData,
            sent: 0,
            total: campaignData.total_messages,
            delivered: 0,
            replies: 0,
            status: 'active',
            account: accounts.find(a => a.id == campaignData.sender_account_id)?.username || 'unknown',
            audience: `${audienceType} (${recipients.length || campaignData.total_messages} получателей)`
        });
        
        updateCampaignsTable();
        updateStats();
        closeCampaignModal();
        showNotification('Рассылка создана и запущена', 'success');
    } catch (error) {
        console.error('Error creating campaign:', error);
        showNotification('Ошибка при создании рассылки', 'error');
    }
}

function createTemplate() {
    // TODO: Implement template creation modal
    showNotification('Функция создания шаблонов будет доступна в следующей версии', 'info');
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
        showNotification(`Рассылка ${campaign.status === 'active' ? 'запущена' : 'приостановлена'}`, 'info');
    }
}

function viewCampaignDetails(campaignId) {
    // TODO: Implement campaign details view
    showNotification('Детальная статистика будет доступна в следующей версии', 'info');
}

function deleteCampaign(campaignId) {
    if (confirm('Удалить эту рассылку?')) {
        campaigns = campaigns.filter(c => c.id !== campaignId);
        updateCampaignsTable();
        showNotification('Рассылка удалена', 'success');
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