// AI Assistant page JavaScript

let currentTab = 'content';
let aiStats = {};
let recentInteractions = [];

document.addEventListener('DOMContentLoaded', async () => {
    await loadAIData();
    setupEventListeners();
});

async function loadAIData() {
    try {
        aiStats = await api.getAIStats();
        recentInteractions = await api.getRecentInteractions();
        updateAIStats();
        renderRecentInteractions();
    } catch (error) {
        console.error('Error loading AI data:', error);
        // Заглушка с тестовыми данными
        aiStats = generateMockAIStats();
        recentInteractions = generateMockInteractions();
        updateAIStats();
        renderRecentInteractions();
    }
}

function generateMockAIStats() {
    return {
        generated_content: 1247,
        ai_responses: 3856,
        accuracy: 98.7,
        tokens_used: 12500,
        content_today: 47,
        content_month: 1247,
        content_total: 8520,
        responses_today: 156,
        responses_month: 3856,
        responses_total: 24120,
        tokens_today: 2100,
        tokens_month: 12500,
        tokens_limit: 100000
    };
}

function generateMockInteractions() {
    const interactions = [];
    const types = ['comment', 'dm', 'story_mention'];
    const accounts = ['user_001', 'user_002', 'user_003', 'user_004', 'user_005'];
    
    for (let i = 0; i < 10; i++) {
        interactions.push({
            id: i + 1,
            type: types[Math.floor(Math.random() * types.length)],
            account: accounts[Math.floor(Math.random() * accounts.length)],
            message: `Пример сообщения ${i + 1}...`,
            response: `Автоответ ${i + 1}...`,
            timestamp: new Date(Date.now() - Math.random() * 24 * 60 * 60 * 1000),
            confidence: Math.floor(Math.random() * 20) + 80 // 80-100%
        });
    }
    
    return interactions;
}

function setupEventListeners() {
    document.getElementById('ai-model').addEventListener('change', (e) => {
        console.log('AI model changed to:', e.target.value);
    });
    
    // Content generation form
    document.getElementById('content-type').addEventListener('change', updateContentForm);
}

function updateAIStats() {
    document.getElementById('generated-content').textContent = aiStats.generated_content.toLocaleString();
    document.getElementById('ai-responses').textContent = aiStats.ai_responses.toLocaleString();
    document.getElementById('ai-accuracy').textContent = aiStats.accuracy + '%';
    document.getElementById('tokens-used').textContent = (aiStats.tokens_used / 1000).toFixed(1) + 'K';
}

function renderRecentInteractions() {
    const container = document.getElementById('recent-interactions');
    
    if (recentInteractions.length === 0) {
        container.innerHTML = '<p class="text-slate-400 text-sm">Нет недавних взаимодействий</p>';
        return;
    }
    
    const html = recentInteractions.map(interaction => `
        <div class="bg-slate-600/30 rounded-lg p-3 border-l-4 border-${getInteractionColor(interaction.type)}-400">
            <div class="flex items-center justify-between mb-2">
                <span class="text-white font-medium">@${interaction.account}</span>
                <span class="text-xs text-slate-400">${formatTime(interaction.timestamp)}</span>
            </div>
            <div class="text-sm text-slate-300 mb-1">${interaction.message}</div>
            <div class="text-sm text-slate-400 mb-2">↳ ${interaction.response}</div>
            <div class="flex items-center justify-between">
                <span class="text-xs text-slate-500">${getInteractionTypeText(interaction.type)}</span>
                <span class="text-xs text-green-400">${interaction.confidence}% точность</span>
            </div>
        </div>
    `).join('');
    
    container.innerHTML = html;
}

function getInteractionColor(type) {
    const colors = {
        'comment': 'blue',
        'dm': 'green',
        'story_mention': 'purple'
    };
    return colors[type] || 'gray';
}

function getInteractionTypeText(type) {
    const types = {
        'comment': 'Комментарий',
        'dm': 'Личное сообщение',
        'story_mention': 'Упоминание в Stories'
    };
    return types[type] || 'Неизвестно';
}

function formatTime(date) {
    return new Date(date).toLocaleTimeString('ru-RU', {
        hour: '2-digit',
        minute: '2-digit'
    });
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
    ['content-tab', 'interaction-tab', 'analytics-tab', 'settings-tab'].forEach(tabId => {
        const element = document.getElementById(tabId);
        if (tabId === `${tab}-tab`) {
            element.classList.remove('hidden');
        } else {
            element.classList.add('hidden');
        }
    });
    
    // Re-initialize icons for newly shown content
    lucide.createIcons();
}

function updateContentForm() {
    const contentType = document.getElementById('content-type').value;
    const themeInput = document.getElementById('content-theme');
    const instructionsInput = document.getElementById('content-instructions');
    
    // Update placeholder text based on content type
    const placeholders = {
        'caption': 'Например: мотивация, бизнес, красота...',
        'story': 'Например: за кулисами, вопрос дня...',
        'comment': 'Например: благодарность, поддержка...',
        'bio': 'Например: предприниматель, творчество...',
        'hashtags': 'Например: фитнес, еда, путешествия...'
    };
    
    themeInput.placeholder = placeholders[contentType] || 'Введите тематику...';
    
    const instructions = {
        'caption': 'Создайте привлекательную подпись с призывом к действию',
        'story': 'Напишите короткий и интересный текст для Stories',
        'comment': 'Создайте дружелюбный комментарий',
        'bio': 'Напишите краткое и запоминающееся описание профиля',
        'hashtags': 'Сгенерируйте релевантные хештеги'
    };
    
    instructionsInput.placeholder = instructions[contentType] || 'Особые требования к контенту...';
}

async function generateContent() {
    const contentType = document.getElementById('content-type').value;
    const theme = document.getElementById('content-theme').value;
    const tone = document.getElementById('content-tone').value;
    const instructions = document.getElementById('content-instructions').value;
    const model = document.getElementById('ai-model').value;
    
    if (!theme.trim()) {
        alert('Пожалуйста, укажите тематику контента');
        return;
    }
    
    const resultContainer = document.getElementById('generated-result');
    resultContainer.innerHTML = '<div class="text-blue-400">Генерируем контент...</div>';
    
    try {
        const result = await api.generateContent({
            type: contentType,
            theme: theme,
            tone: tone,
            instructions: instructions,
            model: model
        });
        
        resultContainer.innerHTML = `<div class="text-white whitespace-pre-wrap">${result.content}</div>`;
    } catch (error) {
        console.error('Error generating content:', error);
        // Заглушка с примером контента
        const mockContent = generateMockContent(contentType, theme, tone);
        resultContainer.innerHTML = `<div class="text-white whitespace-pre-wrap">${mockContent}</div>`;
    }
}

function generateMockContent(type, theme, tone) {
    const mockContents = {
        'caption': `✨ ${theme} — это то, что вдохновляет нас каждый день! 

Поделитесь в комментариях, что мотивирует вас? 💪

#${theme.replace(/\s+/g, '')} #мотивация #вдохновение #успех`,

        'story': `${theme} за кулисами 🎬

Расскажите, интересно ли вам знать больше?`,

        'comment': `Спасибо за то, что делитесь таким контентом о ${theme}! Очень вдохновляет! 🙌`,

        'bio': `${theme} энтузиаст 🌟
Делюсь вдохновением каждый день
👇 Ссылка на полезности`,

        'hashtags': `#${theme.replace(/\s+/g, '')} #вдохновение #мотивация #успех #развитие #цели #достижения #lifestyle #позитив #энергия`
    };
    
    return mockContents[type] || `Сгенерированный контент на тему "${theme}" в ${tone} тоне.`;
}

function copyContent() {
    const content = document.getElementById('generated-result').textContent;
    
    if (!content || content.includes('Нажмите') || content.includes('Генерируем')) {
        alert('Нет контента для копирования');
        return;
    }
    
    navigator.clipboard.writeText(content).then(() => {
        alert('Контент скопирован в буфер обмена!');
    }).catch(() => {
        alert('Ошибка при копировании');
    });
}

function saveContent() {
    const content = document.getElementById('generated-result').textContent;
    const contentType = document.getElementById('content-type').value;
    
    if (!content || content.includes('Нажмите') || content.includes('Генерируем')) {
        alert('Нет контента для сохранения');
        return;
    }
    
    // Здесь можно добавить логику сохранения в базу данных
    alert('Контент сохранен в библиотеку!');
}

function startAI() {
    const model = document.getElementById('ai-model').value;
    alert(`Запуск ИИ с моделью ${model} (будет реализовано)`);
}

function saveAISettings() {
    alert('Настройки ИИ сохранены!');
} 