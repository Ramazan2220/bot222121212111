// API Configuration
const API_BASE_URL = 'http://localhost:8080/api';

// API Client Class
class APIClient {
    constructor(baseURL = API_BASE_URL) {
        this.baseURL = baseURL;
    }

    async request(endpoint, options = {}) {
        const url = `${this.baseURL}${endpoint}`;
        const config = {
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            },
            ...options
        };

        try {
            const response = await fetch(url, config);
            const data = await response.json();
            
            if (!response.ok) {
                throw new Error(data.error || `HTTP error! status: ${response.status}`);
            }
            
            return data;
        } catch (error) {
            console.error('API Request failed:', error);
            throw error;
        }
    }

    // GET request
    async get(endpoint) {
        return this.request(endpoint, { method: 'GET' });
    }

    // POST request
    async post(endpoint, data) {
        return this.request(endpoint, {
            method: 'POST',
            body: JSON.stringify(data)
        });
    }

    // PUT request
    async put(endpoint, data) {
        return this.request(endpoint, {
            method: 'PUT',
            body: JSON.stringify(data)
        });
    }

    // DELETE request
    async delete(endpoint) {
        return this.request(endpoint, { method: 'DELETE' });
    }
}

// Create API client instance
const api = new APIClient();

// Добавляем методы для работы с аккаунтами к объекту api
api.getAccounts = getAccounts;
api.addAccount = addAccount;
api.bulkAddAccounts = bulkAddAccounts;
api.updateAccount = updateAccount;
api.deleteAccount = deleteAccount;
api.assignProxyToAccount = assignProxyToAccount;

// Добавляем методы для работы с прокси к объекту api
api.getProxies = getProxies;
api.addProxy = addProxy;
api.bulkAddProxies = bulkAddProxies;
api.checkProxy = checkProxy;
api.checkAllProxies = checkAllProxies;
api.deleteProxy = deleteProxy;
api.updateProxy = updateProxy;

// Добавляем методы для статистики к объекту api
api.getStats = getStats;

// Добавляем методы для работы с постами к объекту api
api.getPosts = getPosts;
api.createPost = createPost;
api.createCarouselPost = createCarouselPost;
api.createStory = createStory;
api.deletePost = deletePost;
api.getGroups = getGroups;
api.getTaskStatus = getTaskStatus;

// Добавляем метод для проверки юзернейма
api.checkUsernameAvailability = checkUsernameAvailability;

// Update profiles for multiple accounts
async function updateProfiles(formData) {
    try {
        const response = await fetch(`${API_BASE_URL}/profiles/update`, {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.error || `HTTP error! status: ${response.status}`);
        }
        
        return data;
    } catch (error) {
        console.error('Error updating profiles:', error);
        throw error;
    }
}

// Добавляем метод для обновления профилей
api.updateProfiles = updateProfiles;

// =============================================================================
// ACCOUNTS API
// =============================================================================

// Get all accounts
async function getAccounts() {
    try {
        const response = await api.get('/accounts');
        return response.data || [];
    } catch (error) {
        console.error('Error fetching accounts:', error);
        showNotification('Ошибка при загрузке аккаунтов: ' + error.message, 'error');
        return [];
    }
}

// Add single account
async function addAccount(accountData) {
    try {
        const response = await api.post('/accounts', accountData);
        
        // Проверяем, обрабатывается ли аккаунт в фоновом режиме
        if (response.processing) {
            showNotification(response.message, 'info');
            
            // Запускаем периодическое обновление списка аккаунтов
            startAccountProcessingCheck();
        } else {
            // Показываем подробное сообщение в зависимости от результата
            if (response.message) {
                showNotification(response.message, 'success');
            } else {
                showNotification('Аккаунт успешно добавлен', 'success');
            }
        }
        
        // Логируем дополнительную информацию
        if (response.data) {
            console.log('Account added:', response.data);
            if (response.data.login_successful === false && !response.processing) {
                showNotification('Внимание: Не удалось войти в Instagram. Проверьте данные аккаунта.', 'warning');
            }
            if (response.data.proxy_assigned === false) {
                showNotification('Внимание: Не удалось назначить прокси аккаунту.', 'warning');
            }
        }
        
        return response;
    } catch (error) {
        console.error('Error adding account:', error);
        showNotification('Ошибка при добавлении аккаунта: ' + error.message, 'error');
        throw error;
    }
}

// Bulk add accounts
async function bulkAddAccounts(accountsData, groupId = null, validateAccounts = false, parallelThreads = 2) {
    try {
        const requestData = { 
            accounts: accountsData,
            group_id: groupId,
            validate_accounts: validateAccounts,
            parallel_threads: parallelThreads
        };
        
        const response = await api.post('/accounts/bulk', requestData);
        
        // Проверяем, обрабатываются ли аккаунты в фоновом режиме
        if (response.processing) {
            const threadsText = parallelThreads === 1 ? 'последовательно' : `в ${parallelThreads} потоков`;
            showNotification(`${response.message} (обработка ${threadsText})`, 'info');
            
            // Запускаем периодическое обновление списка аккаунтов
            startAccountProcessingCheck();
        } else {
            // Показываем результаты синхронной обработки
            const results = response.data;
            
            if (results.success.length > 0) {
                showNotification(`Успешно добавлено ${results.success.length} аккаунтов`, 'success');
            }
            
            if (results.failed.length > 0) {
                showNotification(`Не удалось добавить ${results.failed.length} аккаунтов`, 'warning');
                console.warn('Failed accounts:', results.failed);
            }
        }
        
        return response;
    } catch (error) {
        console.error('Error bulk adding accounts:', error);
        showNotification('Ошибка при массовом добавлении аккаунтов: ' + error.message, 'error');
        throw error;
    }
}

// Update account
async function updateAccount(accountId, updateData) {
    try {
        const response = await api.put(`/accounts/${accountId}`, updateData);
        showNotification('Аккаунт успешно обновлен', 'success');
        return response.data;
    } catch (error) {
        console.error('Error updating account:', error);
        showNotification('Ошибка при обновлении аккаунта: ' + error.message, 'error');
        throw error;
    }
}

// Delete account
async function deleteAccount(accountId) {
    try {
        await api.delete(`/accounts/${accountId}`);
        showNotification('Аккаунт успешно удален', 'success');
        return true;
    } catch (error) {
        console.error('Error deleting account:', error);
        showNotification('Ошибка при удалении аккаунта: ' + error.message, 'error');
        throw error;
    }
}

// Assign proxy to account
async function assignProxyToAccount(accountId, proxyId) {
    try {
        await api.post(`/accounts/${accountId}/proxy`, { proxy_id: proxyId });
        showNotification('Прокси успешно назначен аккаунту', 'success');
        return true;
    } catch (error) {
        console.error('Error assigning proxy:', error);
        showNotification('Ошибка при назначении прокси: ' + error.message, 'error');
        throw error;
    }
}

// =============================================================================
// PROXIES API
// =============================================================================

// Get all proxies
async function getProxies() {
    try {
        const response = await api.get('/proxies');
        return response.data || [];
    } catch (error) {
        console.error('Error fetching proxies:', error);
        showNotification('Ошибка при загрузке прокси: ' + error.message, 'error');
        return [];
    }
}

// Add proxy
async function addProxy(proxyData) {
    try {
        const response = await api.post('/proxies', proxyData);
        showNotification('Прокси успешно добавлен', 'success');
        return response.data;
    } catch (error) {
        console.error('Error adding proxy:', error);
        showNotification('Ошибка при добавлении прокси: ' + error.message, 'error');
        throw error;
    }
}

// Bulk add proxies
async function bulkAddProxies(proxiesData, checkProxies = false) {
    try {
        const requestData = { 
            proxies: proxiesData,
            check_proxies: checkProxies
        };
        
        const response = await api.post('/proxies/bulk', requestData);
        
        if (response.processing) {
            showNotification(response.message, 'info');
        } else {
            showNotification(`Успешно добавлено ${proxiesData.length} прокси`, 'success');
        }
        
        return response;
    } catch (error) {
        console.error('Error bulk adding proxies:', error);
        showNotification('Ошибка при массовом добавлении прокси: ' + error.message, 'error');
        throw error;
    }
}

// Check single proxy
async function checkProxy(proxyId) {
    try {
        const response = await api.post(`/proxies/${proxyId}/check`);
        showNotification(
            response.is_active ? 'Прокси работает корректно' : 'Прокси не отвечает', 
            response.is_active ? 'success' : 'error'
        );
        return response;
    } catch (error) {
        console.error('Error checking proxy:', error);
        showNotification('Ошибка при проверке прокси: ' + error.message, 'error');
        throw error;
    }
}

// Check all proxies
async function checkAllProxies() {
    try {
        const response = await api.post('/proxies/check-all');
        showNotification(response.message || 'Проверка всех прокси завершена', 'info');
        return response;
    } catch (error) {
        console.error('Error checking all proxies:', error);
        showNotification('Ошибка при проверке прокси: ' + error.message, 'error');
        throw error;
    }
}

// Delete proxy
async function deleteProxy(proxyId) {
    try {
        await api.delete(`/proxies/${proxyId}`);
        showNotification('Прокси успешно удален', 'success');
        return true;
    } catch (error) {
        console.error('Error deleting proxy:', error);
        showNotification('Ошибка при удалении прокси: ' + error.message, 'error');
        throw error;
    }
}

// Update proxy
async function updateProxy(proxyId, updateData) {
    try {
        const response = await api.put(`/proxies/${proxyId}`, updateData);
        showNotification('Прокси успешно обновлен', 'success');
        return response.data;
    } catch (error) {
        console.error('Error updating proxy:', error);
        showNotification('Ошибка при обновлении прокси: ' + error.message, 'error');
        throw error;
    }
}

// =============================================================================
// STATISTICS API
// =============================================================================

// Get dashboard statistics
async function getStats() {
    try {
        const response = await api.get('/stats');
        return response.data;
    } catch (error) {
        console.error('Error fetching stats:', error);
        showNotification('Ошибка при загрузке статистики: ' + error.message, 'error');
        return {
            accounts: { total: 0, active: 0, inactive: 0, with_proxy: 0 },
            proxies: { total: 0, active: 0, inactive: 0 }
        };
    }
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

// Show notification
function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `fixed top-4 right-4 z-50 p-4 rounded-lg shadow-lg max-w-sm transition-all duration-300 transform translate-x-full`;
    
    // Set notification style based on type
    switch (type) {
        case 'success':
            notification.classList.add('bg-green-600', 'text-white');
            break;
        case 'error':
            notification.classList.add('bg-red-600', 'text-white');
            break;
        case 'warning':
            notification.classList.add('bg-yellow-600', 'text-white');
            break;
        default:
            notification.classList.add('bg-blue-600', 'text-white');
    }
    
    notification.innerHTML = `
        <div class="flex items-center justify-between">
            <span>${message}</span>
            <button onclick="this.parentElement.parentElement.remove()" class="ml-4 text-white hover:text-gray-200">
                <i data-lucide="x" class="w-4 h-4"></i>
            </button>
        </div>
    `;
    
    // Add to page
    document.body.appendChild(notification);
    
    // Initialize Lucide icons
    if (typeof lucide !== 'undefined') {
        lucide.createIcons();
    }
    
    // Animate in
    setTimeout(() => {
        notification.classList.remove('translate-x-full');
    }, 100);
    
    // Auto remove after 5 seconds
    setTimeout(() => {
        notification.classList.add('translate-x-full');
        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, 300);
    }, 5000);
}

// Format date
function formatDate(dateString) {
    if (!dateString) return 'Не указано';
    
    const date = new Date(dateString);
    return date.toLocaleString('ru-RU', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    });
}

// Format account status
function formatAccountStatus(isActive) {
    return isActive ? 'Активен' : 'Неактивен';
}

// Get status badge class
function getStatusBadgeClass(isActive) {
    return isActive 
        ? 'bg-green-100 text-green-800 border-green-200' 
        : 'bg-red-100 text-red-800 border-red-200';
}

// Parse accounts from text (for bulk import)
function parseAccountsFromText(text, format = 'colon') {
    const lines = text.trim().split('\n').filter(line => line.trim());
    const accounts = [];
    
    for (const line of lines) {
        try {
            let account = {};
            
            if (format === 'colon') {
                // Format: username:password:email:email_password
                const parts = line.split(':');
                if (parts.length >= 2) {
                    account.username = parts[0].trim();
                    account.password = parts[1].trim();
                    if (parts.length >= 3) account.email = parts[2].trim();
                    if (parts.length >= 4) account.email_password = parts[3].trim();
                }
            } else if (format === 'json') {
                // JSON format
                account = JSON.parse(line);
            }
            
            // Validate required fields
            if (account.username && account.password) {
                accounts.push(account);
            }
        } catch (error) {
            console.warn('Failed to parse line:', line, error);
        }
    }
    
    return accounts;
}

// Check username availability
async function checkUsernameAvailability(username) {
    try {
        const response = await api.post('/check-username', { username });
        return response;
    } catch (error) {
        console.error('Error checking username availability:', error);
        throw error;
    }
}

// =============================================================================
// POSTS API
// =============================================================================

// Get all posts
async function getPosts() {
    try {
        const response = await api.get('/posts');
        return response.data || [];
    } catch (error) {
        console.error('Error fetching posts:', error);
        showNotification('Ошибка при загрузке постов: ' + error.message, 'error');
        return [];
    }
}

// Create post
async function createPost(file, postData) {
    try {
        const formData = new FormData();
        formData.append('media', file);
        formData.append('type', postData.type);
        formData.append('caption', postData.caption || '');
        formData.append('hashtags', postData.hashtags || '');
        formData.append('publish_time', postData.publish_now ? 'now' : 'scheduled');
        if (postData.scheduled_time) {
            formData.append('scheduled_time', postData.scheduled_time);
        }
        formData.append('account_selection', postData.accounts.length === 0 ? 'all' : 'specific');
        formData.append('uniquify_content', postData.uniquify || false);
        
        // Добавляем выбранные аккаунты
        if (postData.accounts && postData.accounts.length > 0) {
            postData.accounts.forEach(accountId => {
                formData.append('selected_accounts[]', accountId);
            });
        }
        
        const response = await fetch(`${API_BASE_URL}/posts`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (!response.ok) {
            throw new Error(result.error || `HTTP error! status: ${response.status}`);
        }
        
        showNotification(result.message || 'Пост успешно создан!', 'success');
        return result;
    } catch (error) {
        console.error('Error creating post:', error);
        showNotification('Ошибка при создании поста: ' + error.message, 'error');
        throw error;
    }
}

// Create carousel post
async function createCarouselPost(files, postData) {
    try {
        const formData = new FormData();
        
        // Add all carousel images
        files.forEach((file, index) => {
            formData.append(`media_${index}`, file);
        });
        
        formData.append('type', 'carousel');
        formData.append('media_count', files.length);
        formData.append('caption', postData.caption || '');
        formData.append('hashtags', postData.hashtags || '');
        formData.append('publish_time', postData.publish_now ? 'now' : 'scheduled');
        if (postData.scheduled_time) {
            formData.append('scheduled_time', postData.scheduled_time);
        }
        formData.append('account_selection', postData.accounts.length === 0 ? 'all' : 'specific');
        formData.append('uniquify_content', postData.uniquify || false);
        formData.append('concurrent_threads', postData.concurrent_threads || 3);
        formData.append('publish_delay', postData.publish_delay || 60);
        
        // Добавляем выбранные аккаунты
        if (postData.accounts && postData.accounts.length > 0) {
            postData.accounts.forEach(accountId => {
                formData.append('selected_accounts[]', accountId);
            });
        }
        
        const response = await fetch(`${API_BASE_URL}/posts`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (!response.ok) {
            throw new Error(result.error || `HTTP error! status: ${response.status}`);
        }
        
        showNotification(result.message || 'Карусель успешно создана!', 'success');
        return result;
    } catch (error) {
        console.error('Error creating carousel post:', error);
        showNotification('Ошибка при создании карусели: ' + error.message, 'error');
        throw error;
    }
}

// Create story
async function createStory(file, postData) {
    try {
        const formData = new FormData();
        formData.append('media', file);
        formData.append('type', 'story');
        formData.append('caption', postData.caption || '');
        formData.append('publish_time', postData.publish_now ? 'now' : 'scheduled');
        if (postData.scheduled_time) {
            formData.append('scheduled_time', postData.scheduled_time);
        }
        formData.append('account_selection', postData.accounts.length === 0 ? 'all' : 'specific');
        formData.append('uniquify_content', postData.uniquify || false);
        formData.append('concurrent_threads', postData.concurrent_threads || 3);
        formData.append('publish_delay', postData.publish_delay || 60);
        
        // Добавляем выбранные аккаунты
        if (postData.accounts && postData.accounts.length > 0) {
            postData.accounts.forEach(accountId => {
                formData.append('selected_accounts[]', accountId);
            });
        }
        
        // Добавляем настройки истории
        if (postData.story_link) {
            formData.append('story_link', postData.story_link);
        }
        
        const response = await fetch(`${API_BASE_URL}/posts`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (!response.ok) {
            throw new Error(result.error || `HTTP error! status: ${response.status}`);
        }
        
        showNotification(result.message || 'История успешно создана!', 'success');
        return result;
    } catch (error) {
        console.error('Error creating story:', error);
        showNotification('Ошибка при создании истории: ' + error.message, 'error');
        throw error;
    }
}

// Delete post
async function deletePost(postId) {
    try {
        const response = await api.delete(`/posts/${postId}`);
        // Не показываем уведомление здесь, так как posts.js покажет более детальное
        return response.data || response;
    } catch (error) {
        console.error('Error deleting post:', error);
        // Также не показываем ошибку здесь
        throw error;
    }
}

// Get groups
async function getGroups() {
    try {
        const response = await api.get('/groups');
        return response.data || [];
    } catch (error) {
        console.error('Error fetching groups:', error);
        showNotification('Ошибка при загрузке групп: ' + error.message, 'error');
        return [];
    }
}

// Get task status
async function getTaskStatus(taskId) {
    try {
        const response = await api.get(`/posts/task/${taskId}/status`);
        return response.data;
    } catch (error) {
        console.error('Error fetching task status:', error);
        return null;
    }
}

// Добавляем методы для постов к объекту api
api.getPosts = getPosts;
api.createPost = createPost;
api.deletePost = deletePost;
api.getGroups = getGroups;
api.getTaskStatus = getTaskStatus;

async function getWarmupStats() {
    return {
        total_accounts: 0,
        warming_up: 0,
        completed: 0,
        failed: 0
    };
}

async function getAnalyticsData() {
    return {
        engagement: [],
        followers: [],
        posts: []
    };
}

async function getAIModels() {
    return ['GPT-4', 'Claude', 'Gemini'];
}

async function generateContent(prompt, model) {
    return `Generated content for: ${prompt} using ${model}`;
}

// Система отслеживания обработки аккаунтов
let processingCheckInterval = null;
let processingStartTime = null;
let lastAccountCount = 0;
let processingNotificationId = null;

function startAccountProcessingCheck() {
    // Останавливаем предыдущий интервал, если он есть
    if (processingCheckInterval) {
        clearInterval(processingCheckInterval);
    }
    
    processingStartTime = Date.now();
    
    // Получаем текущее количество аккаунтов
    getAccounts().then(response => {
        lastAccountCount = response.data.length;
    });
    
    // Создаем постоянное уведомление о процессе
    processingNotificationId = showPersistentNotification('🔄 Обработка аккаунтов в процессе...', 'info');
    
    // Проверяем каждые 3 секунды
    processingCheckInterval = setInterval(async () => {
        try {
            const response = await getAccounts();
            const currentAccountCount = response.data.length;
            
            // Если количество аккаунтов увеличилось, значит обработка идет
            if (currentAccountCount > lastAccountCount) {
                const addedCount = currentAccountCount - lastAccountCount;
                
                // Обновляем постоянное уведомление
                updatePersistentNotification(
                    processingNotificationId, 
                    `✅ Добавлено ${addedCount} аккаунтов. Обработка продолжается...`, 
                    'success'
                );
                
                // Показываем временное уведомление
                showNotification(`✨ Добавлено ${addedCount} новых аккаунтов!`, 'success');
                
                lastAccountCount = currentAccountCount;
                
                // Обновляем интерфейс
                if (typeof loadInitialData === 'function') {
                    loadInitialData();
                }
            }
            
            // Обновляем таймер в уведомлении
            const elapsed = Math.floor((Date.now() - processingStartTime) / 1000);
            if (processingNotificationId) {
                updatePersistentNotification(
                    processingNotificationId, 
                    `🔄 Обработка аккаунтов... (${elapsed}с)`, 
                    'info'
                );
            }
            
            // Останавливаем проверку через 5 минут
            if (Date.now() - processingStartTime > 5 * 60 * 1000) {
                stopAccountProcessingCheck();
                showNotification('⏰ Обработка аккаунтов завершена или превышено время ожидания', 'warning');
            }
        } catch (error) {
            console.error('Error checking account processing:', error);
        }
    }, 3000);
}

function stopAccountProcessingCheck() {
    if (processingCheckInterval) {
        clearInterval(processingCheckInterval);
        processingCheckInterval = null;
    }
    
    // Удаляем постоянное уведомление
    if (processingNotificationId) {
        removePersistentNotification(processingNotificationId);
        processingNotificationId = null;
    }
}

// Система постоянных уведомлений
let persistentNotifications = new Map();
let notificationIdCounter = 0;

function showPersistentNotification(message, type = 'info') {
    const id = ++notificationIdCounter;
    
    const notification = document.createElement('div');
    notification.id = `persistent-notification-${id}`;
    notification.className = `fixed top-4 right-4 z-50 p-4 rounded-lg shadow-lg max-w-sm transition-all duration-300`;
    
    switch (type) {
        case 'success':
            notification.classList.add('bg-green-600', 'text-white');
            break;
        case 'error':
            notification.classList.add('bg-red-600', 'text-white');
            break;
        case 'warning':
            notification.classList.add('bg-yellow-600', 'text-white');
            break;
        default:
            notification.classList.add('bg-blue-600', 'text-white');
    }
    
    notification.innerHTML = `
        <div class="flex items-center justify-between">
            <span class="persistent-message">${message}</span>
            <button onclick="removePersistentNotification(${id})" class="ml-4 text-white hover:text-gray-200">
                <i data-lucide="x" class="w-4 h-4"></i>
            </button>
        </div>
    `;
    
    document.body.appendChild(notification);
    
    if (typeof lucide !== 'undefined') {
        lucide.createIcons();
    }
    
    persistentNotifications.set(id, notification);
    return id;
}

function updatePersistentNotification(id, message, type = null) {
    const notification = persistentNotifications.get(id);
    if (!notification) return;
    
    const messageElement = notification.querySelector('.persistent-message');
    if (messageElement) {
        messageElement.textContent = message;
    }
    
    if (type) {
        notification.className = notification.className.replace(/bg-(green|red|yellow|blue)-600/, '');
        switch (type) {
            case 'success':
                notification.classList.add('bg-green-600');
                break;
            case 'error':
                notification.classList.add('bg-red-600');
                break;
            case 'warning':
                notification.classList.add('bg-yellow-600');
                break;
            default:
                notification.classList.add('bg-blue-600');
        }
    }
}

function removePersistentNotification(id) {
    const notification = persistentNotifications.get(id);
    if (!notification) return;
    
    notification.style.transform = 'translateX(100%)';
    setTimeout(() => {
        if (notification.parentElement) {
            notification.remove();
        }
        persistentNotifications.delete(id);
    }, 300);
}

// =============================================================================
// WARMUP API
// =============================================================================

// Get warmup processes
async function getWarmupProcesses() {
    try {
        const response = await api.get('/warmup/processes');
        return response.data || [];
    } catch (error) {
        console.error('Error fetching warmup processes:', error);
        return [];
    }
}

// Get warmup templates
async function getWarmupTemplates() {
    try {
        const response = await api.get('/warmup/templates');
        return response.data || [];
    } catch (error) {
        console.error('Error fetching warmup templates:', error);
        return [];
    }
}

// Save warmup settings
async function saveWarmupSettings(settings) {
    try {
        const response = await api.post('/warmup/settings', settings);
        return response.data;
    } catch (error) {
        console.error('Error saving warmup settings:', error);
        throw error;
    }
}

// Save warmup template
async function saveWarmupTemplate(template) {
    try {
        const response = await api.post('/warmup/templates', template);
        return response.data;
    } catch (error) {
        console.error('Error saving warmup template:', error);
        throw error;
    }
}

// Start warmup process
async function startWarmupProcess(accountId, settings) {
    try {
        const response = await api.post('/warmup/start', { account_id: accountId, settings });
        return response.data;
    } catch (error) {
        console.error('Error starting warmup process:', error);
        throw error;
    }
}

// Pause warmup process
async function pauseWarmupProcess(processId) {
    try {
        const response = await api.post(`/warmup/processes/${processId}/pause`);
        return response.data;
    } catch (error) {
        console.error('Error pausing warmup process:', error);
        throw error;
    }
}

// Stop warmup process
async function stopWarmupProcess(processId) {
    try {
        const response = await api.post(`/warmup/processes/${processId}/stop`);
        return response.data;
    } catch (error) {
        console.error('Error stopping warmup process:', error);
        throw error;
    }
}

// Add warmup methods to api object
api.getWarmupProcesses = getWarmupProcesses;
api.getWarmupTemplates = getWarmupTemplates;
api.saveWarmupSettings = saveWarmupSettings;
api.saveWarmupTemplate = saveWarmupTemplate;
api.startWarmupProcess = startWarmupProcess;
api.pauseWarmupProcess = pauseWarmupProcess;
api.stopWarmupProcess = stopWarmupProcess;

// =============================================================================
// FOLLOW API
// =============================================================================

// Get follow tasks
async function getFollowTasks() {
    try {
        const response = await api.get('/follow/tasks');
        return response;
    } catch (error) {
        console.error('Error fetching follow tasks:', error);
        return { success: false, tasks: [], error: error.message };
    }
}

// Create follow task
async function createFollowTask(taskData) {
    try {
        const response = await api.post('/follow/tasks', taskData);
        return response;
    } catch (error) {
        console.error('Error creating follow task:', error);
        throw error;
    }
}

// Update follow task
async function updateFollowTask(taskId, updateData) {
    try {
        const response = await api.put(`/follow/tasks/${taskId}`, updateData);
        return response;
    } catch (error) {
        console.error('Error updating follow task:', error);
        throw error;
    }
}

// Delete follow task
async function deleteFollowTask(taskId) {
    try {
        const response = await api.delete(`/follow/tasks/${taskId}`);
        return response;
    } catch (error) {
        console.error('Error deleting follow task:', error);
        throw error;
    }
}

// Get follow stats
async function getFollowStats() {
    try {
        const response = await api.get('/follow/stats');
        return response;
    } catch (error) {
        console.error('Error fetching follow stats:', error);
        return { 
            success: false, 
            active_tasks: 0, 
            today_follows: 0, 
            total_followed: 0, 
            success_rate: 0 
        };
    }
}

// Stop all follow tasks
async function stopAllFollowTasks() {
    try {
        const response = await api.post('/follow/tasks/stop-all');
        return response;
    } catch (error) {
        console.error('Error stopping all follow tasks:', error);
        throw error;
    }
}

// Add follow methods to api object
api.getFollowTasks = getFollowTasks;
api.createFollowTask = createFollowTask;
api.updateFollowTask = updateFollowTask;
api.deleteFollowTask = deleteFollowTask;
api.getFollowStats = getFollowStats;
api.stopAllFollowTasks = stopAllFollowTasks;

// Экспортируем api в глобальную область видимости
window.api = api;