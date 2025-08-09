// Dashboard JavaScript - точная копия логики Next.js версии

// Dashboard functionality
let dashboardData = {};

// Initialize dashboard
document.addEventListener('DOMContentLoaded', async function() {
    await loadDashboardData();
    setupEventListeners();
});

// Load dashboard data
async function loadDashboardData() {
    try {
        showLoadingState();
        
        // Load statistics from API
        const stats = await getStats();
        dashboardData = stats;
        
        // Update dashboard UI
        updateStatistics(stats);
        
        hideLoadingState();
        
        console.log('Dashboard data loaded:', stats);
        
    } catch (error) {
        console.error('Error loading dashboard data:', error);
        hideLoadingState();
        showNotification('Ошибка при загрузке данных дашборда: ' + error.message, 'error');
        
        // Show fallback data
        updateStatistics({
            accounts: { total: 0, active: 0, inactive: 0, with_proxy: 0 },
            proxies: { total: 0, active: 0, inactive: 0 }
        });
    }
}

// Setup event listeners
function setupEventListeners() {
    // Refresh button
    const refreshBtn = document.getElementById('refresh-dashboard');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', async () => {
            await loadDashboardData();
            showNotification('Данные обновлены', 'success');
        });
    }

    // Quick action buttons
    const quickActions = {
        'add-account-quick': () => window.location.href = 'accounts.html',
        'add-proxy-quick': () => window.location.href = 'proxy.html',
        'create-post-quick': () => window.location.href = 'posts.html',
        'view-analytics-quick': () => window.location.href = 'analytics.html'
    };

    Object.entries(quickActions).forEach(([id, action]) => {
        const button = document.getElementById(id);
        if (button) {
            button.addEventListener('click', action);
        }
    });
}

// Update statistics display
function updateStatistics(stats) {
    // Update account statistics
    updateStatCard('total-accounts', stats.accounts.total, 'Всего аккаунтов');
    updateStatCard('active-accounts', stats.accounts.active, 'Активных аккаунтов');
    updateStatCard('inactive-accounts', stats.accounts.inactive, 'Неактивных аккаунтов');
    updateStatCard('accounts-with-proxy', stats.accounts.with_proxy, 'С прокси');

    // Update proxy statistics
    updateStatCard('total-proxies', stats.proxies.total, 'Всего прокси');
    updateStatCard('active-proxies', stats.proxies.active, 'Активных прокси');
    updateStatCard('inactive-proxies', stats.proxies.inactive, 'Неактивных прокси');

    // Calculate and update success rate
    const successRate = stats.accounts.total > 0 
        ? Math.round((stats.accounts.active / stats.accounts.total) * 100) 
        : 0;
    updateStatCard('success-rate', successRate + '%', 'Успешность');

    // Update progress bars
    updateProgressBar('accounts-progress', stats.accounts.active, stats.accounts.total);
    updateProgressBar('proxies-progress', stats.proxies.active, stats.proxies.total);
}

// Update individual stat card
function updateStatCard(elementId, value, label) {
    const element = document.getElementById(elementId);
    if (element) {
        // Find the value element (usually has text-2xl class)
        const valueElement = element.querySelector('.text-2xl, .text-xl, [data-stat-value]');
        if (valueElement) {
            valueElement.textContent = value;
        }

        // Find the label element (usually has text-sm class)
        const labelElement = element.querySelector('.text-sm, [data-stat-label]');
        if (labelElement) {
            labelElement.textContent = label;
        }
    }
}

// Update progress bar
function updateProgressBar(elementId, current, total) {
    const progressBar = document.getElementById(elementId);
    if (progressBar) {
        const percentage = total > 0 ? Math.round((current / total) * 100) : 0;
        progressBar.style.width = percentage + '%';
        
        // Update color based on percentage
        progressBar.className = progressBar.className.replace(/bg-(red|yellow|green)-\d+/, '');
        if (percentage < 30) {
            progressBar.classList.add('bg-red-500');
        } else if (percentage < 70) {
            progressBar.classList.add('bg-yellow-500');
        } else {
            progressBar.classList.add('bg-green-500');
        }
    }
}

// Show loading state
function showLoadingState() {
    // Add loading spinners to stat cards
    const statCards = document.querySelectorAll('[data-stat-value]');
    statCards.forEach(card => {
        const originalContent = card.textContent;
        card.setAttribute('data-original-content', originalContent);
        card.innerHTML = '<div class="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>';
    });

    // Show loading message
    const loadingMessage = document.getElementById('loading-message');
    if (loadingMessage) {
        loadingMessage.classList.remove('hidden');
    }
}

// Hide loading state
function hideLoadingState() {
    // Restore stat cards content
    const statCards = document.querySelectorAll('[data-stat-value]');
    statCards.forEach(card => {
        const originalContent = card.getAttribute('data-original-content');
        if (originalContent) {
            card.textContent = originalContent;
            card.removeAttribute('data-original-content');
        }
    });

    // Hide loading message
    const loadingMessage = document.getElementById('loading-message');
    if (loadingMessage) {
        loadingMessage.classList.add('hidden');
    }
}

// Format number with K/M suffixes
function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

// Get status color class
function getStatusColor(percentage) {
    if (percentage >= 80) return 'text-green-400';
    if (percentage >= 60) return 'text-yellow-400';
    return 'text-red-400';
}

// Auto-refresh dashboard data every 30 seconds
setInterval(async () => {
    try {
        const stats = await getStats();
        updateStatistics(stats);
        console.log('Dashboard auto-refreshed');
    } catch (error) {
        console.error('Auto-refresh failed:', error);
    }
}, 30000);

// Export functions for global access
window.loadDashboardData = loadDashboardData;
window.updateStatistics = updateStatistics;

// Навигационные функции
function navigateTo(page) {
    window.location.href = page;
}

function startWarmup() {
    if (confirm('Запустить прогрев для всех активных аккаунтов?')) {
        // Здесь будет логика запуска прогрева
        alert('Прогрев запущен! (функция будет реализована в API)');
    }
}

// Обработчики событий для quick actions
document.addEventListener('click', (e) => {
    if (e.target.closest('button')) {
        const button = e.target.closest('button');
        
        // Добавляем визуальную обратную связь
        button.style.transform = 'translateY(-2px)';
        setTimeout(() => {
            button.style.transform = '';
        }, 150);
    }
});

// Функция для обновления данных по требованию
function refreshData() {
    loadDashboardData();
}

// Функции для работы с API в реальном времени
async function loadRealData() {
    try {
        // Пытаемся загрузить реальные данные из API
        const accounts = await api.getAccounts();
        const tasks = await api.getTasks();
        const proxies = await api.getProxies();

        // Обновляем статистику на основе реальных данных
        const realStats = {
            accounts: {
                total: accounts.length,
                active: accounts.filter(acc => acc.is_active).length,
                inactive: accounts.filter(acc => !acc.is_active).length
            },
            posts: {
                today: tasks.filter(task => task.status === 'completed').length,
                total: tasks.length
            },
            proxies: {
                total: proxies.length,
                active: proxies.filter(proxy => proxy.is_active).length
            },
            successRate: calculateSuccessRate(tasks)
        };

        updateStats(realStats);
        
        console.log('Загружены реальные данные:', realStats);
    } catch (error) {
        console.log('Используем тестовые данные:', error.message);
    }
}

function calculateSuccessRate(tasks) {
    if (tasks.length === 0) return 0;
    const completed = tasks.filter(task => task.status === 'completed').length;
    return Math.round((completed / tasks.length) * 100);
}

// Автоматическая попытка загрузки реальных данных
setTimeout(loadRealData, 1000); 