// Analytics page JavaScript

let analyticsData = {};
let charts = {};

document.addEventListener('DOMContentLoaded', async () => {
    await loadAnalyticsData();
    setupEventListeners();
    renderCharts();
    renderTopPerformers();
    renderAnalyticsTable();
});

async function loadAnalyticsData() {
    try {
        analyticsData = await api.getAnalyticsData();
    } catch (error) {
        console.error('Error loading analytics:', error);
        // Заглушка с тестовыми данными
        analyticsData = generateMockAnalyticsData();
    }
}

function generateMockAnalyticsData() {
    const days = 30;
    const accounts = [];
    
    // Generate follower growth data
    const followersData = [];
    const engagementData = [];
    const postsData = [];
    const activityData = [];
    
    for (let i = days; i >= 0; i--) {
        const date = new Date();
        date.setDate(date.getDate() - i);
        
        followersData.push({
            date: date.toLocaleDateString('ru-RU'),
            followers: 125000 + Math.floor(Math.random() * 1000) - 500,
            following: 850 + Math.floor(Math.random() * 50) - 25
        });
        
        engagementData.push({
            date: date.toLocaleDateString('ru-RU'),
            likes: 5000 + Math.floor(Math.random() * 2000),
            comments: 500 + Math.floor(Math.random() * 200),
            shares: 200 + Math.floor(Math.random() * 100)
        });
        
        postsData.push({
            date: date.toLocaleDateString('ru-RU'),
            posts: Math.floor(Math.random() * 50) + 10,
            stories: Math.floor(Math.random() * 20) + 5,
            reels: Math.floor(Math.random() * 15) + 3
        });
        
        activityData.push({
            date: date.toLocaleDateString('ru-RU'),
            follows: Math.floor(Math.random() * 100) + 20,
            unfollows: Math.floor(Math.random() * 50) + 5,
            likes: Math.floor(Math.random() * 500) + 100
        });
    }
    
    // Generate account data
    for (let i = 1; i <= 47; i++) {
        accounts.push({
            id: i,
            username: `account_${i.toString().padStart(3, '0')}`,
            followers: Math.floor(Math.random() * 10000) + 1000,
            growth: (Math.random() * 20 - 5).toFixed(1), // -5% to +15%
            engagement: (Math.random() * 8 + 1).toFixed(1), // 1% to 9%
            posts: Math.floor(Math.random() * 100) + 10,
            last_activity: new Date(Date.now() - Math.random() * 7 * 24 * 60 * 60 * 1000)
        });
    }
    
    return {
        followers: followersData,
        engagement: engagementData,
        posts: postsData,
        activity: activityData,
        accounts: accounts,
        summary: {
            total_followers: 125400,
            engagement_rate: 4.8,
            posts_published: 1247,
            reach: 2100000
        }
    };
}

function setupEventListeners() {
    document.getElementById('time-period').addEventListener('change', async (e) => {
        await loadAnalyticsData();
        renderCharts();
        renderTopPerformers();
        renderAnalyticsTable();
    });
    
    document.getElementById('analytics-search').addEventListener('input', (e) => {
        filterAnalyticsTable(e.target.value);
    });
    
    document.getElementById('sort-by').addEventListener('change', (e) => {
        sortAnalyticsTable(e.target.value);
    });
}

function renderCharts() {
    renderFollowersChart();
    renderEngagementChart();
    renderPostsChart();
    renderActivityChart();
}

function renderFollowersChart() {
    const ctx = document.getElementById('followers-chart').getContext('2d');
    
    if (charts.followers) {
        charts.followers.destroy();
    }
    
    charts.followers = new Chart(ctx, {
        type: 'line',
        data: {
            labels: analyticsData.followers.map(d => d.date),
            datasets: [{
                label: 'Подписчики',
                data: analyticsData.followers.map(d => d.followers),
                borderColor: '#3b82f6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: '#e2e8f0'
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: '#94a3b8'
                    },
                    grid: {
                        color: '#374151'
                    }
                },
                y: {
                    ticks: {
                        color: '#94a3b8'
                    },
                    grid: {
                        color: '#374151'
                    }
                }
            }
        }
    });
}

function renderEngagementChart() {
    const ctx = document.getElementById('engagement-chart').getContext('2d');
    
    if (charts.engagement) {
        charts.engagement.destroy();
    }
    
    charts.engagement = new Chart(ctx, {
        type: 'line',
        data: {
            labels: analyticsData.engagement.map(d => d.date),
            datasets: [
                {
                    label: 'Лайки',
                    data: analyticsData.engagement.map(d => d.likes),
                    borderColor: '#ef4444',
                    backgroundColor: 'rgba(239, 68, 68, 0.1)',
                    fill: false
                },
                {
                    label: 'Комментарии',
                    data: analyticsData.engagement.map(d => d.comments),
                    borderColor: '#10b981',
                    backgroundColor: 'rgba(16, 185, 129, 0.1)',
                    fill: false
                },
                {
                    label: 'Репосты',
                    data: analyticsData.engagement.map(d => d.shares),
                    borderColor: '#8b5cf6',
                    backgroundColor: 'rgba(139, 92, 246, 0.1)',
                    fill: false
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: '#e2e8f0'
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: '#94a3b8'
                    },
                    grid: {
                        color: '#374151'
                    }
                },
                y: {
                    ticks: {
                        color: '#94a3b8'
                    },
                    grid: {
                        color: '#374151'
                    }
                }
            }
        }
    });
}

function renderPostsChart() {
    const ctx = document.getElementById('posts-chart').getContext('2d');
    
    if (charts.posts) {
        charts.posts.destroy();
    }
    
    charts.posts = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: analyticsData.posts.map(d => d.date).slice(-7), // Last 7 days
            datasets: [
                {
                    label: 'Посты',
                    data: analyticsData.posts.map(d => d.posts).slice(-7),
                    backgroundColor: '#3b82f6'
                },
                {
                    label: 'Сториз',
                    data: analyticsData.posts.map(d => d.stories).slice(-7),
                    backgroundColor: '#10b981'
                },
                {
                    label: 'Reels',
                    data: analyticsData.posts.map(d => d.reels).slice(-7),
                    backgroundColor: '#8b5cf6'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: '#e2e8f0'
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: '#94a3b8'
                    },
                    grid: {
                        color: '#374151'
                    }
                },
                y: {
                    ticks: {
                        color: '#94a3b8'
                    },
                    grid: {
                        color: '#374151'
                    }
                }
            }
        }
    });
}

function renderActivityChart() {
    const ctx = document.getElementById('activity-chart').getContext('2d');
    
    if (charts.activity) {
        charts.activity.destroy();
    }
    
    charts.activity = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Подписки', 'Отписки', 'Лайки'],
            datasets: [{
                data: [
                    analyticsData.activity.reduce((sum, d) => sum + d.follows, 0),
                    analyticsData.activity.reduce((sum, d) => sum + d.unfollows, 0),
                    analyticsData.activity.reduce((sum, d) => sum + d.likes, 0)
                ],
                backgroundColor: ['#3b82f6', '#ef4444', '#10b981']
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: '#e2e8f0'
                    }
                }
            }
        }
    });
}

function renderTopPerformers() {
    renderTopAccounts();
    renderTopPosts();
}

function renderTopAccounts() {
    const topAccounts = analyticsData.accounts
        .sort((a, b) => parseFloat(b.growth) - parseFloat(a.growth))
        .slice(0, 5);
    
    const container = document.getElementById('top-accounts');
    
    const html = topAccounts.map((account, index) => `
        <div class="flex items-center justify-between p-3 bg-slate-700/30 rounded-lg">
            <div class="flex items-center gap-3">
                <span class="w-8 h-8 bg-gradient-to-r from-yellow-400 to-orange-400 rounded-full flex items-center justify-center text-sm font-bold text-black">
                    ${index + 1}
                </span>
                <div>
                    <p class="text-white font-medium">@${account.username}</p>
                    <p class="text-slate-400 text-sm">${account.followers.toLocaleString()} подписчиков</p>
                </div>
            </div>
            <div class="text-right">
                <p class="text-white font-medium">${account.growth > 0 ? '+' : ''}${account.growth}%</p>
                <p class="text-slate-400 text-sm">${account.engagement}% вовл.</p>
            </div>
        </div>
    `).join('');
    
    container.innerHTML = html;
}

function renderTopPosts() {
    // Mock top posts data
    const topPosts = [
        { id: 1, caption: 'Лучший пост недели...', likes: 15420, comments: 342, engagement: 8.5 },
        { id: 2, caption: 'Новый продукт запущен...', likes: 12350, comments: 298, engagement: 7.8 },
        { id: 3, caption: 'За кулисами съемки...', likes: 11200, comments: 256, engagement: 7.2 },
        { id: 4, caption: 'Мотивация понедельника...', likes: 10890, comments: 234, engagement: 6.9 },
        { id: 5, caption: 'Секреты успеха...', likes: 9750, comments: 198, engagement: 6.4 }
    ];
    
    const container = document.getElementById('top-posts');
    
    const html = topPosts.map((post, index) => `
        <div class="flex items-center justify-between p-3 bg-slate-700/30 rounded-lg">
            <div class="flex items-center gap-3">
                <span class="w-8 h-8 bg-gradient-to-r from-yellow-400 to-orange-400 rounded-full flex items-center justify-center text-sm font-bold text-black">
                    ${index + 1}
                </span>
                <div>
                    <p class="text-white font-medium">${post.caption.substring(0, 30)}...</p>
                    <p class="text-slate-400 text-sm">${post.likes.toLocaleString()} лайков • ${post.comments} комментариев</p>
                </div>
            </div>
            <div class="text-right">
                <p class="text-white font-medium">${post.engagement}%</p>
                <p class="text-slate-400 text-sm">вовлеченность</p>
            </div>
        </div>
    `).join('');
    
    container.innerHTML = html;
}

function renderAnalyticsTable() {
    const container = document.getElementById('analytics-table');
    
    const html = analyticsData.accounts.map(account => `
        <tr class="border-b border-slate-700 hover:bg-slate-700/30 transition-colors">
            <td class="py-3">
                <div class="flex items-center gap-3">
                    <div class="w-8 h-8 bg-gradient-to-r from-blue-500 to-purple-500 rounded-full flex items-center justify-center">
                        <span class="text-white text-sm font-bold">${account.username.charAt(0).toUpperCase()}</span>
                    </div>
                    <span class="text-white">@${account.username}</span>
                </div>
            </td>
            <td class="py-3 text-white">${account.followers.toLocaleString()}</td>
            <td class="py-3">
                <span class="text-white ${parseFloat(account.growth) >= 0 ? 'text-green-400' : 'text-red-400'}">
                    ${account.growth > 0 ? '+' : ''}${account.growth}%
                </span>
            </td>
            <td class="py-3 text-white">${account.engagement}%</td>
            <td class="py-3 text-white">${account.posts}</td>
            <td class="py-3 text-slate-400">${formatDate(account.last_activity)}</td>
        </tr>
    `).join('');
    
    container.innerHTML = html;
}

function filterAnalyticsTable(searchTerm) {
    const rows = document.querySelectorAll('#analytics-table tr');
    
    rows.forEach(row => {
        const username = row.querySelector('span').textContent.toLowerCase();
        if (username.includes(searchTerm.toLowerCase())) {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
    });
}

function sortAnalyticsTable(sortBy) {
    let sortedAccounts = [...analyticsData.accounts];
    
    switch (sortBy) {
        case 'followers':
            sortedAccounts.sort((a, b) => b.followers - a.followers);
            break;
        case 'engagement':
            sortedAccounts.sort((a, b) => parseFloat(b.engagement) - parseFloat(a.engagement));
            break;
        case 'growth':
            sortedAccounts.sort((a, b) => parseFloat(b.growth) - parseFloat(a.growth));
            break;
        case 'posts':
            sortedAccounts.sort((a, b) => b.posts - a.posts);
            break;
    }
    
    analyticsData.accounts = sortedAccounts;
    renderAnalyticsTable();
}

function formatDate(date) {
    return new Date(date).toLocaleDateString('ru-RU', {
        day: '2-digit',
        month: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function exportReport() {
    alert('Экспорт отчета (будет реализовано)');
} 