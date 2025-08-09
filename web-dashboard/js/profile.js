// Profile page JavaScript

let accounts = [];
let groups = [];
let currentAccountId = null;
let currentStylingType = 'single';
let selectedAccounts = new Set();
let uploadedAvatars = [];

document.addEventListener('DOMContentLoaded', async () => {
    await Promise.all([loadAccounts(), loadGroups()]);
    setupFormListeners();
    setupUniquifierToggle();
    updatePreview();
    
    // Populate account selectors
    populateAccountSelectors();
    populateGroupSelector();
    
    // Initialize account selection
    toggleAccountSelection();
    
    // Initialize selected accounts count
    updateSelectedAccountsCount();
});

async function loadAccounts() {
    try {
        accounts = await api.getAccounts();
        console.log('Loaded accounts:', accounts);
        console.log('Active accounts:', accounts.filter(acc => acc.is_active === true));
        console.log('Inactive accounts:', accounts.filter(acc => acc.is_active === false));
        populateAccountSelectors();
    } catch (error) {
        console.error('Error loading accounts:', error);
        showNotification('Ошибка загрузки аккаунтов', 'error');
    }
}

async function loadGroups() {
    try {
        groups = await api.getGroups();
        populateGroupSelector();
    } catch (error) {
        console.error('Error loading groups:', error);
    }
}

function populateAccountSelectors() {
    // Filter only active accounts
    const activeAccounts = accounts.filter(acc => acc.is_active === true);
    const inactiveAccounts = accounts.filter(acc => acc.is_active === false);
    
    // Single account selector - show all accounts with status
    const singleSelect = document.getElementById('single-account');
    if (singleSelect) {
        let options = '<option value="">Выберите аккаунт</option>';
        
        if (activeAccounts.length > 0) {
            options += '<optgroup label="Активные аккаунты">';
            options += activeAccounts.map(acc => 
                `<option value="${acc.id}">@${acc.username}</option>`
            ).join('');
            options += '</optgroup>';
        }
        
        if (inactiveAccounts.length > 0) {
            options += '<optgroup label="Неактивные аккаунты">';
            options += inactiveAccounts.map(acc => 
                `<option value="${acc.id}" disabled>@${acc.username} (неактивен)</option>`
            ).join('');
            options += '</optgroup>';
        }
        
        singleSelect.innerHTML = options;
    }
    
    // Multiple accounts selector with checkboxes
    const accountsList = document.getElementById('accounts-list');
    if (accountsList) {
        if (accounts.length === 0) {
            accountsList.innerHTML = '<p class="text-slate-400 text-center py-4">Нет доступных аккаунтов</p>';
        } else {
            let html = '';
            
            // Active accounts
            if (activeAccounts.length > 0) {
                html += '<div class="mb-3"><p class="text-xs text-slate-400 mb-2">Активные аккаунты</p>';
                html += activeAccounts.map(acc => `
                    <label class="flex items-center gap-2 p-2 hover:bg-slate-700 rounded cursor-pointer">
                        <input type="checkbox" class="account-checkbox" value="${acc.id}" 
                               onchange="updateSelectedAccountsCount()">
                        <span class="text-white">@${acc.username}</span>
                    </label>
                `).join('');
                html += '</div>';
            }
            
            // Inactive accounts
            if (inactiveAccounts.length > 0) {
                html += '<div><p class="text-xs text-slate-400 mb-2">Неактивные аккаунты</p>';
                html += inactiveAccounts.map(acc => `
                    <label class="flex items-center gap-2 p-2 opacity-50 cursor-not-allowed">
                        <input type="checkbox" disabled class="cursor-not-allowed">
                        <span class="text-slate-400 line-through">@${acc.username}</span>
                    </label>
                `).join('');
                html += '</div>';
            }
            
            accountsList.innerHTML = html;
        }
    }
    
    // Setup select all button
    setupSelectAllButton();
    
    // Update account stats display
    updateAccountStats(activeAccounts.length, inactiveAccounts.length);
}

function setupSelectAllButton() {
    const selectAllBtn = document.getElementById('select-all-accounts');
    if (selectAllBtn) {
        selectAllBtn.onclick = function() {
            const checkboxes = document.querySelectorAll('.account-checkbox:not(:disabled)');
            const allChecked = Array.from(checkboxes).every(cb => cb.checked);
            
            checkboxes.forEach(cb => {
                cb.checked = !allChecked;
            });
            
            selectAllBtn.textContent = allChecked ? 'Выбрать все' : 'Снять выделение';
            updateSelectedAccountsCount();
        };
    }
}

function updateSelectedAccountsCount() {
    // Обновляем Set выбранных аккаунтов
    selectedAccounts.clear();
    
    // Проверяем тип выбора
    const selectionType = document.getElementById('account-selection-type').value;
    
    if (selectionType === 'single') {
        const singleSelect = document.getElementById('single-account');
        if (singleSelect && singleSelect.value) {
            selectedAccounts.add(parseInt(singleSelect.value));
        }
    } else if (selectionType === 'multiple') {
        const checkboxes = document.querySelectorAll('.account-checkbox:checked');
        checkboxes.forEach(cb => {
            selectedAccounts.add(parseInt(cb.value));
        });
    } else if (selectionType === 'all') {
        // Добавляем все активные аккаунты
        const activeAccounts = accounts.filter(acc => acc.is_active === true);
        activeAccounts.forEach(acc => {
            selectedAccounts.add(acc.id);
        });
    }
    
    // Обновляем отображение количества
    const selectedCount = selectedAccounts.size;
    const countSpan = document.getElementById('selected-accounts-count');
    if (countSpan) {
        countSpan.textContent = `Выбрано: ${selectedCount}`;
    }
    
    // Update select all button text
    const selectAllBtn = document.getElementById('select-all-accounts');
    if (selectAllBtn) {
        const checkboxes = document.querySelectorAll('.account-checkbox:not(:disabled)');
        const allChecked = checkboxes.length > 0 && Array.from(checkboxes).every(cb => cb.checked);
        selectAllBtn.textContent = allChecked ? 'Снять выделение' : 'Выбрать все';
    }
    
    console.log('Selected accounts:', Array.from(selectedAccounts));
}

function updateAccountStats(activeCount, inactiveCount) {
    // Create or update stats display
    let statsDiv = document.getElementById('account-stats');
    if (!statsDiv) {
        // Create stats div if it doesn't exist
        const selectionDiv = document.querySelector('.bg-slate-800');
        if (selectionDiv) {
            statsDiv = document.createElement('div');
            statsDiv.id = 'account-stats';
            statsDiv.className = 'mb-4 p-3 bg-slate-700 rounded-lg text-sm';
            selectionDiv.insertBefore(statsDiv, selectionDiv.firstChild);
        }
    }
    
    if (statsDiv) {
        const total = activeCount + inactiveCount;
        statsDiv.innerHTML = `
            <div class="flex items-center justify-between">
                <div>
                    <span class="text-slate-400">Всего аккаунтов:</span> 
                    <span class="text-white font-medium">${total}</span>
                </div>
                <div class="flex gap-4">
                    <div>
                        <span class="text-green-400">●</span> 
                        <span class="text-slate-400">Активных:</span> 
                        <span class="text-white font-medium">${activeCount}</span>
                    </div>
                    <div>
                        <span class="text-red-400">●</span> 
                        <span class="text-slate-400">Неактивных:</span> 
                        <span class="text-white font-medium">${inactiveCount}</span>
                    </div>
                </div>
            </div>
            ${inactiveCount > 0 ? `
                <div class="mt-2 text-xs text-yellow-400">
                    <i data-lucide="alert-triangle" class="h-3 w-3 inline mr-1"></i>
                    Неактивные аккаунты не могут быть использованы для изменения профиля
                </div>
            ` : ''}
        `;
        
        // Re-initialize lucide icons
        if (typeof lucide !== 'undefined') {
            lucide.createIcons();
        }
    }
}

function populateGroupSelector() {
    const groupSelect = document.getElementById('account-group');
    if (groupSelect) {
        groupSelect.innerHTML = '<option value="">Выберите группу</option>' +
            groups.map(g => `<option value="${g.id}">${g.name} (${g.accounts?.length || 0} акк.)</option>`).join('');
    }
}

function toggleAccountSelection() {
    const type = document.getElementById('account-selection-type').value;
    
    // Hide all selection types
    document.getElementById('single-account-select').classList.toggle('hidden', type !== 'single');
    document.getElementById('multiple-accounts-select').classList.toggle('hidden', type !== 'multiple');
    document.getElementById('group-select').classList.toggle('hidden', type !== 'group');
    
    // Show/hide the second column in grid
    const singleSelect = document.getElementById('single-account-select');
    if (type === 'all') {
        singleSelect.style.display = 'none';
    } else {
        singleSelect.style.display = '';
    }
    
    // Toggle username input based on selection type
    const singleUsernameInput = document.getElementById('single-username-input');
    const multipleUsernamesInput = document.getElementById('multiple-usernames-input');
    
    if (type === 'single') {
        singleUsernameInput.classList.remove('hidden');
        multipleUsernamesInput.classList.add('hidden');
    } else {
        singleUsernameInput.classList.add('hidden');
        multipleUsernamesInput.classList.remove('hidden');
    }
    
    // Toggle name input based on selection type
    const singleNameInput = document.getElementById('single-name-input');
    const multipleNamesInput = document.getElementById('multiple-names-input');
    
    if (type === 'single') {
        singleNameInput.classList.remove('hidden');
        multipleNamesInput.classList.add('hidden');
    } else {
        singleNameInput.classList.add('hidden');
        multipleNamesInput.classList.remove('hidden');
    }
    
    // Toggle avatar mode
    const singleAvatarMode = document.getElementById('single-avatar-mode');
    const multipleAvatarsMode = document.getElementById('multiple-avatars-mode');
    
    if (type === 'single') {
        singleAvatarMode.classList.remove('hidden');
        multipleAvatarsMode.classList.add('hidden');
    } else {
        singleAvatarMode.classList.add('hidden');
        multipleAvatarsMode.classList.remove('hidden');
    }
    
    // Update selected accounts count
    updateSelectedAccountsCount();
}

// Username validation
function validateUsername() {
    const usernameInput = document.getElementById('new-username');
    const statusDiv = document.getElementById('username-status');
    
    if (!usernameInput || !statusDiv) return;
    
    const username = usernameInput.value.trim();
    
    if (!username) {
        statusDiv.innerHTML = '';
        return;
    }
    
    // Validate username format
    const usernameRegex = /^[a-zA-Z0-9._]{1,30}$/;
    if (!usernameRegex.test(username)) {
        statusDiv.innerHTML = '<span class="text-red-400 text-xs">Неверный формат (только буквы, цифры, точки и подчеркивания, макс. 30 символов)</span>';
        return;
    }
    
    statusDiv.innerHTML = '<span class="text-slate-400 text-xs">Формат корректный. Нажмите кнопку для проверки доступности.</span>';
}

// Check username availability with real API
async function checkUsernameAvailability() {
    const usernameInput = document.getElementById('new-username');
    const statusDiv = document.getElementById('username-status');
    
    if (!usernameInput || !statusDiv) return;
    
    const username = usernameInput.value.trim();
    
    if (!username) {
        statusDiv.innerHTML = '<span class="text-red-400">Введите юзернейм</span>';
        return;
    }
    
    // Validate username format first
    const usernameRegex = /^[a-zA-Z0-9._]{1,30}$/;
    if (!usernameRegex.test(username)) {
        statusDiv.innerHTML = '<span class="text-red-400">Неверный формат юзернейма</span>';
        return;
    }
    
    statusDiv.innerHTML = '<span class="text-blue-400"><i data-lucide="loader" class="h-4 w-4 inline mr-1 animate-spin"></i>Проверяем...</span>';
    lucide.createIcons();
    
    try {
        const result = await api.checkUsernameAvailability(username);
        
        if (result.warning) {
            // Если есть предупреждение (нет активных аккаунтов или не удалось проверить)
            statusDiv.innerHTML = `<span class="text-yellow-400"><i data-lucide="alert-triangle" class="h-4 w-4 inline mr-1"></i>${result.message}</span>`;
        } else if (result.available === true) {
            statusDiv.innerHTML = '<span class="text-green-400"><i data-lucide="check-circle" class="h-4 w-4 inline mr-1"></i>Юзернейм доступен</span>';
        } else if (result.available === false) {
            statusDiv.innerHTML = '<span class="text-red-400"><i data-lucide="x-circle" class="h-4 w-4 inline mr-1"></i>Юзернейм занят</span>';
            
            // Показываем предложения, если есть
            if (result.suggestions && result.suggestions.length > 0) {
                const suggestions = result.suggestions.slice(0, 3).join(', ');
                statusDiv.innerHTML += `<br><span class="text-slate-400 text-xs">Попробуйте: ${suggestions}</span>`;
            }
        } else {
            statusDiv.innerHTML = '<span class="text-yellow-400"><i data-lucide="alert-triangle" class="h-4 w-4 inline mr-1"></i>Не удалось проверить доступность</span>';
        }
        
    } catch (error) {
        console.error('Error checking username:', error);
        statusDiv.innerHTML = '<span class="text-red-400"><i data-lucide="x-circle" class="h-4 w-4 inline mr-1"></i>Ошибка при проверке</span>';
    }
    
    lucide.createIcons();
}

// Validate multiple usernames
function validateBulkUsernames() {
    const textarea = document.getElementById('bulk-usernames');
    const statusDiv = document.getElementById('username-status');
    
    if (!textarea || !statusDiv) return;
    
    const usernames = textarea.value
        .split('\n')
        .map(u => u.trim())
        .filter(u => u.length > 0);
    
    if (usernames.length === 0) {
        statusDiv.innerHTML = '';
        return;
    }
    
    const results = [];
    const usernameRegex = /^[a-zA-Z0-9._]{1,30}$/;
    
    for (const username of usernames) {
        if (!usernameRegex.test(username)) {
            results.push({ username, valid: false });
        } else {
            results.push({ username, valid: true });
        }
    }
    
    const validCount = results.filter(r => r.valid).length;
    const invalidCount = results.filter(r => !r.valid).length;
    
    if (invalidCount === 0) {
        statusDiv.innerHTML = `<span class="text-slate-400 text-xs">Все ${validCount} юзернеймов корректны. Нажмите кнопку для проверки доступности.</span>`;
    } else {
        statusDiv.innerHTML = `<span class="text-yellow-400 text-xs">${validCount} корректны, ${invalidCount} с ошибками формата</span>`;
    }
}

// Check multiple usernames availability
async function checkBulkUsernames() {
    const textarea = document.getElementById('bulk-usernames');
    const statusDiv = document.getElementById('username-status');
    
    if (!textarea || !statusDiv) return;
    
    const usernames = textarea.value
        .split('\n')
        .map(u => u.trim())
        .filter(u => u.length > 0);
    
    if (usernames.length === 0) {
        statusDiv.innerHTML = '<span class="text-red-400">Введите хотя бы один юзернейм</span>';
        return;
    }
    
    statusDiv.innerHTML = '<span class="text-blue-400"><i data-lucide="loader" class="h-4 w-4 inline mr-1 animate-spin"></i>Проверяем юзернеймы...</span>';
    lucide.createIcons();
    
    try {
        const results = [];
        const usernameRegex = /^[a-zA-Z0-9._]{1,30}$/;
        
        // Сначала проверяем формат
        const validUsernames = [];
        for (const username of usernames) {
            if (!usernameRegex.test(username)) {
                results.push({ username, status: 'invalid' });
            } else {
                validUsernames.push(username);
            }
        }
        
        // Проверяем доступность валидных юзернеймов
        for (const username of validUsernames) {
            try {
                const result = await api.checkUsernameAvailability(username);
                
                if (result.warning) {
                    results.push({ username, status: 'unknown' });
                } else if (result.available === true) {
                    results.push({ username, status: 'available' });
                } else {
                    results.push({ username, status: 'taken' });
                }
            } catch (error) {
                results.push({ username, status: 'error' });
            }
        }
        
        // Отображаем результаты
        const availableCount = results.filter(r => r.status === 'available').length;
        const takenCount = results.filter(r => r.status === 'taken').length;
        const invalidCount = results.filter(r => r.status === 'invalid').length;
        const unknownCount = results.filter(r => r.status === 'unknown' || r.status === 'error').length;
        
        let html = `<div class="space-y-2">
            <div class="text-sm">
                <span class="text-green-400">${availableCount} доступно</span> • 
                <span class="text-red-400">${takenCount} занято</span>`;
        
        if (invalidCount > 0) {
            html += ` • <span class="text-yellow-400">${invalidCount} неверный формат</span>`;
        }
        if (unknownCount > 0) {
            html += ` • <span class="text-gray-400">${unknownCount} не проверено</span>`;
        }
        
        html += `</div><div class="max-h-32 overflow-y-auto space-y-1">`;
        
        results.forEach(({ username, status }) => {
            const config = {
                'available': { color: 'green', icon: 'check-circle', text: 'доступен' },
                'taken': { color: 'red', icon: 'x-circle', text: 'занят' },
                'invalid': { color: 'yellow', icon: 'alert-triangle', text: 'неверный формат' },
                'unknown': { color: 'gray', icon: 'help-circle', text: 'не удалось проверить' },
                'error': { color: 'red', icon: 'alert-triangle', text: 'ошибка проверки' }
            };
            
            const { color, icon, text } = config[status] || config['error'];
            
            html += `<div class="text-xs text-${color}-400">
                <i data-lucide="${icon}" class="h-3 w-3 inline mr-1"></i>
                ${username} - ${text}
            </div>`;
        });
        
        html += '</div></div>';
        statusDiv.innerHTML = html;
        
    } catch (error) {
        console.error('Error checking usernames:', error);
        statusDiv.innerHTML = '<span class="text-red-400"><i data-lucide="x-circle" class="h-4 w-4 inline mr-1"></i>Ошибка при проверке</span>';
    }
    
    lucide.createIcons();
}

function setupFormListeners() {
    // Update preview in real-time
    const inputIds = ['new-username', 'profile-name', 'profile-bio'];
    inputIds.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.addEventListener('input', updatePreview);
        }
    });
    
    // Добавляем обработчик для single account select
    const singleAccountSelect = document.getElementById('single-account');
    if (singleAccountSelect) {
        singleAccountSelect.addEventListener('change', updateSelectedAccountsCount);
    }
    
    // Добавляем обработчик для account-selection-type
    const accountSelectionType = document.getElementById('account-selection-type');
    if (accountSelectionType) {
        accountSelectionType.addEventListener('change', toggleAccountSelection);
    }
    
    // Username validation on input
    const usernameInput = document.getElementById('new-username');
    if (usernameInput) {
        usernameInput.addEventListener('input', validateUsername);
    }
    
    // Bulk usernames validation on input
    const bulkUsernamesTextarea = document.getElementById('bulk-usernames');
    if (bulkUsernamesTextarea) {
        bulkUsernamesTextarea.addEventListener('input', validateBulkUsernames);
    }
    
    // Bio character counter
    const bioTextarea = document.getElementById('profile-bio');
    const bioCounter = document.getElementById('bio-counter');
    if (bioTextarea && bioCounter) {
        bioTextarea.addEventListener('input', () => {
            const length = bioTextarea.value.length;
            bioCounter.textContent = `${length}/150`;
            bioCounter.classList.toggle('text-red-400', length > 150);
        });
    }
    
    // Form submission
    const form = document.getElementById('profile-form');
    if (form) {
        console.log('Form found, adding submit listener');
        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            console.log('Form submitted!');
            
            // Проверяем, выбраны ли аккаунты
            if (selectedAccounts.size === 0) {
                showNotification('Пожалуйста, выберите хотя бы один аккаунт', 'error');
                return;
            }
            
            const submitButton = form.querySelector('button[type="submit"]');
            const originalButtonText = submitButton.textContent;
            submitButton.disabled = true;
            submitButton.textContent = 'Обновление...';
            
            const formData = new FormData(form);
            
            // Добавляем выбранные аккаунты
            selectedAccounts.forEach(accountId => {
                formData.append('account_ids[]', accountId);
            });
            
            // Добавляем username
            const username = document.getElementById('new-username')?.value || '';
            if (username) {
                formData.append('username', username);
            }
            
            // Добавляем display_name
            const displayName = document.getElementById('profile-name')?.value || '';
            if (displayName) {
                formData.append('display_name', displayName);
            }
            
            // Добавляем bio
            const bio = document.getElementById('profile-bio')?.value || '';
            if (bio) {
                formData.append('bio', bio);
            }
            

            
            // Добавляем avatar
            const avatarInput = document.getElementById('profile-picture-input');
            if (avatarInput && avatarInput.files.length > 0) {
                formData.append('avatar', avatarInput.files[0]);
            }
            
            // Добавляем имена для распределения
            const displayNamesInput = document.getElementById('displayNames');
            if (displayNamesInput && displayNamesInput.value.trim()) {
                const names = displayNamesInput.value.split(',').map(name => name.trim()).filter(name => name);
                names.forEach(name => {
                    formData.append('display_names[]', name);
                });
            }
            
            // Добавляем параметры уникализации
            formData.append('enable_uniquifier', document.getElementById('enable-uniquifier')?.checked || false);
            formData.append('uniquify_avatar', document.getElementById('uniquify-avatar')?.checked || false);
            formData.append('uniquify_bio', document.getElementById('uniquify-bio')?.checked || false);
            formData.append('uniquify_name', document.getElementById('uniquify-name')?.checked || false);
            
            // Получаем параметры потоков
            const threadCount = parseInt(document.getElementById('thread-count').value) || 3;
            const actionDelay = parseInt(document.getElementById('action-delay').value) || 5;
            
            // Добавляем параметры потоков в FormData
            formData.append('thread_count', threadCount);
            formData.append('action_delay', actionDelay);
            
            // Обработка множественных аватаров
            const selectionType = document.getElementById('account-selection-type').value;
            if (selectionType !== 'single' && uploadedAvatars.length > 0) {
                // Удаляем одиночный аватар если есть
                formData.delete('avatar');
                
                // Добавляем множественные аватары
                uploadedAvatars.forEach((file, index) => {
                    formData.append(`avatars[]`, file);
                });
                
                // Добавляем опции распределения
                formData.append('distribute_avatars', document.getElementById('distribute-avatars').checked);
                formData.append('randomize_avatars', document.getElementById('randomize-avatars').checked);
            }
            
            console.log('FormData prepared, sending request...');
            
            try {
                // Показываем, какие поля будут обновлены
                const updatingFields = [];
                if (formData.get('username')) updatingFields.push('юзернейм');
                if (formData.get('display_name') || formData.getAll('display_names[]').length > 0) updatingFields.push('имя');
                if (formData.get('bio')) updatingFields.push('описание');
                const avatarFile = formData.get('avatar');
                if (avatarFile && avatarFile.size > 0) updatingFields.push('аватар');
                if (formData.getAll('avatars[]').length > 0) updatingFields.push(`${formData.getAll('avatars[]').length} аватаров`);
                
                console.log('Updating fields:', updatingFields);
                console.log('Selected accounts:', Array.from(selectedAccounts));
                
                if (updatingFields.length > 0) {
                    showNotification(`Обновление профилей: ${updatingFields.join(', ')}...`, 'info');
                }
                
                console.log('Calling updateProfiles...');
                const result = await updateProfiles(formData);
                console.log('Update result:', result);
                
                if (result.success) {
                    showNotification(result.message, 'success');
                    
                    // Показываем детали обновления
                    if (result.results) {
                        const successCount = result.results.success.length;
                        const failedCount = result.results.failed.length;
                        
                        if (successCount > 0) {
                            console.log('Успешно обновлены аккаунты:', result.results.success);
                            
                            // Показываем список успешно обновленных полей
                            const successDetails = result.results.success.map(acc => 
                                `${acc.username}: обновлено`
                            ).join('\n');
                            
                            if (successCount <= 5) {
                                showNotification(`✅ Успешно обновлено:\n${successDetails}`, 'success', 5000);
                            }
                        }
                        
                        if (failedCount > 0) {
                            console.error('Ошибки при обновлении:', result.results.failed);
                            const failedDetails = result.results.failed.map(acc => 
                                `${acc.account_id}: ${acc.error}`
                            ).join('\n');
                            showNotification(`⚠️ Ошибки:\n${failedDetails}`, 'error', 7000);
                        }
                    }
                    
                    // Очищаем форму после успешного обновления
                    form.reset();
                    selectedAccounts.clear();
                    updateSelectedAccountsDisplay();
                    
                    // Обновляем список аккаунтов
                    await loadAccounts();
                } else {
                    showNotification(result.error || 'Ошибка при обновлении профилей', 'error');
                }
            } catch (error) {
                console.error('Error updating profiles:', error);
                showNotification('Ошибка при обновлении профилей', 'error');
            } finally {
                submitButton.disabled = false;
                submitButton.textContent = originalButtonText;
            }
        });
    }
}

function setupUniquifierToggle() {
    const toggle = document.getElementById('enable-uniquifier');
    const options = document.getElementById('uniquifier-options');
    
    if (toggle && options) {
        toggle.addEventListener('change', () => {
            options.classList.toggle('hidden', !toggle.checked);
        });
    }
}

// Functions for multiple names management
function toggleNameMode() {
    const checkbox = document.getElementById('use-single-name');
    const namesList = document.getElementById('names-list');
    const addButton = document.querySelector('[onclick="addNameField()"]');
    
    if (checkbox.checked) {
        // Single name mode
        namesList.classList.add('opacity-50', 'pointer-events-none');
        addButton.classList.add('hidden');
        
        // Keep only first input
        const inputs = namesList.querySelectorAll('.name-field');
        inputs.forEach((field, index) => {
            if (index > 0) field.remove();
        });
        
        // Update placeholder
        const firstInput = namesList.querySelector('input');
        if (firstInput) {
            firstInput.placeholder = 'Одно имя для всех аккаунтов';
        }
    } else {
        // Multiple names mode
        namesList.classList.remove('opacity-50', 'pointer-events-none');
        addButton.classList.remove('hidden');
        
        // Update placeholder
        const firstInput = namesList.querySelector('input');
        if (firstInput) {
            firstInput.placeholder = 'Имя 1';
        }
    }
}

function addNameField() {
    const namesList = document.getElementById('names-list');
    const fieldCount = namesList.querySelectorAll('.name-field').length;
    
    const newField = document.createElement('div');
    newField.className = 'name-field flex gap-2';
    newField.innerHTML = `
        <input type="text" class="flex-1 px-3 py-2 bg-slate-800 border border-slate-600 rounded-md text-white placeholder-slate-400" placeholder="Имя ${fieldCount + 1}">
        <button type="button" onclick="removeNameField(this)" class="px-3 py-2 bg-red-600 hover:bg-red-700 text-white rounded-md transition-colors">
            <i data-lucide="x" class="h-4 w-4"></i>
        </button>
    `;
    
    namesList.appendChild(newField);
    lucide.createIcons();
    
    // Update placeholders
    updateNamePlaceholders();
}

function removeNameField(button) {
    const field = button.closest('.name-field');
    const namesList = document.getElementById('names-list');
    
    // Don't remove if it's the last field
    if (namesList.querySelectorAll('.name-field').length > 1) {
        field.remove();
        updateNamePlaceholders();
    }
}

function updateNamePlaceholders() {
    const namesList = document.getElementById('names-list');
    const fields = namesList.querySelectorAll('.name-field input');
    
    fields.forEach((input, index) => {
        input.placeholder = `Имя ${index + 1}`;
    });
}

function previewProfilePicture(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    if (!file.type.startsWith('image/')) {
        showNotification('Пожалуйста, выберите изображение', 'error');
        return;
    }
    
    if (file.size > 2 * 1024 * 1024) {
        showNotification('Размер файла не должен превышать 2MB', 'error');
        return;
    }
    
    const reader = new FileReader();
    reader.onload = (e) => {
        const preview = document.getElementById('profile-picture-preview');
        const defaultIcon = document.getElementById('default-avatar');
        
        if (preview && defaultIcon) {
            preview.src = e.target.result;
            preview.classList.remove('hidden');
            defaultIcon.classList.add('hidden');
            updatePreview();
        }
    };
    reader.readAsDataURL(file);
}

function updatePreview() {
    const username = document.getElementById('new-username').value || 'username';
    const name = document.getElementById('profile-name').value || 'Имя профиля';
    const bio = document.getElementById('profile-bio').value || 'Описание профиля появится здесь...';
    
    document.getElementById('preview-username').textContent = '@' + username;
    document.getElementById('preview-name').textContent = name;
    document.getElementById('preview-bio').textContent = bio;
    
    // Update preview avatar
    const previewImg = document.getElementById('profile-picture-preview');
    const previewAvatar = document.getElementById('preview-avatar');
    const previewDefault = document.getElementById('preview-default-avatar');
    
    if (previewImg && !previewImg.classList.contains('hidden')) {
        previewAvatar.src = previewImg.src;
        previewAvatar.classList.remove('hidden');
        previewDefault.classList.add('hidden');
    } else {
        previewAvatar.classList.add('hidden');
        previewDefault.classList.remove('hidden');
    }
}

async function applyProfileChanges() {
    const selectionType = document.getElementById('account-selection-type').value;
    
    // Validate bio length
    const bio = document.getElementById('profile-bio').value;
    if (bio.length > 150) {
        showNotification('Описание профиля не должно превышать 150 символов', 'error');
        return;
    }
    
    // Get selected accounts based on selection type
    let targetAccounts = [];
    
    switch (selectionType) {
        case 'single':
            const singleId = document.getElementById('single-account').value;
            if (!singleId) {
                showNotification('Выберите аккаунт', 'error');
                return;
            }
            targetAccounts = [singleId];
            break;
            
        case 'multiple':
            const selectedCheckboxes = document.querySelectorAll('.account-checkbox:checked');
            targetAccounts = Array.from(selectedCheckboxes).map(cb => cb.value);
            if (targetAccounts.length === 0) {
                showNotification('Выберите хотя бы один аккаунт', 'error');
                return;
            }
            break;
            
        case 'group':
            const groupId = document.getElementById('account-group').value;
            if (!groupId) {
                showNotification('Выберите группу', 'error');
                return;
            }
            // Get accounts from group
            const group = groups.find(g => g.id === groupId);
            targetAccounts = group?.accounts || [];
            break;
            
        case 'all':
            targetAccounts = accounts.map(acc => acc.id);
            break;
    }
    
    if (targetAccounts.length === 0) {
        showNotification('Нет аккаунтов для применения изменений', 'error');
        return;
    }
    
    // Prepare profile data
        const profileData = {
        username: document.getElementById('new-username').value,
        bio: document.getElementById('profile-bio').value,
        enable_uniquifier: document.getElementById('enable-uniquifier').checked,
        uniquify_avatar: document.getElementById('uniquify-avatar').checked,
        uniquify_bio: document.getElementById('uniquify-bio').checked,
        uniquify_name: document.getElementById('uniquify-name').checked
    };
    
    // Handle display names based on selection type
    if (selectionType === 'single') {
        // For single account, use the single name input
        profileData.display_name = document.getElementById('profile-name').value;
    } else {
        // For multiple accounts, check if using single name or multiple
        const useSingleName = document.getElementById('use-single-name').checked;
        
        if (useSingleName) {
            // Use the first name field for all accounts
            const firstNameInput = document.querySelector('#names-list input');
            profileData.display_name = firstNameInput ? firstNameInput.value : '';
        } else {
            // Collect all names from the list
            const nameInputs = document.querySelectorAll('#names-list input');
            const names = Array.from(nameInputs)
                .map(input => input.value.trim())
                .filter(name => name.length > 0);
            
            if (names.length > 0) {
                profileData.display_names = names;
            }
        }
    }
    
    // Get profile picture if uploaded
    const pictureFile = document.getElementById('profile-picture-input').files[0];
    
    try {
        showNotification('Применение изменений...', 'info');
        
        const result = await api.updateProfiles(targetAccounts, profileData, pictureFile);
        
        if (result.results) {
            const successCount = result.results.success.length;
            const failedCount = result.results.failed.length;
            
            if (successCount > 0 && failedCount === 0) {
                showNotification(`✅ Изменения успешно применены к ${successCount} аккаунт${successCount > 1 ? 'ам' : 'у'}`, 'success');
            } else if (successCount > 0 && failedCount > 0) {
                showNotification(`⚠️ Изменения применены к ${successCount} из ${targetAccounts.length} аккаунтов`, 'warning');
            } else {
                showNotification(`❌ Не удалось применить изменения`, 'error');
            }
            
            // Log details for debugging
            if (result.results.failed.length > 0) {
                console.error('Failed accounts:', result.results.failed);
            }
        } else {
            showNotification(result.message || `✅ Изменения применены к ${targetAccounts.length} аккаунт${targetAccounts.length > 1 ? 'ам' : 'у'}`, 'success');
        }
        
        // Clear form after success
        if (selectionType !== 'single') {
            clearForm();
        }
    } catch (error) {
        console.error('Error applying profile changes:', error);
        showNotification('Ошибка при применении изменений: ' + error.message, 'error');
    }
}

async function previewChanges() {
    updatePreview();
    showNotification('Предпросмотр обновлен', 'info');
}

async function generateProfileData() {
    try {
        showNotification('Генерация данных профиля...', 'info');
        
        // This would call an AI service or use predefined templates
        const generated = await api.generateProfileData();
        
        // Fill form with generated data
            if (generated.username) document.getElementById('new-username').value = generated.username;
    if (generated.name) document.getElementById('profile-name').value = generated.name;
    if (generated.bio) document.getElementById('profile-bio').value = generated.bio;
        
        updatePreview();
        showNotification('Данные профиля сгенерированы', 'success');
    } catch (error) {
        console.error('Error generating profile data:', error);
        showNotification('Ошибка при генерации данных', 'error');
    }
}

function loadTemplate() {
    // Show template selection modal
    showNotification('Загрузка шаблонов (будет реализовано)', 'info');
}

function clearForm() {
    document.getElementById('new-username').value = '';
    document.getElementById('profile-name').value = '';
    document.getElementById('profile-bio').value = '';
    
    document.getElementById('enable-uniquifier').checked = false;
    document.getElementById('uniquify-avatar').checked = true;
    document.getElementById('uniquify-bio').checked = false;
    document.getElementById('uniquify-name').checked = false;
    document.getElementById('uniquifier-options').classList.add('hidden');
    
    // Clear profile picture
    document.getElementById('profile-picture-input').value = '';
    document.getElementById('profile-picture-preview').classList.add('hidden');
    document.getElementById('default-avatar').classList.remove('hidden');
    
    // Reset bio counter
    document.getElementById('bio-counter').textContent = '0/150';
    
    clearPreview();
}

function clearPreview() {
    document.getElementById('preview-username').textContent = '@username';
    document.getElementById('preview-name').textContent = 'Имя профиля';
    document.getElementById('preview-bio').textContent = 'Описание профиля появится здесь...';
    document.getElementById('preview-avatar').classList.add('hidden');
    document.getElementById('preview-default-avatar').classList.remove('hidden');
}

// Notification function
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
    
    // Animate in
    setTimeout(() => {
        notification.classList.remove('translate-x-full');
    }, 10);
    
    // Remove after duration
    setTimeout(() => {
        notification.classList.add('translate-x-full');
        setTimeout(() => {
            notification.remove();
        }, 300);
    }, duration);
}

function updateSelectedAccountsDisplay() {
    // Эта функция обновляет отображение выбранных аккаунтов
    // В данном случае она вызывает updateSelectedAccountsCount
    updateSelectedAccountsCount();
}

// Multiple avatars handling
function handleMultipleAvatars(event) {
    const files = Array.from(event.target.files);
    const previewGrid = document.getElementById('avatars-preview-grid');
    
    // Clear previous uploads
    uploadedAvatars = [];
    previewGrid.innerHTML = '';
    
    if (files.length === 0) {
        previewGrid.classList.add('hidden');
        return;
    }
    
    previewGrid.classList.remove('hidden');
    
    files.forEach((file, index) => {
        if (file.type.startsWith('image/')) {
            uploadedAvatars.push(file);
            
            const reader = new FileReader();
            reader.onload = (e) => {
                const avatarItem = document.createElement('div');
                avatarItem.className = 'relative group';
                avatarItem.innerHTML = `
                    <div class="aspect-square bg-slate-700 rounded-lg overflow-hidden">
                        <img src="${e.target.result}" class="w-full h-full object-cover" alt="Avatar ${index + 1}">
                    </div>
                    <button type="button" onclick="removeAvatar(${index})" 
                            class="absolute top-1 right-1 bg-red-600 text-white p-1 rounded opacity-0 group-hover:opacity-100 transition-opacity">
                        <i data-lucide="x" class="h-3 w-3"></i>
                    </button>
                    <div class="text-center mt-1">
                        <span class="text-xs text-slate-400">${file.name.substring(0, 10)}...</span>
                    </div>
                `;
                previewGrid.appendChild(avatarItem);
                
                // Re-create lucide icons
                setTimeout(() => lucide.createIcons(), 100);
            };
            reader.readAsDataURL(file);
        }
    });
    
    showNotification(`Загружено ${files.length} изображений`, 'success');
}

function removeAvatar(index) {
    uploadedAvatars.splice(index, 1);
    
    // Re-render the grid
    const input = document.getElementById('multiple-avatars-input');
    const dataTransfer = new DataTransfer();
    uploadedAvatars.forEach(file => dataTransfer.items.add(file));
    input.files = dataTransfer.files;
    
    handleMultipleAvatars({ target: input });
}

// Drag and drop support
document.addEventListener('DOMContentLoaded', () => {
    const dropZone = document.querySelector('#multiple-avatars-mode .bg-slate-800');
    
    if (dropZone) {
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            dropZone.addEventListener(eventName, preventDefaults, false);
        });
        
        function preventDefaults(e) {
            e.preventDefault();
            e.stopPropagation();
        }
        
        ['dragenter', 'dragover'].forEach(eventName => {
            dropZone.addEventListener(eventName, highlight, false);
        });
        
        ['dragleave', 'drop'].forEach(eventName => {
            dropZone.addEventListener(eventName, unhighlight, false);
        });
        
        function highlight(e) {
            dropZone.classList.add('border-blue-500', 'bg-slate-700');
        }
        
        function unhighlight(e) {
            dropZone.classList.remove('border-blue-500', 'bg-slate-700');
        }
        
        dropZone.addEventListener('drop', handleDrop, false);
        
        function handleDrop(e) {
            const dt = e.dataTransfer;
            const files = dt.files;
            
            document.getElementById('multiple-avatars-input').files = files;
            handleMultipleAvatars({ target: { files } });
        }
    }
}); 