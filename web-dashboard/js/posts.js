// Posts page JavaScript

let currentTab = 'all';
let posts = [];
let accounts = [];
let groups = [];
let selectedAccounts = [];
let selectedGroups = [];
let uploadedFiles = [];
let currentSearchTerm = '';

document.addEventListener('DOMContentLoaded', async () => {
    console.log('DOM Content Loaded - posts.js');
    await Promise.all([loadPosts(), loadAccounts(), loadGroups()]);
    setupContentUpload();
    setupEventListeners();
    console.log('All initialization completed');
});

async function loadPosts() {
    try {
        showLoading();
        posts = await api.getPosts();
        renderPosts();
        updateCounts();
    } catch (error) {
        console.error('Error loading posts:', error);
        showNotification('Ошибка при загрузке постов: ' + error.message, 'error');
        posts = [];
        renderPosts();
        updateCounts();
    }
}

async function loadAccounts() {
    try {
        console.log('loadAccounts: Начинается загрузка аккаунтов...');
        accounts = await api.getAccounts();
        console.log('loadAccounts: Получены аккаунты:', accounts);
        console.log('loadAccounts: Количество аккаунтов:', accounts.length);
        
        if (accounts.length > 0) {
            console.log('loadAccounts: Первые 3 аккаунта:', accounts.slice(0, 3));
            accounts.forEach((acc, index) => {
                if (index < 5) { // Логируем первые 5 аккаунтов
                    console.log(`loadAccounts: Аккаунт ${index + 1}:`, {
                        id: acc.id,
                        username: acc.username,
                        is_active: acc.is_active,
                        keys: Object.keys(acc)
                    });
                }
            });
        }
        
        populateAccountsSelect();
        console.log('loadAccounts: Завершена загрузка аккаунтов');
    } catch (error) {
        console.error('loadAccounts: Ошибка при загрузке аккаунтов:', error);
    }
}

async function loadGroups() {
    try {
        groups = await api.getGroups();
        populateGroupsSelect();
    } catch (error) {
        console.error('Error loading groups:', error);
    }
}

function populateAccountsSelect() {
    const accountsList = document.getElementById('accounts-list');
    accountsList.innerHTML = '';
    
    accounts.forEach(account => {
        const accountItem = document.createElement('label');
        accountItem.className = 'flex items-center gap-2 p-2 hover:bg-slate-600 rounded cursor-pointer transition-colors';
        
        accountItem.innerHTML = `
            <input 
                type="checkbox" 
                value="${account.id}" 
                class="account-checkbox w-4 h-4 text-blue-600 bg-slate-800 border-slate-500 rounded focus:ring-blue-500"
                ${!account.is_active ? 'disabled' : ''}
            >
            <span class="${!account.is_active ? 'text-slate-400' : 'text-white'}">
                @${account.username} ${!account.is_active ? '(Неактивен)' : ''}
            </span>
        `;
        
        accountsList.appendChild(accountItem);
    });
    
    // Add event listeners to all checkboxes
    const checkboxes = accountsList.querySelectorAll('.account-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.addEventListener('change', updateSelectedAccountsDisplay);
    });
}

function populateGroupsSelect() {
    const select = document.getElementById('post-group');
    if (select) {
        select.innerHTML = '<option value="">Выберите группу</option>' + 
            groups.map(group => `<option value="${group.id}">${group.name}</option>`).join('');
    }
}

// New functions for the redesigned interface

function openPublishModal() {
    const modal = document.getElementById('publish-modal');
    modal.classList.remove('hidden');
    modal.classList.add('modal-backdrop');
    
    // Reset form
    resetPublishForm();
    
    // Set default values
    const now = new Date();
    now.setMinutes(now.getMinutes() - now.getTimezoneOffset());
    document.getElementById('post-date').value = now.toISOString().slice(0, 16);
}

function closePublishModal() {
    const modal = document.getElementById('publish-modal');
    modal.classList.add('hidden');
    modal.classList.remove('modal-backdrop');
    resetPublishForm();
}

function resetPublishForm() {
    document.querySelector('#publish-modal form').reset();
    document.getElementById('content-preview').classList.add('hidden');
    document.getElementById('carousel-preview').classList.add('hidden');
    document.getElementById('hashtags-section').style.display = 'block';
    document.getElementById('schedule-section').classList.add('hidden');
    document.getElementById('account-selection-section').style.display = 'block';
    
    // Reset uploaded files
    uploadedFiles = [];
    
    // Clear preview wrapper
    const previewWrapper = document.getElementById('preview-wrapper');
    if (previewWrapper) {
        previewWrapper.remove();
    }
    
    // Clear file input
    const fileInput = document.getElementById('post-content');
    if (fileInput) {
        fileInput.value = '';
    }
    
    // Clear selected accounts
    clearSelectedAccounts();
    
    // Reset story settings
    document.getElementById('story-link').value = '';
    
    // Reset post type to feed
    document.querySelector('input[name="post-type"][value="feed"]').checked = true;
    togglePostType();
}

function togglePostType() {
    const selectedType = document.querySelector('input[name="post-type"]:checked').value;
    const hashtagsSection = document.getElementById('hashtags-section');
    const storySettingsSection = document.getElementById('story-settings-section');
    const captionSection = document.getElementById('caption-section');
    const uploadHint = document.getElementById('upload-hint');
    const contentInput = document.getElementById('post-content');
    const contentPreview = document.getElementById('content-preview');
    const carouselPreview = document.getElementById('carousel-preview');
    
    // Reset previews
    contentPreview.classList.add('hidden');
    carouselPreview.classList.add('hidden');
    
    // Reset uploaded files for carousel
    if (selectedType !== 'carousel') {
        uploadedFiles = [];
    }
    
    // Update upload hints and accepted file types
    switch (selectedType) {
        case 'feed':
            uploadHint.textContent = 'Изображения: JPG, PNG до 10MB';
            contentInput.accept = 'image/*';
            contentInput.multiple = false;
            hashtagsSection.style.display = 'block';
            storySettingsSection.classList.add('hidden');
            captionSection.style.display = 'block';
            break;
        case 'carousel':
            uploadHint.textContent = 'Изображения: JPG, PNG до 10MB (можно выбрать до 10 файлов)';
            contentInput.accept = 'image/*';
            contentInput.multiple = true;
            hashtagsSection.style.display = 'block';
            storySettingsSection.classList.add('hidden');
            captionSection.style.display = 'block';
            break;
        case 'reels':
            uploadHint.textContent = 'Видео: MP4, MOV до 100MB';
            contentInput.accept = 'video/*';
            contentInput.multiple = false;
            hashtagsSection.style.display = 'block';
            storySettingsSection.classList.add('hidden');
            captionSection.style.display = 'block';
            break;
        case 'story':
            uploadHint.textContent = 'Изображения или видео: JPG, PNG, MP4 до 50MB';
            contentInput.accept = 'image/*,video/*';
            contentInput.multiple = false;
            hashtagsSection.style.display = 'none';
            storySettingsSection.classList.remove('hidden');
            captionSection.style.display = 'none';
            break;
    }
}

function toggleScheduling() {
    const selectedTime = document.querySelector('input[name="publish-time"]:checked').value;
    const scheduleSection = document.getElementById('schedule-section');
    
    if (selectedTime === 'scheduled') {
        scheduleSection.classList.remove('hidden');
        scheduleSection.classList.add('section-toggle');
    } else {
        scheduleSection.classList.add('hidden');
        scheduleSection.classList.remove('section-toggle');
    }
}

function toggleAllAccounts() {
    const allAccountsChecked = document.getElementById('publish-all-accounts').checked;
    const accountSelectionSection = document.getElementById('account-selection-section');
    
    if (allAccountsChecked) {
        accountSelectionSection.style.display = 'none';
        clearSelectedAccounts();
    } else {
        accountSelectionSection.style.display = 'block';
    }
}

// New function to update selected accounts display
function updateSelectedAccountsDisplay() {
    const checkboxes = document.querySelectorAll('.account-checkbox:checked');
    const manualInput = document.getElementById('manual-accounts');
    const displayContainer = document.getElementById('selected-accounts-display');
    const listContainer = document.getElementById('selected-accounts-list');
    
    // Get selected accounts from checkboxes
    const selectedFromCheckboxes = Array.from(checkboxes).map(cb => parseInt(cb.value));
    
    // Get manually entered accounts
    const manualAccounts = manualInput.value
        .split(',')
        .map(username => username.trim())
        .filter(username => username.length > 0);
    
    // Combine all selected accounts
    const allSelectedAccounts = [];
    
    // Add accounts from checkboxes
    selectedFromCheckboxes.forEach(accountId => {
        const account = accounts.find(acc => acc.id === accountId);
        if (account) {
            allSelectedAccounts.push({
                id: account.id,
                username: account.username,
                source: 'checkbox'
            });
        }
    });
    
    // Add manual accounts
    manualAccounts.forEach(username => {
        // Remove @ symbol if present
        const cleanUsername = username.replace('@', '');
        const account = accounts.find(acc => acc.username === cleanUsername);
        
        if (account) {
            // Check if not already added
            if (!allSelectedAccounts.find(a => a.id === account.id)) {
                allSelectedAccounts.push({
                    id: account.id,
                    username: account.username,
                    source: 'manual'
                });
            }
        } else {
            // Account not found, but still add it to show error
            allSelectedAccounts.push({
                id: null,
                username: cleanUsername,
                source: 'manual',
                error: true
            });
        }
    });
    
    // Clear and rebuild display
    listContainer.innerHTML = '';
    
    if (allSelectedAccounts.length > 0) {
        displayContainer.classList.remove('hidden');
        
        allSelectedAccounts.forEach(account => {
            const tag = document.createElement('div');
            tag.className = `inline-flex items-center gap-1 px-3 py-1 rounded-full text-sm ${
                account.error 
                    ? 'bg-red-600/20 text-red-400 border border-red-600/50' 
                    : 'bg-blue-600/20 text-blue-400 border border-blue-600/50'
            }`;
            
            tag.innerHTML = `
                <span>@${account.username}</span>
                ${account.error ? '<span class="text-xs">(не найден)</span>' : ''}
                <button type="button" onclick="removeSelectedAccount('${account.username}', '${account.source}')" class="ml-1 hover:text-white transition-colors">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                    </svg>
                </button>
            `;
            
            listContainer.appendChild(tag);
        });
    } else {
        displayContainer.classList.add('hidden');
    }
}

// Function to remove selected account
function removeSelectedAccount(username, source) {
    if (source === 'checkbox') {
        // Uncheck checkbox
        const checkboxes = document.querySelectorAll('.account-checkbox');
        checkboxes.forEach(checkbox => {
            const account = accounts.find(acc => acc.id === parseInt(checkbox.value));
            if (account && account.username === username) {
                checkbox.checked = false;
            }
        });
    } else if (source === 'manual') {
        // Remove from manual input
        const manualInput = document.getElementById('manual-accounts');
        const currentAccounts = manualInput.value
            .split(',')
            .map(u => u.trim())
            .filter(u => {
                // Remove @ symbol for comparison
                const cleanU = u.replace('@', '');
                return cleanU !== username;
            });
        manualInput.value = currentAccounts.join(', ');
    }
    
    // Update display
    updateSelectedAccountsDisplay();
}

// Function to clear all selected accounts
function clearSelectedAccounts() {
    // Clear checkboxes
    const checkboxes = document.querySelectorAll('.account-checkbox');
    checkboxes.forEach(checkbox => checkbox.checked = false);
    
    // Clear manual input
    const manualInput = document.getElementById('manual-accounts');
    manualInput.value = '';
    
    // Update display
    updateSelectedAccountsDisplay();
}

function setupContentUpload() {
    const uploadArea = document.getElementById('content-upload-area');
    const fileInput = document.getElementById('post-content');
    const preview = document.getElementById('content-preview');
    const previewContent = document.getElementById('preview-content');

    uploadArea.addEventListener('click', () => fileInput.click());
    
    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.classList.add('border-blue-500');
    });
    
    uploadArea.addEventListener('dragleave', () => {
        uploadArea.classList.remove('border-blue-500');
    });
    
    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.classList.remove('border-blue-500');
        const files = e.dataTransfer.files;
        const selectedType = document.querySelector('input[name="post-type"]:checked').value;
        
        if (selectedType === 'carousel' && files.length > 0) {
            handleCarouselUpload(files);
        } else if (files.length > 0) {
            handleContentUpload(files[0]);
        }
    });
    
    fileInput.addEventListener('change', (e) => {
        const selectedType = document.querySelector('input[name="post-type"]:checked').value;
        
        if (selectedType === 'carousel' && e.target.files.length > 0) {
            handleCarouselUpload(e.target.files);
        } else if (e.target.files.length > 0) {
            handleContentUpload(e.target.files[0]);
        }
    });
}

function handleContentUpload(file) {
    const selectedType = document.querySelector('input[name="post-type"]:checked').value;
    
    // Validate file type based on selected post type
    if ((selectedType === 'feed' || selectedType === 'carousel') && !file.type.startsWith('image/')) {
        alert('Для публикации в ленту выберите изображение');
        return;
    }
    
    if (selectedType === 'reels' && !file.type.startsWith('video/')) {
        alert('Для Reels выберите видео');
        return;
    }
    
    // Validate file size
    const maxSize = selectedType === 'reels' ? 100 * 1024 * 1024 : 50 * 1024 * 1024; // 100MB for reels, 50MB for others
    if (file.size > maxSize) {
        alert(`Файл слишком большой. Максимальный размер: ${maxSize / (1024 * 1024)}MB`);
        return;
    }
    
    const reader = new FileReader();
    reader.onload = (e) => {
        const contentPreview = document.getElementById('content-preview');
        const previewContent = document.getElementById('preview-content');
        
        // Create preview with delete button
        const previewWrapper = document.createElement('div');
        previewWrapper.className = 'relative group';
        previewWrapper.id = 'preview-wrapper';
        
        if (file.type.startsWith('video/')) {
            previewWrapper.innerHTML = `
                <video class="w-full max-w-xs rounded-lg mx-auto" controls>
                    <source src="${e.target.result}" type="${file.type}">
                </video>
                <div class="absolute inset-0 bg-black bg-opacity-50 opacity-0 group-hover:opacity-100 transition-opacity rounded-lg flex items-center justify-center">
                    <button type="button" onclick="removeContent()" class="bg-red-600 hover:bg-red-700 text-white rounded-full p-3">
                        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path>
                        </svg>
                    </button>
                </div>
            `;
        } else {
            previewWrapper.innerHTML = `
                <img class="w-full max-w-xs rounded-lg mx-auto" src="${e.target.result}" alt="Preview">
                <div class="absolute inset-0 bg-black bg-opacity-50 opacity-0 group-hover:opacity-100 transition-opacity rounded-lg flex items-center justify-center">
                    <button type="button" onclick="removeContent()" class="bg-red-600 hover:bg-red-700 text-white rounded-full p-3">
                        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path>
                        </svg>
                    </button>
                </div>
            `;
        }
        
        // Replace old preview with new one
        if (previewContent) {
            previewContent.replaceWith(previewWrapper);
        } else {
            // If no preview exists, append the wrapper
            contentPreview.innerHTML = '';
            contentPreview.appendChild(previewWrapper);
        }
        
        contentPreview.classList.remove('hidden');
    };
    reader.readAsDataURL(file);
}

// Add function to remove single content
function removeContent() {
    // Clear the file input
    const fileInput = document.getElementById('post-content');
    fileInput.value = '';
    
    // Hide preview
    document.getElementById('content-preview').classList.add('hidden');
    
    // Remove preview wrapper
    const previewWrapper = document.getElementById('preview-wrapper');
    if (previewWrapper) {
        previewWrapper.remove();
    }
    
    // Reset upload hint based on selected type
    const selectedType = document.querySelector('input[name="post-type"]:checked').value;
    const uploadHint = document.getElementById('upload-hint');
    
    if (selectedType === 'feed') {
        uploadHint.textContent = 'Изображения: JPG, PNG до 10MB';
    } else if (selectedType === 'reels') {
        uploadHint.textContent = 'Видео: MP4, MOV до 100MB';
    }
}

function handleCarouselUpload(files) {
    const fileArray = Array.from(files);
    
    // Add new files to existing ones instead of replacing
    const newFiles = [...uploadedFiles, ...fileArray];
    
    // Validate max 10 files
    if (newFiles.length > 10) {
        alert(`Максимум 10 изображений для карусели. Уже выбрано: ${uploadedFiles.length}, пытаетесь добавить: ${fileArray.length}`);
        return;
    }
    
    // Validate all files are images
    const nonImageFiles = fileArray.filter(file => !file.type.startsWith('image/'));
    if (nonImageFiles.length > 0) {
        alert('Для карусели можно загружать только изображения');
        return;
    }
    
    // Validate file sizes
    const maxSize = 10 * 1024 * 1024; // 10MB per image
    const oversizedFiles = fileArray.filter(file => file.size > maxSize);
    if (oversizedFiles.length > 0) {
        alert('Некоторые файлы превышают максимальный размер 10MB');
        return;
    }
    
    // Store files (append to existing)
    uploadedFiles = newFiles;
    
    // Show carousel preview
    const carouselPreview = document.getElementById('carousel-preview');
    const filesList = document.getElementById('carousel-files-list');
    
    filesList.innerHTML = '';
    
    // Show all files with correct numbering
    uploadedFiles.forEach((file, index) => {
        const reader = new FileReader();
        reader.onload = (e) => {
            const previewItem = document.createElement('div');
            previewItem.className = 'relative group cursor-move';
            previewItem.draggable = true;
            previewItem.dataset.index = index;
            previewItem.dataset.position = index + 1; // Store current position
            previewItem.innerHTML = `
                <img src="${e.target.result}" alt="Carousel item ${index + 1}" class="w-full h-32 object-cover rounded-lg pointer-events-none">
                <div class="absolute inset-0 bg-black bg-opacity-50 opacity-0 group-hover:opacity-100 transition-opacity rounded-lg flex items-center justify-between p-2 pointer-events-none">
                    <div class="flex gap-1 pointer-events-auto">
                        ${index > 0 ? `<button type="button" onclick="moveCarouselItem(${index}, ${index - 1})" class="bg-blue-600 hover:bg-blue-700 text-white rounded p-1" title="Переместить влево">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"></path>
                            </svg>
                        </button>` : ''}
                        ${index < uploadedFiles.length - 1 ? `<button type="button" onclick="moveCarouselItem(${index}, ${index + 1})" class="bg-blue-600 hover:bg-blue-700 text-white rounded p-1" title="Переместить вправо">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                            </svg>
                        </button>` : ''}
                    </div>
                    <button type="button" onclick="removeCarouselItem(${index})" class="bg-red-600 hover:bg-red-700 text-white rounded-full p-2 pointer-events-auto">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                        </svg>
                    </button>
                </div>
                <div class="absolute bottom-1 right-1 bg-black bg-opacity-75 text-white text-xs px-2 py-1 rounded pointer-events-none carousel-position">
                    ${index + 1}
                </div>
            `;
            
            // Add drag and drop event listeners
            previewItem.addEventListener('dragstart', handleDragStart);
            previewItem.addEventListener('dragover', handleDragOver);
            previewItem.addEventListener('drop', handleDrop);
            previewItem.addEventListener('dragend', handleDragEnd);
            
            filesList.appendChild(previewItem);
        };
        reader.readAsDataURL(file);
    });
    
    carouselPreview.classList.remove('hidden');
    
    // Update upload hint to show current count
    const uploadHint = document.getElementById('upload-hint');
    uploadHint.textContent = `Изображения: JPG, PNG до 10MB (выбрано ${uploadedFiles.length} из 10)`;
}

async function createPost(event) {
    event.preventDefault();
    
    // Get form data
    const postType = document.querySelector('input[name="post-type"]:checked').value;
    const caption = document.getElementById('post-caption').value;
    const hashtags = document.getElementById('post-hashtags').value;
    const publishTime = document.querySelector('input[name="publish-time"]:checked').value;
    const scheduledTime = publishTime === 'scheduled' ? document.getElementById('post-date').value : null;
    const uniquifyContent = document.getElementById('uniquify-content').checked;
    const publishAllAccounts = document.getElementById('publish-all-accounts').checked;
    
    // Получаем настройки публикации
    const concurrentThreads = parseInt(document.getElementById('concurrent-threads').value);
    const publishDelay = parseInt(document.getElementById('publish-delay').value);
    
    // Get content based on post type
    let content;
    if (postType === 'carousel') {
        if (uploadedFiles.length === 0) {
            alert('Пожалуйста, выберите изображения для карусели');
            return;
        }
        content = uploadedFiles;
    } else {
        content = document.getElementById('post-content').files[0];
        if (!content) {
            alert('Пожалуйста, выберите контент для публикации');
            return;
        }
    }
    
    // Описание обязательно только для постов, каруселей и reels
    if (postType !== 'story' && !caption.trim()) {
        alert('Пожалуйста, введите описание');
        return;
    }
    
    // Get target accounts
    let targetAccounts = [];
    
    if (publishAllAccounts) {
        console.log('Total accounts:', accounts.length);
        console.log('Accounts data:', accounts);
        
        const activeAccounts = accounts.filter(acc => acc.is_active);
        console.log('Active accounts:', activeAccounts.length);
        console.log('Active accounts data:', activeAccounts);
        
        targetAccounts = activeAccounts.map(acc => acc.id);
        
        if (targetAccounts.length === 0) {
            // Показываем более детальную информацию
            const totalAccounts = accounts.length;
            const activeCount = activeAccounts.length;
            
            alert(`Нет активных аккаунтов для публикации.\nВсего аккаунтов: ${totalAccounts}\nАктивных: ${activeCount}\n\nПроверьте статус аккаунтов в разделе "Аккаунты".`);
            return;
        }
    } else {
        // Get selected accounts from checkboxes
        const checkboxes = document.querySelectorAll('.account-checkbox:checked');
        const selectedFromCheckboxes = Array.from(checkboxes).map(cb => parseInt(cb.value));
        
        console.log('Selected from checkboxes:', selectedFromCheckboxes);
        
        // Get manually entered accounts
        const manualAccountsInput = document.getElementById('manual-accounts');
        const manualAccountsText = manualAccountsInput.value.trim();
        const manualAccounts = [];
        
        if (manualAccountsText) {
            const usernames = manualAccountsText
                .split(',')
                .map(username => username.trim())
                .filter(username => username.length > 0);
                
            console.log('Manual usernames entered:', usernames);
            
            usernames.forEach(username => {
                // Remove @ symbol if present
                const cleanUsername = username.replace('@', '');
                const account = accounts.find(acc => acc.username === cleanUsername);
                
                if (account) {
                    manualAccounts.push(account.id);
                    console.log(`Found account for ${cleanUsername}:`, account.id);
                } else {
                    console.warn(`Account not found for username: ${cleanUsername}`);
                }
            });
        }
        
        console.log('Manual account IDs:', manualAccounts);
        
        // Combine and deduplicate
        targetAccounts = [...new Set([...selectedFromCheckboxes, ...manualAccounts])];
        
        console.log('Final target accounts:', targetAccounts);
        
        if (targetAccounts.length === 0) {
            alert('Выберите хотя бы один аккаунт или введите username вручную');
            return;
        }
    }
    
    try {
        // Show loading state
        const submitButton = event.target.querySelector('button[type="submit"]');
        const originalText = submitButton.innerHTML;
        submitButton.innerHTML = '<i data-lucide="loader" class="h-5 w-5 mr-2 inline animate-spin"></i>Публикуется...';
        submitButton.disabled = true;
        
        // Create post data с новыми параметрами
        const postData = {
            type: postType,
            caption: caption,
            hashtags: hashtags,
            scheduled_time: scheduledTime,
            accounts: targetAccounts,
            uniquify: uniquifyContent,
            publish_now: publishTime === 'now',
            concurrent_threads: concurrentThreads,
            publish_delay: publishDelay
        };
        
        // Добавляем настройки для историй
        if (postType === 'story') {
            postData.story_link = document.getElementById('story-link').value;
        }
        
        // Call API based on post type
        if (postType === 'carousel') {
            await api.createCarouselPost(content, postData);
        } else if (postType === 'story') {
            await api.createStory(content, postData);
        } else {
            await api.createPost(content, postData);
        }
        
        closePublishModal();
        await loadPosts();
        
        showNotification('Пост успешно создан!', 'success');
        
    } catch (error) {
        console.error('Error creating post:', error);
        showNotification('Ошибка при создании поста: ' + error.message, 'error');
    } finally {
        // Reset button state
        const submitButton = event.target.querySelector('button[type="submit"]');
        if (submitButton) {
            submitButton.innerHTML = '<i data-lucide="send" class="h-5 w-5 mr-2 inline"></i>Опубликовать';
            submitButton.disabled = false;
            lucide.createIcons();
        }
    }
}

function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `fixed top-4 right-4 z-50 p-4 rounded-lg text-white max-w-sm transform transition-all duration-300 translate-x-full`;
    
    // Set color based on type
    switch (type) {
        case 'success':
            notification.className += ' bg-green-600';
            break;
        case 'error':
            notification.className += ' bg-red-600';
            break;
        case 'warning':
            notification.className += ' bg-yellow-600';
            break;
        default:
            notification.className += ' bg-blue-600';
    }
    
    // Разбиваем сообщение на строки для правильного отображения
    const messageLines = message.split('\n').filter(line => line.trim());
    const messageHtml = messageLines.length > 1 
        ? messageLines.map(line => `<div>${line}</div>`).join('')
        : message;
    
    notification.innerHTML = `
        <div class="flex items-start gap-2">
            <i data-lucide="${type === 'success' ? 'check-circle' : type === 'error' ? 'x-circle' : type === 'warning' ? 'alert-triangle' : 'info'}" class="h-5 w-5 flex-shrink-0 mt-0.5"></i>
            <div class="flex-1">${messageHtml}</div>
        </div>
    `;
    
    document.body.appendChild(notification);
    lucide.createIcons();
    
    // Animate in
    setTimeout(() => {
        notification.classList.remove('translate-x-full');
    }, 100);
    
    // Animate out and remove
    setTimeout(() => {
        notification.classList.add('translate-x-full');
        setTimeout(() => {
            document.body.removeChild(notification);
        }, 300);
    }, 5000);
}

// Keep existing functions for compatibility
function renderPosts() {
    const grid = document.getElementById('posts-grid');
    let filteredPosts = filterPostsByTab(posts, currentTab);
    
    // Применяем поиск если есть поисковый запрос
    if (currentSearchTerm) {
        filteredPosts = filteredPosts.filter(post => 
            (post.caption && post.caption.toLowerCase().includes(currentSearchTerm)) ||
            (post.account_username && post.account_username.toLowerCase().includes(currentSearchTerm))
        );
    }
    
    if (filteredPosts.length === 0) {
        grid.innerHTML = `
            <div class="col-span-full text-center py-12">
                <i data-lucide="image" class="h-12 w-12 text-slate-400 mx-auto mb-4"></i>
                <h3 class="text-lg font-medium text-white mb-2">Нет постов</h3>
                <p class="text-slate-400 mb-4">Создайте первый пост для начала работы</p>
                <button id="create-post-button" class="bg-blue-600 hover:bg-blue-700 px-4 py-2 rounded-md text-white transition-colors">
                    Создать пост
                </button>
            </div>
        `;
        lucide.createIcons();
        
        // Добавляем обработчик клика для кнопки создания поста
        const createButton = document.getElementById('create-post-button');
        if (createButton) {
            createButton.addEventListener('click', openPublishModal);
        }
        
        return;
    }

    // Группируем посты по batch_id для группового отображения
    const groupedPosts = {};
    const singlePosts = [];
    
    filteredPosts.forEach(post => {
        if (post.batch_id) {
            if (!groupedPosts[post.batch_id]) {
                groupedPosts[post.batch_id] = [];
            }
            groupedPosts[post.batch_id].push(post);
        } else {
            singlePosts.push(post);
        }
    });

    let html = '';
    
    // Отображаем групповые посты
    Object.entries(groupedPosts).forEach(([batchId, batchPosts]) => {
        const firstPost = batchPosts[0];
        const successCount = batchPosts.filter(p => p.status === 'COMPLETED' || p.status === 'published').length;
        const failedCount = batchPosts.filter(p => p.status === 'FAILED' || p.status === 'failed').length;
        
        html += `
            <div class="bg-slate-800/50 border border-slate-700 rounded-lg p-4 hover:bg-slate-800/70 transition-colors">
                <div class="flex items-center gap-4">
                    <!-- Маленькая превьюшка -->
                    <div class="flex-shrink-0 w-16 h-16 bg-slate-700 rounded-lg overflow-hidden">
                        ${firstPost.media_path ? 
                            `<img src="/media/${firstPost.media_path.split('/').pop()}" alt="Post" class="w-full h-full object-cover" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                            <div class="w-full h-full items-center justify-center hidden">
                                <i data-lucide="image" class="h-6 w-6 text-slate-400"></i>
                            </div>` :
                            `<div class="w-full h-full flex items-center justify-center">
                                <i data-lucide="image" class="h-6 w-6 text-slate-400"></i>
                            </div>`
                        }
                    </div>
                    
                    <!-- Информация о посте -->
                    <div class="flex-1 min-w-0">
                        <div class="flex items-center gap-3 mb-1">
                            <!-- Тип поста -->
                            <span class="inline-flex items-center gap-1 px-2 py-1 text-xs rounded-full bg-slate-700 text-slate-300">
                                ${getPostTypeIcon(firstPost.post_type)}
                                ${getPostTypeText(firstPost.post_type)}
                            </span>
                            
                            <!-- Статус группы -->
                            <span class="inline-flex items-center gap-1 px-2 py-1 text-xs rounded-full ${successCount === batchPosts.length ? 'bg-green-600/20 text-green-400' : failedCount > 0 ? 'bg-red-600/20 text-red-400' : 'bg-yellow-600/20 text-yellow-400'}">
                                <i data-lucide="users" class="h-3 w-3"></i>
                                ${successCount}/${batchPosts.length} успешно
                            </span>
                            
                            <!-- Дата -->
                            <span class="text-xs text-slate-400">
                                ${formatDate(firstPost.created_at)}
                            </span>
                        </div>
                        
                        <!-- Описание -->
                        <p class="text-sm text-slate-300 truncate mb-2">${firstPost.caption || 'Без описания'}</p>
                        
                        <!-- Выпадающий список аккаунтов -->
                        <details class="group">
                            <summary class="cursor-pointer text-xs text-slate-400 hover:text-slate-300 list-none flex items-center gap-2">
                                <i data-lucide="chevron-right" class="h-3 w-3 transition-transform group-open:rotate-90"></i>
                                Показать аккаунты (${batchPosts.length})
                            </summary>
                            <div class="mt-2 pl-5 space-y-1">
                                ${batchPosts.map(post => `
                                    <div class="flex items-center justify-between text-xs">
                                        <span class="text-slate-300">@${post.account_username}</span>
                                        <span class="inline-block px-2 py-0.5 rounded ${getStatusColor(post.status)} text-white">
                                            ${getStatusText(post.status)}
                                        </span>
                                    </div>
                                `).join('')}
                            </div>
                        </details>
                    </div>
                    
                    <!-- Кнопка удаления -->
                    <button onclick="deleteBatch('${batchId}')" class="flex-shrink-0 p-2 text-slate-400 hover:text-red-400 transition-colors">
                        <i data-lucide="trash-2" class="h-5 w-5"></i>
                    </button>
                </div>
            </div>
        `;
    });
    
    // Отображаем одиночные посты
    singlePosts.forEach(post => {
        html += `
            <div class="bg-slate-800/50 border border-slate-700 rounded-lg p-4 hover:bg-slate-800/70 transition-colors">
                <div class="flex items-center gap-4">
                    <!-- Маленькая превьюшка -->
                    <div class="flex-shrink-0 w-16 h-16 bg-slate-700 rounded-lg overflow-hidden">
                        ${post.media_path ? 
                            `<img src="/media/${post.media_path.split('/').pop()}" alt="Post" class="w-full h-full object-cover" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                            <div class="w-full h-full items-center justify-center hidden">
                                <i data-lucide="image" class="h-6 w-6 text-slate-400"></i>
                            </div>` :
                            `<div class="w-full h-full flex items-center justify-center">
                                <i data-lucide="image" class="h-6 w-6 text-slate-400"></i>
                            </div>`
                        }
                    </div>
                    
                    <!-- Информация о посте -->
                    <div class="flex-1 min-w-0">
                        <div class="flex items-center gap-3 mb-1">
                            <!-- Тип поста -->
                            <span class="inline-flex items-center gap-1 px-2 py-1 text-xs rounded-full bg-slate-700 text-slate-300">
                                ${getPostTypeIcon(post.post_type)}
                                ${getPostTypeText(post.post_type)}
                            </span>
                            
                            <!-- Статус -->
                            <span class="inline-block px-2 py-1 text-xs rounded-full ${getStatusColor(post.status)} text-white">
                                ${getStatusText(post.status)}
                            </span>
                            
                            <!-- Дата -->
                            <span class="text-xs text-slate-400">
                                ${formatDate(post.created_at)}
                            </span>
                        </div>
                        
                        <!-- Описание -->
                        <p class="text-sm text-slate-300 truncate mb-1">${post.caption || 'Без описания'}</p>
                        
                        <!-- Аккаунт -->
                        <p class="text-xs text-slate-400">
                            <i data-lucide="user" class="h-3 w-3 inline mr-1"></i>
                            @${post.account_username || 'Неизвестный аккаунт'}
                        </p>
                    </div>
                    
                    <!-- Кнопка удаления -->
                    <button onclick="event.stopPropagation(); deletePost(${post.id})" class="flex-shrink-0 p-2 text-slate-400 hover:text-red-400 transition-colors">
                        <i data-lucide="trash-2" class="h-5 w-5"></i>
                    </button>
                </div>
            </div>
        `;
    });
    
    // Изменяем контейнер на одну колонку для горизонтального отображения
    grid.className = 'space-y-3';
    grid.innerHTML = html;
    lucide.createIcons();
}

function filterPostsByTab(posts, tab) {
    switch (tab) {
        case 'scheduled':
            return posts.filter(post => post.status === 'SCHEDULED' || post.status === 'scheduled');
        case 'published':
            return posts.filter(post => post.status === 'COMPLETED' || post.status === 'published');
        case 'failed':
            return posts.filter(post => post.status === 'FAILED' || post.status === 'failed');
        default:
            return posts;
    }
}

function updateCounts() {
    const all = posts.length;
    const scheduled = posts.filter(p => p.status === 'SCHEDULED' || p.status === 'scheduled').length;
    const published = posts.filter(p => p.status === 'COMPLETED' || p.status === 'published').length;
    const failed = posts.filter(p => p.status === 'FAILED' || p.status === 'failed').length;
    
    document.getElementById('all-count').textContent = all;
    document.getElementById('scheduled-count').textContent = scheduled;
    document.getElementById('published-count').textContent = published;
    document.getElementById('failed-count').textContent = failed;
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
    
    renderPosts();
}

function getStatusColor(status) {
    switch (status) {
        case 'SCHEDULED':
        case 'scheduled': 
            return 'bg-blue-600';
        case 'COMPLETED':
        case 'published': 
            return 'bg-green-600';
        case 'FAILED':
        case 'failed': 
            return 'bg-red-600';
        case 'PROCESSING':
            return 'bg-yellow-600';
        case 'PENDING':
            return 'bg-slate-600';
        default: 
            return 'bg-slate-600';
    }
}

function getStatusText(status) {
    switch (status) {
        case 'SCHEDULED':
        case 'scheduled': 
            return 'Запланирован';
        case 'COMPLETED':
        case 'published': 
            return 'Опубликован';
        case 'FAILED':
        case 'failed': 
            return 'Ошибка';
        case 'PROCESSING':
            return 'Обрабатывается';
        case 'PENDING':
            return 'В очереди';
        default: 
            return 'Черновик';
    }
}

function getPostTypeIcon(type) {
    switch (type) {
        case 'photo':
        case 'feed':
            return '<i data-lucide="image" class="h-3 w-3"></i>';
        case 'carousel':
            return '<i data-lucide="layers" class="h-3 w-3"></i>';
        case 'reels':
        case 'reel':
        case 'video':
            return '<i data-lucide="video" class="h-3 w-3"></i>';
        case 'story':
            return '<i data-lucide="clock" class="h-3 w-3"></i>';
        default:
            return '<i data-lucide="file" class="h-3 w-3"></i>';
    }
}

function getPostTypeText(type) {
    switch (type) {
        case 'photo':
        case 'feed':
            return 'Лента';
        case 'carousel':
            return 'Карусель';
        case 'reels':
        case 'reel':
        case 'video':
            return 'Reels';
        case 'story':
            return 'История';
        default:
            return 'Пост';
    }
}

function formatDate(dateString) {
    if (!dateString) return '';
    const date = new Date(dateString);
    return date.toLocaleDateString('ru-RU', {
        day: '2-digit',
        month: '2-digit',
        year: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function editPost(postId) {
    alert(`Редактирование поста ${postId} (будет реализовано)`);
}

// Глобальная переменная для отслеживания удаляемых постов
const deletingPosts = new Set();

async function deletePost(postId) {
    // Проверяем, не удаляется ли уже этот пост
    if (deletingPosts.has(postId)) {
        console.log(`Post ${postId} is already being deleted`);
        return;
    }
    
    if (!confirm('Удалить этот пост?')) return;
    
    // Добавляем пост в список удаляемых
    deletingPosts.add(postId);
    
    // Показываем уведомление о начале процесса
    showNotification('🔄 Удаление поста...', 'info');
    
    try {
        const response = await api.deletePost(postId);
        
        // Показываем уведомление в зависимости от результата удаления из Instagram
        if (response.instagram_deleted === true) {
            showNotification('✅ Пост успешно удален из Instagram и базы данных', 'success');
        } else if (response.instagram_deleted === false && response.instagram_error) {
            showNotification(`⚠️ ${response.message}\n\nОшибка: ${response.instagram_error}`, 'warning');
        } else {
            showNotification(response.message || 'Пост удален из базы данных', 'success');
        }
        
        await loadPosts();
    } catch (error) {
        showNotification('Ошибка при удалении: ' + error.message, 'error');
    } finally {
        // Убираем пост из списка удаляемых
        deletingPosts.delete(postId);
    }
}

async function deleteBatch(batchId) {
    if (!confirm('Удалить все посты из этой группы?')) return;
    
    try {
        // Находим все посты с этим batch_id
        const batchPosts = posts.filter(p => p.batch_id === batchId);
        
        // Удаляем каждый пост
        for (const post of batchPosts) {
            await api.deletePost(post.id);
        }
        
        await loadPosts();
        showNotification(`Удалено ${batchPosts.length} постов`, 'success');
    } catch (error) {
        showNotification('Ошибка при удалении группы: ' + error.message, 'error');
    }
}

function showLoading() {
    document.getElementById('posts-grid').innerHTML = `
        <div class="col-span-full">
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                <div class="skeleton h-64 rounded-lg"></div>
                <div class="skeleton h-64 rounded-lg"></div>
                <div class="skeleton h-64 rounded-lg"></div>
            </div>
        </div>
    `;
}

function hideLoading() {
    // Loading will be replaced by renderPosts()
}

// Search functionality
document.getElementById('search-input').addEventListener('input', (e) => {
    const searchTerm = e.target.value.toLowerCase();
    
    // Сохраняем текущий поисковый запрос
    currentSearchTerm = searchTerm;
    
    // Перерисовываем посты с учетом поиска
    renderPosts();
});

// Setup event listeners
function setupEventListeners() {
    // Mobile-specific touch handlers for publication button
    setupMobilePublishButton();
    
    // Modal close on backdrop click
    const modal = document.getElementById('publish-modal');
    if (modal) {
        modal.addEventListener('click', function(e) {
            if (e.target === this) {
                closePublishModal();
            }
        });
    }
    
    // Keyboard shortcuts
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            closePublishModal();
        }
    });
    
    // Manual accounts input listener
    const manualAccountsInput = document.getElementById('manual-accounts');
    if (manualAccountsInput) {
        manualAccountsInput.addEventListener('input', updateSelectedAccountsDisplay);
    }
}

function setupMobilePublishButton() {
    const publishButton = document.getElementById('publish-button');
    if (publishButton) {
        console.log('Setting up mobile handlers for publish button');
        
        // Добавляем обработчики для touch событий на мобильных
        publishButton.addEventListener('touchstart', function(e) {
            console.log('Touch start on publish button');
            e.preventDefault();
            e.stopPropagation();
            this.style.transform = 'scale(0.98)';
            this.style.opacity = '0.9';
        }, { passive: false });
        
        publishButton.addEventListener('touchend', function(e) {
            console.log('Touch end on publish button - opening modal');
            e.preventDefault();
            e.stopPropagation();
            this.style.transform = '';
            this.style.opacity = '';
            // Вызываем функцию открытия модала с небольшой задержкой
            setTimeout(() => {
                openPublishModal();
            }, 50);
        }, { passive: false });
        
        // Обработчик клика для десктопа и резервный для мобильных
        publishButton.addEventListener('click', function(e) {
            console.log('Click on publish button');
            e.preventDefault();
            e.stopPropagation();
            openPublishModal();
        });
        
        // Добавляем дополнительный pointer-события обработчик
        publishButton.addEventListener('pointerup', function(e) {
            console.log('Pointer up on publish button');
            if (e.pointerType === 'touch') {
                e.preventDefault();
                e.stopPropagation();
                setTimeout(() => {
                    openPublishModal();
                }, 50);
            }
        });
    } else {
        console.log('Publish button not found!');
    }
}

function handleSearch(e) {
    const searchTerm = e.target.value.toLowerCase();
    const filtered = posts.filter(post => 
        (post.caption && post.caption.toLowerCase().includes(searchTerm)) ||
        (post.account_username && post.account_username.toLowerCase().includes(searchTerm))
    );
    
    const grid = document.getElementById('posts-grid');
    if (filtered.length === 0) {
        grid.innerHTML = '<div class="col-span-full text-center py-12 text-slate-400">Ничего не найдено</div>';
        return;
    }
    
    const html = filtered.map(post => `
        <div class="bg-slate-800/50 border border-slate-700 rounded-lg overflow-hidden card-hover">
            <div class="aspect-square bg-slate-700">
                ${post.image_url ? 
                    `<img src="${post.image_url}" alt="Post" class="w-full h-full object-cover">` :
                    `<div class="w-full h-full flex items-center justify-center">
                        <i data-lucide="image" class="h-12 w-12 text-slate-400"></i>
                    </div>`
                }
            </div>
            <div class="p-4">
                <div class="flex items-center justify-between mb-2">
                    <span class="inline-block px-2 py-1 text-xs rounded ${getStatusColor(post.status)} text-white">
                        ${getStatusText(post.status)}
                    </span>
                    <div class="flex items-center gap-2">
                        <button onclick="editPost(${post.id})" class="p-1 text-slate-400 hover:text-white transition-colors">
                            <i data-lucide="edit" class="h-4 w-4"></i>
                        </button>
                        <button onclick="event.stopPropagation(); deletePost(${post.id})" class="p-1 text-slate-400 hover:text-red-400 transition-colors">
                            <i data-lucide="trash-2" class="h-4 w-4"></i>
                        </button>
                    </div>
                </div>
                <p class="text-white text-sm mb-2 line-clamp-3">${post.caption || 'Без описания'}</p>
                <div class="flex items-center justify-between text-xs text-slate-400">
                    <span>${post.account_username ? '@' + post.account_username : 'Все аккаунты'}</span>
                    <span>${formatDate(post.scheduled_time || post.created_at)}</span>
                </div>
            </div>
        </div>
    `).join('');
    
    grid.innerHTML = html;
    lucide.createIcons();
}

// Legacy function names for compatibility
function openCreatePostModal() {
    openPublishModal();
}

function closeCreatePostModal() {
    closePublishModal();
}

// Новые функции для обновления лейблов слайдеров
function updateThreadsLabel() {
    const threadsSlider = document.getElementById('concurrent-threads');
    const threadsLabel = document.getElementById('threads-label');
    if (threadsSlider && threadsLabel) {
        const value = threadsSlider.value;
        threadsLabel.textContent = `${value} поток${value === '1' ? '' : value < 5 ? 'а' : 'ов'}`;
    }
}

function updateDelayLabel() {
    const delaySlider = document.getElementById('publish-delay');
    const delayLabel = document.getElementById('delay-label');
    if (delaySlider && delayLabel) {
        const value = delaySlider.value;
        if (value < 60) {
            delayLabel.textContent = `${value} сек`;
        } else {
            const minutes = Math.floor(value / 60);
            const seconds = value % 60;
            if (seconds === 0) {
                delayLabel.textContent = `${minutes} мин`;
            } else {
                delayLabel.textContent = `${minutes}:${seconds.toString().padStart(2, '0')} мин`;
            }
        }
    }
}

// Function to remove item from carousel
function removeCarouselItem(index) {
    uploadedFiles.splice(index, 1);
    
    // If no files left, hide the carousel preview
    if (uploadedFiles.length === 0) {
        document.getElementById('carousel-preview').classList.add('hidden');
        // Reset upload hint
        const uploadHint = document.getElementById('upload-hint');
        uploadHint.textContent = 'Изображения: JPG, PNG до 10MB (можно выбрать до 10 файлов)';
    } else {
        // Re-render the carousel preview
        handleCarouselUpload([]);
    }
}

function moveCarouselItem(fromIndex, toIndex) {
    // Remove item from original position
    const [movedItem] = uploadedFiles.splice(fromIndex, 1);
    // Insert at new position
    uploadedFiles.splice(toIndex, 0, movedItem);
    // Re-render the entire carousel to ensure DOM matches array order
    handleCarouselUpload([]);
}

function updateCarouselPreview() {
    // Update numbering and buttons without full re-render
    const items = document.querySelectorAll('#carouselFilesList > div');
    items.forEach((item, index) => {
        // Update data attributes
        item.dataset.index = index;
        item.dataset.position = index + 1;
        
        // Update position number
        const positionElement = item.querySelector('.carousel-position');
        if (positionElement) {
            positionElement.textContent = index + 1;
        }
        
        // Update navigation buttons
        const buttonsContainer = item.querySelector('.flex.gap-1');
        if (buttonsContainer) {
            buttonsContainer.innerHTML = `
                ${index > 0 ? `<button type="button" onclick="moveCarouselItem(${index}, ${index - 1})" class="bg-blue-600 hover:bg-blue-700 text-white rounded p-1" title="Переместить влево">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"></path>
                    </svg>
                </button>` : ''}
                ${index < uploadedFiles.length - 1 ? `<button type="button" onclick="moveCarouselItem(${index}, ${index + 1})" class="bg-blue-600 hover:bg-blue-700 text-white rounded p-1" title="Переместить вправо">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                    </svg>
                </button>` : ''}
            `;
        }
        
        // Update remove button
        const removeButton = item.querySelector('button[onclick^="removeCarouselItem"]');
        if (removeButton) {
            removeButton.setAttribute('onclick', `removeCarouselItem(${index})`);
        }
    });
}

// Drag and drop functionality
let draggedElement = null;
let draggedIndex = null;

function handleDragStart(e) {
    draggedElement = this;
    draggedIndex = parseInt(this.dataset.index);
    this.style.opacity = '0.5';
    this.classList.add('dragging');
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData('text/html', this.innerHTML);
}

function handleDragOver(e) {
    if (e.preventDefault) {
        e.preventDefault();
    }
    e.dataTransfer.dropEffect = 'move';
    return false;
}

function handleDrop(e) {
    if (e.stopPropagation) {
        e.stopPropagation();
    }
    
    e.preventDefault();
    
    const dropElement = e.target.closest('div[draggable="true"]');
    if (!dropElement) return false;
    
    const dropIndex = parseInt(dropElement.dataset.index);
    
    if (draggedIndex !== dropIndex && draggedIndex !== undefined && dropIndex !== undefined) {
        moveCarouselItem(draggedIndex, dropIndex);
    }
    
    return false;
}

function handleDragEnd(e) {
    this.style.opacity = '';
    this.classList.remove('dragging');
    
    // Clean up
    const items = document.querySelectorAll('#carouselFilesList > div');
    items.forEach(item => {
        item.classList.remove('drag-over');
        item.classList.remove('dragging');
    });
}

function getDragAfterElement(container, x) {
    const draggableElements = [...container.querySelectorAll('div[draggable="true"]:not(.dragging)')];
    
    return draggableElements.reduce((closest, child) => {
        const box = child.getBoundingClientRect();
        const offset = x - box.left - box.width / 2;
        
        if (offset < 0 && offset > closest.offset) {
            return { offset: offset, element: child };
        } else {
            return closest;
        }
    }, { offset: Number.NEGATIVE_INFINITY }).element;
} 