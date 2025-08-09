// Mobile Navigation and Responsive JavaScript

class MobileNavigation {
    constructor() {
        this.sidebar = null;
        this.overlay = null;
        this.hamburger = null;
        this.isOpen = false;
        
        this.init();
    }
    
    init() {
        this.createMobileElements();
        this.setupEventListeners();
        this.handleResize();
    }
    
    createMobileElements() {
        // Create mobile header if it doesn't exist
        if (!document.querySelector('.mobile-header')) {
            this.createMobileHeader();
        }
        
        // Add mobile classes to existing sidebar
        this.sidebar = document.querySelector('.w-64');
        if (this.sidebar) {
            this.sidebar.classList.add('sidebar');
        }
        
        // Create overlay
        this.createOverlay();
        
        // Update main content
        const mainContent = document.querySelector('.flex-1');
        if (mainContent) {
            mainContent.classList.add('main-content');
        }
    }
    
    createMobileHeader() {
        const header = document.createElement('div');
        header.className = 'mobile-header mobile-only hidden';
        header.innerHTML = `
            <div class="hamburger" id="mobile-hamburger">
                <span></span>
                <span></span>
                <span></span>
            </div>
            <div class="flex items-center gap-2">
                <div class="w-8 h-8 bg-gradient-to-r from-blue-500 to-purple-500 rounded-md flex items-center justify-center">
                    <span class="font-bold text-white">IA</span>
                </div>
                <h1 class="text-lg font-bold bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
                    InstaAutomation
                </h1>
            </div>
            <div></div>
        `;
        
        document.body.insertBefore(header, document.body.firstChild);
        this.hamburger = document.getElementById('mobile-hamburger');
    }
    
    createOverlay() {
        this.overlay = document.createElement('div');
        this.overlay.className = 'sidebar-overlay';
        document.body.appendChild(this.overlay);
    }
    
    setupEventListeners() {
        // Hamburger click
        if (this.hamburger) {
            this.hamburger.addEventListener('click', () => this.toggleSidebar());
        }
        
        // Overlay click
        if (this.overlay) {
            this.overlay.addEventListener('click', () => this.closeSidebar());
        }
        
        // Window resize
        window.addEventListener('resize', () => this.handleResize());
        
        // Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.closeSidebar();
            }
        });
        
        // Close sidebar when clicking on navigation links
        const navLinks = document.querySelectorAll('.sidebar a');
        navLinks.forEach(link => {
            link.addEventListener('click', () => {
                if (window.innerWidth <= 768) {
                    this.closeSidebar();
                }
            });
        });
    }
    
    toggleSidebar() {
        if (this.isOpen) {
            this.closeSidebar();
        } else {
            this.openSidebar();
        }
    }
    
    openSidebar() {
        if (this.sidebar) {
            this.sidebar.classList.add('open');
        }
        if (this.overlay) {
            this.overlay.classList.add('active');
        }
        if (this.hamburger) {
            this.hamburger.classList.add('active');
        }
        
        this.isOpen = true;
        document.body.style.overflow = 'hidden';
    }
    
    closeSidebar() {
        if (this.sidebar) {
            this.sidebar.classList.remove('open');
        }
        if (this.overlay) {
            this.overlay.classList.remove('active');
        }
        if (this.hamburger) {
            this.hamburger.classList.remove('active');
        }
        
        this.isOpen = false;
        document.body.style.overflow = '';
    }
    
    handleResize() {
        const isMobile = window.innerWidth <= 768;
        const mobileHeader = document.querySelector('.mobile-header');
        
        if (isMobile) {
            if (mobileHeader) {
                mobileHeader.classList.remove('hidden');
                mobileHeader.classList.add('mobile-flex');
            }
            
            // Close sidebar if open and switching to mobile
            if (this.isOpen) {
                this.closeSidebar();
            }
        } else {
            if (mobileHeader) {
                mobileHeader.classList.add('hidden');
                mobileHeader.classList.remove('mobile-flex');
            }
            
            // Reset sidebar state for desktop
            if (this.sidebar) {
                this.sidebar.classList.remove('open');
            }
            if (this.overlay) {
                this.overlay.classList.remove('active');
            }
            if (this.hamburger) {
                this.hamburger.classList.remove('active');
            }
            
            this.isOpen = false;
            document.body.style.overflow = '';
        }
    }
}

// Mobile Table Handler
class MobileTableHandler {
    constructor() {
        this.init();
    }
    
    init() {
        this.convertTablesToCards();
        window.addEventListener('resize', () => this.handleResize());
    }
    
    convertTablesToCards() {
        if (window.innerWidth <= 768) {
            const tables = document.querySelectorAll('table');
            tables.forEach(table => this.convertTableToCards(table));
        }
    }
    
    convertTableToCards(table) {
        if (table.dataset.mobileConverted) return;
        
        const headers = Array.from(table.querySelectorAll('th')).map(th => th.textContent.trim());
        const rows = Array.from(table.querySelectorAll('tbody tr'));
        
        const cardsContainer = document.createElement('div');
        cardsContainer.className = 'mobile-table-cards mobile-only hidden';
        
        rows.forEach(row => {
            const cells = Array.from(row.querySelectorAll('td'));
            const card = document.createElement('div');
            card.className = 'mobile-table-card';
            
            let cardContent = '';
            cells.forEach((cell, index) => {
                if (headers[index]) {
                    cardContent += `
                        <div class="flex justify-between items-center py-1">
                            <span class="text-slate-400 text-sm">${headers[index]}:</span>
                            <span class="text-white">${cell.innerHTML}</span>
                        </div>
                    `;
                }
            });
            
            card.innerHTML = cardContent;
            cardsContainer.appendChild(card);
        });
        
        table.parentNode.insertBefore(cardsContainer, table.nextSibling);
        table.dataset.mobileConverted = 'true';
        
        this.toggleTableDisplay();
    }
    
    toggleTableDisplay() {
        const isMobile = window.innerWidth <= 768;
        const tables = document.querySelectorAll('table[data-mobile-converted]');
        const cardContainers = document.querySelectorAll('.mobile-table-cards');
        
        tables.forEach(table => {
            if (isMobile) {
                table.style.display = 'none';
            } else {
                table.style.display = '';
            }
        });
        
        cardContainers.forEach(container => {
            if (isMobile) {
                container.classList.remove('hidden');
                container.classList.add('mobile-grid');
            } else {
                container.classList.add('hidden');
                container.classList.remove('mobile-grid');
            }
        });
    }
    
    handleResize() {
        this.toggleTableDisplay();
    }
}

// Mobile Form Handler
class MobileFormHandler {
    constructor() {
        this.init();
    }
    
    init() {
        this.addMobileClasses();
        this.setupFloatingActionButtons();
    }
    
    addMobileClasses() {
        // Add mobile classes to grids
        const grids = document.querySelectorAll('.grid');
        grids.forEach(grid => {
            if (grid.classList.contains('grid-cols-4')) {
                grid.classList.add('stats-grid');
            }
            if (grid.classList.contains('grid-cols-2')) {
                grid.classList.add('charts-grid');
            }
            if (grid.classList.contains('gap-4') || grid.classList.contains('gap-6')) {
                grid.classList.add('accounts-grid');
            }
        });
        
        // Add mobile classes to forms
        const forms = document.querySelectorAll('form .space-y-4, form .grid');
        forms.forEach(form => {
            form.classList.add('form-grid');
        });
        
        // Add mobile classes to button groups
        const buttonGroups = document.querySelectorAll('.flex.gap-2, .flex.gap-3');
        buttonGroups.forEach(group => {
            if (group.querySelectorAll('button').length > 1) {
                group.classList.add('action-buttons');
            }
        });
        
        // Add mobile classes to search and filters
        const searchContainers = document.querySelectorAll('.flex.gap-4');
        searchContainers.forEach(container => {
            if (container.querySelector('input[type="text"]')) {
                container.classList.add('search-filters');
            }
        });
    }
    
    setupFloatingActionButtons() {
        const primaryButtons = document.querySelectorAll('.bg-blue-600, .bg-green-600');
        primaryButtons.forEach(button => {
            if (button.textContent.includes('Добавить') || button.textContent.includes('Создать')) {
                this.createFloatingButton(button);
            }
        });
    }
    
    createFloatingButton(originalButton) {
        const fab = document.createElement('button');
        fab.className = 'floating-action-button mobile-only hidden';
        fab.innerHTML = '<i data-lucide="plus" class="h-6 w-6"></i>';
        fab.onclick = originalButton.onclick;
        
        document.body.appendChild(fab);
        
        // Hide original button on mobile
        originalButton.classList.add('mobile-hidden');
        
        // Initialize lucide icons
        if (window.lucide) {
            lucide.createIcons();
        }
    }
}

// Mobile Chart Handler
class MobileChartHandler {
    constructor() {
        this.init();
    }
    
    init() {
        this.addMobileClasses();
        this.handleChartResize();
        window.addEventListener('resize', () => this.handleChartResize());
    }
    
    addMobileClasses() {
        const chartContainers = document.querySelectorAll('canvas');
        chartContainers.forEach(canvas => {
            const container = canvas.closest('.bg-slate-800\\/50, .bg-slate-800');
            if (container) {
                container.classList.add('chart-container');
                const title = container.querySelector('h3');
                if (title) {
                    title.classList.add('chart-title');
                }
            }
        });
    }
    
    handleChartResize() {
        // Trigger chart resize if Chart.js is available
        if (window.Chart && window.Chart.instances) {
            Object.values(window.Chart.instances).forEach(chart => {
                chart.resize();
            });
        }
    }
}

// Touch Gesture Handler
class TouchGestureHandler {
    constructor() {
        this.startX = 0;
        this.startY = 0;
        this.init();
    }
    
    init() {
        this.setupSwipeGestures();
        this.setupTouchTargets();
    }
    
    setupSwipeGestures() {
        document.addEventListener('touchstart', (e) => {
            this.startX = e.touches[0].clientX;
            this.startY = e.touches[0].clientY;
        });
        
        document.addEventListener('touchend', (e) => {
            if (!this.startX || !this.startY) return;
            
            const endX = e.changedTouches[0].clientX;
            const endY = e.changedTouches[0].clientY;
            
            const diffX = this.startX - endX;
            const diffY = this.startY - endY;
            
            // Horizontal swipe
            if (Math.abs(diffX) > Math.abs(diffY) && Math.abs(diffX) > 50) {
                if (diffX > 0) {
                    // Swipe left - close sidebar
                    if (window.mobileNav && window.mobileNav.isOpen) {
                        window.mobileNav.closeSidebar();
                    }
                } else {
                    // Swipe right - open sidebar
                    if (window.mobileNav && !window.mobileNav.isOpen && this.startX < 50) {
                        window.mobileNav.openSidebar();
                    }
                }
            }
            
            this.startX = 0;
            this.startY = 0;
        });
    }
    
    setupTouchTargets() {
        const smallButtons = document.querySelectorAll('button');
        smallButtons.forEach(button => {
            if (!button.classList.contains('touch-target')) {
                button.classList.add('touch-target');
            }
        });
    }
}

// Initialize mobile functionality
document.addEventListener('DOMContentLoaded', () => {
    // Initialize mobile navigation
    window.mobileNav = new MobileNavigation();
    
    // Initialize other mobile handlers
    new MobileTableHandler();
    new MobileFormHandler();
    new MobileChartHandler();
    new TouchGestureHandler();
    
    // Add mobile utility classes
    document.body.classList.add('mobile-ready');
});

// Export for use in other files
window.MobileUtils = {
    isMobile: () => window.innerWidth <= 768,
    isTablet: () => window.innerWidth > 768 && window.innerWidth <= 1024,
    isDesktop: () => window.innerWidth > 1024,
    
    showMobileOnly: (element) => {
        element.classList.remove('hidden');
        element.classList.add('mobile-only');
    },
    
    hideMobileOnly: (element) => {
        element.classList.add('hidden');
        element.classList.remove('mobile-only');
    },
    
    toggleMobileClass: (element, className, condition = null) => {
        const shouldAdd = condition !== null ? condition : window.innerWidth <= 768;
        if (shouldAdd) {
            element.classList.add(className);
        } else {
            element.classList.remove(className);
        }
    }
}; 