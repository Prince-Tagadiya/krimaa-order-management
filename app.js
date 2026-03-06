// API_URL is now in config.js as SHEETS_API_URL

// ===== Multi-User Auth System =====
const USERS = [
    { username: 'Krimaa', password: 'Krimaa4484', role: 'admin', displayName: 'Admin' },
    { username: 'Krimaa_Users', password: 'Krimaa123', role: 'order', displayName: 'Order Entry' }
];

const AppState = {
    accounts: [],
    accountDetails: [],
    dashboardData: [],
    company1Accounts: [],
    company1Details: [],
    company2Accounts: [],
    company2Details: [],
    company1Data: [],
    company2Data: [],
    availableSheetMonths: [],
    sheetArchiveCache: {},
    moneyBackups: [],
    selectedMoneyBackupId: '',
    karigars: [],
    karigarTransactions: [],
    designPrices: {},
    currentSection: 'data-sheet',
    currentCompany: 'company1',
    currentUser: null, // { username, role, displayName }
    heatmapMonth: new Date().getMonth(),
    heatmapYear: new Date().getFullYear(),
    isSwitchingCompany: false
};

function getTodayISODate() {
    const now = new Date();
    return new Date(now.getTime() - (now.getTimezoneOffset() * 60000)).toISOString().split('T')[0];
}

function normalizeToISODate(rawDate) {
    if (!rawDate) return '';
    if (rawDate instanceof Date && !isNaN(rawDate.getTime())) {
        return new Date(rawDate.getTime() - (rawDate.getTimezoneOffset() * 60000)).toISOString().split('T')[0];
    }

    const str = String(rawDate).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;

    // Supports dd/mm/yyyy (preferred) and mm/dd/yyyy when day is clearly >12.
    const slashMatch = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (slashMatch) {
        const p1 = parseInt(slashMatch[1], 10);
        const p2 = parseInt(slashMatch[2], 10);
        const year = slashMatch[3];
        let day = p1;
        let month = p2;
        if (p1 <= 12 && p2 > 12) {
            day = p2;
            month = p1;
        }
        return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    }

    const parsed = new Date(str);
    if (!isNaN(parsed.getTime())) {
        return new Date(parsed.getTime() - (parsed.getTimezoneOffset() * 60000)).toISOString().split('T')[0];
    }
    return '';
}

function diffDays(fromISO, toISO) {
    if (!fromISO || !toISO) return -1;
    const fromDate = new Date(`${fromISO}T00:00:00`);
    const toDate = new Date(`${toISO}T00:00:00`);
    if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) return -1;
    return Math.max(0, Math.floor((toDate - fromDate) / (1000 * 60 * 60 * 24)));
}

function formatISODateForDisplay(rawDate) {
    const iso = normalizeToISODate(rawDate);
    if (!iso) return '';
    const [y, m, d] = iso.split('-');
    return `${d}/${m}/${y}`;
}

function getRelativeDayText(fromISO) {
    const days = diffDays(fromISO, getTodayISODate());
    if (days === -1) return 'Unknown';
    if (days === 0) return 'Today';
    return `${days} day${days > 1 ? 's' : ''} ago`;
}

function getRechargeText(rawDate) {
    const iso = normalizeToISODate(rawDate);
    if (!iso) return 'Not added';
    return `${formatISODateForDisplay(iso)} (${getRelativeDayText(iso)})`;
}

function getCompanyDisplayName(companyId = AppState.currentCompany) {
    const btn = document.querySelector(`.company-btn[data-company="${companyId}"]`);
    if (btn && btn.textContent) return btn.textContent.trim();
    if (companyId === 'company1') return 'Company 1';
    if (companyId === 'company2') return 'Company 2';
    return companyId;
}

function setCompanyButtonsDisabled(disabled) {
    document.querySelectorAll('.company-btn').forEach(btn => {
        btn.disabled = disabled;
        btn.classList.toggle('is-disabled', disabled);
    });
}

function setOrderDateDefaults(force = false) {
    const orderDateInput = document.getElementById('order-date');
    if (!orderDateInput) return;
    if (force || !orderDateInput.value) {
        orderDateInput.value = getTodayISODate();
    }
    updateOrderDateLabel();
}

function updateOrderCompanyLabel() {
    const label = document.getElementById('order-company-label');
    if (!label) return;
    label.textContent = `Selected Company: ${getCompanyDisplayName()}`;
}

function showProgressToast(message) {
    const toast = document.getElementById('toast');
    if (!toast) return;
    if (toast._timer) clearTimeout(toast._timer);
    toast.style.background = "#3b82f6";
    toast.style.color = "white";
    toast.style.border = "none";
    toast.style.boxShadow = "0 10px 15px -3px rgba(0,0,0,0.1)";
    toast.innerHTML = `<i class='bx bx-loader-alt bx-spin'></i> <span>${message}</span>`;
    toast.classList.remove('hidden');
}

function changeHeatmapMonth(offset) {
    if (!Number.isFinite(offset) || offset === 0) return;
    let month = AppState.heatmapMonth + offset;
    let year = AppState.heatmapYear;
    while (month < 0) { month += 12; year--; }
    while (month > 11) { month -= 12; year++; }
    AppState.heatmapMonth = month;
    AppState.heatmapYear = year;
    renderCalendarHeatmap();
}

// ===== Account Position Management =====
function getAccountPositions() {
    try {
        const saved = localStorage.getItem(`accountPositions_${AppState.currentCompany}`);
        return saved ? JSON.parse(saved) : {};
    } catch(e) { return {}; }
}

function saveAccountPositions(orderedAccounts) {
    const positions = {};
    orderedAccounts.forEach((acc, idx) => { positions[acc] = idx; });
    localStorage.setItem(`accountPositions_${AppState.currentCompany}`, JSON.stringify(positions));
}

function getSortedAccounts() {
    return [...AppState.accounts];
}

document.addEventListener('DOMContentLoaded', () => { initApp(); });

function initApp() {
    checkAuth();
    attachEventListeners();
    initSyncIndicator();

    // Backup button
    const backupBtn = document.getElementById('backup-btn');
    if (backupBtn) backupBtn.addEventListener('click', backupToSheets);

    const today = getTodayISODate();
    setOrderDateDefaults(true);
    
    const savedCompany = localStorage.getItem('selectedCompany');
    if (savedCompany) {
        AppState.currentCompany = savedCompany;
        document.querySelectorAll('.company-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.company === savedCompany);
        });
    }
    updateOrderCompanyLabel();
    
    // Dashboard filter listeners
    document.getElementById('dash-filter-type').addEventListener('change', (e) => {
        const val = e.target.value;
        const dateRange = document.getElementById('custom-date-range');
        if (val === 'custom_date') {
            dateRange.classList.remove('hidden');
            const fromInput = document.getElementById('dash-filter-from');
            const toInput = document.getElementById('dash-filter-to');
            if (!fromInput.value) fromInput.value = today;
            if (!toInput.value) toInput.value = today;
        } else {
            dateRange.classList.add('hidden');
        }
        renderDashboard();
    });
    ['dash-filter-from', 'dash-filter-to'].forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        el.addEventListener('input', renderDashboard);
        el.addEventListener('change', renderDashboard);
    });

    // Company Switcher
    document.querySelectorAll('.company-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const companyId = btn.dataset.company;
            if (companyId === AppState.currentCompany || AppState.isSwitchingCompany) return;
            const previousCompany = AppState.currentCompany;
            document.querySelectorAll('.company-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            AppState.currentCompany = companyId;
            localStorage.setItem('selectedCompany', companyId);
            switchCompany(previousCompany);
        });
    });

    // Heatmap navigation
    const heatmapPrevBtn = document.getElementById('heatmap-prev');
    const heatmapNextBtn = document.getElementById('heatmap-next');
    if (heatmapPrevBtn) {
        heatmapPrevBtn.addEventListener('click', (e) => {
            e.preventDefault();
            changeHeatmapMonth(-1);
        });
    }
    if (heatmapNextBtn) {
        heatmapNextBtn.addEventListener('click', (e) => {
            e.preventDefault();
            changeHeatmapMonth(1);
        });
    }

    // Export CSV
    if (document.getElementById('export-csv-btn')) {
        document.getElementById('export-csv-btn').addEventListener('click', exportToCSV);
    }
}

async function switchCompany(previousCompany = '') {
    if (AppState.isSwitchingCompany) return;
    AppState.isSwitchingCompany = true;
    const companyName = getCompanyDisplayName(AppState.currentCompany);
    setCompanyButtonsDisabled(true);
    showProgressToast(`Switching to ${companyName}...`);

    try {
        // Force flush before switching (just to be safe although firebase queues these up beautifully)
        if (typeof FirebaseService !== 'undefined') {
            await FirebaseService.flushWrites();
        }
        
        AppState.accounts = [];
        AppState.dashboardData = [];
        await Promise.all([fetchAccounts(), fetchDashboardData()]);
        
        // Don't block company switch for slow Excel meta-data fetch
        // Just refresh the dashboard data which is now only Firebase current month
        fetchAllCompaniesData({ refreshArchiveMonths: false }).catch(e => {
            console.warn('Silent update of all-company dashboard data', e);
        });
        
        if (AppState.currentSection === 'dashboard') renderDashboard();
        else if (AppState.currentSection === 'daily-order') {
            setOrderDateDefaults();
            renderOrderEntryTable();
            checkExistingOrdersForDate();
        }
        else if (AppState.currentSection === 'add-account') renderAccountsList();
        else if (AppState.currentSection === 'money-management') renderMoneyManagement();
        else if (AppState.currentSection === 'money-backup') renderMoneyBackupPage();
        else if (AppState.currentSection === 'data-sheet') { 
            // Also switch data sheet filter and force re-render
            const cmFilter = document.getElementById('sheet-company-filter');
            if (cmFilter) cmFilter.value = AppState.currentCompany;
            populateSheetMonthFilter(); 
            renderDataSheet(); 
        }
        updateOrderCompanyLabel();
        
        showToast(`Switched to ${companyName} successfully!`, "success");
    } catch (err) { 
        if (previousCompany && previousCompany !== AppState.currentCompany) {
            AppState.currentCompany = previousCompany;
            localStorage.setItem('selectedCompany', previousCompany);
            document.querySelectorAll('.company-btn').forEach(btn => {
                btn.classList.toggle('active', btn.dataset.company === previousCompany);
            });
        }
        updateOrderCompanyLabel();
        showToast("Error loading company data", "error"); 
    } finally {
        AppState.isSwitchingCompany = false;
        setCompanyButtonsDisabled(false);
    }
}

// ===== AUTH =====
function checkAuth() {
    const isLogged = localStorage.getItem('isLogged');
    const savedRole = localStorage.getItem('userRole');
    const savedName = localStorage.getItem('userName');
    
    if (isLogged && savedRole) {
        AppState.currentUser = { role: savedRole, displayName: savedName || savedRole };
        document.getElementById('login-screen').classList.add('hidden');
        document.getElementById('app-screen').classList.remove('hidden');
        applyRolePermissions();
        loadInitialData();

    } else {
        document.getElementById('login-screen').classList.remove('hidden');
        document.getElementById('app-screen').classList.add('hidden');
    }
}

function applyRolePermissions() {
    const role = AppState.currentUser?.role;
    const userInfo = document.getElementById('sidebar-user-info');
    
    // Show user info in sidebar - Simplified as requested (Removed avatar with "A")
    userInfo.innerHTML = `<span>${AppState.currentUser.displayName}</span>`;
    
    if (role === 'order') {
        // Hide Dashboard, Data Sheet and Manage Accounts for order role
        document.getElementById('nav-dashboard').style.display = 'none';
        document.getElementById('nav-data-sheet').style.display = 'none';
        document.getElementById('nav-manage-accounts').style.display = 'none';
        document.getElementById('nav-money-management').style.display = 'none';
    } else {
        document.getElementById('nav-dashboard').style.display = '';
        document.getElementById('nav-data-sheet').style.display = '';
        document.getElementById('nav-manage-accounts').style.display = '';
        document.getElementById('nav-money-management').style.display = '';
    }
}

function attachEventListeners() {
    // Login
    document.getElementById('login-form').addEventListener('submit', (e) => {
        e.preventDefault();
        const user = document.getElementById('username').value;
        const pass = document.getElementById('password').value;
        
        const foundUser = USERS.find(u => u.username === user && u.password === pass);
        if (foundUser) {
            localStorage.setItem('isLogged', 'true');
            localStorage.setItem('userRole', foundUser.role);
            localStorage.setItem('userName', foundUser.displayName);
            AppState.currentUser = { role: foundUser.role, displayName: foundUser.displayName };
            checkAuth();
            showToast(`Welcome, ${foundUser.displayName}!`, "success");
        } else {
            showToast("Invalid Credentials", "error");
        }
    });

    // Navigation
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            if (btn.id === 'logout-btn') {
                e.preventDefault();
                localStorage.removeItem('isLogged');
                localStorage.removeItem('userRole');
                localStorage.removeItem('userName');
                AppState.currentUser = null;
                checkAuth();
                return;
            }
            e.preventDefault();
            const target = btn.getAttribute('data-target');
            
            // Check permissions
            if (target === 'add-account' && AppState.currentUser?.role === 'order') {
                showToast("You don't have permission to manage accounts", "error");
                return;
            }
            
            navigateTo(target);
            const sidebar = document.getElementById('sidebar');
            if(window.innerWidth <= 768 && sidebar.classList.contains('open')) sidebar.classList.remove('open');
        });
    });

    document.getElementById('menu-toggle').addEventListener('click', () => {
        document.getElementById('sidebar').classList.toggle('open');
    });

    // Add Account
    document.getElementById('add-account-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const accountName = document.getElementById('new-account-name').value.trim();
        const mobile = document.getElementById('new-account-mobile')?.value.trim();
        const rechargeDate = document.getElementById('new-account-recharge')?.value;
        if (!accountName) return;
        const btn = document.getElementById('add-account-btn');
        btn.disabled = true;
        document.getElementById('new-account-name').value = '';
        if (document.getElementById('new-account-mobile')) document.getElementById('new-account-mobile').value = '';
        if (document.getElementById('new-account-recharge')) document.getElementById('new-account-recharge').value = '';
        
        const container = document.getElementById('active-account-list');
        const tempId = 'temp-' + Date.now();
        const tempHtml = `<div id="${tempId}" class="account-item" style="opacity: 0.6; border: 1px dashed var(--primary);"><i class='bx bx-loader-alt bx-spin'></i><span class="account-name">${accountName} <small class="text-muted">(Syncing...)</small></span></div>`;
        if (container.querySelector('p')) container.innerHTML = '';
        container.insertAdjacentHTML('afterbegin', tempHtml);
        try {
            const res = await apiRequest({ action: 'addAccount', accountName, mobile, gstin: '', rechargeDate, companyId: AppState.currentCompany });
            if (res.success) { showToast("Account saved!", "success"); await fetchAccounts(); await fetchAllCompaniesData(); renderAccountsList(); }
            else { showToast(res.message, "error"); document.getElementById(tempId).remove(); }
        } catch (err) { showToast("Network Error!", "error"); document.getElementById(tempId).remove(); }
        finally { btn.disabled = false; }
    });

    // Submit Orders
    document.getElementById('submit-orders-btn').addEventListener('click', async () => {
        const date = document.getElementById('order-date').value;
        if(!date) return showToast("Please select a date", "error");
        const orders = [];
        let hasData = false;
        document.querySelectorAll('.order-row').forEach(row => {
            const accountName = row.dataset.account;
            const meesho = parseInt(row.querySelector('.inp-meesho').value) || 0;
            if (meesho > 0) hasData = true;
            orders.push({ accountName, meesho, total: meesho });
        });
        if (!hasData) return showToast("Please enter at least one order value!", "error");
        const btn = document.getElementById('submit-orders-btn');
        btn.disabled = true; btn.textContent = "Submitting..."; showLoader();
        try {
            await apiRequest({ action: 'submitOrders', date, orders, companyId: AppState.currentCompany });
            showToast("Orders saved!", "success");
            document.querySelectorAll('.order-row input').forEach(inp => inp.value = '');
            document.querySelectorAll('.row-total').forEach(tot => tot.textContent = '0');
            calculateGrandTotals();
            await fetchDashboardData();
            try { await fetchAllCompaniesData(); } catch (e) { console.warn('Non-blocking: failed to refresh all-company data after submit', e); }
            // Order role stays on daily-order, admin goes to dashboard
            if (AppState.currentUser?.role === 'order') {
                showToast('Orders saved! You can enter more orders.', 'success');
            } else {
                navigateTo('dashboard');
            }
        } catch (err) { showToast("Network Error!", "error"); }
        finally { btn.disabled = false; btn.textContent = "Submit All Orders"; hideLoader(); }
    });

    document.getElementById('order-date').addEventListener('change', () => { checkExistingOrdersForDate(); updateOrderDateLabel(); });
    document.getElementById('already-submitted-alert').addEventListener('click', () => { document.getElementById('order-details-modal').classList.add('show'); });

    // Modal closes
    document.getElementById('close-modal-btn').addEventListener('click', () => document.getElementById('order-details-modal').classList.remove('show'));
    document.getElementById('order-details-modal').addEventListener('click', (e) => { if(e.target.id === 'order-details-modal') e.target.classList.remove('show'); });
    
    document.getElementById('close-edit-modal-btn').addEventListener('click', () => document.getElementById('edit-account-modal').classList.remove('show'));
    document.getElementById('edit-account-modal').addEventListener('click', (e) => { if(e.target.id === 'edit-account-modal') e.target.classList.remove('show'); });
    document.getElementById('edit-account-recharge')?.addEventListener('change', updateEditRechargeMeta);
    document.getElementById('edit-account-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const oldName = document.getElementById('edit-account-old-name').value;
        const newName = document.getElementById('edit-account-name').value.trim();
        const mobile = document.getElementById('edit-account-mobile')?.value.trim();
        const rechargeDate = document.getElementById('edit-account-recharge')?.value;
        if (!newName) return;
        showLoader();
        try {
            const res = await apiRequest({ action: 'editAccount', oldName, newName, mobile, gstin: '', rechargeDate, companyId: AppState.currentCompany });
            if (res.success) {
                showToast("Account updated!", "success");
                document.getElementById('edit-account-modal').classList.remove('show');
                await fetchAccounts();
                renderAccountsList();
                await fetchDashboardData();
                try { await fetchAllCompaniesData(); } catch (e) { console.warn('Non-blocking: failed to refresh all-company data after edit', e); }
            }
            else showToast(res.message || "Failed to update", "error");
        } catch(err) { showToast("Network Error!", "error"); }
        finally { hideLoader(); }
    });

    const closeDeleteModalBtn = document.getElementById('close-delete-modal-btn');
    if (closeDeleteModalBtn) {
        closeDeleteModalBtn.addEventListener('click', () => document.getElementById('delete-account-modal').classList.remove('show'));
    }
    document.getElementById('delete-account-modal').addEventListener('click', (e) => { if(e.target.id === 'delete-account-modal') e.target.classList.remove('show'); });
    document.getElementById('cancel-delete-btn').addEventListener('click', () => document.getElementById('delete-account-modal').classList.remove('show'));
    document.getElementById('confirm-delete-btn').addEventListener('click', async () => {
        const accountName = document.getElementById('delete-account-name').value;
        showLoader();
        try {
            const res = await apiRequest({ action: 'deleteAccount', accountName, companyId: AppState.currentCompany });
            if (res.success) { showToast("Account deleted!", "success"); document.getElementById('delete-account-modal').classList.remove('show'); await fetchAccounts(); renderAccountsList(); }
            else showToast(res.message || "Failed to delete", "error");
        } catch(err) { showToast("Network Error!", "error"); }
        finally { hideLoader(); }
    });
}

function navigateTo(sectionId) {
    
    // Permission check for order role
    if (AppState.currentUser?.role === 'order' && (sectionId === 'add-account' || sectionId === 'dashboard' || sectionId === 'data-sheet' || sectionId === 'money-management' || sectionId === 'money-backup')) {
        showToast("Access denied", "error"); return;
    }
    
    AppState.currentSection = sectionId;
    document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
    const activeBtn = document.querySelector(`[data-target="${sectionId}"]`);
    if(activeBtn) activeBtn.classList.add('active');
    
    const titles = {
        'dashboard': 'Dashboard',
        'daily-order': 'Daily Order Entry',
        'add-account': 'Manage Accounts',
        'money-management': 'Money Management',
        'money-backup': 'Money Backup History',
        'data-sheet': 'Data Sheet',
        'karigar': 'Karigar Management'
    };
    
    // Update global title if it exists, and section-specific title
    const globalTitle = document.getElementById('page-title');
    if (globalTitle) globalTitle.textContent = titles[sectionId];
    
    document.querySelectorAll('.content-section').forEach(sec => sec.classList.remove('active'));
    const targetSec = document.getElementById(sectionId);
    if (targetSec) {
        targetSec.classList.add('active');
        const secTitle = targetSec.querySelector('.page-title');
        if (secTitle) secTitle.textContent = titles[sectionId];
    }

    if (sectionId === 'daily-order') {
        setOrderDateDefaults();
        updateOrderCompanyLabel();
        renderOrderEntryTable();
        checkExistingOrdersForDate();
    }
    else if (sectionId === 'dashboard') renderDashboard();
    else if (sectionId === 'add-account') renderAccountsList();
    else if (sectionId === 'money-management') renderMoneyManagement();
    else if (sectionId === 'money-backup') renderMoneyBackupPage();
    else if (sectionId === 'karigar') renderKarigarPage();
    else if (sectionId === 'data-sheet') { 
        document.getElementById('sheet-company-filter').value = AppState.currentCompany;
        loadAvailableSheetMonths().then(() => populateSheetMonthFilter());
        renderDataSheet(); 
    }
}

async function ensureLegacyOrdersMigrated() {
    if (localStorage.getItem('legacy_migrated_v2')) return;
    try {
        showProgressToast('Optimizing older database records...');
        let migratedCount = 0;
        let batch;
        do {
            batch = await FirebaseService.migrateLegacyOrdersToDailyOrders();
            if (batch?.count) migratedCount += batch.count;
        } while (batch && batch.count === 500); // 500 is the limit on our migration function
        localStorage.setItem('legacy_migrated_v2', 'true');
        if (migratedCount > 0) {
            showToast(`Successfully optimized ${migratedCount} database records!`, 'success');
        }
    } catch(e) {
        console.error('Migration failed:', e);
    }
}

async function autoFixMismatchedAccountNames() {
    if (localStorage.getItem('mismatched_accounts_fixed_v1')) return;
    try {
        console.log("Checking for mismatched account names in historical daily_orders...");
        let changed = false;
        
        const accountsC1Snap = await FirebaseService.db.collection('accounts').where('companyId', '==', 'company1').get();
        const accountsC2Snap = await FirebaseService.db.collection('accounts').where('companyId', '==', 'company2').get();
        
        const accountsC1 = [];
        const accountsC2 = [];
        accountsC1Snap.forEach(d => accountsC1.push(d.data().name));
        accountsC2Snap.forEach(d => accountsC2.push(d.data().name));
        
        const ordSnap = await FirebaseService.db.collection('daily_orders').get();
        const ops = [];
        
        ordSnap.forEach(doc => {
            const data = doc.data();
            const companyId = data.companyId;
            const validAccounts = companyId === 'company1' ? accountsC1 : accountsC2;
            
            let docChanged = false;
            const newAccs = [...(data.accounts || [])];
            
            for(let i=0; i<newAccs.length; i++) {
                const oldName = newAccs[i];
                if (!validAccounts.includes(oldName)) {
                    // Try to find a fuzzy match inside validAccounts
                    const fuzzyMatch = validAccounts.find(v => oldName.toLowerCase().startsWith(v.toLowerCase()) || v.toLowerCase().startsWith(oldName.toLowerCase()) || (oldName.length > 5 && v.length > 5 && oldName.substring(0, 5).toLowerCase() === v.substring(0, 5).toLowerCase()));
                    if (fuzzyMatch && fuzzyMatch !== oldName) {
                        console.log(`Auto-fixing mismatch in ${companyId}: "${oldName}" -> "${fuzzyMatch}"`);
                        newAccs[i] = fuzzyMatch;
                        docChanged = true;
                    }
                }
            }
            if (docChanged) {
                ops.push(b => b.update(doc.ref, { accounts: newAccs }));
                changed = true;
            }
        });
        
        if (changed) {
            await FirebaseService.commitInChunks(ops);
            console.log("Mismatched accounts auto-fixed!");
        }
        localStorage.setItem('mismatched_accounts_fixed_v1', 'true');
    } catch(e) {
        console.error("Auto-fix failed:", e);
    }
}

async function loadInitialData() {
    showLoader();
    try {
        console.log("🚀 [INITIALIZATION] Starting web app...");
        FirebaseService.init();
        
        console.log("🚀 [INITIALIZATION] Checking legacy migration and seeds...");
        // One-time migration: Sheets → Firebase
        await ensureFirebaseSeeded();
        await ensureLegacyOrdersMigrated();
        await autoFixMismatchedAccountNames();
        
        console.log("🚀 [INITIALIZATION] Fetching fresh data instantly from Firestore...");
        
        // Wait for all actual Firebase data to load first, avoiding empty flash entirely!
        await Promise.all([
            fetchAccounts(),
            fetchAllCompaniesData({ refreshArchiveMonths: false })
        ]);
        
        console.log("🚀 [INITIALIZATION] Local data ready. Booting UI in background...");
        // Load the sheets metadata seamlessly in background
        loadAvailableSheetMonths(true).catch(e => console.warn('Background meta fetch:', e));
        
        // Update backup time in UI (now isolated from auto-trigger)
        checkAutoBackup();
        
        console.log("🚀 [INITIALIZATION] Finished completely. Routing to UI.");
        if (AppState.currentUser?.role === 'order') {
            navigateTo('daily-order');
        } else {
            navigateTo('data-sheet');
        }
    } catch (err) { console.error(err); showToast("Error loading data: " + err.message, "error"); }
    finally { hideLoader(); }
}

async function fetchAccounts() {
    try {
        const res = await apiRequest({ action: 'getAccounts', companyId: AppState.currentCompany });
        if (res && res.success === false) throw new Error(res.message || 'Unable to fetch accounts');
        if (Array.isArray(res?.data)) AppState.accounts = res.data;
        else if (Array.isArray(res)) AppState.accounts = res; // compatibility with legacy shape
        else AppState.accounts = [];
        
        if (res?.details) AppState.accountDetails = res.details;
    } catch(e) {
        console.error(e);
        throw e;
    }
}

function getSheetsCache(companyId) {
    try {
        const cached = localStorage.getItem(`sheetsCache_${companyId}`);
        if (cached) return JSON.parse(cached);
    } catch(e) {}
    return [];
}

function setSheetsCache(companyId, data) {
    try {
        localStorage.setItem(`sheetsCache_${companyId}`, JSON.stringify(data));
    } catch(e) {}
}

function mergeOrders(historical, recent) {
    const map = new Map();
    (historical||[]).forEach(r => map.set(`${r.date}_${r.accountName}`, r));
    (recent||[]).forEach(r => map.set(`${r.date}_${r.accountName}`, r));
    return Array.from(map.values()).sort((a,b) => (b.date > a.date ? 1 : -1));
}

let _isSyncingBg = false;
function showBackgroundLoader() {
    const loader = document.getElementById('background-sheets-loader');
    if (loader) loader.classList.remove('hidden');
}
function hideBackgroundLoader() {
    const loader = document.getElementById('background-sheets-loader');
    if (loader) loader.classList.add('hidden');
}

async function backgroundSheetsSync() {
    if (_isSyncingBg) return;
    showBackgroundLoader();
    _isSyncingBg = true;
    try {
        const [res1, res2] = await Promise.all([
            sheetsApiRequest({ action: 'getDashboardData', companyId: 'company1' }).catch(()=>null),
            sheetsApiRequest({ action: 'getDashboardData', companyId: 'company2' }).catch(()=>null)
        ]);
        
        let shouldRender = false;

        if (res1 && Array.isArray(res1.data)) {
            setSheetsCache('company1', res1.data);
            shouldRender = true;
        }
        if (res2 && Array.isArray(res2.data)) {
            setSheetsCache('company2', res2.data);
            shouldRender = true;
        }
        
        if (shouldRender) {
            const f1 = await apiRequest({ action: 'getDashboardData', companyId: 'company1' }).catch(()=>null);
            const f2 = await apiRequest({ action: 'getDashboardData', companyId: 'company2' }).catch(()=>null);
            
            AppState.company1Data = mergeOrders(getSheetsCache('company1'), f1?.data || []);
            AppState.company2Data = mergeOrders(getSheetsCache('company2'), f2?.data || []);
            
            AppState.dashboardData = AppState.currentCompany === 'company1' ? AppState.company1Data : AppState.company2Data;
            
            if (AppState.currentSection === 'dashboard') renderDashboard();
            else if (AppState.currentSection === 'data-sheet') renderDataSheet();
        }
    } catch (e) {
        console.warn('Background sync failed', e);
    } finally {
        _isSyncingBg = false;
        hideBackgroundLoader();
    }
}

async function fetchDashboardData() {
    try {
        const res = await apiRequest({ action: 'getDashboardData', companyId: AppState.currentCompany });
        const fbData = Array.isArray(res?.data) ? res.data : [];
        const cached = getSheetsCache(AppState.currentCompany);
        
        AppState.dashboardData = mergeOrders(cached, fbData);
        if (AppState.currentCompany === 'company1') AppState.company1Data = AppState.dashboardData;
        if (AppState.currentCompany === 'company2') AppState.company2Data = AppState.dashboardData;
    } catch(e) {
        console.error(e);
        throw e;
    }
}

async function fetchAllCompaniesData(options = {}) {
    const refreshArchiveMonths = !!options.refreshArchiveMonths;
    const toList = (res) => {
        if (!res || res.success === false) return null;
        if (Array.isArray(res?.data)) return res.data;
        if (Array.isArray(res)) return res;
        return [];
    };

    const [c1Acc, c2Acc, f1, f2] = await Promise.all([
        apiRequest({ action: 'getAccounts', companyId: 'company1' }).catch(() => null),
        apiRequest({ action: 'getAccounts', companyId: 'company2' }).catch(() => null),
        apiRequest({ action: 'getDashboardData', companyId: 'company1' }).catch(() => null),
        apiRequest({ action: 'getDashboardData', companyId: 'company2' }).catch(() => null)
    ]);

    const c1Accounts = toList(c1Acc);
    const c2Accounts = toList(c2Acc);
    const fData1 = toList(f1) || [];
    const fData2 = toList(f2) || [];

    if (c1Accounts !== null) AppState.company1Accounts = c1Accounts;
    if (c2Accounts !== null) AppState.company2Accounts = c2Accounts;
    if (c1Acc && c1Acc.details) AppState.company1Details = c1Acc.details;
    if (c2Acc && c2Acc.details) AppState.company2Details = c2Acc.details;

    AppState.company1Data = mergeOrders(getSheetsCache('company1'), fData1);
    AppState.company2Data = mergeOrders(getSheetsCache('company2'), fData2);

    // Run sheet sync in background
    backgroundSheetsSync();

    if (refreshArchiveMonths) {
        try {
            await loadAvailableSheetMonths(true);
        } catch (e) {}
    }
}

// ===== RENDER FUNCTIONS =====

function renderAccountsList() {
    const container = document.getElementById('active-account-list');
    if (AppState.accounts.length === 0) { container.innerHTML = '<p class="text-muted">No accounts added yet.</p>'; return; }
    const sorted = getSortedAccounts();
    const isAdmin = AppState.currentUser?.role === 'admin';
    
    // Add Sortable to container if not initialized
    if (isAdmin && !container._sortableInstance) {
        container._sortableInstance = Sortable.create(container, {
            handle: '.drag-handle',
            animation: 250,
            onEnd: function() {
                const newOrder = Array.from(container.children).map(c => c.dataset.account);
                AppState.accounts = [...newOrder];
                saveAccountPositions(newOrder);
                saveAccountOrderToSheet(newOrder);
                Array.from(container.children).forEach((el, idx) => {
                    const posEl = el.querySelector('.account-position-badge');
                    if (posEl) posEl.textContent = idx + 1;
                });
            }
        });
    }

    container.innerHTML = sorted.map((acc, idx) => {
        const details = AppState.accountDetails?.find(d => d.name === acc) || {};
        const rechargeTxt = getRechargeText(details.rechargeDate);
        const extraHtml = `
            <div class="account-meta">
                <span><i class='bx bx-phone'></i> Mobile: ${details.mobile || 'Not added'}</span>
                <span><i class='bx bx-calendar'></i> Recharge: ${rechargeTxt}</span>
            </div>`;

        return `
        <div class="account-item" data-account="${acc.replace(/"/g, '&quot;')}">
            <div style="display: flex; align-items: center; gap: 10px;">
                ${isAdmin ? `<i class='bx bx-menu drag-handle' style="cursor: grab; color: #a0aec0;"></i>` : ''}
                <div class="account-position-badge">${idx + 1}</div>
                <div>
                    <span class="account-name font-bold">${acc}</span>
                    ${extraHtml}
                </div>
            </div>
            ${isAdmin ? `<div class="account-actions">
                <button class="btn btn-outline btn-sm edit-btn" onclick="openEditAccount('${acc.replace(/'/g, "\\'")}')" title="Edit details"><i class='bx bx-edit-alt'></i></button>
                <button class="btn btn-outline btn-sm delete-btn" onclick="openDeleteAccount('${acc.replace(/'/g, "\\'")}')" title="Delete"><i class='bx bx-trash'></i></button>
            </div>` : ''}
        </div>
    `}).join('');
}

function openEditAccount(name) {
    document.getElementById('edit-account-old-name').value = name;
    document.getElementById('edit-account-name').value = name;
    const nameLabel = document.getElementById('edit-account-name-display');
    if (nameLabel) nameLabel.textContent = name;
    
    // Fill the detailed fields if available
    const details = AppState.accountDetails?.find(d => d.name === name);
    if (document.getElementById('edit-account-mobile')) document.getElementById('edit-account-mobile').value = details?.mobile || '';
    if (document.getElementById('edit-account-recharge')) document.getElementById('edit-account-recharge').value = details?.rechargeDate || '';
    updateEditRechargeMeta();
    
    document.getElementById('edit-account-modal').classList.add('show');
    setTimeout(() => document.getElementById('edit-account-mobile')?.focus(), 200);
}

function updateEditRechargeMeta() {
    const rechargeInput = document.getElementById('edit-account-recharge');
    const meta = document.getElementById('edit-account-recharge-meta');
    if (!rechargeInput || !meta) return;
    meta.textContent = rechargeInput.value ? getRechargeText(rechargeInput.value) : 'Not added';
}

function openDeleteAccount(name) {
    document.getElementById('delete-account-name').value = name;
    document.getElementById('delete-account-name-display').textContent = `"${name}"?`;
    document.getElementById('delete-account-modal').classList.add('show');
}

function renderOrderEntryTable() {
    const tbody = document.getElementById('daily-order-tbody');
    const container = document.getElementById('order-form-container');
    const msg = document.getElementById('no-accounts-msg');
    if (AppState.accounts.length === 0) { container.classList.add('hidden'); msg.classList.remove('hidden'); return; }
    container.classList.remove('hidden'); msg.classList.add('hidden');
    const sorted = getSortedAccounts();
    tbody.innerHTML = sorted.map((acc, index) => `
        <tr class="order-row" data-account="${acc}">
            <td class="drag-handle-cell"><i class='bx bx-menu drag-handle'></i></td>
            <td class="position-number">${index + 1}</td>
            <td class="font-medium">${acc}</td>
            <td><input type="number" min="0" class="inp-meesho" data-index="${index}" placeholder="0"></td>
            <td><span class="row-total" id="total-${index}">0</span></td>
        </tr>
    `).join('');
    document.querySelectorAll('.inp-meesho').forEach(inp => {
        inp.addEventListener('input', (e) => {
            const idx = e.target.dataset.index;
            const m = parseInt(document.querySelector(`.inp-meesho[data-index="${idx}"]`).value) || 0;
            document.getElementById(`total-${idx}`).textContent = m;
            calculateGrandTotals();
        });
    });
    calculateGrandTotals();
    initDragAndDrop();
}

function initDragAndDrop() {
    const tbody = document.getElementById('daily-order-tbody');
    if (!tbody || typeof Sortable === 'undefined') return;
    if (tbody._sortableInstance) tbody._sortableInstance.destroy();
    tbody._sortableInstance = Sortable.create(tbody, {
        handle: '.drag-handle', animation: 250, easing: 'cubic-bezier(0.25, 1, 0.5, 1)',
        ghostClass: 'sortable-ghost', chosenClass: 'sortable-chosen', dragClass: 'sortable-drag',
        onEnd: function(evt) {
            const rows = tbody.querySelectorAll('.order-row');
            const newOrder = [];
            rows.forEach((row, idx) => {
                newOrder.push(row.dataset.account);
                row.querySelector('.position-number').textContent = idx + 1;
                row.querySelector('.inp-meesho').dataset.index = idx;
            });
            saveAccountOrderToSheet(newOrder);
            document.querySelectorAll('.inp-meesho').forEach(inp => {
                const newInp = inp.cloneNode(true);
                inp.parentNode.replaceChild(newInp, inp);
                newInp.addEventListener('input', (e) => {
                    const idx = e.target.dataset.index;
                    const m = parseInt(document.querySelector(`.inp-meesho[data-index="${idx}"]`).value) || 0;
                    document.getElementById(`total-${idx}`).textContent = m;
                    calculateGrandTotals();
                });
            });
            showToast('Saving position...', 'success');
        }
    });
}

async function saveAccountOrderToSheet(orderedAccounts, paramCompanyId) {
    const compId = paramCompanyId || AppState.currentCompany;
    try {
        const res = await apiRequest({ action: 'updateAccountOrder', orderedAccounts, companyId: compId });
        if (res.success) showToast('Position saved!', 'success');
        else showToast(res.message || 'Failed', 'error');
    } catch(err) { showToast('Position saved locally, sheet sync failed', 'error'); }
}

function calculateGrandTotals() {
    let gm = 0;
    document.querySelectorAll('.inp-meesho').forEach(inp => gm += (parseInt(inp.value) || 0));
    document.getElementById('table-grand-meesho').textContent = gm;
    document.getElementById('table-grand-total').textContent = gm;
}

function checkExistingOrdersForDate() {
    const alert = document.getElementById('already-submitted-alert');
    const formContainer = document.getElementById('order-form-container');
    const dateInput = normalizeToISODate(document.getElementById('order-date').value);
    if (!dateInput || !AppState.dashboardData) return;
    const existingOrders = AppState.dashboardData.filter(d => normalizeToISODate(d.date) === dateInput);
    if (existingOrders.length > 0) {
        const todayStr = getTodayISODate();
        const isToday = dateInput === todayStr;
        const headingText = isToday ? "Today's Orders Submitted Successfully" : "Orders Already Submitted";
        alert.classList.remove('hidden');
        alert.innerHTML = `
            <div class="flex-between">
                <div>
                    <div class="font-bold text-success flex items-center gap-2" style="color:#10b981"><i class='bx bxs-check-circle'></i> ${headingText}</div>
                    <div class="text-sm text-muted mt-1">Total <strong class="text-main" id="submitted-grand-total">${existingOrders.reduce((s,o)=>s+(parseInt(o.total)||0),0)}</strong> orders added for this date.</div>
                </div>
                <div>
                    <span class="btn btn-outline btn-sm bg-white">View Details <i class='bx bx-right-arrow-alt'></i></span>
                </div>
            </div>
        `;
        formContainer.classList.remove('hidden');
        let gm = 0, gt = 0;
        const modalBody = document.getElementById('modal-details-tbody');
        modalBody.innerHTML = existingOrders.map(o => {
            const m = parseInt(o.meesho)||0, t = parseInt(o.total)||0;
            gm += m; gt += t;
            return `<tr><td>${o.accountName}</td><td style="text-align:right;">${m}</td><td style="text-align:right;font-weight:600;">${t}</td></tr>`;
        }).join('');
        document.getElementById('submitted-grand-total').textContent = gt;
        const [yyyy, mm, dd] = dateInput.split('-');
        document.getElementById('modal-date').textContent = `${dd}/${mm}/${yyyy}`;
        document.getElementById('modal-grand-meesho').textContent = gm;
        document.getElementById('modal-grand-total').textContent = gt;
    } else {
        alert.classList.add('hidden'); formContainer.classList.remove('hidden');
        document.querySelectorAll('.order-row input').forEach(inp => inp.value = '');
        document.querySelectorAll('.row-total').forEach(tot => tot.textContent = '0');
        calculateGrandTotals();
    }
}

// ===== Dashboard Filter Helpers =====
function filterDataByDate(data) {
    const filterType = document.getElementById('dash-filter-type').value;
    const todayStr = getTodayISODate();
    const currentMonthStr = todayStr.substring(0, 7);
    const fromRaw = normalizeToISODate(document.getElementById('dash-filter-from').value);
    const toRaw = normalizeToISODate(document.getElementById('dash-filter-to').value);
    const from = fromRaw && toRaw && fromRaw > toRaw ? toRaw : fromRaw;
    const to = fromRaw && toRaw && fromRaw > toRaw ? fromRaw : toRaw;

    return data.filter(d => {
        const rowDate = normalizeToISODate(d.date);
        if (!rowDate) return false;
        if (filterType === 'this_month') return rowDate.startsWith(currentMonthStr);
        else if (filterType === 'all_time') return true;
        else if (filterType === 'custom_date') {
            if (from && to) return rowDate >= from && rowDate <= to;
            if (from) return rowDate >= from;
            if (to) return rowDate <= to;
            return true;
        }
        return true;
    });
}

function sumTotals(data) {
    let meesho = 0, total = 0;
    data.forEach(row => { meesho += parseInt(row.meesho)||0; total += parseInt(row.total)||0; });
    return { meesho, total };
}

// ===== MAIN DASHBOARD RENDER =====
function renderDashboard() {
    const filterType = document.getElementById('dash-filter-type').value;
    
    // Update title
    let titleTxt = "Overview";
    if (filterType === 'this_month') {
        const now = new Date();
        const ms = ['January','February','March','April','May','June','July','August','September','October','November','December'];
        titleTxt = `${ms[now.getMonth()]} ${now.getFullYear()} Summary`;
    }
    else if (filterType === 'custom_date') {
        const fromRaw = normalizeToISODate(document.getElementById('dash-filter-from').value);
        const toRaw = normalizeToISODate(document.getElementById('dash-filter-to').value);
        const from = fromRaw && toRaw && fromRaw > toRaw ? toRaw : fromRaw;
        const to = fromRaw && toRaw && fromRaw > toRaw ? fromRaw : toRaw;
        if (from && to) {
            const [fy,fm,fd] = from.split('-');
            const [ty,tm,td] = to.split('-');
            titleTxt = `Overview (${fd}/${fm}/${fy} - ${td}/${tm}/${ty})`;
        }
        else titleTxt = "Select Date Range";
    }
    document.getElementById('dash-title-txt').textContent = titleTxt;

    const c1Filtered = filterDataByDate(AppState.company1Data);
    const c2Filtered = filterDataByDate(AppState.company2Data);
    const c1Totals = sumTotals(c1Filtered);
    const c2Totals = sumTotals(c2Filtered);
    
    // KPI Cards
    document.getElementById('dash-c1-meesho').textContent = c1Totals.meesho;
    document.getElementById('dash-c1-total').textContent = c1Totals.total;
    document.getElementById('dash-c2-meesho').textContent = c2Totals.meesho;
    document.getElementById('dash-c2-total').textContent = c2Totals.total;
    document.getElementById('dash-combined-meesho').textContent = c1Totals.meesho + c2Totals.meesho;
    document.getElementById('dash-combined-total').textContent = c1Totals.total + c2Totals.total;

    // Account-wise Totals with dividers
    renderAccountTotals(c1Filtered, c2Filtered);
    // Today's Status
    renderTodayStatus();
    // Inactive Alerts
    renderInactiveAlerts(c1Filtered, c2Filtered);
    // Company Comparison
    renderCompanyComparison(c1Totals, c2Totals);
    // Highest / Lowest Day
    renderHighLowDay(c1Filtered, c2Filtered);
    // Account Ranking
    renderAccountRanking(c1Filtered, c2Filtered);
    // Calendar Heatmap
    renderCalendarHeatmap();
}

function renderAccountTotals(c1Filtered, c2Filtered) {
    const accTbody = document.getElementById('dash-account-totals');
    accTbody.innerHTML = '';
    
    const buildAccTotals = (accounts, filtered) => {
        const totals = {};
        accounts.forEach(a => totals[a] = { meesho:0, total:0 });
        filtered.forEach(row => { if (totals[row.accountName]) { totals[row.accountName].meesho += parseInt(row.meesho)||0; totals[row.accountName].total += parseInt(row.total)||0; } });
        return totals;
    };
    
    const c1AccTotals = buildAccTotals(AppState.company1Accounts, c1Filtered);
    const c2AccTotals = buildAccTotals(AppState.company2Accounts, c2Filtered);

    let html = '';
    
    // Company A Header
    if (Object.keys(c1AccTotals).length > 0) {
        html += `<tr class="table-divider-row"><td colspan="5">Company A Accounts</td></tr>`;
        Object.keys(c1AccTotals).forEach(acc => {
            const d = c1AccTotals[acc];
            html += `<tr><td class="font-medium">${acc}</td><td class="text-center"><span class="dot dot-a"></span></td><td class="text-right text-muted">${d.meesho}</td><td class="text-right font-bold text-main">${d.total}</td></tr>`;
        });
    }

    // Company B Header
    if (Object.keys(c2AccTotals).length > 0) {
        html += `<tr class="table-divider-row"><td colspan="5">Company B Accounts</td></tr>`;
        Object.keys(c2AccTotals).forEach(acc => {
            const d = c2AccTotals[acc];
            html += `<tr><td class="font-medium">${acc}</td><td class="text-center"><span class="dot dot-b"></span></td><td class="text-right text-muted">${d.meesho}</td><td class="text-right font-bold text-main">${d.total}</td></tr>`;
        });
    }

    if (!html) html = '<tr><td colspan="5" class="text-center text-muted">No accounts found.</td></tr>';
    accTbody.innerHTML = html;
}

function renderRecentRecords(c1Filtered, c2Filtered) {
    const recentBody = document.getElementById('dash-recent-records');
    const allRecords = [];
    c1Filtered.forEach(r => allRecords.push({ ...r, company: 'A' }));
    c2Filtered.forEach(r => allRecords.push({ ...r, company: 'B' }));
    
    const dateGroups = {};
    allRecords.forEach(r => {
        if (!dateGroups[r.date]) dateGroups[r.date] = { totalMeesho: 0, totalOrders: 0, records: [] };
        const m = parseInt(r.meesho)||0, t = parseInt(r.total)||0;
        dateGroups[r.date].totalMeesho += m; dateGroups[r.date].totalOrders += t;
        dateGroups[r.date].records.push(r);
    });
    
    const sortedDates = Object.keys(dateGroups).sort((a,b) => new Date(b) - new Date(a)).slice(0, 7);
    let html = '';
    sortedDates.forEach(date => {
        const group = dateGroups[date]; const dateId = date.replace(/-/g, '');
        html += `<tr class="date-header-row" onclick="toggleDateDetails('${dateId}')"><td><span class="date-toggle-icon" id="icon-${dateId}"><i class='bx bx-chevron-right'></i></span>${date}</td><td>All</td><td>All Accounts</td><td>${group.totalMeesho}</td><td style="font-weight:600;">${group.totalOrders}</td></tr>`;
        group.records.forEach(r => {
            const m = parseInt(r.meesho)||0, t = parseInt(r.total)||0;
            html += `<tr class="date-detail-row" data-date="${dateId}"><td></td><td><span class="company-badge badge-${r.company.toLowerCase()}">${r.company}</span></td><td>${r.accountName}</td><td>${m}</td><td style="font-weight:500;">${t}</td></tr>`;
        });
    });
    if (sortedDates.length === 0) html = '<tr><td colspan="6" class="text-center text-muted">No records found.</td></tr>';
    recentBody.innerHTML = html;
}

// ===== TODAY'S STATUS BANNER =====
function renderTodayStatus() {
    const todayStr = getTodayISODate();
    const banner = document.getElementById('today-status-banner');
    
    const c1Today = AppState.company1Data.filter(d => normalizeToISODate(d.date) === todayStr);
    const c2Today = AppState.company2Data.filter(d => normalizeToISODate(d.date) === todayStr);
    const c1Done = c1Today.length > 0;
    const c2Done = c2Today.length > 0;
    
    const c1Total = c1Today.reduce((s, r) => s + (parseInt(r.total)||0), 0);
    const c2Total = c2Today.reduce((s, r) => s + (parseInt(r.total)||0), 0);
    
    if (c1Done && c2Done) {
        banner.className = 'status-banner status-done mb-2';
        banner.innerHTML = `
            <div class="status-banner-content">
                <div class="status-icon"><i class='bx bxs-check-circle'></i></div>
                <div class="status-text">
                    <strong>All Orders Submitted Today</strong>
                    <p>Data for ${todayStr} is complete</p>
                </div>
            </div>
            <div class="status-banner-stats">
                <div class="stat-box"><p>Company A</p><h3>${c1Total}</h3></div>
                <div class="stat-divider"></div>
                <div class="stat-box"><p>Company B</p><h3>${c2Total}</h3></div>
                <div class="stat-divider"></div>
                <div class="stat-box"><p class="text-success">Total</p><h3 class="text-success">${c1Total + c2Total}</h3></div>
            </div>
        `;
    } else if (c1Done || c2Done) {
        banner.className = 'status-banner status-partial mb-2';
        const done = c1Done ? 'A' : 'B';
        const pending = c1Done ? 'B' : 'A';
        const doneTotal = c1Done ? c1Total : c2Total;
        banner.innerHTML = `
            <div class="status-banner-content">
                <div class="status-icon"><i class='bx bxs-info-circle'></i></div>
                <div class="status-text">
                    <strong>Partially Submitted</strong>
                    <p>Company ${done} submitted, Company ${pending} pending</p>
                </div>
            </div>
            <div class="status-banner-stats">
                <div class="stat-box"><p>Company ${done}</p><h3>${doneTotal}</h3></div>
                <div class="stat-divider"></div>
                <div class="stat-box"><p>Company ${pending}</p><h3>0</h3></div>
                <div class="stat-divider"></div>
                <div class="stat-box"><p class="text-success">Total</p><h3 class="text-success">${doneTotal}</h3></div>
            </div>
        `;
    } else {
        banner.className = 'status-banner status-pending mb-2';
        banner.innerHTML = `
            <div class="status-banner-content">
                <div class="status-icon"><i class='bx bxs-time-five'></i></div>
                <div class="status-text">
                    <strong>No Orders Submitted Today</strong>
                    <p>Both Company A and Company B orders are pending</p>
                </div>
            </div>
            <div class="status-banner-stats">
                <div class="stat-box"><p>Company A</p><h3>0</h3></div>
                <div class="stat-divider"></div>
                <div class="stat-box"><p>Company B</p><h3>0</h3></div>
                <div class="stat-divider"></div>
                <div class="stat-box"><p class="text-success">Total</p><h3 class="text-success">0</h3></div>
            </div>
        `;
    }
}

// ===== INACTIVE ACCOUNT ALERTS =====
// ===== INACTIVE ACCOUNT ALERTS =====
function renderInactiveAlerts(c1Filtered, c2Filtered) {
    const container = document.getElementById('inactive-alerts-container');
    const inactiveCount = document.getElementById('inactive-count');
    const inactiveCard = document.getElementById('inactive-card-container');
    const inactiveAccounts = [];
    
    const getLastOrderMeta = (accountName, rawData) => {
        let lastDate = null;
        rawData.forEach(r => {
            if (r.accountName === accountName && (parseInt(r.total) || 0) > 0) {
                const normalizedDate = normalizeToISODate(r.date);
                if (normalizedDate && (!lastDate || normalizedDate > lastDate)) {
                    lastDate = normalizedDate;
                }
            }
        });
        if (!lastDate) return { days: -1, lastDate: '' };
        return { days: diffDays(lastDate, getTodayISODate()), lastDate };
    };

    AppState.company1Accounts.forEach(acc => {
        if (!c1Filtered.some(r => r.accountName === acc && (parseInt(r.total)||0) > 0)) {
            const meta = getLastOrderMeta(acc, AppState.company1Data);
            inactiveAccounts.push({
                name: acc,
                companyName: getCompanyDisplayName('company1'),
                class: 'a',
                days: meta.days,
                lastDate: meta.lastDate
            });
        }
    });
    AppState.company2Accounts.forEach(acc => {
        if (!c2Filtered.some(r => r.accountName === acc && (parseInt(r.total)||0) > 0)) {
            const meta = getLastOrderMeta(acc, AppState.company2Data);
            inactiveAccounts.push({
                name: acc,
                companyName: getCompanyDisplayName('company2'),
                class: 'b',
                days: meta.days,
                lastDate: meta.lastDate
            });
        }
    });
    
    inactiveAccounts.sort((a, b) => {
        const aRank = a.days === -1 ? Number.MAX_SAFE_INTEGER : a.days;
        const bRank = b.days === -1 ? Number.MAX_SAFE_INTEGER : b.days;
        return bRank - aRank;
    });

    inactiveCount.textContent = inactiveAccounts.length;
    
    if (inactiveAccounts.length > 0) {
        inactiveCard.classList.remove('hidden');
        container.innerHTML = inactiveAccounts.map(a => {
            const inactiveForDays = a.days === -1 ? null : Math.max(1, a.days);
            const zeroOrdersTxt = inactiveForDays === null
                ? '0 orders from last all recorded days'
                : `0 orders from last ${inactiveForDays} day${inactiveForDays > 1 ? 's' : ''}`;
            const daysTxt = a.days === -1
                ? 'Last order: Never'
                : (a.days === 0 ? 'Last order: Today' : `Last order: ${a.days} day${a.days > 1 ? 's' : ''} ago (${a.lastDate})`);
            return `
            <div class="inactive-item">
                <div style="flex:1;">
                    <span class="inactive-name">${a.name}</span>
                    <div class="inactive-subline">${a.companyName} • ${zeroOrdersTxt}</div>
                    <div class="inactive-subline">${daysTxt}</div>
                </div>
                <span class="badge badge-${a.class}">0 Orders</span>
            </div>
        `}).join('');
    } else {
        inactiveCard.classList.add('hidden');
        container.innerHTML = '';
    }
}

// ===== COMPANY A vs B COMPARISON =====
function renderCompanyComparison(c1Totals, c2Totals) {
    const grid = document.getElementById('comparison-grid');
    
    const items = [
        { label: 'Meesho Orders', a: c1Totals.meesho, b: c2Totals.meesho },
        { label: 'Total Orders', a: c1Totals.total, b: c2Totals.total }
    ];
    
    grid.innerHTML = items.map(item => {
        const total = item.a + item.b;
        const aWidth = total === 0 ? 0 : Math.round((item.a / total) * 100);
        const bWidth = total === 0 ? 0 : Math.round((item.b / total) * 100);
        const aBarWidth = total === 0 ? 50 : aWidth;
        const bBarWidth = total === 0 ? 50 : bWidth;
        const hoverText = `Company 1: ${item.a} (${aWidth}%) • Company 2: ${item.b} (${bWidth}%)`;
        return `
        <div class="comp-row">
            <div class="comp-header">
                <span>${item.label}</span>
                <div class="comp-stats">
                    <span class="text-xs"><span class="dot-sm dot-a"></span> ${getCompanyDisplayName('company1')}: ${item.a}</span>
                    <span class="text-xs pl-2"><span class="dot-sm dot-b"></span> ${getCompanyDisplayName('company2')}: ${item.b}</span>
                </div>
            </div>
            <div class="comp-bar-shell" title="${hoverText}">
                <div class="comp-bar-bg">
                    <div class="comp-bar-a" style="width: ${aBarWidth}%;"></div>
                    <div class="comp-bar-b" style="width: ${bBarWidth}%;"></div>
                </div>
                <div class="comp-tooltip">${hoverText}</div>
            </div>
        </div>`;
    }).join('');
}

// ===== HIGHEST / LOWEST DAY =====
function renderHighLowDay(c1Filtered, c2Filtered) {
    const container = document.getElementById('high-low-container');
    const allData = [...c1Filtered, ...c2Filtered];
    
    const dateGrouped = {};
    allData.forEach(r => {
        const rowDate = normalizeToISODate(r.date);
        if (!rowDate) return;
        if (!dateGrouped[rowDate]) dateGrouped[rowDate] = 0;
        dateGrouped[rowDate] += parseInt(r.total) || 0;
    });
    
    const dates = Object.entries(dateGrouped);
    if (dates.length === 0) {
        container.innerHTML = '<div class="hl-box best"><div class="hl-header"><i class="bx bx-trending-up"></i> Best Day</div><div class="hl-val">0</div></div><div class="hl-box worst"><div class="hl-header"><i class="bx bx-trending-down"></i> Lowest Day</div><div class="hl-val">0</div></div>';
        return;
    }
    
    dates.sort((a, b) => b[1] - a[1]);
    const highest = dates[0];
    const lowest = dates[dates.length - 1];
    
    const formatDate = (d) => { const [y, m, day] = d.split('-'); const ms = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']; return `${day} ${ms[parseInt(m)-1]} ${y}`; };
    
    container.innerHTML = `
        <div class="hl-box best">
            <div class="hl-header"><i class='bx bx-trending-up'></i> Best Day</div>
            <div class="hl-val">${highest[1]}</div>
            <div class="hl-date">${formatDate(highest[0])}</div>
        </div>
        <div class="hl-box worst">
            <div class="hl-header"><i class='bx bx-trending-down'></i> Lowest Day</div>
            <div class="hl-val">${lowest[1]}</div>
            <div class="hl-date">${formatDate(lowest[0])}</div>
        </div>
    `;
}

// ===== ACCOUNT RANKING =====
function renderAccountRanking(c1Filtered, c2Filtered) {
    const rankTbody = document.getElementById('ranking-tbody');
    const allAccounts = [];
    
    const c1AccTotals = {}; AppState.company1Accounts.forEach(a => c1AccTotals[a] = 0);
    c1Filtered.forEach(r => { if (c1AccTotals[r.accountName] !== undefined) c1AccTotals[r.accountName] += parseInt(r.total)||0; });
    Object.entries(c1AccTotals).forEach(([name, total]) => allAccounts.push({ name, company: 'A', total }));
    
    const c2AccTotals = {}; AppState.company2Accounts.forEach(a => c2AccTotals[a] = 0);
    c2Filtered.forEach(r => { if (c2AccTotals[r.accountName] !== undefined) c2AccTotals[r.accountName] += parseInt(r.total)||0; });
    Object.entries(c2AccTotals).forEach(([name, total]) => allAccounts.push({ name, company: 'B', total }));
    
    allAccounts.sort((a, b) => b.total - a.total);
    const top = allAccounts; // Show all accounts
    
    if (top.length === 0) { rankTbody.innerHTML = '<div class="text-center text-muted p-2">No data</div>'; return; }
    
    rankTbody.innerHTML = top.map((acc, idx) => {
        const rank = idx + 1;
        const rankClass = rank === 1 ? 'top-1' : '';
        const badgeClass = acc.company === 'A' ? 'text-a' : 'text-b';
        return `
        <div class="ranking-item ${rankClass}">
            <div class="rank-num">${rank}</div>
            <div class="ranking-info">
                <div class="ranking-name">${acc.name}</div>
                <div class="ranking-comp ${badgeClass}">Company ${acc.company}</div>
            </div>
            <div class="ranking-total">${acc.total}</div>
        </div>`;
    }).join('');
}

// ===== CALENDAR HEATMAP =====
// ===== CALENDAR HEATMAP =====
function renderCalendarHeatmap() {
    const grid = document.getElementById('heatmap-grid');
    const label = document.getElementById('heatmap-month-label');
    const year = AppState.heatmapYear;
    const month = AppState.heatmapMonth;
    
    const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    label.textContent = `${monthNames[month]} ${year}`;
    
    const monthStr = `${year}-${String(month + 1).padStart(2, '0')}`;
    const allData = [...AppState.company1Data, ...AppState.company2Data];
    
    // Sum orders by day
    const dayTotals = {};
    allData.forEach(r => {
        const rowDate = normalizeToISODate(r.date);
        if (rowDate && rowDate.startsWith(monthStr)) {
            const day = parseInt(rowDate.split('-')[2], 10);
            if (!dayTotals[day]) dayTotals[day] = 0;
            dayTotals[day] += parseInt(r.total) || 0;
        }
    });
    
    const values = Object.values(dayTotals);
    const maxOrders = values.length > 0 ? Math.max(...values) : 0;
    
    const getLevel = (count) => {
        if (count === 0 || !count) return 0;
        if (maxOrders === 0) return 0;
        const pct = count / maxOrders;
        if (pct <= 0.25) return 1;
        if (pct <= 0.5) return 2;
        if (pct <= 0.75) return 3;
        return 4;
    };
    
    const firstDayRaw = new Date(year, month, 1).getDay(); // 0=Sun
    const firstDay = firstDayRaw === 0 ? 6 : firstDayRaw - 1; // 0=Mon
    const daysInMonth = new Date(year, month + 1, 0).getDate();
    
    let html = '';
    
    for (let i = 0; i < firstDay; i++) {
        html += '<div class="heatmap-day heatmap-empty"></div>';
    }
    
    for (let day = 1; day <= daysInMonth; day++) {
        const total = dayTotals[day] || 0;
        const level = getLevel(total);
        const tooltipText = `${day} ${monthNames[month]}: ${total} orders`;
        const fullDate = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
        html += `<div class="heatmap-day heatmap-${level}" title="${tooltipText}" onclick="showHeatmapDetails('${fullDate}')">${day}</div>`;
    }
    
    grid.innerHTML = html;
}

window.showHeatmapDetails = function(dateStr) {
    const detailsBox = document.getElementById('heatmap-details');
    detailsBox.classList.remove('hidden');
    
    const [y, m, d] = dateStr.split('-');
    const weekday = new Date(y, parseInt(m)-1, d).toLocaleDateString('en-US', { weekday: 'long' });
    
    let c1M = 0, c1T = 0;
    AppState.company1Data.filter(r => normalizeToISODate(r.date) === dateStr).forEach(r => { c1M += parseInt(r.meesho)||0; c1T += parseInt(r.total)||0; });
    
    let c2M = 0, c2T = 0;
    AppState.company2Data.filter(r => normalizeToISODate(r.date) === dateStr).forEach(r => { c2M += parseInt(r.meesho)||0; c2T += parseInt(r.total)||0; });
    const dayTotal = c1T + c2T;
    const company1Name = getCompanyDisplayName('company1');
    const company2Name = getCompanyDisplayName('company2');
    
    detailsBox.innerHTML = `
        <div class="date-details-header">
            <div>
                <h4 class="text-sm font-bold">Details for ${d}/${m}/${y}</h4>
                <p class="date-details-weekday">${weekday}</p>
            </div>
            <span class="date-total-chip">${dayTotal} Orders</span>
        </div>
        <div class="date-company-grid">
            <div class="date-company-card date-company-a">
                <div class="date-company-head">
                    <div class="date-company-title"><span class="dot-sm dot-a"></span> ${company1Name}</div>
                    <span class="date-company-total">${c1T}</span>
                </div>
                <div class="date-company-chips">
                    <span>Meesho: <strong>${c1M}</strong></span>
                </div>
            </div>
            <div class="date-company-card date-company-b">
                <div class="date-company-head">
                    <div class="date-company-title"><span class="dot-sm dot-b"></span> ${company2Name}</div>
                    <span class="date-company-total">${c2T}</span>
                </div>
                <div class="date-company-chips">
                    <span>Meesho: <strong>${c2M}</strong></span>
                </div>
            </div>
        </div>
        <div class="date-details-footer">
            <span>Daily Total</span>
            <span class="date-footer-total">${dayTotal}</span>
        </div>
    `;
};

// ===== EXPORT TO CSV =====
function exportToCSV() {
    const c1Filtered = filterDataByDate(AppState.company1Data);
    const c2Filtered = filterDataByDate(AppState.company2Data);
    
    const rows = [['Date', 'Company', 'Account', 'Meesho', 'Total']];
    
    c1Filtered.forEach(r => {
        rows.push([normalizeToISODate(r.date), 'Company A', r.accountName, parseInt(r.meesho)||0, parseInt(r.total)||0]);
    });
    c2Filtered.forEach(r => {
        rows.push([normalizeToISODate(r.date), 'Company B', r.accountName, parseInt(r.meesho)||0, parseInt(r.total)||0]);
    });
    
    if (rows.length <= 1) { showToast('No data to export', 'error'); return; }
    
    const csv = rows.map(row => row.map(cell => `"${cell}"`).join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    
    const filterType = document.getElementById('dash-filter-type').value;
    let filename = 'krimaa_orders';
    if (filterType === 'this_month') {
        const now = new Date();
        filename += `_${now.getFullYear()}_${String(now.getMonth()+1).padStart(2,'0')}`;
    } else if (filterType === 'custom_date') {
        const from = document.getElementById('dash-filter-from').value;
        const to = document.getElementById('dash-filter-to').value;
        if (from && to) filename += `_${from}_to_${to}`;
    }
    
    link.download = `${filename}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    showToast('CSV exported!', 'success');
}

// ===== TOGGLE DATE DETAILS =====
function toggleDateDetails(dateId) {
    const rows = document.querySelectorAll(`.date-detail-row[data-date="${dateId}"]`);
    const icon = document.getElementById(`icon-${dateId}`);
    const isOpen = rows[0] && rows[0].classList.contains('show');
    rows.forEach(row => row.classList.toggle('show', !isOpen));
    if (icon) icon.classList.toggle('open', !isOpen);
}

// ===== API (routes through Firebase) =====
async function apiRequest(payload) {
    const action = payload?.action;
    const companyId = payload?.companyId || AppState.currentCompany || 'company1';
    try {
        switch (action) {
            case 'getAccounts': return await FirebaseService.getAccounts(companyId);
            case 'getDashboardData': return await FirebaseService.getOrders(companyId, payload.month);
            case 'submitOrders': return await FirebaseService.submitOrders(payload.date, payload.orders, companyId);
            case 'addAccount': return await FirebaseService.addAccount(payload.accountName, companyId, payload.mobile, payload.gstin, payload.rechargeDate);
            case 'editAccount': return await FirebaseService.editAccount(payload.oldName, payload.newName, companyId, payload.mobile, payload.gstin, payload.rechargeDate);
            case 'deleteAccount': return await FirebaseService.deleteAccount(payload.accountName, companyId);
            case 'updateAccountOrder': return await FirebaseService.updateAccountOrder(payload.orderedAccounts, companyId);
            case 'saveRemark': return await FirebaseService.saveRemark(payload.date, payload.remark);
            case 'getRemarks': return await FirebaseService.getRemarks();
            case 'updateOrder': return await FirebaseService.updateOrder(payload.date, payload.accountName, payload.field, payload.value, companyId);
            case 'getCompanies': return { success: true, data: [{id: 'company1', name: 'Company 1'}, {id: 'company2', name: 'Company 2'}] };
            case 'updateMoney': return await FirebaseService.updateMoney(payload.accountName, companyId, payload.money, payload.expense, payload.date);
            case 'resetAllMoney': return await FirebaseService.resetAllMoney(payload.date);
            case 'createMoneyBackup': return await FirebaseService.createMoneyBackup(payload.date, payload.rows, payload.reason);
            case 'getMoneyBackups': return await FirebaseService.getMoneyBackups();
            case 'deleteMoneyBackup': return await FirebaseService.deleteMoneyBackup(payload.backupId);
            default: throw new Error('Unknown action: ' + action);
        }
    } catch (err) {
        console.error(`Firebase ${action} error:`, err);
        throw err;
    }
}

// Google Sheets API – used only for backup/archive operations
async function sheetsApiRequest(payload) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000); // 15s timeout
    try {
        const res = await fetch(SHEETS_API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'text/plain;charset=utf-8' },
            body: JSON.stringify(payload),
            signal: controller.signal
        });
        clearTimeout(timeout);
        if (!res.ok) throw new Error(`Sheets API error (${res.status})`);
        const text = await res.text();
        try { return JSON.parse(text); } catch (e) { throw new Error('Invalid JSON from Sheets API'); }
    } catch (e) {
        clearTimeout(timeout);
        if (e.name === 'AbortError') throw new Error('Sheets API timeout (15s)');
        throw e;
    }
}

// ===== FIREBASE MIGRATION (one-time) =====
async function ensureFirebaseSeeded() {
    const seeded = localStorage.getItem('firebase_seeded');
    if (seeded) return;
    
    try {
        // Timeout entire migration to 30s max so app doesn't hang
        await Promise.race([
            _doFirebaseSeed(),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Migration timeout')), 30000))
        ]);
    } catch (e) {
        console.warn('Firebase seeding skipped/failed:', e);
        showToast('Loading from Firebase…', 'info');
    }
}

async function _doFirebaseSeed() {
    let c1Empty = true, c2Empty = true;
    try {
        c1Empty = await FirebaseService.isEmpty('company1');
        c2Empty = await FirebaseService.isEmpty('company2');
    } catch (e) {
        console.warn('Firestore check failed:', e);
        // If Firestore itself fails, skip seeding entirely
        return;
    }
    
    if (!c1Empty && !c2Empty) {
        localStorage.setItem('firebase_seeded', 'true');
        return;
    }
    
    showProgressToast('Setting up Firebase… (one-time)');
    
    try {
        if (c1Empty) {
            const [accRes, ordRes] = await Promise.all([
                sheetsApiRequest({ action: 'getAccounts', companyId: 'company1' }),
                sheetsApiRequest({ action: 'getDashboardData', companyId: 'company1' })
            ]);
            await FirebaseService.seedFromSheets('company1', accRes?.data || [], ordRes?.data || []);
        }
        if (c2Empty) {
            const [accRes, ordRes] = await Promise.all([
                sheetsApiRequest({ action: 'getAccounts', companyId: 'company2' }),
                sheetsApiRequest({ action: 'getDashboardData', companyId: 'company2' })
            ]);
            await FirebaseService.seedFromSheets('company2', accRes?.data || [], ordRes?.data || []);
        }
        // Seed remarks
        try {
            const remRes = await sheetsApiRequest({ action: 'getRemarks' });
            if (remRes?.data) {
                for (const [date, remark] of Object.entries(remRes.data)) {
                    await FirebaseService.saveRemark(date, remark);
                }
            }
        } catch(e) { console.warn('Remarks migration skipped:', e); }
        
        localStorage.setItem('firebase_seeded', 'true');
        showToast('Firebase ready!', 'success');
    } catch (e) {
        console.error('Migration error:', e);
        showToast('Migration skipped – data will load from Firebase', 'info');
    }
}

// ===== BACKUP TO SHEETS =====
async function backupToSheets(isAuto = false) {
    const btn = document.getElementById('backup-btn');
    if (!isAuto && btn) { btn.disabled = true; btn.innerHTML = "<i class='bx bx-loader-alt bx-spin'></i> Fetching Local Data..."; }
    if (!isAuto) showToast('Starting full database backup to Google Sheets…', 'info');
    
    try {
        // 1. Flush any pending Firestore writes
        await FirebaseService.flushWrites();
        
        // 2. Get EVERY single piece of data from Firebase
        const allData = await FirebaseService.getAllDataForBackup();
        
        if (!isAuto && btn) { btn.innerHTML = "<i class='bx bx-loader-alt bx-spin'></i> Syncing to Google Sheets..."; }
        
        // 3. Send all data to Google Sheets in one comprehensive call
        const response = await sheetsApiRequest({
            action: 'saveFullBackup',
            data: allData
        });
        
        if (response && response.success) {
            const now = new Date().toISOString();
            await FirebaseService.setBackupMeta({ 
                lastBackup: now,
                totalOrders: (allData.company1.orders.length || 0) + (allData.company2.orders.length || 0)
            });
            updateLastBackupTimeDisplay(now);
            if (!isAuto) showToast('Full backup successful!', 'success');
            
            // Cleanup Firestore after successful backup
            if (!isAuto) showToast('Maintaining 30-day window in Firestore…', 'info');
            
            if (!isAuto && btn) {
                btn.innerHTML = `<i class='bx bx-check-double'></i> Saved ${response.totalRows || 0} Records`;
                btn.classList.add('btn-success', 'text-white');
                btn.classList.remove('btn-outline');
            }
            
            await FirebaseService.clearOldOrders();
            
            if (!isAuto && btn) {
                setTimeout(() => {
                    btn.disabled = false;
                    btn.innerHTML = "<i class='bx bx-cloud-upload'></i> Backup to Sheets";
                    btn.classList.remove('btn-success', 'text-white');
                    btn.classList.add('btn-outline');
                }, 3000);
            }
        } else {
            throw new Error(response?.message || 'Backup failed at Sheets layer');
        }
    } catch (e) {
        console.error('Backup error:', e);
        if (!isAuto) showToast('Full backup failed. Check connection.', 'error');
        if (!isAuto && btn) {
            btn.disabled = false;
            btn.innerHTML = "<i class='bx bx-cloud-upload'></i> Backup to Sheets";
        }
    }
}

function updateLastBackupTimeDisplay(isoString) {
    const el = document.getElementById('last-backup-time');
    if (!el) return;
    if (!isoString) {
        el.textContent = 'Never backed up';
        return;
    }
    try {
        const d = new Date(isoString);
        el.textContent = 'Last Backup: ' + d.toLocaleString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
    } catch(e) {
        el.textContent = '';
    }
}

// Monthly cleanup: move all data to Sheets, then clear old Firebase data
async function monthlyCleanup(isAuto = false) {
    if (!isAuto) showToast('Running monthly backup…', 'info');
    try {
        await backupToSheets(isAuto);
        const deleted = await FirebaseService.clearOldOrders(); // No parameter anymore
        await FirebaseService.setBackupMeta({
            lastMonthlyBackup: new Date().toISOString()
        });
        if (!isAuto) showToast(`Monthly backup done! ${deleted} archived rows cleared.`, 'success');
    } catch (e) {
        console.error('Monthly cleanup error:', e);
        if (!isAuto) showToast('Monthly cleanup failed', 'error');
    }
}

async function checkAutoBackup() {
    try {
        const meta = await FirebaseService.getBackupMeta();
        
        if (meta && meta.lastBackup) {
            updateLastBackupTimeDisplay(meta.lastBackup);
        } else {
            updateLastBackupTimeDisplay(null);
        }
        
        // The user intentionally requested: "dont do auto backup only manual backup"
        console.log("🛑 Automated backups are permanently disabled by user choice. Manual backups ONLY.");
        
    } catch (e) {
        console.warn('Auto-backup meta check failed:', e);
    }
}

// ===== SYNC INDICATOR =====
function initSyncIndicator() {
    FirebaseService.onSyncStatusChange(status => {
        const pending = typeof FirebaseService !== 'undefined' && FirebaseService.getPendingCount ? FirebaseService.getPendingCount() : 0;
        const el = document.getElementById('sync-indicator');
        if (el) {
            el.className = 'sync-indicator sync-' + status;
            const icons = {
                idle:    'bx-check-circle',
                saved:   'bx-check-circle',
                pending: 'bx-time-five',
                syncing: 'bx-loader-alt bx-spin',
                error:   'bx-error-circle'
            };
            const labels = {
                idle:    'Synced',
                saved:   'Saved ✓',
                pending: `${pending} unsaved`,
                syncing: 'Saving…',
                error:   'Sync error'
            };
            el.innerHTML = `<i class='bx ${icons[status] || icons.idle}'></i><span>${labels[status] || 'Synced'}</span>`;
        }
        
        // Manual Save Button on Data Sheet
        const saveBtn = document.getElementById('sheet-save-btn');
        if (saveBtn) {
            const pendingParams = typeof FirebaseService !== 'undefined' && FirebaseService.getPendingCount ? FirebaseService.getPendingCount() : 0;
            if (pendingParams > 0 || status === 'pending') {
                saveBtn.classList.remove('hidden');
                saveBtn.disabled = false;
                saveBtn.innerHTML = `<i class='bx bx-save'></i> Save ${pendingParams} changes`;
            } else if (status === 'syncing') {
                saveBtn.classList.remove('hidden');
                saveBtn.disabled = true;
                saveBtn.innerHTML = `<i class='bx bx-loader-alt bx-spin'></i> Saving…`;
            } else {
                saveBtn.classList.add('hidden');
            }
        }
    });
    
    // Bind Save Now button
    document.getElementById('sheet-save-btn')?.addEventListener('click', () => {
        if (typeof FirebaseService !== 'undefined') {
            FirebaseService.flushWrites();
        }
    });
}

function updateOrderDateLabel() {
    const val = normalizeToISODate(document.getElementById('order-date').value);
    if(!val) return;
    const [y,m,d] = val.split('-');
    const dateObj = new Date(y, parseInt(m)-1, d);
    const dayName = dateObj.toLocaleDateString('en-US', { weekday: 'long' });
    const todayStr = getTodayISODate();
    
    if (val === todayStr) {
        document.getElementById('order-date-label').textContent = `${dayName}, ${d}/${m}/${y} (Today)`;
    } else {
        document.getElementById('order-date-label').textContent = `${dayName}, ${d}/${m}/${y}`;
    }
}

// ===== UI Helpers =====
function showProgressToast(message) {
    showToast(message, "info");
}
function showToast(message, type = "success") {
    const toast = document.getElementById('toast');
    if (!toast) return;
    if (toast._timer) clearTimeout(toast._timer);

    toast.className = 'toast show';
    if (type === 'error') toast.classList.add('toast-error');
    else if (type === 'info') toast.classList.add('toast-info');
    else toast.classList.add('toast-success');

    let icon = 'bx-check-circle';
    if (type === 'error') icon = 'bx-error-circle';
    else if (type === 'info') icon = 'bx-info-circle';

    toast.innerHTML = `
        <div class="toast-content">
            <i class='bx ${icon} toast-icon'></i>
            <span class="toast-msg">${message}</span>
            <button class="toast-close" onclick="this.parentElement.parentElement.classList.remove('show')">&times;</button>
        </div>
        <div class="toast-progress"></div>
    `;

    if (type !== "info") {
        toast._timer = setTimeout(() => {
            toast.classList.remove('show');
        }, 3500);
    }
}
function showLoader() { document.getElementById('global-loader').classList.remove('hidden'); }
function hideLoader() { document.getElementById('global-loader').classList.add('hidden'); }

// Global exports
window.app = { navigateTo };
window.openEditAccount = openEditAccount;
window.openDeleteAccount = openDeleteAccount;
window.toggleDateDetails = toggleDateDetails;

// ===== DATA SHEET =====

// Debounce helper
const _sheetSaveTimers = {};
function debouncedSave(key, fn, delayMs = 1500) {
    if (_sheetSaveTimers[key]) clearTimeout(_sheetSaveTimers[key]);
    _sheetSaveTimers[key] = setTimeout(fn, delayMs);
}

// Remark cache
let _remarkCache = null;
let _remarkCacheLoaded = false;

async function loadRemarksFromBackend() {
    if (_remarkCacheLoaded && _remarkCache) return _remarkCache;
    try {
        const res = await apiRequest({ action: 'getRemarks' });
        if (res && res.success !== false && res.data) {
            _remarkCache = res.data;
            // Also sync to localStorage as fallback
            localStorage.setItem('sheetRemarks', JSON.stringify(res.data));
        } else {
            _remarkCache = {};
        }
    } catch (e) {
        console.warn('Could not load remarks from backend, using local', e);
        try { _remarkCache = JSON.parse(localStorage.getItem('sheetRemarks') || '{}'); } catch(ex) { _remarkCache = {}; }
    }
    _remarkCacheLoaded = true;
    return _remarkCache;
}

function initDataSheetListeners() {
    const compFilter = document.getElementById('sheet-company-filter');
    const monthFilter = document.getElementById('sheet-month-filter');
    const exportBtn = document.getElementById('sheet-export-btn');
    const reorderBtn = document.getElementById('sheet-reorder-btn');
    
    if (compFilter) compFilter.addEventListener('change', renderDataSheet);
    if (monthFilter) monthFilter.addEventListener('change', renderDataSheet);
    if (exportBtn) exportBtn.addEventListener('click', exportSheetCSV);
    if (reorderBtn) reorderBtn.addEventListener('click', openReorderColumnsModal);

    const closeReorderBtn = document.getElementById('close-reorder-modal-btn');
    if (closeReorderBtn) {
        closeReorderBtn.addEventListener('click', () => {
            document.getElementById('reorder-columns-modal').classList.remove('show');
        });
    }

    const reorderModal = document.getElementById('reorder-columns-modal');
    if (reorderModal) {
        reorderModal.addEventListener('click', (e) => {
            if (e.target.id === 'reorder-columns-modal') e.target.classList.remove('show');
        });
    }
}

function openReorderColumnsModal() {
    const modal = document.getElementById('reorder-columns-modal');
    const list = document.getElementById('reorder-account-list');
    if (!modal || !list) return;
    
    const companyFilter = document.getElementById('sheet-company-filter').value;
    let accounts = [];
    if (companyFilter === 'company1') {
        accounts = [...AppState.company1Accounts];
    } else if (companyFilter === 'company2') {
        accounts = [...AppState.company2Accounts];
    } else {
        showToast('Please select Company 1 or Company 2 first to reorder.', 'info');
        return;
    }
    
    list.innerHTML = accounts.map(acc => `
        <div class="account-item reorder-item" data-account="${acc.replace(/'/g, "\\'")}" style="cursor: grab; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 8px; background: #f8fafc; display: flex; align-items: center; gap: 0.75rem;">
            <i class='bx bx-menu text-muted'></i>
            <span class="font-bold text-sm">${acc}</span>
        </div>
    `).join('');
    
    modal.classList.add('show');
    
    if (list._sortableInstance) list._sortableInstance.destroy();
    list._sortableInstance = Sortable.create(list, {
        animation: 150,
        ghostClass: 'sortable-ghost',
        onEnd: function() {
            const newOrder = [];
            list.querySelectorAll('.reorder-item').forEach(item => {
                newOrder.push(item.dataset.account);
            });
            if (companyFilter === 'company1') {
                AppState.company1Accounts = newOrder;
            } else {
                AppState.company2Accounts = newOrder;
            }
            saveAccountOrderToSheet(newOrder, companyFilter);
            renderDataSheet();
        }
    });
}

async function loadAvailableSheetMonths(force = false) {
    if (!force && AppState.availableSheetMonths.length > 0) return AppState.availableSheetMonths;
    try {
        const res1 = await sheetsApiRequest({ action: 'getAvailableOrderMonths', companyId: 'company1' });
        const res2 = await sheetsApiRequest({ action: 'getAvailableOrderMonths', companyId: 'company2' });
        const combined = new Set([...(res1.data || []), ...(res2.data || [])]);
        AppState.availableSheetMonths = Array.from(combined).sort().reverse();
        return AppState.availableSheetMonths;
    } catch (e) {
        console.warn('Failed to load available months from sheets:', e);
        return [];
    }
}

function populateSheetMonthFilter() {
    const monthFilter = document.getElementById('sheet-month-filter');
    if (!monthFilter) return;
    
    const currentMonth = getTodayISODate().substring(0, 7);
    const months = new Set();
    months.add(currentMonth); // Always have current month

    // Add local data months
    const allData = [...AppState.company1Data, ...AppState.company2Data];
    allData.forEach(r => {
        const d = normalizeToISODate(r.date);
        if (d) months.add(d.substring(0, 7));
    });

    // Add archive months from AppState
    (AppState.availableSheetMonths || []).forEach(m => months.add(m));
    
    const sortedMonths = [...months].sort().reverse();
    const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    
    // Remember current selection
    const prevVal = monthFilter.value;
    
    let html = `<option value="all" ${!prevVal || prevVal === 'all' ? 'selected' : ''}>All Time</option>`;
    sortedMonths.forEach(m => {
        const [y, mo] = m.split('-');
        const isCurrent = m === currentMonth;
        const label = `${monthNames[parseInt(mo)-1]} ${y}${isCurrent ? ' (Current)' : ''}`;
        const sel = (prevVal && prevVal === m) ? 'selected' : '';
        html += `<option value="${m}" ${sel}>${label}</option>`;
    });
    monthFilter.innerHTML = html;
}

async function loadHistoricalData(companyId, month) {
    showLoader();
    try {
        showToast(`Fetching ${month} data from Google Sheets…`, 'info');
        const res = await sheetsApiRequest({ action: 'getDashboardData', companyId, month });
        if (res?.data && Array.isArray(res.data)) {
            // Merge with local state but only for the Data Sheet view
            if (companyId === 'company1') {
                AppState.company1Data = mergeOrders(AppState.company1Data, res.data);
            } else {
                AppState.company2Data = mergeOrders(AppState.company2Data, res.data);
            }
            showToast(`Loaded ${res.data.length} records from archive.`, 'success');
            await renderDataSheet();
        } else {
            showToast('No historical data found in Sheets.', 'info');
        }
    } catch (e) {
        console.error('Load historical failed:', e);
        showToast('Failed to load from Sheets', 'error');
    } finally {
        hideLoader();
    }
}

async function renderDataSheet() {
    const thead = document.getElementById('sheet-thead');
    const tbody = document.getElementById('sheet-tbody');
    const emptyMsg = document.getElementById('sheet-empty');
    const wrapper = document.querySelector('.sheet-wrapper');
    if (!thead || !tbody) return;
    
    const companyFilter = document.getElementById('sheet-company-filter').value;
    const monthFilter = document.getElementById('sheet-month-filter').value;
    
    const sheetTitle = document.getElementById('sheet-title');
    if (sheetTitle) {
        const dStr = getTodayISODate();
        if (monthFilter === 'all') {
            sheetTitle.innerHTML = `<span class="text-primary font-bold mr-2"><i class='bx bx-calendar'></i> ${dStr}</span> Viewing complete history`;
        } else if (monthFilter) {
            sheetTitle.innerHTML = `<span class="text-primary font-bold mr-2"><i class='bx bx-calendar'></i> ${dStr}</span> Viewing data for ${monthFilter}`;
        } else {
            sheetTitle.innerHTML = `<span class="text-primary font-bold mr-2"><i class='bx bx-calendar'></i> ${dStr}</span> Viewing current 30 days data`;
        }
    }
    
    // Determine which companies to show
    let accounts = [];
    let rawData = [];
    
    if (companyFilter === 'company1') {
        accounts = AppState.company1Accounts.map(a => ({ name: a, company: 'company1', label: getCompanyDisplayName('company1') }));
        rawData = [...AppState.company1Data];
    } else if (companyFilter === 'company2') {
        accounts = AppState.company2Accounts.map(a => ({ name: a, company: 'company2', label: getCompanyDisplayName('company2') }));
        rawData = [...AppState.company2Data];
    } else {
        AppState.company1Accounts.forEach(a => accounts.push({ name: a, company: 'company1', label: getCompanyDisplayName('company1') }));
        AppState.company2Accounts.forEach(a => accounts.push({ name: a, company: 'company2', label: getCompanyDisplayName('company2') }));
        rawData = [...AppState.company1Data, ...AppState.company2Data];
    }
    
    // Filter by month
    const currentMonth = getTodayISODate().substring(0, 7);
    if (monthFilter && monthFilter !== 'all') {
        rawData = rawData.filter(r => {
            const d = normalizeToISODate(r.date);
            return d && d.startsWith(monthFilter);
        });
    }
    
    // Collect unique dates, newest first
    const dateSet = new Set();
    rawData.forEach(r => {
        const d = normalizeToISODate(r.date);
        if (d) dateSet.add(d);
    });
    
    // Always include today's date in the data sheet if it matches the current month filter (and not viewing archive)
    const todayStr = getTodayISODate();
    if (monthFilter === currentMonth || monthFilter === 'all') {
        dateSet.add(todayStr);
    }
    
    const dates = [...dateSet].sort().reverse();
    
    if (accounts.length === 0 || dates.length === 0) {
        wrapper.classList.add('hidden');
        emptyMsg.classList.remove('hidden');
        
        // If a specific month is selected and no data locally, offer to load from Sheets
        if (monthFilter && monthFilter !== 'all' && monthFilter !== currentMonth) {
            const compId = companyFilter === 'all' ? 'company1' : companyFilter; // Default to comp1 if all
            emptyMsg.innerHTML = `
                <div class="p-8 text-center">
                    <i class='bx bx-history text-4xl text-muted mb-4'></i>
                    <p class="mb-4">No local data found for ${monthFilter}.</p>
                    <button class="btn btn-primary" onclick="loadHistoricalData('${compId}', '${monthFilter}')">
                        <i class='bx bx-cloud-download'></i> Load from Google Sheets Archive
                    </button>
                    <p class="text-xs text-muted mt-2">Firestore only keeps the last 30 days for speed.</p>
                </div>
            `;
        } else {
            emptyMsg.innerHTML = '<p class="p-8 text-center text-muted">No records found for the selected filter.</p>';
        }
        return;
    }
    wrapper.classList.remove('hidden');
    emptyMsg.classList.add('hidden');
    
    // Build lookup: { date -> { accountName -> { meesho, total, company } } }
    const lookup = {};
    rawData.forEach(r => {
        const d = normalizeToISODate(r.date);
        if (!d) return;
        if (!lookup[d]) lookup[d] = {};
        const key = r.accountName;
        if (!lookup[d][key]) lookup[d][key] = { meesho: 0, total: 0 };
        lookup[d][key].meesho += parseInt(r.meesho) || 0;
        lookup[d][key].total += parseInt(r.total) || 0;
    });
    
    // Load remarks (from backend first time, then cached)
    const savedRemarks = await loadRemarksFromBackend();
    
    const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    
    // === HEADER ===
    let headerRow1 = '<tr><th class="sheet-date-col">Date</th>';
    accounts.forEach((acc, idx) => {
        const compTag = companyFilter === 'all' ? `<span class="sheet-company-tag">${acc.label}</span>` : '';
        headerRow1 += `<th class="sheet-acct-group sheet-acct-border">${acc.name}${compTag}</th>`;
    });
    headerRow1 += '<th class="sheet-acct-group sheet-total-col">Total</th>';
    headerRow1 += '<th class="sheet-acct-group" style="min-width:140px;">Remarks</th>';
    headerRow1 += '</tr>';
    
    let headerRow2 = ''; // Removed second header row for headers
    thead.innerHTML = headerRow1 + headerRow2;
    
    // === BODY ===
    let bodyHtml = '';
    const grandTotals = { meesho: new Array(accounts.length).fill(0), total: 0 };
    
    dates.forEach(date => {
        const [y, m, d] = date.split('-');
        const dateObj = new Date(parseInt(y), parseInt(m)-1, parseInt(d));
        const dayName = dayNames[dateObj.getDay()];
        const isToday = date === todayStr;
        const dayChipClass = isToday ? 'sheet-day-chip sheet-today' : 'sheet-day-chip';
        const formattedDate = `${d}/${m}/${y}`;
        
        bodyHtml += '<tr>';
        bodyHtml += `<td class="sheet-date-cell">${formattedDate} <span class="${dayChipClass}">${dayName}</span></td>`;
        
        let rowTotal = 0;
        accounts.forEach((acc, idx) => {
            const cell = lookup[date]?.[acc.name] || { meesho: 0, total: 0 };
            const meeshoVal = cell.meesho || '';
            rowTotal += cell.total;
            grandTotals.meesho[idx] += cell.meesho;
            
            bodyHtml += `<td class="sheet-editable sheet-acct-border"><input type="number" class="sheet-cell-input" value="${meeshoVal || ''}" data-date="${date}" data-account="${acc.name}" data-company="${acc.company}" data-field="meesho" min="0" placeholder="-"></td>`;
        });
        grandTotals.total += rowTotal;
        
        bodyHtml += `<td class="sheet-total-cell">${rowTotal}</td>`;
        
        const remarkVal = savedRemarks[date] || '';
        bodyHtml += `<td class="sheet-editable"><input type="text" class="sheet-cell-input sheet-remark-input" value="${remarkVal.replace(/"/g, '&quot;')}" data-date="${date}" data-field="remark" placeholder="Add note..."></td>`;
        bodyHtml += '</tr>';
    });
    
    // Grand total row
    bodyHtml += '<tr class="sheet-grand-row">';
    bodyHtml += '<td class="sheet-date-cell">TOTAL</td>';
    accounts.forEach((_, idx) => {
        bodyHtml += `<td class="sheet-acct-border">${grandTotals.meesho[idx]}</td>`;
    });
    bodyHtml += `<td class="sheet-total-cell">${grandTotals.total}</td>`;
    bodyHtml += '<td></td>';
    bodyHtml += '</tr>';
    
    tbody.innerHTML = bodyHtml;
    
    // === EVENT: Save remarks (buffered to Firebase) ===
    tbody.querySelectorAll('input[data-field="remark"]').forEach(inp => {
        inp.addEventListener('change', () => {
            const date = inp.dataset.date;
            const val = inp.value;
            
            // Update cache immediately
            if (!_remarkCache) _remarkCache = {};
            _remarkCache[date] = val;
            localStorage.setItem('sheetRemarks', JSON.stringify(_remarkCache));
            
            // Buffer the write to Firebase (flushes after APP_CONFIG.writeBufferMs)
            FirebaseService.bufferWrite(`remark_${date}`, () =>
                FirebaseService.saveRemark(date, val)
            );
        });
    });
    
    // === EVENT: Save order cell edits (buffered to Firebase) ===
    tbody.querySelectorAll('input[data-field="meesho"]').forEach(inp => {
        inp.addEventListener('change', () => {
            const { date, account, company, field } = inp.dataset;
            const value = parseInt(inp.value) || 0;
            
            // Update local state immediately
            const stateData = company === 'company1' ? AppState.company1Data : AppState.company2Data;
            const row = stateData.find(r => normalizeToISODate(r.date) === date && r.accountName === account);
            if (row) {
                row[field] = value;
                row.total = parseInt(row.meesho) || 0;
            }
            
            // Recalculate totals in UI immediately
            recalcSheetRowTotal(inp);
            
            // Buffer the write to Firebase
            FirebaseService.bufferWrite(`order_${date}_${account}_${field}`, () =>
                FirebaseService.updateOrder(date, account, field, value, company)
            );
        });
    });
}

// Recalculate a row's total cell when a value changes
function recalcSheetRowTotal(inputEl) {
    const tr = inputEl.closest('tr');
    if (!tr) return;
    const inputs = tr.querySelectorAll('input[data-field="meesho"]');
    let total = 0;
    inputs.forEach(inp => total += parseInt(inp.value) || 0);
    const totalCell = tr.querySelector('.sheet-total-cell');
    if (totalCell) totalCell.textContent = total;
    
    // Also recalculate grand total row
    const tbody = document.getElementById('sheet-tbody');
    if (!tbody) return;
    const grandRow = tbody.querySelector('.sheet-grand-row');
    if (!grandRow) return;
    const allRows = tbody.querySelectorAll('tr:not(.sheet-grand-row)');
    let grandTotal = 0;
    allRows.forEach(row => {
        const tc = row.querySelector('.sheet-total-cell');
        if (tc) grandTotal += parseInt(tc.textContent) || 0;
    });
    const grandTotalCell = grandRow.querySelector('.sheet-total-cell');
    if (grandTotalCell) grandTotalCell.textContent = grandTotal;
}

function exportSheetCSV() {
    const table = document.getElementById('sheet-table');
    if (!table) return;
    
    const rows = [];
    const headerCells = table.querySelectorAll('thead tr');
    const accounts = [];
    headerCells[0]?.querySelectorAll('th.sheet-acct-group').forEach(th => {
        const txt = th.textContent.trim();
        if (txt !== 'Total' && txt !== 'Remarks') accounts.push(txt);
    });
    
    const headerRow = ['Date'];
    accounts.forEach(name => {
        headerRow.push(`${name} - Meesho`);
    });
    headerRow.push('Total', 'Remarks');
    rows.push(headerRow);
    
    table.querySelectorAll('tbody tr:not(.sheet-grand-row)').forEach(tr => {
        const cells = tr.querySelectorAll('td');
        const row = [];
        cells.forEach(td => {
            const input = td.querySelector('input');
            row.push(input ? input.value : td.textContent.trim());
        });
        rows.push(row);
    });
    
    if (rows.length <= 1) { showToast('No data to export', 'error'); return; }
    
    const csv = rows.map(r => r.map(c => `"${c}"`).join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `krimaa_data_sheet_${getTodayISODate()}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    showToast('Sheet exported!', 'success');
}

// Initialize sheet listeners after DOM ready
document.addEventListener('DOMContentLoaded', () => {
    initDataSheetListeners();
});

// ===== MONEY MANAGEMENT =====
async function renderMoneyManagement() {
    const tbody = document.getElementById('money-management-tbody');
    if (!tbody) return;
    
    // Combine accounts from both companies + their details in their set order
    const mgmtAccounts = [];
    
    // Determine which company to show first based on current view for consistency
    const sortedCompanies = AppState.currentCompany === 'company2' ? ['company2', 'company1'] : ['company1', 'company2'];
    
    sortedCompanies.forEach(compId => {
        const accounts = compId === 'company1' ? AppState.company1Accounts : AppState.company2Accounts;
        const detailsList = compId === 'company1' ? AppState.company1Details : AppState.company2Details;
        const companyLabel = compId === 'company1' ? 'Company 1' : 'Company 2';
        
        (accounts || []).forEach(accName => {
            const details = (detailsList || []).find(d => d.name === accName) || {};
            mgmtAccounts.push({
                name: accName,
                company: companyLabel,
                companyId: compId,
                money: parseInt(details.money) || 0,
                expense: parseInt(details.expense) || 0,
                moneyDate: details.moneyDate || ''
            });
        });
    });
    
    // Set default date to today
    const dateInput = document.getElementById('money-date');
    if (dateInput && !dateInput.value) {
        dateInput.value = getTodayISODate();
        dateInput.classList.remove('hidden');
    }
    
    if (mgmtAccounts.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-center p-4 text-muted">No accounts found.</td></tr>';
        document.getElementById('money-grand-money').textContent = '0';
        document.getElementById('money-grand-expense').textContent = '0';
        document.getElementById('money-grand-total').textContent = '0';
        return;
    }
    
    tbody.innerHTML = mgmtAccounts.map((acc, idx) => `
        <tr class="money-row" data-account="${acc.name.replace(/"/g, '&quot;')}" data-company-id="${acc.companyId}">
            <td class="drag-handle-cell"><i class='bx bx-menu money-drag-handle' style="cursor: grab; color: #a0aec0;"></i></td>
            <td class="position-number">${idx + 1}</td>
            <td><span class="badge ${acc.companyId === 'company1' ? 'badge-a' : 'badge-b'}">${acc.company}</span></td>
            <td class="font-medium">${acc.name}</td>
            <td><input type="number" class="inp-money sheet-cell-input" min="0" value="${acc.money || ''}" data-index="${idx}" placeholder="0"></td>
            <td><input type="number" class="inp-expense sheet-cell-input" min="0" value="${acc.expense || ''}" data-index="${idx}" placeholder="0"></td>
            <td><span class="row-total font-bold" id="money-total-${idx}">${acc.money - acc.expense}</span></td>
        </tr>
    `).join('');
    
    const updateGrandTotals = () => {
        let totalMoney = 0;
        let totalExpense = 0;
        tbody.querySelectorAll('.money-row').forEach((row, idx) => {
            const m = parseInt(row.querySelector('.inp-money').value) || 0;
            const e = parseInt(row.querySelector('.inp-expense').value) || 0;
            row.querySelector('.row-total').textContent = m - e;
            totalMoney += m;
            totalExpense += e;
        });
        document.getElementById('money-grand-money').textContent = totalMoney;
        document.getElementById('money-grand-expense').textContent = totalExpense;
        document.getElementById('money-grand-total').textContent = totalMoney - totalExpense;
    };
    
    tbody.querySelectorAll('.inp-money, .inp-expense').forEach(inp => {
        inp.addEventListener('input', () => {
             updateGrandTotals();
             
             // Unhide save button
             const saveBtn = document.getElementById('money-save-btn');
             if (saveBtn) saveBtn.classList.remove('hidden');
             
             const row = inp.closest('.money-row');
             const accName = row.dataset.account;
             const companyId = row.dataset.companyId;
             const money = parseInt(row.querySelector('.inp-money').value) || 0;
             const expense = parseInt(row.querySelector('.inp-expense').value) || 0;
             const date = document.getElementById('money-date').value || getTodayISODate();
             
             // Also update local state
             const detailsList = companyId === 'company1' ? AppState.company1Details : AppState.company2Details;
             if (detailsList) {
                 const det = detailsList.find(d => d.name === accName);
                 if (det) { det.money = money; det.expense = expense; det.moneyDate = date; }
             }
             
             FirebaseService.bufferWrite(`money_${companyId}_${accName}`, () => FirebaseService.updateMoney(accName, companyId, money, expense, date));
        });
    });
    
    updateGrandTotals();
    
    // Sortable for Money Management
    if (tbody._sortableInstance) tbody._sortableInstance.destroy();
    tbody._sortableInstance = Sortable.create(tbody, {
        handle: '.money-drag-handle',
        animation: 150,
        ghostClass: 'sortable-ghost',
        onEnd: function() {
            // Need to save new order for company 1 and company 2 separately
            const newOrder1 = [];
            const newOrder2 = [];
            Array.from(tbody.children).forEach((row, idx) => {
                row.querySelector('.position-number').textContent = idx + 1;
                const companyId = row.dataset.companyId;
                const accountName = row.dataset.account;
                if (companyId === 'company1') newOrder1.push(accountName);
                if (companyId === 'company2') newOrder2.push(accountName);
            });
            // Update UI/local state visually and trigger sheet updates
            AppState.company1Accounts = newOrder1;
            AppState.company2Accounts = newOrder2;
            
            showToast('Saving new order...', 'info');
            Promise.all([
                saveAccountOrderToSheet(newOrder1, 'company1'),
                saveAccountOrderToSheet(newOrder2, 'company2')
            ]).then(() => showToast('Order saved!', 'success')).catch(() => showToast('Failed to save order', 'error'));
        }
    });
}

function buildCurrentMoneySnapshotRows() {
    const rows = [];

    const addCompanyRows = (companyId, companyName, accounts, detailsList) => {
        (accounts || []).forEach(accountName => {
            const details = (detailsList || []).find(d => d.name === accountName) || {};
            const money = parseInt(details.money) || 0;
            const expense = parseInt(details.expense) || 0;
            rows.push({
                companyId,
                companyName,
                accountName,
                money,
                expense,
                balance: money - expense
            });
        });
    };

    addCompanyRows('company1', getCompanyDisplayName('company1'), AppState.company1Accounts, AppState.company1Details);
    addCompanyRows('company2', getCompanyDisplayName('company2'), AppState.company2Accounts, AppState.company2Details);

    return rows;
}

async function loadMoneyBackups(force = false) {
    if (!force && Array.isArray(AppState.moneyBackups) && AppState.moneyBackups.length > 0) {
        return AppState.moneyBackups;
    }

    const res = await apiRequest({ action: 'getMoneyBackups' });
    AppState.moneyBackups = Array.isArray(res?.data) ? res.data : [];
    if (!AppState.selectedMoneyBackupId && AppState.moneyBackups.length > 0) {
        AppState.selectedMoneyBackupId = AppState.moneyBackups[0].id;
    }
    return AppState.moneyBackups;
}

function getSelectedMoneyBackup() {
    if (!Array.isArray(AppState.moneyBackups) || AppState.moneyBackups.length === 0) return null;
    const selected = AppState.moneyBackups.find(b => b.id === AppState.selectedMoneyBackupId);
    return selected || AppState.moneyBackups[0];
}

async function renderMoneyBackupPage(forceReload = false) {
    const select = document.getElementById('money-backup-select');
    const tbody = document.getElementById('money-backup-tbody');
    const empty = document.getElementById('money-backup-empty');
    if (!select || !tbody || !empty) return;

    try {
        await loadMoneyBackups(forceReload);
    } catch (e) {
        showToast('Failed to load money backups', 'error');
        return;
    }

    if (!Array.isArray(AppState.moneyBackups) || AppState.moneyBackups.length === 0) {
        select.innerHTML = '<option value="">No Backup Found</option>';
        tbody.innerHTML = '';
        empty.classList.remove('hidden');
        document.getElementById('money-backup-total-money').textContent = '0';
        document.getElementById('money-backup-total-expense').textContent = '0';
        document.getElementById('money-backup-total-balance').textContent = '0';
        return;
    }

    select.innerHTML = AppState.moneyBackups.map(backup => {
        const created = backup.createdAt?.toDate ? backup.createdAt.toDate().toLocaleString() : '';
        const label = created ? `${backup.backupDate} (${created})` : backup.backupDate;
        const selected = backup.id === AppState.selectedMoneyBackupId ? 'selected' : '';
        return `<option value="${backup.id}" ${selected}>${label}</option>`;
    }).join('');

    const activeBackup = getSelectedMoneyBackup();
    const rows = Array.isArray(activeBackup?.rows) ? activeBackup.rows : [];

    tbody.innerHTML = rows.map(r => `
        <tr>
            <td>${activeBackup.backupDate || '-'}</td>
            <td>${r.companyName || r.companyId || '-'}</td>
            <td class="font-medium">${r.accountName || '-'}</td>
            <td class="text-right">${parseInt(r.money) || 0}</td>
            <td class="text-right">${parseInt(r.expense) || 0}</td>
            <td class="text-right font-bold">${parseInt(r.balance) || 0}</td>
        </tr>
    `).join('');

    let totalMoney = 0;
    let totalExpense = 0;
    rows.forEach(r => {
        totalMoney += parseInt(r.money) || 0;
        totalExpense += parseInt(r.expense) || 0;
    });

    document.getElementById('money-backup-total-money').textContent = totalMoney;
    document.getElementById('money-backup-total-expense').textContent = totalExpense;
    document.getElementById('money-backup-total-balance').textContent = totalMoney - totalExpense;
    empty.classList.toggle('hidden', rows.length > 0);
}

function exportMoneyBackupToExcel() {
    const backup = getSelectedMoneyBackup();
    if (!backup || !Array.isArray(backup.rows) || backup.rows.length === 0) {
        showToast('No backup rows to export', 'error');
        return;
    }

    const rows = [['Backup Date', 'Company', 'Account Name', 'Money', 'Expense', 'Balance']];
    backup.rows.forEach(r => {
        rows.push([
            backup.backupDate || '',
            r.companyName || r.companyId || '',
            r.accountName || '',
            parseInt(r.money) || 0,
            parseInt(r.expense) || 0,
            parseInt(r.balance) || 0
        ]);
    });

    const tsv = rows.map(row => row.map(cell => String(cell).replace(/\t/g, ' ')).join('\t')).join('\n');
    const blob = new Blob([tsv], { type: 'application/vnd.ms-excel;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `money_backup_${backup.backupDate || getTodayISODate()}.xls`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    showToast('Money backup exported', 'success');
}

// Reset ALL money data
document.addEventListener('DOMContentLoaded', () => {
    const openBackupBtn = document.getElementById('money-backup-open-btn');
    if (openBackupBtn) {
        openBackupBtn.addEventListener('click', () => navigateTo('money-backup'));
    }

    const resetBtn = document.getElementById('money-reset-btn');
    if (resetBtn) {
        resetBtn.addEventListener('click', async () => {
            if(confirm('Are you sure you want to clear all money and expense data across all companies?')) {
                showToast('Resetting data...', 'info');
                const date = document.getElementById('money-date').value || getTodayISODate();
                try {
                    const snapshotRows = buildCurrentMoneySnapshotRows();
                    const backupRes = await apiRequest({
                        action: 'createMoneyBackup',
                        date,
                        rows: snapshotRows,
                        reason: 'pre_reset'
                    });
                    if (backupRes?.success === false) {
                        throw new Error(backupRes.message || 'Backup failed');
                    }

                    // Also store the same backup in Google Sheets (Money_Backups tab).
                    try {
                        await sheetsApiRequest({
                            action: 'saveMoneyBackup',
                            date,
                            rows: snapshotRows,
                            reason: 'pre_reset'
                        });
                    } catch (sheetErr) {
                        console.warn('Money backup sheet write failed (non-blocking):', sheetErr);
                    }

                    await apiRequest({ action: 'resetAllMoney', date });
                    // clear local stores
                    ['company1Details', 'company2Details'].forEach(detKey => {
                        (AppState[detKey] || []).forEach(d => { d.money = 0; d.expense = 0; d.moneyDate = date; });
                    });
                    AppState.moneyBackups = [];
                    await loadMoneyBackups(true);
                    renderMoneyManagement();
                    showToast('All data reset successfully. Backup saved.', 'success');
                } catch(e) {
                    showToast(`Failed to reset: ${e.message || 'Unknown error'}`, 'error');
                }
            }
        });
    }
    
    const moneySaveBtn = document.getElementById('money-save-btn');
    if (moneySaveBtn) {
        // Clone to remove old bound listeners
        const newMoneySaveBtn = moneySaveBtn.cloneNode(true);
        moneySaveBtn.parentNode.replaceChild(newMoneySaveBtn, moneySaveBtn);
        newMoneySaveBtn.addEventListener('click', () => {
            FirebaseService.flushWrites();
            newMoneySaveBtn.classList.add('hidden');
            showToast('Changes saved instantly!', 'success');
        });
    }

    const backupSelect = document.getElementById('money-backup-select');
    if (backupSelect) {
        backupSelect.addEventListener('change', () => {
            AppState.selectedMoneyBackupId = backupSelect.value;
            renderMoneyBackupPage();
        });
    }

    const backupRefreshBtn = document.getElementById('money-backup-refresh-btn');
    if (backupRefreshBtn) {
        backupRefreshBtn.addEventListener('click', () => renderMoneyBackupPage(true));
    }

    const backupDeleteBtn = document.getElementById('money-backup-delete-btn');
    if (backupDeleteBtn) {
        backupDeleteBtn.addEventListener('click', async () => {
            const activeBackup = getSelectedMoneyBackup();
            if (!activeBackup || !activeBackup.id) {
                showToast('Please select a backup to delete', 'info');
                return;
            }
            if (confirm(`Are you sure you want to delete this backup from ${activeBackup.backupDate}? This action cannot be undone.`)) {
                showProgressToast('Deleting backup...');
                try {
                    const res = await apiRequest({ action: 'deleteMoneyBackup', backupId: activeBackup.id });
                    if (res?.success !== false) {
                        showToast('Backup deleted successfully', 'success');
                        AppState.selectedMoneyBackupId = '';
                        renderMoneyBackupPage(true);
                    } else {
                        showToast(res.message || 'Failed to delete backup', 'error');
                    }
                } catch (e) {
                    showToast('Failed to delete backup', 'error');
                }
            }
        });
    }

    const backupExportBtn = document.getElementById('money-backup-export-btn');
    if (backupExportBtn) {
        backupExportBtn.addEventListener('click', exportMoneyBackupToExcel);
    }
});

/* ===== KARIGAR MANAGEMENT ===== */
async function loadKarigarData() {
    try {
        const [kRes, tRes, pRes] = await Promise.all([
            FirebaseService.getKarigars(),
            FirebaseService.getKarigarTransactions(),
            FirebaseService.getDesignPrices()
        ]);
        
        if (kRes.success) AppState.karigars = kRes.data || [];
        if (tRes.success) AppState.karigarTransactions = (tRes.data || []).sort((a,b) => new Date(b.date) - new Date(a.date));
        if (pRes.success) AppState.designPrices = pRes.data || {};
    } catch(e) {
        console.error("Failed to load karigar data", e);
    }
}

async function renderKarigarPage() {
    await loadKarigarData();
    renderKarigarGrid();
    setupKarigarListeners();
}

function calculateKarigarBalance(karigarName) {
    if (!karigarName) return { totalJama: 0, totalUpad: 0, balance: 0, lastTx: null };
    const searchName = karigarName.trim().toLowerCase();
    const tx = AppState.karigarTransactions.filter(t => (t.karigarName || '').trim().toLowerCase() === searchName);
    let totalJama = 0;
    let totalUpad = 0;
    tx.forEach(t => {
        if (t.type === 'jama') {
            totalJama += (parseFloat(t.total) || 0);
            totalUpad += (parseFloat(t.upadAmount) || 0);
        }
        if (t.type === 'upad') totalUpad += (parseFloat(t.amount) || 0);
    });
    return { totalJama, totalUpad, balance: totalJama - totalUpad, lastTx: tx[0] };
}

function renderKarigarGrid() {
    const grid = document.getElementById('karigar-grid');
    const searchInput = document.getElementById('karigar-search-input');
    const query = (searchInput.value || '').trim().toLowerCase();
    const btnCreate = document.getElementById('btn-create-karigar');
    
    if (!grid) return;
    grid.innerHTML = '';
    
    let filtered = AppState.karigars;
    if (query) {
        filtered = AppState.karigars.filter(k => (k.name || '').toLowerCase().includes(query));
    }
    
    const exactMatch = AppState.karigars.some(k => (k.name || '').toLowerCase() === query);
    if (query && !exactMatch) {
        btnCreate.style.display = 'block';
    } else {
        btnCreate.style.display = 'none';
    }
    
    if (filtered.length === 0) {
        grid.innerHTML = `<div class="card p-4 text-center text-muted" style="grid-column: 1 / -1;"><i class="bx bx-user-x text-4xl mb-2"></i><p>No karigars found.</p></div>`;
        return;
    }
    
    // Check reset logic: Show "Monthly Reset Required" if it hasn't been reset this month.
    // It will show on the 1st, 2nd, etc. until the user actually clicks it and finishes the reset.
    const currentMonth = getTodayISODate().substring(0, 7);
    const lastResetMonth = localStorage.getItem('karigar_last_reset_month') || '';
    const resetBtn = document.getElementById('btn-reset-karigar');
    
    if (resetBtn) {
        if (lastResetMonth !== currentMonth) {
            resetBtn.style.display = 'block';
            resetBtn.classList.remove('btn-outline-primary', 'text-primary');
            resetBtn.classList.add('btn-danger', 'text-white');
            resetBtn.innerHTML = "<i class='bx bx-error-circle'></i> Monthly Reset Required";
            
            // Rebind click
            const newResetBtn = resetBtn.cloneNode(true);
            resetBtn.parentNode.replaceChild(newResetBtn, resetBtn);
            newResetBtn.addEventListener('click', confirmKarigarMonthlyReset);
        } else {
            resetBtn.style.display = 'none'; // hide completely if done
        }
    }
    
    let html = '';
    let globalBalanceSum = 0;
    
    filtered.forEach(k => {
        const stats = calculateKarigarBalance(k.name);
        globalBalanceSum += stats.balance;
        
        const lastActivity = stats.lastTx ? `<span class="text-xs text-muted">Last active: ${formatISODateForDisplay(stats.lastTx.date)}</span>` : '<span class="text-xs text-muted">No activity yet</span>';
        
        const isNegative = stats.balance < 0;
        const balColor = isNegative ? 'var(--danger)' : 'var(--success)';
        
        html += `
            <div class="card" style="display:flex; flex-direction:column; gap: 1rem;">
                <div class="flex-between">
                    <div>
                        <h3 class="font-bold text-lg">${k.name}</h3>
                        ${lastActivity}
                    </div>
                    <div style="background: ${isNegative ? '#fee2e2' : '#dcfce7'}; color: ${balColor}; padding: 0.5rem 0.75rem; border-radius: 8px; font-weight: bold; text-align: right;">
                        <span style="font-size: 0.7rem; display:block; text-transform:uppercase; margin-bottom: -2px;">Balance</span>
                        ₹${Math.abs(stats.balance).toFixed(2)}${isNegative ? ' Due' : ' Clear'}
                    </div>
                </div>
                
                <div style="display: flex; gap: 1rem; border-top: 1px solid var(--border-color); border-bottom: 1px solid var(--border-color); padding: 0.75rem 0; margin: 0 -0.5rem; justify-content: space-around; text-align: center;">
                    <div>
                        <span class="text-xs text-muted" style="display:block;">Total Maal (Jama)</span>
                        <strong class="text-success">₹${stats.totalJama.toFixed(2)}</strong>
                    </div>
                    <div style="width: 1px; background: var(--border-color);"></div>
                    <div>
                        <span class="text-xs text-muted" style="display:block;">Total Borrow (Upad)</span>
                        <strong class="text-danger">₹${stats.totalUpad.toFixed(2)}</strong>
                    </div>
                </div>

                <div class="flex gap-2 w-100" style="margin-top: auto; display:grid; grid-template-columns: 1fr 1fr; gap:0.5rem;">
                    <button class="btn btn-outline btn-karigar-upad" data-name="${k.name.replace(/"/g, '&quot;')}">
                        <i class='bx bx-minus-circle'></i> Borrow (Upad)
                    </button>
                    <button class="btn btn-primary btn-karigar-jama" data-name="${k.name.replace(/"/g, '&quot;')}">
                        <i class='bx bx-plus-circle'></i> Maal (Jama)
                    </button>
                    <button class="btn btn-outline btn-sm btn-karigar-history" data-name="${k.name.replace(/"/g, '&quot;')}" style="grid-column: span 2;">
                        <i class='bx bx-history'></i> View Detailed History
                    </button>
                </div>
            </div>
        `;
    });
    grid.innerHTML = html;
    
    const obEl = document.getElementById('karigar-overall-balance');
    if (obEl) {
        obEl.textContent = '₹' + Math.abs(globalBalanceSum).toFixed(2) + (globalBalanceSum < 0 ? ' Due' : ' Clear');
        obEl.className = globalBalanceSum < 0 ? 'text-lg text-danger' : 'text-lg text-success';
    }
    
    // Bind buttons
    grid.querySelectorAll('.btn-karigar-upad').forEach(btn => {
        btn.addEventListener('click', (e) => openKarigarUpadModal(e.target.closest('.btn-karigar-upad').dataset.name));
    });
    grid.querySelectorAll('.btn-karigar-jama').forEach(btn => {
        btn.addEventListener('click', (e) => openKarigarJamaModal(e.target.closest('.btn-karigar-jama').dataset.name));
    });
    grid.querySelectorAll('.btn-karigar-history').forEach(btn => {
        btn.addEventListener('click', (e) => openKarigarHistoryModal(e.target.closest('.btn-karigar-history').dataset.name));
    });
}

function setupKarigarListeners() {
    const searchInput = document.getElementById('karigar-search-input');
    if (searchInput && !searchInput.dataset.bound) {
        searchInput.addEventListener('input', renderKarigarGrid);
        searchInput.dataset.bound = 'true';
    }
    
    const createBtn = document.getElementById('btn-create-karigar');
    if (createBtn && !createBtn.dataset.bound) {
        createBtn.addEventListener('click', () => {
            document.getElementById('karigar-create-name').value = document.getElementById('karigar-search-input').value.trim();
            document.getElementById('karigar-create-modal').classList.add('show');
        });
        createBtn.dataset.bound = 'true';
    }
    
    document.getElementById('close-karigar-create-modal')?.addEventListener('click', () => {
        document.getElementById('karigar-create-modal').classList.remove('show');
    });
    document.getElementById('close-karigar-jama-modal')?.addEventListener('click', () => {
        document.getElementById('karigar-jama-modal').classList.remove('show');
    });
    document.getElementById('close-karigar-upad-modal')?.addEventListener('click', () => {
        document.getElementById('karigar-upad-modal').classList.remove('show');
    });
    document.getElementById('close-karigar-history-modal')?.addEventListener('click', () => {
        document.getElementById('karigar-history-modal').classList.remove('show');
    });
    
    // Auto-fill price & total calculate for Jama
    const jamaDesign = document.getElementById('karigar-jama-design');
    const jamaPrice = document.getElementById('karigar-jama-price');
    const jamaPic = document.getElementById('karigar-jama-pic');
    const jamaTotalDisplay = document.getElementById('karigar-jama-total-display');
    
    const jamaSize = document.getElementById('karigar-jama-size');
    if (jamaSize && !jamaSize.dataset.bound) {
        jamaSize.addEventListener('input', () => {
            jamaSize.value = jamaSize.value.toUpperCase();
        });
        jamaSize.dataset.bound = 'true';
    }

    if (jamaDesign && !jamaDesign.dataset.bound) {
        jamaDesign.addEventListener('input', () => {
            const up = jamaDesign.value.trim().toUpperCase();
            if (AppState.designPrices[up]) {
                const oldVal = jamaPrice.value;
                jamaPrice.value = AppState.designPrices[up];
                
                // Show AI animation if it auto-filled a different price
                if (oldVal != jamaPrice.value) {
                    jamaPrice.classList.add('rainbow-autofill');
                    setTimeout(() => jamaPrice.classList.remove('rainbow-autofill'), 2500);
                }
            }
            updateJamaTotal();
        });
        jamaPrice.addEventListener('input', updateJamaTotal);
        jamaPic.addEventListener('input', updateJamaTotal);
        function updateJamaTotal() {
            const p = parseFloat(jamaPrice.value) || 0;
            const q = parseInt(jamaPic.value) || 0;
            jamaTotalDisplay.textContent = '₹' + (p * q).toFixed(2);
        }
        jamaDesign.dataset.bound = 'true';
    }

    // Upad Live Balance Update
    const upadAmount = document.getElementById('karigar-upad-amount');
    const upadBalanceDisplay = document.getElementById('karigar-upad-balance-display');
    if (upadAmount && !upadAmount.dataset.bound) {
        upadAmount.addEventListener('input', () => {
            const karigarName = document.getElementById('karigar-upad-id').value;
            const stats = calculateKarigarBalance(karigarName);
            const borrow = parseFloat(upadAmount.value) || 0;
            const newBal = stats.balance - borrow;
            
            upadBalanceDisplay.textContent = '₹' + newBal.toFixed(2);
            if (newBal < 0) {
                upadBalanceDisplay.classList.remove('text-primary', 'text-success');
                upadBalanceDisplay.classList.add('text-danger');
            } else {
                upadBalanceDisplay.classList.remove('text-danger');
                upadBalanceDisplay.classList.add('text-primary');
            }
        });
        upadAmount.dataset.bound = 'true';
    }
    
    // Form submits
    const createForm = document.getElementById('karigar-create-form');
    if (createForm && !createForm.dataset.bound) {
        createForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const name = document.getElementById('karigar-create-name').value.trim();
            if (!name) return;
            showLoader();
            try {
                await FirebaseService.addKarigar(name);
                document.getElementById('karigar-search-input').value = '';
                document.getElementById('karigar-create-modal').classList.remove('show');
                showToast("Karigar created successfully!", "success");
                await renderKarigarPage();
            } catch (err) { showToast("Failed to create", "error"); }
            finally { hideLoader(); }
        });
        createForm.dataset.bound = 'true';
    }
    
    const jamaForm = document.getElementById('karigar-jama-form');
    if (jamaForm && !jamaForm.dataset.bound) {
        jamaForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const karigarName = document.getElementById('karigar-jama-id').value;
            const date = document.getElementById('karigar-jama-date').value;
            const designName = document.getElementById('karigar-jama-design').value;
            const size = document.getElementById('karigar-jama-size').value;
            const pic = document.getElementById('karigar-jama-pic').value;
            const price = document.getElementById('karigar-jama-price').value;
            const upadAmount = document.getElementById('karigar-jama-upad').value || 0;
            
            showLoader();
            try {
                await FirebaseService.addKarigarJama({ 
                    karigarName, date, designName, size, pic, price, upadAmount, 
                    companyId: AppState.currentCompany,
                    addedBy: AppState.currentUser?.role || 'admin'
                });
                document.getElementById('karigar-jama-modal').classList.remove('show');
                showToast("Maal (Jama) added successfully!", "success");
                
                // Update local price cache immediately
                if (designName && price) {
                    AppState.designPrices[designName.toString().trim().toUpperCase()] = parseFloat(price);
                }
                
                await renderKarigarPage();
            } catch (err) { showToast("Failed to add", "error"); }
            finally { hideLoader(); }
        });
        jamaForm.dataset.bound = 'true';
    }
    
    const upadForm = document.getElementById('karigar-upad-form');
    if (upadForm && !upadForm.dataset.bound) {
        upadForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const karigarName = document.getElementById('karigar-upad-id').value;
            const date = document.getElementById('karigar-upad-date').value;
            const amount = document.getElementById('karigar-upad-amount').value;
            
            showLoader();
            try {
                await FirebaseService.addKarigarUpad({ 
                    karigarName, date, amount, 
                    companyId: AppState.currentCompany,
                    addedBy: AppState.currentUser?.role || 'admin'
                });
                document.getElementById('karigar-upad-modal').classList.remove('show');
                showToast("Borrow (Upad) added successfully!", "success");
                await renderKarigarPage();
            } catch (err) { showToast("Failed to add", "error"); }
            finally { hideLoader(); }
        });
        upadForm.dataset.bound = 'true';
    }
}

function openKarigarJamaModal(name) {
    document.getElementById('karigar-jama-id').value = name;
    document.getElementById('karigar-jama-name-display').textContent = name;
    document.getElementById('karigar-jama-date').value = getTodayISODate();
    document.getElementById('karigar-jama-design').value = '';
    document.getElementById('karigar-jama-size').value = '';
    document.getElementById('karigar-jama-pic').value = '';
    document.getElementById('karigar-jama-price').value = '';
    document.getElementById('karigar-jama-upad').value = '';
    document.getElementById('karigar-jama-total-display').textContent = '0';
    document.getElementById('karigar-jama-modal').classList.add('show');
}

function openKarigarUpadModal(name) {
    const stats = calculateKarigarBalance(name);
    document.getElementById('karigar-upad-id').value = name;
    document.getElementById('karigar-upad-name-display').textContent = name;
    document.getElementById('karigar-upad-date').value = getTodayISODate();
    document.getElementById('karigar-upad-amount').value = '';
    
    // Set summary displays
    document.getElementById('karigar-upad-jama-display').textContent = '₹' + stats.totalJama.toFixed(2);
    const balanceDisplay = document.getElementById('karigar-upad-balance-display');
    balanceDisplay.textContent = '₹' + stats.balance.toFixed(2);
    
    if (stats.balance < 0) {
        balanceDisplay.classList.remove('text-primary', 'text-success');
        balanceDisplay.classList.add('text-danger');
    } else {
        balanceDisplay.classList.remove('text-danger');
        balanceDisplay.classList.add('text-primary');
    }
    
    document.getElementById('karigar-upad-modal').classList.add('show');
}

function openKarigarHistoryModal(name) {
    const txs = AppState.karigarTransactions.filter(t => t.karigarName === name);
    document.getElementById('karigar-history-name-display').textContent = name;
    const tbody = document.getElementById('karigar-history-tbody');
    tbody.innerHTML = '';
    
    let totalJama = 0;
    let totalUpad = 0;
    
    if (txs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-center p-4 text-muted">No transactions recorded for this karigar.</td></tr>';
    } else {
        txs.forEach(t => {
            const isJama = t.type === 'jama';
            const jamaVal = isJama ? parseFloat(t.total || 0) : 0;
            const upadVal = isJama ? parseFloat(t.upadAmount || 0) : parseFloat(t.amount || 0);
            
            totalJama += jamaVal;
            totalUpad += upadVal;
            
            let companyTag = '';
            if (t.companyId === 'company1') companyTag = `<span class="badge badge-primary badge-sm" style="font-size: 0.6rem; padding: 2px 4px;">C1</span>`;
            if (t.companyId === 'company2') companyTag = `<span class="badge badge-success badge-sm" style="font-size: 0.6rem; padding: 2px 4px;">C2</span>`;
            
            let addedByTag = '';
            if (t.addedBy === 'admin') addedByTag = `<span class="text-xs text-muted mt-1 block"><i class='bx bx-user'></i> Admin</span>`;
            if (t.addedBy === 'order') addedByTag = `<span class="text-xs text-muted mt-1 block"><i class='bx bx-user'></i> Order</span>`;
            
            const row = `
                <tr style="border-bottom: 1px solid var(--border-light); ${!isJama ? 'background: #fffcfc;' : ''}">
                    <td class="p-2 text-sm">${formatISODateForDisplay(t.date)} <div class="mt-1">${companyTag}</div></td>
                    <td class="p-2 text-sm font-medium">${isJama ? (t.designName || 'Maal') : '<span class="text-danger font-bold">Direct Borrow (Upad)</span>'} ${addedByTag}</td>
                    <td class="p-2 text-sm text-center">${t.size || '-'}</td>
                    <td class="p-2 text-sm text-right font-bold">${t.pic || '-'}</td>
                    <td class="p-2 text-sm text-right font-mono">${t.price ? '₹'+parseFloat(t.price).toFixed(2) : '-'}</td>
                    <td class="p-2 text-sm text-right text-success font-bold">${isJama ? '₹'+jamaVal.toFixed(2) : '-'}</td>
                    <td class="p-2 text-sm text-right text-danger font-bold">${upadVal > 0 ? '₹'+upadVal.toFixed(2) : '-'}</td>
                    <td class="p-2 text-sm text-center">
                        <button onclick="deleteKarigarHistory('${t.id}', '${name}')" class="btn-icon text-danger" title="Delete record"><i class='bx bx-trash'></i></button>
                    </td>
                </tr>
            `;
            tbody.insertAdjacentHTML('beforeend', row);
        });
    }
    
    document.getElementById('history-total-jama').textContent = '₹' + totalJama.toFixed(2);
    document.getElementById('history-total-upad').textContent = '₹' + totalUpad.toFixed(2);
    document.getElementById('history-net-balance').textContent = '₹' + (totalJama - totalUpad).toFixed(2);
    
    const balEl = document.getElementById('history-net-balance');
    if (totalJama - totalUpad < 0) {
        balEl.classList.remove('text-primary', 'text-success');
        balEl.classList.add('text-danger');
    } else {
        balEl.classList.remove('text-danger');
        balEl.classList.add('text-success');
    }
    
    const deleteBtn = document.getElementById('btn-delete-employee');
    deleteBtn.onclick = () => deleteCompleteKarigar(name);
    
    document.getElementById('karigar-history-modal').classList.add('show');
}

async function deleteCompleteKarigar(karigarName) {
    if (!confirm(`!! WARNING !!\nAre you sure you want to completely delete "${karigarName}"? This will permanently remove them from the system.`)) return;
    
    showLoader();
    try {
        await FirebaseService.deleteKarigar(karigarName);
        showToast(`Employee ${karigarName} deleted`, 'success');
        
        document.getElementById('karigar-history-modal').classList.remove('show');
        
        // Remove locally and re-render
        AppState.karigars = AppState.karigars.filter(k => k.name !== karigarName);
        renderKarigarGrid();
    } catch (e) {
        console.error('Failed to delete karigar:', e);
        showToast('Failed to delete employee', 'error');
    } finally {
        hideLoader();
    }
}

async function deleteKarigarHistory(txId, karigarName) {
    if (!confirm('Are you certain you want to specifically delete this record? This cannot be undone.')) return;
    
    showLoader();
    try {
        await FirebaseService.deleteKarigarTransaction(txId);
        showToast('Record deleted successfully', 'success');
        
        // Remove locally without full fetch
        AppState.karigarTransactions = AppState.karigarTransactions.filter(t => t.id !== txId);
        
        // Refresh the open modal and background grid immediately
        openKarigarHistoryModal(karigarName);
        renderKarigarGrid();
    } catch (e) {
        console.error('Failed to delete transaction:', e);
        showToast('Failed to delete record.', 'error');
    } finally {
        hideLoader();
    }
}

window.onload = function() {
    checkAuth();
};

async function confirmKarigarMonthlyReset() {
    if (!confirm("This will backup ALL current Karigar data to Google Sheets and instantly clear it from the app for the new month.\n\nAre you sure you want to proceed?")) return;
    
    showLoader();
    try {
        showToast('Initiating backup to Google Sheets...', 'info');
        
        // Ensure all pending writes are flushed first
        await FirebaseService.flushWrites();
        
        // Full database backup is the safest way so nothing else gets missed
        const allData = await FirebaseService.getAllDataForBackup();
        const response = await sheetsApiRequest({ action: 'saveFullBackup', data: allData });
        
        if (response && response.success) {
            showToast('Backup successful. Clearing local database...', 'info');
            
            // Delete from firestore
            const deleted = await FirebaseService.clearKarigarMonthlyData();
            
            // Update UI & memory state
            AppState.karigarTransactions = [];
            const currentMonth = getTodayISODate().substring(0, 7);
            localStorage.setItem('karigar_last_reset_month', currentMonth);
            
            showToast(`Monthly Reset complete! Cleared ${deleted} records.`, 'success');
            renderKarigarGrid();
        } else {
            showToast('Backup failed. Local data was kept safe.', 'error');
        }
    } catch (e) {
        console.error('Reset error:', e);
        showToast('System Error during reset.', 'error');
    } finally {
        hideLoader();
    }
}
function exportTableToPDF(title, headers, bodyData, filename) {
    if (!window.jspdf) { showToast('PDF Library not loaded', 'error'); return; }
    const { jsPDF } = window.jspdf;
    const doc = new jsPDF();
    doc.text(title, 14, 15);
    doc.setFontSize(10);
    doc.text(`Generated on: ${new Date().toLocaleString()}`, 14, 22);
    
    doc.autoTable({
        startY: 25,
        head: [headers],
        body: bodyData,
        theme: 'grid',
        headStyles: { fillColor: [19, 91, 236] },
        styles: { fontSize: 8, cellPadding: 2 }
    });
    doc.save(filename);
    showToast('PDF Exported!', 'success');
}

document.getElementById('export-pdf-sheet-btn')?.addEventListener('click', () => {
    const table = document.getElementById('sheet-table');
    if (!table) return;
    const headers = [];
    table.querySelectorAll('thead th').forEach(th => {
        const text = th.textContent.trim();
        if(text && text !== '#' && text !== '') headers.push(text);
    });
    
    // Custom logic to parse Data Sheet since it has inputs
    const rows = [];
    table.querySelectorAll('tbody tr:not(.sheet-grand-row)').forEach(tr => {
        const rowData = [];
        tr.querySelectorAll('td').forEach((td, idx) => {
            if(idx === 0 || idx === 1) return; // skip drag handle and #
            const input = td.querySelector('input');
            rowData.push(input ? input.value : td.textContent.trim());
        });
        if(rowData.length > 0) rows.push(rowData);
    });
    
    exportTableToPDF('Combined Data Sheet', headers, rows, `Data_Sheet_${getTodayISODate()}.pdf`);
});

document.getElementById('export-pdf-money-btn')?.addEventListener('click', () => {
    const rows = [];
    document.querySelectorAll('#money-management-tbody tr').forEach(tr => {
        const rowData = [
            tr.children[2].textContent.trim(), // Company
            tr.children[3].textContent.trim(), // Name
            tr.querySelector('.inp-money')?.value || '0',
            tr.querySelector('.inp-expense')?.value || '0',
            tr.querySelector('.row-total')?.textContent.trim() || '0'
        ];
        rows.push(rowData);
    });
    exportTableToPDF('Money Management Report', ['Company', 'Account', 'Money', 'Expense', 'Balance'], rows, `Money_Report_${getTodayISODate()}.pdf`);
});

document.getElementById('export-pdf-karigar-btn')?.addEventListener('click', () => {
    const rows = [];
    document.querySelectorAll('#karigar-grid .card').forEach(card => {
        const name = card.querySelector('.font-bold.text-lg')?.textContent.trim() || '';
        const jama = card.querySelector('.text-success.font-bold')?.textContent.replace('₹', '').trim() || '0';
        const upad = card.querySelector('.text-danger.font-bold')?.textContent.replace('₹', '').trim() || '0';
        const balance = card.querySelector('.text-primary, .text-danger')?.textContent.replace('₹', '').trim() || '0';
        rows.push([name, jama, upad, balance]);
    });
    exportTableToPDF('Karigar Management Report', ['Name', 'Total Jama', 'Total Upad', 'Balance'], rows, `Karigar_Report_${getTodayISODate()}.pdf`);
});

document.getElementById('global-export-btn')?.addEventListener('click', () => {
    document.getElementById('global-export-modal').classList.add('show');
    document.getElementById('export-from-date').value = getTodayISODate();
    document.getElementById('export-to-date').value = getTodayISODate();
});
document.getElementById('karigar-backup-open-btn')?.addEventListener('click', () => {
    document.getElementById('global-export-modal').classList.add('show');
    document.getElementById('export-from-date').value = getTodayISODate();
    document.getElementById('export-to-date').value = getTodayISODate();
});
document.getElementById('close-global-export-modal')?.addEventListener('click', () => {
    document.getElementById('global-export-modal').classList.remove('show');
});
document.getElementById('cancel-global-export')?.addEventListener('click', () => {
    document.getElementById('global-export-modal').classList.remove('show');
});

document.getElementById('confirm-global-export')?.addEventListener('click', async () => {
    const fromDate = document.getElementById('export-from-date').value;
    const toDate = document.getElementById('export-to-date').value;
    if (!fromDate || !toDate) { showToast('Please select both dates', 'error'); return; }
    
    document.getElementById('global-export-modal').classList.remove('show');
    showLoader();
    try {
        showToast('Generating Multi-Page PDF...', 'info');
        
        // Actually fetch the real data here directly from API.
        const res = await apiRequest({ action: 'getGlobalReport', fromDate, toDate });
        if(!res || !res.success) throw new Error(res?.message || 'Failed to generate report');
        
        if (!window.jspdf) { throw new Error('PDF Library not loaded'); }
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF();
        
        doc.setFontSize(20);
        doc.text('Krimaa Global Report', 14, 20);
        doc.setFontSize(10);
        doc.text(`Report Period: ${fromDate} to ${toDate}`, 14, 28);
        doc.text(`Generated exactly at: ${new Date().toLocaleString()}`, 14, 34);
        
        // Data Sheet Table
        doc.setFontSize(14);
        doc.text('Daily Orders Overview', 14, 45);
        doc.autoTable({
            startY: 50,
            head: [['Date', 'Company', 'Account', 'Orders', 'Amount']],
            body: res.orders.map(o => [o.date, o.companyId, o.accountName, o.meesho, o.total]),
            theme: 'grid',
            headStyles: { fillColor: [40, 40, 40] }
        });
        
        // Karigars Table
        doc.addPage();
        doc.text('Karigar Transactions Snapshot', 14, 15);
        doc.autoTable({
            startY: 20,
            head: [['Date', 'Type', 'Karigar', 'Design', 'Amount']],
            body: res.karigarTransactions.map(k => [k.date || '-', k.type || '-', k.karigarName || '-', k.designName || '-', k.amount || k.total || '0']),
            theme: 'grid',
            headStyles: { fillColor: [19, 91, 236] }
        });
        
        doc.save(`Krimaa_Global_Report_${fromDate}_to_${toDate}.pdf`);
        showToast('Global Report downloaded!', 'success');
        
    } catch(e) {
        console.error(e);
        showToast('Error generating global report', 'error');
    } finally {
        hideLoader();
    }
});
