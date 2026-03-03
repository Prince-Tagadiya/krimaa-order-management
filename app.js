const API_URL = "https://script.google.com/macros/s/AKfycbwj9oL5WWKMGzSGYv3llJTjbPcHg8z2DvCdtquDIvmMAlsEt01mDvd0_IFdzSRVvPgT/exec"; 

// ===== Multi-User Auth System =====
const USERS = [
    { username: 'Krimaa', password: 'Kirmaa4484', role: 'admin', displayName: 'Admin' },
    { username: 'Order', password: 'Order123', role: 'order', displayName: 'Order Entry' }
];

const AppState = {
    accounts: [],
    dashboardData: [],
    company1Accounts: [],
    company2Accounts: [],
    company1Data: [],
    company2Data: [],
    currentSection: 'dashboard',
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
        AppState.accounts = [];
        AppState.dashboardData = [];
        await Promise.all([fetchAccounts(), fetchDashboardData()]);
        try {
            await fetchAllCompaniesData();
        } catch (e) {
            console.warn('Non-blocking: failed to refresh all-company dashboard data', e);
        }
        
        if (AppState.currentSection === 'dashboard') renderDashboard();
        else if (AppState.currentSection === 'daily-order') {
            setOrderDateDefaults();
            renderOrderEntryTable();
            checkExistingOrdersForDate();
        }
        else if (AppState.currentSection === 'add-account') renderAccountsList();
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
        // Hide Dashboard and Manage Accounts for order role
        document.getElementById('nav-dashboard').style.display = 'none';
        document.getElementById('nav-manage-accounts').style.display = 'none';
    } else {
        document.getElementById('nav-dashboard').style.display = '';
        document.getElementById('nav-manage-accounts').style.display = '';
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
        if (!accountName) return;
        const btn = document.getElementById('add-account-btn');
        btn.disabled = true;
        document.getElementById('new-account-name').value = '';
        const container = document.getElementById('active-account-list');
        const tempId = 'temp-' + Date.now();
        const tempHtml = `<div id="${tempId}" class="account-item" style="opacity: 0.6; border: 1px dashed var(--primary);"><i class='bx bx-loader-alt bx-spin'></i><span class="account-name">${accountName} <small class="text-muted">(Syncing...)</small></span></div>`;
        if (container.querySelector('p')) container.innerHTML = '';
        container.insertAdjacentHTML('afterbegin', tempHtml);
        try {
            const res = await apiRequest({ action: 'addAccount', accountName, companyId: AppState.currentCompany });
            if (res.success) { showToast("Account saved!", "success"); await fetchAccounts(); renderAccountsList(); }
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
            const flipkart = parseInt(row.querySelector('.inp-flipkart').value) || 0;
            if (meesho > 0 || flipkart > 0) hasData = true;
            orders.push({ accountName, meesho, flipkart, total: meesho + flipkart });
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
    document.getElementById('edit-account-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const oldName = document.getElementById('edit-account-old-name').value;
        const newName = document.getElementById('edit-account-name').value.trim();
        if (!newName) return;
        showLoader();
        try {
            const res = await apiRequest({ action: 'editAccount', oldName, newName, companyId: AppState.currentCompany });
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
    if (API_URL === "YOUR_GOOGLE_APPS_SCRIPT_WEB_APP_URL") { showToast("Please set the API_URL first!", "error"); return; }
    
    // Permission check for order role
    if (AppState.currentUser?.role === 'order' && (sectionId === 'add-account' || sectionId === 'dashboard')) {
        showToast("Access denied", "error"); return;
    }
    
    AppState.currentSection = sectionId;
    document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
    const activeBtn = document.querySelector(`[data-target="${sectionId}"]`);
    if(activeBtn) activeBtn.classList.add('active');
    const titles = { 'dashboard': 'Dashboard', 'daily-order': 'Daily Order Entry', 'add-account': 'Manage Accounts' };
    document.getElementById('page-title').textContent = titles[sectionId];
    document.querySelectorAll('.content-section').forEach(sec => sec.classList.remove('active'));
    document.getElementById(sectionId).classList.add('active');

    if (sectionId === 'daily-order') {
        setOrderDateDefaults();
        updateOrderCompanyLabel();
        renderOrderEntryTable();
        checkExistingOrdersForDate();
    }
    else if (sectionId === 'dashboard') renderDashboard();
    else if (sectionId === 'add-account') renderAccountsList();
}

async function loadInitialData() {
    if (API_URL === "YOUR_GOOGLE_APPS_SCRIPT_WEB_APP_URL") { showToast("Set API_URL in app.js.", "error"); return; }
    showLoader();
    try {
        await Promise.all([fetchAccounts(), fetchDashboardData()]);
        try {
            await fetchAllCompaniesData();
        } catch (e) {
            console.warn('Non-blocking: failed to preload all-company dashboard data', e);
        }
        if (AppState.currentUser?.role === 'order') {
            navigateTo('daily-order');
        } else {
            navigateTo('dashboard');
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
    } catch(e) {
        console.error(e);
        throw e;
    }
}

async function fetchDashboardData() {
    try {
        const res = await apiRequest({ action: 'getDashboardData', companyId: AppState.currentCompany });
        if (res && res.success === false) throw new Error(res.message || 'Unable to fetch dashboard data');
        if (Array.isArray(res?.data)) AppState.dashboardData = res.data;
        else if (Array.isArray(res)) AppState.dashboardData = res; // compatibility with legacy shape
        else AppState.dashboardData = [];
    } catch(e) {
        console.error(e);
        throw e;
    }
}

async function fetchAllCompaniesData() {
    const toList = (res) => {
        if (!res || (res && res.success === false)) return null;
        if (Array.isArray(res?.data)) return res.data;
        if (Array.isArray(res)) return res;
        return [];
    };

    const [c1Acc, c2Acc, c1Data, c2Data] = await Promise.all([
        apiRequest({ action: 'getAccounts', companyId: 'company1' }).catch(() => null),
        apiRequest({ action: 'getAccounts', companyId: 'company2' }).catch(() => null),
        apiRequest({ action: 'getDashboardData', companyId: 'company1' }).catch(() => null),
        apiRequest({ action: 'getDashboardData', companyId: 'company2' }).catch(() => null)
    ]);

    const c1Accounts = toList(c1Acc);
    const c2Accounts = toList(c2Acc);
    const c1Rows = toList(c1Data);
    const c2Rows = toList(c2Data);

    if (c1Accounts !== null) AppState.company1Accounts = c1Accounts;
    if (c2Accounts !== null) AppState.company2Accounts = c2Accounts;
    if (c1Rows !== null) AppState.company1Data = c1Rows;
    if (c2Rows !== null) AppState.company2Data = c2Rows;
}

// ===== RENDER FUNCTIONS =====

function renderAccountsList() {
    const container = document.getElementById('active-account-list');
    if (AppState.accounts.length === 0) { container.innerHTML = '<p class="text-muted">No accounts added yet.</p>'; return; }
    const sorted = getSortedAccounts();
    const isAdmin = AppState.currentUser?.role === 'admin';
    container.innerHTML = sorted.map((acc, idx) => `
        <div class="account-item">
            <div class="account-position-badge">${idx + 1}</div>
            <span class="account-name">${acc}</span>
            ${isAdmin ? `<div class="account-actions">
                <button class="btn btn-outline btn-sm edit-btn" onclick="openEditAccount('${acc.replace(/'/g, "\\'")}')" title="Edit"><i class='bx bx-edit-alt'></i> Edit</button>
                <button class="btn btn-outline btn-sm delete-btn" onclick="openDeleteAccount('${acc.replace(/'/g, "\\'")}')" title="Delete"><i class='bx bx-trash'></i></button>
            </div>` : ''}
        </div>
    `).join('');
}

function openEditAccount(name) {
    document.getElementById('edit-account-old-name').value = name;
    document.getElementById('edit-account-name').value = name;
    document.getElementById('edit-account-modal').classList.add('show');
    setTimeout(() => document.getElementById('edit-account-name').focus(), 200);
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
            <td><input type="number" min="0" class="inp-flipkart" data-index="${index}" placeholder="0"></td>
            <td><span class="row-total" id="total-${index}">0</span></td>
        </tr>
    `).join('');
    document.querySelectorAll('.inp-meesho, .inp-flipkart').forEach(inp => {
        inp.addEventListener('input', (e) => {
            const idx = e.target.dataset.index;
            const m = parseInt(document.querySelector(`.inp-meesho[data-index="${idx}"]`).value) || 0;
            const f = parseInt(document.querySelector(`.inp-flipkart[data-index="${idx}"]`).value) || 0;
            document.getElementById(`total-${idx}`).textContent = (m + f);
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
                row.querySelector('.inp-flipkart').dataset.index = idx;
                row.querySelector('.row-total').id = `total-${idx}`;
            });
            saveAccountPositions(newOrder);
            saveAccountOrderToSheet(newOrder);
            document.querySelectorAll('.inp-meesho, .inp-flipkart').forEach(inp => {
                const newInp = inp.cloneNode(true);
                inp.parentNode.replaceChild(newInp, inp);
                newInp.addEventListener('input', (e) => {
                    const idx = e.target.dataset.index;
                    const m = parseInt(document.querySelector(`.inp-meesho[data-index="${idx}"]`).value) || 0;
                    const f = parseInt(document.querySelector(`.inp-flipkart[data-index="${idx}"]`).value) || 0;
                    document.getElementById(`total-${idx}`).textContent = (m + f);
                    calculateGrandTotals();
                });
            });
            showToast('Saving position...', 'success');
        }
    });
}

async function saveAccountOrderToSheet(orderedAccounts) {
    try {
        const res = await apiRequest({ action: 'updateAccountOrder', orderedAccounts, companyId: AppState.currentCompany });
        if (res.success) showToast('Position saved!', 'success');
        else showToast(res.message || 'Failed', 'error');
    } catch(err) { showToast('Position saved locally, sheet sync failed', 'error'); }
}

function calculateGrandTotals() {
    let gm = 0, gf = 0;
    document.querySelectorAll('.inp-meesho').forEach(inp => gm += (parseInt(inp.value) || 0));
    document.querySelectorAll('.inp-flipkart').forEach(inp => gf += (parseInt(inp.value) || 0));
    document.getElementById('table-grand-meesho').textContent = gm;
    document.getElementById('table-grand-flipkart').textContent = gf;
    document.getElementById('table-grand-total').textContent = (gm + gf);
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
        let gm = 0, gf = 0, gt = 0;
        const modalBody = document.getElementById('modal-details-tbody');
        modalBody.innerHTML = existingOrders.map(o => {
            const m = parseInt(o.meesho)||0, f = parseInt(o.flipkart)||0, t = parseInt(o.total)||0;
            gm += m; gf += f; gt += t;
            return `<tr><td>${o.accountName}</td><td style="text-align:right;">${m}</td><td style="text-align:right;">${f}</td><td style="text-align:right;font-weight:600;">${t}</td></tr>`;
        }).join('');
        document.getElementById('submitted-grand-total').textContent = gt;
        const [yyyy, mm, dd] = dateInput.split('-');
        document.getElementById('modal-date').textContent = `${dd}/${mm}/${yyyy}`;
        document.getElementById('modal-grand-meesho').textContent = gm;
        document.getElementById('modal-grand-flipkart').textContent = gf;
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
    let meesho = 0, flipkart = 0, total = 0;
    data.forEach(row => { meesho += parseInt(row.meesho)||0; flipkart += parseInt(row.flipkart)||0; total += parseInt(row.total)||0; });
    return { meesho, flipkart, total };
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
    document.getElementById('dash-c1-flipkart').textContent = c1Totals.flipkart;
    document.getElementById('dash-c1-total').textContent = c1Totals.total;
    document.getElementById('dash-c2-meesho').textContent = c2Totals.meesho;
    document.getElementById('dash-c2-flipkart').textContent = c2Totals.flipkart;
    document.getElementById('dash-c2-total').textContent = c2Totals.total;
    document.getElementById('dash-combined-meesho').textContent = c1Totals.meesho + c2Totals.meesho;
    document.getElementById('dash-combined-flipkart').textContent = c1Totals.flipkart + c2Totals.flipkart;
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
        accounts.forEach(a => totals[a] = { meesho:0, flipkart:0, total:0 });
        filtered.forEach(row => { if (totals[row.accountName]) { totals[row.accountName].meesho += parseInt(row.meesho)||0; totals[row.accountName].flipkart += parseInt(row.flipkart)||0; totals[row.accountName].total += parseInt(row.total)||0; } });
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
            html += `<tr><td class="font-medium">${acc}</td><td class="text-center"><span class="dot dot-a"></span></td><td class="text-right text-muted">${d.meesho}</td><td class="text-right text-muted">${d.flipkart}</td><td class="text-right font-bold text-main">${d.total}</td></tr>`;
        });
    }

    // Company B Header
    if (Object.keys(c2AccTotals).length > 0) {
        html += `<tr class="table-divider-row"><td colspan="5">Company B Accounts</td></tr>`;
        Object.keys(c2AccTotals).forEach(acc => {
            const d = c2AccTotals[acc];
            html += `<tr><td class="font-medium">${acc}</td><td class="text-center"><span class="dot dot-b"></span></td><td class="text-right text-muted">${d.meesho}</td><td class="text-right text-muted">${d.flipkart}</td><td class="text-right font-bold text-main">${d.total}</td></tr>`;
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
        if (!dateGroups[r.date]) dateGroups[r.date] = { totalMeesho: 0, totalFlipkart: 0, totalOrders: 0, records: [] };
        const m = parseInt(r.meesho)||0, f = parseInt(r.flipkart)||0, t = parseInt(r.total)||0;
        dateGroups[r.date].totalMeesho += m; dateGroups[r.date].totalFlipkart += f; dateGroups[r.date].totalOrders += t;
        dateGroups[r.date].records.push(r);
    });
    
    const sortedDates = Object.keys(dateGroups).sort((a,b) => new Date(b) - new Date(a)).slice(0, 7);
    let html = '';
    sortedDates.forEach(date => {
        const group = dateGroups[date]; const dateId = date.replace(/-/g, '');
        html += `<tr class="date-header-row" onclick="toggleDateDetails('${dateId}')"><td><span class="date-toggle-icon" id="icon-${dateId}"><i class='bx bx-chevron-right'></i></span>${date}</td><td>All</td><td>All Accounts</td><td>${group.totalMeesho}</td><td>${group.totalFlipkart}</td><td style="font-weight:600;">${group.totalOrders}</td></tr>`;
        group.records.forEach(r => {
            const m = parseInt(r.meesho)||0, f = parseInt(r.flipkart)||0, t = parseInt(r.total)||0;
            html += `<tr class="date-detail-row" data-date="${dateId}"><td></td><td><span class="company-badge badge-${r.company.toLowerCase()}">${r.company}</span></td><td>${r.accountName}</td><td>${m}</td><td>${f}</td><td style="font-weight:500;">${t}</td></tr>`;
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
        { label: 'Flipkart Orders', a: c1Totals.flipkart, b: c2Totals.flipkart },
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
    
    let c1M = 0, c1F = 0, c1T = 0;
    AppState.company1Data.filter(r => normalizeToISODate(r.date) === dateStr).forEach(r => { c1M += parseInt(r.meesho)||0; c1F += parseInt(r.flipkart)||0; c1T += parseInt(r.total)||0; });
    
    let c2M = 0, c2F = 0, c2T = 0;
    AppState.company2Data.filter(r => normalizeToISODate(r.date) === dateStr).forEach(r => { c2M += parseInt(r.meesho)||0; c2F += parseInt(r.flipkart)||0; c2T += parseInt(r.total)||0; });
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
                    <span>Flipkart: <strong>${c1F}</strong></span>
                </div>
            </div>
            <div class="date-company-card date-company-b">
                <div class="date-company-head">
                    <div class="date-company-title"><span class="dot-sm dot-b"></span> ${company2Name}</div>
                    <span class="date-company-total">${c2T}</span>
                </div>
                <div class="date-company-chips">
                    <span>Meesho: <strong>${c2M}</strong></span>
                    <span>Flipkart: <strong>${c2F}</strong></span>
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
    
    const rows = [['Date', 'Company', 'Account', 'Meesho', 'Flipkart', 'Total']];
    
    c1Filtered.forEach(r => {
        rows.push([normalizeToISODate(r.date), 'Company A', r.accountName, parseInt(r.meesho)||0, parseInt(r.flipkart)||0, parseInt(r.total)||0]);
    });
    c2Filtered.forEach(r => {
        rows.push([normalizeToISODate(r.date), 'Company B', r.accountName, parseInt(r.meesho)||0, parseInt(r.flipkart)||0, parseInt(r.total)||0]);
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

// ===== API =====
async function apiRequest(payload) {
    const parseJsonResponse = async (res) => {
        const text = await res.text();
        try {
            return JSON.parse(text);
        } catch (e) {
            throw new Error('Invalid JSON response from API');
        }
    };

    const postRequest = async () => {
        const res = await fetch(API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'text/plain;charset=utf-8' },
            body: JSON.stringify(payload)
        });
        if (!res.ok) throw new Error(`Network response was not ok (${res.status})`);
        return parseJsonResponse(res);
    };

    try {
        return await postRequest();
    } catch (postError) {
        // Backward-compatible fallback for read actions on older Apps Script deployments.
        const action = payload?.action;
        const isReadAction = action === 'getAccounts' || action === 'getDashboardData' || action === 'getCompanies';
        if (!isReadAction) throw postError;

        const params = new URLSearchParams({
            action: action || '',
            companyId: payload?.companyId || AppState.currentCompany || 'company1'
        });
        const getRes = await fetch(`${API_URL}?${params.toString()}`, { method: 'GET' });
        if (!getRes.ok) throw postError;
        return parseJsonResponse(getRes);
    }
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
function showToast(message, type = "success") {
    const toast = document.getElementById('toast');
    if (!toast) return;
    if (toast._timer) clearTimeout(toast._timer);

    // Reset styles
    toast.style.border = "none";
    toast.style.boxShadow = "0 10px 15px -3px rgba(0,0,0,0.1)";

    // Set icon and colors based on type
    let icon = `<i class='bx bxs-check-circle'></i>`;
    let bg = "#10b981", color = "white"; // default success
    
    if (type === "error") { 
        icon = `<i class='bx bxs-error-circle'></i>`; 
        bg = "#ef4444"; 
    } else if (type === "info") { 
        icon = `<i class='bx bxs-info-circle'></i>`; 
        bg = "#f8fafc"; 
        color = "#1e293b"; 
        toast.style.border = "1px solid #cbd5e1"; 
    }
    
    toast.style.background = bg;
    toast.style.color = color;
    toast.innerHTML = `${icon} <span>${message}</span>`;
    
    toast.classList.remove('hidden');
    
    // Auto-hide unless it's info (which we usually manage manually)
    if (type !== "info") {
        toast._timer = setTimeout(() => {
            toast.classList.add('hidden');
        }, 3000);
    }
}
function showLoader() { document.getElementById('global-loader').classList.remove('hidden'); }
function hideLoader() { document.getElementById('global-loader').classList.add('hidden'); }

// Global exports
window.app = { navigateTo };
window.openEditAccount = openEditAccount;
window.openDeleteAccount = openDeleteAccount;
window.toggleDateDetails = toggleDateDetails;
