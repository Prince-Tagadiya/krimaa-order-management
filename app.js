const API_URL = "https://script.google.com/macros/s/AKfycbwj9oL5WWKMGzSGYv3llJTjbPcHg8z2DvCdtquDIvmMAlsEt01mDvd0_IFdzSRVvPgT/exec"; 

// Simple hardcoded auth according to requirements
const ADMIN_USER = "Krimaa";
const ADMIN_PASS = "Kirmaa4484";

const AppState = {
    accounts: [],
    dashboardData: [],
    currentSection: 'dashboard'
};

document.addEventListener('DOMContentLoaded', () => {
    initApp();
});

function initApp() {
    checkAuth();
    attachEventListeners();
    
    // Set today's date in order input
    const today = new Date().toISOString().split('T')[0];
    document.getElementById('order-date').value = today;
    
    // Set current month label
    const monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
    document.getElementById('current-month-label').textContent = monthNames[new Date().getMonth()] + " " + new Date().getFullYear();
}

function checkAuth() {
    const isLogged = localStorage.getItem('isLogged');
    if (isLogged) {
        document.getElementById('login-screen').classList.add('hidden');
        document.getElementById('app-screen').classList.remove('hidden');
        loadInitialData();
    } else {
        document.getElementById('login-screen').classList.remove('hidden');
        document.getElementById('app-screen').classList.add('hidden');
    }
}

function attachEventListeners() {
    // Login Form
    document.getElementById('login-form').addEventListener('submit', (e) => {
        e.preventDefault();
        const user = document.getElementById('username').value;
        const pass = document.getElementById('password').value;
        if (user === ADMIN_USER && pass === ADMIN_PASS) {
            localStorage.setItem('isLogged', 'true');
            checkAuth();
            showToast("Login Successful / લોગિન સફળ", "success");
        } else {
            showToast("Invalid Credentials / અમાન્ય ઓળખપત્રો", "error");
        }
    });

    // Navigation
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            if (btn.id === 'logout-btn') {
                e.preventDefault();
                localStorage.removeItem('isLogged');
                checkAuth();
                return;
            }
            
            e.preventDefault();
            const target = btn.getAttribute('data-target');
            navigateTo(target);

            // Close mobile menu if open
            const sidebar = document.getElementById('sidebar');
            if(window.innerWidth <= 768 && sidebar.classList.contains('open')) {
                sidebar.classList.remove('open');
            }
        });
    });

    // Mobile Menu Toggle
    document.getElementById('menu-toggle').addEventListener('click', () => {
        document.getElementById('sidebar').classList.toggle('open');
    });

    // Add Account Form
    document.getElementById('add-account-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const accountName = document.getElementById('new-account-name').value.trim();
        if (!accountName) return;

        const btn = document.getElementById('add-account-btn');
        btn.disabled = true;

        // OPTIMISTIC UI: Instant update
        document.getElementById('new-account-name').value = '';
        const container = document.getElementById('active-account-list');
        const tempId = 'temp-' + Date.now();
        const tempHtml = `
            <div id="${tempId}" class="account-item" style="opacity: 0.6; border: 1px dashed var(--primary);">
                <i class='bx bx-loader-alt bx-spin'></i>
                <span>${accountName} <small class="text-muted">(Syncing... / ગુગલ શીટ્સ સાથે સિંક થઈ રહ્યું છે...)</small></span>
            </div>
        `;
        if (container.querySelector('p')) container.innerHTML = '';
        container.insertAdjacentHTML('afterbegin', tempHtml);

        try {
            const res = await apiRequest({ action: 'addAccount', accountName });
            if (res.success) {
                showToast("Account saved correctly! / એક્સેલમાં એકાઉન્ટ સેવ થયું!", "success");
                await fetchAccounts();
                renderAccountsList();
            } else {
                showToast(res.message, "error");
                document.getElementById(tempId).remove();
            }
        } catch (err) {
            showToast("Network Error! / નેટવર્ક ભૂલ!", "error");
            document.getElementById(tempId).remove();
        } finally {
            btn.disabled = false;
        }
    });

    // Submit Orders
    document.getElementById('submit-orders-btn').addEventListener('click', async () => {
        const date = document.getElementById('order-date').value;
        if(!date) return showToast("Please select a date / કૃપા કરીને તારીખ પસંદ કરો", "error");

        const orders = [];
        let hasData = false;
        
        document.querySelectorAll('.order-row').forEach(row => {
            const accountName = row.dataset.account;
            const meesho = parseInt(row.querySelector('.inp-meesho').value) || 0;
            const flipkart = parseInt(row.querySelector('.inp-flipkart').value) || 0;
            const total = meesho + flipkart;
            
            if (meesho > 0 || flipkart > 0) hasData = true;

            orders.push({ accountName, meesho, flipkart, total });
        });

        if (!hasData) {
            return showToast("Please enter at least one order value! / ઓછામાં ઓછું 1 ઓર્ડર મૂલ્ય દાખલ કરો!", "error");
        }

        const btn = document.getElementById('submit-orders-btn');
        btn.disabled = true;
        btn.textContent = "Submitting... / સબમિટ થઈ રહ્યું છે...";
        showLoader();

        try {
            const res = await apiRequest({ action: 'submitOrders', date, orders });
            if (res.success) {
                showToast("Orders saved! / ઓર્ડર એક્સેલમાં સાચવવામાં આવ્યા!", "success");
                // Clear inputs
                document.querySelectorAll('.order-row input').forEach(inp => inp.value = '');
                document.querySelectorAll('.row-total').forEach(tot => tot.textContent = '0');
                // Refresh dashboard cache later
                await fetchDashboardData();
                navigateTo('dashboard');
            } else {
                showToast(res.message, "error");
            }
        } catch (err) {
            showToast("Network Error! / નેટવર્ક ભૂલ!", "error");
        } finally {
            btn.disabled = false;
            btn.textContent = "Submit All Orders / બધા ઓર્ડર સબમિટ કરો";
            hideLoader();
        }
    });

    // Date change listener
    document.getElementById('order-date').addEventListener('change', () => {
        checkExistingOrdersForDate();
    });

    // Alert click to open modal
    document.getElementById('already-submitted-alert').addEventListener('click', () => {
        document.getElementById('order-details-modal').classList.add('show');
    });

    // Modal Close
    document.getElementById('close-modal-btn').addEventListener('click', () => {
        document.getElementById('order-details-modal').classList.remove('show');
    });
    document.getElementById('order-details-modal').addEventListener('click', (e) => {
        if(e.target.id === 'order-details-modal') {
            document.getElementById('order-details-modal').classList.remove('show');
        }
    });
}

function navigateTo(sectionId) {
    if (API_URL === "YOUR_GOOGLE_APPS_SCRIPT_WEB_APP_URL") {
        showToast("Please set the API_URL in app.js first!", "error");
        return;
    }

    // Update state
    AppState.currentSection = sectionId;

    // Update Nav Buttons
    document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
    const activeBtn = document.querySelector(`[data-target="${sectionId}"]`);
    if(activeBtn) activeBtn.classList.add('active');

    // Update Title
    const titles = {
        'dashboard': 'Dashboard / ડેશબોર્ડ',
        'daily-order': 'Daily Order Entry / દૈનિક ઓર્ડર દાખલ કરો',
        'add-account': 'Manage Accounts / એકાઉન્ટ મેનેજ કરો'
    };
    document.getElementById('page-title').textContent = titles[sectionId];

    // Hide/Show Sections
    document.querySelectorAll('.content-section').forEach(sec => sec.classList.remove('active'));
    document.getElementById(sectionId).classList.add('active');

    // Section specific logic
    if (sectionId === 'daily-order') {
        renderOrderEntryTable();
        checkExistingOrdersForDate();
    } else if (sectionId === 'dashboard') {
        renderDashboard();
    } else if (sectionId === 'add-account') {
        renderAccountsList();
    }
}

async function loadInitialData() {
    if (API_URL === "YOUR_GOOGLE_APPS_SCRIPT_WEB_APP_URL") {
        showToast("Welcome! Set API_URL in app.js to connect database.", "error");
        return;
    }
    
    showLoader();
    try {
        await Promise.all([fetchAccounts(), fetchDashboardData()]);
        renderDashboard();
    } catch (err) {
        showToast("Error loading data / ડેટા લોડ કરવામાં ભૂલ", "error");
    } finally {
        hideLoader();
    }
}

async function fetchAccounts() {
    try {
        const res = await apiRequest({ action: 'getAccounts' });
        if (res.success) {
            AppState.accounts = res.data;
        }
    } catch(e) { console.error(e); }
}

async function fetchDashboardData() {
    try {
        const res = await apiRequest({ action: 'getDashboardData' });
        if (res.success) {
            AppState.dashboardData = res.data;
        }
    } catch(e) { console.error(e); }
}

function renderAccountsList() {
    const container = document.getElementById('active-account-list');
    if (AppState.accounts.length === 0) {
        container.innerHTML = '<p class="text-muted">No accounts added yet. / હજી સુધી કોઈ એકાઉન્ટ ઉમેરવામાં આવ્યું નથી.</p>';
        return;
    }
    
    container.innerHTML = AppState.accounts.map(acc => `
        <div class="account-item">
            <i class='bx bx-user'></i>
            <span>${acc}</span>
        </div>
    `).join('');
}

function renderOrderEntryTable() {
    const tbody = document.getElementById('daily-order-tbody');
    const container = document.getElementById('order-form-container');
    const msg = document.getElementById('no-accounts-msg');

    if (AppState.accounts.length === 0) {
        container.classList.add('hidden');
        msg.classList.remove('hidden');
        return;
    }

    container.classList.remove('hidden');
    msg.classList.add('hidden');

    tbody.innerHTML = AppState.accounts.map((acc, index) => `
        <tr class="order-row" data-account="${acc}">
            <td class="font-medium">${acc}</td>
            <td><input type="number" min="0" class="inp-meesho" data-index="${index}" placeholder="0"></td>
            <td><input type="number" min="0" class="inp-flipkart" data-index="${index}" placeholder="0"></td>
            <td><span class="row-total" id="total-${index}">0</span></td>
        </tr>
    `).join('');

    // Attach dynamic total calculation
    document.querySelectorAll('.inp-meesho, .inp-flipkart').forEach(inp => {
        inp.addEventListener('input', (e) => {
            const idx = e.target.dataset.index;
            const meeshoStr = document.querySelector(`.inp-meesho[data-index="${idx}"]`).value;
            const flipkartStr = document.querySelector(`.inp-flipkart[data-index="${idx}"]`).value;
            
            const meesho = parseInt(meeshoStr) || 0;
            const flipkart = parseInt(flipkartStr) || 0;
            
            document.getElementById(`total-${idx}`).textContent = (meesho + flipkart);
            calculateGrandTotals();
        });
    });
    
    // Reset grand totals visibly
    calculateGrandTotals();
}

function calculateGrandTotals() {
    let grandMeesho = 0;
    let grandFlipkart = 0;
    
    document.querySelectorAll('.inp-meesho').forEach(inp => grandMeesho += (parseInt(inp.value) || 0));
    document.querySelectorAll('.inp-flipkart').forEach(inp => grandFlipkart += (parseInt(inp.value) || 0));
    
    document.getElementById('table-grand-meesho').textContent = grandMeesho;
    document.getElementById('table-grand-flipkart').textContent = grandFlipkart;
    document.getElementById('table-grand-total').textContent = (grandMeesho + grandFlipkart);
}

function checkExistingOrdersForDate() {
    const alert = document.getElementById('already-submitted-alert');
    const formContainer = document.getElementById('order-form-container');
    const dateInput = document.getElementById('order-date').value;
    
    if (!dateInput || !AppState.dashboardData) return;
    
    const existingOrders = AppState.dashboardData.filter(d => d.date === dateInput);
    
    if (existingOrders.length > 0) {
        // Already submitted
        alert.classList.remove('hidden');
        formContainer.classList.remove('hidden');
        
        let grandMeesho = 0;
        let grandFlipkart = 0;
        let grandTotal = 0;
        
        const modalBody = document.getElementById('modal-details-tbody');
        modalBody.innerHTML = existingOrders.map(o => {
            const m = parseInt(o.meesho) || 0;
            const f = parseInt(o.flipkart) || 0;
            const t = parseInt(o.total) || 0;
            grandMeesho += m;
            grandFlipkart += f;
            grandTotal += t;
            
            return `<tr>
                <td>${o.accountName}</td>
                <td style="text-align: right;">${m}</td>
                <td style="text-align: right;">${f}</td>
                <td style="text-align: right; font-weight: 600;">${t}</td>
            </tr>`;
        }).join('');
        
        document.getElementById('submitted-grand-total').textContent = grandTotal;
        
        // Convert to readable format like DD/MM/YYYY
        const [yyyy, mm, dd] = dateInput.split('-');
        document.getElementById('modal-date').textContent = `${dd}/${mm}/${yyyy}`;
        
        document.getElementById('modal-grand-meesho').textContent = grandMeesho;
        document.getElementById('modal-grand-flipkart').textContent = grandFlipkart;
        document.getElementById('modal-grand-total').textContent = grandTotal;
        
    } else {
        // Not submitted
        alert.classList.add('hidden');
        formContainer.classList.remove('hidden');
        document.querySelectorAll('.order-row input').forEach(inp => inp.value = '');
        document.querySelectorAll('.row-total').forEach(tot => tot.textContent = '0');
        calculateGrandTotals();
    }
}

function renderDashboard() {
    // Current Month Filter Logic (Filter dashboardData to current month)
    const currentMonthStr = new Date().toISOString().substring(0, 7); // yyyy-MM
    
    let monthData = AppState.dashboardData.filter(d => {
        // Date from Google Script usually "yyyy-MM-dd"
        if (!d.date) return false;
        return d.date.startsWith(currentMonthStr);
    });

    // Calculate totals
    let totalMeesho = 0;
    let totalFlipkart = 0;
    let overall = 0;
    
    // Account-wise grouping
    const accTotals = {};
    AppState.accounts.forEach(a => accTotals[a] = { meesho:0, flipkart:0, total:0 });

    monthData.forEach(row => {
        const m = parseInt(row.meesho) || 0;
        const f = parseInt(row.flipkart) || 0;
        const t = parseInt(row.total) || 0;
        
        totalMeesho += m;
        totalFlipkart += f;
        overall += t;

        if (accTotals[row.accountName]) {
            accTotals[row.accountName].meesho += m;
            accTotals[row.accountName].flipkart += f;
            accTotals[row.accountName].total += t;
        }
    });

    // Update KPI UI
    document.getElementById('dash-meesho-total').textContent = totalMeesho;
    document.getElementById('dash-flipkart-total').textContent = totalFlipkart;
    document.getElementById('dash-overall-total').textContent = overall;

    // Update Account Totals Table
    const accTbody = document.getElementById('dash-account-totals');
    accTbody.innerHTML = '';
    
    Object.keys(accTotals).forEach(acc => {
        const d = accTotals[acc];
        if(d.total > 0 || AppState.accounts.includes(acc)) {
            accTbody.innerHTML += `
                <tr>
                    <td>${acc}</td>
                    <td>${d.meesho}</td>
                    <td>${d.flipkart}</td>
                    <td style="font-weight: 600;">${d.total}</td>
                </tr>
            `;
        }
    });

    // Update Recent Records (Last 5 daily entries)
    const recentBody = document.getElementById('dash-recent-records');
    
    // Group raw data by date
    const dateGroups = {};
    AppState.dashboardData.forEach(r => {
        if (!dateGroups[r.date]) dateGroups[r.date] = 0;
        dateGroups[r.date] += parseInt(r.total) || 0;
    });

    // Sort dates desc
    const sortedDates = Object.keys(dateGroups).sort((a,b) => new Date(b) - new Date(a)).slice(0, 5);
    
    recentBody.innerHTML = sortedDates.map(date => `
        <tr>
            <td>${date}</td>
            <td>All Accounts / બધા એકાઉન્ટ્સ</td>
            <td style="font-weight: 600;">${dateGroups[date]}</td>
        </tr>
    `).join('');
    
    if (sortedDates.length === 0) {
        recentBody.innerHTML = '<tr><td colspan="3" class="text-center text-muted">No records found. / કોઈ રેકોર્ડ મળ્યો નથી.</td></tr>';
    }
}

// Network Request Helper
async function apiRequest(payload) {
    const res = await fetch(API_URL, {
        method: 'POST',
        headers: {
            'Content-Type': 'text/plain;charset=utf-8'
        },
        body: JSON.stringify(payload)
    });
    
    if (!res.ok) throw new Error("Network response was not ok");
    return await res.json();
}

// UI Helpers
function showToast(message, type = "success") {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className = `toast toast-${type}`;
    
    setTimeout(() => {
        toast.classList.add('hidden');
    }, 3000);
}

function showLoader() {
    document.getElementById('global-loader').classList.remove('hidden');
}

function hideLoader() {
    document.getElementById('global-loader').classList.add('hidden');
}

// Make globally available for inline onclicks
window.app = {
    navigateTo
};
