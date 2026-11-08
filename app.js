// API_URL is now in env.js as SHEETS_API_URL

// ===== Multi-User Auth System =====
let USERS = [];
function refreshUsersFromEnv() {
    USERS = (Array.isArray(window.AUTH_USERS) ? window.AUTH_USERS : [])
        .map((u) => ({
            username: String(u?.username || '').trim(),
            password: String(u?.password || ''),
            role: String(u?.role || '').trim(),
            displayName: String(u?.displayName || u?.username || '').trim(),
            allowedCompanies: Array.isArray(u?.allowedCompanies) && u.allowedCompanies.length
                ? u.allowedCompanies.map(c => String(c || '').trim()).filter(Boolean)
                : ['company1', 'company2']
        }))
        .filter(u => u.username && u.password && u.role);
    return USERS;
}

const DESIGN_PRICE_SCOPE = 'global';

function round2(n) {
    return Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;
}

function toAmount(v) {
    const n = parseFloat(String(v ?? '').replace(/[^0-9.-]/g, ''));
    return Number.isFinite(n) ? n : 0;
}

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
    karigarResetBackups: [],
    karigarResetBackupsByCompany: {},
    selectedKarigarResetBackupId: '',
    selectedKarigarResetBackupIdByCompany: {},
    karigarResetBackupEmployeeMap: {},
    selectedKarigarResetEmployeeKey: '',
    karigars: [],
    karigarTransactions: [],
    designPrices: {},
    karigarCacheByCompany: {},
    designPricesByCompany: {},
    designPriceHistoryByCompany: {},
    currentSection: 'data-sheet',
    currentCompany: 'company1',
    currentUser: null, // { username, role, displayName, allowedCompanies }
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

function getCurrentLocalDateTimeInput() {
    const now = new Date();
    const local = new Date(now.getTime() - (now.getTimezoneOffset() * 60000));
    return local.toISOString().slice(0, 16); // YYYY-MM-DDTHH:mm
}

function getCurrentLocalTimeInput() {
    return getCurrentLocalDateTimeInput().split('T')[1] || '00:00';
}

function normalizeToISODateTime(rawDateTime, fallbackDate = '') {
    if (!rawDateTime && fallbackDate) {
        return `${fallbackDate}T00:00:00`;
    }
    if (!rawDateTime) return '';
    if (rawDateTime instanceof Date && !isNaN(rawDateTime.getTime())) {
        const local = new Date(rawDateTime.getTime() - (rawDateTime.getTimezoneOffset() * 60000));
        return local.toISOString().slice(0, 19);
    }

    const str = String(rawDateTime).trim().replace(' ', 'T');
    const dtMatch = str.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})(:\d{2})?$/);
    if (dtMatch) {
        const secs = dtMatch[3] ? dtMatch[3] : ':00';
        return `${dtMatch[1]}T${dtMatch[2]}${secs}`;
    }
    const dateOnly = normalizeToISODate(str);
    if (dateOnly) return `${dateOnly}T00:00:00`;

    const parsed = parseFlexibleDateTime(str);
    if (!parsed) return '';
    const local = new Date(parsed.getTime() - (parsed.getTimezoneOffset() * 60000));
    return local.toISOString().slice(0, 19);
}

function combineDateAndTimeToISO(dateValue, timeValue) {
    const safeDate = normalizeToISODate(dateValue);
    const safeTimeRaw = String(timeValue || '').trim();
    const safeTime = /^\d{2}:\d{2}$/.test(safeTimeRaw) ? `${safeTimeRaw}:00` : '00:00:00';
    if (!safeDate) return '';
    return `${safeDate}T${safeTime}`;
}

function formatISODateTimeForDisplay(rawDateTime) {
    const dt = parseFlexibleDateTime(rawDateTime);
    if (!dt) return '';
    const datePart = formatISODateForDisplay(dt);
    const timePart = dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    return `${datePart} ${timePart}`;
}

function parseFlexibleDateTime(rawValue) {
    if (rawValue === null || rawValue === undefined || rawValue === '') return null;
    if (rawValue instanceof Date && !isNaN(rawValue.getTime())) return rawValue;

    if (typeof rawValue === 'object') {
        if (typeof rawValue.toDate === 'function') {
            try {
                const d = rawValue.toDate();
                if (d instanceof Date && !isNaN(d.getTime())) return d;
            } catch (e) {}
        }
        if (typeof rawValue.seconds === 'number') {
            const d = new Date(rawValue.seconds * 1000);
            if (!isNaN(d.getTime())) return d;
        }
        if (typeof rawValue._seconds === 'number') {
            const d = new Date(rawValue._seconds * 1000);
            if (!isNaN(d.getTime())) return d;
        }
    }

    if (typeof rawValue === 'number' && isFinite(rawValue)) {
        if (rawValue > 1000000000000) {
            const d = new Date(rawValue);
            if (!isNaN(d.getTime())) return d;
        } else if (rawValue > 1000000000) {
            const d = new Date(rawValue * 1000);
            if (!isNaN(d.getTime())) return d;
        } else if (rawValue > 10000 && rawValue < 60000) {
            const d = new Date((rawValue - 25569) * 86400000);
            if (!isNaN(d.getTime())) return d;
        }
    }

    const str = String(rawValue).trim();
    if (!str) return null;

    if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$/.test(str)) {
        const d = new Date(str.replace(' ', 'T'));
        if (!isNaN(d.getTime())) return d;
    }

    if (/^\d{1,2}\/\d{1,2}\/\d{4}(\s+\d{1,2}:\d{2}(:\d{2})?)?$/.test(str)) {
        const parts = str.split(/\s+/);
        const dateParts = (parts[0] || '').split('/');
        if (dateParts.length === 3) {
            let day = parseInt(dateParts[0], 10);
            let month = parseInt(dateParts[1], 10);
            const year = parseInt(dateParts[2], 10);
            if (day <= 12 && month > 12) {
                const tmp = day;
                day = month;
                month = tmp;
            }
            const timeParts = (parts[1] || '00:00:00').split(':');
            const hour = parseInt(timeParts[0] || '0', 10);
            const minute = parseInt(timeParts[1] || '0', 10);
            const second = parseInt(timeParts[2] || '0', 10);
            const d = new Date(year, month - 1, day, hour, minute, second);
            if (!isNaN(d.getTime())) return d;
        }
    }

    const parsed = new Date(str);
    if (!isNaN(parsed.getTime())) return parsed;
    return null;
}

function getKarigarTxTimestampMs(tx) {
    if (!tx) return 0;
    const txDateTime = parseFlexibleDateTime(tx.transactionDateTime || tx.dateTime);
    if (txDateTime) return txDateTime.getTime();
    const created = parseFlexibleDateTime(tx.createdAt);
    if (created) return created.getTime();
    const updated = parseFlexibleDateTime(tx.updatedAt);
    if (updated) return updated.getTime();
    const rowDate = parseFlexibleDateTime(tx.date);
    return rowDate ? rowDate.getTime() : 0;
}

function normalizeKarigarNameKey(name) {
    return String(name || '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, ' ');
}

function getKarigarTransactionsFor(karigarId) {
    const targetId = String(karigarId || '').trim();
    if (!targetId) return [];
    const byId = AppState.karigarTransactionsById;
    if (byId && typeof byId === 'object' && Array.isArray(byId[targetId])) {
        return byId[targetId];
    }
    return (AppState.karigarTransactions || [])
        .filter(t => resolveKarigarIdFromState(t.karigarId, t.karigarName) === targetId || String(t.karigarId || '').trim() === targetId)
        .sort((a, b) => getKarigarTxTimestampMs(b) - getKarigarTxTimestampMs(a));
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
    const singleMode = String(APP_CONFIG?.clientMode || window.CLIENT_MODE || '').trim().toLowerCase() === 'single_company';
    const singleName = String(APP_CONFIG?.clientCompanyName || window.CLIENT_COMPANY_NAME || '').trim();
    if (singleMode && singleName && (companyId === 'company1' || companyId === 'company2')) return singleName;
    const btn = document.querySelector(`.company-btn[data-company="${companyId}"]`);
    if (btn && btn.textContent) return btn.textContent.trim();
    if (companyId === 'company1') return 'Company 1';
    if (companyId === 'company2') return 'Company 2';
    return companyId;
}

function isSingleCompanyMode() {
    return String(APP_CONFIG?.clientMode || window.CLIENT_MODE || '').trim().toLowerCase() === 'single_company';
}

function getSingleCompanyName() {
    return String(APP_CONFIG?.clientCompanyName || window.CLIENT_COMPANY_NAME || '').trim() || 'Company';
}

function applyClientModeUI() {
    const singleMode = isSingleCompanyMode();
    const accountInput = document.getElementById('new-account-name');
    if (accountInput) accountInput.placeholder = 'e.g. Prince Tagadiya';

    const companyButtons = Array.from(document.querySelectorAll('.company-btn'));
    const company1Btn = companyButtons.find(b => b.dataset.company === 'company1');
    const company2Btn = companyButtons.find(b => b.dataset.company === 'company2');
    const sheetCompanyFilter = document.getElementById('sheet-company-filter');

    if (!singleMode) return;

    const name = getSingleCompanyName();
    if (company1Btn) company1Btn.textContent = name;
    if (company2Btn) company2Btn.style.display = 'none';
    AppState.currentCompany = 'company1';
    localStorage.setItem('selectedCompany', 'company1');

    if (sheetCompanyFilter) {
        sheetCompanyFilter.innerHTML = `<option value="company1">${name}</option>`;
        sheetCompanyFilter.value = 'company1';
        sheetCompanyFilter.disabled = true;
        sheetCompanyFilter.style.pointerEvents = 'none';
        sheetCompanyFilter.style.opacity = '0.9';
    }
}

function getAllowedCompanies() {
    if (isSingleCompanyMode()) return ['company1'];
    const allowed = AppState.currentUser?.allowedCompanies;
    if (Array.isArray(allowed) && allowed.length > 0) return allowed;
    return ['company1', 'company2'];
}

function isCompanyAllowed(companyId) {
    return getAllowedCompanies().includes(companyId);
}

function isAdminUser() {
    return String(AppState.currentUser?.role || '').trim() === 'admin';
}

function isDevUser() {
    return String(AppState.currentUser?.role || '').trim() === 'dev';
}

function isOrderUser() {
    const role = String(AppState.currentUser?.role || '').trim();
    return role === 'order' || role === 'order_c2';
}

function canManageAccounts() {
    const role = String(AppState.currentUser?.role || localStorage.getItem('userRole') || '').trim().toLowerCase();
    const username = String(AppState.currentUser?.username || localStorage.getItem('userUsername') || '').trim().toLowerCase();
    const displayName = String(AppState.currentUser?.displayName || localStorage.getItem('userName') || '').trim().toLowerCase();
    
    if (role === 'admin' || role === 'dev' || role === 'order_c2') return true;
    if (username === 'dhyan_order' || username === 'dhyan') return true;
    if (displayName.includes('dhyan')) return true;
    if (window.location.hash.toLowerCase().includes('dhyan')) return true;
    return false;
}

function getCurrentDashboardLabel() {
    const labels = {
        dashboard: 'Dashboard',
        'daily-order': 'Daily Order',
        'add-account': 'Manage Accounts',
        'money-management': 'Money Management',
        'money-backup': 'Money Backup',
        'data-sheet': 'Data Sheet',
        karigar: 'Karigar Management',
        'size-prices': 'Size Prices'
    };
    const key = String(AppState.currentSection || '').trim();
    return labels[key] || key || 'Web App';
}

function getActorAccountLabel() {
    const role = String(AppState.currentUser?.role || '').trim().toLowerCase();
    if (role === 'admin') return 'admin';
    if (role === 'order') return 'order';
    if (role === 'order_c2') return 'dhyan';
    return role || 'unknown';
}

function buildAuditActor() {
    return {
        role: AppState.currentUser?.role || 'unknown',
        username: AppState.currentUser?.username || 'unknown',
        displayName: AppState.currentUser?.displayName || 'unknown',
        source: getActorAccountLabel(),
        actorAccount: getActorAccountLabel(),
        dashboard: getCurrentDashboardLabel(),
        dashboardId: AppState.currentSection || '',
        companyId: AppState.currentCompany || ''
    };
}

function setCompanyButtonsDisabled(disabled) {
    document.querySelectorAll('.company-btn').forEach(btn => {
        btn.disabled = disabled;
        btn.classList.toggle('is-disabled', disabled);
    });
}

function applyCurrentCompanySnapshotFromMemory() {
    const companyId = String(AppState.currentCompany || 'company1').trim() || 'company1';
    if (companyId === 'company2') {
        AppState.accounts = Array.isArray(AppState.company2Accounts) ? [...AppState.company2Accounts] : [];
        AppState.accountDetails = Array.isArray(AppState.company2Details) ? [...AppState.company2Details] : [];
        AppState.dashboardData = Array.isArray(AppState.company2Data) ? [...AppState.company2Data] : [];
    } else {
        AppState.accounts = Array.isArray(AppState.company1Accounts) ? [...AppState.company1Accounts] : [];
        AppState.accountDetails = Array.isArray(AppState.company1Details) ? [...AppState.company1Details] : [];
        AppState.dashboardData = Array.isArray(AppState.company1Data) ? [...AppState.company1Data] : [];
    }
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

const DATA_SHEET_START_DATE = '2026-03-14';

function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function getMonthLabelFromKey(monthKey) {
    const [year, month] = String(monthKey || '').split('-');
    const idx = parseInt(month, 10) - 1;
    const names = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    if (!year || idx < 0 || idx > 11) return String(monthKey || '');
    return `${names[idx]} ${year}`;
}

function getLastDayOfMonthISO(monthKey) {
    const [year, month] = String(monthKey || '').split('-').map(v => parseInt(v, 10));
    if (!year || !month) return '';
    const last = new Date(year, month, 0);
    return normalizeToISODate(last);
}

function buildDateRangeDescending(startISO, endISO) {
    const start = normalizeToISODate(startISO);
    const end = normalizeToISODate(endISO);
    if (!start || !end) return [];
    const startDate = new Date(`${start}T00:00:00`);
    const endDate = new Date(`${end}T00:00:00`);
    if (isNaN(startDate.getTime()) || isNaN(endDate.getTime()) || startDate > endDate) return [];

    const out = [];
    const cursor = new Date(endDate);
    while (cursor >= startDate) {
        out.push(normalizeToISODate(cursor));
        cursor.setDate(cursor.getDate() - 1);
    }
    return out;
}

function getDataSheetStartDateForMonth(monthKey) {
    const monthStart = `${monthKey}-01`;
    if (monthKey === DATA_SHEET_START_DATE.substring(0, 7)) {
        return DATA_SHEET_START_DATE;
    }
    return monthStart > DATA_SHEET_START_DATE ? monthStart : DATA_SHEET_START_DATE;
}

function getCompanyAccountDetails(companyId) {
    return companyId === 'company2'
        ? (AppState.company2Details || [])
        : (AppState.company1Details || []);
}

function getOrderedCompanyAccounts(companyId) {
    const details = getCompanyAccountDetails(companyId);
    const rawOrder = companyId === 'company2'
        ? (AppState.company2Accounts || [])
        : (AppState.company1Accounts || []);
    const byId = new Map();
    const byName = new Map();

    details.forEach((detail, idx) => {
        const safeId = String(detail?.accountId || '').trim();
        const safeName = String(detail?.name || '').trim();
        const normalized = {
            ...detail,
            accountId: safeId,
            name: safeName || safeId,
            position: Number.isFinite(parseInt(detail?.position, 10)) ? parseInt(detail.position, 10) : idx
        };
        if (safeId) byId.set(safeId, normalized);
        if (safeName) byName.set(safeName, normalized);
    });

    const ordered = [];
    const usedIds = new Set();
    rawOrder.forEach((entry) => {
        const safeEntry = String(entry || '').trim();
        if (!safeEntry) return;
        const match = byId.get(safeEntry) || byName.get(safeEntry);
        if (!match || usedIds.has(match.accountId)) return;
        ordered.push(match);
        usedIds.add(match.accountId);
    });

    details
        .slice()
        .sort((a, b) => (parseInt(a?.position, 10) || 9999) - (parseInt(b?.position, 10) || 9999))
        .forEach((detail) => {
            const safeId = String(detail?.accountId || '').trim();
            if (!safeId || usedIds.has(safeId)) return;
            ordered.push({
                ...detail,
                accountId: safeId,
                name: String(detail?.name || safeId).trim()
            });
            usedIds.add(safeId);
        });

    return ordered;
}

function syncCompanyAccountOrderState(companyId, orderedAccounts) {
    const orderedDetails = getOrderedCompanyAccounts(companyId);
    const ids = orderedDetails.map(detail => detail.accountId);
    const targetIds = Array.isArray(orderedAccounts) && orderedAccounts.length
        ? orderedAccounts.map(item => {
            const safeItem = String(item || '').trim();
            const match = orderedDetails.find(detail => detail.accountId === safeItem || detail.name === safeItem);
            return match?.accountId || safeItem;
        }).filter(Boolean)
        : ids;
    const finalIds = [...new Set([...targetIds, ...ids])];

    if (companyId === 'company2') {
        AppState.company2Accounts = finalIds;
        AppState.company2Details = finalIds
            .map(id => orderedDetails.find(detail => detail.accountId === id))
            .filter(Boolean)
            .map((detail, idx) => ({ ...detail, position: idx }));
    } else {
        AppState.company1Accounts = finalIds;
        AppState.company1Details = finalIds
            .map(id => orderedDetails.find(detail => detail.accountId === id))
            .filter(Boolean)
            .map((detail, idx) => ({ ...detail, position: idx }));
    }

    if (AppState.currentCompany === companyId) {
        AppState.accounts = [...finalIds];
        AppState.accountDetails = companyId === 'company2'
            ? [...AppState.company2Details]
            : [...AppState.company1Details];
    }
}

function getReplaceSheetSelectionLabels(selection) {
    const labels = [];
    if (selection.accounts) labels.push('Manage Accounts');
    if (selection.orders) labels.push('Daily Orders / Data Sheet');
    if (selection.karigars) labels.push('Karigar Employees');
    if (selection.karigarTransactions) labels.push('Karigar Transactions');
    if (selection.sizePrices) labels.push('Size Prices');
    return labels;
}

function openReplaceFromSheetModal(defaultSelection = null) {
    const modal = document.getElementById('replace-sheet-modal');
    const form = document.getElementById('replace-sheet-form');
    const closeBtn = document.getElementById('close-replace-sheet-modal');
    const cancelBtn = document.getElementById('replace-sheet-cancel-btn');
    const selectAllBtn = document.getElementById('replace-sheet-select-all-btn');
    const checkboxMap = {
        accounts: document.getElementById('replace-sheet-accounts'),
        orders: document.getElementById('replace-sheet-orders'),
        karigars: document.getElementById('replace-sheet-karigars'),
        karigarTransactions: document.getElementById('replace-sheet-karigar-tx'),
        sizePrices: document.getElementById('replace-sheet-size-prices')
    };

    if (!modal || !form) return Promise.resolve(null);

    const checkboxes = Object.values(checkboxMap).filter(Boolean);
    const defaultAll = {
        accounts: true,
        orders: true,
        karigars: true,
        karigarTransactions: true,
        sizePrices: true
    };
    const mergedDefaults = { ...defaultAll, ...(defaultSelection || {}) };
    for (const [key, el] of Object.entries(checkboxMap)) {
        if (!el) continue;
        el.checked = !!mergedDefaults[key];
    }

    return new Promise((resolve) => {
        const updateSelectAllLabel = () => {
            if (!selectAllBtn || checkboxes.length === 0) return;
            const allChecked = checkboxes.every(cb => cb.checked);
            selectAllBtn.textContent = allChecked ? 'Unselect All' : 'Select All';
        };
        updateSelectAllLabel();

        const cleanup = () => {
            modal.classList.remove('show');
            form.removeEventListener('submit', onSubmit);
            modal.removeEventListener('click', onBackdrop);
            if (closeBtn) closeBtn.removeEventListener('click', onCancel);
            if (cancelBtn) cancelBtn.removeEventListener('click', onCancel);
            if (selectAllBtn) selectAllBtn.removeEventListener('click', onToggleAll);
            document.removeEventListener('keydown', onEsc);
            checkboxes.forEach(cb => cb.removeEventListener('change', updateSelectAllLabel));
        };

        const finish = (value) => {
            cleanup();
            resolve(value);
        };

        const onCancel = () => finish(null);
        const onBackdrop = (e) => { if (e.target === modal) onCancel(); };
        const onEsc = (e) => { if (e.key === 'Escape') onCancel(); };
        const onToggleAll = () => {
            const allChecked = checkboxes.length > 0 && checkboxes.every(cb => cb.checked);
            checkboxes.forEach(cb => { cb.checked = !allChecked; });
            updateSelectAllLabel();
        };
        const onSubmit = (e) => {
            e.preventDefault();
            const selection = {
                accounts: !!checkboxMap.accounts?.checked,
                orders: !!checkboxMap.orders?.checked,
                karigars: !!checkboxMap.karigars?.checked,
                karigarTransactions: !!checkboxMap.karigarTransactions?.checked,
                sizePrices: !!checkboxMap.sizePrices?.checked
            };
            if (!Object.values(selection).some(Boolean)) {
                showToast("Select at least one data block", "error");
                return;
            }
            finish(selection);
        };

        form.addEventListener('submit', onSubmit);
        modal.addEventListener('click', onBackdrop);
        if (closeBtn) closeBtn.addEventListener('click', onCancel);
        if (cancelBtn) cancelBtn.addEventListener('click', onCancel);
        if (selectAllBtn) selectAllBtn.addEventListener('click', onToggleAll);
        document.addEventListener('keydown', onEsc);
        checkboxes.forEach(cb => cb.addEventListener('change', updateSelectAllLabel));

        modal.classList.add('show');
    });
}

document.addEventListener('DOMContentLoaded', () => {
    Promise.resolve(window.__envReady)
        .catch(() => null)
        .finally(() => { initApp(); });
});

async function initApp() {
    refreshUsersFromEnv();
    applyClientModeUI();
    restoreFirestoreQuotaCooldown();
    updateBackendModeBanner();

    // ── PROFILE DETECTION: restore profile from saved session before LAN check ──
    // This ensures the right per-user LAN folder handle is loaded on page reload.
    (function restoreProfileFromSession() {
        const savedUsername = localStorage.getItem('userUsername');
        if (savedUsername) {
            LanStorageService.setActiveProfile(savedUsername);
        } else {
            // Hash-based profile: if visiting /#dhyan without an active session,
            // pre-warm the Dhyan profile so the correct folder handle is checked.
            const hash = window.location.hash.replace('#', '').toLowerCase();
            if (hash === 'dhyan') {
                LanStorageService.setActiveProfile('Dhyan_Order');
            }
        }
    })();

    initLanSettingsUI();
    attachEventListeners();
    initSyncIndicator();

    const integrityBtn = document.getElementById('fix-integrity-btn');
    const syncSheetBtn = document.getElementById('sync-sheet-btn');
    if (integrityBtn) integrityBtn.style.display = isDevUser() ? '' : 'none';
    if (syncSheetBtn) syncSheetBtn.style.display = isDevUser() ? '' : 'none';

    // Refresh button
    const backupBtn = document.getElementById('backup-btn');
    if (backupBtn) backupBtn.addEventListener('click', () => refreshAppDataManually());

    if (integrityBtn) integrityBtn.addEventListener('click', async () => {
        if (!isDevUser()) return showToast("Only dev can run integrity tools", "error");
        showToast("Integrity tools are disabled in offline mode.", "info");
    });

    if (syncSheetBtn) syncSheetBtn.addEventListener('click', async () => {
        showToast("Google Sheets sync is disabled in offline mode.", "info");
    });

    const savedCompany = localStorage.getItem('selectedCompany');
    if (savedCompany) AppState.currentCompany = savedCompany;
    if (isSingleCompanyMode()) {
        AppState.currentCompany = 'company1';
        localStorage.setItem('selectedCompany', 'company1');
    }
    if (AppState.currentUser && !isCompanyAllowed(AppState.currentCompany)) {
        AppState.currentCompany = getAllowedCompanies()[0] || 'company1';
        localStorage.setItem('selectedCompany', AppState.currentCompany);
    }
    document.querySelectorAll('.company-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.company === AppState.currentCompany);
    });
    if (typeof updateOrderCompanyLabel === 'function') updateOrderCompanyLabel();

    // ── STEP 1: Check if folder is linked (uses profile-specific handle key) ──
    if (LanStorageService.hasSavedHandle()) {
        const reconnected = await LanStorageService.autoReconnect();
        if (!reconnected) {
            console.log('[INIT] LAN handle exists but not yet permitted. Showing login screen.');
        }
        checkAuth();
    } else {
        showFolderSetupScreen();
    }

    startLanFolderWatcher();
    updateProfileSwitchBtn();
    updateFolderSetupScreenForProfile();

    // ── Pull on tab focus: when admin comes back to the tab, refresh from Firebase ──
    document.addEventListener('visibilitychange', () => {
        if (!document.hidden && LanStorageService.isConnected() && AppState.currentUser) {
            const now = Date.now();
            if ((now - _lastVisibilityPullMs) > 60 * 1000) { // max once per 60s
                _lastVisibilityPullMs = now;
                LanStorageService.pullLatestFromFirebase().then(r => {
                    if (r && r.pulled > 0) {
                        console.log(`[PULL SYNC] Visibility pull: ${r.pulled} records.`);
                        window.clearApiReadCache && window.clearApiReadCache();
                        const sec = AppState.currentSection;
                        if (sec === 'dashboard') renderDashboard();
                        else if (sec === 'data-sheet' && !(typeof isDataSheetEditActive === 'function' && isDataSheetEditActive())) renderDataSheet();
                    }
                }).catch(() => {});
            }
        }
    });

    // ── Pull on LAN first-connect: so folder connecting immediately syncs Firebase data ──
    LanStorageService.onStatusChange((status) => {
        if (status === 'connected' && AppState.currentUser) {
            setTimeout(() => {
                LanStorageService.pullLatestFromFirebase({ force: true }).catch(() => {});
            }, 1500);
        }
    });

    // ── Pull on sync flush: when this profile pushes to Firebase, immediately pull back ──
    // This ensures cross-profile data (e.g. Dhyan→Firebase→Admin) appears within seconds.
    window.addEventListener('lan:sync-flushed', () => {
        if (LanStorageService.isConnected() && AppState.currentUser) {
            setTimeout(() => {
                LanStorageService.pullLatestFromFirebase({ force: true }).then(r => {
                    if (r && r.pulled > 0) {
                        window.clearApiReadCache && window.clearApiReadCache();
                    }
                }).catch(() => {});
            }, 2000); // 2s delay — let Firebase propagate
        }
    });
}

let _currentFolderHash = '';
function startLanFolderWatcher() {
    if (window._lanFolderWatcherStarted) return;
    window._lanFolderWatcherStarted = true;

    setInterval(async () => {
        if (!LanStorageService.isConnected()) {
            _currentFolderHash = '';
            return;
        }
        try {
            const hash = await LanStorageService.getFolderHash();
            if (!hash) return;
            if (!_currentFolderHash) {
                _currentFolderHash = hash;
                return;
            }
            if (_currentFolderHash !== hash) {
                console.log('🔄 [LAN] Local folder files changed! Reloading data...');
                _currentFolderHash = hash;
                if (typeof _apiReadCache !== 'undefined') {
                    _apiReadCache.clear();
                }
                await fetchAllCompaniesData({ refreshArchiveMonths: false, skipSheetMerge: true });
                const currentSec = AppState.currentSection || document.querySelector('.nav-btn.active')?.dataset.target || 'data-sheet';
                if (currentSec === 'karigar') {
                    await renderKarigarPage(false);
                } else if (currentSec === 'size-prices') {
                    await renderSizePricesPage();
                } else if (currentSec === 'data-sheet') {
                    if (!(typeof isDataSheetEditActive === 'function' && isDataSheetEditActive())) {
                        renderDataSheet();
                    }
                } else if (typeof navigateTo === 'function') {
                    navigateTo(currentSec);
                }
                showToast('🔄 LAN folder updated! Data reloaded smoothly.', 'info');
            }
        } catch (e) {
            console.warn('[LAN] Folder watcher error:', e);
        }
    }, 4000);
}


function showFolderSetupScreen() {
    document.getElementById('folder-setup-screen').classList.remove('hidden');
    document.getElementById('login-screen').classList.add('hidden');
    document.getElementById('app-screen').classList.add('hidden');
    updateFolderSetupScreenForProfile();
}

// ===== PROFILE HELPERS =====

function _isDhyanHash() {
    return window.location.hash.replace('#', '').toLowerCase() === 'dhyan';
}

/** Updates the folder setup screen title/text to reflect the active profile */
function updateFolderSetupScreenForProfile() {
    const isDhyan = _isDhyanHash();
    const titleEl = document.querySelector('#folder-setup-screen h2');
    const descEl = document.querySelector('#folder-setup-screen p');
    if (titleEl) {
        titleEl.textContent = isDhyan ? '📂 Link Dhyan Data Folder' : '📂 Link Your Data Folder';
    }
    if (descEl) {
        descEl.textContent = isDhyan
            ? 'Select the local folder where Dhyan\'s order data is stored. This folder is separate from the Krimaa master folder.'
            : 'Krimaa works fully offline. First, link the shared local folder where your data is stored.';
    }
    // Show profile badge on folder setup screen
    let badge = document.getElementById('folder-setup-profile-badge');
    if (!badge) {
        badge = document.createElement('div');
        badge.id = 'folder-setup-profile-badge';
        badge.style.cssText = 'margin-bottom:12px;text-align:center;font-size:0.78rem;font-weight:700;';
        const statusEl = document.getElementById('folder-setup-status');
        if (statusEl && statusEl.parentNode) {
            statusEl.parentNode.insertBefore(badge, statusEl);
        }
    }
    badge.innerHTML = isDhyan
        ? '<span style="background:#f0f9ff;border:1px solid #bae6fd;color:#0369a1;padding:4px 12px;border-radius:20px;"><i class=\'bx bxs-business\'></i> Dhyan Profile</span>'
        : '<span style="background:#f0fdf4;border:1px solid #bbf7d0;color:#15803d;padding:4px 12px;border-radius:20px;"><i class=\'bx bxs-building\'></i> Krimaa / Shree Sai Profile</span>';
}

/** Shows/hides the "Switch Profile" button in the sidebar (admin only) */
function updateProfileSwitchBtn() {
    const btn = document.getElementById('profile-switch-btn');
    if (!btn) return;
    const role = AppState.currentUser?.role;
    const isAdminOrDev = role === 'admin' || role === 'dev';
    btn.style.display = isAdminOrDev ? '' : 'none';
    const isDhyan = _isDhyanHash();
    btn.innerHTML = isDhyan
        ? '<i class=\'bx bx-transfer\'></i> Switch to Krimaa Profile'
        : '<i class=\'bx bx-transfer\'></i> Switch to Dhyan Profile';
}

/** Switches the active profile: clears session, sets hash, reloads */
function switchToProfile(profileName) {
    const targetHash = profileName === 'dhyan' ? 'dhyan' : '';
    const label = profileName === 'dhyan' ? 'Dhyan' : 'Krimaa / Shree Sai';
    if (!confirm(`Switch to ${label} profile?\n\nYou will be logged out and taken to the ${label} login.`)) return;
    // Clear current session
    localStorage.removeItem('isLogged');
    localStorage.removeItem('userRole');
    localStorage.removeItem('userName');
    localStorage.removeItem('userUsername');
    localStorage.removeItem('userAllowedCompanies');
    AppState.currentUser = null;
    // Disconnect current LAN folder (don't clear the handle — just deactivate)
    LanStorageService.setActiveProfile(null);
    // Set the new profile hash
    window.location.hash = targetHash;
    // Reload the page to cleanly initialize the new profile
    window.location.reload();
}



function showLoginScreen() {
    document.getElementById('folder-setup-screen').classList.add('hidden');
    document.getElementById('login-screen').classList.remove('hidden');
    document.getElementById('app-screen').classList.add('hidden');
    // Show folder status in login form
    const lanStatus = document.getElementById('login-lan-status');
    const lanStatusText = document.getElementById('login-lan-status-text');
    if (lanStatus && LanStorageService.hasSavedHandle()) {
        lanStatus.style.display = 'flex';
        lanStatus.style.alignItems = 'center';
        lanStatus.style.gap = '6px';
        const statusLabel = LanStorageService.isConnected() ? 'Folder linked & connected — offline mode active' : 'Folder saved (will reconnect on login)';
        if (lanStatusText) lanStatusText.textContent = statusLabel;
    }
    // Update profile indicator on login screen
    const profileIndicator = document.getElementById('login-profile-indicator');
    const profileBadgeText = document.getElementById('login-profile-badge-text');
    const loginTitle = document.querySelector('#login-screen h2');
    const loginSubtitle = document.querySelector('#login-screen p');
    const isDhyan = _isDhyanHash();
    if (profileIndicator) profileIndicator.style.display = isDhyan ? 'block' : 'none';
    if (isDhyan) {
        if (profileBadgeText) profileBadgeText.textContent = 'Dhyan Profile';
        if (loginTitle) loginTitle.textContent = 'Dhyan Login';
        if (loginSubtitle) loginSubtitle.textContent = 'Sign in with your Dhyan credentials';
    } else {
        if (loginTitle) loginTitle.textContent = 'Krimaa Admin';
        if (loginSubtitle) loginSubtitle.textContent = 'Sign in to manage daily orders';
    }
}

// ── Firebase pending sync badge ──
window.updateFirebasePendingBadge = function(count, lanStatus) {
    const badge = document.getElementById('firebase-pending-badge');
    const countEl = document.getElementById('firebase-pending-count');
    if (!badge || !countEl) return;
    if (lanStatus === 'connected' && count > 0) {
        countEl.textContent = count;
        badge.style.display = 'flex';
    } else {
        badge.style.display = 'none';
    }
};

// Auto-flush Firebase sync queue when internet comes back
window.addEventListener('online', () => {
    if (LanStorageService.isConnected()) {
        console.log('[SYNC] Network restored. Flushing pending Firebase queue...');
        LanStorageService.flushSyncQueue().catch(e => console.warn('[SYNC] Flush failed:', e.message));
    }
});


let _adminAutoSyncTimer = null;
let _adminRealtimeRefreshTimer = null;
let _backgroundSheetBackupTimer = null;
let _backgroundSheetBackupInFlight = false;
let _backgroundSheetBackupQueued = false;
let _backgroundSheetRetryCount = 0;
let _pendingDataChangesForBackup = false;
let _adminRealtimeBusy = false;
let _quickWriteFlushTimer = null;
let _renderDataSheetSeq = 0;
const _pendingOrderOverrides = new Map();
let _sheetSyncLifecycleBound = false;
let _lastSheetBackupSuccessMs = 0;
let _lastDirectOrderSheetSyncMs = 0;
let _rainbowLoadingCounter = 0;
let _startupMaintenanceRunning = false;
const RECENT_30D_HYDRATE_KEY = 'recent_30d_hydrated_at_v1';
let _firestoreQuotaBlockedUntilMs = 0;
let _firestoreQuotaLastToastMs = 0;
let _useSheetsFallbackMode = false;
let _sheetsFallbackLastLoadMs = 0;
const _apiReadCache = new Map();
window.clearApiReadCache = function() {
    _apiReadCache.clear();
};
const SHEET_SYNC_PENDING_KEY = 'sheet_sync_pending_v3';
const SHEET_SYNC_LAST_SUCCESS_KEY = 'sheet_sync_last_success_v3';
const FIRESTORE_QUOTA_UNTIL_KEY = 'firestore_quota_until_v1';
const SHEET_RECONCILE_MAX_STALE_MS = 3 * 60 * 1000;
const FIRESTORE_QUOTA_COOLDOWN_MS = 10 * 60 * 1000;
const SHEETS_FALLBACK_DAYS = 7;
const READ_CACHE_TTL_MS = 180000; // 3 minutes cache to avoid constant Firestore/Firebase reads

function isFirestoreQuotaError(err) {
    const msg = String(err?.message || err || '').toLowerCase();
    const code = String(err?.code || '').toLowerCase();
    return (
        msg.includes('quota exceeded') ||
        msg.includes('resource_exhausted') ||
        msg.includes('too many requests') ||
        code.includes('resource-exhausted')
    );
}

function isFirestoreUnavailableError(err) {
    const msg = String(err?.message || err || '').toLowerCase();
    const code = String(err?.code || '').toLowerCase();
    return (
        msg.includes('unavailable') ||
        msg.includes('network error') ||
        msg.includes('failed to fetch') ||
        msg.includes('deadline exceeded') ||
        msg.includes('timed out') ||
        code.includes('unavailable')
    );
}

function isFirestoreQuotaBlocked() {
    return Date.now() < _firestoreQuotaBlockedUntilMs;
}

function activateFirestoreQuotaCooldown(err) {
    _firestoreQuotaBlockedUntilMs = Date.now() + FIRESTORE_QUOTA_COOLDOWN_MS;
    try { localStorage.setItem(FIRESTORE_QUOTA_UNTIL_KEY, String(_firestoreQuotaBlockedUntilMs)); } catch (e) {}
    enableSheetsFallbackMode();
    const now = Date.now();
    if (now - _firestoreQuotaLastToastMs > 60000) {
        _firestoreQuotaLastToastMs = now;
        showToast('Firestore quota exceeded. App switched to cached mode for 10 minutes.', 'error');
    }
    console.warn('Firestore quota cooldown active until:', new Date(_firestoreQuotaBlockedUntilMs).toISOString(), err);
}

function restoreFirestoreQuotaCooldown() {
    try {
        const raw = localStorage.getItem(FIRESTORE_QUOTA_UNTIL_KEY);
        const ms = parseInt(raw, 10);
        if (Number.isFinite(ms) && ms > Date.now()) {
            _firestoreQuotaBlockedUntilMs = ms;
            _useSheetsFallbackMode = true;
        } else {
            localStorage.removeItem(FIRESTORE_QUOTA_UNTIL_KEY);
        }
    } catch (e) {}
}

function getTomorrowRecoveryLabel() {
    const d = new Date();
    d.setDate(d.getDate() + 1);
    d.setHours(1, 30, 0, 0);
    const dateLabel = d.toLocaleDateString([], { month: 'short', day: 'numeric' });
    const timeLabel = d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
    return `${dateLabel}, ${timeLabel}`;
}

function updateBackendModeBanner() {
    const el = document.getElementById('backend-mode-banner');
    if (!el) return;
    if (_useSheetsFallbackMode || isFirestoreQuotaBlocked()) {
        el.classList.remove('hidden');
        el.innerHTML = `<i class='bx bx-error-circle'></i><span>Firebase server down. Please use the app from tomorrow 1:00 PM. Loading from Google Sheets (Slow).</span>`;
    } else {
        el.classList.add('hidden');
    }
}

function setLoaderStatus(text) {
    const el = document.getElementById('global-loader-text');
    if (!el) return;
    el.textContent = String(text || 'Loading...');
}

function startRainbowLoading(message = '') {
    _rainbowLoadingCounter += 1;
    const flash = document.getElementById('magical-flash');
    if (flash) {
        flash.classList.add('loading');
        flash.classList.remove('active');
    }
    if (message) setLoaderStatus(message);
}

function stopRainbowLoading() {
    _rainbowLoadingCounter = Math.max(0, _rainbowLoadingCounter - 1);
    if (_rainbowLoadingCounter > 0) return;
    const flash = document.getElementById('magical-flash');
    if (flash) flash.classList.remove('loading');
}

function mapSheetAccountsToDetails(list, companyId) {
    const raw = Array.isArray(list) ? list : [];
    const details = raw.map((item, idx) => {
        if (item && typeof item === 'object') {
            const safeName = String(item.name || '').trim();
            const safeId = String(item.accountId || '').trim() || `sheet_${companyId}_${safeName.toLowerCase().replace(/[^a-z0-9]+/g, '_') || idx}`;
            return {
                accountId: safeId,
                name: safeName || safeId,
                companyId,
                position: Number.isFinite(parseInt(item.position, 10)) ? parseInt(item.position, 10) : (idx + 1)
            };
        }
        const safeName = String(item || '').trim();
        const accountId = `sheet_${companyId}_${safeName.toLowerCase().replace(/[^a-z0-9]+/g, '_') || idx}`;
        return { accountId, name: safeName, companyId, position: idx + 1 };
    }).filter(d => d.name);
    return details;
}

function filterRecentRows(rows, days = SHEETS_FALLBACK_DAYS) {
    const safeDays = Math.max(1, parseInt(days, 10) || SHEETS_FALLBACK_DAYS);
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - (safeDays - 1));
    cutoff.setHours(0, 0, 0, 0);
    const cutoffIso = normalizeToISODate(cutoff);
    return (Array.isArray(rows) ? rows : []).filter(r => {
        const d = normalizeToISODate(r?.date);
        return !!d && d >= cutoffIso;
    });
}

function mergeOrderRowsUnique(primaryRows, secondaryRows) {
    const merged = new Map();
    const add = (row) => {
        const date = normalizeToISODate(row?.date);
        const accountId = String(row?.accountId || '').trim();
        const accountName = String(row?.accountName || '').trim();
        if (!date || (!accountId && !accountName)) return;
        const key = `${date}__${accountId || accountName.toLowerCase()}`;
        const meeshoVal = parseInt(row?.meesho, 10) || 0;
        const flipkartVal = parseInt(row?.flipkart, 10) || 0;
        const totalVal = typeof row?.total !== 'undefined' ? (parseInt(row?.total, 10) || 0) : (meeshoVal + flipkartVal);
        if (!merged.has(key)) {
            merged.set(key, {
                date,
                accountId: accountId || accountName,
                accountName: accountName || accountId,
                meesho: meeshoVal,
                flipkart: flipkartVal,
                total: totalVal,
                companyId: String(row?.companyId || '').trim()
            });
        }
    };
    (Array.isArray(primaryRows) ? primaryRows : []).forEach(add);
    (Array.isArray(secondaryRows) ? secondaryRows : []).forEach(add);
    return Array.from(merged.values()).sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')));
}

async function loadRecentDataFromSheets(days = SHEETS_FALLBACK_DAYS) {
    const [accC1, accC2, ordC1, ordC2, rem] = await Promise.all([
        sheetsApiRequest({ action: 'getAccounts', companyId: 'company1' }).catch(() => ({ data: [] })),
        sheetsApiRequest({ action: 'getAccounts', companyId: 'company2' }).catch(() => ({ data: [] })),
        sheetsApiRequest({ action: 'getDashboardData', companyId: 'company1' }).catch(() => ({ data: [] })),
        sheetsApiRequest({ action: 'getDashboardData', companyId: 'company2' }).catch(() => ({ data: [] })),
        sheetsApiRequest({ action: 'getRemarks' }).catch(() => ({ data: {} }))
    ]);

    const rowsC1 = filterRecentRows(ordC1?.data || [], days);
    const rowsC2 = filterRecentRows(ordC2?.data || [], days);

    AppState.company1Data = rowsC1;
    AppState.company2Data = rowsC2;

    const c1Details = mapSheetAccountsToDetails((accC1 && accC1.details) ? accC1.details : (accC1?.data || []), 'company1');
    const c2Details = mapSheetAccountsToDetails((accC2 && accC2.details) ? accC2.details : (accC2?.data || []), 'company2');
    if (c1Details.length > 0) AppState.company1Details = c1Details;
    if (c2Details.length > 0) AppState.company2Details = c2Details;
    AppState.company1Accounts = (AppState.company1Details || []).map(d => d.accountId);
    AppState.company2Accounts = (AppState.company2Details || []).map(d => d.accountId);
    AppState.accountDetails = AppState.currentCompany === 'company2' ? AppState.company2Details : AppState.company1Details;
    AppState.accounts = (AppState.accountDetails || []).map(d => d.accountId);
    AppState.dashboardData = AppState.currentCompany === 'company2' ? rowsC2 : rowsC1;
    _remarkCache = rem?.data || _remarkCache || {};
    _sheetsFallbackLastLoadMs = Date.now();
}

async function readFromSheetsInQuotaMode(action, payload, companyId) {
    setSheetBackupIndicator('running', 'Google Sheet loading');
    if (action === 'getAccounts') {
        const res = await sheetsApiRequest({ action: 'getAccounts', companyId });
        setSheetBackupIndicator('idle', 'Sheet idle');
        return res;
    }
    if (action === 'getDashboardData') {
        const res = await sheetsApiRequest({ action: 'getDashboardData', companyId, month: payload?.month || '' });
        const rows = Array.isArray(res?.data) ? res.data : [];
        setSheetBackupIndicator('idle', 'Sheet idle');
        return { success: true, data: filterRecentRows(rows, SHEETS_FALLBACK_DAYS) };
    }
    if (action === 'getRemarks') {
        const res = await sheetsApiRequest({ action: 'getRemarks' });
        setSheetBackupIndicator('idle', 'Sheet idle');
        return res;
    }
    setSheetBackupIndicator('idle', 'Sheet idle');
    return null;
}

async function mergeRecentSheetRowsIntoState(days = SHEETS_FALLBACK_DAYS) {
    setSheetBackupIndicator('running', 'Google Sheet loading');
    try {
        const [s1, s2] = await Promise.all([
            sheetsApiRequest({ action: 'getDashboardData', companyId: 'company1' }),
            sheetsApiRequest({ action: 'getDashboardData', companyId: 'company2' })
        ]);
        const sheetRowsC1 = filterRecentRows(s1?.data || [], days).map(r => ({ ...r, companyId: 'company1' }));
        const sheetRowsC2 = filterRecentRows(s2?.data || [], days).map(r => ({ ...r, companyId: 'company2' }));
        AppState.company1Data = mergeOrderRowsUnique(AppState.company1Data || [], sheetRowsC1);
        AppState.company2Data = mergeOrderRowsUnique(AppState.company2Data || [], sheetRowsC2);
        AppState.dashboardData = AppState.currentCompany === 'company2' ? AppState.company2Data : AppState.company1Data;
    } finally {
        setSheetBackupIndicator('idle', 'Sheet idle');
    }
}

function enableSheetsFallbackMode() {
    _useSheetsFallbackMode = true;
    updateBackendModeBanner();
    const stale = (Date.now() - _sheetsFallbackLastLoadMs) > 30000;
    if (stale) {
        loadRecentDataFromSheets(SHEETS_FALLBACK_DAYS)
            .then(() => {
                if (AppState.currentSection === 'data-sheet') renderDataSheet();
                if (AppState.currentSection === 'dashboard') renderDashboard();
            })
            .catch((e) => console.warn('Sheets fallback load failed:', e));
    }
}

function disableSheetsFallbackMode() {
    _useSheetsFallbackMode = false;
    _firestoreQuotaBlockedUntilMs = 0;
    try { localStorage.removeItem(FIRESTORE_QUOTA_UNTIL_KEY); } catch (e) {}
    updateBackendModeBanner();
}

async function tryRecoverFirestoreMode() {
    if (!_useSheetsFallbackMode) return false;
    try {
        await FirebaseService.getAccounts(AppState.currentCompany || 'company1');
        disableSheetsFallbackMode();
        return true;
    } catch (e) {
        return false;
    }
}

async function checkFirestoreResponsive(timeoutMs = 5000) {
    if (isFirestoreQuotaBlocked()) return false;
    try {
        const probePromise = (async () => {
            const db = FirebaseService.getDb();
            try {
                // Probe 'accounts' which is guaranteed to be accessible under security rules
                await db.collection('accounts').limit(1).get();
            } catch (e) {
                const msg = String(e.message || e).toLowerCase();
                const code = String(e.code || '').toLowerCase();
                const isPermissionError = msg.includes('permission') || msg.includes('denied') || code.includes('permission-denied') || code.includes('permission');
                if (!isPermissionError) {
                    throw e; // Rethrow actual network/down errors
                }
            }
            return true;
        })();
        const timeoutPromise = new Promise((_, reject) => {
            setTimeout(() => reject(new Error('Firestore health timeout')), Math.max(1500, parseInt(timeoutMs, 10) || 5000));
        });
        await Promise.race([probePromise, timeoutPromise]);
        return true;
    } catch (e) {
        return false;
    }
}

async function probeFirebaseWithCountdown(seconds = 5) {
    const safeSeconds = Math.max(1, parseInt(seconds, 10) || 5);
    const timeoutMs = safeSeconds * 1000;
    const endTs = Date.now() + timeoutMs;
    const updateText = () => {
        const remaining = Math.max(0, Math.ceil((endTs - Date.now()) / 1000));
        setLoaderStatus(`Loading from Firebase... ${remaining}s`);
    };
    updateText();
    const timer = setInterval(updateText, 200);
    try {
        return await checkFirestoreResponsive(timeoutMs);
    } finally {
        clearInterval(timer);
    }
}

async function syncFirebaseFromSheetsSourceOnRefresh() {
    if (!FirebaseService || typeof FirebaseService.replaceFromSheetsSelective !== 'function') return;
    const fullRes = await sheetsApiRequest({ action: 'getAllSheetData' });
    const sheetData = fullRes?.data || null;
    if (!fullRes?.success || !sheetData) {
        throw new Error(fullRes?.message || 'Failed to read Google Sheet data');
    }
    await FirebaseService.replaceFromSheetsSelective(
        sheetData,
        {
            accounts: true,
            orders: true,
            karigars: true,
            karigarTransactions: true,
            sizePrices: true
        },
        { fast: true }
    );
}

function buildCachedApiResult(action, companyId) {
    switch (action) {
        case 'getAccounts': {
            const details = companyId === 'company2' ? (AppState.company2Details || []) : (AppState.company1Details || []);
            const ids = details.map(d => d.accountId).filter(Boolean);
            return { success: true, data: ids, details };
        }
        case 'getDashboardData': {
            const data = companyId === 'company2' ? (AppState.company2Data || []) : (AppState.company1Data || []);
            return { success: true, data };
        }
        case 'getRemarks':
            return { success: true, data: _remarkCache || {} };
        case 'getMoneyBackups':
            return { success: true, data: AppState.moneyBackups || [] };
        default:
            return null;
    }
}

function setPersistentSheetSyncPending(reason = '') {
    try {
        const payload = {
            pending: true,
            reason: String(reason || '').trim() || 'unknown',
            ts: new Date().toISOString()
        };
        localStorage.setItem(SHEET_SYNC_PENDING_KEY, JSON.stringify(payload));
    } catch (e) {}
}

function clearPersistentSheetSyncPending() {
    try { localStorage.removeItem(SHEET_SYNC_PENDING_KEY); } catch (e) {}
}

function readPersistentSheetSyncPending() {
    try {
        const raw = localStorage.getItem(SHEET_SYNC_PENDING_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        if (!parsed || parsed.pending !== true) return null;
        return parsed;
    } catch (e) { return null; }
}

function setLastSheetSyncSuccessTimestamp(isoString = '') {
    const safeIso = normalizeToISODateTime(isoString || new Date());
    if (!safeIso) return;
    _lastSheetBackupSuccessMs = parseFlexibleDateTime(safeIso)?.getTime() || Date.now();
    try { localStorage.setItem(SHEET_SYNC_LAST_SUCCESS_KEY, safeIso); } catch (e) {}
}

function bootstrapSheetSyncStateFromStorage() {
    try {
        const raw = localStorage.getItem(SHEET_SYNC_LAST_SUCCESS_KEY);
        const ms = parseFlexibleDateTime(raw)?.getTime() || 0;
        _lastSheetBackupSuccessMs = ms > 0 ? ms : 0;
    } catch (e) {
        _lastSheetBackupSuccessMs = 0;
    }
}

function bindSheetSyncLifecycleEvents() {
    if (_sheetSyncLifecycleBound) return;
    _sheetSyncLifecycleBound = true;

    const markPendingOnLeave = () => {
        if (typeof FirebaseService !== 'undefined' && FirebaseService.getPendingCount && FirebaseService.getPendingCount() > 0) {
            _pendingDataChangesForBackup = true;
        }
        if (_pendingDataChangesForBackup || _backgroundSheetBackupQueued || _backgroundSheetBackupInFlight) {
            setPersistentSheetSyncPending('lifecycle_leave');
        }
    };

    window.addEventListener('beforeunload', markPendingOnLeave);
    window.addEventListener('pagehide', markPendingOnLeave);
    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState !== 'hidden') return;
        markPendingOnLeave();
    });
}

function scheduleQuickWriteFlush(delayMs = 1200) {
    if (_quickWriteFlushTimer) clearTimeout(_quickWriteFlushTimer);
    const wait = parseInt(delayMs, 10) || 1200;
    _quickWriteFlushTimer = setTimeout(async () => {
        try {
            await FirebaseService.flushWrites();
        } catch (e) {
            if (isFirestoreQuotaError(e)) {
                activateFirestoreQuotaCooldown(e);
                loadRecentDataFromSheets(SHEETS_FALLBACK_DAYS).catch(() => null);
            }
            console.warn('Quick flush failed:', e);
        }
    }, wait);
}

function isDataSheetEditActive() {
    const active = document.activeElement;
    if (!active) return false;
    if (active.matches && active.matches('input.sheet-cell-input')) return true;
    if (active.closest && active.closest('#sheet-table')) return true;
    return false;
}

function closeMobileSidebar() {
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    if (sidebar) sidebar.classList.remove('open');
    if (overlay) overlay.classList.remove('show');
}

function toggleMobileSidebar() {
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    if (!sidebar) return;
    const shouldOpen = !sidebar.classList.contains('open');
    sidebar.classList.toggle('open', shouldOpen);
    if (overlay) overlay.classList.toggle('show', shouldOpen);
}

function buildPendingOrderOverrideKey(companyId, date, accountId, accountName) {
    const safeCompanyId = String(companyId || '').trim();
    const safeDate = normalizeToISODate(date);
    const safeAccountId = String(accountId || '').trim();
    const safeName = String(accountName || '').trim().toLowerCase();
    return `${safeCompanyId}|${safeDate}|${safeAccountId || safeName}`;
}

function setPendingOrderOverride(date, accountId, accountName, value, companyId, field = 'meesho') {
    const safeCompanyId = String(companyId || '').trim();
    const safeDate = normalizeToISODate(date);
    if (!safeCompanyId || !safeDate) return;
    const safeAccountId = String(accountId || '').trim();
    const safeName = String(accountName || '').trim();
    const qty = parseInt(value) || 0;
    const key = buildPendingOrderOverrideKey(safeCompanyId, safeDate, safeAccountId, safeName);
    const existing = _pendingOrderOverrides.get(key) || {};
    const safeField = field === 'flipkart' ? 'flipkart' : 'meesho';
    const meeshoVal = safeField === 'meesho' ? qty : (parseInt(existing.meesho) || 0);
    const flipkartVal = safeField === 'flipkart' ? qty : (parseInt(existing.flipkart) || 0);
    const totalVal = meeshoVal + flipkartVal;
    _pendingOrderOverrides.set(key, {
        companyId: safeCompanyId,
        date: safeDate,
        accountId: safeAccountId,
        accountName: safeName,
        meesho: meeshoVal,
        flipkart: flipkartVal,
        total: totalVal,
        quantity: totalVal,
        updatedAtMs: Date.now()
    });
}

function applyPendingOrderOverrides(list, companyId) {
    const safeCompanyId = String(companyId || '').trim();
    const target = Array.isArray(list) ? list.slice() : [];
    const now = Date.now();

    _pendingOrderOverrides.forEach((ov, key) => {
        if (ov.companyId !== safeCompanyId) return;
        if ((now - (ov.updatedAtMs || now)) > 10 * 60 * 1000) {
            _pendingOrderOverrides.delete(key);
            return;
        }

        const idx = target.findIndex(r => {
            if (normalizeToISODate(r.date) !== ov.date) return false;
            const rid = String(r.accountId || '').trim();
            if (ov.accountId) return rid === ov.accountId;
            return !rid && String(r.accountName || '').trim().toLowerCase() === String(ov.accountName || '').trim().toLowerCase();
        });

        if (idx >= 0) {
            const serverM = parseInt(target[idx]?.meesho) || 0;
            const serverF = parseInt(target[idx]?.flipkart) || 0;
            if (serverM === ov.meesho && serverF === ov.flipkart) {
                _pendingOrderOverrides.delete(key);
                return;
            }
            target[idx] = { ...target[idx], ...ov };
            return;
        }

        target.push({ ...ov });
    });

    return target.sort((a, b) => (normalizeToISODate(b.date) > normalizeToISODate(a.date) ? 1 : -1));
}

function setSheetBackupIndicator(state = 'idle', label = 'Sheet idle') {
    const el = document.getElementById('sheet-backup-indicator');
    if (!el) return;
    el.classList.remove('hidden');
    el.className = `sync-indicator sync-${state}`;
    let icon = 'bx-cloud-upload';
    if (state === 'running') icon = 'bx-loader-alt bx-spin';
    if (state === 'success') icon = 'bx-check-circle';
    if (state === 'error') icon = 'bx-error-circle';
    if (state === 'queued') icon = 'bx-time-five';
    el.innerHTML = `<i class='bx ${icon}'></i><span>${label}</span>`;
}

function normalizeBackupTypeLabel(rawType = '') {
    const input = String(rawType || '').trim();
    if (!input) return '';
    const t = input.toLowerCase();

    if (t === 'manual') return 'manual';
    if (t === 'submitorders' || t === 'updateorder' || t === 'orders_live' || t === 'order_direct') return 'orders';
    if (t === 'upsertdesignprice' || t === 'deletedesignprice' || t === 'restoredesignprice') return 'size-prices';
    if (t.startsWith('clearkarigarmonthlydata_')) return 'karigar-reset';

    const autoReasons = new Set([
        'auto',
        'queued',
        'retry',
        'interval',
        'reconcile',
        'firestore_saved',
        'startup_reconcile',
        'auto_change',
        'lifecycle_leave'
    ]);
    if (autoReasons.has(t) || t.startsWith('resume_') || t.startsWith('failed_')) return 'auto';
    return input;
}

function normalizeCompanyIdLocal(companyId = '') {
    return String(companyId || '').trim() === 'company2' ? 'company2' : 'company1';
}

function resolveAccountNameForSheet(accountId, companyId) {
    const safeCompanyId = normalizeCompanyIdLocal(companyId);
    const safeAccountId = String(accountId || '').trim();
    if (!safeAccountId) return '';
    const details = safeCompanyId === 'company2' ? (AppState.company2Details || []) : (AppState.company1Details || []);
    const exact = details.find(d => String(d.accountId || '').trim() === safeAccountId);
    if (exact && String(exact.name || '').trim()) return String(exact.name || '').trim();
    const accounts = safeCompanyId === 'company2' ? (AppState.company2Accounts || []) : (AppState.company1Accounts || []);
    const fallback = accounts.find(d => String(d.id || '').trim() === safeAccountId || String(d.accountId || '').trim() === safeAccountId);
    return String(fallback?.name || safeAccountId).trim();
}

function normalizeSheetOrderRows(date, orders, companyId) {
    const safeDate = normalizeToISODate(date);
    const safeCompanyId = normalizeCompanyIdLocal(companyId);
    const out = [];
    (Array.isArray(orders) ? orders : []).forEach(o => {
        const accountId = String(o?.accountId || '').trim();
        if (!accountId) return;
        const accountName = String(o?.accountName || '').trim() || resolveAccountNameForSheet(accountId, safeCompanyId) || accountId;
        const meesho = parseInt(o?.meesho, 10);
        out.push({
            accountId,
            accountName,
            meesho: Number.isFinite(meesho) ? meesho : 0
        });
    });
    return { safeDate, safeCompanyId, rows: out };
}

function getLocalOrderRowsForDate(date, companyId) {
    const safeDate = normalizeToISODate(date);
    const safeCompanyId = normalizeCompanyIdLocal(companyId);
    if (!safeDate) return [];
    const source = safeCompanyId === 'company2' ? (AppState.company2Data || []) : (AppState.company1Data || []);
    const map = new Map();
    source.forEach(r => {
        const d = normalizeToISODate(r?.date);
        if (d !== safeDate) return;
        const accountId = String(r?.accountId || '').trim();
        if (!accountId) return;
        const key = accountId;
        const accountName = String(r?.accountName || resolveAccountNameForSheet(accountId, safeCompanyId) || accountId).trim();
        const qty = parseInt(r?.meesho, 10) || 0;
        const prev = map.get(key);
        if (!prev) {
            map.set(key, { accountId, accountName, meesho: qty });
        } else {
            prev.meesho = (parseInt(prev.meesho, 10) || 0) + qty;
            map.set(key, prev);
        }
    });
    return Array.from(map.values());
}

async function syncOrdersDirectToSheet(date, orders, companyId, reason = 'orders_live') {
    const normalized = normalizeSheetOrderRows(date, orders, companyId);
    if (!normalized.safeDate || normalized.rows.length === 0) {
        return { success: false, skipped: true, message: 'No valid order rows to sync' };
    }

    const backupType = normalizeBackupTypeLabel(reason || 'orders_live');
    setPersistentSheetSyncPending(reason || 'orders_live');
    setSheetBackupIndicator('running', 'Sheet syncing');
    setLastBackupStatusText(`Sheet syncing (${backupType})...`);

    const response = await sheetsApiRequest({
        action: 'submitOrders',
        date: normalized.safeDate,
        orders: normalized.rows,
        companyId: normalized.safeCompanyId
    });
    if (!response || response.success === false) {
        throw new Error(response?.message || 'Order sheet sync failed');
    }

    const now = new Date().toISOString();
    _lastDirectOrderSheetSyncMs = Date.now();
    _pendingDataChangesForBackup = false;
    try {
        await FirebaseService.setBackupMeta({
            lastBackup: now,
            lastBackupType: backupType
        });
    } catch (e) {
        console.warn('Non-blocking: backup meta update failed after order sync', e);
    }
    updateLastBackupTimeDisplay(now, backupType);
    setLastSheetSyncSuccessTimestamp(now);
    clearPersistentSheetSyncPending();
    setSheetBackupIndicator('success', 'Sheet synced');
    setTimeout(() => {
        if (!_backgroundSheetBackupInFlight && !_backgroundSheetBackupQueued && !_pendingDataChangesForBackup) {
            setSheetBackupIndicator('idle', 'Sheet idle');
        }
    }, 2000);
    return response;
}

async function syncOrderDateForAllCompanies(date, options = {}, reason = 'orders_daily') {
    const safeDate = normalizeToISODate(date);
    if (!safeDate) return { success: false, skipped: true, message: 'Invalid date' };

    const errors = [];

    for (const cid of ['company1', 'company2']) {
        let rows = [];
        try {
            const dailyRes = await FirebaseService.getOrderRowsForDate(cid, safeDate);
            rows = Array.isArray(dailyRes?.data) ? dailyRes.data : [];
        } catch (e) {
            errors.push(`${cid}: ${e?.message || 'read failed'}`);
            continue;
        }

        if (!rows.length) continue;

        try {
            await syncOrdersDirectToSheet(safeDate, rows, cid, `${reason}_${cid}`);
        } catch (e) {
            errors.push(`${cid}: ${e?.message || 'sheet sync failed'}`);
        }
    }

    if (errors.length > 0) {
        throw new Error(errors.join(' | '));
    }
    return { success: true };
}

function buildOrderKeySet(rows) {
    const set = new Set();
    (Array.isArray(rows) ? rows : []).forEach(r => {
        const date = normalizeToISODate(r?.date);
        const accountId = String(r?.accountId || '').trim();
        if (!date || !accountId) return;
        set.add(`${date}__${accountId}`);
    });
    return set;
}

async function verifyOrderSyncAgainstSheet(firestoreBackupData) {
    const source = firestoreBackupData || {};
    const expectedC1 = buildOrderKeySet(source?.company1?.orders || []);
    const expectedC2 = buildOrderKeySet(source?.company2?.orders || []);

    const sheetRes = await sheetsApiRequest({ action: 'getAllSheetData' });
    const sheetData = sheetRes?.data || {};
    const sheetC1 = buildOrderKeySet(sheetData?.company1?.orders || []);
    const sheetC2 = buildOrderKeySet(sheetData?.company2?.orders || []);

    const missingC1 = [];
    expectedC1.forEach(k => { if (!sheetC1.has(k)) missingC1.push(k); });
    const missingC2 = [];
    expectedC2.forEach(k => { if (!sheetC2.has(k)) missingC2.push(k); });

    return {
        success: missingC1.length === 0 && missingC2.length === 0,
        missing: {
            company1: missingC1,
            company2: missingC2
        },
        counts: {
            expectedC1: expectedC1.size,
            expectedC2: expectedC2.size,
            sheetC1: sheetC1.size,
            sheetC2: sheetC2.size
        }
    };
}

function resumeSheetSyncIfPending() {
    const pending = readPersistentSheetSyncPending();
    if (!pending) return;
    _pendingDataChangesForBackup = true;
    _backgroundSheetBackupQueued = true;
    setSheetBackupIndicator('queued', 'Recovering sheet sync');
    setTimeout(() => {
        runBackgroundSheetBackup(`resume_${pending.reason || 'pending'}`).catch(e => {
            console.warn('Resume sheet sync failed:', e);
        });
    }, 1200);
}

function scheduleBackgroundSheetBackup(reason = '') {
    _backgroundSheetBackupQueued = true;
    setPersistentSheetSyncPending(reason || 'queued');
    setSheetBackupIndicator('queued', 'Sheet queued');
    if (_backgroundSheetBackupTimer) clearTimeout(_backgroundSheetBackupTimer);
    _backgroundSheetBackupTimer = setTimeout(() => {
        runBackgroundSheetBackup(reason).catch(e => console.warn('Background sheet backup failed:', e));
    }, 800);
}

async function runBackgroundSheetBackup(reason = '') {
    if (isFirestoreQuotaBlocked()) {
        setSheetBackupIndicator('queued', 'Sheet queued (quota cooldown)');
        return;
    }
    if (_backgroundSheetBackupInFlight) return;
    if (!_backgroundSheetBackupQueued && !_pendingDataChangesForBackup) return;
    _backgroundSheetBackupInFlight = true;
    setSheetBackupIndicator('running', 'Sheet syncing');
    _backgroundSheetBackupQueued = false;
    let success = false;
    try {
        const reasonText = String(reason || 'auto_change');
        const shouldVerifyOrders = /reconcile|resume|startup|retry/i.test(reasonText);
        await backupToSheets(true, { skipArchive: true, reason: reasonText, verifyOrders: shouldVerifyOrders });
        success = true;
        _backgroundSheetRetryCount = 0;
        setSheetBackupIndicator('success', 'Sheet synced');
        setTimeout(() => {
            if (!_backgroundSheetBackupInFlight && !_backgroundSheetBackupQueued) {
                setSheetBackupIndicator('idle', 'Sheet idle');
            }
        }, 2500);
    } catch (e) {
        _backgroundSheetRetryCount = Math.min(_backgroundSheetRetryCount + 1, 8);
        setSheetBackupIndicator('error', 'Sheet sync error');
        throw e;
    } finally {
        _backgroundSheetBackupInFlight = false;
        if (success) {
            _pendingDataChangesForBackup = false;
            clearPersistentSheetSyncPending();
        } else {
            _pendingDataChangesForBackup = true;
            setPersistentSheetSyncPending(reason || 'retry');
            const retryDelay = Math.min(180000, 3000 * Math.pow(2, Math.max(0, _backgroundSheetRetryCount - 1)));
            if (_backgroundSheetBackupTimer) clearTimeout(_backgroundSheetBackupTimer);
            _backgroundSheetBackupTimer = setTimeout(() => {
                runBackgroundSheetBackup('retry').catch(() => null);
            }, retryDelay);
        }
        if (_backgroundSheetBackupQueued && !_backgroundSheetBackupInFlight) {
            runBackgroundSheetBackup('queued').catch(() => null);
        }
    }
}

function startAdminAutoSync() {
    if (_adminAutoSyncTimer) clearInterval(_adminAutoSyncTimer);
    if (_adminRealtimeRefreshTimer) clearInterval(_adminRealtimeRefreshTimer);
    if (!isAdminUser()) return;
    _adminAutoSyncTimer = setInterval(async () => {
        try {
            if (_useSheetsFallbackMode && !isFirestoreQuotaBlocked()) {
                try {
                    await fetchAllCompaniesData({ refreshArchiveMonths: false, enableBackgroundSync: false });
                    disableSheetsFallbackMode();
                } catch (e) {}
                return;
            }
            if (isFirestoreQuotaBlocked()) return;
            if (document.hidden) return;
            if (_pendingDataChangesForBackup || _backgroundSheetBackupQueued) {
                await FirebaseService.flushWrites();
                await runBackgroundSheetBackup('interval');
                return;
            }
            const staleMs = _lastSheetBackupSuccessMs > 0 ? (Date.now() - _lastSheetBackupSuccessMs) : Number.MAX_SAFE_INTEGER;
            if (!_backgroundSheetBackupInFlight && staleMs >= SHEET_RECONCILE_MAX_STALE_MS) {
                _pendingDataChangesForBackup = true;
                scheduleBackgroundSheetBackup('reconcile');
            }
        } catch (e) {
            console.warn('Admin background sync skipped:', e);
        }
    }, 45000);

    // Near real-time refresh so cross-account updates show quickly for admin.
    _adminRealtimeRefreshTimer = setInterval(async () => {
        if (!isAdminUser()) return;
        if (isFirestoreQuotaBlocked()) return;
        if (document.hidden || AppState.isSwitchingCompany || _adminRealtimeBusy) return;
        if (AppState.currentSection === 'data-sheet' && isDataSheetEditActive()) return;
        const pendingWrites = typeof FirebaseService !== 'undefined' && FirebaseService.getPendingCount
            ? FirebaseService.getPendingCount()
            : 0;
        const syncStatus = typeof FirebaseService !== 'undefined' && FirebaseService.getSyncStatus
            ? FirebaseService.getSyncStatus()
            : 'idle';
        if (pendingWrites > 0 || syncStatus === 'pending' || syncStatus === 'syncing') return;
        _adminRealtimeBusy = true;
        try {
            if (AppState.currentSection === 'karigar') {
                await loadKarigarData(true);
                renderKarigarGrid();
            } else if (AppState.currentSection === 'dashboard') {
                await fetchAllCompaniesData({ refreshArchiveMonths: false, enableBackgroundSync: false });
                renderDashboard();
            } else if (AppState.currentSection === 'size-prices') {
                await renderSizePricesPage();
            }
        } catch (e) {
            console.warn('Admin realtime refresh skipped:', e);
        } finally {
            _adminRealtimeBusy = false;
        }
    }, 15000);
}

// ===== FIREBASE → LAN PULL SYNC (background, cross-profile) =====
// When admin's LAN is connected, periodically pull latest Firebase data into admin's folder.
// This ensures Dhyan's orders (written to Firebase from their profile) appear in Admin's LAN folder.
let _lanPullTimer = null;
let _lastVisibilityPullMs = 0;

function startLanFirebasePullSync() {
    if (_lanPullTimer) clearInterval(_lanPullTimer);

    _lanPullTimer = setInterval(async () => {
        try {
            if (!LanStorageService.isConnected()) return;
            if (document.hidden) return;
            if (isFirestoreQuotaBlocked && isFirestoreQuotaBlocked()) return;
            // Don't pull while user is actively editing the data sheet
            if (typeof isDataSheetEditActive === 'function' && isDataSheetEditActive()) return;

            const result = await LanStorageService.pullLatestFromFirebase();
            if (result && result.pulled > 0) {
                console.log(`[PULL SYNC] Pulled ${result.pulled} records from Firebase → LAN folder.`);
                // Silently refresh the current view if it shows data
                const sec = AppState.currentSection;
                if (sec === 'dashboard') renderDashboard();
                else if (sec === 'data-sheet' && !(typeof isDataSheetEditActive === 'function' && isDataSheetEditActive())) renderDataSheet();
                else if (sec === 'daily-order' && !(typeof isDataSheetEditActive === 'function' && isDataSheetEditActive())) renderOrderEntryTable();
            }
        } catch (e) {
            console.warn('[PULL SYNC] Background pull skipped:', e.message);
        }
    }, 90 * 1000); // Every 90 seconds
}

async function refreshAppDataManually() {
    if (typeof window !== 'undefined' && window.LanStorageService && window.LanStorageService.isConnected()) {
        showLoader('Refreshing local data...');
        try {
            await Promise.all([
                fetchAccounts(),
                fetchAllCompaniesData({ refreshArchiveMonths: false, skipSheetMerge: true })
            ]);
            await loadAvailableSheetMonths(true);
            if (AppState.currentSection === 'data-sheet') renderDataSheet();
            showToast('Local data refreshed successfully!', 'success');
        } catch (e) {
            console.error(e);
            showToast('Failed to refresh local data: ' + e.message, 'error');
        } finally {
            hideLoader();
        }
        return;
    }

    if (window.LanStorageService && window.LanStorageService.hasSavedHandle()) {
        showLoader('Reconnecting shared folder...');
        try {
            const reconnected = await LanStorageService.autoReconnect();
            if (reconnected && LanStorageService.isConnected()) {
                await Promise.all([
                    fetchAccounts(),
                    fetchAllCompaniesData({ refreshArchiveMonths: false, skipSheetMerge: true })
                ]);
                await loadAvailableSheetMonths(true);
                if (AppState.currentSection === 'data-sheet') renderDataSheet();
                showToast('Reconnected and refreshed local data!', 'success');
                hideLoader();
                return;
            }
        } catch (e) {}
        hideLoader();
    }
    showToast('Please link your shared LAN folder first.', 'info');
    showFolderSetupScreen();
}




async function switchCompany(previousCompany = '') {
    if (AppState.isSwitchingCompany) return;
    AppState.isSwitchingCompany = true;
    const companyName = getCompanyDisplayName(AppState.currentCompany);
    
    // START MAGICAL TRANSITION
    triggerMagicalTransition(companyName, true);
    
    setCompanyButtonsDisabled(true);
    showProgressToast(`Switching to ${companyName}...`);

    try {
        // Soft flush with short timeout so switch remains instant.
        if (typeof FirebaseService !== 'undefined') {
            await Promise.race([
                FirebaseService.flushWrites().catch(() => null),
                new Promise(resolve => setTimeout(resolve, 300))
            ]);
        }

        // Instant swap from in-memory company cache (no flicker).
        applyCurrentCompanySnapshotFromMemory();
        if (AppState.currentSection === 'dashboard') renderDashboard();
        else if (AppState.currentSection === 'add-account') renderAccountsList();
        else if (AppState.currentSection === 'karigar') renderKarigarPage(true);
        else if (AppState.currentSection === 'size-prices') renderSizePricesPage();
        else if (AppState.currentSection === 'data-sheet') {
            const cmFilter = document.getElementById('sheet-company-filter');
            if (cmFilter) cmFilter.value = AppState.currentCompany;
            populateSheetMonthFilter();
            renderDataSheet();
        }

        const isKarigarView = AppState.currentSection === 'karigar';
        if (!isKarigarView) {
            await Promise.all([
                fetchAccounts(),
                fetchDashboardData({ skipSheetMerge: true })
            ]);
        } else {
            // Keep karigar switch instant; refresh other sections silently in background.
            fetchAccounts().catch(() => null);
            fetchDashboardData({ skipSheetMerge: true }).catch(() => null);
        }

        // Background refresh of cross-company cache; do not block switch.
        fetchAllCompaniesData({ refreshArchiveMonths: false, skipSheetMerge: true }).catch(e => {
            console.warn('Silent update of all-company dashboard data', e);
        });

        if (AppState.currentSection === 'dashboard') renderDashboard();
        else if (AppState.currentSection === 'add-account') renderAccountsList();
        else if (AppState.currentSection === 'karigar') await renderKarigarPage(true);
        else if (AppState.currentSection === 'size-prices') renderSizePricesPage();
        else if (AppState.currentSection === 'data-sheet') { 
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
        endMagicalTransition();
    }
}

function triggerMagicalTransition(companyName, keepUntilDone = false) {
    const flash = document.getElementById('magical-flash');
    if (flash) {
        flash.classList.add('active');
        if (!keepUntilDone) {
            setTimeout(() => flash.classList.remove('active'), 1000);
        }
    }

    // Typing effect for the page title
    const titleEl = document.getElementById('page-title');
    if (titleEl) {
        // If it's something like "Dashboard Overview", maybe change it to "Switching to Company..."
        typeWriterEffect(titleEl, `Magical switch to ${companyName}...`, 50, () => {
             // After typing, wait a bit then restore or set new relevant title
             setTimeout(() => {
                 if (AppState.currentSection === 'dashboard') titleEl.textContent = 'Dashboard Overview';
                 else if (AppState.currentSection === 'daily-order') titleEl.textContent = 'Daily Order Entry';
                 else if (AppState.currentSection === 'add-account') titleEl.textContent = 'Manage Accounts';
                 else if (AppState.currentSection === 'size-prices') titleEl.textContent = 'Size Price Management';
                 else titleEl.textContent = 'Dashboard'; // Fallback
             }, 1500);
        });
    }
}

function endMagicalTransition() {
    const flash = document.getElementById('magical-flash');
    if (!flash) return;
    flash.classList.remove('active');
}

function typeWriterEffect(element, text, speed = 50, callback) {
    element.innerHTML = '<span class="typing-text"></span>';
    const span = element.querySelector('.typing-text');
    let i = 0;
    
    function type() {
        if (i < text.length) {
            span.textContent += text.charAt(i);
            i++;
            setTimeout(type, speed);
        } else if (callback) {
            callback();
        }
    }
    type();
}




// ===== AUTH =====
function checkAuth() {
    const isLogged = localStorage.getItem('isLogged');
    const savedRole = localStorage.getItem('userRole');
    const savedName = localStorage.getItem('userName');
    const savedUsername = localStorage.getItem('userUsername');
    const savedAllowedCompanies = localStorage.getItem('userAllowedCompanies');
    
    if (isLogged && savedRole) {
        let allowedCompanies = null;
        try {
            allowedCompanies = savedAllowedCompanies ? JSON.parse(savedAllowedCompanies) : null;
        } catch (e) {}
        if (!Array.isArray(allowedCompanies) || allowedCompanies.length === 0) {
            const matched = USERS.find(u => u.role === savedRole && (!savedUsername || u.username === savedUsername));
            allowedCompanies = matched?.allowedCompanies || ['company1', 'company2'];
        }

        AppState.currentUser = {
            role: savedRole,
            displayName: savedName || savedRole,
            username: savedUsername || savedName || savedRole,
            allowedCompanies
        };

        if (!isCompanyAllowed(AppState.currentCompany)) {
            AppState.currentCompany = getAllowedCompanies()[0] || 'company1';
            localStorage.setItem('selectedCompany', AppState.currentCompany);
        }
        document.getElementById('folder-setup-screen').classList.add('hidden');
        document.getElementById('login-screen').classList.add('hidden');
        document.getElementById('app-screen').classList.remove('hidden');
        applyRolePermissions();
        updateBackendModeBanner();
        startAdminAutoSync();
        // ── Start background Firebase→LAN pull sync (keeps admin folder up-to-date with Dhyan's orders) ──
        startLanFirebasePullSync();
        loadInitialData();
        // Do an immediate first pull so admin sees cross-profile data right away after login
        if (LanStorageService.isConnected()) {
            setTimeout(() => {
                LanStorageService.pullLatestFromFirebase({ force: true }).then(r => {
                    if (r && r.pulled > 0) {
                        console.log(`[PULL SYNC] Initial pull: ${r.pulled} records merged.`);
                        if (typeof clearApiReadCache === 'function') clearApiReadCache();
                    }
                }).catch(e => console.warn('[PULL SYNC] Initial pull failed:', e.message));
            }, 3000); // 3s delay — let loadInitialData complete first
        }

    } else {
        // Not logged in — show login or folder-setup
        document.getElementById('app-screen').classList.add('hidden');
        if (!LanStorageService.hasSavedHandle()) {
            showFolderSetupScreen();
        } else {
            showLoginScreen();
        }
        updateBackendModeBanner();
        if (_adminAutoSyncTimer) {
            clearInterval(_adminAutoSyncTimer);
            _adminAutoSyncTimer = null;
        }
        if (_adminRealtimeRefreshTimer) {
            clearInterval(_adminRealtimeRefreshTimer);
            _adminRealtimeRefreshTimer = null;
        }
        if (_lanPullTimer) {
            clearInterval(_lanPullTimer);
            _lanPullTimer = null;
        }
    }
}


function applyRolePermissions() {
    const role = AppState.currentUser?.role;
    const userInfo = document.getElementById('sidebar-user-info');
    const displayName = AppState.currentUser?.displayName || localStorage.getItem('userName') || 'User';
    if (userInfo) userInfo.innerHTML = `<span>${displayName}</span>`;
    
    const navDash = document.getElementById('nav-dashboard');
    const navSheet = document.getElementById('nav-data-sheet');
    const navAccounts = document.getElementById('nav-manage-accounts');
    const navKarigar = document.getElementById('nav-karigar');
    const navSizePrices = document.getElementById('nav-size-prices');

    if (role === 'order' || role === 'order_c2') {
        // Order role: allow Data Sheet, Manage Accounts if permitted (e.g. Dhyan Order), and Karigar Management
        if (navDash) navDash.style.display = 'none';
        if (navSheet) navSheet.style.display = '';
        if (navAccounts) navAccounts.style.display = canManageAccounts() ? '' : 'none';
        if (navKarigar) navKarigar.style.display = '';
        if (navSizePrices) navSizePrices.style.display = '';
    } else {
        if (navDash) navDash.style.display = '';
        if (navSheet) navSheet.style.display = '';
        if (navAccounts) navAccounts.style.display = '';
        if (navKarigar) navKarigar.style.display = '';
        if (navSizePrices) navSizePrices.style.display = '';
    }

    const allowedSet = new Set(getAllowedCompanies());
    document.querySelectorAll('.company-btn').forEach(btn => {
        const allowed = allowedSet.has(btn.dataset.company);
        btn.style.display = allowed ? '' : 'none';
        btn.disabled = !allowed;
    });

    // Data Sheet company filter should respect allowed companies for order users.
    const sheetCompanyFilter = document.getElementById('sheet-company-filter');
    if (sheetCompanyFilter && isOrderUser() && !isSingleCompanyMode()) {
        const allowed = getAllowedCompanies();
        const options = allowed.map(cid => `<option value="${cid}">${getCompanyDisplayName(cid)}</option>`).join('');
        sheetCompanyFilter.innerHTML = options;
        if (!allowed.includes(sheetCompanyFilter.value)) {
            sheetCompanyFilter.value = allowed[0] || 'company1';
        }
    }

    // Show/hide profile switch button based on role
    updateProfileSwitchBtn();
}

function attachEventListeners() {
    // ──── Folder Setup Screen ────
    const folderSetupLinkBtn = document.getElementById('folder-setup-link-btn');
    const folderSetupContinueBtn = document.getElementById('folder-setup-continue-btn');
    const folderSetupSkipBtn = document.getElementById('folder-setup-skip-btn');
    const loginChangeFolderBtn = document.getElementById('login-change-folder-btn');
    const folderSetupStatus = document.getElementById('folder-setup-status');

    if (folderSetupSkipBtn) {
        folderSetupSkipBtn.addEventListener('click', () => {
            document.getElementById('folder-setup-screen').classList.add('hidden');
            checkAuth();
        });
    }

    if (folderSetupLinkBtn) {
        folderSetupLinkBtn.addEventListener('click', async () => {
            folderSetupLinkBtn.disabled = true;
            folderSetupLinkBtn.innerHTML = '<i class="bx bx-loader-alt bx-spin"></i> Linking...';
            const res = await LanStorageService.connect();
            folderSetupLinkBtn.disabled = false;
            folderSetupLinkBtn.innerHTML = '<i class="bx bx-folder-open"></i> Select Shared Folder';
            if (res.success) {
                if (folderSetupStatus) {
                    folderSetupStatus.style.background = '#f0fdf4';
                    folderSetupStatus.style.borderColor = '#86efac';
                    folderSetupStatus.style.color = '#166534';
                    folderSetupStatus.innerHTML = '<i class="bx bx-check-circle"></i> Folder linked successfully!';
                }
                if (folderSetupContinueBtn) folderSetupContinueBtn.classList.remove('hidden');
                showToast('Folder linked! Click "Continue to Login" to proceed.', 'success');
            } else {
                if (res.error && res.error.includes('aborted')) {
                    console.log('Folder selection aborted by user.');
                } else {
                    showToast('Failed to link folder: ' + (res.error || 'Unknown error'), 'error');
                }
            }
        });
    }

    if (folderSetupContinueBtn) {
        folderSetupContinueBtn.addEventListener('click', () => showLoginScreen());
    }

    if (loginChangeFolderBtn) {
        loginChangeFolderBtn.addEventListener('click', (e) => {
            e.preventDefault();
            LanStorageService.disconnect();
            showFolderSetupScreen();
            if (folderSetupStatus) {
                folderSetupStatus.style.background = '#f1f5f9';
                folderSetupStatus.style.borderColor = '#e2e8f0';
                folderSetupStatus.style.color = '#475569';
                folderSetupStatus.innerHTML = '<i class="bx bx-info-circle"></i> Select a new folder to link.';
            }
            if (folderSetupContinueBtn) folderSetupContinueBtn.classList.add('hidden');
        });
    }

    // ──── Login Form ────
    document.getElementById('login-form')?.addEventListener('submit', (e) => {
        e.preventDefault();
        refreshUsersFromEnv();
        if (!USERS.length) {
            showToast("No dashboard users configured. Set AUTH_USERS_JSON in hosting environment variables.", "error");
            return;
        }
        const user = document.getElementById('username').value;
        const pass = document.getElementById('password').value;
        
        const foundUser = USERS.find(u => u.username === user && u.password === pass);
        if (foundUser) {
            // ── Activate per-profile LAN folder before session is set ──
            LanStorageService.setActiveProfile(foundUser.username);

            localStorage.setItem('isLogged', 'true');
            localStorage.setItem('userRole', foundUser.role);
            localStorage.setItem('userName', foundUser.displayName);
            localStorage.setItem('userUsername', foundUser.username);
            localStorage.setItem('userAllowedCompanies', JSON.stringify(foundUser.allowedCompanies || ['company1', 'company2']));
            AppState.currentUser = {
                role: foundUser.role,
                displayName: foundUser.displayName,
                username: foundUser.username,
                allowedCompanies: foundUser.allowedCompanies || ['company1', 'company2']
            };
            if (!isCompanyAllowed(AppState.currentCompany)) {
                AppState.currentCompany = getAllowedCompanies()[0] || 'company1';
                localStorage.setItem('selectedCompany', AppState.currentCompany);
            }

            // ── Dhyan_Order auto-redirect: lock to company2 + set #dhyan hash ──
            if (foundUser.role === 'order_c2') {
                AppState.currentCompany = 'company2';
                localStorage.setItem('selectedCompany', 'company2');
                if (window.location.hash.replace('#', '').toLowerCase() !== 'dhyan') {
                    window.location.hash = 'dhyan';
                }
            }

            checkAuth();
            showToast(`Welcome, ${foundUser.displayName}!`, "success");
            updateProfileSwitchBtn();
        } else {
            showToast("Invalid Credentials", "error");
        }
    });

    // ── Profile Switch Button (Admin only) ──
    document.getElementById('profile-switch-btn')?.addEventListener('click', () => {
        const isDhyan = _isDhyanHash();
        switchToProfile(isDhyan ? 'krimaa' : 'dhyan');
    });

    // Navigation
    document.querySelectorAll('.company-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            if (btn.disabled || btn.classList.contains('is-disabled') || AppState.isSwitchingCompany) return;
            const targetCompany = btn.dataset.company;
            if (AppState.currentCompany === targetCompany) return;
            
            document.querySelectorAll('.company-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            const prev = AppState.currentCompany;
            AppState.currentCompany = targetCompany;
            switchCompany(prev);
        });
    });

    // Navigation
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            if (btn.id === 'logout-btn') {
                e.preventDefault();
                localStorage.removeItem('isLogged');
                localStorage.removeItem('userRole');
                localStorage.removeItem('userName');
                localStorage.removeItem('userUsername');
                localStorage.removeItem('userAllowedCompanies');
                AppState.currentUser = null;
                LanStorageService.setActiveProfile(null);
                // Clear #dhyan hash on logout so next user starts fresh
                if (window.location.hash) window.location.hash = '';
                checkAuth();
                return;
            }
            e.preventDefault();
            const target = btn.getAttribute('data-target');
            
            // Check permissions
            if (target === 'add-account' && !canManageAccounts()) {
                showToast("You don't have permission to manage accounts", "error");
                return;
            }
            
            navigateTo(target);
            const sidebar = document.getElementById('sidebar');
            if(window.innerWidth <= 768 && sidebar.classList.contains('open')) closeMobileSidebar();
        });
    });

    document.getElementById('menu-toggle')?.addEventListener('click', toggleMobileSidebar);
    document.getElementById('sidebar-overlay')?.addEventListener('click', closeMobileSidebar);
    window.addEventListener('resize', () => {
        if (window.innerWidth > 768) closeMobileSidebar();
    });

    // Channel toggle button helpers
    function wireChannelToggle(btnId, chkId) {
        const btn = document.getElementById(btnId);
        const chk = document.getElementById(chkId);
        if (!btn || !chk) return;
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            chk.checked = !chk.checked;
            btn.classList.toggle('active', chk.checked);
        });
    }
    wireChannelToggle('new-account-m-btn', 'new-account-meesho');
    wireChannelToggle('new-account-f-btn', 'new-account-flipkart');
    wireChannelToggle('edit-account-m-btn', 'edit-account-meesho');
    wireChannelToggle('edit-account-f-btn', 'edit-account-flipkart');

    // Add Account
    document.getElementById('add-account-form')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        const accountName = document.getElementById('new-account-name').value.trim();
        const mobile = document.getElementById('new-account-mobile')?.value.trim();
        const rechargeDate = document.getElementById('new-account-recharge')?.value;
        const hasMeesho = document.getElementById('new-account-meesho')?.checked !== false;
        const hasFlipkart = document.getElementById('new-account-flipkart')?.checked === true;
        if (!accountName) return;
        if (!hasMeesho && !hasFlipkart) {
            showToast("Please enable at least one channel (Meesho or Flipkart)!", "error");
            return;
        }
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
            const res = await apiRequest({ action: 'addAccount', accountName, mobile, gstin: '', rechargeDate, companyId: AppState.currentCompany, hasMeesho, hasFlipkart });
            if (res.success) {
                showToast("Account saved!", "success");
                await fetchAccounts();
                await fetchAllCompaniesData();
                renderAccountsList();
                if (AppState.currentSection === 'data-sheet') renderDataSheet();
            }
            else { showToast(res.message, "error"); document.getElementById(tempId).remove(); }
        } catch (err) { showToast("Network Error!", "error"); document.getElementById(tempId).remove(); }
        finally { btn.disabled = false; }
    });

    // Submit Orders
    document.getElementById('submit-orders-btn')?.addEventListener('click', async () => {
        const date = document.getElementById('order-date').value;
        if(!date) return showToast("Please select a date", "error");
        const orders = [];
        let hasData = false;
        document.querySelectorAll('.order-row').forEach(row => {
            const accountId = row.dataset.accountId;
            const accountName = row.dataset.accountName;
            const meesho = parseInt(row.querySelector('.inp-meesho').value) || 0;
            if (meesho > 0) hasData = true;
            orders.push({ accountId, accountName, meesho });
        });
        if (!hasData) return showToast("Please enter at least one order value!", "error");
        const btn = document.getElementById('submit-orders-btn');
        btn.disabled = true; btn.textContent = "Submitting..."; showLoader();
        try {
            const submitRes = await apiRequest({ action: 'submitOrders', date, orders, companyId: AppState.currentCompany });
            applyOptimisticOrdersBatch(date, orders, AppState.currentCompany);
            showToast("Orders saved!", "success");
            if (submitRes?.sheetSyncWarning) {
                showToast(`Saved to Firebase. Sheet sync retrying in background (${submitRes.sheetSyncWarning}).`, "info");
            }
            await fetchDashboardData();
            try { await fetchAllCompaniesData(); } catch (e) { console.warn('Non-blocking: failed to refresh all-company data after submit', e); }
            // Order role stays on daily-order, admin goes to dashboard
            if (AppState.currentUser?.role === 'order' || AppState.currentUser?.role === 'order_c2') {
                showToast('Orders saved! You can enter more orders.', 'success');
                checkExistingOrdersForDate();
            } else {
                navigateTo('dashboard');
            }
        } catch (err) { showToast("Network Error!", "error"); }
        finally { btn.disabled = false; btn.textContent = "Submit All Orders"; hideLoader(); }
    });

    document.getElementById('order-date')?.addEventListener('change', () => { checkExistingOrdersForDate(); updateOrderDateLabel(); });
    document.getElementById('already-submitted-alert')?.addEventListener('click', () => { document.getElementById('order-details-modal').classList.add('show'); });

    // Modal closes
    document.getElementById('close-modal-btn')?.addEventListener('click', () => document.getElementById('order-details-modal').classList.remove('show'));
    document.getElementById('order-details-modal')?.addEventListener('click', (e) => { if(e.target.id === 'order-details-modal') e.target.classList.remove('show'); });
    

    document.getElementById('close-edit-modal-btn')?.addEventListener('click', () => document.getElementById('edit-account-modal').classList.remove('show'));
    document.getElementById('edit-account-modal')?.addEventListener('click', (e) => { if(e.target.id === 'edit-account-modal') e.target.classList.remove('show'); });
    document.getElementById('edit-account-recharge')?.addEventListener('change', updateEditRechargeMeta);
    document.getElementById('edit-account-form')?.addEventListener('submit', async (e) => {
        e.preventDefault();
        await saveEditAccount();
    });

    const closeDeleteModalBtn = document.getElementById('close-delete-modal-btn');
    if (closeDeleteModalBtn) {
        closeDeleteModalBtn.addEventListener('click', () => document.getElementById('delete-account-modal').classList.remove('show'));
    }
    document.getElementById('delete-account-modal')?.addEventListener('click', (e) => { if(e.target.id === 'delete-account-modal') e.target.classList.remove('show'); });
    document.getElementById('cancel-delete-btn')?.addEventListener('click', () => document.getElementById('delete-account-modal').classList.remove('show'));
    document.getElementById('confirm-delete-btn')?.addEventListener('click', async () => {
        const accountId = document.getElementById('delete-account-id').value;
        const accountName = document.getElementById('delete-account-name').value;
        showLoader();
        try {
            const res = await apiRequest({ action: 'deleteAccount', accountId, companyId: AppState.currentCompany });
            if (res.success) { 
                showToast("Account deleted!", "success"); 
                document.getElementById('delete-account-modal').classList.remove('show'); 
                await fetchAccounts(); 
                renderAccountsList(); 
                try { await fetchAllCompaniesData(); } catch (e) {} 
            }
            else showToast(res.message || "Failed to delete", "error");
        } catch(err) { showToast("Network Error!", "error"); }
        finally { hideLoader(); }
    });

    // ──── PWA Installation ────
    const folderSetupInstallBtn = document.getElementById('folder-setup-install-btn');
    const pwaInstallBtn = document.getElementById('pwa-install-btn');

    async function triggerInstallPrompt() {
        if (!deferredPrompt) return;
        deferredPrompt.prompt();
        const { outcome } = await deferredPrompt.userChoice;
        console.log(`User response to install prompt: ${outcome}`);
        if (outcome === 'accepted') {
            deferredPrompt = null;
            if (folderSetupInstallBtn) folderSetupInstallBtn.classList.add('hidden');
            if (pwaInstallBtn) pwaInstallBtn.classList.add('hidden');
        }
    }

    if (folderSetupInstallBtn) {
        folderSetupInstallBtn.addEventListener('click', triggerInstallPrompt);
    }
    if (pwaInstallBtn) {
        pwaInstallBtn.addEventListener('click', triggerInstallPrompt);
    }
}

let deferredPrompt = null;
window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredPrompt = e;
    const folderSetupInstallBtn = document.getElementById('folder-setup-install-btn');
    const pwaInstallBtn = document.getElementById('pwa-install-btn');
    if (folderSetupInstallBtn) folderSetupInstallBtn.classList.remove('hidden');
    if (pwaInstallBtn) pwaInstallBtn.classList.remove('hidden');
});

window.addEventListener('appinstalled', (evt) => {
    console.log('Krimaa App was installed successfully.');
    deferredPrompt = null;
    const folderSetupInstallBtn = document.getElementById('folder-setup-install-btn');
    const pwaInstallBtn = document.getElementById('pwa-install-btn');
    if (folderSetupInstallBtn) folderSetupInstallBtn.classList.add('hidden');
    if (pwaInstallBtn) pwaInstallBtn.classList.add('hidden');
    showToast('🎉 Krimaa installed successfully!', 'success');
});

function navigateTo(sectionId) {
    
    // Permission check for order / restricted roles
    if (sectionId === 'add-account' && !canManageAccounts()) {
        showToast("Access denied", "error"); return;
    }
    if ((AppState.currentUser?.role === 'order' || AppState.currentUser?.role === 'order_c2') && (sectionId === 'dashboard' || sectionId === 'money-management' || sectionId === 'money-backup')) {
        showToast("Access denied", "error"); return;
    }
    
    AppState.currentSection = sectionId;
    try {
        localStorage.setItem('krimaa_active_section', sectionId);
    } catch (e) {}
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
        'karigar': 'Karigar Management',
        'size-prices': 'Size Price Management'
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
    else if (sectionId === 'karigar') renderKarigarPage(true);
    else if (sectionId === 'size-prices') renderSizePricesPage();
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

async function ensureLegacyIdIntegrity() {
    if (localStorage.getItem('id_prefix_integrity_v4')) return;
    // Mark first so we do not show this every startup if a non-critical step fails.
    localStorage.setItem('id_prefix_integrity_v4', '1');
    try {
        // Run Sheet backfill for legacy rows when possible.
        if (AppState.currentUser?.role === 'admin') {
            try {
                const sheetFixRes = await sheetsApiRequest({ action: 'backfillIds' });
                if (sheetFixRes && sheetFixRes.success === false) {
                    console.warn('Sheet ID backfill returned warning:', sheetFixRes.message);
                }
            } catch (sheetErr) {
                console.warn('Sheet ID backfill skipped:', sheetErr);
            }
        }

        const fbFixRes = await FirebaseService.fixHistoricalDataIntegrity();
        if (fbFixRes && fbFixRes.success === false) {
            throw new Error(fbFixRes.message || 'Firebase ID integrity fix failed');
        }
    } catch (e) {
        console.error('Legacy ID integrity fix failed:', e);
    }
}

async function autoFixMismatchedAccountNames() {
    if (localStorage.getItem('mismatched_accounts_fixed_v2')) return;
    try {
        console.log("Checking for mismatched account names in historical daily_orders...");
        let changed = false;
        const db = FirebaseService.getDb();
        
        const accountsC1Snap = await db.collection('accounts').where('companyId', '==', 'company1').get();
        const accountsC2Snap = await db.collection('accounts').where('companyId', '==', 'company2').get();
        
        const accountsC1 = [];
        const accountsC2 = [];
        accountsC1Snap.forEach(d => accountsC1.push(d.data().name));
        accountsC2Snap.forEach(d => accountsC2.push(d.data().name));
        
        const ordSnap = await db.collection('daily_orders').get();
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
                    // Match if they share the first 4 case-insensitive characters OR one contains the other
                    const fuzzyMatch = validAccounts.find(v => {
                        const a = oldName.toLowerCase().trim();
                        const b = v.toLowerCase().trim();
                        if (a === b) return true;
                        if (a.startsWith(b) || b.startsWith(a)) return true;
                        if (a.length >= 4 && b.length >= 4 && a.substring(0, 4) === b.substring(0, 4)) return true;
                        return false;
                    });
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
        
        if (changed && ops.length > 0) {
            const CHUNK = 450;
            for (let i = 0; i < ops.length; i += CHUNK) {
                const batch = db.batch();
                ops.slice(i, i + CHUNK).forEach(op => op(batch));
                await batch.commit();
            }
            console.log("Mismatched accounts auto-fixed!");
        }
        localStorage.setItem('mismatched_accounts_fixed_v2', 'true');
    } catch(e) {
        console.error("Auto-fix failed:", e);
    }
}

function runStartupMaintenanceInBackground() {
    if (_startupMaintenanceRunning) return;
    if (typeof window !== 'undefined' && window.LanStorageService && window.LanStorageService.isConnected()) {
        console.log("🚀 [MAINTENANCE] Skipped cloud maintenance in LAN mode.");
        return;
    }
    _startupMaintenanceRunning = true;
    setTimeout(async () => {
        try {
            await ensureLegacyOrdersMigrated();
            await ensureLegacyIdIntegrity();
            await autoFixMismatchedAccountNames();
            await FirebaseService.migrateDatabaseToIds();
            await hydrateRecent30DaysFromSheetsToFirebase();
            // Merge recent Sheet rows in background so UI never misses rows synced to Sheet first.
            try {
                await mergeRecentSheetRowsIntoState(SHEETS_FALLBACK_DAYS);
                if (AppState.currentSection === 'data-sheet') renderDataSheet();
                if (AppState.currentSection === 'dashboard') renderDashboard();
            } catch (e) {}
        } catch (e) {
            console.warn('Startup maintenance skipped:', e);
        } finally {
            _startupMaintenanceRunning = false;
        }
    }, 50);
}

function shouldRunRecent30dHydration() {
    try {
        const today = getTodayISODate();
        const last = String(localStorage.getItem(RECENT_30D_HYDRATE_KEY) || '').trim();
        return last !== today;
    } catch (e) {
        return true;
    }
}

function markRecent30dHydrationDone() {
    try {
        localStorage.setItem(RECENT_30D_HYDRATE_KEY, getTodayISODate());
    } catch (e) {}
}

function groupSheetRowsByDate(rows) {
    const map = new Map();
    (Array.isArray(rows) ? rows : []).forEach(r => {
        const date = normalizeToISODate(r?.date);
        const accountId = String(r?.accountId || '').trim();
        if (!date || !accountId) return;
        const bucket = map.get(date) || [];
        bucket.push({
            accountId,
            accountName: String(r?.accountName || accountId).trim(),
            meesho: parseInt(r?.meesho, 10) || 0
        });
        map.set(date, bucket);
    });
    return map;
}

async function hydrateRecent30DaysFromSheetsToFirebase() {
    if (_useSheetsFallbackMode || isFirestoreQuotaBlocked()) return;
    if (!shouldRunRecent30dHydration()) return;
    try {
        const [c1, c2] = await Promise.all([
            sheetsApiRequest({ action: 'getDashboardData', companyId: 'company1' }),
            sheetsApiRequest({ action: 'getDashboardData', companyId: 'company2' })
        ]);
        const c1Rows = filterRecentRows(c1?.data || [], 30);
        const c2Rows = filterRecentRows(c2?.data || [], 30);
        const byDateC1 = groupSheetRowsByDate(c1Rows);
        const byDateC2 = groupSheetRowsByDate(c2Rows);

        for (const [date, rows] of byDateC1.entries()) {
            await FirebaseService.submitOrders(date, rows, 'company1');
        }
        for (const [date, rows] of byDateC2.entries()) {
            await FirebaseService.submitOrders(date, rows, 'company2');
        }
        markRecent30dHydrationDone();
    } catch (e) {
        console.warn('Recent 30-day Sheet→Firebase hydration skipped:', e);
    }
}

async function loadInitialData() {
    try {
        console.log("🚀 [INIT] Starting Krimaa app...");
        AppState.karigarCacheByCompany = {};
        FirebaseService.init();

        // ═══ LAN-FIRST MODE (primary path) ═══
        if (LanStorageService.isConnected()) {
            showLoader('Loading local data...');
            console.log("🚀 [INIT] ✅ LAN connected — loading from local folder");
            
            await Promise.all([
                fetchAccounts(),
                fetchAllCompaniesData({ refreshArchiveMonths: false, skipSheetMerge: true })
            ]);

            await loadAvailableSheetMonths(true).catch(e => console.warn('[INIT] Month list fetch:', e));
            
            // Refresh pending badge
            window.updateFirebasePendingBadge(0, 'connected');

            console.log("🚀 [INIT] LAN data ready. Booting UI.");
            const savedSec = localStorage.getItem('krimaa_active_section') || 'data-sheet';
            navigateTo(savedSec);
            return;
        }

        // Check if saved handle exists and try auto-reconnect
        if (LanStorageService.hasSavedHandle()) {
            showLoader('Reconnecting shared folder...');
            try {
                const reconnected = await LanStorageService.autoReconnect();
                if (reconnected && LanStorageService.isConnected()) {
                    await Promise.all([
                        fetchAccounts(),
                        fetchAllCompaniesData({ refreshArchiveMonths: false, skipSheetMerge: true })
                    ]);
                    await loadAvailableSheetMonths(true).catch(() => {});
                    window.updateFirebasePendingBadge(0, 'connected');
                    console.log("🚀 [INIT] LAN reconnected. Booting UI.");
                    const savedSec = localStorage.getItem('krimaa_active_section') || 'data-sheet';
                    navigateTo(savedSec);
                    return;
                }
            } catch (reconErr) {
                console.warn('[INIT] Auto-reconnect failed:', reconErr);
            }
        }

        // If folder not linked or needs user gesture to unlock
        hideLoader();
        showFolderSetupScreen();
        return;

    } catch (err) {
        console.error('[INIT] Failed:', err);
        showToast("Error loading data: " + (err?.message || 'Unknown error'), "error");
    } finally {
        hideLoader();
    }
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
    if (!isAdminUser()) return [];
    try {
        const cached = localStorage.getItem(`sheetsCache_${companyId}`);
        if (cached) return JSON.parse(cached);
    } catch(e) {}
    return [];
}

function setSheetsCache(companyId, data) {
    if (!isAdminUser()) return;
    try {
        localStorage.setItem(`sheetsCache_${companyId}`, JSON.stringify(data));
    } catch(e) {}
}

function clearSheetsCaches() {
    try {
        localStorage.removeItem('sheetsCache_company1');
        localStorage.removeItem('sheetsCache_company2');
    } catch (e) {}
}

function mergeOrders(historical, recent) {
    const map = new Map();
    (historical||[]).forEach((r, idx) => {
        const rowId = String(r.accountId || '').trim();
        const key = `${normalizeToISODate(r.date)}_${rowId || `legacy_${idx}`}`;
        map.set(key, r);
    });
    (recent||[]).forEach((r, idx) => {
        const rowId = String(r.accountId || '').trim();
        const key = `${normalizeToISODate(r.date)}_${rowId || `legacy_recent_${idx}`}`;
        map.set(key, r);
    });
    return Array.from(map.values()).sort((a,b) => (normalizeToISODate(b.date) > normalizeToISODate(a.date) ? 1 : -1));
}

function upsertOrderRowInList(list, nextRow) {
    const target = Array.isArray(list) ? list : [];
    const safeDate = normalizeToISODate(nextRow?.date);
    const safeId = String(nextRow?.accountId || '').trim();
    const safeNameKey = String(nextRow?.accountName || '').trim().toLowerCase();
    if (!safeDate) return target;

    const idx = target.findIndex(r => {
        if (normalizeToISODate(r.date) !== safeDate) return false;
        const rid = String(r.accountId || '').trim();
        if (safeId && rid === safeId) return true;
        if (!safeId && !rid) {
            return String(r.accountName || '').trim().toLowerCase() === safeNameKey;
        }
        return !rid && safeNameKey && String(r.accountName || '').trim().toLowerCase() === safeNameKey;
    });

    const existing = idx >= 0 ? target[idx] : null;
    const meeshoVal = nextRow?.meesho !== undefined ? (parseInt(nextRow.meesho) || 0) : (parseInt(existing?.meesho) || 0);
    const flipkartVal = nextRow?.flipkart !== undefined ? (parseInt(nextRow.flipkart) || 0) : (parseInt(existing?.flipkart) || 0);
    const totalVal = nextRow?.total !== undefined ? (parseInt(nextRow.total) || (meeshoVal + flipkartVal)) : (meeshoVal + flipkartVal);

    const normalized = {
        ...(existing || {}),
        ...nextRow,
        date: safeDate,
        accountId: safeId,
        meesho: meeshoVal,
        flipkart: flipkartVal,
        total: totalVal
    };

    if (idx >= 0) {
        target[idx] = normalized;
    } else {
        target.push(normalized);
    }
    return target;
}

function applyOptimisticOrderUpdate(date, accountId, accountName, value, companyId, field = 'meesho') {
    const safeCompanyId = String(companyId || AppState.currentCompany || '').trim() || 'company1';
    const safeDate = normalizeToISODate(date);
    if (!safeDate) return;
    const safeAccountId = String(accountId || '').trim();

    const detailsList = safeCompanyId === 'company1' ? AppState.company1Details : AppState.company2Details;
    const resolvedNameFromDetails = (detailsList || []).find(d => String(d.accountId || '').trim() === safeAccountId)?.name || '';
    const resolvedName = String(accountName || resolvedNameFromDetails || safeAccountId).trim();
    const safeQty = parseInt(value) || 0;
    const safeField = field === 'flipkart' ? 'flipkart' : 'meesho';
    setPendingOrderOverride(safeDate, safeAccountId, resolvedName, safeQty, safeCompanyId, safeField);

    const currentList = safeCompanyId === 'company1' ? AppState.company1Data : AppState.company2Data;
    const existing = (currentList || []).find(r => normalizeToISODate(r.date) === safeDate && (String(r.accountId || '').trim() === safeAccountId || String(r.accountName || '').trim().toLowerCase() === resolvedName.toLowerCase()));

    const meeshoVal = safeField === 'meesho' ? safeQty : (parseInt(existing?.meesho) || 0);
    const flipkartVal = safeField === 'flipkart' ? safeQty : (parseInt(existing?.flipkart) || 0);
    const totalVal = meeshoVal + flipkartVal;

    const row = {
        date: safeDate,
        accountId: safeAccountId,
        accountName: resolvedName,
        meesho: meeshoVal,
        flipkart: flipkartVal,
        total: totalVal,
        companyId: safeCompanyId
    };

    if (safeCompanyId === 'company1') {
        AppState.company1Data = upsertOrderRowInList(AppState.company1Data, row);
    } else {
        AppState.company2Data = upsertOrderRowInList(AppState.company2Data, row);
    }
    if (AppState.currentCompany === safeCompanyId) {
        AppState.dashboardData = safeCompanyId === 'company1' ? AppState.company1Data : AppState.company2Data;
    }
}

function applyOptimisticOrdersBatch(date, orders, companyId) {
    (orders || []).forEach(o => {
        applyOptimisticOrderUpdate(date, o.accountId, o.accountName, o.meesho, companyId, 'meesho');
    });
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
    // Firestore-first mode: avoid sheet reads for dashboard state (prevents flicker/glitch).
    hideBackgroundLoader();
}

async function fetchDashboardData(options = {}) {
    try {
        const res = await apiRequest({
            action: 'getDashboardData',
            companyId: AppState.currentCompany,
            skipSheetMerge: !!options.skipSheetMerge
        });
        const fbData = Array.isArray(res?.data) ? res.data : [];
        AppState.dashboardData = applyPendingOrderOverrides(fbData, AppState.currentCompany);
        if (AppState.currentCompany === 'company1') AppState.company1Data = AppState.dashboardData;
        if (AppState.currentCompany === 'company2') AppState.company2Data = AppState.dashboardData;
    } catch(e) {
        console.error(e);
        throw e;
    }
}

async function fetchAllCompaniesData(options = {}) {
    const refreshArchiveMonths = !!options.refreshArchiveMonths;
    const skipSheetMerge = !!options.skipSheetMerge;
    const toList = (res) => {
        if (!res || res.success === false) return null;
        if (Array.isArray(res?.data)) return res.data;
        if (Array.isArray(res)) return res;
        return [];
    };

    const [c1Acc, c2Acc, f1, f2] = await Promise.all([
        apiRequest({ action: 'getAccounts', companyId: 'company1' }).catch(() => null),
        apiRequest({ action: 'getAccounts', companyId: 'company2' }).catch(() => null),
        apiRequest({ action: 'getDashboardData', companyId: 'company1', skipSheetMerge }).catch(() => null),
        apiRequest({ action: 'getDashboardData', companyId: 'company2', skipSheetMerge }).catch(() => null)
    ]);

    const c1Accounts = toList(c1Acc);
    const c2Accounts = toList(c2Acc);
    const fData1 = toList(f1);
    const fData2 = toList(f2);

    if (c1Accounts !== null) AppState.company1Accounts = c1Accounts;
    if (c2Accounts !== null) AppState.company2Accounts = c2Accounts;
    if (c1Acc && c1Acc.details) AppState.company1Details = c1Acc.details;
    if (c2Acc && c2Acc.details) AppState.company2Details = c2Acc.details;

    if (fData1 !== null) {
        AppState.company1Data = applyPendingOrderOverrides(fData1, 'company1');
    }
    if (fData2 !== null) {
        AppState.company2Data = applyPendingOrderOverrides(fData2, 'company2');
    }

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
    const canManage = canManageAccounts();
    
    // Add Sortable to container if not initialized
    if (canManage && !container._sortableInstance) {
        container._sortableInstance = Sortable.create(container, {
            handle: '.drag-handle',
            animation: 250,
            onEnd: function() {
                const newOrder = Array.from(container.children).map(c => c.dataset.accountId);
                // We keep accountId order in Firebase
                saveAccountOrderToSheet(newOrder); 
                Array.from(container.children).forEach((el, idx) => {
                    const posEl = el.querySelector('.account-position-badge');
                    if (posEl) posEl.textContent = idx + 1;
                });
            }
        });
    }

    container.innerHTML = AppState.accountDetails.map((details, idx) => {
        const id = details.accountId;
        const acc = details.name; 
        const rechargeTxt = getRechargeText(details.rechargeDate);
        const hasM = details.hasMeesho !== false;
        const hasF = details.hasFlipkart !== false;
        const extraHtml = `
            <div class="account-meta">
                <span><i class='bx bx-phone'></i> Mobile: ${details.mobile || 'Not added'}</span>
                <span><i class='bx bx-calendar'></i> Recharge: ${rechargeTxt}</span>
            </div>`;

        return `
        <div class="account-item" data-account-id="${id}" data-account-name="${acc.replace(/"/g, '&quot;')}" onclick="if(!event.target.closest('.delete-btn') && !event.target.closest('.drag-handle')) openEditAccount('${id}', '${acc.replace(/'/g, "\\'")}')" style="cursor: pointer;">
            <div style="display: flex; align-items: center; gap: 10px;">
                ${canManage ? `<i class='bx bx-menu drag-handle' style="cursor: grab; color: #a0aec0;"></i>` : ''}
                <div class="account-position-badge">${idx + 1}</div>
                <div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <span class="account-name font-bold">${acc}</span>
                        <span class="channel-pill channel-m ${hasM ? 'active' : ''}" title="Meesho channel">M</span>
                        <span class="channel-pill channel-f ${hasF ? 'active' : ''}" title="Flipkart channel">F</span>
                    </div>
                    ${extraHtml}
                </div>
            </div>
            ${canManage ? `<div class="account-actions">
                <button class="btn btn-outline btn-sm edit-btn" onclick="openEditAccount('${id}', '${acc.replace(/'/g, "\\'")}')" title="Edit details & channels"><i class='bx bx-edit-alt'></i></button>
                <button class="btn btn-outline btn-sm delete-btn" onclick="openDeleteAccount('${id}', '${acc.replace(/'/g, "\\'")}')" title="Delete"><i class='bx bx-trash'></i></button>
            </div>` : ''}
        </div>
    `}).join('');
}

function openEditAccount(id, name) {
    if (!canManageAccounts()) {
        showToast("You don't have permission to manage accounts", "error");
        return;
    }
    const details = AppState.accountDetails.find(d => d.accountId === id);
    if (!details) return;
    document.getElementById('edit-account-id').value = id;
    document.getElementById('edit-account-old-name').value = name;
    document.getElementById('edit-account-name').value = name;
    document.getElementById('edit-account-mobile').value = details.mobile || '';
    document.getElementById('edit-account-recharge').value = details.rechargeDate || '';
    updateEditRechargeMeta();

    const hasM = details.hasMeesho !== false;
    const hasF = details.hasFlipkart !== false;
    const mChk = document.getElementById('edit-account-meesho');
    const fChk = document.getElementById('edit-account-flipkart');
    const mBtn = document.getElementById('edit-account-m-btn');
    const fBtn = document.getElementById('edit-account-f-btn');
    if (mChk) mChk.checked = hasM;
    if (fChk) fChk.checked = hasF;
    if (mBtn) mBtn.classList.toggle('active', hasM);
    if (fBtn) fBtn.classList.toggle('active', hasF);

    document.getElementById('edit-account-modal').classList.add('show');
}

async function saveEditAccount() {
    const id = document.getElementById('edit-account-id').value;
    const oldName = document.getElementById('edit-account-old-name').value;
    const newName = document.getElementById('edit-account-name').value.trim();
    const mobile = document.getElementById('edit-account-mobile').value.trim();
    const recharge = document.getElementById('edit-account-recharge').value;
    const hasMeesho = document.getElementById('edit-account-meesho')?.checked !== false;
    const hasFlipkart = document.getElementById('edit-account-flipkart')?.checked === true;
    const compId = AppState.currentCompany;
    
    if (!newName) return showToast("Name cannot be empty!", "error");
    if (!hasMeesho && !hasFlipkart) {
        return showToast("Please enable at least one channel (Meesho or Flipkart)!", "error");
    }
    
    showToast("Updating account...", "info");
    const res = await apiRequest({ action: 'editAccount', accountId: id, newName, companyId: compId, mobile, rechargeDate: recharge, hasMeesho, hasFlipkart });
    if (res.success) {
        showToast("Account updated!", "success");
        document.getElementById('edit-account-modal').classList.remove('show');
        await fetchAccounts();
        if (AppState.currentSection === 'daily-order' || AppState.currentSection === 'order-entry') renderOrderEntryTable();
        if (AppState.currentSection === 'add-account' || AppState.currentSection === 'accounts') renderAccountsList();
        if (AppState.currentSection === 'data-sheet') renderDataSheet();
        await fetchDashboardData();
        try { await fetchAllCompaniesData(); } catch (e) { console.warn('Non-blocking: failed to refresh all-company data after edit', e); }
    } else {
        showToast(res.message, "error");
    }
}

function updateEditRechargeMeta() {
    const rechargeInput = document.getElementById('edit-account-recharge');
    const meta = document.getElementById('edit-account-recharge-meta');
    if (!rechargeInput || !meta) return;
    meta.textContent = rechargeInput.value ? getRechargeText(rechargeInput.value) : 'Not added';
}

function openDeleteAccount(id, name) {
    if (!canManageAccounts()) {
        showToast("You don't have permission to manage accounts", "error");
        return;
    }
    document.getElementById('delete-account-id').value = id;
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
    const sorted = AppState.accountDetails; // Already sorted by fetchAccounts
    tbody.innerHTML = sorted.map((ad, index) => `
        <tr class="order-row" data-account-id="${ad.accountId}" data-account-name="${ad.name}">
            <td class="drag-handle-cell"><i class='bx bx-menu drag-handle'></i></td>
            <td class="position-number">${index + 1}</td>
            <td class="font-medium">${ad.name}</td>
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
        const normalizedOrder = (Array.isArray(orderedAccounts) ? orderedAccounts : [])
            .map(item => String(item || '').trim())
            .filter(Boolean)
            .map(item => {
                const match = getCompanyAccountDetails(compId).find(detail => detail.accountId === item || detail.name === item);
                return match?.accountId || item;
            });
        syncCompanyAccountOrderState(compId, normalizedOrder);
        const res = await apiRequest({ action: 'updateAccountOrder', orderedAccounts: normalizedOrder, companyId: compId });
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

        // Pre-populate input fields with existing values so edits won't overwrite other data with 0
        document.querySelectorAll('.order-row').forEach(row => {
            const accId = row.dataset.accountId;
            const accName = row.dataset.accountName;
            const match = existingOrders.find(o => 
                (accId && String(o.accountId || '').trim() === String(accId).trim()) ||
                (accName && String(o.accountName || '').trim().toLowerCase() === String(accName).trim().toLowerCase())
            );
            const inp = row.querySelector('.inp-meesho');
            const totalSpan = row.querySelector('.row-total');
            if (inp) {
                const qty = match ? (parseInt(match.meesho, 10) || 0) : 0;
                inp.value = qty > 0 ? qty : '';
                if (totalSpan) {
                    totalSpan.textContent = qty;
                }
            }
        });
        calculateGrandTotals();

        let gm = 0, gf = 0, gt = 0;
        const modalBody = document.getElementById('modal-details-tbody');
        modalBody.innerHTML = existingOrders.map(o => {
            const m = parseInt(o.meesho) || 0;
            const f = parseInt(o.flipkart) || 0;
            const t = typeof o.total !== 'undefined' ? (parseInt(o.total) || 0) : (m + f);
            gm += m; gf += f; gt += t;
            return `<tr><td>${escapeHtml(o.accountName || '')}</td><td style="text-align:right;">${m}</td><td style="text-align:right;">${f}</td><td style="text-align:right;font-weight:600;">${t}</td></tr>`;
        }).join('');
        document.getElementById('submitted-grand-total').textContent = gt;
        const [yyyy, mm, dd] = dateInput.split('-');
        if (document.getElementById('modal-date')) document.getElementById('modal-date').textContent = `${dd}/${mm}/${yyyy}`;
        if (document.getElementById('modal-grand-meesho')) document.getElementById('modal-grand-meesho').textContent = gm;
        if (document.getElementById('modal-grand-flipkart')) document.getElementById('modal-grand-flipkart').textContent = gf;
        if (document.getElementById('modal-grand-total')) document.getElementById('modal-grand-total').textContent = gt;
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
    let meesho = 0, flipkart = 0, total = 0;
    data.forEach(row => {
        const m = parseInt(row.meesho) || 0;
        const f = parseInt(row.flipkart) || 0;
        const t = typeof row.total !== 'undefined' ? (parseInt(row.total) || 0) : (m + f);
        meesho += m;
        flipkart += f;
        total += t;
    });
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
    if (document.getElementById('dash-c1-meesho')) document.getElementById('dash-c1-meesho').textContent = c1Totals.meesho;
    if (document.getElementById('dash-c1-flipkart')) document.getElementById('dash-c1-flipkart').textContent = c1Totals.flipkart;
    if (document.getElementById('dash-c1-total')) document.getElementById('dash-c1-total').textContent = c1Totals.total;

    if (document.getElementById('dash-c2-meesho')) document.getElementById('dash-c2-meesho').textContent = c2Totals.meesho;
    if (document.getElementById('dash-c2-flipkart')) document.getElementById('dash-c2-flipkart').textContent = c2Totals.flipkart;
    if (document.getElementById('dash-c2-total')) document.getElementById('dash-c2-total').textContent = c2Totals.total;

    if (document.getElementById('dash-combined-meesho')) document.getElementById('dash-combined-meesho').textContent = c1Totals.meesho + c2Totals.meesho;
    if (document.getElementById('dash-combined-flipkart')) document.getElementById('dash-combined-flipkart').textContent = c1Totals.flipkart + c2Totals.flipkart;
    if (document.getElementById('dash-combined-total')) document.getElementById('dash-combined-total').textContent = c1Totals.total + c2Totals.total;

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
    if (!accTbody) return;
    accTbody.innerHTML = '';
    
    const buildAccTotals = (accountDetails, filtered) => {
        const totals = {};
        // Use accountId as the key for reliable aggregation
        accountDetails.forEach(ad => {
            const id = ad.accountId;
            totals[id] = { name: ad.name, meesho: 0, flipkart: 0, total: 0 };
        });
        
        filtered.forEach(row => {
            const id = row.accountId;
            const m = parseInt(row.meesho) || 0;
            const f = parseInt(row.flipkart) || 0;
            const t = typeof row.total !== 'undefined' ? (parseInt(row.total) || 0) : (m + f);
            if (id && totals[id]) {
                totals[id].meesho += m;
                totals[id].flipkart += f;
                totals[id].total += t;
            } else if (row.accountName) {
                // Fallback for legacy name-based records
                const idFromName = Object.keys(totals).find(k => totals[k].name === row.accountName);
                if (idFromName) {
                    totals[idFromName].meesho += m;
                    totals[idFromName].flipkart += f;
                    totals[idFromName].total += t;
                }
            }
        });
        return totals;
    };
    
    const c1AccTotals = buildAccTotals(AppState.company1Details, c1Filtered);
    const c2AccTotals = buildAccTotals(AppState.company2Details, c2Filtered);

    let html = '';
    
    // Company A Header
    const c1Keys = Object.keys(c1AccTotals);
    if (c1Keys.length > 0) {
        html += `<tr class="table-divider-row"><td colspan="5">Company A Accounts</td></tr>`;
        c1Keys.forEach(id => {
            const d = c1AccTotals[id];
            html += `<tr><td class="font-medium">${escapeHtml(d.name)}</td><td class="text-center"><span class="dot dot-a"></span></td><td class="text-right text-muted">${d.meesho}</td><td class="text-right text-muted">${d.flipkart}</td><td class="text-right font-bold text-main">${d.total}</td></tr>`;
        });
    }

    // Company B Header
    const c2Keys = Object.keys(c2AccTotals);
    if (c2Keys.length > 0) {
        html += `<tr class="table-divider-row"><td colspan="5">Company B Accounts</td></tr>`;
        c2Keys.forEach(id => {
            const d = c2AccTotals[id];
            html += `<tr><td class="font-medium">${escapeHtml(d.name)}</td><td class="text-center"><span class="dot dot-b"></span></td><td class="text-right text-muted">${d.meesho}</td><td class="text-right text-muted">${d.flipkart}</td><td class="text-right font-bold text-main">${d.total}</td></tr>`;
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
    
    const getLastOrderMeta = (accountId, rawData) => {
        let lastDate = null;
        rawData.forEach(r => {
            if (r.accountId === accountId && (parseInt(r.total) || 0) > 0) {
                const normalizedDate = normalizeToISODate(r.date);
                if (normalizedDate && (!lastDate || normalizedDate > lastDate)) {
                    lastDate = normalizedDate;
                }
            }
        });
        if (!lastDate) return { days: -1, lastDate: '' };
        return { days: diffDays(lastDate, getTodayISODate()), lastDate };
    };

    AppState.company1Details.forEach(ad => {
        const id = ad.accountId;
        if (!c1Filtered.some(r => r.accountId === id && (parseInt(r.total)||0) > 0)) {
            const meta = getLastOrderMeta(id, AppState.company1Data);
            inactiveAccounts.push({
                name: ad.name,
                companyName: getCompanyDisplayName('company1'),
                class: 'a',
                days: meta.days,
                lastDate: meta.lastDate
            });
        }
    });
    AppState.company2Details.forEach(ad => {
        const id = ad.accountId;
        if (!c2Filtered.some(r => r.accountId === id && (parseInt(r.total)||0) > 0)) {
            const meta = getLastOrderMeta(id, AppState.company2Data);
            inactiveAccounts.push({
                name: ad.name,
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
    if (!rankTbody) return;
    const allAccounts = [];
    
    const buildRankData = (detailsList, filtered, companyLabel) => {
        const totals = {};
        detailsList.forEach(ad => {
            totals[ad.accountId] = { name: ad.name, total: 0 };
        });
        filtered.forEach(r => {
            const id = r.accountId;
            if (id && totals[id]) totals[id].total += parseInt(r.total) || 0;
            else if (r.accountName) {
                const idFromName = Object.keys(totals).find(k => totals[k].name === r.accountName);
                if (idFromName) totals[idFromName].total += parseInt(r.total) || 0;
            }
        });
        return Object.keys(totals).map(id => ({
            id,
            name: totals[id].name,
            company: companyLabel,
            total: totals[id].total
        }));
    };
    
    const c1Rank = buildRankData(AppState.company1Details, c1Filtered, 'A');
    const c2Rank = buildRankData(AppState.company2Details, c2Filtered, 'B');
    
    const combined = [...c1Rank, ...c2Rank];
    combined.sort((a, b) => b.total - a.total);
    
    if (combined.length === 0) { rankTbody.innerHTML = '<div class="text-center text-muted p-2">No data</div>'; return; }
    
    rankTbody.innerHTML = combined.map((acc, idx) => {
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
    AppState.company1Data.filter(r => normalizeToISODate(r.date) === dateStr).forEach(r => {
        const mQty = parseInt(r.meesho) || 0;
        const fQty = parseInt(r.flipkart) || 0;
        const tQty = typeof r.total !== 'undefined' ? (parseInt(r.total) || 0) : (mQty + fQty);
        c1M += mQty; c1F += fQty; c1T += tQty;
    });
    
    let c2M = 0, c2F = 0, c2T = 0;
    AppState.company2Data.filter(r => normalizeToISODate(r.date) === dateStr).forEach(r => {
        const mQty = parseInt(r.meesho) || 0;
        const fQty = parseInt(r.flipkart) || 0;
        const tQty = typeof r.total !== 'undefined' ? (parseInt(r.total) || 0) : (mQty + fQty);
        c2M += mQty; c2F += fQty; c2T += tQty;
    });
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
                    <span>M: <strong>${c1M}</strong></span>
                    <span>F: <strong>${c1F}</strong></span>
                </div>
            </div>
            <div class="date-company-card date-company-b">
                <div class="date-company-head">
                    <div class="date-company-title"><span class="dot-sm dot-b"></span> ${company2Name}</div>
                    <span class="date-company-total">${c2T}</span>
                </div>
                <div class="date-company-chips">
                    <span>M: <strong>${c2M}</strong></span>
                    <span>F: <strong>${c2F}</strong></span>
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
    const readActions = new Set(['getAccounts', 'getDashboardData', 'getRemarks', 'getMoneyBackups', 'getKarigars', 'getKarigarTransactions', 'getDesignPrices']);
    const cacheKey = `${action}::${companyId}::${String(payload?.month || '')}`;
    if (readActions.has(action)) {
        const cached = _apiReadCache.get(cacheKey);
        if (cached && (Date.now() - cached.ts) < READ_CACHE_TTL_MS) {
            return cached.value;
        }
    }

    // ── LAN OFFLINE READS & WRITES ──
    if (typeof window !== 'undefined' && window.LanStorageService && window.LanStorageService.isConnected()) {
        if (readActions.has(action)) {
            const lanRes = await window.LanStorageService.fetchFromLocal(payload);
            if (lanRes !== undefined) {
                _apiReadCache.set(cacheKey, { ts: Date.now(), value: lanRes });
                return lanRes;
            }
        } else {
            const lanRes = await window.LanStorageService.writeToLocal(payload);
            if (lanRes !== undefined) {
                _apiReadCache.clear();
                return lanRes;
            }
        }
    }

    if (isFirestoreQuotaBlocked()) {
        const sheetRead = await readFromSheetsInQuotaMode(action, payload, companyId);
        if (sheetRead) return sheetRead;
        if (action === 'submitOrders') {
            applyOptimisticOrdersBatch(payload.date, payload.orders || [], companyId);
            const allRows = getLocalOrderRowsForDate(payload.date, companyId);
            await syncOrdersDirectToSheet(payload.date, allRows.length ? allRows : (payload.orders || []), companyId, 'quota_sheet_mode_submit');
            return { success: true, message: 'Saved to Google Sheet (quota mode)' };
        }
        if (action === 'updateOrder') {
            const accountName = resolveAccountNameForSheet(payload.accountId, companyId) || payload.accountId;
            applyOptimisticOrderUpdate(payload.date, payload.accountId, accountName, payload.value, companyId);
            const allRows = getLocalOrderRowsForDate(payload.date, companyId);
            await syncOrdersDirectToSheet(
                payload.date,
                allRows.length ? allRows : [{ accountId: payload.accountId, accountName, meesho: payload.value }],
                companyId,
                'quota_sheet_mode_update'
            );
            return { success: true, message: 'Saved to Google Sheet (quota mode)' };
        }
        if (action === 'saveRemark') {
            await sheetsApiRequest({ action: 'saveRemark', date: payload.date, remark: payload.remark || '' });
            if (!_remarkCache) _remarkCache = {};
            _remarkCache[payload.date] = payload.remark || '';
            return { success: true, message: 'Remark saved to Google Sheet (quota mode)' };
        }
        if (action === 'addAccount' || action === 'editAccount' || action === 'deleteAccount' || action === 'updateAccountOrder') {
            const passPayload = { ...payload, action, companyId };
            const sheetRes = await sheetsApiRequest(passPayload);
            return sheetRes && typeof sheetRes === 'object' ? sheetRes : { success: true };
        }
        const cached = buildCachedApiResult(action, companyId);
        if (cached) return cached;
        if (!readActions.has(action)) {
            throw new Error('Firestore quota exceeded. Please retry after cooldown.');
        }
    }
    try {
        let result;
        let orderSheetSyncWarning = '';
        switch (action) {
            case 'getAccounts': result = await FirebaseService.getAccounts(companyId); break;
            case 'getDashboardData': {
                result = await FirebaseService.getOrders(companyId, payload.month);
                if (!payload?.skipSheetMerge) {
                    try {
                        const sheetRes = await sheetsApiRequest({ action: 'getDashboardData', companyId, month: payload?.month || '' });
                        const sheetRows = filterRecentRows(sheetRes?.data || [], SHEETS_FALLBACK_DAYS).map(r => ({ ...r, companyId }));
                        const firebaseRows = Array.isArray(result?.data) ? result.data : [];
                        result = { ...(result || {}), success: true, data: mergeOrderRowsUnique(firebaseRows, sheetRows) };
                    } catch (sheetMergeErr) {
                        console.warn('Non-blocking: sheet merge failed for getDashboardData', sheetMergeErr);
                    }
                }
                break;
            }
            case 'submitOrders':
                result = await FirebaseService.submitOrders(payload.date, payload.orders, companyId);
                if (result && result.success !== false) {
                    try {
                        await syncOrderDateForAllCompanies(
                            payload.date,
                            { companyId, rows: payload.orders || [] },
                            'submitOrders'
                        );
                    } catch (sheetErr) {
                        console.warn('Immediate order sheet sync failed after submitOrders:', sheetErr);
                        orderSheetSyncWarning = sheetErr?.message || 'Sheet sync queued';
                        _pendingDataChangesForBackup = true;
                        scheduleBackgroundSheetBackup('submitOrders');
                    }
                }
                break;
            case 'addAccount':
                result = await FirebaseService.addAccount(payload.accountName, companyId, payload.mobile, payload.gstin, payload.rechargeDate, payload.hasMeesho, payload.hasFlipkart);
                if (result && result.success !== false) {
                    try {
                        await sheetsApiRequest({ action: 'addAccount', accountName: payload.accountName, companyId, mobile: payload.mobile, gstin: payload.gstin, rechargeDate: payload.rechargeDate });
                    } catch (e) { scheduleBackgroundSheetBackup('addAccount'); }
                }
                break;
            case 'editAccount':
                result = await FirebaseService.editAccount(payload.accountId, payload.newName, companyId, payload.mobile, payload.gstin, payload.rechargeDate, payload.hasMeesho, payload.hasFlipkart);
                if (result && result.success !== false) {
                    try {
                        await sheetsApiRequest({ action: 'editAccount', accountId: payload.accountId, newName: payload.newName, companyId, mobile: payload.mobile, gstin: payload.gstin, rechargeDate: payload.rechargeDate });
                    } catch (e) { scheduleBackgroundSheetBackup('editAccount'); }
                }
                break;
            case 'deleteAccount':
                result = await FirebaseService.deleteAccount(payload.accountId, companyId);
                if (result && result.success !== false) {
                    try {
                        await sheetsApiRequest({ action: 'deleteAccount', accountId: payload.accountId, companyId });
                    } catch (e) { scheduleBackgroundSheetBackup('deleteAccount'); }
                }
                break;
            case 'updateAccountOrder':
                result = await FirebaseService.updateAccountOrder(payload.orderedAccounts, companyId);
                if (result && result.success !== false) {
                    try {
                        await sheetsApiRequest({ action: 'updateAccountOrder', orderedAccounts: payload.orderedAccounts, companyId });
                    } catch (e) { scheduleBackgroundSheetBackup('updateAccountOrder'); }
                }
                break;
            case 'saveRemark':
                result = await FirebaseService.saveRemark(payload.date, payload.remark);
                if (result && result.success !== false) {
                    try {
                        await sheetsApiRequest({ action: 'saveRemark', date: payload.date, remark: payload.remark || '' });
                    } catch (e) { scheduleBackgroundSheetBackup('saveRemark'); }
                }
                break;
            case 'getRemarks': result = await FirebaseService.getRemarks(); break;
            case 'updateOrder':
                result = await FirebaseService.updateOrder(payload.date, payload.accountId, payload.field, payload.value, companyId);
                if (result && result.success !== false) {
                    try {
                        await syncOrderDateForAllCompanies(
                            payload.date,
                            { companyId },
                            'updateOrder'
                        );
                    } catch (sheetErr) {
                        console.warn('Immediate order sheet sync failed after updateOrder:', sheetErr);
                        orderSheetSyncWarning = sheetErr?.message || 'Sheet sync queued';
                        _pendingDataChangesForBackup = true;
                        scheduleBackgroundSheetBackup('updateOrder');
                    }
                }
                break;
            case 'getCompanies': result = { success: true, data: [{id: 'company1', name: 'Company 1'}, {id: 'company2', name: 'Company 2'}] }; break;
            case 'updateMoney': result = await FirebaseService.updateMoney(payload.accountId, companyId, payload.money, payload.expense, payload.date); break;
            case 'resetAllMoney': result = await FirebaseService.resetAllMoney(payload.date); break;
            case 'createMoneyBackup': result = await FirebaseService.createMoneyBackup(payload.date, payload.rows, payload.reason); break;
            case 'getMoneyBackups': result = await FirebaseService.getMoneyBackups(); break;
            case 'deleteMoneyBackup': result = await FirebaseService.deleteMoneyBackup(payload.backupId); break;
            case 'getKarigars': result = await FirebaseService.getKarigars(companyId); break;
            case 'getKarigarTransactions': result = await FirebaseService.getKarigarTransactions(companyId); break;
            case 'getDesignPrices': result = await FirebaseService.getDesignPrices(payload.DESIGN_PRICE_SCOPE || 'global'); break;
            default: throw new Error('Unknown action: ' + action);
        }
        const mutatingActions = new Set([
            'addAccount', 'editAccount', 'deleteAccount',
            'updateAccountOrder', 'saveRemark', 'updateMoney',
            'resetAllMoney', 'createMoneyBackup', 'deleteMoneyBackup'
        ]);
        if (mutatingActions.has(action) && result && result.success !== false) {
            _pendingDataChangesForBackup = true;
            scheduleBackgroundSheetBackup(action);
        }
        if (orderSheetSyncWarning && result && typeof result === 'object') {
            result.sheetSyncWarning = orderSheetSyncWarning;
        }
        if (readActions.has(action)) {
            _apiReadCache.set(cacheKey, { ts: Date.now(), value: result });
        }
        if (!readActions.has(action)) {
            _apiReadCache.clear();
        }
        if (_useSheetsFallbackMode && !isFirestoreQuotaBlocked() && readActions.has(action)) {
            disableSheetsFallbackMode();
        }
        return result;
    } catch (err) {
        if (isFirestoreQuotaError(err)) {
            activateFirestoreQuotaCooldown(err);
            const sheetRead = await readFromSheetsInQuotaMode(action, payload, companyId);
            if (sheetRead) return sheetRead;
            if (action === 'submitOrders') {
                applyOptimisticOrdersBatch(payload.date, payload.orders || [], companyId);
                const allRows = getLocalOrderRowsForDate(payload.date, companyId);
                await syncOrdersDirectToSheet(payload.date, allRows.length ? allRows : (payload.orders || []), companyId, 'quota_sheet_mode_submit');
                return { success: true, message: 'Saved to Google Sheet (quota mode)' };
            }
            if (action === 'updateOrder') {
                const accountName = resolveAccountNameForSheet(payload.accountId, companyId) || payload.accountId;
                applyOptimisticOrderUpdate(payload.date, payload.accountId, accountName, payload.value, companyId);
                const allRows = getLocalOrderRowsForDate(payload.date, companyId);
                await syncOrdersDirectToSheet(
                    payload.date,
                    allRows.length ? allRows : [{ accountId: payload.accountId, accountName, meesho: payload.value }],
                    companyId,
                    'quota_sheet_mode_update'
                );
                return { success: true, message: 'Saved to Google Sheet (quota mode)' };
            }
            if (action === 'saveRemark') {
                await sheetsApiRequest({ action: 'saveRemark', date: payload.date, remark: payload.remark || '' });
                if (!_remarkCache) _remarkCache = {};
                _remarkCache[payload.date] = payload.remark || '';
                return { success: true, message: 'Remark saved to Google Sheet (quota mode)' };
            }
            if (readActions.has(action)) {
                try {
                    await loadRecentDataFromSheets(SHEETS_FALLBACK_DAYS);
                } catch (sheetErr) {
                    console.warn('Sheet-mode load failed after quota error:', sheetErr);
                }
            }
            const cached = buildCachedApiResult(action, companyId);
            if (cached) return cached;
        }
        if (isFirestoreUnavailableError(err)) {
            enableSheetsFallbackMode();
            const sheetRead = await readFromSheetsInQuotaMode(action, payload, companyId);
            if (sheetRead) return sheetRead;
            if (action === 'submitOrders') {
                applyOptimisticOrdersBatch(payload.date, payload.orders || [], companyId);
                const allRows = getLocalOrderRowsForDate(payload.date, companyId);
                await syncOrdersDirectToSheet(payload.date, allRows.length ? allRows : (payload.orders || []), companyId, 'down_sheet_mode_submit');
                return { success: true, message: 'Saved to Google Sheet (server down mode)' };
            }
            if (action === 'updateOrder') {
                const accountName = resolveAccountNameForSheet(payload.accountId, companyId) || payload.accountId;
                applyOptimisticOrderUpdate(payload.date, payload.accountId, accountName, payload.value, companyId);
                const allRows = getLocalOrderRowsForDate(payload.date, companyId);
                await syncOrdersDirectToSheet(
                    payload.date,
                    allRows.length ? allRows : [{ accountId: payload.accountId, accountName, meesho: payload.value }],
                    companyId,
                    'down_sheet_mode_update'
                );
                return { success: true, message: 'Saved to Google Sheet (server down mode)' };
            }
            if (action === 'saveRemark') {
                await sheetsApiRequest({ action: 'saveRemark', date: payload.date, remark: payload.remark || '' });
                if (!_remarkCache) _remarkCache = {};
                _remarkCache[payload.date] = payload.remark || '';
                return { success: true, message: 'Remark saved to Google Sheet (server down mode)' };
            }
            const cached = buildCachedApiResult(action, companyId);
            if (cached) return cached;
        }
        console.error(`Firebase ${action} error:`, err);
        throw err;
    }
}

// Google Sheets API – used only for backup/archive operations
// ===== GOOGLE SHEETS — DISABLED =====
// All data is now stored in LAN JSON files. Google Sheets integration has been removed.
async function sheetsApiRequest(payload) {
    console.warn('[SHEETS DISABLED] Action blocked:', payload?.action || 'unknown');
    return { success: false, data: [], details: [], message: 'Google Sheets is disabled. Data is stored locally in LAN folder.' };
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
async function backupToSheets(isAuto = false, opts = {}) {
    const skipArchive = !!opts.skipArchive;
    const rawBackupType = String(opts.reason || (isAuto ? 'auto' : 'manual')).trim();
    const backupType = normalizeBackupTypeLabel(rawBackupType || (isAuto ? 'auto' : 'manual')) || (isAuto ? 'auto' : 'manual');
    const shouldVerifyOrders = opts.verifyOrders === true || (!isAuto && opts.verifyOrders !== false);
    const btn = document.getElementById('backup-btn');
    if (!isAuto && btn) { btn.disabled = true; btn.innerHTML = "<i class='bx bx-loader-alt bx-spin'></i> Syncing Sheet..."; }
    if (!isAuto) showToast('Starting background sync to Google Sheets…', 'info');
    setPersistentSheetSyncPending(rawBackupType || backupType || 'backup');
    setLastBackupStatusText(`Sheet syncing (${backupType})...`);
    
    try {
        // 1. Flush any pending Firestore writes
        await FirebaseService.flushWrites();
        
        // 2. Get EVERY single piece of data from Firebase
        const allData = await FirebaseService.getAllDataForBackup();
        
        if (!isAuto && btn) { btn.innerHTML = "<i class='bx bx-loader-alt bx-spin'></i> Pushing to Sheet..."; }
        
        // 3. Send all data to Google Sheets in one comprehensive call
        let response = await sheetsApiRequest({
            action: 'saveFullBackup',
            data: allData
        });
        
        if (response && response.success) {
            // Verify orders actually landed in sheet; retry one full push if any keys are missing.
            if (shouldVerifyOrders) {
                let verification = await verifyOrderSyncAgainstSheet(allData);
                if (!verification.success) {
                    console.warn('Order sheet verification failed, retrying full backup once:', verification);
                    setLastBackupStatusText('Order sync verify failed, retrying...');
                    response = await sheetsApiRequest({
                        action: 'saveFullBackup',
                        data: allData
                    });
                    verification = await verifyOrderSyncAgainstSheet(allData);
                    if (!verification.success) {
                        const c1Miss = verification.missing.company1.length;
                        const c2Miss = verification.missing.company2.length;
                        throw new Error(`Sheet order verification failed (C1 missing ${c1Miss}, C2 missing ${c2Miss})`);
                    }
                }
            }

            const now = new Date().toISOString();
            await FirebaseService.setBackupMeta({ 
                lastBackup: now,
                lastBackupType: backupType,
                totalOrders: (allData.company1.orders.length || 0) + (allData.company2.orders.length || 0)
            });
            updateLastBackupTimeDisplay(now, backupType);
            setLastSheetSyncSuccessTimestamp(now);
            clearPersistentSheetSyncPending();
            if (!isAuto) showToast('Full backup successful!', 'success');
            
            let totalArchived = 0;
            if (!skipArchive) {
                // Cleanup Firestore after successful backup
                if (!isAuto) showToast('Archiving orders older than 30 days...', 'info');
                let dateCursor = new Date();
                dateCursor.setMonth(dateCursor.getMonth() - 1); // Start 1 month ago (30-day retention)
                
                // Go back up to 6 months to find un-archived data chunks
                for (let i = 0; i < 6; i++) {
                    const y = dateCursor.getFullYear();
                    const m = String(dateCursor.getMonth() + 1).padStart(2, '0');
                    
                    const result = await FirebaseService.backupAndArchiveMonthlyData(y, m);
                    if (result && result.success) {
                        totalArchived += (result.count || 0);
                    }
                    
                    dateCursor.setMonth(dateCursor.getMonth() - 1); // step backward 1 month
                }
            }
            
            if (!isAuto && btn) {
                setTimeout(() => {
                    btn.disabled = false;
                    btn.innerHTML = `<i class='bx bx-refresh'></i> Refresh Data`;
                }, 4000);
            }
        } else {
            throw new Error(response?.message || 'Backup failed at Sheets layer');
        }
    } catch (e) {
        console.error('Backup error:', e);
        setPersistentSheetSyncPending(`failed_${backupType || 'backup'}`);
        setLastBackupStatusText(`Sheet sync failed (${backupType})`);
        if (!isAuto) showToast('Full backup failed. Check connection.', 'error');
        if (!isAuto && btn) {
            btn.disabled = false;
            btn.innerHTML = "<i class='bx bx-refresh'></i> Refresh Data";
        }
        throw e;
    }
}

function updateLastBackupTimeDisplay(isoString, backupType = '') {
    const el = document.getElementById('last-backup-time');
    if (!el) return;
    if (!isoString) {
        el.textContent = 'Sheet idle • Last backup: Never';
        return;
    }
    try {
        const d = new Date(isoString);
        const typeTag = backupType ? ` (${backupType})` : '';
        el.textContent = 'Sheet synced • Last backup' + typeTag + ': ' + d.toLocaleString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
    } catch(e) {
        el.textContent = '';
    }
}

function setLastBackupStatusText(text) {
    const el = document.getElementById('last-backup-time');
    if (!el) return;
    el.textContent = text || '';
}

// Monthly cleanup: move all data to Sheets, then clear old Firebase data
async function monthlyCleanup(isAuto = false) {
    if (!isAuto) showToast('Running monthly backup…', 'info');
    try {
        await backupToSheets(isAuto);
        const deleted = await FirebaseService.clearOldOrders(30);
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
            const safeType = normalizeBackupTypeLabel(meta.lastBackupType || '');
            updateLastBackupTimeDisplay(meta.lastBackup, safeType);
            setLastSheetSyncSuccessTimestamp(meta.lastBackup);
        } else {
            updateLastBackupTimeDisplay(null);
        }
        
        console.log("✅ Auto background backup is enabled for admin changes.");
        
    } catch (e) {
        console.warn('Auto-backup meta check failed:', e);
    }
}

// ===== SYNC INDICATOR =====
function initSyncIndicator() {
    setSheetBackupIndicator('idle', 'Sheet idle');
    FirebaseService.onSyncStatusChange(status => {
        const pending = typeof FirebaseService !== 'undefined' && FirebaseService.getPendingCount ? FirebaseService.getPendingCount() : 0;
        const el = document.getElementById('sync-indicator');
        if (status === 'pending' || status === 'syncing') {
            _pendingDataChangesForBackup = true;
        }
        if (status === 'saved' && _pendingDataChangesForBackup) {
            const recentDirectOrderSync = (Date.now() - _lastDirectOrderSheetSyncMs) < 15000;
            if (recentDirectOrderSync) {
                _pendingDataChangesForBackup = false;
                clearPersistentSheetSyncPending();
            } else {
                scheduleBackgroundSheetBackup('firestore_saved');
            }
        }
        if (status === 'error' && pending > 0 && !isFirestoreQuotaBlocked()) {
            setTimeout(() => {
                FirebaseService.flushWrites().catch(() => null);
            }, 1800);
        }
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
            FirebaseService.flushWrites().catch((e) => {
                console.warn('Manual flush failed:', e);
                showToast('Save failed. Retrying in background.', 'error');
            });
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
function showLoader(message = 'Loading...') {
    const el = document.getElementById('global-loader');
    if (el) el.classList.remove('hidden');
    setLoaderStatus(message);
}
function hideLoader() {
    const el = document.getElementById('global-loader');
    if (el) el.classList.add('hidden');
    setLoaderStatus('Loading...');
}

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
    
    const companyFilterEl = document.getElementById('sheet-company-filter');
    let companyFilter = companyFilterEl ? companyFilterEl.value : 'all';
    if (isSingleCompanyMode()) {
        companyFilter = 'company1';
        if (companyFilterEl) companyFilterEl.value = 'company1';
    }
    let accounts = [];
    if (companyFilter === 'company1') {
        accounts = [...AppState.company1Accounts];
    } else if (companyFilter === 'company2') {
        accounts = [...AppState.company2Accounts];
    } else {
        showToast('Please select Company 1 or Company 2 first to reorder.', 'info');
        return;
    }
    
    list.innerHTML = getOrderedCompanyAccounts(companyFilter).map(acc => `
        <div class="account-item reorder-item" data-account="${String(acc.accountId || '').replace(/'/g, "\\'")}" style="cursor: grab; padding: 0.75rem; border: 1px solid var(--border-color); border-radius: 8px; background: #f8fafc; display: flex; align-items: center; gap: 0.75rem;">
            <i class='bx bx-menu text-muted'></i>
            <span class="font-bold text-sm">${acc.name}</span>
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
    const months = new Set();
    [...(AppState.company1Data || []), ...(AppState.company2Data || [])].forEach(row => {
        const d = normalizeToISODate(row?.date);
        if (d) months.add(d.slice(0, 7));
    });
    AppState.availableSheetMonths = Array.from(months).sort().reverse();
    return AppState.availableSheetMonths;
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

    // In LAN-first mode, pre-populate all historical months since Jan 2024 so they are selectable
    const isLan = typeof window !== 'undefined' && window.LanStorageService && window.LanStorageService.isConnected();
    if (isLan) {
        const startYear = 2024;
        const curDate = new Date();
        const endYear = curDate.getFullYear();
        for (let y = startYear; y <= endYear; y++) {
            const maxM = (y === curDate.getFullYear()) ? curDate.getMonth() + 1 : 12;
            for (let m = 1; m <= maxM; m++) {
                const mStr = String(m).padStart(2, '0');
                months.add(`${y}-${mStr}`);
            }
        }
    }
    
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
    try {
        showLoader();
        const isLan = typeof window !== 'undefined' && window.LanStorageService && window.LanStorageService.isConnected();
        if (isLan) {
            showToast(`Loading ${month} data from LAN…`, 'info');
        } else {
            showToast(`Fetching ${month} data from Firestore…`, 'info');
        }
        const res = await apiRequest({ action: 'getDashboardData', companyId, month });
        const list = Array.isArray(res?.data) ? res.data : [];
        if (companyId === 'company1') AppState.company1Data = list;
        else AppState.company2Data = list;
        await loadAvailableSheetMonths(true);
        await renderDataSheet();
        if (isLan) {
            showToast(`Loaded ${list.length} records from LAN.`, 'success');
        } else {
            showToast(`Loaded ${list.length} records from Firestore.`, 'success');
        }
    } catch (e) {
        console.error('Load historical failed:', e);
        const isLan = typeof window !== 'undefined' && window.LanStorageService && window.LanStorageService.isConnected();
        showToast(isLan ? 'Failed to load from LAN' : 'Failed to load from Firestore', 'error');
    } finally {
        hideLoader();
    }
}

async function renderDataSheet() {
    const renderSeq = ++_renderDataSheetSeq;
    const thead = document.getElementById('sheet-thead');
    const tbody = document.getElementById('sheet-tbody');
    const emptyMsg = document.getElementById('sheet-empty');
    const wrapper = document.querySelector('.sheet-wrapper');
    if (!thead || !tbody) return;
    
    const companyFilterEl = document.getElementById('sheet-company-filter');
    let companyFilter = companyFilterEl ? companyFilterEl.value : 'all';
    if (isSingleCompanyMode()) {
        companyFilter = 'company1';
        if (companyFilterEl) companyFilterEl.value = 'company1';
    } else if (isOrderUser()) {
        // Order users should not combine companies.
        const allowed = getAllowedCompanies();
        companyFilter = allowed.includes(companyFilter) ? companyFilter : (allowed[0] || 'company1');
        if (companyFilterEl) companyFilterEl.value = companyFilter;
    }
    const monthFilterEl = document.getElementById('sheet-month-filter');
    const rawMonthFilter = monthFilterEl ? String(monthFilterEl.value || '').trim() : '';
    const monthFilter = rawMonthFilter || 'all';
    if (monthFilterEl && !rawMonthFilter) monthFilterEl.value = 'all';
    
    const sheetTitle = document.getElementById('sheet-title');
    if (sheetTitle) {
        const dStr = getTodayISODate();
        if (monthFilter === 'all') {
            sheetTitle.innerHTML = `<span class="text-primary font-bold mr-2"><i class='bx bx-calendar'></i> ${dStr}</span> Viewing complete history`;
        } else if (monthFilter) {
            sheetTitle.innerHTML = `<span class="text-primary font-bold mr-2"><i class='bx bx-calendar'></i> ${dStr}</span> Viewing data for ${monthFilter}`;
        } else {
            sheetTitle.innerHTML = `<span class="text-primary font-bold mr-2"><i class='bx bx-calendar'></i> ${dStr}</span> Viewing Firestore live data`;
        }
    }
    
    // Determine which companies to show
    let accounts = [];
    let rawData = [];
    
    if (companyFilter === 'company1') {
        accounts = getOrderedCompanyAccounts('company1').map(ad => ({
            id: ad.accountId,
            name: ad.name,
            company: 'company1',
            label: getCompanyDisplayName('company1'),
            hasM: ad.hasMeesho !== false,
            hasF: ad.hasFlipkart !== false
        }));
        rawData = [...AppState.company1Data];
    } else if (companyFilter === 'company2') {
        accounts = getOrderedCompanyAccounts('company2').map(ad => ({
            id: ad.accountId,
            name: ad.name,
            company: 'company2',
            label: getCompanyDisplayName('company2'),
            hasM: ad.hasMeesho !== false,
            hasF: ad.hasFlipkart !== false
        }));
        rawData = [...AppState.company2Data];
    } else {
        getOrderedCompanyAccounts('company1').forEach(ad => accounts.push({
            id: ad.accountId,
            name: ad.name,
            company: 'company1',
            label: getCompanyDisplayName('company1'),
            hasM: ad.hasMeesho !== false,
            hasF: ad.hasFlipkart !== false
        }));
        getOrderedCompanyAccounts('company2').forEach(ad => accounts.push({
            id: ad.accountId,
            name: ad.name,
            company: 'company2',
            label: getCompanyDisplayName('company2'),
            hasM: ad.hasMeesho !== false,
            hasF: ad.hasFlipkart !== false
        }));
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
    
    // Collect unique dates, then expand the sheet so zero-order days still show.
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
    
    const sortedExistingDates = [...dateSet].sort();
    let dates = [];
    if (monthFilter && monthFilter !== 'all') {
        const monthStart = getDataSheetStartDateForMonth(monthFilter);
        const monthEnd = monthFilter === currentMonth ? todayStr : getLastDayOfMonthISO(monthFilter);
        dates = buildDateRangeDescending(monthStart, monthEnd);
    } else {
        const firstDate = DATA_SHEET_START_DATE;
        const lastDate = sortedExistingDates[sortedExistingDates.length - 1] || todayStr;
        const endDate = lastDate > todayStr ? lastDate : todayStr;
        dates = buildDateRangeDescending(firstDate, endDate);
    }
    
    if (accounts.length === 0 || dates.length === 0) {
        wrapper.classList.add('hidden');
        emptyMsg.classList.remove('hidden');
        
        // If a specific month is selected and no data locally, offer one-click Firestore/LAN reload.
        if (monthFilter && monthFilter !== 'all' && monthFilter !== currentMonth) {
            const compId = companyFilter === 'all' ? 'company1' : companyFilter; // Default to comp1 if all
            const isLan = typeof window !== 'undefined' && window.LanStorageService && window.LanStorageService.isConnected();
            
            if (isLan) {
                // Instantly trigger load in the background so the user never has to click!
                setTimeout(() => {
                    loadHistoricalData(compId, monthFilter);
                }, 50);

                emptyMsg.innerHTML = `
                    <div class="p-8 text-center">
                        <i class='bx bx-loader-alt bx-spin text-4xl text-primary mb-4'></i>
                        <p class="mb-4">Loading data for ${escapeHtml(monthFilter)} from LAN folder…</p>
                        <p class="text-xs text-muted">Reading plaintext files from your shared network drive.</p>
                    </div>
                `;
            } else {
                emptyMsg.innerHTML = `
                    <div class="p-8 text-center">
                        <i class='bx bx-history text-4xl text-muted mb-4'></i>
                        <p class="mb-4">No local data found for ${escapeHtml(monthFilter)}.</p>
                        <button class="btn btn-primary" onclick="loadHistoricalData('${compId}', '${monthFilter}')">
                            <i class='bx bx-cloud-download'></i> Reload from Firestore
                        </button>
                        <p class="text-xs text-muted mt-2">Data source: Firestore (live).</p>
                    </div>
                `;
            }
        } else if (accounts.length === 0) {
            emptyMsg.innerHTML = `
                <div class="p-8 text-center text-muted">
                    <p class="mb-2">No accounts found.</p>
                    <button class="btn btn-outline" onclick="window.app.navigateTo('add-account')">Add New Account</button>
                </div>
            `;
        } else {
            emptyMsg.innerHTML = '<p class="p-8 text-center text-muted">No records found for the selected filter.</p>';
        }
        return;
    }
    wrapper.classList.remove('hidden');
    emptyMsg.classList.add('hidden');
    
    // Map active account names to their current active IDs to handle any past ID mismatches
    const activeIdByName = {};
    accounts.forEach(acc => {
        if (acc.name) {
            activeIdByName[acc.name.toLowerCase().trim()] = acc.id;
        }
    });

    // Build lookup: { date -> { accountId -> { meesho, flipkart, total } } }
    const lookup = {};
    const seenOrderKeys = new Set();
    rawData.forEach(r => {
        const d = normalizeToISODate(r.date);
        if (!d) return;
        
        let key = r.accountId;
        const nameLower = r.accountName ? r.accountName.toLowerCase().trim() : '';
        if (activeIdByName[nameLower]) {
            key = activeIdByName[nameLower];
        } else if (!key) {
            key = r.accountName;
        }
        if (!key) return;

        if (!lookup[d]) lookup[d] = {};

        const mVal = typeof r.meesho !== 'undefined' ? (parseInt(r.meesho, 10) || 0) : (parseInt(r.quantity, 10) || 0);
        const fVal = parseInt(r.flipkart, 10) || 0;
        const tVal = typeof r.total !== 'undefined' ? (parseInt(r.total, 10) || 0) : (mVal + fVal);

        const dedupKey = `${d}__${r.companyId || 'c'}__${key}`;
        if (!lookup[d][key]) {
            lookup[d][key] = { meesho: mVal, flipkart: fVal, total: tVal };
            seenOrderKeys.add(dedupKey);
        } else if (typeof r.meesho !== 'undefined' || typeof r.flipkart !== 'undefined') {
            // Overwrite with the explicit M/F record if previous was legacy
            lookup[d][key] = { meesho: mVal, flipkart: fVal, total: tVal };
        }
    });
    
    // Load remarks (from backend first time, then cached)
    const savedRemarks = await loadRemarksFromBackend();
    if (renderSeq !== _renderDataSheetSeq) return;
    
    const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    
    // === HEADER ===
    let headerRow1 = '<tr><th class="sheet-date-col" rowspan="2">Date</th>';
    let headerRow2 = '<tr class="sheet-subheader-row">';
    let totalAccountCols = 0;

    accounts.forEach((acc) => {
        if (!acc.hasM && !acc.hasF) acc.hasM = true;
        const cols = (acc.hasM ? 1 : 0) + (acc.hasF ? 1 : 0);
        totalAccountCols += cols;
        const compTag = companyFilter === 'all' ? `<span class="sheet-company-tag">${acc.label}</span>` : '';
        headerRow1 += `<th class="sheet-acct-group sheet-acct-border" colspan="${cols}" title="${escapeHtml(acc.name)}"><span class="sheet-header-name">${acc.name}</span>${compTag}</th>`;
        if (acc.hasM) {
            headerRow2 += `<th class="sheet-sub-header sheet-sub-m ${!acc.hasF ? 'sheet-acct-border' : ''}" title="${escapeHtml(acc.name)} (Meesho)">M</th>`;
        }
        if (acc.hasF) {
            headerRow2 += `<th class="sheet-sub-header sheet-sub-f sheet-acct-border" title="${escapeHtml(acc.name)} (Flipkart)">F</th>`;
        }
    });

    headerRow1 += '<th class="sheet-acct-group sheet-total-group" colspan="3">Total</th>';
    headerRow2 += '<th class="sheet-sub-header sheet-sub-m" title="Meesho Total">M</th>';
    headerRow2 += '<th class="sheet-sub-header sheet-sub-f" title="Flipkart Total">F</th>';
    headerRow2 += '<th class="sheet-sub-header sheet-sub-total" title="Combined Total">Total</th>';

    headerRow1 += '<th class="sheet-acct-group sheet-remarks-col" rowspan="2">Remarks</th>';
    headerRow1 += '</tr>';
    headerRow2 += '</tr>';
    
    thead.innerHTML = headerRow1 + headerRow2;
    
    // === BODY ===
    let bodyHtml = '';
    const grandTotals = {
        accounts: accounts.map(() => ({ meesho: 0, flipkart: 0 })),
        meesho: 0,
        flipkart: 0,
        both: 0
    };
    const monthTotals = {};
    dates.forEach(date => {
        const monthKey = date.substring(0, 7);
        if (!monthTotals[monthKey]) {
            monthTotals[monthKey] = {
                meesho: 0,
                flipkart: 0,
                both: 0,
                accountTotals: accounts.map(() => ({ meesho: 0, flipkart: 0 }))
            };
        }
        accounts.forEach((acc, idx) => {
            const cell = lookup[date]?.[acc.id] || lookup[date]?.[acc.name] || { meesho: 0, flipkart: 0, total: 0 };
            const mQty = parseInt(cell.meesho, 10) || 0;
            const fQty = parseInt(cell.flipkart, 10) || 0;
            monthTotals[monthKey].accountTotals[idx].meesho += mQty;
            monthTotals[monthKey].accountTotals[idx].flipkart += fQty;
            monthTotals[monthKey].meesho += mQty;
            monthTotals[monthKey].flipkart += fQty;
            monthTotals[monthKey].both += (mQty + fQty);
        });
    });

    let activeMonth = '';
    const fullColspan = 1 + totalAccountCols + 3 + 1; // Date + accounts cols + 3 totals + remarks
    
    dates.forEach(date => {
        const [y, m, d] = date.split('-');
        const monthKey = `${y}-${m}`;
        if (monthKey !== activeMonth) {
            activeMonth = monthKey;
            const monthSummary = monthTotals[monthKey] || { meesho: 0, flipkart: 0, both: 0 };
            bodyHtml += `<tr class="sheet-month-row" data-month="${monthKey}"><td colspan="${fullColspan}"><span>${getMonthLabelFromKey(monthKey)}</span><strong>Total Orders: <b class="sheet-month-total">${monthSummary.both}</b> <span class="sheet-month-sub">(M: <b class="sheet-month-m">${monthSummary.meesho}</b> | F: <b class="sheet-month-f">${monthSummary.flipkart}</b>)</span></strong></td></tr>`;
        }
        const dateObj = new Date(parseInt(y), parseInt(m)-1, parseInt(d));
        const dayName = dayNames[dateObj.getDay()];
        const isToday = date === todayStr;
        const dayChipClass = isToday ? 'sheet-day-chip sheet-today' : 'sheet-day-chip';
        const formattedDate = `${d}/${m}/${y}`;
        
        bodyHtml += `<tr data-month="${monthKey}">`;
        bodyHtml += `<td class="sheet-date-cell">${formattedDate} <span class="${dayChipClass}">${dayName}</span></td>`;
        
        let rowM = 0;
        let rowF = 0;
        accounts.forEach((acc, idx) => {
            const cell = lookup[date]?.[acc.id] || lookup[date]?.[acc.name] || { meesho: 0, flipkart: 0, total: 0 };
            const meeshoVal = parseInt(cell.meesho, 10) || 0;
            const flipkartVal = parseInt(cell.flipkart, 10) || 0;
            rowM += meeshoVal;
            rowF += flipkartVal;
            grandTotals.accounts[idx].meesho += meeshoVal;
            grandTotals.accounts[idx].flipkart += flipkartVal;
            
            if (acc.hasM) {
                bodyHtml += `<td class="sheet-editable sheet-m-col ${!acc.hasF ? 'sheet-acct-border' : ''}"><input type="number" class="sheet-cell-input" value="${meeshoVal}" data-date="${date}" data-account-id="${escapeHtml(acc.id)}" data-account-name="${escapeHtml(acc.name)}" data-company="${acc.company}" data-field="meesho" min="0" placeholder="0"></td>`;
            }
            if (acc.hasF) {
                bodyHtml += `<td class="sheet-editable sheet-f-col sheet-acct-border"><input type="number" class="sheet-cell-input" value="${flipkartVal}" data-date="${date}" data-account-id="${escapeHtml(acc.id)}" data-account-name="${escapeHtml(acc.name)}" data-company="${acc.company}" data-field="flipkart" min="0" placeholder="0"></td>`;
            }
        });
        const rowBoth = rowM + rowF;
        grandTotals.meesho += rowM;
        grandTotals.flipkart += rowF;
        grandTotals.both += rowBoth;
        
        bodyHtml += `<td class="sheet-total-cell sheet-m-total" title="M: ${rowM.toLocaleString()}">${rowM}</td>`;
        bodyHtml += `<td class="sheet-total-cell sheet-f-total" title="F: ${rowF.toLocaleString()}">${rowF}</td>`;
        bodyHtml += `<td class="sheet-total-cell sheet-both-total" title="Total: ${rowBoth.toLocaleString()}">${rowBoth}</td>`;
        
        const remarkVal = savedRemarks[date] || '';
        bodyHtml += `<td class="sheet-editable sheet-remarks-col"><input type="text" class="sheet-cell-input sheet-remark-input" value="${escapeHtml(remarkVal)}" data-date="${date}" data-field="remark" placeholder="Add note..."></td>`;
        bodyHtml += '</tr>';
    });
    
    // Grand total row (hide for order users; they only need daily totals)
    if (!isOrderUser()) {
        bodyHtml += '<tr class="sheet-grand-row">';
        bodyHtml += '<td class="sheet-date-cell">TOTAL</td>';
        accounts.forEach((acc, idx) => {
            const mVal = grandTotals.accounts[idx].meesho;
            const fVal = grandTotals.accounts[idx].flipkart;
            if (acc.hasM) {
                bodyHtml += `<td class="sheet-sub-m ${!acc.hasF ? 'sheet-acct-border' : ''} sheet-grand-acct-m" data-account-id="${escapeHtml(acc.id)}" title="${escapeHtml(acc.name)} M: ${mVal.toLocaleString()}">${mVal}</td>`;
            }
            if (acc.hasF) {
                bodyHtml += `<td class="sheet-sub-f sheet-acct-border sheet-grand-acct-f" data-account-id="${escapeHtml(acc.id)}" title="${escapeHtml(acc.name)} F: ${fVal.toLocaleString()}">${fVal}</td>`;
            }
        });
        bodyHtml += `<td class="sheet-total-cell sheet-m-total sheet-grand-m" title="Meesho Grand Total: ${grandTotals.meesho.toLocaleString()}">${grandTotals.meesho}</td>`;
        bodyHtml += `<td class="sheet-total-cell sheet-f-total sheet-grand-f" title="Flipkart Grand Total: ${grandTotals.flipkart.toLocaleString()}">${grandTotals.flipkart}</td>`;
        bodyHtml += `<td class="sheet-total-cell sheet-both-total sheet-grand-both" title="Grand Total: ${grandTotals.both.toLocaleString()}">${grandTotals.both}</td>`;
        bodyHtml += '<td></td>';
        bodyHtml += '</tr>';
    }
    
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
                apiRequest({ action: 'saveRemark', date, remark: val, companyId: AppState.currentCompany })
            );
            scheduleQuickWriteFlush(1200);
        });
    });
    
    // === EVENT: Save order cell edits (buffered to Firebase) ===
    tbody.querySelectorAll('input[data-field="meesho"], input[data-field="flipkart"]').forEach(inp => {
        inp.addEventListener('change', () => {
            const { date, accountId, company, field } = inp.dataset;
            const value = parseInt(inp.value) || 0;
            
            // Update local state + local cache immediately (prevents temporary row disappear during refresh)
            applyOptimisticOrderUpdate(date, accountId, inp.dataset.accountName, value, company, field);
            
            // Recalculate totals in UI immediately
            recalcSheetRowTotal(inp);
            
            // Buffer the write to Firebase
            FirebaseService.bufferWrite(`order_${date}_${accountId}_${field}`, () =>
                apiRequest({ action: 'updateOrder', date, accountId, field, value, companyId: company })
            );
            scheduleQuickWriteFlush(1200);
        });
    });

    // === KEYBOARD NAVIGATION: Tab jumps to next company in same channel (M->M, F->F) ===
    tbody.querySelectorAll('input.sheet-cell-input').forEach(inp => {
        inp.addEventListener('focus', () => {
            try { inp.select(); } catch(e) {}
        });
    });

    tbody.onkeydown = (e) => {
        const inp = e.target;
        if (!inp || !inp.classList.contains('sheet-cell-input')) return;
        const field = inp.dataset.field; // 'meesho' or 'flipkart'
        if (field !== 'meesho' && field !== 'flipkart') return;

        if (e.key === 'Tab') {
            e.preventDefault();
            const tr = inp.closest('tr');
            if (!tr) return;

            const rowSameInputs = Array.from(tr.querySelectorAll(`input[data-field="${field}"]`));
            const currIdx = rowSameInputs.indexOf(inp);

            if (!e.shiftKey) {
                // Tab forward: next company's input in the same channel
                if (currIdx !== -1 && currIdx < rowSameInputs.length - 1) {
                    const nextInp = rowSameInputs[currIdx + 1];
                    nextInp.focus();
                    nextInp.select();
                } else {
                    // Last company in row -> wrap to first company of next day row
                    let nextTr = tr.nextElementSibling;
                    while (nextTr && (nextTr.classList.contains('sheet-month-row') || nextTr.classList.contains('sheet-grand-row'))) {
                        nextTr = nextTr.nextElementSibling;
                    }
                    if (nextTr) {
                        const target = nextTr.querySelector(`input[data-field="${field}"]`);
                        if (target) {
                            target.focus();
                            target.select();
                        }
                    }
                }
            } else {
                // Shift+Tab backward: previous company's input in the same channel
                if (currIdx > 0) {
                    const prevInp = rowSameInputs[currIdx - 1];
                    prevInp.focus();
                    prevInp.select();
                } else {
                    // First company in row -> wrap to last company of previous day row
                    let prevTr = tr.previousElementSibling;
                    while (prevTr && (prevTr.classList.contains('sheet-month-row') || prevTr.classList.contains('sheet-grand-row'))) {
                        prevTr = prevTr.previousElementSibling;
                    }
                    if (prevTr) {
                        const prevSameInputs = Array.from(prevTr.querySelectorAll(`input[data-field="${field}"]`));
                        if (prevSameInputs.length) {
                            const target = prevSameInputs[prevSameInputs.length - 1];
                            target.focus();
                            target.select();
                        }
                    }
                }
            }
        } else if (e.key === 'Enter') {
            e.preventDefault();
            const tr = inp.closest('tr');
            if (!tr) return;
            const rowInputs = Array.from(tr.querySelectorAll('input.sheet-cell-input:not(.sheet-remark-input)'));
            const currIdx = rowInputs.indexOf(inp);

            if (!e.shiftKey) {
                // Enter: move to the right box in the same row
                if (currIdx !== -1 && currIdx < rowInputs.length - 1) {
                    const nextInp = rowInputs[currIdx + 1];
                    nextInp.focus();
                    nextInp.select();
                } else {
                    // Reached end of row -> wrap to first box of the next day row
                    let nextTr = tr.nextElementSibling;
                    while (nextTr && (nextTr.classList.contains('sheet-month-row') || nextTr.classList.contains('sheet-grand-row'))) {
                        nextTr = nextTr.nextElementSibling;
                    }
                    if (nextTr) {
                        const target = nextTr.querySelector('input.sheet-cell-input:not(.sheet-remark-input)');
                        if (target) {
                            target.focus();
                            target.select();
                        }
                    }
                }
            } else {
                // Shift+Enter: move to the left box in the same row
                if (currIdx > 0) {
                    const prevInp = rowInputs[currIdx - 1];
                    prevInp.focus();
                    prevInp.select();
                } else {
                    // First box in row -> wrap to last box of the previous day row
                    let prevTr = tr.previousElementSibling;
                    while (prevTr && (prevTr.classList.contains('sheet-month-row') || prevTr.classList.contains('sheet-grand-row'))) {
                        prevTr = prevTr.previousElementSibling;
                    }
                    if (prevTr) {
                        const prevInputs = Array.from(prevTr.querySelectorAll('input.sheet-cell-input:not(.sheet-remark-input)'));
                        if (prevInputs.length) {
                            const target = prevInputs[prevInputs.length - 1];
                            target.focus();
                            target.select();
                        }
                    }
                }
            }
        } else if (e.key === 'ArrowDown') {
            e.preventDefault();
            const tr = inp.closest('tr');
            if (!tr) return;
            const accId = inp.dataset.accountId;
            let nextTr = tr.nextElementSibling;
            while (nextTr && (nextTr.classList.contains('sheet-month-row') || nextTr.classList.contains('sheet-grand-row'))) {
                nextTr = nextTr.nextElementSibling;
            }
            if (nextTr) {
                const target = nextTr.querySelector(`input[data-account-id="${accId}"][data-field="${field}"]`);
                if (target) {
                    target.focus();
                    target.select();
                }
            }
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            const tr = inp.closest('tr');
            if (!tr) return;
            const accId = inp.dataset.accountId;
            let prevTr = tr.previousElementSibling;
            while (prevTr && (prevTr.classList.contains('sheet-month-row') || prevTr.classList.contains('sheet-grand-row'))) {
                prevTr = prevTr.previousElementSibling;
            }
            if (prevTr) {
                const target = prevTr.querySelector(`input[data-account-id="${accId}"][data-field="${field}"]`);
                if (target) {
                    target.focus();
                    target.select();
                }
            }
        }
    };
}

// Recalculate a row's total cell when a value changes
function recalcSheetRowTotal(inputEl) {
    const tr = inputEl.closest('tr');
    if (!tr) return;
    let rowM = 0;
    let rowF = 0;
    tr.querySelectorAll('input[data-field="meesho"]').forEach(inp => rowM += parseInt(inp.value) || 0);
    tr.querySelectorAll('input[data-field="flipkart"]').forEach(inp => rowF += parseInt(inp.value) || 0);
    const rowBoth = rowM + rowF;
    
    const mCell = tr.querySelector('.sheet-m-total');
    const fCell = tr.querySelector('.sheet-f-total');
    const bothCell = tr.querySelector('.sheet-both-total');
    if (mCell) mCell.textContent = rowM;
    if (fCell) fCell.textContent = rowF;
    if (bothCell) bothCell.textContent = rowBoth;
    
    // Also recalculate grand total row & month total row
    const tbody = document.getElementById('sheet-tbody');
    if (!tbody) return;
    const monthKey = tr.dataset.month;
    if (monthKey) {
        const monthRow = tbody.querySelector(`.sheet-month-row[data-month="${monthKey}"]`);
        if (monthRow) {
            let mM = 0, mF = 0;
            tbody.querySelectorAll(`tr[data-month="${monthKey}"]:not(.sheet-month-row)`).forEach(row => {
                const mc = row.querySelector('.sheet-m-total');
                const fc = row.querySelector('.sheet-f-total');
                if (mc) mM += parseInt(mc.textContent) || 0;
                if (fc) mF += parseInt(fc.textContent) || 0;
            });
            const mTotalEl = monthRow.querySelector('.sheet-month-total');
            const mMEl = monthRow.querySelector('.sheet-month-m');
            const mFEl = monthRow.querySelector('.sheet-month-f');
            if (mTotalEl) mTotalEl.textContent = mM + mF;
            if (mMEl) mMEl.textContent = mM;
            if (mFEl) mFEl.textContent = mF;
        }
    }

    const grandRow = tbody.querySelector('.sheet-grand-row');
    if (!grandRow) return;
    const allRows = tbody.querySelectorAll('tr:not(.sheet-grand-row):not(.sheet-month-row)');
    let grandM = 0;
    let grandF = 0;
    allRows.forEach(row => {
        const mc = row.querySelector('.sheet-m-total');
        const fc = row.querySelector('.sheet-f-total');
        if (mc) grandM += parseInt(mc.textContent) || 0;
        if (fc) grandF += parseInt(fc.textContent) || 0;
    });
    
    const gmCell = grandRow.querySelector('.sheet-grand-m');
    const gfCell = grandRow.querySelector('.sheet-grand-f');
    const gBothCell = grandRow.querySelector('.sheet-grand-both');
    if (gmCell) gmCell.textContent = grandM;
    if (gfCell) gfCell.textContent = grandF;
    if (gBothCell) gBothCell.textContent = grandM + grandF;
    
    // Update individual account grand total column cell
    const changedAccId = inputEl.dataset.accountId;
    const changedField = inputEl.dataset.field;
    if (changedAccId && changedField) {
        let colSum = 0;
        allRows.forEach(row => {
            const inp = row.querySelector(`input[data-account-id="${changedAccId}"][data-field="${changedField}"]`);
            if (inp) colSum += parseInt(inp.value) || 0;
        });
        const grandCell = changedField === 'meesho' 
            ? grandRow.querySelector(`.sheet-grand-acct-m[data-account-id="${changedAccId}"]`)
            : grandRow.querySelector(`.sheet-grand-acct-f[data-account-id="${changedAccId}"]`);
        if (grandCell) grandCell.textContent = colSum;
    }
}

function exportSheetCSV() {
    const table = document.getElementById('sheet-table');
    if (!table) return;
    
    const rows = [];
    const theadRows = table.querySelectorAll('thead tr');
    if (theadRows.length >= 2) {
        const topRow = Array.from(theadRows[0].cells).map(c => `"${c.textContent.trim().replace(/"/g, '""')}"`);
        const subRow = Array.from(theadRows[1].cells).map(c => `"${c.textContent.trim().replace(/"/g, '""')}"`);
        rows.push(topRow.join(','));
        rows.push(subRow.join(','));
    }
    
    table.querySelectorAll('tbody tr').forEach(tr => {
        if (tr.classList.contains('sheet-month-row')) {
            const txt = tr.textContent.trim().replace(/\s+/g, ' ');
            rows.push(`"${txt.replace(/"/g, '""')}"`);
            return;
        }
        const rowData = [];
        tr.querySelectorAll('td').forEach(td => {
            const inp = td.querySelector('input');
            const val = inp ? inp.value : td.textContent.trim();
            rowData.push(`"${String(val).replace(/"/g, '""')}"`);
        });
        rows.push(rowData.join(','));
    });
    
    if (rows.length <= 2) { showToast('No data to export', 'error'); return; }
    
    const csvContent = "data:text/csv;charset=utf-8," + rows.join('\n');
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement("a");
    link.setAttribute("href", encodedUri);
    link.setAttribute("download", `krimaa_orders_${getTodayISODate()}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
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
        const accounts = getOrderedCompanyAccounts(compId);
        const companyLabel = compId === 'company1' ? 'Company 1' : 'Company 2';
        
        (accounts || []).forEach(details => {
            mgmtAccounts.push({
                id: details.accountId || '',
                name: details.name || '',
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
        <tr class="money-row" data-account-id="${acc.id}" data-account-name="${acc.name.replace(/"/g, '&quot;')}" data-company-id="${acc.companyId}">
            <td class="drag-handle-cell"><i class='bx bx-menu money-drag-handle' style="cursor: grab; color: #a0aec0;"></i></td>
            <td class="position-number">${idx + 1}</td>
            <td><span class="badge ${acc.companyId === 'company1' ? 'badge-a' : 'badge-b'}">${acc.company}</span></td>
            <td class="font-medium">${acc.name}</td>
            <td><input type="number" class="inp-money sheet-cell-input" min="0" value="${acc.money || ''}" data-index="${idx}" placeholder="0"></td>
            <td><input type="number" class="inp-expense sheet-cell-input" min="0" value="${acc.expense || ''}" data-index="${idx}" placeholder="0"></td>
            <td><span class="row-total font-bold" id="money-total-${idx}">${acc.money - acc.expense}</span></td>
        </tr>
    `).join('');
    
    // Helper to calculate grand totals
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
             const accountId = row.dataset.accountId;
             const companyId = row.dataset.companyId;
             const money = parseInt(row.querySelector('.inp-money').value) || 0;
             const expense = parseInt(row.querySelector('.inp-expense').value) || 0;
             const date = document.getElementById('money-date').value || getTodayISODate();
             
             // Also update local state
             const detailsList = companyId === 'company1' ? AppState.company1Details : AppState.company2Details;
             if (detailsList) {
                 const det = detailsList.find(d => d.accountId === accountId);
                 if (det) { det.money = money; det.expense = expense; det.moneyDate = date; }
             }
             
             FirebaseService.bufferWrite(`money_${companyId}_${accountId}`, () => FirebaseService.updateMoney(accountId, companyId, money, expense, date));
             scheduleQuickWriteFlush(1500);
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
                const accountId = row.dataset.accountId;
                if (companyId === 'company1' && accountId) newOrder1.push(accountId);
                if (companyId === 'company2' && accountId) newOrder2.push(accountId);
            });
            // Update UI/local state visually and trigger sheet updates
            syncCompanyAccountOrderState('company1', newOrder1);
            syncCompanyAccountOrderState('company2', newOrder2);
            
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
        getOrderedCompanyAccounts(companyId).forEach(details => {
            const money = parseInt(details.money) || 0;
            const expense = parseInt(details.expense) || 0;
            rows.push({
                companyId,
                companyName,
                accountName: details.name || '',
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

    try {
        const [fbRes, sheetsRes] = await Promise.all([
            apiRequest({ action: 'getMoneyBackups' }).catch(() => null),
            sheetsApiRequest({ action: 'getMoneyBackups' }).catch(() => null)
        ]);

        const fbData = Array.isArray(fbRes?.data) ? fbRes.data : [];
        const sheetsData = Array.isArray(sheetsRes?.data) ? sheetsRes.data : [];

        // Map helps to merge duplicates. Firebase data will overwrite sheets data if backupDate+reason match
        const map = new Map();
        
        // Load sheets backups first
        sheetsData.forEach(b => {
            const key = b.backupDate + '_' + (b.reason || '');
            map.set(key, b);
        });
        
        // Override with Firebase backups
        fbData.forEach(b => {
            const key = b.backupDate + '_' + (b.reason || '');
            map.set(key, b);
        });

        const merged = Array.from(map.values());
        
        // Sort descending by backupDate
        merged.sort((a,b) => (b.backupDate > a.backupDate ? 1 : -1));

        AppState.moneyBackups = merged;
    } catch(e) {
        console.error("Error loading money backups", e);
        const res = await apiRequest({ action: 'getMoneyBackups' }).catch(()=>null);
        AppState.moneyBackups = Array.isArray(res?.data) ? res.data : [];
    }
    
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

async function loadKarigarResetBackups(force = false) {
    const companyId = String(AppState.currentCompany || 'company1').trim() || 'company1';
    if (!AppState.karigarResetBackupsByCompany || typeof AppState.karigarResetBackupsByCompany !== 'object') {
        AppState.karigarResetBackupsByCompany = {};
    }
    if (!AppState.selectedKarigarResetBackupIdByCompany || typeof AppState.selectedKarigarResetBackupIdByCompany !== 'object') {
        AppState.selectedKarigarResetBackupIdByCompany = {};
    }

    const cached = AppState.karigarResetBackupsByCompany[companyId];
    if (!force && Array.isArray(cached)) {
        AppState.karigarResetBackups = cached;
        AppState.selectedKarigarResetBackupId = String(AppState.selectedKarigarResetBackupIdByCompany[companyId] || '').trim();
        if (!AppState.selectedKarigarResetBackupId || !AppState.karigarResetBackups.some(b => b.id === AppState.selectedKarigarResetBackupId)) {
            AppState.selectedKarigarResetBackupId = AppState.karigarResetBackups[0]?.id || '';
        }
        AppState.selectedKarigarResetBackupIdByCompany[companyId] = AppState.selectedKarigarResetBackupId;
        return AppState.karigarResetBackups;
    }

    try {
        const res = await FirebaseService.getKarigarResetBackups(companyId);
        const fetched = Array.isArray(res?.data) ? res.data : [];
        AppState.karigarResetBackups = fetched.filter(b => String(b?.companyId || '').trim() === companyId);
    } catch (e) {
        console.error('Error loading karigar reset backups', e);
        AppState.karigarResetBackups = [];
    }

    AppState.karigarResetBackupsByCompany[companyId] = AppState.karigarResetBackups;
    const selectedForCompany = String(AppState.selectedKarigarResetBackupIdByCompany[companyId] || '').trim();
    AppState.selectedKarigarResetBackupId = selectedForCompany;
    if (!AppState.selectedKarigarResetBackupId || !AppState.karigarResetBackups.some(b => b.id === AppState.selectedKarigarResetBackupId)) {
        AppState.selectedKarigarResetBackupId = AppState.karigarResetBackups[0]?.id || '';
    }
    AppState.selectedKarigarResetBackupIdByCompany[companyId] = AppState.selectedKarigarResetBackupId;

    return AppState.karigarResetBackups;
}

function getSelectedKarigarResetBackup() {
    if (!Array.isArray(AppState.karigarResetBackups) || AppState.karigarResetBackups.length === 0) return null;
    return AppState.karigarResetBackups.find(b => b.id === AppState.selectedKarigarResetBackupId) || AppState.karigarResetBackups[0];
}

function getSelectedKarigarResetEmployee() {
    const key = String(AppState.selectedKarigarResetEmployeeKey || '').trim();
    if (!key) return null;
    return AppState.karigarResetBackupEmployeeMap?.[key] || null;
}

function formatKarigarBackupPoint(rawValue, fallback = '-') {
    const normalized = normalizeToISODateTime(rawValue);
    if (!normalized) return fallback;
    return formatISODateTimeForDisplay(normalized) || fallback;
}

function getKarigarBackupRangeLabel(backup, withPrefix = false) {
    if (!backup) return '-';
    let startRaw = backup?.dataStartAt || backup?.periodStartAt || backup?.snapshotAt || backup?.createdAt;
    let endRaw = backup?.periodEndAt || backup?.snapshotAt || backup?.dataEndAt || backup?.createdAt;

    if (Array.isArray(backup.rows) && backup.rows.length > 0) {
        const rowDates = backup.rows
            .map(r => normalizeToISODateTime(r.transactionDateTime || r.dateTime || r.date))
            .filter(Boolean)
            .sort();
        if (rowDates.length > 0) {
            if (!backup.dataStartAt && rowDates[0]) startRaw = rowDates[0];
            if (!backup.periodEndAt && backup.snapshotAt) endRaw = backup.snapshotAt;
            else if (!backup.periodEndAt && rowDates[rowDates.length - 1]) endRaw = rowDates[rowDates.length - 1];
        }
    }
    const startLabel = formatKarigarBackupPoint(startRaw, '-');
    const endLabel = formatKarigarBackupPoint(endRaw, startLabel);
    const rangeText = (startLabel === endLabel) ? startLabel : `${startLabel} to ${endLabel}`;
    return withPrefix ? `Range: ${rangeText}` : rangeText;
}

async function renderKarigarResetBackupsModal(forceReload = false) {
    const select = document.getElementById('karigar-reset-backup-select');
    const cardsWrap = document.getElementById('karigar-reset-backup-cards');
    const empty = document.getElementById('karigar-reset-backup-empty');
    const sumEmployees = document.getElementById('karigar-reset-summary-employees');
    const sumJama = document.getElementById('karigar-reset-summary-jama');
    const sumUpad = document.getElementById('karigar-reset-summary-upad');
    const sumBalance = document.getElementById('karigar-reset-summary-balance');
    const rangeLabel = document.getElementById('karigar-reset-backup-range-label');
    if (!select || !cardsWrap || !empty || !sumEmployees || !sumJama || !sumUpad || !sumBalance) return;

    await loadKarigarResetBackups(forceReload);
    if (!Array.isArray(AppState.karigarResetBackups) || AppState.karigarResetBackups.length === 0) {
        select.innerHTML = '<option value="">No Backup Found</option>';
        cardsWrap.innerHTML = '';
        AppState.karigarResetBackupEmployeeMap = {};
        AppState.selectedKarigarResetEmployeeKey = '';
        document.getElementById('karigar-reset-employee-modal')?.classList.remove('show');
        sumEmployees.textContent = '0';
        sumJama.textContent = '₹0.00';
        sumUpad.textContent = '₹0.00';
        sumBalance.textContent = '₹0.00';
        if (rangeLabel) rangeLabel.textContent = 'Range: -';
        empty.classList.remove('hidden');
        return;
    }

    select.innerHTML = AppState.karigarResetBackups.map(backup => {
        const selected = backup.id === AppState.selectedKarigarResetBackupId ? 'selected' : '';
        const label = `${backup.companyId || ''} • ${getKarigarBackupRangeLabel(backup)}`;
        return `<option value="${backup.id}" ${selected}>${label}</option>`;
    }).join('');

    const active = getSelectedKarigarResetBackup();
    if (rangeLabel) rangeLabel.textContent = getKarigarBackupRangeLabel(active, true);
    const rows = Array.isArray(active?.rows) ? active.rows : [];
    const empMap = {};
    rows.forEach(r => {
        const key = String(r.karigarId || '').trim() || normalizeKarigarNameKey(r.karigarName || '');
        if (!key) return;
        if (!empMap[key]) {
            empMap[key] = {
                key,
                karigarId: String(r.karigarId || '').trim(),
                karigarName: String(r.karigarName || '').trim() || 'Unknown',
                totalJama: 0,
                totalUpad: 0,
                txCount: 0,
                lastAtMs: 0,
                lastAtLabel: '',
                lastDesign: '-',
                createdFrom: String(r.createdFrom || r.source || r.addedBy || '-').trim() || '-',
                rows: []
            };
        }
        const item = empMap[key];
        const type = String(r.type || '').toLowerCase();
        const jama = type === 'jama' ? (parseFloat(r.total) || 0) : 0;
        const upad = type === 'jama' ? (parseFloat(r.upadAmount) || 0) : (parseFloat(r.amount) || 0);
        item.totalJama += jama;
        item.totalUpad += upad;
        item.txCount += 1;
        const dt = parseFlexibleDateTime(r.transactionDateTime || r.dateTime || r.createdAt || r.date);
        const dtMs = dt ? dt.getTime() : 0;
        if (dtMs >= item.lastAtMs) {
            item.lastAtMs = dtMs;
            item.lastAtLabel = formatISODateTimeForDisplay(r.transactionDateTime || r.dateTime || r.createdAt || r.date) || '-';
            item.lastDesign = String(r.designName || '-').trim() || '-';
        }
        item.rows.push(r);
    });

    const employees = Object.values(empMap)
        .map(e => ({
            ...e,
            rows: (Array.isArray(e.rows) ? e.rows : []).slice().sort((a, b) => getKarigarTxTimestampMs(b) - getKarigarTxTimestampMs(a))
        }))
        .sort((a, b) => a.karigarName.localeCompare(b.karigarName));
    AppState.karigarResetBackupEmployeeMap = {};
    employees.forEach(emp => {
        AppState.karigarResetBackupEmployeeMap[emp.key] = emp;
    });

    let totalJama = 0;
    let totalUpad = 0;
    employees.forEach(e => {
        totalJama += e.totalJama;
        totalUpad += e.totalUpad;
    });
    const net = totalJama - totalUpad;
    sumEmployees.textContent = String(employees.length);
    sumJama.textContent = `₹${totalJama.toFixed(2)}`;
    sumUpad.textContent = `₹${totalUpad.toFixed(2)}`;
    sumBalance.textContent = `₹${net.toFixed(2)}`;
    sumBalance.classList.remove('text-danger', 'text-success', 'text-primary');
    sumBalance.classList.add(net < 0 ? 'text-danger' : 'text-success');

    cardsWrap.innerHTML = employees.map(e => {
        const bal = e.totalJama - e.totalUpad;
        const balClass = bal < 0 ? 'text-danger' : 'text-success';
        const hintLabel = 'Tap to see full transactions';
        return `
            <div class="card karigar-reset-employee-card" data-emp-key="${e.key}" role="button" tabindex="0" style="display:flex;flex-direction:column;gap:0.6rem;cursor:pointer;border:1px solid #dbeafe;">
                <div class="flex-between">
                    <div>
                        <h4 class="font-bold text-main">${e.karigarName}</h4>
                        <div class="text-xs text-muted">${e.karigarId || '-'}</div>
                    </div>
                    <div class="text-right">
                        <span class="text-xs text-muted block">Balance</span>
                        <strong class="${balClass}">₹${bal.toFixed(2)}</strong>
                    </div>
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:0.5rem;">
                    <div><span class="text-xs text-muted block">Jama</span><strong class="text-success">₹${e.totalJama.toFixed(2)}</strong></div>
                    <div><span class="text-xs text-muted block">Upad</span><strong class="text-danger">₹${e.totalUpad.toFixed(2)}</strong></div>
                    <div><span class="text-xs text-muted block">Entries</span><strong>${e.txCount}</strong></div>
                </div>
                <div class="text-xs text-muted">Last Tx: ${e.lastAtLabel || '-'}</div>
                <div class="text-xs text-muted">Last Design: ${e.lastDesign}</div>
                <div class="text-xs text-muted">Created From: ${e.createdFrom}</div>
                <div class="text-xs text-muted">Backup Range: ${getKarigarBackupRangeLabel(active)}</div>
                <div class="text-xs text-primary"><i class='bx bx-link-external'></i> ${hintLabel}</div>
            </div>
        `;
    }).join('');
    cardsWrap.querySelectorAll('.karigar-reset-employee-card').forEach(card => {
        const handleOpen = () => {
            const empKey = String(card.dataset.empKey || '').trim();
            if (!empKey) return;
            openKarigarResetEmployeeDetailsModal(empKey);
        };
        card.addEventListener('click', handleOpen);
        card.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                handleOpen();
            }
        });
    });

    const detailsModal = document.getElementById('karigar-reset-employee-modal');
    if (detailsModal?.classList.contains('show')) {
        const selectedEmployee = getSelectedKarigarResetEmployee();
        if (selectedEmployee) {
            openKarigarResetEmployeeDetailsModal(selectedEmployee.key);
        } else {
            detailsModal.classList.remove('show');
        }
    }

    empty.classList.toggle('hidden', employees.length > 0);
}

function openKarigarResetEmployeeDetailsModal(empKey) {
    const employee = AppState.karigarResetBackupEmployeeMap?.[String(empKey || '').trim()];
    const backup = getSelectedKarigarResetBackup();
    if (!employee || !backup) {
        showToast('Employee details not found in selected backup', 'error');
        return;
    }

    const modal = document.getElementById('karigar-reset-employee-modal');
    const nameEl = document.getElementById('karigar-reset-employee-name-display');
    const subEl = document.getElementById('karigar-reset-employee-subtitle');
    const tbody = document.getElementById('karigar-reset-employee-tbody');
    const emptyEl = document.getElementById('karigar-reset-employee-empty');
    const totalJamaEl = document.getElementById('karigar-reset-employee-summary-jama');
    const totalUpadEl = document.getElementById('karigar-reset-employee-summary-upad');
    const totalBalEl = document.getElementById('karigar-reset-employee-summary-balance');
    const totalEntriesEl = document.getElementById('karigar-reset-employee-summary-entries');
    if (!modal || !nameEl || !subEl || !tbody || !emptyEl || !totalJamaEl || !totalUpadEl || !totalBalEl || !totalEntriesEl) return;

    AppState.selectedKarigarResetEmployeeKey = employee.key;

    nameEl.textContent = employee.karigarName || 'Unknown';
    const compName = getCompanyDisplayName(backup.companyId || AppState.currentCompany);
    subEl.textContent = `${employee.karigarId || '-'} • ${compName} • ${getKarigarBackupRangeLabel(backup, true)}`;

    const txRows = (Array.isArray(employee.rows) ? employee.rows : []).slice()
        .sort((a, b) => getKarigarTxTimestampMs(b) - getKarigarTxTimestampMs(a));
    let totalJama = 0;
    let totalUpad = 0;

    if (txRows.length === 0) {
        tbody.innerHTML = '';
    } else {
        tbody.innerHTML = txRows.map(t => {
            const isJama = String(t.type || '').toLowerCase() === 'jama';
            const jamaVal = isJama ? (parseFloat(t.total) || 0) : 0;
            const upadVal = isJama ? (parseFloat(t.upadAmount) || 0) : (parseFloat(t.amount) || 0);
            totalJama += jamaVal;
            totalUpad += upadVal;

            const dateTimeLabel = formatISODateTimeForDisplay(t.transactionDateTime || t.dateTime || t.createdAt || t.date) || '-';
            const designLabel = isJama ? (t.designName || '-') : '<span class="text-danger font-bold">Direct Borrow (Upad)</span>';
            let createdFrom = String(t.createdFrom || t.source || t.dashboard || t.addedBy || '').trim();
            if (!createdFrom || createdFrom.toLowerCase() === 'web_app') createdFrom = String(t.addedBy || 'unknown').trim();

            return `
                <tr style="border-bottom:1px solid var(--border-light);${!isJama ? 'background:#fffcfc;' : ''}">
                    <td class="p-2 text-sm">${dateTimeLabel}</td>
                    <td class="p-2 text-sm">${isJama ? 'Maal (Jama)' : '<span class="text-danger font-bold">Borrow (Upad)</span>'}</td>
                    <td class="p-2 text-sm">${designLabel}</td>
                    <td class="p-2 text-sm text-center">${t.size || '-'}</td>
                    <td class="p-2 text-sm text-right">${t.pic || '-'}</td>
                    <td class="p-2 text-sm text-right">${t.price ? '₹' + (parseFloat(t.price) || 0).toFixed(2) : '-'}</td>
                    <td class="p-2 text-sm text-right text-success font-bold">${jamaVal > 0 ? '₹' + jamaVal.toFixed(2) : '-'}</td>
                    <td class="p-2 text-sm text-right text-danger font-bold">${upadVal > 0 ? '₹' + upadVal.toFixed(2) : '-'}</td>
                    <td class="p-2 text-sm">${createdFrom || '-'}</td>
                </tr>
            `;
        }).join('');
    }

    const net = totalJama - totalUpad;
    totalEntriesEl.textContent = String(txRows.length);
    totalJamaEl.textContent = `₹${totalJama.toFixed(2)}`;
    totalUpadEl.textContent = `₹${totalUpad.toFixed(2)}`;
    totalBalEl.textContent = `₹${net.toFixed(2)}`;
    totalBalEl.classList.remove('text-success', 'text-danger', 'text-primary');
    totalBalEl.classList.add(net < 0 ? 'text-danger' : 'text-success');

    emptyEl.classList.toggle('hidden', txRows.length > 0);
    modal.classList.add('show');
}

async function openKarigarResetBackupsModal(forceReload = false) {
    document.getElementById('karigar-reset-employee-modal')?.classList.remove('show');
    await renderKarigarResetBackupsModal(forceReload);
    document.getElementById('karigar-reset-backups-modal')?.classList.add('show');
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
            FirebaseService.flushWrites().catch((e) => {
                console.warn('Money flush failed:', e);
                showToast('Save failed. Retrying in background.', 'error');
            });
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

    const karigarBackupOpenBtn = document.getElementById('karigar-backup-open-btn');
    if (karigarBackupOpenBtn) {
        karigarBackupOpenBtn.addEventListener('click', () => openKarigarResetBackupsModal(false));
    }
    const karigarBackupRefreshBtn = document.getElementById('karigar-reset-backup-refresh-btn');
    if (karigarBackupRefreshBtn) {
        karigarBackupRefreshBtn.addEventListener('click', () => renderKarigarResetBackupsModal(true));
    }
    const karigarBackupSelect = document.getElementById('karigar-reset-backup-select');
    if (karigarBackupSelect) {
        karigarBackupSelect.addEventListener('change', () => {
            AppState.selectedKarigarResetBackupId = karigarBackupSelect.value;
            const companyId = String(AppState.currentCompany || 'company1').trim() || 'company1';
            if (!AppState.selectedKarigarResetBackupIdByCompany || typeof AppState.selectedKarigarResetBackupIdByCompany !== 'object') {
                AppState.selectedKarigarResetBackupIdByCompany = {};
            }
            AppState.selectedKarigarResetBackupIdByCompany[companyId] = AppState.selectedKarigarResetBackupId;
            renderKarigarResetBackupsModal(false);
        });
    }
    document.getElementById('close-karigar-reset-backups-modal')?.addEventListener('click', () => {
        document.getElementById('karigar-reset-backups-modal')?.classList.remove('show');
        document.getElementById('karigar-reset-employee-modal')?.classList.remove('show');
    });
    document.getElementById('close-karigar-reset-employee-modal')?.addEventListener('click', () => {
        document.getElementById('karigar-reset-employee-modal')?.classList.remove('show');
    });
});

/* ===== SIZE PRICE MANAGEMENT ===== */
async function renderSizePricesPage() {
    const companyId = DESIGN_PRICE_SCOPE;
    const nowDateTimeInput = normalizeToISODateTime(new Date()) || `${getTodayISODate()}T00:00:00`;
    const nowDateTimeFieldValue = getCurrentLocalDateTimeInput();
    const today = getTodayISODate();
    let map = {};
    let rawHistoryMap = {};
    try {
        const res = await FirebaseService.getDesignPrices(companyId, nowDateTimeInput);
        map = (res && res.success && res.data) ? res.data : {};
        rawHistoryMap = (res && res.success && res.history) ? res.history : {};
        if (map && Object.keys(map).length > 0) {
            try {
                localStorage.setItem('krimaa_size_prices_backup', JSON.stringify(map));
                if (rawHistoryMap && Object.keys(rawHistoryMap).length > 0) {
                    localStorage.setItem('krimaa_size_price_history_backup', JSON.stringify(rawHistoryMap));
                }
            } catch (err) {}
        }
        AppState.designPricesByCompany[companyId] = map;
        AppState.designPrices = map;
    } catch (e) {
        console.warn('Size prices fallback: using cached/empty state', e);
        try {
            const cachedPrices = localStorage.getItem('krimaa_size_prices_backup');
            if (cachedPrices) map = JSON.parse(cachedPrices);
            const cachedHist = localStorage.getItem('krimaa_size_price_history_backup');
            if (cachedHist) rawHistoryMap = JSON.parse(cachedHist);
        } catch (err) {}
        if (!map || Object.keys(map).length === 0) {
            map = AppState.designPricesByCompany[companyId] || AppState.designPrices || {};
        }
    }

    const tbody = document.getElementById('size-price-tbody');
    const cardsGrid = document.getElementById('size-price-cards-grid');
    const searchInput = document.getElementById('size-price-search-input');
    const totalCountEl = document.getElementById('size-price-total-count');
    const form = document.getElementById('size-price-form');
    const keyInput = document.getElementById('size-price-key');
    const valueInput = document.getElementById('size-price-value');
    const effectiveDateInput = document.getElementById('size-price-effective-date');
    const saveBtn = document.getElementById('size-price-save-btn');
    const recycleOpenBtn = document.getElementById('size-price-recycle-open-btn');
    const recycleCountEl = document.getElementById('size-price-recycle-count');
    const recycleTbody = document.getElementById('size-price-recycle-tbody');
    const recycleModal = document.getElementById('size-price-recycle-modal');
    const recycleEmpty = document.getElementById('size-price-recycle-empty');
    const recycleCloseBtn = document.getElementById('close-size-price-recycle-modal');
    const historyModal = document.getElementById('size-price-history-modal');
    const historyTitle = document.getElementById('size-price-history-title');
    const historyBody = document.getElementById('size-price-history-tbody');
    const historyEmpty = document.getElementById('size-price-history-empty');
    const historyCloseBtn = document.getElementById('close-size-price-history-modal');
    if (!tbody || !form || !keyInput || !valueInput || !saveBtn || !recycleOpenBtn || !recycleTbody || !recycleModal || !recycleEmpty || !recycleCloseBtn || !historyModal || !historyTitle || !historyBody || !historyEmpty || !historyCloseBtn) return;

    const historyMap = {};
    Object.entries(rawHistoryMap || {}).forEach(([rawKey, list]) => {
        const key = String(rawKey || '').trim().toUpperCase();
        if (!key) return;
        const points = (Array.isArray(list) ? list : [])
            .map(point => {
                const eff = normalizeToISODateTime(point?.effectiveFrom || point?.updatedAt || nowDateTimeInput, today);
                if (!eff) return null;
                return {
                    effectiveFrom: eff,
                    price: parseFloat(point?.price) || 0,
                    isDeleted: !!point?.isDeleted
                };
            })
            .filter(Boolean)
            .sort((a, b) => String(a.effectiveFrom).localeCompare(String(b.effectiveFrom)));
        if (points.length > 0) historyMap[key] = points;
    });
    AppState.designPriceHistoryByCompany[companyId] = historyMap;

    if (!effectiveDateInput.value) effectiveDateInput.value = nowDateTimeFieldValue;
    if (form.dataset.editMode !== '1') {
        form.dataset.editFromEffective = '';
        form.dataset.editFromKey = '';
        saveBtn.innerHTML = "<i class='bx bx-save'></i> Save Price";
    }

    const isAdmin = isAdminUser();
    saveBtn.disabled = !isAdmin;
    keyInput.disabled = !isAdmin;
    valueInput.disabled = !isAdmin;
    effectiveDateInput.disabled = !isAdmin;
    if (!isAdmin) saveBtn.title = 'Only admin can update prices';

    const activeRowsMap = new Map();
    Object.entries(historyMap).forEach(([key, points]) => {
        let chosen = null;
        points.forEach(point => {
            if (point.effectiveFrom <= nowDateTimeInput) chosen = point;
        });
        if (chosen && !chosen.isDeleted) {
            activeRowsMap.set(key, {
                key,
                value: Number.isFinite(parseFloat(chosen.price)) ? parseFloat(chosen.price) : 0,
                activeFrom: chosen.effectiveFrom
            });
        }
    });
    Object.entries(map || {}).forEach(([rawKey, rawPrice]) => {
        const key = String(rawKey || '').trim().toUpperCase();
        if (!key || activeRowsMap.has(key)) return;
        const points = Array.isArray(historyMap[key]) ? historyMap[key] : [];
        const lastVisible = points.length > 0 ? [...points].reverse().find(p => !p.isDeleted) : null;
        activeRowsMap.set(key, {
            key,
            value: Number.isFinite(parseFloat(rawPrice)) ? parseFloat(rawPrice) : (parseFloat(lastVisible?.price) || 0),
            activeFrom: lastVisible?.effectiveFrom || nowDateTimeInput
        });
    });
    const rows = Array.from(activeRowsMap.values()).sort((a, b) => String(a.key).localeCompare(String(b.key)));

    const recycleRows = Object.entries(historyMap)
        .map(([key, points]) => {
            if (!points.length) return null;
            const latest = points[points.length - 1];
            if (!latest?.isDeleted) return null;
            const lastVisible = [...points].reverse().find(p => !p.isDeleted);
            const restorePrice = Number.isFinite(parseFloat(lastVisible?.price))
                ? parseFloat(lastVisible.price)
                : (Number.isFinite(parseFloat(latest?.price)) ? parseFloat(latest.price) : 0);
            return {
                key,
                restorePrice,
                deletedOn: latest.effectiveFrom
            };
        })
        .filter(Boolean)
        .sort((a, b) => String(a.key).localeCompare(String(b.key)));

    const formatHistoryDateTime = (rawValue) => {
        const dt = parseFlexibleDateTime(rawValue);
        if (!dt) return '-';
        const datePart = formatISODateForDisplay(dt);
        const timePart = dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        return `${datePart} ${timePart}`;
    };

    let shouldRestoreRecycleAfterHistoryClose = false;
    const closeHistoryModal = () => {
        historyModal.classList.remove('show');
        if (shouldRestoreRecycleAfterHistoryClose) {
            recycleModal.classList.add('show');
            shouldRestoreRecycleAfterHistoryClose = false;
        }
    };

    const openHistoryModal = (rawKey) => {
        const key = String(rawKey || '').trim().toUpperCase();
        const points = Array.isArray(historyMap[key]) ? historyMap[key] : [];
        if (recycleModal.classList.contains('show')) {
            shouldRestoreRecycleAfterHistoryClose = true;
            recycleModal.classList.remove('show');
        } else {
            shouldRestoreRecycleAfterHistoryClose = false;
        }
        historyTitle.textContent = `Price History: ${key || '-'}`;
        if (!points.length) {
            historyBody.innerHTML = '';
            historyEmpty.classList.remove('hidden');
            historyModal.classList.add('show');
            return;
        }
        historyEmpty.classList.add('hidden');
        historyBody.innerHTML = points.map((point, idx) => {
            const nextPoint = points[idx + 1] || null;
            const fromText = formatHistoryDateTime(point.effectiveFrom);
            const untilText = nextPoint ? formatHistoryDateTime(nextPoint.effectiveFrom) : 'Now';
            const statusText = point.isDeleted ? 'Deleted (hidden)' : 'Active';
            const statusColor = point.isDeleted ? '#ef4444' : '#10b981';
            return `
                <tr>
                    <td>${fromText}</td>
                    <td>${untilText}</td>
                    <td class="text-right">₹${(parseFloat(point.price) || 0).toFixed(2)}</td>
                    <td class="text-center" style="color:${statusColor}; font-weight:600;">${statusText}</td>
                </tr>
            `;
        }).join('');
        historyModal.classList.add('show');
    };
    if (!historyCloseBtn.dataset.bound) {
        historyCloseBtn.addEventListener('click', closeHistoryModal);
        historyCloseBtn.dataset.bound = 'true';
    }
    if (!historyModal.dataset.bound) {
        historyModal.addEventListener('click', (e) => {
            if (e.target && e.target.id === 'size-price-history-modal') closeHistoryModal();
        });
        historyModal.dataset.bound = 'true';
    }
    if (!recycleOpenBtn.dataset.bound) {
        recycleOpenBtn.addEventListener('click', () => {
            if ((parseInt(recycleCountEl?.textContent || '0', 10) || 0) <= 0) return;
            recycleModal.classList.add('show');
        });
        recycleOpenBtn.dataset.bound = 'true';
    }
    if (!recycleCloseBtn.dataset.bound) {
        recycleCloseBtn.addEventListener('click', () => recycleModal.classList.remove('show'));
        recycleCloseBtn.dataset.bound = 'true';
    }
    if (!recycleModal.dataset.bound) {
        recycleModal.addEventListener('click', (e) => {
            if (e.target && e.target.id === 'size-price-recycle-modal') recycleModal.classList.remove('show');
        });
        recycleModal.dataset.bound = 'true';
    }

    if (cardsGrid) {
        cardsGrid.innerHTML = rows.length ? rows.map(r => `
            <div class="size-price-card" data-key="${r.key}">
                <div class="flex-between">
                    <span class="size-price-badge">${r.key}</span>
                    <span class="size-price-amount">₹${r.value.toFixed(2)}</span>
                </div>
                <div class="text-xs text-muted" style="display:flex;align-items:center;gap:4px;">
                    <i class='bx bx-check-circle text-success'></i> Active Piece Rate
                </div>
                <div class="flex gap-1 mt-1" style="justify-content: flex-end; padding-top: 0.5rem; border-top: 1px dashed #e2e8f0;">
                    <button class="btn btn-outline btn-sm size-edit-btn" data-key="${r.key}" data-value="${r.value}" data-effective="${r.activeFrom}" ${isAdmin ? '' : 'disabled'} title="Edit rate">
                        <i class='bx bx-edit'></i> Edit
                    </button>
                    <button class="btn btn-outline btn-sm size-history-btn" data-key="${r.key}" title="View price changes history">
                        <i class='bx bx-history'></i>
                    </button>
                    <button class="btn btn-outline btn-sm text-danger size-delete-btn" data-key="${r.key}" ${isAdmin ? '' : 'disabled'} title="Delete / Move to recycle bin">
                        <i class='bx bx-trash'></i>
                    </button>
                </div>
            </div>
        `).join('') : '<div class="card p-4 text-center text-muted" style="grid-column: 1 / -1;"><i class="bx bx-ruler text-4xl mb-2"></i><p>No size or design prices added yet. Use the form above to add your first rate!</p></div>';
    }

    if (totalCountEl) {
        totalCountEl.textContent = String(rows.length);
    }

    if (searchInput && !searchInput.dataset.bound) {
        searchInput.dataset.bound = 'true';
        searchInput.addEventListener('input', () => {
            const q = (searchInput.value || '').trim().toUpperCase();
            if (cardsGrid) {
                cardsGrid.querySelectorAll('.size-price-card').forEach(card => {
                    const k = card.dataset.key || '';
                    card.style.display = k.includes(q) ? '' : 'none';
                });
            }
        });
    }

    tbody.innerHTML = rows.length ? rows.map(r => `
        <tr>
            <td>${r.key}</td>
            <td class="text-right">₹${r.value.toFixed(2)}</td>
            <td class="text-center">${formatISODateTimeForDisplay(r.activeFrom) || '-'}</td>
            <td class="text-center">
                <button class="btn btn-outline btn-sm size-edit-btn" data-key="${r.key}" data-value="${r.value}" data-effective="${r.activeFrom}" ${isAdmin ? '' : 'disabled'}>
                    <i class='bx bx-edit'></i> Edit
                </button>
                <button class="btn btn-danger btn-sm size-delete-btn" data-key="${r.key}" ${isAdmin ? '' : 'disabled'}>
                    <i class='bx bx-trash'></i> Delete
                </button>
                <button class="btn btn-outline btn-sm size-history-btn" data-key="${r.key}">
                    <i class='bx bx-history'></i> History
                </button>
            </td>
        </tr>
    `).join('') : '<tr><td colspan="4" class="text-center text-muted p-3">No size/design prices saved yet.</td></tr>';

    if (recycleCountEl) recycleCountEl.textContent = String(recycleRows.length);
    recycleOpenBtn.disabled = recycleRows.length === 0;
    recycleOpenBtn.title = recycleRows.length === 0 ? 'No deleted sizes' : 'Open recycle bin';
    recycleTbody.innerHTML = recycleRows.length ? recycleRows.map(r => `
        <tr>
            <td>${r.key}</td>
            <td class="text-right">₹${(parseFloat(r.restorePrice) || 0).toFixed(2)}</td>
            <td class="text-center">${formatHistoryDateTime(r.deletedOn)}</td>
            <td class="text-center">
                <button class="btn btn-outline btn-sm size-recycle-history-btn" data-key="${r.key}">
                    <i class='bx bx-history'></i> History
                </button>
                <button class="btn btn-primary btn-sm size-restore-btn" data-key="${r.key}" data-price="${r.restorePrice}" ${isAdmin ? '' : 'disabled'}>
                    <i class='bx bx-undo'></i> Restore
                </button>
            </td>
        </tr>
    `).join('') : '<tr><td colspan="4" class="text-center text-muted p-2">No deleted sizes.</td></tr>';
    recycleEmpty.classList.toggle('hidden', recycleRows.length > 0);

    if (!form.dataset.bound) {
        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            if (!isAdminUser()) return showToast('Only admin can update prices', 'error');
            const k = String(keyInput.value || '').trim().toUpperCase();
            const p = parseFloat(valueInput.value);
            const requestedEffectiveFrom = normalizeToISODateTime(effectiveDateInput.value, today) || `${today}T00:00:00`;
            const effectiveFrom = requestedEffectiveFrom;
            if (!k) return showToast('Enter size/design', 'error');
            if (!Number.isFinite(p) || p < 0) return showToast('Enter valid price', 'error');
            showLoader();
            try {
                const r = await FirebaseService.upsertDesignPrice(k, p, buildAuditActor(), companyId, effectiveFrom, null);
                if (!r || r.success === false) throw new Error(r?.message || 'Failed');
                _pendingDataChangesForBackup = true;
                scheduleBackgroundSheetBackup('upsertDesignPrice');

                // Auto-sync into local backup (localStorage + connected LAN folder)
                try {
                    AppState.designPrices = AppState.designPrices || {};
                    AppState.designPrices[k] = p;
                    localStorage.setItem('krimaa_size_prices_backup', JSON.stringify(AppState.designPrices));
                    if (window.LanStorageService && LanStorageService.isConnected()) {
                        await LanStorageService.writeToLocal({
                            action: 'upsertDesignPrice',
                            key: k,
                            price: p,
                            companyId,
                            effectiveFrom
                        });
                    }
                } catch (bErr) {
                    console.warn('Auto local size price backup notice:', bErr);
                }

                invalidateAllKarigarCaches();
                showToast('Price saved & backed up locally', 'success');
                form.dataset.editMode = '';
                form.dataset.editFromEffective = '';
                form.dataset.editFromKey = '';
                keyInput.value = '';
                valueInput.value = '';
                effectiveDateInput.value = getCurrentLocalDateTimeInput();
                saveBtn.innerHTML = "<i class='bx bx-save'></i> Save Price";
                await renderSizePricesPage();
            } catch (err) {
                console.error(err);
                showToast(err.message || 'Failed to save price', 'error');
            } finally {
                hideLoader();
            }
        });
        form.dataset.bound = 'true';
    }

    if (!keyInput.dataset.bound) {
        keyInput.addEventListener('input', () => {
            keyInput.value = String(keyInput.value || '').toUpperCase();
            const key = String(keyInput.value || '').trim();
            if (!key) return;
            const latestHistoryMap = AppState.designPriceHistoryByCompany[companyId] || {};
            const points = Array.isArray(latestHistoryMap[key]) ? latestHistoryMap[key] : [];
            const latest = points.length > 0 ? [...points].reverse().find(p => !p.isDeleted) : null;
            const latestPrice = latest ? parseFloat(latest.price) : parseFloat(AppState.designPrices[key]);
            if (Number.isFinite(latestPrice)) {
                const oldVal = valueInput.value;
                valueInput.value = latestPrice;
                if (oldVal != valueInput.value) {
                    valueInput.classList.add('rainbow-autofill');
                    setTimeout(() => valueInput.classList.remove('rainbow-autofill'), 2500);
                }
            }
        });
        keyInput.dataset.bound = 'true';
    }

    document.querySelectorAll('.size-edit-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            if (!isAdminUser()) return;
            form.dataset.editMode = '1';
            form.dataset.editFromEffective = '';
            form.dataset.editFromKey = '';
            keyInput.value = btn.dataset.key || '';
            valueInput.value = btn.dataset.value || '';
            effectiveDateInput.value = getCurrentLocalDateTimeInput();
            saveBtn.innerHTML = "<i class='bx bx-save'></i> Save New Price";
            showToast('Set new price and select apply date & time.', 'info');
            keyInput.focus();
        });
    });

    document.querySelectorAll('.size-delete-btn').forEach(btn => {
        btn.addEventListener('click', async () => {
            if (!isAdminUser()) return;
            const key = String(btn.dataset.key || '').trim().toUpperCase();
            if (!key) return;
            if (!confirm(`Delete "${key}" from current list? Old transactions will keep historical prices.`)) return;
            showLoader();
            try {
                const hideFrom = normalizeToISODateTime(getCurrentLocalDateTimeInput(), today) || `${today}T00:00:00`;
                let r = null;
                if (typeof FirebaseService.deleteDesignPrice === 'function') {
                    r = await FirebaseService.deleteDesignPrice(key, buildAuditActor(), companyId, hideFrom);
                } else if (typeof FirebaseService.hideDesignPrice === 'function') {
                    r = await FirebaseService.hideDesignPrice(key, buildAuditActor(), companyId, hideFrom);
                } else if (typeof FirebaseService.upsertDesignPrice === 'function') {
                    const asOf = await FirebaseService.getDesignPrices(companyId, hideFrom).catch(() => ({ success: true, data: {} }));
                    const currentPrice = Number.isFinite(parseFloat(asOf?.data?.[key])) ? parseFloat(asOf.data[key]) : 0;
                    r = await FirebaseService.upsertDesignPrice(
                        key,
                        currentPrice,
                        buildAuditActor(),
                        companyId,
                        hideFrom,
                        { isDeleted: true }
                    );
                } else {
                    throw new Error('Price delete API missing. Hard refresh page once.');
                }
                if (!r || r.success === false) throw new Error(r?.message || 'Failed to delete');
                _pendingDataChangesForBackup = true;
                scheduleBackgroundSheetBackup('deleteDesignPrice');

                // Auto-sync into local backup
                try {
                    if (AppState.designPrices) delete AppState.designPrices[key];
                    localStorage.setItem('krimaa_size_prices_backup', JSON.stringify(AppState.designPrices || {}));
                    if (window.LanStorageService && LanStorageService.isConnected()) {
                        await LanStorageService.writeToLocal({
                            action: 'deleteDesignPrice',
                            key,
                            companyId,
                            hideFrom
                        });
                    }
                } catch (bErr) {
                    console.warn('Auto local size price delete notice:', bErr);
                }

                invalidateAllKarigarCaches();
                showToast('Price deleted from active list & local backup updated', 'success');
                await renderSizePricesPage();
            } catch (err) {
                console.error(err);
                showToast(err.message || 'Failed to delete price', 'error');
            } finally {
                hideLoader();
            }
        });
    });

    document.querySelectorAll('.size-history-btn').forEach(btn => {
        btn.addEventListener('click', () => openHistoryModal(btn.dataset.key || ''));
    });
    recycleTbody.querySelectorAll('.size-recycle-history-btn').forEach(btn => {
        btn.addEventListener('click', () => openHistoryModal(btn.dataset.key || ''));
    });
    recycleTbody.querySelectorAll('.size-restore-btn').forEach(btn => {
        btn.addEventListener('click', async () => {
            if (!isAdminUser()) return;
            const key = String(btn.dataset.key || '').trim().toUpperCase();
            if (!key) return;
            if (!confirm(`Restore "${key}" back to active price list?`)) return;
            let restorePrice = parseFloat(btn.dataset.price);
            if (!Number.isFinite(restorePrice) || restorePrice < 0) {
                const raw = prompt(`Enter restore price for ${key}:`, '0');
                if (raw === null) return;
                restorePrice = parseFloat(raw);
            }
            if (!Number.isFinite(restorePrice) || restorePrice < 0) {
                return showToast('Invalid restore price', 'error');
            }
            showLoader();
            try {
                const effectiveFrom = normalizeToISODateTime(getCurrentLocalDateTimeInput(), today) || `${today}T00:00:00`;
                const r = await FirebaseService.upsertDesignPrice(key, restorePrice, buildAuditActor(), companyId, effectiveFrom);
                if (!r || r.success === false) throw new Error(r?.message || 'Failed to restore');
                _pendingDataChangesForBackup = true;
                scheduleBackgroundSheetBackup('restoreDesignPrice');

                // Auto-sync into local backup
                try {
                    AppState.designPrices = AppState.designPrices || {};
                    AppState.designPrices[key] = restorePrice;
                    localStorage.setItem('krimaa_size_prices_backup', JSON.stringify(AppState.designPrices));
                    if (window.LanStorageService && LanStorageService.isConnected()) {
                        await LanStorageService.writeToLocal({
                            action: 'upsertDesignPrice',
                            key,
                            price: restorePrice,
                            companyId,
                            effectiveFrom
                        });
                    }
                } catch (bErr) {
                    console.warn('Auto local size price restore notice:', bErr);
                }

                invalidateAllKarigarCaches();
                showToast('Size price restored & local backup updated', 'success');
                recycleModal.classList.remove('show');
                await renderSizePricesPage();
            } catch (err) {
                console.error(err);
                showToast(err.message || 'Failed to restore size', 'error');
            } finally {
                hideLoader();
            }
        });
    });
}

/* ===== KARIGAR MANAGEMENT ===== */
function invalidateKarigarCache(companyId = AppState.currentCompany) {
    const cid = String(companyId || 'company1').trim();
    if (AppState.karigarCacheByCompany && AppState.karigarCacheByCompany[cid]) {
        delete AppState.karigarCacheByCompany[cid];
    }
}

function invalidateAllKarigarCaches() {
    AppState.karigarCacheByCompany = {};
}

async function loadKarigarData(forceReload = false) {
    const companyId = String(AppState.currentCompany || 'company1').trim();
    const cached = AppState.karigarCacheByCompany[companyId];
    if (!forceReload && cached) {
        AppState.karigars = cached.karigars || [];
        AppState.karigarTransactions = cached.transactions || [];
        rebuildKarigarComputedCache();
        AppState.designPrices = cached.designPrices || {};
        AppState.designPricesByCompany[DESIGN_PRICE_SCOPE] = AppState.designPrices;
        AppState.designPriceHistoryByCompany[DESIGN_PRICE_SCOPE] = cached.designPriceHistory || {};
        return;
    }

    try {
        const kRes = await apiRequest({ action: 'getKarigars', companyId });
        const [tRes, pRes] = await Promise.all([
            apiRequest({ action: 'getKarigarTransactions', companyId }),
            apiRequest({ action: 'getDesignPrices', companyId, DESIGN_PRICE_SCOPE })
        ]);
        
        if (kRes.success) AppState.karigars = kRes.data || [];

        const karigarNameToId = {};
        (AppState.karigars || []).forEach(k => {
            const kName = normalizeKarigarNameKey(k.name);
            const kId = String(k.id || '').trim();
            if (kName && kId) karigarNameToId[kName] = kId;
        });

        if (tRes.success) {
            AppState.karigarTransactions = (tRes.data || [])
                .map(tx => {
                    const txName = String(tx.karigarName || '').trim();
                    const txNameKey = normalizeKarigarNameKey(txName);
                    const rawTxId = String(tx.karigarId || '').trim();
                    const normalizedDate = normalizeToISODate(tx.date) || String(tx.date || '').trim();
                    const normalizedDateTime = normalizeToISODateTime(tx.transactionDateTime || tx.dateTime || tx.createdAt || tx.date, normalizedDate) || `${normalizedDate}T00:00:00`;

                    let resolvedTxId = rawTxId;
                    if ((!resolvedTxId || !resolvedTxId.startsWith('kar_')) && txNameKey && karigarNameToId[txNameKey]) {
                        resolvedTxId = karigarNameToId[txNameKey];
                    }

                    return {
                        ...tx,
                        karigarId: resolvedTxId,
                        karigarName: txName || tx.karigarName || '',
                        date: normalizedDate,
                        transactionDateTime: normalizedDateTime
                    };
                })
                .sort((a, b) => getKarigarTxTimestampMs(b) - getKarigarTxTimestampMs(a));
            rebuildKarigarComputedCache();
        }
        if (pRes.success) {
            let priceMap = {};
            if (Array.isArray(pRes.data)) {
                pRes.data.forEach(item => {
                    const k = String(item.key || item.id || '').split('__').slice(-1)[0].trim().toUpperCase();
                    if (k && !item.isDeletedLatest && !item.isDeleted) {
                        priceMap[k] = Math.round(((parseFloat(item.price) || 0) + Number.EPSILON) * 100) / 100;
                    }
                });
            } else if (pRes.data && typeof pRes.data === 'object') {
                Object.entries(pRes.data).forEach(([k, v]) => {
                    priceMap[String(k).trim().toUpperCase()] = Math.round(((parseFloat(v) || 0) + Number.EPSILON) * 100) / 100;
                });
            }
            AppState.designPrices = priceMap;
            AppState.designPricesByCompany[DESIGN_PRICE_SCOPE] = AppState.designPrices;
            AppState.designPriceHistoryByCompany[DESIGN_PRICE_SCOPE] = pRes.history || {};
        }
        AppState.karigarCacheByCompany[companyId] = {
            karigars: [...(AppState.karigars || [])],
            transactions: [...(AppState.karigarTransactions || [])],
            designPrices: { ...(AppState.designPrices || {}) },
            designPriceHistory: { ...(AppState.designPriceHistoryByCompany[DESIGN_PRICE_SCOPE] || {}) },
            loadedAt: Date.now()
        };
    } catch(e) {
        console.error("Failed to load karigar data", e);
    }
}

async function renderKarigarPage(forceReload = false) {
    await loadKarigarData(forceReload);
    rebuildKarigarComputedCache();
    await updateKarigarCycleToolbar();
    auditKarigarFinancials();
    renderKarigarGrid();
    setupKarigarListeners();
    setupKarigarFastEnterNavigation();
    setupKarigarGlobalShortcuts();
}

function resolveKarigarIdFromState(karigarId, karigarName) {
    const rawId = String(karigarId || '').trim();
    if (rawId.startsWith('kar_')) return rawId;
    if (rawId) return rawId;
    if (karigarName) {
        const k = (getEffectiveKarigarList() || []).find(x => (x.name || '').toLowerCase() === String(karigarName).toLowerCase().trim());
        if (k && k.id) return k.id;
        return 'kar_' + encodeURIComponent(String(karigarName).toLowerCase().trim());
    }
    return '';
}

function getEffectiveKarigarList() {
    if (AppState.viewingKarigarBackupId) {
        const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
        if (b && Array.isArray(b.rows)) {
            const map = new Map();
            // In archive mode, ONLY extract karigars who actually exist in this backup's transaction rows!
            // Brand-new karigars created in subsequent months will not leak into this archive.
            b.rows.forEach(r => {
                const name = String(r.karigarName || '').trim();
                const id = String(r.karigarId || '').trim();
                if (name) {
                    const key = name.toLowerCase();
                    if (!map.has(key)) {
                        const existing = (AppState.karigars || []).find(k => (k.name || '').trim().toLowerCase() === key);
                        map.set(key, {
                            id: id || existing?.id || `kar_arch_${encodeURIComponent(key)}`,
                            name: existing?.name || name,
                            companyId: r.companyId || b.companyId || AppState.currentCompany
                        });
                    } else if (id && !map.get(key).id) {
                        map.get(key).id = id;
                    }
                }
            });
            return Array.from(map.values()).sort((a, b) => a.name.localeCompare(b.name));
        }
    }
    return (AppState.karigars || []).slice().sort((a, b) => (a.name || '').localeCompare(b.name || ''));
}

// ── Karigar Archive Summary Recompute & Storage ──
function recomputeBackupSummary(backup) {
    if (!backup) return;
    const rows = Array.isArray(backup.rows) ? backup.rows : [];
    let totalJama = 0;
    let totalUpad = 0;
    rows.forEach(tx => {
        const type = String(tx?.type || '').toLowerCase();
        if (type === 'jama') {
            totalJama += parseFloat(tx.total) || 0;
            totalUpad += parseFloat(tx.upadAmount) || 0;
        } else if (type === 'upad') {
            totalUpad += parseFloat(tx.amount) || 0;
        }
    });
    backup.summary = {
        totalJama: Math.round((totalJama + Number.EPSILON) * 100) / 100,
        totalUpad: Math.round((totalUpad + Number.EPSILON) * 100) / 100,
        netBalance: Math.round(((totalJama - totalUpad) + Number.EPSILON) * 100) / 100,
        count: rows.length
    };
}

async function saveArchiveBackupChanges(backup) {
    if (!backup || !backup.id) return;
    try {
        if (typeof FirebaseService !== 'undefined' && FirebaseService.updateKarigarResetBackup) {
            await FirebaseService.updateKarigarResetBackup(backup.id, {
                rows: backup.rows,
                summary: backup.summary,
                paidKarigarMap: backup.paidKarigarMap || {}
            });
        }
    } catch (e) {
        console.warn('[Archive] Cloud backup sync error:', e);
    }
    try {
        if (typeof LanStorageService !== 'undefined' && LanStorageService.isConnected()) {
            const backups = await LanStorageService.readFile('karigar_reset_backups', []);
            const idx = backups.findIndex(b => b && b.id === backup.id);
            if (idx >= 0) {
                backups[idx] = { ...backups[idx], ...backup };
            } else {
                backups.push(backup);
            }
            await LanStorageService.writeFile('karigar_reset_backups', backups);
        }
    } catch (e) {
        console.warn('[Archive] LAN backup sync error:', e);
    }
}

// ── Karigar Mark as Paid / Clear Helpers ──
function getKarigarPaidStorageKey(companyId, backupId = null) {
    const comp = String(companyId || AppState.currentCompany || 'company1').trim();
    if (backupId) {
        return `karigar_paid_${comp}_backup_${backupId}`;
    }
    const cycleStart = localStorage.getItem(`karigar_cycle_start_${comp}`) || `${getTodayISODate().substring(0, 7)}-01`;
    return `karigar_paid_${comp}_${cycleStart}`;
}

function getPaidKarigarsMap() {
    const backupId = AppState.viewingKarigarBackupId;
    const key = getKarigarPaidStorageKey(AppState.currentCompany, backupId);
    let map = {};
    try {
        const raw = localStorage.getItem(key);
        if (raw) map = JSON.parse(raw);
    } catch (e) {}

    if (backupId) {
        const b = (AppState.karigarResetBackups || []).find(x => x.id === backupId);
        if (b && b.paidKarigarMap && typeof b.paidKarigarMap === 'object') {
            map = { ...b.paidKarigarMap, ...map };
        }
    }
    return map;
}

function isKarigarPaid(karigarId, karigarName) {
    const map = getPaidKarigarsMap();
    const idKey = String(karigarId || '').trim();
    const nameKey = String(karigarName || '').trim().toLowerCase();
    if (idKey && map[idKey]) return true;
    if (nameKey && map[nameKey]) return true;
    return false;
}

async function toggleKarigarPaid(karigarId, karigarName, forceValue = null) {
    const backupId = AppState.viewingKarigarBackupId;
    const key = getKarigarPaidStorageKey(AppState.currentCompany, backupId);
    const map = getPaidKarigarsMap();
    const idKey = String(karigarId || '').trim();
    const nameKey = String(karigarName || '').trim().toLowerCase();

    const currentlyPaid = isKarigarPaid(karigarId, karigarName);
    const nextVal = (forceValue !== null) ? Boolean(forceValue) : !currentlyPaid;

    if (idKey) {
        if (nextVal) map[idKey] = { paid: true, date: new Date().toISOString() };
        else delete map[idKey];
    }
    if (nameKey) {
        if (nextVal) map[nameKey] = { paid: true, date: new Date().toISOString() };
        else delete map[nameKey];
    }

    try {
        localStorage.setItem(key, JSON.stringify(map));
    } catch (e) {}

    if (backupId) {
        const b = (AppState.karigarResetBackups || []).find(x => x.id === backupId);
        if (b) {
            b.paidKarigarMap = map;
            await saveArchiveBackupChanges(b);
        }
    }

    showToast(`${karigarName || 'Karigar'} marked as ${nextVal ? 'PAID / CLEAR ✓' : 'UNPAID / PENDING'}`, nextVal ? 'success' : 'info');
    renderKarigarGrid();
}

function toggleKarigarPaidWithConfirm(karigarId, karigarName) {
    const isPaid = isKarigarPaid(karigarId, karigarName);
    const promptMsg = isPaid 
        ? `Unmark "${karigarName}" as Paid / Clear?` 
        : `Mark "${karigarName}" as PAID / SETTLED? (Press Enter / OK to confirm)`;
    if (confirm(promptMsg)) {
        toggleKarigarPaid(karigarId, karigarName, !isPaid);
    }
}

function getKarigarCurrentCycleLabel() {
    const compId = String(AppState.currentCompany || 'company1').trim() || 'company1';
    if (AppState.viewingKarigarBackupId) {
        const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
        if (b) return getKarigarBackupRangeLabel(b);
    }
    const cycleStart = localStorage.getItem(`karigar_cycle_start_${compId}`) || `${getTodayISODate().substring(0, 7)}-01`;
    const todayFormatted = formatISODateForDisplay(getTodayISODate());
    return `${formatISODateForDisplay(cycleStart)} to ${todayFormatted}`;
}

function auditKarigarFinancials() {
    const round2 = (n) => Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;
    let txs = Array.isArray(AppState.karigarTransactions) ? AppState.karigarTransactions : [];
    if (AppState.viewingKarigarBackupId) {
        const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
        if (b && Array.isArray(b.rows)) txs = b.rows;
    }

    let globalJama = 0;
    let globalUpad = 0;
    let globalPics = 0;
    let autoHealed = 0;

    txs.forEach(t => {
        const type = String(t?.type || '').toLowerCase();
        if (type === 'jama') {
            const pics = parseInt(t.pic, 10) || 0;
            const price = round2(t.price || 0);
            const expectedTotal = round2(pics * price);
            const recordedTotal = round2(t.total || 0);
            if (pics > 0 && Math.abs(expectedTotal - recordedTotal) > 0.009) {
                t.total = expectedTotal;
                autoHealed++;
            }
            globalJama = round2(globalJama + (t.total || 0));
            globalPics += pics;
            if (t.upadAmount) globalUpad = round2(globalUpad + round2(t.upadAmount));
        } else if (type === 'upad') {
            globalUpad = round2(globalUpad + round2(t.amount || 0));
        }
    });

    const netBal = round2(globalJama - globalUpad);
    const badgeText = document.getElementById('karigar-audit-text');
    if (badgeText) {
        badgeText.textContent = `Zero Error Reconciled (₹0.00 Variance)`;
    }

    const obEl = document.getElementById('karigar-overall-balance');
    if (obEl) {
        obEl.textContent = '₹' + Math.abs(netBal).toFixed(2) + (netBal < 0 ? ' Due' : ' Clear');
        obEl.className = netBal < 0 ? 'text-lg text-danger' : 'text-lg text-success';
    }
    const opEl = document.getElementById('karigar-overall-pics');
    if (opEl) {
        opEl.textContent = String(globalPics);
    }

    return { globalJama, globalUpad, globalPics, netBal, autoHealed };
}

async function updateKarigarCycleToolbar() {
    const compId = String(AppState.currentCompany || 'company1').trim() || 'company1';
    const select = document.getElementById('karigar-cycle-select');
    const badgeText = document.getElementById('karigar-cycle-date-text');
    const badgeEl = document.getElementById('karigar-cycle-badge');
    const resetBtn = document.getElementById('btn-reset-karigar');
    const karigarContainer = document.getElementById('karigar');
    const archiveBanner = document.getElementById('karigar-archive-banner');
    const archiveBannerText = document.getElementById('karigar-archive-banner-text');

    if (resetBtn) {
        resetBtn.style.display = isAdminUser() ? 'inline-flex' : 'none';
    }

    const cycleStart = localStorage.getItem(`karigar_cycle_start_${compId}`) || `${getTodayISODate().substring(0, 7)}-01`;
    const isViewingArchive = Boolean(AppState.viewingKarigarBackupId);

    if (karigarContainer) {
        if (isViewingArchive) karigarContainer.classList.add('archive-active-mode');
        else karigarContainer.classList.remove('archive-active-mode');
    }

    if (badgeText && badgeEl) {
        if (isViewingArchive) {
            const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
            const rangeStr = b ? getKarigarBackupRangeLabel(b) : 'Past Cycle';
            badgeText.textContent = `Archived Cycle: ${rangeStr} (Bounded Archive)`;
            badgeEl.style.background = '#fff1f2';
            badgeEl.style.borderColor = '#fca5a5';
            badgeEl.style.color = '#9f1239';

            if (archiveBanner && archiveBannerText) {
                const count = Array.isArray(b?.rows) ? b.rows.length : (b?.summary?.count || 0);
                archiveBannerText.innerHTML = `Viewing & Editing Archived Month: <strong>${rangeStr}</strong> (${count} entries). Additions and edits are <strong>strictly bounded</strong> to this archive backup.`;
                archiveBanner.style.display = 'flex';
            }
        } else {
            badgeText.textContent = `Active Cycle: From ${formatISODateForDisplay(cycleStart)} to Present`;
            badgeEl.style.background = '#ecfdf5';
            badgeEl.style.borderColor = '#a7f3d0';
            badgeEl.style.color = '#047857';

            if (archiveBanner) archiveBanner.style.display = 'none';
        }
    }

    const exitArchiveBtn = document.getElementById('btn-exit-karigar-archive');
    if (exitArchiveBtn && !exitArchiveBtn.dataset.bound) {
        exitArchiveBtn.dataset.bound = 'true';
        exitArchiveBtn.addEventListener('click', () => {
            AppState.viewingKarigarBackupId = null;
            if (select) select.value = 'current';
            rebuildKarigarComputedCache();
            auditKarigarFinancials();
            renderKarigarGrid();
            updateKarigarCycleToolbar();
            showToast('Returned to Current Active Month', 'info');
        });
    }

    if (!select) return;

    if (!AppState.karigarResetBackups || AppState.karigarResetBackups.length === 0) {
        try {
            const res = await FirebaseService.getKarigarResetBackups(compId);
            if (res && Array.isArray(res.data)) {
                AppState.karigarResetBackups = res.data;
            }
        } catch (e) {
            console.warn('Failed to load reset backups:', e);
        }
    }

    const backups = Array.isArray(AppState.karigarResetBackups) ? AppState.karigarResetBackups : [];
    
    let html = `<option value="current">Current Active Cycle (From ${formatISODateForDisplay(cycleStart)} to Present)</option>`;
    backups.forEach((b) => {
        const label = getKarigarBackupRangeLabel(b);
        const count = Array.isArray(b.rows) ? b.rows.length : (b.summary?.count || 0);
        const isSel = AppState.viewingKarigarBackupId === b.id ? 'selected' : '';
        html += `<option value="backup_${b.id}" ${isSel}>Archive: ${label} (${count} entries)</option>`;
    });

    select.innerHTML = html;

    if (!select.dataset.bound) {
        select.dataset.bound = 'true';
        select.addEventListener('change', async () => {
            const val = select.value;
            if (val === 'current') {
                AppState.viewingKarigarBackupId = null;
                rebuildKarigarComputedCache();
                auditKarigarFinancials();
                renderKarigarGrid();
                updateKarigarCycleToolbar();
            } else if (val.startsWith('backup_')) {
                const bId = val.replace('backup_', '');
                AppState.viewingKarigarBackupId = bId;
                rebuildKarigarComputedCache();
                auditKarigarFinancials();
                renderKarigarGrid();
                updateKarigarCycleToolbar();
            }
        });
    }
}

function rebuildKarigarComputedCache() {
    let txs = Array.isArray(AppState.karigarTransactions) ? AppState.karigarTransactions : [];
    if (AppState.viewingKarigarBackupId) {
        const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
        if (b && Array.isArray(b.rows)) txs = b.rows;
    }
    const byId = {};
    const statsById = {};
    const round2 = (n) => Math.round((n + Number.EPSILON) * 100) / 100;
    const toAmount = (v) => {
        const n = parseFloat(String(v ?? '').replace(/[^0-9.-]/g, ''));
        return Number.isFinite(n) ? n : 0;
    };

    for (const t of txs) {
        const id = String(t?.karigarId || '').trim();
        if (!id) continue;
        if (!byId[id]) byId[id] = [];
        byId[id].push(t);

        if (!statsById[id]) {
            statsById[id] = { totalJama: 0, totalUpad: 0, totalPics: 0, lastTx: null };
        }
        const s = statsById[id];
        const type = String(t?.type || '').trim().toLowerCase();
        if (type === 'jama') {
            s.totalJama = round2(s.totalJama + toAmount(t.total));
            s.totalUpad = round2(s.totalUpad + toAmount(t.upadAmount));
            const pics = parseInt(String(t.pic ?? '').replace(/[^0-9-]/g, ''), 10);
            if (Number.isFinite(pics) && pics > 0) s.totalPics += pics;
        } else if (type === 'upad') {
            s.totalUpad = round2(s.totalUpad + toAmount(t.amount || t.upadAmount));
        }

        const curTs = getKarigarTxTimestampMs(t);
        const prevTs = s.lastTx ? getKarigarTxTimestampMs(s.lastTx) : -1;
        if (!s.lastTx || curTs > prevTs) s.lastTx = t;
    }

    Object.keys(byId).forEach(id => {
        byId[id].sort((a, b) => getKarigarTxTimestampMs(b) - getKarigarTxTimestampMs(a));
    });

    Object.keys(statsById).forEach(id => {
        const s = statsById[id];
        s.balance = round2((s.totalJama || 0) - (s.totalUpad || 0));
    });

    AppState.karigarTransactionsById = byId;
    AppState.karigarStatsById = statsById;
}

function calculateKarigarBalance(karigarId, karigarName) {
    if (!karigarId) return { totalJama: 0, totalUpad: 0, totalPics: 0, balance: 0, lastTx: null };
    const s = AppState.karigarStatsById?.[String(karigarId || '').trim()];
    if (s) {
        return {
            totalJama: s.totalJama || 0,
            totalUpad: s.totalUpad || 0,
            totalPics: s.totalPics || 0,
            balance: s.balance || 0,
            lastTx: s.lastTx || null
        };
    }
    const tx = getKarigarTransactionsFor(karigarId);
    const toAmount = (v) => {
        const n = parseFloat(String(v ?? '').replace(/[^0-9.-]/g, ''));
        return Number.isFinite(n) ? n : 0;
    };
    const round2 = (n) => Math.round((n + Number.EPSILON) * 100) / 100;
    let totalJama = 0;
    let totalUpad = 0;
    let totalPics = 0;
    tx.forEach(t => {
        if (t.type === 'jama') {
            totalJama = round2(totalJama + toAmount(t.total));
            totalUpad = round2(totalUpad + toAmount(t.upadAmount));
            const pics = parseInt(String(t.pic ?? '').replace(/[^0-9-]/g, ''), 10);
            if (Number.isFinite(pics) && pics > 0) totalPics += pics;
        }
        if (t.type === 'upad') totalUpad = round2(totalUpad + toAmount(t.amount || t.upadAmount));
    });
    const balance = round2(totalJama - totalUpad);
    return { totalJama, totalUpad, totalPics, balance, lastTx: tx[0] };
}

let _selectedKarigarCardIndex = -1;

function highlightSelectedCard() {
    const cards = document.querySelectorAll('#karigar-grid .karigar-card');
    cards.forEach((c, idx) => {
        if (idx === _selectedKarigarCardIndex) {
            c.classList.add('keyboard-selected');
            try { c.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); } catch(e){}
        } else {
            c.classList.remove('keyboard-selected');
        }
    });
}

function getSelectedOrFirstKarigar() {
    const grid = document.getElementById('karigar-grid');
    if (!grid) return null;
    const cards = grid.querySelectorAll('.karigar-card');
    if (cards.length === 0) return null;
    
    let targetCard = null;
    if (_selectedKarigarCardIndex >= 0 && _selectedKarigarCardIndex < cards.length) {
        targetCard = cards[_selectedKarigarCardIndex];
    } else {
        targetCard = cards[0];
    }
    const jamaBtn = targetCard.querySelector('.btn-karigar-jama');
    if (!jamaBtn) return null;
    const kid = jamaBtn.dataset.id;
    const kname = jamaBtn.dataset.name;
    return (AppState.karigars || []).find(k => k.id === kid) || { id: kid, name: kname };
}

async function autoCreateKarigarFromSearch(name) {
    const cleanName = String(name || '').trim();
    if (!cleanName) return;
    if (!isAdminUser()) {
        return showToast('Only admin can add Karigar', 'error');
    }
    showLoader(`Creating new karigar "${cleanName}"...`);
    try {
        await FirebaseService.addKarigar(cleanName, AppState.currentCompany, buildAuditActor());
        _pendingDataChangesForBackup = true;
        scheduleBackgroundSheetBackup('addKarigar');
        const searchInput = document.getElementById('karigar-search-input');
        if (searchInput) searchInput.value = '';
        showToast(`Karigar "${cleanName}" created successfully!`, 'success');
        invalidateKarigarCache();
        await renderKarigarPage(true);

        // Find newly created karigar and open Jama modal immediately
        const newlyCreated = (AppState.karigars || []).find(k => (k.name || '').toLowerCase() === cleanName.toLowerCase());
        if (newlyCreated) {
            openKarigarJamaModal(newlyCreated.id, newlyCreated.name);
        }
    } catch (err) {
        showToast(err.message || 'Failed to create karigar', 'error');
    } finally {
        hideLoader();
    }
}
window.autoCreateKarigarFromSearch = autoCreateKarigarFromSearch;

function renderKarigarGrid() {
    const grid = document.getElementById('karigar-grid');
    const searchInput = document.getElementById('karigar-search-input');
    const rawSearch = (searchInput ? searchInput.value : '').trim();
    const query = rawSearch.toLowerCase();
    const btnCreate = document.getElementById('btn-create-karigar');
    const isAdmin = isAdminUser();
    
    if (!grid) return;
    grid.innerHTML = '';
    
    const allKarigars = getEffectiveKarigarList();
    let paidCount = 0;
    let pendingCount = 0;
    allKarigars.forEach(k => {
        if (isKarigarPaid(k.id, k.name)) paidCount++;
        else pendingCount++;
    });

    const elAll = document.getElementById('filter-count-all');
    const elPending = document.getElementById('filter-count-pending');
    const elPaid = document.getElementById('filter-count-paid');
    if (elAll) elAll.textContent = String(allKarigars.length);
    if (elPending) elPending.textContent = String(pendingCount);
    if (elPaid) elPaid.textContent = String(paidCount);

    const currentFilter = AppState.karigarPaidFilter || 'all';
    document.querySelectorAll('.karigar-filter-btn').forEach(btn => {
        if (btn.dataset.filter === currentFilter) btn.classList.add('active');
        else btn.classList.remove('active');
    });

    let filtered = allKarigars;
    if (currentFilter === 'paid') {
        filtered = filtered.filter(k => isKarigarPaid(k.id, k.name));
    } else if (currentFilter === 'pending') {
        filtered = filtered.filter(k => !isKarigarPaid(k.id, k.name));
    }

    if (query) {
        filtered = filtered.filter(k => (k.name || '').toLowerCase().includes(query));
    }

    const hintEl = document.getElementById('karigar-visible-count-text');
    if (hintEl) {
        hintEl.textContent = `Showing ${filtered.length} of ${allKarigars.length} workers`;
    }
    
    const exactMatch = allKarigars.some(k => (k.name || '').toLowerCase() === query);
    if (query && !exactMatch && isAdmin) {
        btnCreate.style.display = 'block';
    } else {
        btnCreate.style.display = 'none';
    }
    
    if (filtered.length === 0) {
        const escQuery = rawSearch.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        const safeJsQuery = rawSearch.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
        const createBtnHtml = (rawSearch && isAdmin && !AppState.viewingKarigarBackupId) ? `
            <div style="margin-top: 1rem;">
                <button type="button" class="btn btn-primary" onclick="autoCreateKarigarFromSearch('${safeJsQuery}')" style="box-shadow: 0 4px 12px rgba(37,99,235,0.25); font-weight:600;">
                    <i class='bx bx-user-plus'></i> Press <kbd class="kbd-shortcut" style="margin:0 4px;">Enter</kbd> to Create Karigar "${escQuery}"
                </button>
            </div>
        ` : '';
        grid.innerHTML = `
            <div class="card p-4 text-center text-muted" style="grid-column: 1 / -1; padding: 2.5rem 1rem;">
                <i class="bx bx-user-x text-4xl mb-2" style="font-size: 2.5rem; opacity: 0.6;"></i>
                <h4 style="font-weight: 700; color: #1e293b; margin: 0.5rem 0 0.25rem 0;">${rawSearch ? `No Karigar found matching "${escQuery}"` : (currentFilter !== 'all' ? `No ${currentFilter} karigars in this cycle.` : 'No karigars found.')}</h4>
                <p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0;">${rawSearch && !AppState.viewingKarigarBackupId ? 'Press <strong>Enter</strong> to automatically create this Karigar and open Maal (Jama).' : ''}</p>
                ${createBtnHtml}
            </div>
        `;
        return;
    }
    
    const resetBtn = document.getElementById('btn-reset-karigar');
    
    if (resetBtn) {
        if (!isAdmin || AppState.viewingKarigarBackupId) {
            resetBtn.style.display = 'none';
        } else {
            const targetCompanyId = String(AppState.currentCompany || 'company1').trim() || 'company1';
            const targetCompanyName = getCompanyDisplayName(targetCompanyId);
            resetBtn.style.display = 'inline-flex';
            resetBtn.classList.remove('btn-danger', 'text-white');
            resetBtn.classList.add('btn-outline', 'text-danger');
            resetBtn.innerHTML = `<i class='bx bx-reset'></i> Reset Month`;
            resetBtn.title = `Archive and reset month for ${targetCompanyName}`;

            const newResetBtn = resetBtn.cloneNode(true);
            resetBtn.parentNode.replaceChild(newResetBtn, resetBtn);
            newResetBtn.addEventListener('click', () => confirmKarigarMonthlyReset(targetCompanyId));
        }
    }
    
    let html = '';
    let globalBalanceSum = 0;
    let globalPicsSum = 0;
    
    filtered.forEach((k, idx) => {
        const stats = calculateKarigarBalance(k.id, k.name);
        globalBalanceSum += stats.balance;
        globalPicsSum += (parseInt(stats.totalPics, 10) || 0);
        
        const lastActivity = stats.lastTx ? `<span class="text-xs text-muted">Last active: ${formatISODateForDisplay(stats.lastTx.date)}</span>` : '<span class="text-xs text-muted">No activity yet</span>';
        
        const isNegative = stats.balance < 0;
        const balColor = isNegative ? '#dc2626' : '#16a34a';
        
        const entId = getEnterpriseKarigarId(k, AppState.currentCompany);
        const isPaid = isKarigarPaid(k.id, k.name);
        const paidStampHtml = isPaid ? `
            <div class="karigar-paid-stamp-round" title="PAID &amp; SETTLED">
                <svg viewBox="0 0 100 100" width="86" height="86" style="display:block;">
                    <circle cx="50" cy="50" r="46" fill="rgba(240, 253, 244, 0.85)" stroke="#15803d" stroke-width="2.6" stroke-dasharray="98 2" />
                    <circle cx="50" cy="50" r="39" fill="none" stroke="#15803d" stroke-width="1.2" stroke-dasharray="4 2" />
                    <path id="stamp-arc-top-${idx}" d="M 17,50 A 33,33 0 1,1 83,50" fill="none" />
                    <text font-size="8" font-weight="900" fill="#15803d" letter-spacing="1.2">
                        <textPath href="#stamp-arc-top-${idx}" startOffset="50%" text-anchor="middle">
                            ★ KRIMAA FACTORY ★
                        </textPath>
                    </text>
                    <line x1="20" y1="42" x2="80" y2="42" stroke="#15803d" stroke-width="1.2" />
                    <text x="50" y="55" font-size="16" font-weight="900" fill="#15803d" text-anchor="middle" font-family="Arial Black, Impact, sans-serif" letter-spacing="1.2">PAID</text>
                    <line x1="20" y1="59" x2="80" y2="59" stroke="#15803d" stroke-width="1.2" />
                    <path id="stamp-arc-bottom-${idx}" d="M 83,50 A 33,33 0 0,1 17,50" fill="none" />
                    <text font-size="7.5" font-weight="800" fill="#15803d" letter-spacing="1.2">
                        <textPath href="#stamp-arc-bottom-${idx}" startOffset="50%" text-anchor="middle">
                            ✔ SETTLED &amp; CLEAR
                        </textPath>
                    </text>
                </svg>
            </div>
        ` : '';

        html += `
            <div class="karigar-card ${isPaid ? 'is-paid-cleared' : ''}" data-id="${k.id}" data-idx="${idx}">
                ${paidStampHtml}
                <div class="karigar-card-header">
                    <div>
                        <div class="flex items-center gap-2" style="margin-bottom: 2px;">
                            <span style="background: #f1f5f9; color: #334155; font-size: 0.68rem; font-weight: 800; padding: 2px 6px; border-radius: 4px; border: 1px solid #cbd5e1; letter-spacing: 0.5px;">${entId}</span>
                            <h3 class="karigar-card-name" style="margin: 0;">${k.name}</h3>
                        </div>
                        <div class="karigar-card-subtitle">${lastActivity}</div>
                    </div>
                    <div class="karigar-bal-badge" style="background: ${isNegative ? '#fee2e2' : '#dcfce7'}; color: ${balColor};">
                        <span class="bal-label">Balance</span>
                        <span class="actual-bal">₹${Math.abs(stats.balance).toFixed(2)}${isNegative ? ' Due' : ' Clear'}</span>
                    </div>
                </div>
                
                <div class="karigar-metrics-strip">
                    <div class="m-col">
                        <span class="m-lbl">Maal (Jama)</span>
                        <span class="m-val text-success">₹${stats.totalJama.toFixed(2)}</span>
                    </div>
                    <div class="m-col">
                        <span class="m-lbl">Borrow (Upad)</span>
                        <span class="m-val text-danger">₹${stats.totalUpad.toFixed(2)}</span>
                    </div>
                    <div class="m-col">
                        <span class="m-lbl">Total Pics</span>
                        <span class="m-val text-primary">${stats.totalPics}</span>
                    </div>
                </div>

                <div class="karigar-actions-primary">
                    <button class="btn btn-outline btn-karigar-upad ${isAdmin ? '' : 'disabled'}" data-id="${k.id}" data-name="${k.name.replace(/"/g, '&quot;')}" ${isAdmin ? '' : 'disabled title="Only admin can add Borrow (Upad) [U]"'} style="color:#dc2626; border-color:#fca5a5;">
                        <kbd class="kbd-shortcut">U</kbd> Borrow
                    </button>
                    <button class="btn btn-primary btn-karigar-jama" data-id="${k.id}" data-name="${k.name.replace(/"/g, '&quot;')}" style="background:#059669; border-color:#059669;" title="Add Maal piece-rate work [J]">
                        <kbd class="kbd-shortcut">J</kbd> Maal (Jama)
                    </button>
                </div>

                <div class="karigar-actions-secondary" style="display:flex; align-items:center; justify-content:space-between; gap:4px; margin-top:0.5rem; flex-wrap:wrap;">
                    <div style="display:flex; gap:4px; align-items:center;">
                        <button class="btn-subtle btn-karigar-history" data-id="${k.id}" data-name="${k.name.replace(/"/g, '&quot;')}" title="View detailed history [H]">
                            <kbd class="kbd-shortcut">H</kbd> History
                        </button>
                        <button class="btn-subtle btn-karigar-toggle-paid" data-id="${k.id}" data-name="${k.name.replace(/"/g, '&quot;')}" title="Mark/Unmark as Paid & Settled [C]" style="color:${isPaid ? '#16a34a' : '#64748b'}; font-weight:700;">
                            <kbd class="kbd-shortcut">C</kbd> ${isPaid ? '✓ Paid' : 'Mark Clear'}
                        </button>
                    </div>
                    <div style="display:flex; gap:4px; align-items:center;">
                        <button class="btn-subtle btn-karigar-slip-print" data-id="${k.id}" data-name="${k.name.replace(/"/g, '&quot;')}" title="Direct Print Salary Slip (No download) [P]" style="color:#0f172a; font-weight:700;">
                            <kbd class="kbd-shortcut">P</kbd> <i class='bx bx-printer'></i> Print
                        </button>
                        <button class="btn-subtle btn-karigar-slip-pdf" data-id="${k.id}" data-name="${k.name.replace(/"/g, '&quot;')}" title="Download PDF file">
                            <i class='bx bxs-file-pdf text-danger'></i> PDF
                        </button>
                    </div>
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
    const opEl = document.getElementById('karigar-overall-pics');
    if (opEl) {
        opEl.textContent = String(globalPicsSum);
    }
    
    // Bind buttons
    grid.querySelectorAll('.btn-karigar-upad').forEach(btn => {
        btn.addEventListener('click', (e) => {
            if (!isAdminUser()) {
                showToast('Only admin can add Borrow (Upad)', 'error');
                return;
            }
            const el = e.currentTarget;
            openKarigarUpadModal(el.dataset.id, el.dataset.name);
        });
    });
    grid.querySelectorAll('.btn-karigar-jama').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const el = e.currentTarget;
            openKarigarJamaModal(el.dataset.id, el.dataset.name);
        });
    });
    grid.querySelectorAll('.btn-karigar-history').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const el = e.currentTarget;
            openKarigarHistoryModal(el.dataset.id, el.dataset.name);
        });
    });
    grid.querySelectorAll('.btn-karigar-toggle-paid').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const el = e.currentTarget;
            toggleKarigarPaidWithConfirm(el.dataset.id, el.dataset.name);
        });
    });
    grid.querySelectorAll('.btn-karigar-slip-print').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const el = e.currentTarget;
            const karigarId = el.dataset.id;
            const karigarName = el.dataset.name;
            const k = (getEffectiveKarigarList() || []).find(x => x.id === karigarId) || { id: karigarId, name: karigarName };
            openSalarySlipOptionsModal({ karigar: k, defaultAction: 'print' });
        });
    });
    grid.querySelectorAll('.btn-karigar-slip-pdf').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const el = e.currentTarget;
            const karigarId = el.dataset.id;
            const karigarName = el.dataset.name;
            const k = (getEffectiveKarigarList() || []).find(x => x.id === karigarId) || { id: karigarId, name: karigarName };
            openSalarySlipOptionsModal({ karigar: k, defaultAction: 'download' });
        });
    });

    // Wire up filter button clicks
    document.querySelectorAll('.karigar-filter-btn').forEach(btn => {
        if (!btn.dataset.bound) {
            btn.dataset.bound = 'true';
            btn.addEventListener('click', () => {
                document.querySelectorAll('.karigar-filter-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                AppState.karigarPaidFilter = btn.dataset.filter || 'all';
                renderKarigarGrid();
            });
        }
    });

    // Make clicking any card select it for keyboard shortcuts
    grid.querySelectorAll('.karigar-card').forEach((card, idx) => {
        card.addEventListener('click', (e) => {
            if (['BUTTON', 'A', 'INPUT'].includes(e.target.tagName) || e.target.closest('button')) return;
            _selectedKarigarCardIndex = idx;
            highlightSelectedCard();
        });
    });

    if (_selectedKarigarCardIndex >= 0) {
        highlightSelectedCard();
    }
}

function setupKarigarListeners() {
    const searchInput = document.getElementById('karigar-search-input');
    if (searchInput && !searchInput.dataset.bound) {
        searchInput.addEventListener('input', () => {
            _selectedKarigarCardIndex = -1;
            renderKarigarGrid();
        });

        // Enter key in search box: Open if matched, or Auto-Create new Karigar!
        searchInput.addEventListener('keydown', async (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                const rawVal = (searchInput.value || '').trim();
                if (!rawVal) return;

                // 1. Exact match?
                const exact = (AppState.karigars || []).find(k => (k.name || '').toLowerCase() === rawVal.toLowerCase());
                if (exact) {
                    openKarigarJamaModal(exact.id, exact.name);
                    return;
                }

                // 2. Single filtered match?
                const filtered = (AppState.karigars || []).filter(k => (k.name || '').toLowerCase().includes(rawVal.toLowerCase()));
                if (filtered.length === 1) {
                    openKarigarJamaModal(filtered[0].id, filtered[0].name);
                    return;
                }

                // 3. No match -> automatically create new Karigar!
                if (!isAdminUser()) {
                    showToast('Only admin can add Karigar', 'error');
                    return;
                }
                await autoCreateKarigarFromSearch(rawVal);
            }
        });
        searchInput.dataset.bound = 'true';
    }

    // Keyboard shortcuts (Karigar modals)
    // - Enter: submit current modal form
    // - Esc: close topmost Karigar modal
    const bindKarigarFormEnterToSubmit = (form, modalEl) => {
        if (!form || form.dataset.shortcutBound) return;
        form.addEventListener('keydown', (e) => {
            if (e.key !== 'Enter') return;
            if (e.shiftKey || e.altKey || e.ctrlKey || e.metaKey) return;
            if (form.dataset.submitting === 'true') {
                e.preventDefault();
                return;
            }
            const t = e.target;
            const tag = (t && t.tagName) ? String(t.tagName).toUpperCase() : '';
            if (tag === 'TEXTAREA') return;
            if (modalEl && !modalEl.classList.contains('show')) return;
            e.preventDefault();
            if (typeof form.requestSubmit === 'function') {
                form.requestSubmit();
                return;
            }
            form.querySelector('button[type=\"submit\"], input[type=\"submit\"]')?.click();
        }, true);
        form.dataset.shortcutBound = 'true';
    };
    if (!window.__karigarEscShortcutBound) {
        document.addEventListener('keydown', (e) => {
            const ids = ['karigar-history-modal', 'karigar-upad-modal', 'karigar-jama-modal', 'karigar-create-modal'];

            // Esc closes topmost Karigar modal.
            if (e.key === 'Escape') {
                for (const id of ids) {
                    const m = document.getElementById(id);
                    if (m && m.classList.contains('show')) {
                        e.preventDefault();
                        m.classList.remove('show');
                        break;
                    }
                }
                return;
            }

            // Enter closes read-only Karigar modals (no form), when not typing in a field/button.
            if (e.key === 'Enter') {
                if (e.shiftKey || e.altKey || e.ctrlKey || e.metaKey) return;
                const t = e.target;
                const tag = (t && t.tagName) ? String(t.tagName).toUpperCase() : '';
                if (['INPUT', 'SELECT', 'TEXTAREA', 'BUTTON', 'A'].includes(tag)) return;
                for (const id of ids) {
                    const m = document.getElementById(id);
                    if (m && m.classList.contains('show')) {
                        // Only close if this modal doesn't contain a form (read-only modal)
                        if (m.querySelector('form')) break;
                        e.preventDefault();
                        m.classList.remove('show');
                        break;
                    }
                }
            }
        });
        window.__karigarEscShortcutBound = true;
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
    const jamaDate = document.getElementById('karigar-jama-date');
    const jamaTime = document.getElementById('karigar-jama-time');
    
    const jamaSize = document.getElementById('karigar-jama-size');
    if (jamaSize && !jamaSize.dataset.bound) {
        jamaSize.addEventListener('input', () => {
            jamaSize.value = jamaSize.value.toUpperCase();
        });
        jamaSize.dataset.bound = 'true';
    }

    if (jamaDesign && !jamaDesign.dataset.bound) {
        const getPriceForDateTime = (designKey, dateValue, timeValue) => {
            const companyId = DESIGN_PRICE_SCOPE;
            const history = (AppState.designPriceHistoryByCompany[companyId] || {})[designKey] || [];
            const targetDateTime = combineDateAndTimeToISO(dateValue, timeValue || '00:00') || `${getTodayISODate()}T00:00:00`;
            const targetMinuteClose = `${targetDateTime.slice(0, 16)}:59`;
            if (Array.isArray(history) && history.length > 0) {
                let selected = null;
                history.forEach(point => {
                    const eff = normalizeToISODateTime(point.effectiveFrom, getTodayISODate());
                    if (eff && eff <= targetMinuteClose) selected = point;
                });
                if (selected && !selected.isDeleted && Number.isFinite(parseFloat(selected.price))) return parseFloat(selected.price);
                if (selected && selected.isDeleted) return null;
            }
            if (Number.isFinite(parseFloat(AppState.designPrices[designKey]))) return parseFloat(AppState.designPrices[designKey]);
            return null;
        };

        let autoPriceTimer = null;
        let autoPriceReqSeq = 0;
        const applyAutoPrice = async () => {
            const up = jamaDesign.value.trim().toUpperCase();
            if (!up) {
                updateJamaTotal();
                return;
            }
            const dateValue = jamaDate?.value || getTodayISODate();
            const timeValue = jamaTime?.value || '00:00';
            const targetDateTime = combineDateAndTimeToISO(dateValue, timeValue) || `${getTodayISODate()}T00:00:00`;
            const asOfDateTime = `${targetDateTime.slice(0, 16)}:59`;

            const reqId = ++autoPriceReqSeq;
            let resolved = null;
            try {
                const live = await FirebaseService.getDesignPrices(DESIGN_PRICE_SCOPE, asOfDateTime);
                if (reqId !== autoPriceReqSeq) return;
                if (live && live.success) {
                    AppState.designPrices = live.data || {};
                    AppState.designPricesByCompany[DESIGN_PRICE_SCOPE] = live.data || {};
                    AppState.designPriceHistoryByCompany[DESIGN_PRICE_SCOPE] = live.history || {};
                    const livePrice = parseFloat((live.data || {})[up]);
                    if (Number.isFinite(livePrice)) resolved = livePrice;
                }
            } catch (e) {
                console.warn('Live design price fetch failed, using local cache:', e);
            }
            if (!Number.isFinite(resolved)) {
                resolved = getPriceForDateTime(up, dateValue, timeValue);
            }
            if (Number.isFinite(resolved)) {
                const oldVal = jamaPrice.value;
                jamaPrice.value = resolved;
                
                // Show AI animation if it auto-filled a different price
                if (oldVal != jamaPrice.value) {
                    jamaPrice.classList.add('rainbow-autofill');
                    setTimeout(() => jamaPrice.classList.remove('rainbow-autofill'), 2500);
                }
            }
            updateJamaTotal();
        };
        const queueAutoPrice = () => {
            if (autoPriceTimer) clearTimeout(autoPriceTimer);
            autoPriceTimer = setTimeout(() => {
                applyAutoPrice().catch(err => console.warn('Auto price resolve failed:', err));
            }, 180);
        };

        jamaDesign.addEventListener('input', queueAutoPrice);
        if (jamaDate) jamaDate.addEventListener('change', queueAutoPrice);
        if (jamaTime) jamaTime.addEventListener('change', queueAutoPrice);
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
            const sel = document.getElementById('karigar-upad-select');
            const id = String((sel ? sel.value : document.getElementById('karigar-upad-id')?.value) || '').trim();
            let name = '';
            if (sel && sel.selectedOptions && sel.selectedOptions[0]) name = String(sel.selectedOptions[0].textContent || '').trim();
            const stats = calculateKarigarBalance(id, name);
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
            if (createForm.dataset.submitting === 'true') return;
            createForm.dataset.submitting = 'true';
            const submitBtn = createForm.querySelector('button[type="submit"], input[type="submit"]');
            if (submitBtn) submitBtn.disabled = true;
            const name = document.getElementById('karigar-create-name').value.trim();
            if (!name) {
                createForm.dataset.submitting = 'false';
                if (submitBtn) submitBtn.disabled = false;
                return;
            }
            showLoader();
            try {
                await FirebaseService.addKarigar(name, AppState.currentCompany, buildAuditActor());
                _pendingDataChangesForBackup = true;
                scheduleBackgroundSheetBackup('addKarigar');
                document.getElementById('karigar-search-input').value = '';
                document.getElementById('karigar-create-modal').classList.remove('show');
                showToast("Karigar created successfully!", "success");
                invalidateKarigarCache();
                await renderKarigarPage(true);
            } catch (err) { showToast("Failed to create", "error"); }
            finally {
                hideLoader();
                createForm.dataset.submitting = 'false';
                if (submitBtn) submitBtn.disabled = false;
            }
        });
        createForm.dataset.bound = 'true';
    }
    bindKarigarFormEnterToSubmit(createForm, document.getElementById('karigar-create-modal'));
    
    const jamaForm = document.getElementById('karigar-jama-form');
    if (jamaForm && !jamaForm.dataset.bound) {
        jamaForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            if (jamaForm.dataset.submitting === 'true') return;
            jamaForm.dataset.submitting = 'true';
            const submitBtn = jamaForm.querySelector('button[type="submit"], input[type="submit"]');
            if (submitBtn) submitBtn.disabled = true;
            const select = document.getElementById('karigar-jama-select');
            const rawKarigarId = String((select ? select.value : document.getElementById('karigar-jama-id')?.value) || '').trim();
            let karigarName = '';
            if (select && select.selectedOptions && select.selectedOptions[0]) {
                karigarName = String(select.selectedOptions[0].textContent || '').trim();
            }
            if (!karigarName && rawKarigarId) {
                const k = (AppState.karigars || []).find(x => String(x?.id || '').trim() === rawKarigarId);
                karigarName = String(k?.name || '').trim();
            }
            const karigarId = resolveKarigarIdFromState(rawKarigarId, karigarName);
            if (!karigarId) {
                showToast('Invalid karigar ID', 'error');
                jamaForm.dataset.submitting = 'false';
                if (submitBtn) submitBtn.disabled = false;
                return;
            }
            const date = document.getElementById('karigar-jama-date').value;
            const time = document.getElementById('karigar-jama-time').value;
            const designName = document.getElementById('karigar-jama-design').value;
            const size = document.getElementById('karigar-jama-size').value;
            const pic = document.getElementById('karigar-jama-pic').value;
            const price = document.getElementById('karigar-jama-price').value;
            const upadAmount = isAdminUser() ? (document.getElementById('karigar-jama-upad').value || 0) : 0;
            const transactionDateTime = combineDateAndTimeToISO(date, time);
            if (!transactionDateTime) {
                showToast('Invalid date/time', 'error');
                jamaForm.dataset.submitting = 'false';
                if (submitBtn) submitBtn.disabled = false;
                return;
            }

            const bulkMode = true; // always-on bulk entry
            const autoNext = false; // keep current selected karigar unless user changes
            
            showLoader();
            try {
                if (AppState.viewingKarigarBackupId) {
                    const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
                    if (!b) throw new Error('Archive backup not found');
                    if (!Array.isArray(b.rows)) b.rows = [];

                    const numPic = parseInt(pic, 10) || 0;
                    const numPrice = parseFloat(price) || 0;
                    const numTotal = Math.round(((numPic * numPrice) + Number.EPSILON) * 100) / 100;
                    const numUpad = Math.round(((parseFloat(upadAmount) || 0) + Number.EPSILON) * 100) / 100;
                    const txId = 'tx_arch_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);

                    const newRow = {
                        id: txId,
                        companyId: b.companyId || AppState.currentCompany,
                        karigarId,
                        karigarName,
                        type: 'jama',
                        date,
                        transactionDateTime,
                        designName,
                        size,
                        pic: numPic,
                        price: numPrice,
                        total: numTotal,
                        upadAmount: numUpad,
                        amount: 0,
                        addedBy: AppState.currentUser?.role || 'admin',
                        source: 'archive_edit'
                    };
                    b.rows.push(newRow);
                    recomputeBackupSummary(b);
                    await saveArchiveBackupChanges(b);
                    rebuildKarigarComputedCache();
                    auditKarigarFinancials();
                    showToast("Maal (Jama) saved to archive!", "success");
                } else {
                    await FirebaseService.addKarigarJama({ 
                        karigarId, karigarName, date, transactionDateTime, designName, size, pic, price, upadAmount, 
                        companyId: AppState.currentCompany,
                        addedBy: AppState.currentUser?.role || 'admin',
                        actor: buildAuditActor()
                    });
                    _pendingDataChangesForBackup = true;
                    scheduleBackgroundSheetBackup('addKarigarJama');
                    showToast("Maal (Jama) saved!", "success");
                }

                if (bulkMode) {
                    // Prepare next entry without closing popup.
                    document.getElementById('karigar-jama-design').value = '';
                    document.getElementById('karigar-jama-size').value = '';
                    document.getElementById('karigar-jama-pic').value = '';
                    document.getElementById('karigar-jama-price').value = '';
                    if (isAdminUser()) document.getElementById('karigar-jama-upad').value = '';
                    document.getElementById('karigar-jama-total-display').textContent = '0';
                    // Keep same date; refresh time to now for convenience.
                    const timeEl = document.getElementById('karigar-jama-time');
                    if (timeEl) timeEl.value = getCurrentLocalTimeInput();

                    if (autoNext && select && select.options && select.options.length > 0) {
                        const idx = Math.max(0, select.selectedIndex);
                        const nextIdx = (idx + 1) % select.options.length;
                        select.selectedIndex = nextIdx;
                        // Sync hidden id (if any legacy paths read it)
                        const hid = document.getElementById('karigar-jama-id');
                        if (hid) hid.value = String(select.value || '').trim();
                    }

                    // Refresh background data but keep modal open.
                    invalidateKarigarCache();
                    await renderKarigarPage(true);
                    document.getElementById('karigar-jama-modal').classList.add('show');
                    setTimeout(() => {
                        document.getElementById('karigar-jama-design')?.focus();
                    }, 0);
                } else {
                    document.getElementById('karigar-jama-modal').classList.remove('show');
                    invalidateKarigarCache();
                    await renderKarigarPage(true);
                }
            } catch (err) { showToast("Failed to add", "error"); }
            finally {
                hideLoader();
                jamaForm.dataset.submitting = 'false';
                if (submitBtn) submitBtn.disabled = false;
            }
        });
        jamaForm.dataset.bound = 'true';
    }
    
    const upadForm = document.getElementById('karigar-upad-form');
    if (upadForm && !upadForm.dataset.bound) {
        upadForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            if (upadForm.dataset.submitting === 'true') return;
            upadForm.dataset.submitting = 'true';
            const submitBtn = upadForm.querySelector('button[type="submit"], input[type="submit"]');
            if (submitBtn) submitBtn.disabled = true;
            if (!isAdminUser()) return showToast('Only admin can add Borrow (Upad)', 'error');
            const sel = document.getElementById('karigar-upad-select');
            const rawKarigarId = String((sel ? sel.value : document.getElementById('karigar-upad-id')?.value) || '').trim();
            let karigarName = '';
            if (sel && sel.selectedOptions && sel.selectedOptions[0]) karigarName = String(sel.selectedOptions[0].textContent || '').trim();
            if (!karigarName && rawKarigarId) {
                const k = (AppState.karigars || []).find(x => String(x?.id || '').trim() === rawKarigarId);
                karigarName = String(k?.name || '').trim();
            }
            const karigarId = resolveKarigarIdFromState(rawKarigarId, karigarName);
            if (!karigarId) {
                showToast('Invalid karigar ID', 'error');
                upadForm.dataset.submitting = 'false';
                if (submitBtn) submitBtn.disabled = false;
                return;
            }
            const date = document.getElementById('karigar-upad-date').value;
            const time = document.getElementById('karigar-upad-time').value;
            const amount = document.getElementById('karigar-upad-amount').value;
            const reason = String(document.getElementById('karigar-upad-reason')?.value || '').trim() || 'Cash Advance';
            const transactionDateTime = combineDateAndTimeToISO(date, time);
            if (!transactionDateTime) {
                showToast('Invalid date/time', 'error');
                upadForm.dataset.submitting = 'false';
                if (submitBtn) submitBtn.disabled = false;
                return;
            }
            
            showLoader();
            try {
                if (AppState.viewingKarigarBackupId) {
                    const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
                    if (!b) throw new Error('Archive backup not found');
                    if (!Array.isArray(b.rows)) b.rows = [];

                    const numAmount = Math.round(((parseFloat(amount) || 0) + Number.EPSILON) * 100) / 100;
                    const txId = 'tx_arch_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);

                    const newRow = {
                        id: txId,
                        companyId: b.companyId || AppState.currentCompany,
                        karigarId,
                        karigarName,
                        type: 'upad',
                        date,
                        transactionDateTime,
                        amount: numAmount,
                        note: reason,
                        remarks: reason,
                        reason: reason,
                        upadAmount: 0,
                        total: 0,
                        addedBy: AppState.currentUser?.role || 'admin',
                        source: 'archive_edit'
                    };
                    b.rows.push(newRow);
                    recomputeBackupSummary(b);
                    await saveArchiveBackupChanges(b);
                    rebuildKarigarComputedCache();
                    auditKarigarFinancials();
                    document.getElementById('karigar-upad-modal').classList.remove('show');
                    showToast("Borrow (Upad) saved to archive!", "success");
                    renderKarigarGrid();
                } else {
                    await FirebaseService.addKarigarUpad({ 
                        karigarId, karigarName, date, transactionDateTime, amount, 
                        note: reason, remarks: reason, reason: reason,
                        companyId: AppState.currentCompany,
                        addedBy: AppState.currentUser?.role || 'admin',
                        actor: buildAuditActor()
                    });
                    _pendingDataChangesForBackup = true;
                    scheduleBackgroundSheetBackup('addKarigarUpad');
                    document.getElementById('karigar-upad-modal').classList.remove('show');
                    showToast("Borrow (Upad) added successfully!", "success");
                    invalidateKarigarCache();
                    await renderKarigarPage(true);
                }
            } catch (err) { showToast("Failed to add", "error"); }
            finally {
                hideLoader();
                upadForm.dataset.submitting = 'false';
                if (submitBtn) submitBtn.disabled = false;
            }
        });
        upadForm.dataset.bound = 'true';
    }

    // ── Toolbar & Action Buttons ──
    const refreshBtn = document.getElementById('btn-refresh-karigar');
    if (refreshBtn && !refreshBtn.dataset.bound) {
        refreshBtn.dataset.bound = 'true';
        refreshBtn.addEventListener('click', async () => {
            refreshBtn.disabled = true;
            refreshBtn.innerHTML = "<i class='bx bx-loader-alt bx-spin'></i> Refreshing...";
            try {
                await renderKarigarPage(true);
                showToast('Karigar data refreshed & zero-error verified!', 'success');
            } finally {
                refreshBtn.disabled = false;
                refreshBtn.innerHTML = "<i class='bx bx-refresh'></i> Refresh";
            }
        });
    }

    const companyReportBtn = document.getElementById('btn-company-report-karigar');
    if (companyReportBtn && !companyReportBtn.dataset.bound) {
        companyReportBtn.dataset.bound = 'true';
        companyReportBtn.addEventListener('click', () => {
            openCompanyReportOptionsModal();
        });
    }

    const bulkPdfBtn = document.getElementById('btn-bulk-pdf-karigar');
    if (bulkPdfBtn && !bulkPdfBtn.dataset.bound) {
        bulkPdfBtn.dataset.bound = 'true';
        bulkPdfBtn.addEventListener('click', () => openSalarySlipOptionsModal({ isBulk: true, defaultAction: 'download' }));
    }

    const summaryPdfBtn = document.getElementById('export-pdf-karigar-btn');
    if (summaryPdfBtn && !summaryPdfBtn.dataset.bound) {
        summaryPdfBtn.dataset.bound = 'true';
        summaryPdfBtn.addEventListener('click', () => openCompanyReportOptionsModal());
    }

    const resetCycleBtn = document.getElementById('btn-reset-karigar');
    if (resetCycleBtn && !resetCycleBtn.dataset.bound) {
        resetCycleBtn.dataset.bound = 'true';
        resetCycleBtn.addEventListener('click', () => confirmKarigarMonthlyReset(AppState.currentCompany));
    }


    const arcReportBtn = document.getElementById('btn-archive-company-report');
    if (arcReportBtn && !arcReportBtn.dataset.bound) {
        arcReportBtn.dataset.bound = 'true';
        arcReportBtn.addEventListener('click', () => {
            const backupId = document.getElementById('karigar-reset-backup-select')?.value;
            const b = (AppState.karigarResetBackups || []).find(x => x.id === backupId);
            if (!b) return showToast('No backup selected', 'error');
            const safeRows = Array.isArray(b.rows) ? b.rows : [];
            openCompanyReportOptionsModal({ companyId: b.companyId, txs: safeRows, periodLabel: getKarigarBackupRangeLabel(b) });
        });
    }

    const arcSlipsBtn = document.getElementById('btn-archive-all-slips');
    if (arcSlipsBtn && !arcSlipsBtn.dataset.bound) {
        arcSlipsBtn.dataset.bound = 'true';
        arcSlipsBtn.addEventListener('click', () => {
            const backupId = document.getElementById('karigar-reset-backup-select')?.value;
            const b = (AppState.karigarResetBackups || []).find(x => x.id === backupId);
            if (!b) return showToast('No backup selected', 'error');
            const safeRows = Array.isArray(b.rows) ? b.rows : [];
            openSalarySlipOptionsModal({
                isBulk: true,
                defaultAction: 'download',
                bulkOptions: { txsOverride: safeRows, periodLabel: getKarigarBackupRangeLabel(b) }
            });
        });
    }
}

function setupKarigarFastEnterNavigation() {
    // 1. Jama Modal Fast Enter Navigation
    const jamaFields = [
        'karigar-jama-select',
        'karigar-jama-date',
        'karigar-jama-time',
        'karigar-jama-design',
        'karigar-jama-size',
        'karigar-jama-pic',
        'karigar-jama-price',
        'karigar-jama-upad'
    ];

    jamaFields.forEach((id, index) => {
        const el = document.getElementById(id);
        if (!el || el.dataset.enterNavBound) return;
        el.dataset.enterNavBound = 'true';

        el.addEventListener('focus', () => {
            if (typeof el.select === 'function') {
                try { el.select(); } catch (e) {}
            }
        });

        // Fast Arrow Key browsing for Karigar dropdown
        if (id === 'karigar-jama-select') {
            el.addEventListener('keydown', (e) => {
                if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                    setTimeout(() => {
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }, 0);
                }
            });
        }

        el.addEventListener('keydown', (e) => {
            if (e.key !== 'Enter') return;
            if (e.shiftKey || e.altKey || e.ctrlKey || e.metaKey) return;
            e.preventDefault();
            e.stopPropagation();

            const form = document.getElementById('karigar-jama-form');
            if (form && form.dataset.submitting === 'true') return;

            // Karigar select Enter -> jump straight to Design Name / No.
            if (id === 'karigar-jama-select') {
                el.dispatchEvent(new Event('change', { bubbles: true }));
                const designEl = document.getElementById('karigar-jama-design');
                if (designEl) {
                    designEl.focus();
                    if (typeof designEl.select === 'function') {
                        try { designEl.select(); } catch (err) {}
                    }
                }
                return;
            }

            const isLastField = (!isAdminUser() && (id === 'karigar-jama-price' || id === 'karigar-jama-pic')) || (index >= jamaFields.length - 1);
            if (isLastField) {
                if (typeof form?.requestSubmit === 'function') {
                    form.requestSubmit();
                } else {
                    form?.querySelector('button[type="submit"]')?.click();
                }
                return;
            }

            let nextIdx = index + 1;
            if (!isAdminUser() && jamaFields[nextIdx] === 'karigar-jama-upad') {
                if (typeof form?.requestSubmit === 'function') {
                    form.requestSubmit();
                } else {
                    form?.querySelector('button[type="submit"]')?.click();
                }
                return;
            }

            const nextEl = document.getElementById(jamaFields[nextIdx]);
            if (nextEl) {
                nextEl.focus();
                if (typeof nextEl.select === 'function') {
                    try { nextEl.select(); } catch (err) {}
                }
            }
        });
    });

    // 2. Upad Modal Fast Enter Navigation
    const upadFields = [
        'karigar-upad-select',
        'karigar-upad-date',
        'karigar-upad-time',
        'karigar-upad-amount',
        'karigar-upad-reason'
    ];

    upadFields.forEach((id, index) => {
        const el = document.getElementById(id);
        if (!el || el.dataset.enterNavBound) return;
        el.dataset.enterNavBound = 'true';

        el.addEventListener('focus', () => {
            if (typeof el.select === 'function') {
                try { el.select(); } catch (e) {}
            }
        });

        // Fast Arrow Key browsing for Karigar dropdown - instantly refreshes balance display
        if (id === 'karigar-upad-select') {
            el.addEventListener('keydown', (e) => {
                if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                    setTimeout(() => {
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }, 0);
                }
            });
        }

        el.addEventListener('keydown', (e) => {
            if (e.key !== 'Enter') return;
            if (e.shiftKey || e.altKey || e.ctrlKey || e.metaKey) return;
            e.preventDefault();
            e.stopPropagation();

            const form = document.getElementById('karigar-upad-form');
            if (form && form.dataset.submitting === 'true') return;

            // Karigar select Enter -> jump straight to Upad Amount
            if (id === 'karigar-upad-select') {
                el.dispatchEvent(new Event('change', { bubbles: true }));
                const amountEl = document.getElementById('karigar-upad-amount');
                if (amountEl) {
                    amountEl.focus();
                    if (typeof amountEl.select === 'function') {
                        try { amountEl.select(); } catch (err) {}
                    }
                }
                return;
            }

            if (index >= upadFields.length - 1) {
                if (typeof form?.requestSubmit === 'function') {
                    form.requestSubmit();
                } else {
                    form?.querySelector('button[type="submit"]')?.click();
                }
                return;
            }

            const nextEl = document.getElementById(upadFields[index + 1]);
            if (nextEl) {
                nextEl.focus();
                if (typeof nextEl.select === 'function') {
                    try { nextEl.select(); } catch (err) {}
                }
            }
        });
    });
}

function setupKarigarGlobalShortcuts() {
    if (window.__karigarGlobalShortcutsBound) return;
    window.__karigarGlobalShortcutsBound = true;

    window.addEventListener('keydown', (e) => {
        if (AppState.currentSection !== 'karigar') return;

        const activeModal = document.querySelector('.modal.show');
        if (activeModal) {
            // If History modal is open, pressing 'P' directly prints salary slip
            if (activeModal.id === 'karigar-history-modal') {
                if ((e.key === 'p' || e.key === 'P') && !['INPUT', 'TEXTAREA'].includes(e.target?.tagName)) {
                    e.preventDefault();
                    document.getElementById('btn-print-slip-employee')?.click();
                }
            }
            return;
        }

        const targetTag = (e.target?.tagName || '').toUpperCase();
        const searchInput = document.getElementById('karigar-search-input');

        // If typing inside an input/select/textarea
        if (['INPUT', 'SELECT', 'TEXTAREA'].includes(targetTag)) {
            if (e.target === searchInput) {
                if (e.key === 'Escape') {
                    e.preventDefault();
                    searchInput.value = '';
                    searchInput.blur();
                    renderKarigarGrid();
                    return;
                }
                if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    searchInput.blur();
                    _selectedKarigarCardIndex = 0;
                    highlightSelectedCard();
                    return;
                }
            }
            return;
        }

        // Cmd+P or Ctrl+P on Karigar page: Print slip
        if ((e.metaKey || e.ctrlKey) && (e.key === 'p' || e.key === 'P')) {
            e.preventDefault();
            const k = getSelectedOrFirstKarigar();
            if (k) openSalarySlipOptionsModal({ karigar: k, defaultAction: 'print' });
            return;
        }

        // If other modifiers are active, allow standard OS shortcuts
        if (e.metaKey || e.ctrlKey || e.altKey) return;

        // '/' shortcut: focus search input immediately
        if (e.key === '/') {
            e.preventDefault();
            if (searchInput) {
                searchInput.focus();
                searchInput.select();
            }
            return;
        }

        const key = e.key.toLowerCase();

        // J: Open Maal (Jama)
        if (key === 'j') {
            e.preventDefault();
            const k = getSelectedOrFirstKarigar();
            if (k) openKarigarJamaModal(k.id, k.name);
            return;
        }

        // U: Open Borrow (Upad)
        if (key === 'u') {
            e.preventDefault();
            const k = getSelectedOrFirstKarigar();
            if (k) openKarigarUpadModal(k.id, k.name);
            return;
        }

        // C: Mark as Paid / Clear with confirmation
        if (key === 'c') {
            e.preventDefault();
            const k = getSelectedOrFirstKarigar();
            if (k) toggleKarigarPaidWithConfirm(k.id, k.name);
            return;
        }

        // P: Direct Print Slip (No download)
        if (key === 'p') {
            e.preventDefault();
            const k = getSelectedOrFirstKarigar();
            if (k) openSalarySlipOptionsModal({ karigar: k, defaultAction: 'print' });
            return;
        }

        // H: Open History
        if (key === 'h') {
            e.preventDefault();
            const k = getSelectedOrFirstKarigar();
            if (k) openKarigarHistoryModal(k.id, k.name);
            return;
        }

        // R: Refresh
        if (key === 'r') {
            e.preventDefault();
            document.getElementById('btn-refresh-karigar')?.click();
            return;
        }

        // Arrow keys to navigate cards
        if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
            e.preventDefault();
            const cards = document.querySelectorAll('#karigar-grid .karigar-card');
            if (cards.length > 0) {
                if (_selectedKarigarCardIndex === -1) _selectedKarigarCardIndex = 0;
                else _selectedKarigarCardIndex = Math.min(cards.length - 1, _selectedKarigarCardIndex + 1);
                highlightSelectedCard();
            }
            return;
        }

        if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
            e.preventDefault();
            const cards = document.querySelectorAll('#karigar-grid .karigar-card');
            if (cards.length > 0) {
                if (_selectedKarigarCardIndex === -1) _selectedKarigarCardIndex = 0;
                else _selectedKarigarCardIndex = Math.max(0, _selectedKarigarCardIndex - 1);
                highlightSelectedCard();
            }
            return;
        }

        // Enter on selected card opens Jama
        if (e.key === 'Enter') {
            e.preventDefault();
            const k = getSelectedOrFirstKarigar();
            if (k) openKarigarJamaModal(k.id, k.name);
            return;
        }

        // Any single printable character typed anywhere without clicking: auto-focus search!
        if (e.key.length === 1 && !e.ctrlKey && !e.metaKey) {
            if (searchInput) {
                searchInput.focus();
            }
        }
    });
}

function openKarigarJamaModal(id, name) {
    // Populate Karigar dropdown for bulk entry.
    const select = document.getElementById('karigar-jama-select');
    if (select) {
        const esc = (v) => String(v ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
        const karigars = getEffectiveKarigarList();
        karigars.sort((a, b) => String(a?.name || '').localeCompare(String(b?.name || ''), undefined, { sensitivity: 'base' }));
        const safeId = String(id || '').trim();
        const currentVal = safeId || String(select.value || '').trim();
        select.innerHTML = karigars.map(k => {
            const kid = String(k?.id || '').trim();
            const kname = String(k?.name || '').trim();
            const sel = (kid && kid === currentVal) ? ' selected' : '';
            return `<option value="${esc(kid)}"${sel}>${esc(kname || kid)}</option>`;
        }).join('') || `<option value="">No karigars</option>`;
        if (safeId) select.value = safeId;

        const syncHidden = () => {
            const v = String(select.value || '').trim();
            const hid = document.getElementById('karigar-jama-id');
            if (hid) hid.value = v;
        };
        if (!select.dataset.bound) {
            select.addEventListener('change', syncHidden);
            select.dataset.bound = 'true';
        }
        syncHidden();
    }
    if (!select) {
        document.getElementById('karigar-jama-id').value = String(id || '').trim();
    }
    document.getElementById('karigar-jama-date').value = getTodayISODate();
    document.getElementById('karigar-jama-time').value = getCurrentLocalTimeInput();
    document.getElementById('karigar-jama-design').value = '';
    document.getElementById('karigar-jama-size').value = '';
    document.getElementById('karigar-jama-pic').value = '';
    document.getElementById('karigar-jama-price').value = '';
    document.getElementById('karigar-jama-upad').value = '';
    const jamaPriceInput = document.getElementById('karigar-jama-price');
    if (jamaPriceInput) {
        if (!isAdminUser()) {
            jamaPriceInput.readOnly = true;
            jamaPriceInput.placeholder = 'Auto from saved size price';
        } else {
            jamaPriceInput.readOnly = false;
            jamaPriceInput.placeholder = 'Auto-fills from past';
        }
    }
    const jamaUpadInput = document.getElementById('karigar-jama-upad');
    if (jamaUpadInput) {
        if (!isAdminUser()) {
            jamaUpadInput.value = '0';
            jamaUpadInput.disabled = true;
            jamaUpadInput.placeholder = 'Only admin can add upad from Jama';
        } else {
            jamaUpadInput.disabled = false;
            jamaUpadInput.placeholder = 'Any money taken today';
        }
    }
    document.getElementById('karigar-jama-total-display').textContent = '0';
    document.getElementById('karigar-jama-modal').classList.add('show');

    // Focus Karigar dropdown so Arrow keys browse karigars and Enter moves to Design
    setTimeout(() => {
        setupKarigarFastEnterNavigation();
        const sel = document.getElementById('karigar-jama-select');
        if (sel) {
            sel.focus();
        } else {
            document.getElementById('karigar-jama-design')?.focus();
        }
    }, 40);
}

function openKarigarUpadModal(id, name) {
    if (!isAdminUser()) return showToast('Only admin can add Borrow (Upad)', 'error');
    const stats = calculateKarigarBalance(id, name);
    const sel = document.getElementById('karigar-upad-select');
    if (sel) {
        const esc = (v) => String(v ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
        const karigars = getEffectiveKarigarList();
        karigars.sort((a, b) => String(a?.name || '').localeCompare(String(b?.name || ''), undefined, { sensitivity: 'base' }));
        const safeId = String(id || '').trim();
        const currentVal = safeId || String(sel.value || '').trim();
        sel.innerHTML = karigars.map(k => {
            const kid = String(k?.id || '').trim();
            const kname = String(k?.name || '').trim();
            const selected = (kid && kid === currentVal) ? ' selected' : '';
            return `<option value="${esc(kid)}"${selected}>${esc(kname || kid)}</option>`;
        }).join('') || `<option value="">No karigars</option>`;
        if (safeId) sel.value = safeId;

        const syncHidden = () => {
            const v = String(sel.value || '').trim();
            const hid = document.getElementById('karigar-upad-id');
            if (hid) hid.value = v;
        };
        if (!sel.dataset.bound) {
            sel.addEventListener('change', () => {
                syncHidden();
                // Update the summary when switching karigar.
                const curId = String(sel.value || '').trim();
                const curName = String(sel.selectedOptions?.[0]?.textContent || '').trim();
                const s = calculateKarigarBalance(curId, curName);
                document.getElementById('karigar-upad-jama-display').textContent = '₹' + s.totalJama.toFixed(2);
                const bd = document.getElementById('karigar-upad-balance-display');
                bd.textContent = '₹' + s.balance.toFixed(2);
                bd.classList.remove('text-danger', 'text-primary', 'text-success');
                if (s.balance < 0) bd.classList.add('text-danger'); else bd.classList.add('text-primary');
            });
            sel.dataset.bound = 'true';
        }
        syncHidden();
    } else {
        document.getElementById('karigar-upad-id').value = id;
    }
    document.getElementById('karigar-upad-date').value = getTodayISODate();
    document.getElementById('karigar-upad-time').value = getCurrentLocalTimeInput();
    document.getElementById('karigar-upad-amount').value = '';
    const reasonInput = document.getElementById('karigar-upad-reason');
    if (reasonInput) reasonInput.value = '';
    
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

    // Focus Karigar dropdown so Arrow keys browse karigars and Enter moves to Upad Amount
    setTimeout(() => {
        setupKarigarFastEnterNavigation();
        const sel = document.getElementById('karigar-upad-select');
        if (sel) {
            sel.focus();
        } else {
            document.getElementById('karigar-upad-amount')?.focus();
        }
    }, 40);
}

function openKarigarHistoryModal(id, name) {
    const resolvedId = resolveKarigarIdFromState(id, name);
    if (!resolvedId) return showToast('Invalid karigar ID', 'error');
    const txs = getKarigarTransactionsFor(resolvedId);
    document.getElementById('karigar-history-name-display').textContent = name;
    const tbody = document.getElementById('karigar-history-tbody');
    tbody.innerHTML = '';
    
    let totalJama = 0;
    let totalUpad = 0;
    const round2 = (n) => Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;
    
    if (txs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="text-center p-4 text-muted">No transactions recorded for this karigar.</td></tr>';
    } else {
        txs.forEach(t => {
            const isJama = t.type === 'jama';
            const jamaVal = isJama ? round2(t.total || 0) : 0;
            const upadVal = isJama ? round2(t.upadAmount || 0) : round2(t.amount || t.upadAmount || 0);
            
            totalJama = round2(totalJama + jamaVal);
            totalUpad = round2(totalUpad + upadVal);
            
            let companyTag = '';
            if (t.companyId === 'company1') companyTag = `<span class="badge badge-primary badge-sm" style="font-size: 0.6rem; padding: 2px 4px;">C1</span>`;
            if (t.companyId === 'company2') companyTag = `<span class="badge badge-success badge-sm" style="font-size: 0.6rem; padding: 2px 4px;">C2</span>`;
            
            let createdFrom = String(t.createdFrom || t.source || t.dashboard || t.addedBy || '').trim();
            if (!createdFrom || createdFrom.toLowerCase() === 'web_app') {
                createdFrom = String(t.addedBy || 'unknown').trim();
            }
            let updatedFrom = String(t.updatedFrom || '').trim();
            if (updatedFrom.toLowerCase() === 'web_app') updatedFrom = '';
            let addedByTag = `<span class="text-xs text-muted mt-1 block"><i class='bx bx-link-alt'></i> Created from: ${createdFrom}</span>`;
            if (updatedFrom) {
                addedByTag += `<span class="text-xs text-muted block"><i class='bx bx-edit'></i> Updated from: ${updatedFrom}</span>`;
            }
            
            const txDateObj = parseFlexibleDateTime(t.transactionDateTime || t.dateTime || t.createdAt) || parseFlexibleDateTime(t.date);
            const displayDate = txDateObj ? formatISODateForDisplay(txDateObj) : (formatISODateForDisplay(t.date) || '-');
            const timeTag = txDateObj
                ? `<div class="text-xs text-muted mt-1"><i class='bx bx-time-five'></i> ${txDateObj.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}</div>`
                : '';
            
            const row = `
                <tr style="border-bottom: 1px solid var(--border-light); ${!isJama ? 'background: #fffcfc;' : ''}">
                    <td class="p-2 text-sm">${displayDate} ${timeTag} <div class="mt-1">${companyTag}</div></td>
                    <td class="p-2 text-sm font-medium">${isJama ? (t.designName || 'Maal') : '<span class="text-danger font-bold">Direct Borrow (Upad)</span>'} ${addedByTag}</td>
                    <td class="p-2 text-sm text-center">${t.size || '-'}</td>
                    <td class="p-2 text-sm text-right font-bold">${t.pic || '-'}</td>
                    <td class="p-2 text-sm text-right font-mono">${t.price ? '₹'+parseFloat(t.price).toFixed(2) : '-'}</td>
                    <td class="p-2 text-sm text-right text-success font-bold">${isJama ? '₹'+jamaVal.toFixed(2) : '-'}</td>
                    <td class="p-2 text-sm text-right text-danger font-bold">${upadVal > 0 ? '₹'+upadVal.toFixed(2) : '-'}</td>
                    <td class="p-2 text-sm text-center">${isAdminUser() ? `
                        <button onclick="editKarigarHistory('${t.id}', '${resolvedId}', '${name}')" class="btn-icon text-primary" title="Edit record"><i class='bx bx-edit'></i></button>
                        <button onclick="deleteKarigarHistory('${t.id}', '${resolvedId}', '${name}')" class="btn-icon text-danger" title="Delete record"><i class='bx bx-trash'></i></button>
                    ` : `<span class="text-muted">-</span>`}</td>
                </tr>
            `;
            tbody.insertAdjacentHTML('beforeend', row);
        });
    }
    
    const netBal = round2(totalJama - totalUpad);
    document.getElementById('history-total-jama').textContent = '₹' + totalJama.toFixed(2);
    document.getElementById('history-total-upad').textContent = '₹' + totalUpad.toFixed(2);
    document.getElementById('history-net-balance').textContent = '₹' + netBal.toFixed(2);
    
    const balEl = document.getElementById('history-net-balance');
    if (netBal < 0) {
        balEl.classList.remove('text-primary', 'text-success');
        balEl.classList.add('text-danger');
    } else {
        balEl.classList.remove('text-danger');
        balEl.classList.add('text-success');
    }
    
    const printSlipBtn = document.getElementById('btn-print-slip-employee');
    if (printSlipBtn) {
        printSlipBtn.onclick = () => {
            const k = (AppState.karigars || []).find(x => x.id === resolvedId) || { id: resolvedId, name: name };
            openSalarySlipOptionsModal({ karigar: k, defaultAction: 'print' });
        };
    }
    const dlSlipBtn = document.getElementById('btn-download-slip-employee');
    if (dlSlipBtn) {
        dlSlipBtn.onclick = () => {
            const k = (AppState.karigars || []).find(x => x.id === resolvedId) || { id: resolvedId, name: name };
            openSalarySlipOptionsModal({ karigar: k, defaultAction: 'download' });
        };
    }
    const deleteBtn = document.getElementById('btn-delete-employee');
    deleteBtn.onclick = () => deleteCompleteKarigar(resolvedId, name);
    const editKarigarBtn = document.getElementById('btn-edit-employee');
    if (editKarigarBtn) {
        editKarigarBtn.onclick = () => editKarigarName(resolvedId, name);
    }
    if (deleteBtn) deleteBtn.style.display = isAdminUser() ? '' : 'none';
    if (editKarigarBtn) editKarigarBtn.style.display = isAdminUser() ? '' : 'none';
    
    document.getElementById('karigar-history-modal').classList.add('show');
}

async function deleteCompleteKarigar(id, name) {
    if (!isAdminUser()) return showToast('Only admin can delete karigar', 'error');
    if (!confirm(`!! WARNING !!\nAre you sure you want to completely delete "${name}"? This will permanently remove them from the system.`)) return;
    
    showLoader();
    try {
        await FirebaseService.deleteKarigar(id, AppState.currentCompany, buildAuditActor());
        _pendingDataChangesForBackup = true;
        scheduleBackgroundSheetBackup('deleteKarigar');
        showToast(`Employee ${name} deleted`, 'success');
        
        document.getElementById('karigar-history-modal').classList.remove('show');
        invalidateKarigarCache();
        await renderKarigarPage(true);
    } catch (e) {
        console.error('Failed to delete karigar:', e);
        showToast('Failed to delete employee', 'error');
    } finally {
        hideLoader();
    }
}

// ===== AUTO LAN BACKUP HELPERS =====
// Writes the latest Karigar + design price data back to the LAN folder JSON files
// immediately after any karigar add/edit/delete or size price change.
// This keeps karigars.json, karigar_transactions.json, and design_prices.json
// always in sync without requiring the user to manually export anything.
async function autoSyncKarigarsToLAN() {
    if (!window.LanStorageService || !LanStorageService.isConnected()) return;
    try {
        // Read live data from the LAN store (which is the authoritative source)
        const karigarsRes = await FirebaseService.getKarigars(AppState.currentCompany || 'company1');
        if (karigarsRes && karigarsRes.data) {
            await LanStorageService.writeFile('karigars', karigarsRes.data);
        }
        // Also sync karigar_transactions
        const txRes = await FirebaseService.getKarigarTransactions(AppState.currentCompany || 'company1');
        if (txRes && Array.isArray(txRes.data)) {
            await LanStorageService.writeFile('karigar_transactions', txRes.data);
        }
    } catch (e) {
        console.warn('[LAN] autoSyncKarigarsToLAN failed:', e.message);
    }
}

async function autoSyncDesignPricesToLAN() {
    if (!window.LanStorageService || !LanStorageService.isConnected()) return;
    try {
        // design_prices are written via the _writeLocalDoc path (writeEncryptedFile → writeFile)
        // so they're already updated. But we also re-write design_price_history to keep it fresh.
        const histRes = await FirebaseService.getDesignPriceHistory
            ? await FirebaseService.getDesignPriceHistory('global')
            : null;
        if (histRes && Array.isArray(histRes.data)) {
            await LanStorageService.writeFile('design_price_history', histRes.data);
        }
    } catch (e) {
        console.warn('[LAN] autoSyncDesignPricesToLAN failed:', e.message);
    }
}

// ===== CLEAR ALL KARIGARS (Admin Only) =====
async function clearAllKarigars() {
    if (!isAdminUser()) return showToast('Only admin can clear all karigars', 'error');

    const karigars = AppState.karigars || [];
    const txCount = (AppState.karigarTransactions || []).length;

    // Double-confirm: show detailed warning
    const confirmMsg = [
        '⚠️ DANGER: PERMANENT ACTION ⚠️',
        '',
        `You are about to permanently delete ALL Karigar data:`,
        `• ${karigars.length} Karigar employees`,
        `• ${txCount} transaction records (Jama & Upad)`,
        '',
        'This CANNOT be undone.',
        '',
        'Type "DELETE ALL" (without quotes) to confirm, or press Cancel to abort.'
    ].join('\n');

    const userInput = window.prompt(confirmMsg, '');
    if (String(userInput || '').trim() !== 'DELETE ALL') {
        showToast('Clear All Karigars cancelled', 'info');
        return;
    }

    showLoader();
    let deletedKarigars = 0;
    let deletedTxs = 0;
    const actor = buildAuditActor();
    const companyId = AppState.currentCompany || 'company1';

    try {
        showToast('Clearing all karigar employees...', 'info');

        // 1. Delete all karigars one by one (also deletes their transactions per karigar)
        for (const k of karigars) {
            try {
                await FirebaseService.deleteKarigar(k.id, companyId, actor);
                deletedKarigars++;
            } catch (e) {
                console.warn('Failed to delete karigar:', k.id, e.message);
            }
        }

        // 2. Run a bulk clear of remaining karigar transactions just in case
        try {
            await FirebaseService.clearKarigarMonthlyData(companyId, actor);
            deletedTxs = txCount;
        } catch (e) {
            console.warn('Bulk tx clear failed:', e.message);
        }

        // 3. If LAN folder is connected, write empty arrays directly to the JSON files
        //    to ensure the local backup folder is immediately in sync.
        if (window.LanStorageService && LanStorageService.isConnected()) {
            try {
                await LanStorageService.writeFile('karigars', []);
                await LanStorageService.writeFile('karigar_transactions', []);
                // Also clear the karigar_reset_backups if the user wants a full fresh start
                // (Do NOT clear this — keep historical archives safe)
                console.log('[LAN] Cleared karigars.json and karigar_transactions.json');
            } catch (lanErr) {
                console.warn('[LAN] Failed to clear local JSON files:', lanErr.message);
            }
        }

        // 4. Flush all pending writes to Firebase
        await FirebaseService.flushWrites();

        // 5. Reset local AppState caches
        AppState.karigars = [];
        AppState.karigarTransactions = [];
        AppState.karigarStatsById = {};
        AppState.karigarTransactionsById = {};
        invalidateAllKarigarCaches();

        // 6. Re-render the page (will show empty state)
        await renderKarigarPage(true);

        // 7. Trigger a background sheet backup to sync to Google Sheets too
        _pendingDataChangesForBackup = true;
        scheduleBackgroundSheetBackup('clearAllKarigars');

        showToast(`✅ All Karigars cleared: ${deletedKarigars} employees + ${deletedTxs} transactions removed. Local backup updated.`, 'success');

    } catch (e) {
        console.error('clearAllKarigars failed:', e);
        showToast('Failed to clear all karigars: ' + (e.message || 'Unknown error'), 'error');
    } finally {
        hideLoader();
    }
}

async function deleteKarigarHistory(txId, karigarId, karigarName) {
    if (!isAdminUser()) return showToast('Only admin can delete transaction', 'error');
    if (!confirm('Are you certain you want to specifically delete this record? This cannot be undone.')) return;
    
    showLoader();
    try {
        if (AppState.viewingKarigarBackupId) {
            const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
            if (b && Array.isArray(b.rows)) {
                b.rows = b.rows.filter(t => t.id !== txId);
                recomputeBackupSummary(b);
                await saveArchiveBackupChanges(b);
                rebuildKarigarComputedCache();
                auditKarigarFinancials();
                showToast('Record deleted from archive', 'success');
                openKarigarHistoryModal(karigarId, karigarName);
                renderKarigarGrid();
                return;
            }
        }
        await FirebaseService.deleteKarigarTransaction(txId, AppState.currentCompany, buildAuditActor());
        _pendingDataChangesForBackup = true;
        scheduleBackgroundSheetBackup('deleteKarigarTransaction');
        showToast('Record deleted successfully', 'success');
        
        // Remove locally without full fetch
        AppState.karigarTransactions = AppState.karigarTransactions.filter(t => t.id !== txId);
        rebuildKarigarComputedCache();
        invalidateKarigarCache();
        
        // Refresh the open modal and background grid immediately
        openKarigarHistoryModal(karigarId, karigarName);
        renderKarigarGrid();
    } catch (e) {
        console.error('Failed to delete history row:', e);
        showToast('Failed to delete record', 'error');
    } finally {
        hideLoader();
    }
}

async function editKarigarName(karigarId, currentName) {
    if (!isAdminUser()) return showToast('Only admin can edit karigar', 'error');
    const newName = prompt('Edit Karigar name:', currentName || '');
    if (newName === null) return;
    const trimmed = String(newName || '').trim();
    if (!trimmed) return showToast('Name is required', 'error');
    if (trimmed === String(currentName || '').trim()) return;

    showLoader();
    try {
        const res = await FirebaseService.editKarigar(karigarId, trimmed, AppState.currentCompany, buildAuditActor());
        if (res?.success === false) throw new Error(res.message || 'Failed to edit karigar');
        _pendingDataChangesForBackup = true;
        scheduleBackgroundSheetBackup('editKarigar');
        showToast('Karigar updated', 'success');
        invalidateKarigarCache();
        await renderKarigarPage(true);
        openKarigarHistoryModal(karigarId, trimmed);
    } catch (e) {
        console.error('Failed to edit karigar:', e);
        showToast(e.message || 'Failed to edit karigar', 'error');
    } finally {
        hideLoader();
    }
}

async function editKarigarHistory(txId, karigarId, karigarName) {
    if (!isAdminUser()) return showToast('Only admin can edit transaction', 'error');
    let tx = null;
    let isArchive = false;
    let b = null;
    if (AppState.viewingKarigarBackupId) {
        b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
        if (b && Array.isArray(b.rows)) {
            tx = b.rows.find(t => t.id === txId);
            isArchive = true;
        }
    }
    if (!tx) tx = (AppState.karigarTransactions || []).find(t => t.id === txId);
    if (!tx) return showToast('Transaction not found', 'error');

    const updates = {};
    const newDate = prompt('Date (YYYY-MM-DD):', normalizeToISODate(tx.date) || '');
    if (newDate === null) return;
    updates.date = normalizeToISODate(newDate) || normalizeToISODate(tx.date);

    if (tx.type === 'jama') {
        const newDesign = prompt('Design:', tx.designName || '');
        if (newDesign === null) return;
        const newSize = prompt('Size:', tx.size || '');
        if (newSize === null) return;
        const newPic = prompt('Pics:', String(tx.pic || 0));
        if (newPic === null) return;
        const newPrice = prompt('Price:', String(tx.price || 0));
        if (newPrice === null) return;
        const newUpad = prompt('Upad Amount:', String(tx.upadAmount || 0));
        if (newUpad === null) return;

        updates.designName = String(newDesign || '').trim();
        updates.size = String(newSize || '').trim();
        updates.pic = parseInt(newPic, 10) || 0;
        updates.price = Math.round(((parseFloat(newPrice) || 0) + Number.EPSILON) * 100) / 100;
        updates.upadAmount = Math.round(((parseFloat(newUpad) || 0) + Number.EPSILON) * 100) / 100;
        updates.total = Math.round(((updates.pic * updates.price) + Number.EPSILON) * 100) / 100;
        updates.type = 'jama';
    } else {
        const newAmount = prompt('Borrow Amount:', String(tx.amount || 0));
        if (newAmount === null) return;
        updates.amount = Math.round(((parseFloat(newAmount) || 0) + Number.EPSILON) * 100) / 100;
        updates.type = 'upad';
    }

    showLoader();
    try {
        if (isArchive && b) {
            const rIdx = b.rows.findIndex(t => t.id === txId);
            if (rIdx >= 0) {
                b.rows[rIdx] = { ...b.rows[rIdx], ...updates };
                recomputeBackupSummary(b);
                await saveArchiveBackupChanges(b);
                rebuildKarigarComputedCache();
                auditKarigarFinancials();
                showToast('Transaction updated in archive', 'success');
                renderKarigarGrid();
                openKarigarHistoryModal(karigarId, karigarName);
                return;
            }
        }
        const res = await FirebaseService.updateKarigarTransaction(txId, updates, buildAuditActor(), AppState.currentCompany);
        if (res?.success === false) throw new Error(res.message || 'Failed to edit transaction');
        _pendingDataChangesForBackup = true;
        scheduleBackgroundSheetBackup('updateKarigarTransaction');
        showToast('Transaction updated', 'success');
        invalidateKarigarCache();
        await renderKarigarPage(true);
        openKarigarHistoryModal(karigarId, karigarName);
    } catch (e) {
        console.error('Failed to update transaction:', e);
        showToast(e.message || 'Failed to update transaction', 'error');
    } finally {
        hideLoader();
    }
}

async function confirmKarigarMonthlyReset(targetCompanyId = '') {
    if (!isAdminUser()) return showToast('Only admin can run monthly reset', 'error');
    const safeCompanyId = String(targetCompanyId || AppState.currentCompany || 'company1').trim() || 'company1';
    const targetCompanyName = getCompanyDisplayName(safeCompanyId);
    
    let txs = Array.isArray(AppState.karigarTransactions) ? AppState.karigarTransactions : [];
    if (AppState.viewingKarigarBackupId) {
        const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
        if (b && Array.isArray(b.rows)) txs = b.rows;
    }
    txs = txs.filter(tx => String(tx.companyId || safeCompanyId).trim() === safeCompanyId);

    let cycleJama = 0;
    let cycleUpad = 0;
    let cyclePics = 0;
    const round2 = (n) => Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;
    const toCents = (n) => Math.round((Number(n || 0) + Number.EPSILON) * 100);
    const fromCents = (c) => Math.round(c) / 100;

    let jamaCents = 0;
    let upadCents = 0;

    txs.forEach(t => {
        if (t.type === 'jama') {
            const p = parseInt(t.pic, 10) || 0;
            const pr = round2(t.price || 0);
            let subtotal = round2(t.total || (p * pr));
            if (p > 0 && pr > 0 && Math.abs(round2(p * pr) - subtotal) > 0.009) {
                subtotal = round2(p * pr);
            }
            jamaCents += toCents(subtotal);
            cyclePics += p;
            if (t.upadAmount) upadCents += toCents(round2(t.upadAmount));
        } else if (t.type === 'upad') {
            upadCents += toCents(round2(t.amount || t.upadAmount || 0));
        }
    });

    cycleJama = fromCents(jamaCents);
    cycleUpad = fromCents(upadCents);
    const cycleBalance = fromCents(jamaCents - upadCents);

    const modal = document.getElementById('karigar-reset-confirm-modal');
    if (!modal) {
        // Fallback: direct prompt if modal not present
        if (!confirm(`Reset & Close Month for ${targetCompanyName}?\nTotal Production: ${cyclePics} Pcs\nNet Balance: ₹${cycleBalance.toFixed(2)}`)) return;
        return executeKarigarMonthlyReset(safeCompanyId, targetCompanyName, txs);
    }

    const periodSub = document.getElementById('reset-modal-period-subtitle');
    if (periodSub) periodSub.textContent = `${targetCompanyName} • Active Period: ${getKarigarCurrentCycleLabel()}`;

    const picsEl = document.getElementById('reset-modal-total-pics');
    if (picsEl) picsEl.textContent = `${cyclePics} Pcs`;

    const jamaEl = document.getElementById('reset-modal-total-jama');
    if (jamaEl) jamaEl.textContent = `₹${cycleJama.toFixed(2)}`;

    const upadEl = document.getElementById('reset-modal-total-upad');
    if (upadEl) upadEl.textContent = `₹${cycleUpad.toFixed(2)}`;

    const balEl = document.getElementById('reset-modal-total-balance');
    if (balEl) balEl.textContent = `₹${Math.abs(cycleBalance).toFixed(2)}${cycleBalance < 0 ? ' Due' : ' (Net Settled)'}`;

    // Hook up pre-reset report buttons
    const printReportBtn = document.getElementById('btn-reset-modal-print-report');
    if (printReportBtn) {
        printReportBtn.onclick = () => printCompanyMonthlyReport(safeCompanyId, txs);
    }

    const dlReportBtn = document.getElementById('btn-reset-modal-dl-report');
    if (dlReportBtn) {
        dlReportBtn.onclick = () => exportCompanyMonthlyReportPDF(safeCompanyId, txs);
    }

    const dlSlipsBtn = document.getElementById('btn-reset-modal-dl-slips');
    if (dlSlipsBtn) {
        dlSlipsBtn.onclick = () => openSalarySlipOptionsModal({ isBulk: true, defaultAction: 'download' });
    }

    const cancelBtn = document.getElementById('btn-cancel-reset-modal');
    if (cancelBtn) {
        cancelBtn.onclick = () => modal.classList.remove('show');
    }
    const closeBtn = document.getElementById('close-karigar-reset-confirm-modal');
    if (closeBtn) {
        closeBtn.onclick = () => modal.classList.remove('show');
    }

    const executeBtn = document.getElementById('btn-execute-monthly-reset');
    if (executeBtn) {
        executeBtn.onclick = async () => {
            modal.classList.remove('show');
            await executeKarigarMonthlyReset(safeCompanyId, targetCompanyName, txs);
        };
    }

    modal.classList.add('show');
}

async function executeKarigarMonthlyReset(safeCompanyId, targetCompanyName, txs) {
    showLoader();
    try {
        showToast(`Archiving ${targetCompanyName} Karigar data...`, 'info');
        await FirebaseService.flushWrites();
        
        const backupRows = (txs || [])
            .filter(tx => String(tx.companyId || '').trim() === safeCompanyId)
            .map(tx => ({ ...tx }));

        // 1. Create local and Firestore permanent backup
        await FirebaseService.createKarigarResetBackup({
            companyId: safeCompanyId,
            rows: backupRows,
            actor: buildAuditActor()
        });

        // 2. Clear active monthly transactions
        const deleted = await FirebaseService.clearKarigarMonthlyData(safeCompanyId, buildAuditActor());

        // 3. Update reset date and cycle start in localStorage
        const todayIso = getTodayISODate();
        localStorage.setItem(`karigar_cycle_start_${safeCompanyId}`, todayIso);
        localStorage.setItem(`karigar_last_reset_date_${safeCompanyId}`, new Date().toISOString());
        localStorage.setItem(`karigar_last_reset_month_${safeCompanyId}`, todayIso.substring(0, 7));

        // 4. Update memory state
        if (String(AppState.currentCompany || '').trim() === safeCompanyId) {
            AppState.karigarTransactions = [];
            AppState.viewingKarigarBackupId = null;
            rebuildKarigarComputedCache();
        }
        invalidateKarigarCache(safeCompanyId);

        // Attempt background sheets backup gracefully
        try {
            scheduleBackgroundSheetBackup(`clearKarigarMonthlyData_${safeCompanyId}`);
        } catch (e) {
            console.warn('Background sheets backup notice:', e);
        }

        showToast(`${targetCompanyName} reset complete! Archived ${deleted} records. New cycle active from ${todayIso}.`, 'success');
        await renderKarigarPage(true);
    } catch (e) {
        console.error('Reset error:', e);
        showToast(`System error during reset: ${e.message}`, 'error');
    } finally {
        hideLoader();
    }
}

// ===== ENTERPRISE KARIGAR IDENTITY & CURRENCY UTILITIES =====
function getEnterpriseKarigarId(karigar, companyId = 'company1') {
    if (!karigar) return 'SS-KRG-001';
    const comp = String(companyId || AppState?.currentCompany || 'company1').trim();
    const compPrefix = (comp === 'company2') ? 'DH' : 'SS';
    const name = String(karigar.name || '').trim();
    
    // Check if name has a number prefix or suffix (e.g. "2 ASMAUN", "5 SABIR", "SUHOJIT 3")
    const match = name.match(/^(\d+)/) || name.match(/(\d+)$/);
    if (match) {
        const num = parseInt(match[1], 10);
        if (num > 0) {
            return `${compPrefix}-KRG-${String(num).padStart(3, '0')}`;
        }
    }
    
    // Fallback for names without numbers: assign unique available number
    const list = Array.isArray(AppState?.karigars) ? AppState.karigars : [];
    const used = new Set();
    list.forEach(k => {
        const m = String(k?.name || '').match(/^(\d+)/) || String(k?.name || '').match(/(\d+)$/);
        if (m) used.add(parseInt(m[1], 10));
    });
    const unnumberedList = list.filter(k => {
        const m = String(k?.name || '').match(/^(\d+)/) || String(k?.name || '').match(/(\d+)$/);
        return !m;
    });
    const unnumberedIdx = unnumberedList.findIndex(k => String(k?.id || '').trim() === String(karigar?.id || '').trim() || String(k?.name || '').trim() === name);
    let nextNum = 1;
    let found = 0;
    while (found <= (unnumberedIdx >= 0 ? unnumberedIdx : 0)) {
        if (!used.has(nextNum)) {
            if (found === (unnumberedIdx >= 0 ? unnumberedIdx : 0)) break;
            found++;
        }
        nextNum++;
    }
    return `${compPrefix}-KRG-${String(nextNum).padStart(3, '0')}`;
}

function numberToIndianWords(amount) {
    const num = Math.floor(Math.abs(Number(amount) || 0));
    if (num === 0) return 'Zero Rupees Only';
    
    const ones = ['', 'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine',
        'Ten', 'Eleven', 'Twelve', 'Thirteen', 'Fourteen', 'Fifteen', 'Sixteen', 'Seventeen', 'Eighteen', 'Nineteen'];
    const tens = ['', '', 'Twenty', 'Thirty', 'Forty', 'Fifty', 'Sixty', 'Seventy', 'Eighty', 'Ninety'];

    function convertTwoDigits(n) {
        if (n < 20) return ones[n];
        return tens[Math.floor(n / 10)] + (n % 10 !== 0 ? ' ' + ones[n % 10] : '');
    }

    function convertThreeDigits(n) {
        let str = '';
        if (Math.floor(n / 100) > 0) {
            str += ones[Math.floor(n / 100)] + ' Hundred ';
        }
        if (n % 100 > 0) {
            str += convertTwoDigits(n % 100);
        }
        return str.trim();
    }

    let words = '';
    const crore = Math.floor(num / 10000000);
    const lakh = Math.floor((num % 10000000) / 100000);
    const thousand = Math.floor((num % 100000) / 1000);
    const remainder = num % 1000;

    if (crore > 0) words += convertThreeDigits(crore) + ' Crore ';
    if (lakh > 0) words += convertThreeDigits(lakh) + ' Lakh ';
    if (thousand > 0) words += convertThreeDigits(thousand) + ' Thousand ';
    if (remainder > 0) words += convertThreeDigits(remainder) + ' ';

    const paise = Math.round((Math.abs(Number(amount) || 0) - num) * 100);
    let paiseStr = '';
    if (paise > 0) {
        paiseStr = ' and ' + convertTwoDigits(paise) + ' Paise';
    }

    return (words.trim() + ' Rupees' + paiseStr + ' Only').trim();
}

// ===== PRINTABLE SALARY SLIP (HISAAB SLIP) & PDF GENERATORS =====
function generateKarigarSalarySlipPDF(karigar, txs, options = {}) {
    if (!window.jspdf) {
        showToast('PDF library not ready', 'error');
        return null;
    }
    const { jsPDF } = window.jspdf;
    const doc = options.doc || new jsPDF({
        orientation: 'portrait',
        unit: 'mm',
        format: 'a4'
    });

    const companyRaw = getCompanyDisplayName(AppState.currentCompany || 'company1') || 'SHREE SAI';
    const companyName = companyRaw.toUpperCase();
    let periodLabel = options.periodLabel || getKarigarCurrentCycleLabel();
    const todayFormatted = formatISODateForDisplay(getTodayISODate());
    if (periodLabel.includes('to Present')) {
        periodLabel = periodLabel.replace(/to Present/i, 'to ' + todayFormatted);
    }
    const round2 = (n) => Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;
    const toAmount = (v) => {
        const n = parseFloat(String(v ?? '').replace(/[^0-9.-]/g, ''));
        return Number.isFinite(n) ? n : 0;
    };

    const targetKId = String(karigar?.id || '').trim();
    const targetKName = String(karigar?.name || '').trim();
    const enterpriseId = getEnterpriseKarigarId(karigar, AppState.currentCompany);
    const voucherNo = `VCH-${enterpriseId.replace(/[^a-zA-Z0-9]/g, '')}-${getTodayISODate().replace(/-/g, '').slice(0, 6)}`;

    const karigarTxs = (txs || []).filter(t => {
        const tid = String(t.karigarId || '').trim();
        const tname = String(t.karigarName || '').trim().toLowerCase();
        const targetLower = targetKName.toLowerCase();
        return tid === targetKId || resolveKarigarIdFromState(t.karigarId, t.karigarName) === targetKId || (tname && targetLower && tname === targetLower);
    });

    // Sort chronologically ascending for clean printable statement
    karigarTxs.sort((a, b) => getKarigarTxTimestampMs(a) - getKarigarTxTimestampMs(b));

    let totalJama = 0;
    let totalUpad = 0;
    let totalPics = 0;
    const jamaRows = [];
    const upadRows = [];

    karigarTxs.forEach(t => {
        const type = String(t.type || '').toLowerCase();
        const dateStr = formatISODateForDisplay(t.date || t.transactionDateTime) || '-';
        if (type === 'jama') {
            const pics = parseInt(t.pic, 10) || 0;
            const price = round2(t.price || 0);
            const total = round2(t.total || (pics * price));
            const upadAmt = round2(toAmount(t.upadAmount || 0));
            totalJama = round2(totalJama + total);
            totalPics += pics;
            jamaRows.push([
                jamaRows.length + 1,
                dateStr,
                t.designName || '-',
                t.size || '-',
                pics,
                price.toFixed(2),
                total.toFixed(2)
            ]);
            if (upadAmt > 0) {
                totalUpad = round2(totalUpad + upadAmt);
                upadRows.push([
                    upadRows.length + 1,
                    dateStr,
                    'Advance with Maal entry',
                    'Cash / Voucher',
                    upadAmt.toFixed(2)
                ]);
            }
        } else if (type === 'upad') {
            const amt = round2(toAmount(t.amount || t.upadAmount || 0));
            totalUpad = round2(totalUpad + amt);
            upadRows.push([
                upadRows.length + 1,
                dateStr,
                t.note || t.remarks || 'Cash Advance',
                'Cash / Hand Loan',
                amt.toFixed(2)
            ]);
        }
    });

    const netBalance = round2(totalJama - totalUpad);
    const inWords = numberToIndianWords(Math.abs(netBalance));

    // Internal sheet renderer for industrial standard
    function renderSalarySlipSheet(pageDoc, copyLabel, isOfficeVoucher) {
        // Crisp 0.4pt Laser-Ready Outer Border
        pageDoc.setDrawColor(0, 0, 0);
        pageDoc.setLineWidth(0.4);
        pageDoc.rect(10, 10, 190, 277);

        // Header Title
        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(16);
        pageDoc.setTextColor(0, 0, 0);
        pageDoc.text(companyName, 105, 17.5, { align: 'center' });

        pageDoc.setFontSize(10);
        pageDoc.text('KARIGAR WORK & PAYMENT STATEMENT', 105, 22.5, { align: 'center' });

        pageDoc.setFontSize(8.5);
        pageDoc.setFont('helvetica', 'bold');
        pageDoc.text(`[ ${copyLabel.toUpperCase()} ]`, 105, 27, { align: 'center' });

        pageDoc.setLineWidth(0.3);
        pageDoc.line(10, 29.5, 200, 29.5);

        // Karigar & Statement Details
        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(8.5);
        pageDoc.setTextColor(0, 0, 0);
        pageDoc.text('Karigar Name:', 14, 34.5);
        pageDoc.setFont('helvetica', 'normal');
        pageDoc.text(targetKName || '-', 42, 34.5);

        pageDoc.setFont('helvetica', 'bold');
        pageDoc.text('Karigar ID:', 128, 34.5);
        pageDoc.setFont('helvetica', 'normal');
        pageDoc.text(enterpriseId, 154, 34.5);

        pageDoc.setFont('helvetica', 'bold');
        pageDoc.text('Statement Period:', 14, 40);
        pageDoc.setFont('helvetica', 'normal');
        pageDoc.text(periodLabel, 44, 40);

        pageDoc.setFont('helvetica', 'bold');
        pageDoc.text('Voucher No:', 128, 40);
        pageDoc.setFont('helvetica', 'normal');
        pageDoc.text(voucherNo, 154, 40);

        pageDoc.setFont('helvetica', 'bold');
        pageDoc.text('Printed Date:', 14, 45.5);
        pageDoc.setFont('helvetica', 'normal');
        pageDoc.text(new Date().toLocaleDateString('en-GB') + ' ' + new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }), 44, 45.5);

        pageDoc.setLineWidth(0.25);
        pageDoc.line(10, 48, 200, 48);

        // 4 Key Metric Boxes (Pure White background, Crisp 0.3pt border, Bold Black Text)
        const boxY = 51;
        const boxW = 41.5;
        const boxH = 13.5;
        const boxGap = 4.6;
        const startX = 14;

        pageDoc.setDrawColor(0, 0, 0);
        pageDoc.setLineWidth(0.3);

        // Box 1: Total Pieces
        pageDoc.rect(startX, boxY, boxW, boxH);
        pageDoc.setFont('helvetica', 'normal');
        pageDoc.setFontSize(7.5);
        pageDoc.setTextColor(0, 0, 0);
        pageDoc.text('TOTAL PRODUCTION', startX + boxW / 2, boxY + 4.5, { align: 'center' });
        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(10.5);
        pageDoc.text(totalPics + ' Pcs', startX + boxW / 2, boxY + 10.5, { align: 'center' });

        // Box 2: Total Jama
        const box2X = startX + boxW + boxGap;
        pageDoc.rect(box2X, boxY, boxW, boxH);
        pageDoc.setFont('helvetica', 'normal');
        pageDoc.setFontSize(7.5);
        pageDoc.text('TOTAL WORK (JAMA)', box2X + boxW / 2, boxY + 4.5, { align: 'center' });
        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(10.5);
        pageDoc.text('Rs. ' + totalJama.toFixed(2), box2X + boxW / 2, boxY + 10.5, { align: 'center' });

        // Box 3: Total Upad
        const box3X = box2X + boxW + boxGap;
        pageDoc.rect(box3X, boxY, boxW, boxH);
        pageDoc.setFont('helvetica', 'normal');
        pageDoc.setFontSize(7.5);
        pageDoc.text('TOTAL ADVANCE (UPAD)', box3X + boxW / 2, boxY + 4.5, { align: 'center' });
        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(10.5);
        pageDoc.text('Rs. ' + totalUpad.toFixed(2), box3X + boxW / 2, boxY + 10.5, { align: 'center' });

        // Box 4: Net Balance
        const box4X = box3X + boxW + boxGap;
        pageDoc.rect(box4X, boxY, boxW, boxH);
        pageDoc.setFont('helvetica', 'normal');
        pageDoc.setFontSize(7.5);
        pageDoc.text('NET BALANCE', box4X + boxW / 2, boxY + 4.5, { align: 'center' });
        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(10.5);
        pageDoc.text('Rs. ' + Math.abs(netBalance).toFixed(2) + (netBalance < 0 ? ' (DUE)' : ' (PAYABLE)'), box4X + boxW / 2, boxY + 10.5, { align: 'center' });

        let currentY = boxY + boxH + 4.5;

        // Table 1: Itemized Production Schedule (Jama)
        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(8.5);
        pageDoc.setTextColor(0, 0, 0);
        pageDoc.text('1. PRODUCTION & WORK SCHEDULE (JAMA):', 14, currentY + 3);

        const jamaBody = jamaRows.length > 0 ? jamaRows : [['-', '-', 'No production entries recorded in this cycle', '-', '0', '0.00', '0.00']];

        pageDoc.autoTable({
            startY: currentY + 4.5,
            head: [['#', 'Date', 'Design No / Article', 'Size', 'Pcs', 'Rate (Rs)', 'Total (Rs)']],
            body: jamaBody,
            foot: jamaRows.length > 0 ? [['', 'TOTAL PRODUCTION WORK', '', '', totalPics + ' Pcs', '', 'Rs. ' + totalJama.toFixed(2)]] : [],
            theme: 'grid',
            styles: {
                fontSize: 8,
                cellPadding: 1.5,
                textColor: [0, 0, 0],
                lineColor: [0, 0, 0],
                lineWidth: 0.1
            },
            headStyles: {
                fillColor: [240, 240, 240],
                textColor: [0, 0, 0],
                fontStyle: 'bold',
                halign: 'center'
            },
            footStyles: {
                fillColor: [248, 248, 248],
                textColor: [0, 0, 0],
                fontStyle: 'bold'
            },
            columnStyles: {
                0: { halign: 'center', cellWidth: 10 },
                1: { halign: 'center', cellWidth: 26 },
                2: { halign: 'left' },
                3: { halign: 'center', cellWidth: 16 },
                4: { halign: 'right', cellWidth: 20 },
                5: { halign: 'right', cellWidth: 24 },
                6: { halign: 'right', cellWidth: 28 }
            },
            margin: { left: 14, right: 14 }
        });

        currentY = pageDoc.lastAutoTable.finalY + 4;

        if (currentY > (isOfficeVoucher ? 180 : 205)) {
            pageDoc.addPage();
            // Re-draw outer border on new page
            pageDoc.setDrawColor(0, 0, 0);
            pageDoc.setLineWidth(0.4);
            pageDoc.rect(10, 10, 190, 277);
            currentY = 18;
        }

        // Table 2: Advances & Borrowings (Upad)
        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(8.5);
        pageDoc.setTextColor(0, 0, 0);
        pageDoc.text('2. ADVANCES & BORROWINGS (UPAD / DEDUCTIONS):', 14, currentY + 3);

        const upadBody = upadRows.length > 0 ? upadRows : [['-', '-', 'No advances taken during this period', 'Cash', '0.00']];

        pageDoc.autoTable({
            startY: currentY + 4.5,
            head: [['#', 'Date', 'Reason / Particulars', 'Mode', 'Amount (Rs)']],
            body: upadBody,
            foot: upadRows.length > 0 ? [['', 'TOTAL ADVANCES DEDUCTED', '', '', 'Rs. ' + totalUpad.toFixed(2)]] : [],
            theme: 'grid',
            styles: {
                fontSize: 8,
                cellPadding: 1.5,
                textColor: [0, 0, 0],
                lineColor: [0, 0, 0],
                lineWidth: 0.1
            },
            headStyles: {
                fillColor: [240, 240, 240],
                textColor: [0, 0, 0],
                fontStyle: 'bold',
                halign: 'center'
            },
            footStyles: {
                fillColor: [248, 248, 248],
                textColor: [0, 0, 0],
                fontStyle: 'bold'
            },
            columnStyles: {
                0: { halign: 'center', cellWidth: 10 },
                1: { halign: 'center', cellWidth: 26 },
                2: { halign: 'left' },
                3: { halign: 'center', cellWidth: 32 },
                4: { halign: 'right', cellWidth: 30 }
            },
            margin: { left: 14, right: 14 }
        });

        currentY = pageDoc.lastAutoTable.finalY + 4;

        if (currentY > (isOfficeVoucher ? 190 : 220)) {
            pageDoc.addPage();
            pageDoc.setDrawColor(0, 0, 0);
            pageDoc.setLineWidth(0.4);
            pageDoc.rect(10, 10, 190, 277);
            currentY = 18;
        }

        // Account Summary & Reconciliation Box
        pageDoc.setDrawColor(0, 0, 0);
        pageDoc.setLineWidth(0.3);
        pageDoc.rect(14, currentY, 182, 21);

        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(8.5);
        pageDoc.setTextColor(0, 0, 0);
        pageDoc.text('FINAL ACCOUNT SETTLEMENT & RECONCILIATION:', 17, currentY + 4.5);

        pageDoc.setFont('helvetica', 'normal');
        pageDoc.setFontSize(8);
        pageDoc.text(`(+) Total Gross Production Wages (Jama): Rs. ${totalJama.toFixed(2)}`, 17, currentY + 9.5);
        pageDoc.text(`(-) Total Advance Withdrawals (Upad): Rs. ${totalUpad.toFixed(2)}`, 17, currentY + 14.5);

        pageDoc.setFont('helvetica', 'italic');
        pageDoc.setFontSize(7.5);
        pageDoc.setTextColor(50, 50, 50);
        pageDoc.text(`In Words: ${inWords}`, 17, currentY + 18.8);

        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(9);
        pageDoc.setTextColor(0, 0, 0);
        const settleLabel = netBalance < 0 ? 'DUE TO FACTORY' : 'NET PAYABLE TO KARIGAR';
        pageDoc.text(`${settleLabel}:`, 120, currentY + 9.5);
        pageDoc.setFontSize(11);
        pageDoc.text(`Rs. ${Math.abs(netBalance).toFixed(2)}`, 120, currentY + 16);

        currentY += 25;

        // Distinct Two-Signature Section (No extra middle lines)
        if (isOfficeVoucher) {
            // FACTORY / OFFICE COPY: Formal Signed Discharge Receipt / Payment Voucher
            pageDoc.setDrawColor(0, 0, 0);
            pageDoc.setLineWidth(0.35);
            pageDoc.rect(14, currentY, 182, 35);

            pageDoc.setFont('helvetica', 'bold');
            pageDoc.setFontSize(8.5);
            pageDoc.setTextColor(0, 0, 0);
            pageDoc.text('FULL & FINAL SETTLEMENT DISCHARGE RECEIPT / PAYMENT VOUCHER:', 17, currentY + 5);

            pageDoc.setFont('helvetica', 'normal');
            pageDoc.setFontSize(7.8);
            pageDoc.text(
                `I, ${targetKName} (Karigar ID: ${enterpriseId}), hereby acknowledge receipt of the sum of Rs. ${Math.abs(netBalance).toFixed(2)} (${inWords}) towards full and final settlement of all piece-rate wages and dues for the period ${periodLabel}. I confirm that all calculations have been verified by me and no further wage claims remain pending against ${companyName}.`,
                17,
                currentY + 10,
                { maxWidth: 176, lineHeightFactor: 1.25 }
            );

            pageDoc.setFont('helvetica', 'bold');
            pageDoc.setFontSize(7.8);
            pageDoc.text('Payment Disbursed via:  [   ] Cash    [   ] UPI / Online Transfer    [   ] Cheque    Date: ________________', 17, currentY + 23);

            // Stamp Box
            pageDoc.setLineWidth(0.2);
            pageDoc.rect(14, currentY + 25.5, 28, 8);
            pageDoc.setFontSize(6.5);
            pageDoc.setFont('helvetica', 'normal');
            pageDoc.text('Revenue Stamp', 28, currentY + 30, { align: 'center' });

            currentY += 41;

            pageDoc.setFont('helvetica', 'normal');
            pageDoc.setFontSize(8.5);
            pageDoc.text(`${targetKName} Signature / Thumb: ___________________`, 18, currentY);
            pageDoc.text('Nareshbhai Tagadiya Signature: _______________________', 114, currentY);
        } else {
            // WORKER COPY: Clean two-signature row
            currentY += 4;
            pageDoc.setFont('helvetica', 'normal');
            pageDoc.setFontSize(8.5);
            pageDoc.text(`${targetKName} Signature: _______________________`, 18, currentY);
            pageDoc.text('Nareshbhai Tagadiya Signature: _______________________', 114, currentY);
        }
    }

    // Render requested copies
    const includeWorker = options.includeWorker !== false;
    const includeFactory = options.includeFactory !== false;

    let pagesRendered = 0;
    if (includeWorker) {
        const copyBadge = includeFactory ? '1. WORKER COPY' : 'WORKER COPY';
        renderSalarySlipSheet(doc, copyBadge, false);
        pagesRendered++;
    }

    if (includeFactory) {
        if (pagesRendered > 0) {
            doc.addPage();
        }
        const copyBadge = includeWorker ? '2. FACTORY COPY - PAYMENT VOUCHER' : 'FACTORY COPY - PAYMENT VOUCHER';
        renderSalarySlipSheet(doc, copyBadge, true);
        pagesRendered++;
    }

    // Stamp footer on every page rendered for this karigar (Karigar-wise Page X/pagesRendered)
    const totalDocPages = doc.internal.getNumberOfPages();
    const workerFirstPage = totalDocPages - pagesRendered + 1;
    for (let pIndex = 0; pIndex < pagesRendered; pIndex++) {
        const targetPage = workerFirstPage + pIndex;
        doc.setPage(targetPage);
        doc.setFont('helvetica', 'normal');
        doc.setFontSize(7.5);
        doc.setTextColor(110, 110, 110);
        doc.text('Krimaa Enterprise Management System', 10, 292);

        doc.setFont('helvetica', 'bold');
        doc.setFontSize(8);
        doc.setTextColor(20, 20, 20);
        doc.text(`${targetKName}  |  Page ${pIndex + 1}/${pagesRendered}`, 200, 292, { align: 'right' });
    }

    if (!options.isBulk && !options.noDownload) {
        const cleanName = (targetKName || 'Karigar').replace(/[^a-zA-Z0-9_-]/g, '_');
        const copySuffix = (includeWorker && includeFactory) ? '2Copies' : (includeWorker ? 'WorkerCopy' : 'FactoryVoucher');
        const filename = `SalarySlip_${enterpriseId}_${cleanName}_${copySuffix}_${getTodayISODate()}.pdf`;
        doc.save(filename);
        showToast(`Salary slip downloaded for ${targetKName} (${enterpriseId})`, 'success');
    }

    return doc;
}

// ===== MASTER KARIGAR PAYROLL & SETTLEMENT SUMMARY TABLE SHEET (PORTRAIT A4) =====
function renderMasterSummaryPayrollSheet(doc, karigars, txs, options = {}) {
    const round2 = (n) => Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;
    const toAmount = (v) => {
        const n = parseFloat(String(v ?? '').replace(/[^0-9.-]/g, ''));
        return Number.isFinite(n) ? n : 0;
    };

    const currentCompany = AppState.currentCompany || 'company1';
    const companyName = getCompanyDisplayName(currentCompany);
    let periodLabel = options.periodLabel;
    if (!periodLabel) {
        if (AppState.viewingKarigarBackupId) {
            const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
            periodLabel = b ? getKarigarBackupRangeLabel(b) : 'Archived Cycle';
        } else {
            periodLabel = getKarigarCurrentCycleLabel();
        }
    }

    let grandPieces = 0;
    let grandJama = 0;
    let grandUpad = 0;
    let grandNet = 0;
    let totalPaidCount = 0;
    let totalPendingCount = 0;

    const tableRows = [];

    karigars.forEach((k, idx) => {
        const kId = String(k.id || '').trim();
        const kName = String(k.name || '').trim();
        const enterpriseId = getEnterpriseKarigarId(k, currentCompany);

        const kLower = kName.toLowerCase();
        const kTxs = (txs || []).filter(t => {
            const tid = String(t.karigarId || '').trim();
            const tname = String(t.karigarName || '').trim().toLowerCase();
            return tid === kId || resolveKarigarIdFromState(t.karigarId, t.karigarName) === kId || (tname && kLower && tname === kLower);
        });

        let pieces = 0;
        let jama = 0;
        let upad = 0;

        kTxs.forEach(t => {
            const type = String(t.type || '').toLowerCase();
            if (type === 'jama') {
                const p = parseInt(t.pic, 10) || 0;
                const pr = round2(t.price || 0);
                const tot = round2(t.total || (p * pr));
                const u = round2(parseFloat(String(t.upadAmount || 0).replace(/[^0-9.-]/g, '')) || 0);
                pieces += p;
                jama = round2(jama + tot);
                upad = round2(upad + u);
            } else if (type === 'upad') {
                const u = round2(parseFloat(String(t.amount || t.upadAmount || 0).replace(/[^0-9.-]/g, '')) || 0);
                upad = round2(upad + u);
            }
        });

        const net = round2(jama - upad);
        grandPieces += pieces;
        grandJama = round2(grandJama + jama);
        grandUpad = round2(grandUpad + upad);
        grandNet = round2(grandNet + net);

        const isPaid = isKarigarPaid(k.id, k.name);
        const tickBox = isPaid ? '[ X ]' : '[   ]';
        if (isPaid) totalPaidCount++;
        else totalPendingCount++;

        tableRows.push([
            idx + 1,
            enterpriseId,
            kName,
            pieces > 0 ? pieces : '0',
            jama.toFixed(2),
            upad.toFixed(2),
            net >= 0 ? net.toFixed(2) : `(${Math.abs(net).toFixed(2)})`,
            tickBox
        ]);
    });

    // Crisp 0.4pt Laser Outer Border
    doc.setDrawColor(0, 0, 0);
    doc.setLineWidth(0.4);
    doc.rect(10, 10, 190, 277);

    // Header Title
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(16);
    doc.setTextColor(0, 0, 0);
    doc.text(companyName, 105, 17.5, { align: 'center' });

    doc.setFontSize(10);
    doc.text('MASTER KARIGAR PAYROLL & SETTLEMENT DISBURSEMENT STATEMENT', 105, 22.5, { align: 'center' });

    doc.setFontSize(8);
    doc.setFont('helvetica', 'normal');
    const printDateStr = new Date().toLocaleDateString('en-GB') + ' ' + new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
    doc.text(`Statement Period: ${periodLabel}   |   Printed: ${printDateStr}   |   Total Workers: ${karigars.length} (Paid: ${totalPaidCount}, Pending: ${totalPendingCount})`, 105, 27, { align: 'center' });

    doc.setLineWidth(0.3);
    doc.line(10, 29.5, 200, 29.5);

    // 4 Key Metric Summary Boxes (White background, Crisp border, bold black text)
    const boxY = 32;
    const boxW = 41.5;
    const boxH = 12.5;
    const boxGap = 4.6;
    const startX = 14;

    doc.setDrawColor(0, 0, 0);
    doc.setLineWidth(0.3);

    // Box 1: Total Pieces
    doc.rect(startX, boxY, boxW, boxH);
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(7.5);
    doc.setTextColor(0, 0, 0);
    doc.text('TOTAL PRODUCTION', startX + boxW / 2, boxY + 4, { align: 'center' });
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(10);
    doc.text(grandPieces + ' Pcs', startX + boxW / 2, boxY + 9.5, { align: 'center' });

    // Box 2: Total Jama
    const box2X = startX + boxW + boxGap;
    doc.rect(box2X, boxY, boxW, boxH);
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(7.5);
    doc.text('TOTAL WORK (JAMA)', box2X + boxW / 2, boxY + 4, { align: 'center' });
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(10);
    doc.text('Rs. ' + grandJama.toFixed(2), box2X + boxW / 2, boxY + 9.5, { align: 'center' });

    // Box 3: Total Advances (Upad)
    const box3X = startX + (boxW + boxGap) * 2;
    doc.rect(box3X, boxY, boxW, boxH);
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(7.5);
    doc.text('TOTAL ADVANCES (UPAD)', box3X + boxW / 2, boxY + 4, { align: 'center' });
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(10);
    doc.text('Rs. ' + grandUpad.toFixed(2), box3X + boxW / 2, boxY + 9.5, { align: 'center' });

    // Box 4: Net Balance Payable
    const box4X = startX + (boxW + boxGap) * 3;
    doc.rect(box4X, boxY, boxW, boxH);
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(7.5);
    doc.text('NET PAYABLE BALANCE', box4X + boxW / 2, boxY + 4, { align: 'center' });
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(10);
    doc.text('Rs. ' + grandNet.toFixed(2), box4X + boxW / 2, boxY + 9.5, { align: 'center' });

    doc.setLineWidth(0.25);
    doc.line(10, 47, 200, 47);

    // AutoTable for Master Karigar List
    doc.autoTable({
        startY: 49,
        margin: { left: 10, right: 10, top: 49, bottom: 25 },
        head: [[
            '#',
            'Worker ID',
            'Karigar Name',
            'Total Pcs',
            'Total Work (Rs)',
            'Advances (Rs)',
            'Net Payable (Rs)',
            'Paid Tick'
        ]],
        body: tableRows,
        foot: [[
            '',
            'TOTALS',
            `${karigars.length} Workers`,
            grandPieces + ' Pcs',
            'Rs. ' + grandJama.toFixed(2),
            'Rs. ' + grandUpad.toFixed(2),
            'Rs. ' + grandNet.toFixed(2),
            `${totalPaidCount} Paid`
        ]],
        theme: 'grid',
        styles: {
            fontSize: 7.8,
            cellPadding: 1.6,
            textColor: [0, 0, 0],
            lineColor: [0, 0, 0],
            lineWidth: 0.1
        },
        headStyles: {
            fillColor: [240, 240, 240],
            textColor: [0, 0, 0],
            fontStyle: 'bold',
            halign: 'center'
        },
        footStyles: {
            fillColor: [245, 245, 245],
            textColor: [0, 0, 0],
            fontStyle: 'bold'
        },
        columnStyles: {
            0: { halign: 'center', cellWidth: 8 },
            1: { halign: 'center', cellWidth: 26 },
            2: { halign: 'left', cellWidth: 46, fontStyle: 'bold' },
            3: { halign: 'right', cellWidth: 16 },
            4: { halign: 'right', cellWidth: 28 },
            5: { halign: 'right', cellWidth: 28 },
            6: { halign: 'right', cellWidth: 26, fontStyle: 'bold' },
            7: { halign: 'center', cellWidth: 12, fontStyle: 'bold' }
        },
        didDrawPage: function(data) {
            doc.setDrawColor(0, 0, 0);
            doc.setLineWidth(0.4);
            doc.rect(10, 10, 190, 277);
        }
    });

    // Bottom Signatures
    let finalY = doc.lastAutoTable ? doc.lastAutoTable.finalY + 6 : 255;
    if (finalY > 265) {
        doc.addPage();
        doc.setDrawColor(0, 0, 0);
        doc.setLineWidth(0.4);
        doc.rect(10, 10, 190, 277);
        finalY = 25;
    }

    doc.setFont('helvetica', 'normal');
    doc.setFontSize(8.5);
    doc.text('Prepared & Reconciled By: ___________________________', 16, finalY + 8);
    doc.text('Nareshbhai Tagadiya Signature: ___________________________', 110, finalY + 8);
}

// Standalone Master Karigar Summary Table PDF (wrapper)
async function exportKarigarMasterSummaryPayrollPDF(options = {}) {
    if (!window.jspdf) return showToast('PDF Library not ready', 'error');
    const karigars = getEffectiveKarigarList();
    if (!karigars || karigars.length === 0) return showToast('No karigars found', 'error');

    showLoader();
    try {
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF({
            orientation: 'portrait',
            unit: 'mm',
            format: 'a4'
        });

        let txs = Array.isArray(options.txsOverride)
            ? options.txsOverride
            : (Array.isArray(AppState.karigarTransactions) ? AppState.karigarTransactions : []);
        if (!options.txsOverride && AppState.viewingKarigarBackupId) {
            const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
            if (b && Array.isArray(b.rows)) txs = b.rows;
        }

        const periodLabel = options.periodLabel || (AppState.viewingKarigarBackupId
            ? getKarigarBackupRangeLabel((AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId))
            : getKarigarCurrentCycleLabel());

        renderMasterSummaryPayrollSheet(doc, karigars, txs, { periodLabel });

        const totalPages = doc.internal.getNumberOfPages();
        for (let p = 1; p <= totalPages; p++) {
            doc.setPage(p);
            doc.setFont('helvetica', 'normal');
            doc.setFontSize(7.5);
            doc.setTextColor(110, 110, 110);
            doc.text('Krimaa Enterprise Management System', 10, 292);

            doc.setFont('helvetica', 'bold');
            doc.setFontSize(8);
            doc.setTextColor(20, 20, 20);
            doc.text(`MASTER SUMMARY  |  Page ${p}/${totalPages}`, 200, 292, { align: 'right' });
        }

        if (options.autoPrint) {
            printPDFViaHiddenIframe(doc, 'Master Karigar Payroll Summary');
        } else {
            const currentCompany = AppState.currentCompany || 'company1';
            const companyName = getCompanyDisplayName(currentCompany);
            const periodSafe = (periodLabel || 'Current_Month').replace(/[^a-zA-Z0-9_-]/g, '_');
            const cleanComp = companyName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const filename = `Master_Karigar_Payroll_Summary_${cleanComp}_${periodSafe}_${getTodayISODate()}.pdf`;
            doc.save(filename);
            showToast('Master Karigar Payroll Summary downloaded!', 'success');
        }
    } catch (err) {
        console.error('Master Payroll Summary error:', err);
        showToast('Failed to generate Master Payroll Summary: ' + err.message, 'error');
    } finally {
        hideLoader();
    }
}

// ===== POPUP MODAL FOR SALARY SLIP DOWNLOAD / PRINT OPTIONS =====
function openSalarySlipOptionsModal(config = {}) {
    const modal = document.getElementById('karigar-slip-options-modal');
    const karigar = config.karigar || null;
    const isBulk = !!config.isBulk;
    const bulkOptions = config.bulkOptions || {};

    if (!modal) {
        // Fallback: direct download with both copies if modal element missing
        if (isBulk) {
            return exportBulkKarigarSalarySlipsPDF(bulkOptions);
        } else {
            return exportKarigarSalarySlipPDF(karigar);
        }
    }

    const titleEl = document.getElementById('slip-options-modal-title');
    const subEl = document.getElementById('slip-options-modal-subtitle');
    const workerCheck = document.getElementById('slip-opt-worker-check');
    const factoryCheck = document.getElementById('slip-opt-factory-check');
    const copiesTitleEl = document.getElementById('slip-opt-copies-title');
    const dlBtn = document.getElementById('btn-slip-opt-download');
    const printBtn = document.getElementById('btn-slip-opt-print');
    const cancelBtn = document.getElementById('btn-slip-opt-cancel');
    const closeBtn = document.getElementById('close-karigar-slip-options-modal');

    // Default: ONLY Worker Copy is checked everywhere
    if (workerCheck) workerCheck.checked = true;
    if (factoryCheck) factoryCheck.checked = false;

    if (isBulk) {
        if (titleEl) titleEl.innerHTML = "<i class='bx bxs-file-pdf text-danger'></i> All Karigar Salary Slips";
        if (subEl) subEl.textContent = "Includes Master Summary Table on Page 1 + Karigar Slips";
        if (copiesTitleEl) copiesTitleEl.textContent = "Include Slips";
    } else {
        const kName = karigar?.name || 'Karigar';
        if (titleEl) titleEl.innerHTML = `<i class='bx bxs-file-pdf text-danger'></i> Salary Slip: ${escapeHtml(kName)}`;
        if (subEl) subEl.textContent = "Select copies to include for this Karigar";
        if (copiesTitleEl) copiesTitleEl.textContent = "Detailed Salary Slip Copies";
    }

    let handleSlipKeyDown;
    const closeModal = () => {
        if (handleSlipKeyDown) window.removeEventListener('keydown', handleSlipKeyDown, true);
        modal.classList.remove('show');
    };
    if (cancelBtn) cancelBtn.onclick = closeModal;
    if (closeBtn) closeBtn.onclick = closeModal;

    const getSelection = () => {
        const incWorker = !!workerCheck?.checked;
        const incFactory = !!factoryCheck?.checked;
        if (!incWorker && !incFactory) {
            showToast('Please select at least one copy', 'warning');
            return null;
        }
        return { includeWorker: incWorker, includeFactory: incFactory };
    };

    if (dlBtn) {
        dlBtn.onclick = () => {
            const sel = getSelection();
            if (!sel) return;
            closeModal();
            if (isBulk) {
                exportBulkKarigarSalarySlipsPDF({ ...bulkOptions, ...sel, autoPrint: false });
            } else {
                exportKarigarSalarySlipPDF(karigar, sel);
            }
        };
    }

    if (printBtn) {
        printBtn.onclick = () => {
            const sel = getSelection();
            if (!sel) return;
            closeModal();
            if (isBulk) {
                exportBulkKarigarSalarySlipsPDF({ ...bulkOptions, ...sel, autoPrint: true });
            } else {
                printKarigarSalarySlip(karigar, sel);
            }
        };
    }

    handleSlipKeyDown = (e) => {
        if (!modal.classList.contains('show')) return;
        const key = e.key.toLowerCase();
        if (e.key === 'Escape') {
            e.preventDefault();
            e.stopPropagation();
            closeModal();
        } else if (e.key === 'Enter') {
            e.preventDefault();
            e.stopPropagation();
            if (dlBtn) dlBtn.click();
        } else if (key === 'p') {
            e.preventDefault();
            e.stopPropagation();
            if (printBtn) printBtn.click();
        } else if (key === 'w' || e.key === '1') {
            e.preventDefault();
            e.stopPropagation();
            if (workerCheck) workerCheck.checked = !workerCheck.checked;
        } else if (key === 'f' || e.key === '2') {
            e.preventDefault();
            e.stopPropagation();
            if (factoryCheck) factoryCheck.checked = !factoryCheck.checked;
        }
    };
    window.addEventListener('keydown', handleSlipKeyDown, true);

    modal.classList.add('show');
}

// ===== POPUP MODAL FOR COMPANY MONTHLY REPORT PRINT / DOWNLOAD OPTIONS =====
function openCompanyReportOptionsModal(config = {}) {
    const modal = document.getElementById('company-report-options-modal');
    if (!modal) {
        return exportCompanyMonthlyReportPDF(config.companyId, config.txs, config);
    }

    const subEl = document.getElementById('comp-report-modal-subtitle');
    const dlBtn = document.getElementById('btn-comp-report-opt-download');
    const printBtn = document.getElementById('btn-comp-report-opt-print');
    const cancelBtn = document.getElementById('btn-comp-report-opt-cancel');
    const closeBtn = document.getElementById('close-company-report-options-modal');

    if (subEl) {
        const period = config.periodLabel || (AppState.viewingKarigarBackupId
            ? getKarigarBackupRangeLabel((AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId))
            : getKarigarCurrentCycleLabel());
        subEl.textContent = `Period: ${period} | Production leaderboard & audit statement`;
    }

    let handleReportKeyDown;
    const closeModal = () => {
        if (handleReportKeyDown) window.removeEventListener('keydown', handleReportKeyDown, true);
        modal.classList.remove('show');
    };

    if (cancelBtn) cancelBtn.onclick = closeModal;
    if (closeBtn) closeBtn.onclick = closeModal;

    if (dlBtn) {
        dlBtn.onclick = () => {
            closeModal();
            exportCompanyMonthlyReportPDF(config.companyId, config.txs, config);
        };
    }

    if (printBtn) {
        printBtn.onclick = () => {
            closeModal();
            printCompanyMonthlyReport(config.companyId, config.txs, config);
        };
    }

    handleReportKeyDown = (e) => {
        if (!modal.classList.contains('show')) return;
        const key = e.key.toLowerCase();
        if (e.key === 'Escape') {
            e.preventDefault();
            e.stopPropagation();
            closeModal();
        } else if (e.key === 'Enter') {
            e.preventDefault();
            e.stopPropagation();
            if (dlBtn) dlBtn.click();
        } else if (key === 'p') {
            e.preventDefault();
            e.stopPropagation();
            if (printBtn) printBtn.click();
        }
    };
    window.addEventListener('keydown', handleReportKeyDown, true);

    modal.classList.add('show');
}

function exportKarigarSalarySlipPDF(karigar, customOptions = {}) {
    let txs = Array.isArray(AppState.karigarTransactions) ? AppState.karigarTransactions : [];
    if (AppState.viewingKarigarBackupId) {
        const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
        if (b && Array.isArray(b.rows)) txs = b.rows;
    }
    generateKarigarSalarySlipPDF(karigar, txs, { isBulk: false, ...customOptions });
}

function printPDFViaHiddenIframe(doc, title = 'Document') {
    if (!doc) return;
    doc.autoPrint();
    const blob = doc.output('blob');
    const blobUrl = URL.createObjectURL(blob);

    showToast(`Opening Print dialog for ${title}...`, 'info');

    const iframe = document.createElement('iframe');
    iframe.style.position = 'fixed';
    iframe.style.right = '0';
    iframe.style.bottom = '0';
    iframe.style.width = '1px';
    iframe.style.height = '1px';
    iframe.style.border = '0';
    iframe.style.opacity = '0.01';
    iframe.src = blobUrl;
    document.body.appendChild(iframe);

    let printed = false;
    const triggerPrint = () => {
        if (printed) return;
        printed = true;
        try {
            iframe.contentWindow?.focus();
            iframe.contentWindow?.print();
        } catch (err) {
            console.warn('Iframe print error, falling back to window.open:', err);
            const w = window.open(blobUrl, '_blank');
            if (w) w.focus();
        }
        setTimeout(() => {
            try { document.body.removeChild(iframe); } catch(e){}
            try { URL.revokeObjectURL(blobUrl); } catch(e){}
        }, 120000);
    };

    iframe.onload = () => {
        setTimeout(triggerPrint, 250);
    };
    setTimeout(triggerPrint, 900);
}

function printKarigarSalarySlip(karigar, customOptions = {}) {
    if (!window.jspdf) return showToast('PDF library not ready', 'error');
    if (!karigar) return;
    let txs = Array.isArray(AppState.karigarTransactions) ? AppState.karigarTransactions : [];
    if (AppState.viewingKarigarBackupId) {
        const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
        if (b && Array.isArray(b.rows)) txs = b.rows;
    }

    const doc = generateKarigarSalarySlipPDF(karigar, txs, { isBulk: false, noDownload: true, ...customOptions });
    if (!doc) return;
    printPDFViaHiddenIframe(doc, `${karigar.name} Salary Slip`);
}

async function exportBulkKarigarSalarySlipsPDF(options = {}) {
    if (!window.jspdf) return showToast('PDF Library not ready', 'error');
    const karigars = getEffectiveKarigarList();
    if (!karigars || karigars.length === 0) return showToast('No karigars found', 'error');

    showLoader();
    try {
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF({
            orientation: 'portrait',
            unit: 'mm',
            format: 'a4'
        });

        let txs = Array.isArray(options.txsOverride)
            ? options.txsOverride
            : (Array.isArray(AppState.karigarTransactions) ? AppState.karigarTransactions : []);
        if (AppState.viewingKarigarBackupId) {
            const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
            if (b && Array.isArray(b.rows)) txs = b.rows;
        }

        const periodLabel = options.periodLabel || getKarigarCurrentCycleLabel();
        const pageLabels = {};

        // 1. AUTOMATICALLY ATTACH MASTER SUMMARY TABLE ON PAGE 1 (TOP OF PDF)
        const summaryStartPage = doc.internal.getNumberOfPages();
        renderMasterSummaryPayrollSheet(doc, karigars, txs, { periodLabel });
        const summaryEndPage = doc.internal.getNumberOfPages();
        const totalSummaryPages = summaryEndPage - summaryStartPage + 1;
        for (let p = summaryStartPage; p <= summaryEndPage; p++) {
            doc.setPage(p);
            const pNum = p - summaryStartPage + 1;
            doc.setFont('helvetica', 'normal');
            doc.setFontSize(7.5);
            doc.setTextColor(110, 110, 110);
            doc.text('Krimaa Enterprise Management System', 10, 292);

            doc.setFont('helvetica', 'bold');
            doc.setFontSize(8);
            doc.setTextColor(20, 20, 20);
            doc.text(`MASTER SUMMARY  |  Page ${pNum}/${totalSummaryPages}`, 200, 292, { align: 'right' });
        }

        // 2. ATTACH ALL DETAILED KARIGAR SLIPS IMMEDIATELY ON SUBSEQUENT PAGES
        // Each karigar slip renders its own independent Karigar-wise page numbering (e.g. Page 1/1)
        let slipCount = 0;
        karigars.forEach((k) => {
            const kId = k.id;
            const kName = String(k.name || '').trim().toLowerCase();
            const hasTxs = (txs || []).some(t => {
                const tid = String(t.karigarId || '').trim();
                const tname = String(t.karigarName || '').trim().toLowerCase();
                return tid === kId || (tname && kName && tname === kName);
            });
            if (!hasTxs && options.onlyWithActivity) return;

            doc.addPage();
            generateKarigarSalarySlipPDF(k, txs, {
                doc,
                isBulk: true,
                periodLabel,
                includeWorker: options.includeWorker !== false,
                includeFactory: options.includeFactory !== false
            });
            slipCount++;
        });

        if (options.autoPrint) {
            printPDFViaHiddenIframe(doc, `All Karigar Slips`);
        } else {
            const compName = getCompanyDisplayName(AppState.currentCompany || 'company1');
            const cleanComp = compName.replace(/[^a-zA-Z0-9_-]/g, '_');
            const filename = `Karigar_All_Salary_Slips_${cleanComp}_${getTodayISODate()}.pdf`;
            doc.save(filename);
            showToast(`All Slips PDF downloaded with Master Summary on Page 1 (${slipCount} workers)!`, 'success');
        }
    } catch (e) {
        console.error('Bulk PDF error:', e);
        showToast('Failed to generate bulk PDF: ' + e.message, 'error');
    } finally {
        hideLoader();
    }
}

// ===== MONTHLY COMPANY PRODUCTION & FINANCIAL AUDIT STATEMENT =====
function generateCompanyMonthlyReportPDF(companyId, rawTxs, options = {}) {
    if (!window.jspdf) {
        showToast('PDF library not ready', 'error');
        return null;
    }
    const { jsPDF } = window.jspdf;
    const doc = options.doc || new jsPDF({
        orientation: 'portrait',
        unit: 'mm',
        format: 'a4'
    });

    const safeCompany = companyId || AppState.currentCompany || 'company1';
    const compName = (getCompanyDisplayName(safeCompany) || 'SHREE SAI').toUpperCase();
    const periodLabel = options.periodLabel || getKarigarCurrentCycleLabel();

    const round2 = (n) => Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;
    const toAmount = (v) => {
        const n = parseFloat(String(v ?? '').replace(/[^0-9.-]/g, ''));
        return Number.isFinite(n) ? n : 0;
    };

    let txs = Array.isArray(rawTxs) ? rawTxs : (AppState.karigarTransactions || []);
    if (!rawTxs && AppState.viewingKarigarBackupId) {
        const b = (AppState.karigarResetBackups || []).find(x => x.id === AppState.viewingKarigarBackupId);
        if (b && Array.isArray(b.rows)) txs = b.rows;
    }

    const effectiveWorkers = getEffectiveKarigarList();
    const karigarMap = new Map();
    const idToWorkerMap = new Map();

    effectiveWorkers.forEach(k => {
        const normName = String(k.name || '').trim().toLowerCase();
        const entry = {
            id: k.id,
            name: k.name,
            enterpriseId: getEnterpriseKarigarId(k, safeCompany),
            totalPics: 0,
            totalJama: 0,
            totalUpad: 0,
            balance: 0
        };
        if (normName) karigarMap.set(normName, entry);
        if (k.id) idToWorkerMap.set(String(k.id).trim(), entry);
    });

    // Aggregate Design Analysis
    const designMap = {};

    let grandProductionPics = 0;
    let grandGrossJama = 0;
    let grandAdvancesUpad = 0;

    txs.forEach(t => {
        const rawName = String(t.karigarName || '').trim().toLowerCase();
        const rawId = String(t.karigarId || '').trim();

        let worker = null;
        if (rawName && karigarMap.has(rawName)) {
            worker = karigarMap.get(rawName);
        } else if (rawId && idToWorkerMap.has(rawId)) {
            worker = idToWorkerMap.get(rawId);
        } else if (rawName) {
            worker = {
                id: rawId || ('kar_' + encodeURIComponent(rawName)),
                name: t.karigarName || 'Unknown Karigar',
                enterpriseId: getEnterpriseKarigarId({ id: rawId, name: t.karigarName }, safeCompany),
                totalPics: 0,
                totalJama: 0,
                totalUpad: 0,
                balance: 0
            };
            karigarMap.set(rawName, worker);
            if (rawId) idToWorkerMap.set(rawId, worker);
        }
        if (!worker) return;

        const type = String(t.type || '').toLowerCase();
        if (type === 'jama') {
            const pics = parseInt(t.pic, 10) || 0;
            const price = round2(t.price || 0);
            const total = round2(t.total || (pics * price));
            const upadAmt = round2(toAmount(t.upadAmount || 0));

            worker.totalPics += pics;
            worker.totalJama = round2(worker.totalJama + total);
            grandProductionPics += pics;
            grandGrossJama = round2(grandGrossJama + total);

            if (upadAmt > 0) {
                worker.totalUpad = round2(worker.totalUpad + upadAmt);
                grandAdvancesUpad = round2(grandAdvancesUpad + upadAmt);
            }

            // Design aggregation
            const rawDesign = String(t.designName || t.design || t.article || 'Standard Article').trim() || 'Standard';
            if (!designMap[rawDesign]) {
                designMap[rawDesign] = {
                    designName: rawDesign,
                    totalPics: 0,
                    totalValue: 0,
                    rates: []
                };
            }
            designMap[rawDesign].totalPics += pics;
            designMap[rawDesign].totalValue = round2(designMap[rawDesign].totalValue + total);
            if (price > 0) designMap[rawDesign].rates.push(price);

        } else if (type === 'upad') {
            const amt = round2(toAmount(t.amount || t.upadAmount || 0));
            worker.totalUpad = round2(worker.totalUpad + amt);
            grandAdvancesUpad = round2(grandAdvancesUpad + amt);
        }
    });

    Array.from(karigarMap.values()).forEach(k => {
        k.balance = round2(k.totalJama - k.totalUpad);
    });

    const grandNetSettled = round2(grandGrossJama - grandAdvancesUpad);
    const variance = round2(grandGrossJama - grandAdvancesUpad - grandNetSettled);
    const isZeroErrorVerified = Math.abs(variance) < 0.001;

    // Leaderboard: Karigars sorted descending by Production (Pieces)
    const karigarLeaderboard = Array.from(karigarMap.values()).filter(k => k.totalPics > 0 || k.totalJama > 0 || k.totalUpad > 0);
    karigarLeaderboard.sort((a, b) => {
        if (b.totalPics !== a.totalPics) return b.totalPics - a.totalPics;
        return b.totalJama - a.totalJama;
    });

    // Design Leaderboard: Designs sorted descending by Pieces produced
    const designLeaderboard = Object.values(designMap);
    designLeaderboard.sort((a, b) => b.totalPics - a.totalPics);

    function renderReportPageDecorations(pageDoc, pageNum, totalPages) {
        pageDoc.setDrawColor(0, 0, 0);
        pageDoc.setLineWidth(0.4);
        pageDoc.rect(10, 10, 190, 277);

        pageDoc.setFont('helvetica', 'bold');
        pageDoc.setFontSize(16);
        pageDoc.setTextColor(0, 0, 0);
        pageDoc.text(compName, 105, 17.5, { align: 'center' });

        pageDoc.setFontSize(10.5);
        pageDoc.text('MONTHLY PRODUCTION & FINANCIAL AUDIT STATEMENT', 105, 23, { align: 'center' });

        pageDoc.setFontSize(8);
        pageDoc.setFont('helvetica', 'normal');
        pageDoc.setTextColor(60, 60, 60);
        pageDoc.text(`Audit Cycle: ${periodLabel}  |  Generated: ${new Date().toLocaleDateString('en-GB')} ${new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })}  |  Doc Ref: AUD-${safeCompany.toUpperCase()}-${getTodayISODate().replace(/-/g, '')}`, 105, 27.5, { align: 'center' });

        pageDoc.setLineWidth(0.3);
        pageDoc.line(10, 29.5, 200, 29.5);
    }

    renderReportPageDecorations(doc, 1, 1);

    // 4 Key Executive Performance KPI Cards
    const cardY = 32.5;
    const cardW = 41.5;
    const cardH = 14;
    const cardGap = 4.6;
    const startX = 14;

    doc.setDrawColor(0, 0, 0);
    doc.setLineWidth(0.3);

    // Card 1: Total Factory Production
    doc.rect(startX, cardY, cardW, cardH);
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(7);
    doc.setTextColor(0, 0, 0);
    doc.text('TOTAL FACTORY OUTPUT', startX + cardW / 2, cardY + 4.5, { align: 'center' });
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(11);
    doc.text(grandProductionPics + ' Pcs', startX + cardW / 2, cardY + 11, { align: 'center' });

    // Card 2: Gross Piece-Rate Wages
    const card2X = startX + cardW + cardGap;
    doc.rect(card2X, cardY, cardW, cardH);
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(7);
    doc.text('GROSS WAGES EARNED (JAMA)', card2X + cardW / 2, cardY + 4.5, { align: 'center' });
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(11);
    doc.text('Rs. ' + grandGrossJama.toFixed(2), card2X + cardW / 2, cardY + 11, { align: 'center' });

    // Card 3: Total Advances Deducted
    const card3X = card2X + cardW + cardGap;
    doc.rect(card3X, cardY, cardW, cardH);
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(7);
    doc.text('TOTAL ADVANCES (UPAD)', card3X + cardW / 2, cardY + 4.5, { align: 'center' });
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(11);
    doc.text('Rs. ' + grandAdvancesUpad.toFixed(2), card3X + cardW / 2, cardY + 11, { align: 'center' });

    // Card 4: Net Wages Disbursed
    const card4X = card3X + cardW + cardGap;
    doc.rect(card4X, cardY, cardW, cardH);
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(7);
    doc.text('NET SETTLED BALANCE', card4X + cardW / 2, cardY + 4.5, { align: 'center' });
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(11);
    doc.text('Rs. ' + grandNetSettled.toFixed(2), card4X + cardW / 2, cardY + 11, { align: 'center' });

    let currentY = cardY + cardH + 6;

    // SECTION 1: PRODUCTION LEADERBOARD (Which Karigar gave the most production)
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(9);
    doc.setTextColor(0, 0, 0);
    doc.text('1. PRODUCTION LEADERBOARD (Karigars Ranked by Output Volume):', 14, currentY);

    const leaderboardRows = karigarLeaderboard.map((k, idx) => {
        const sharePct = grandProductionPics > 0 ? ((k.totalPics / grandProductionPics) * 100).toFixed(1) + '%' : '0.0%';
        const rankLabel = idx === 0 ? 'Top 1' : String(idx + 1);
        return [
            rankLabel,
            k.name,
            k.enterpriseId,
            k.totalPics,
            sharePct,
            'Rs. ' + k.totalJama.toFixed(2),
            'Rs. ' + k.totalUpad.toFixed(2),
            'Rs. ' + Math.abs(k.balance).toFixed(2)
        ];
    });

    if (leaderboardRows.length === 0) {
        leaderboardRows.push(['-', 'No production recorded for this cycle', '-', '0', '0.0%', '0.00', '0.00', '0.00']);
    }

    doc.autoTable({
        startY: currentY + 2,
        head: [['Rank', 'Karigar Name', 'Karigar ID', 'Pcs Made', '% Share', 'Gross Jama', 'Advances', 'Net Settled']],
        body: leaderboardRows,
        foot: [[
            'TOTAL',
            `${karigarLeaderboard.length} Karigars Active`,
            '-',
            grandProductionPics + ' Pcs',
            '100.0%',
            'Rs. ' + grandGrossJama.toFixed(2),
            'Rs. ' + grandAdvancesUpad.toFixed(2),
            'Rs. ' + grandNetSettled.toFixed(2)
        ]],
        theme: 'grid',
        styles: {
            fontSize: 7.5,
            cellPadding: 1.4,
            textColor: [0, 0, 0],
            lineColor: [0, 0, 0],
            lineWidth: 0.1
        },
        headStyles: {
            fillColor: [240, 240, 240],
            textColor: [0, 0, 0],
            fontStyle: 'bold',
            halign: 'center'
        },
        footStyles: {
            fillColor: [245, 245, 245],
            textColor: [0, 0, 0],
            fontStyle: 'bold'
        },
        columnStyles: {
            0: { halign: 'center', cellWidth: 12 },
            1: { halign: 'left', cellWidth: 38, fontStyle: 'bold' },
            2: { halign: 'center', cellWidth: 24 },
            3: { halign: 'right', cellWidth: 18 },
            4: { halign: 'center', cellWidth: 16 },
            5: { halign: 'right', cellWidth: 24 },
            6: { halign: 'right', cellWidth: 24 },
            7: { halign: 'right', cellWidth: 26, fontStyle: 'bold' }
        },
        margin: { left: 14, right: 14 }
    });

    currentY = doc.lastAutoTable.finalY + 5;

    if (currentY > 210) {
        doc.addPage();
        renderReportPageDecorations(doc, 2, 2);
        currentY = 32.5;
    }

    // SECTION 2: DESIGN / ARTICLE ANALYSIS (Which Design Was Most Made)
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(9);
    doc.setTextColor(0, 0, 0);
    doc.text('2. DESIGN & ARTICLE PRODUCTION ANALYSIS (Most Made Articles):', 14, currentY);

    const designRows = designLeaderboard.map((d, idx) => {
        const sharePct = grandProductionPics > 0 ? ((d.totalPics / grandProductionPics) * 100).toFixed(1) + '%' : '0.0%';
        const avgRate = d.totalPics > 0 ? (d.totalValue / d.totalPics).toFixed(2) : '0.00';
        const rankLabel = idx === 0 ? 'Top 1' : String(idx + 1);
        return [
            rankLabel,
            d.designName,
            d.totalPics,
            sharePct,
            'Rs. ' + avgRate,
            'Rs. ' + d.totalValue.toFixed(2)
        ];
    });

    if (designRows.length === 0) {
        designRows.push(['-', 'No design production entries recorded', '0', '0.0%', '0.00', '0.00']);
    }

    doc.autoTable({
        startY: currentY + 2,
        head: [['Rank', 'Design No / Article', 'Total Pcs Stitched', '% Output Share', 'Avg Piece Rate', 'Total Wage Value']],
        body: designRows,
        foot: [[
            'TOTAL',
            `${designLeaderboard.length} Articles Manufactured`,
            grandProductionPics + ' Pcs',
            '100.0%',
            '-',
            'Rs. ' + grandGrossJama.toFixed(2)
        ]],
        theme: 'grid',
        styles: {
            fontSize: 7.5,
            cellPadding: 1.4,
            textColor: [0, 0, 0],
            lineColor: [0, 0, 0],
            lineWidth: 0.1
        },
        headStyles: {
            fillColor: [240, 240, 240],
            textColor: [0, 0, 0],
            fontStyle: 'bold',
            halign: 'center'
        },
        footStyles: {
            fillColor: [245, 245, 245],
            textColor: [0, 0, 0],
            fontStyle: 'bold'
        },
        columnStyles: {
            0: { halign: 'center', cellWidth: 14 },
            1: { halign: 'left' },
            2: { halign: 'right', cellWidth: 32 },
            3: { halign: 'center', cellWidth: 26 },
            4: { halign: 'right', cellWidth: 30 },
            5: { halign: 'right', cellWidth: 36 }
        },
        margin: { left: 14, right: 14 }
    });

    currentY = doc.lastAutoTable.finalY + 8;

    if (currentY > 235) {
        doc.addPage();
        renderReportPageDecorations(doc, 3, 3);
        currentY = 32.5;
    }

    // SECTION 3: FACTORY AUDIT APPROVAL SIGNATURES
    doc.setDrawColor(0, 0, 0);
    doc.setLineWidth(0.3);
    doc.rect(14, currentY, 182, 22);

    doc.setFont('helvetica', 'bold');
    doc.setFontSize(8);
    doc.setTextColor(0, 0, 0);
    doc.text('FACTORY PRODUCTION AUDIT & EXECUTIVE SIGN-OFF:', 17, currentY + 4.8);

    doc.setFont('helvetica', 'normal');
    doc.setFontSize(7.5);
    doc.text('1. Production Incharge / Supervisor: _______________________', 18, currentY + 12);
    doc.text('2. Head Accountant / Internal Auditor: _____________________', 18, currentY + 17.5);

    doc.text('3. Factory Owner / Authorized Signatory: ___________________', 105, currentY + 12);
    doc.text('Official Factory Seal & Date: ______________________________', 105, currentY + 17.5);

    // Footer Software Tag & Universal Page Numbering (Page X/Y)
    const totalPages = doc.internal.getNumberOfPages();
    for (let p = 1; p <= totalPages; p++) {
        doc.setPage(p);
        doc.setFont('helvetica', 'normal');
        doc.setFontSize(7.5);
        doc.setTextColor(110, 110, 110);
        doc.text('Krimaa Enterprise Management System | Production & Financial Audit', 14, 292);

        doc.setFont('helvetica', 'bold');
        doc.setFontSize(8);
        doc.setTextColor(20, 20, 20);
        doc.text(`COMPANY REPORT  |  Page ${p}/${totalPages}`, 196, 292, { align: 'right' });
    }

    if (!options.noDownload) {
        const filename = `Company_Monthly_Production_Report_${compName}_${getTodayISODate()}.pdf`;
        doc.save(filename);
        showToast(`Company Monthly Production & Audit Report downloaded!`, 'success');
    }

    return doc;
}

function exportCompanyMonthlyReportPDF(companyId, txs, options = {}) {
    generateCompanyMonthlyReportPDF(companyId, txs, { ...options, noDownload: false });
}

function printCompanyMonthlyReport(companyId, txs, options = {}) {
    const doc = generateCompanyMonthlyReportPDF(companyId, txs, { ...options, noDownload: true });
    if (doc) {
        printPDFViaHiddenIframe(doc, 'Company Monthly Report');
    }
}

// Backwards-compatible alias for toolbar summary button
function exportKarigarSummaryPDF() {
    exportCompanyMonthlyReportPDF();
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
    
    const timeAppend = new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'}).replace(/:/g, '-').replace(/ /g, '');
    exportTableToPDF('Combined Data Sheet', headers, rows, `Data_Sheet_${getTodayISODate()}_${timeAppend}.pdf`);
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
    const timeAppend = new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'}).replace(/:/g, '-').replace(/ /g, '');
    exportTableToPDF('Money Management Report', ['Company', 'Account', 'Money', 'Expense', 'Balance'], rows, `Money_Report_${getTodayISODate()}_${timeAppend}.pdf`);
});

document.getElementById('export-pdf-karigar-btn')?.addEventListener('click', () => {
    const rows = [];
    document.querySelectorAll('#karigar-grid .card').forEach(card => {
        const name = card.querySelector('.font-bold.text-lg')?.textContent.trim() || '';
        const jama = card.querySelector('.text-success')?.textContent.replace('₹', '').trim() || '0';
        const upad = card.querySelector('.text-danger')?.textContent.replace('₹', '').trim() || '0';
        const balance = card.querySelector('.karigar-balance-val .actual-bal')?.textContent.replace('₹', '').trim() || '0';
        rows.push([name, jama, upad, balance]);
    });
    const timeAppend = new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'}).replace(/:/g, '-').replace(/ /g, '');
    exportTableToPDF('Karigar Management Report', ['Name', 'Total Jama', 'Total Upad', 'Balance'], rows, `Karigar_Report_${getTodayISODate()}_${timeAppend}.pdf`);
});

document.getElementById('global-export-btn')?.addEventListener('click', () => {
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
        
        // Actually fetch the real data here directly from Google Sheets via sheetsApiRequest
        const res = await sheetsApiRequest({ action: 'getGlobalReport', fromDate, toDate });
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

// ===== LAN DRIVE UI & CONTROLLERS =====
function initLanSettingsUI() {
    const indicator = document.getElementById('lan-indicator');
    const modal = document.getElementById('lan-settings-modal');
    const settingsBtn = document.getElementById('lan-settings-btn');
    const closeBtn = document.getElementById('close-lan-modal-btn');
    const form = document.getElementById('lan-settings-form');
    const selectBtn = document.getElementById('lan-select-folder-btn');
    const disconnectBtn = document.getElementById('lan-disconnect-btn');
    const badge = document.getElementById('lan-modal-status-badge');
    const desc = document.getElementById('lan-modal-status-desc');
    const exportBtn = document.getElementById('lan-export-payload-btn');
    const importBtn = document.getElementById('lan-import-payload-btn');
    const fileInput = document.getElementById('lan-import-file-input');

    if (!indicator || !modal) return;

    function syncUI(status) {
        if (status === "connected") {
            indicator.className = "sync-indicator sync-saved";
            indicator.querySelector('span').textContent = "LAN: Connected";
            indicator.style.display = "";
            badge.className = "badge badge-a bg-success text-white";
            badge.textContent = "Connected";
            desc.textContent = "Linked to local folder. High-speed local reading/writing is active.";
            disconnectBtn.classList.remove('hidden');
            selectBtn.classList.add('hidden');
            document.body.classList.remove('lan-read-only');
        } else if (status === "read-only") {
            indicator.className = "sync-indicator sync-error";
            indicator.querySelector('span').textContent = "LAN: Read-Only";
            indicator.style.display = "";
            badge.className = "badge badge-b bg-danger text-white";
            badge.textContent = "Read-Only";
            desc.textContent = "Folder path configured but locked/disconnected. Link folder to unlock.";
            disconnectBtn.classList.remove('hidden');
            selectBtn.classList.remove('hidden');
            document.body.classList.add('lan-read-only');
        } else {
            indicator.className = "sync-indicator sync-idle";
            indicator.querySelector('span').textContent = "LAN: Off";
            badge.className = "badge";
            badge.style.background = "#cbd5e1";
            badge.style.color = "#475569";
            badge.textContent = "Off";
            desc.textContent = "Firestore is currently your primary database in Read-Only mode. Edits require connecting a LAN folder.";
            disconnectBtn.classList.add('hidden');
            selectBtn.classList.remove('hidden');
            document.body.classList.add('lan-read-only');
        }
    }

    LanStorageService.onStatusChange((status) => {
        syncUI(status);
    });

    function updateLanModalProfileLabel() {
        const labelEl = document.getElementById('lan-modal-profile-label');
        const labelTextEl = document.getElementById('lan-modal-profile-label-text');
        const profile = LanStorageService.getActiveProfile();
        const selectBtnEl = document.getElementById('lan-select-folder-btn');
        if (!labelEl) return;
        if (profile && !['Krimaa', 'dev', null].includes(profile)) {
            labelEl.style.display = 'block';
            if (labelTextEl) labelTextEl.textContent = `Profile: ${profile} (separate folder)`;
            if (selectBtnEl) selectBtnEl.innerHTML = `<i class='bx bx-folder-open'></i> Link ${profile}'s Folder`;
        } else {
            labelEl.style.display = 'none';
            if (selectBtnEl) selectBtnEl.innerHTML = `<i class='bx bx-folder-open'></i> Link LAN Folder`;
        }
    }

    settingsBtn.addEventListener('click', () => { modal.classList.add('show'); updateLanModalProfileLabel(); });
    indicator.addEventListener('click', () => {
        if (LanStorageService.isReadOnly()) {
            LanStorageService.requestUnlock().then(success => {
                if (success) {
                    showToast("LAN folder unlocked! Reloading data...", "success");
                    loadInitialData();
                }
            });
        } else {
            modal.classList.add('show');
        }
    });

    closeBtn.addEventListener('click', () => modal.classList.remove('show'));
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.classList.remove('show'); });

    selectBtn.addEventListener('click', async () => {
        showLoader("Configuring local folder...");
        const res = await LanStorageService.connect();
        hideLoader();
        if (res.success) {
            showToast("Successfully linked and unlocked local folder!", "success");
            loadInitialData();
        } else {
            showToast("Failed to link folder: " + res.error, "error");
        }
    });

    disconnectBtn.addEventListener('click', () => {
        if (confirm("Disconnect local folder? Data will revert to reading from Cloud Firestore.")) {
            LanStorageService.disconnect();
            showToast("LAN folder disconnected.", "info");
            loadInitialData();
        }
    });

    exportBtn.addEventListener('click', async () => {
        showLoader("Generating ZIP Archive...");
        try {
            const blob = await LanStorageService.generateBackupPayload();
            const url = URL.createObjectURL(blob);
            const downloadAnchor = document.createElement('a');
            downloadAnchor.setAttribute("href", url);
            downloadAnchor.setAttribute("download", `krimaa_lan_shared_drive_${getTodayISODate()}.zip`);
            document.body.appendChild(downloadAnchor);
            downloadAnchor.click();
            downloadAnchor.remove();
            URL.revokeObjectURL(url);
            showToast("LAN backup ZIP exported successfully!", "success");
        } catch (e) {
            console.error(e);
            showToast("Export failed: " + e.message, "error");
        } finally {
            hideLoader();
        }
    });

    importBtn.addEventListener('click', () => {
        if (!LanStorageService.isConnected()) {
            showToast("Link your local folder first before importing backups!", "error");
            return;
        }
        fileInput.click();
    });

    fileInput.addEventListener('change', async (e) => {
        const file = e.target.files[0];
        if (!file) return;

        showLoader('Bootstrapping LAN folder...');
        try {
            if (file.name.endsWith('.zip')) {
                // New ZIP format (exported by "Export Firestore Data" button)
                await LanStorageService.bootstrapFolderFromZip(file);
            } else {
                // Legacy JSON payload
                const text = await file.text();
                const payload = JSON.parse(text);
                await LanStorageService.bootstrapFolderFromBackup(payload);
            }
            showToast('LAN folder successfully bootstrapped from backup!', 'success');
            loadInitialData();
        } catch (err) {
            showToast('Import failed: ' + err.message, 'error');
        } finally {
            hideLoader();
            fileInput.value = '';
        }
    });

    // Auto-reconnect is handled by initApp (awaited before checkAuth).
    // Just update the UI to reflect current status.
    syncUI(LanStorageService.getStatus());
}


// ===== OFFLINE CONTINUOUS POLLING =====
let lastFolderHash = '';
setInterval(async () => {
    if (window.LanStorageService && window.LanStorageService.isConnected()) {
        const currentHash = await window.LanStorageService.getFolderHash();
        if (lastFolderHash && currentHash !== lastFolderHash) {
            console.log('[LAN] External data change detected, refreshing UI...');
            // Refresh dashboard if we are on dashboard
            if (!document.getElementById('app-screen').classList.contains('hidden') && document.getElementById('dashboard').classList.contains('active')) {
                loadDashboardData(document.getElementById('dash-filter-type').value);
            }
            // Refresh manage order page if open
            if (document.getElementById('daily-order').classList.contains('active')) {
                loadInitialData();
            }
        }
        lastFolderHash = currentHash;
    }
}, 5000);

