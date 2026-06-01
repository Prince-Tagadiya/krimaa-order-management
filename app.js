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
        .filter(t => String(t.karigarId || '').trim() === targetId)
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

function initApp() {
    refreshUsersFromEnv();
    applyClientModeUI();
    restoreFirestoreQuotaCooldown();
    bootstrapSheetSyncStateFromStorage();
    bindSheetSyncLifecycleEvents();
    updateBackendModeBanner();
    initLanSettingsUI();
    checkAuth();
    attachEventListeners();
    initSyncIndicator();
    const integrityBtn = document.getElementById('fix-integrity-btn');
    const syncSheetBtn = document.getElementById('sync-sheet-btn');
    if (integrityBtn) integrityBtn.style.display = isDevUser() ? '' : 'none';
    if (syncSheetBtn) syncSheetBtn.style.display = isDevUser() ? '' : 'none';

    // Refresh button (replaces manual backup button in UI)
    const backupBtn = document.getElementById('backup-btn');
    if (backupBtn) backupBtn.addEventListener('click', () => refreshAppDataManually());

    if (integrityBtn) integrityBtn.addEventListener('click', async () => {
        if (!isDevUser()) return showToast("Only dev can run integrity tools", "error");
        if (!confirm("This will repair ALL data integrity issues in Google Sheets (corrupted names, missing IDs, wrong formats). Continue?")) return;
        showLoader();
        try {
            showToast("Reading data from Sheets...", "info");
            const [c1Acc, c2Acc, karResC1, karResC2] = await Promise.all([
                FirebaseService.getAccounts('company1'),
                FirebaseService.getAccounts('company2'),
                FirebaseService.getKarigars('company1'),
                FirebaseService.getKarigars('company2')
            ]);
            
            const allAccounts = [
                ...((c1Acc.details || []).map(a => ({ docId: a.accountId, name: a.name, companyId: 'company1' }))),
                ...((c2Acc.details || []).map(a => ({ docId: a.accountId, name: a.name, companyId: 'company2' })))
            ];
            const allKarigars = [
                ...((karResC1.data || []).map(k => ({ docId: k.id, name: k.name, companyId: 'company1' }))),
                ...((karResC2.data || []).map(k => ({ docId: k.id, name: k.name, companyId: 'company2' })))
            ];
            
            showToast("Repairing names in Google Sheets...", "info");
            const repairRes = await sheetsApiRequest({ 
                action: 'repairFromFirebase', 
                firebaseData: JSON.stringify({ accounts: allAccounts, karigars: allKarigars })
            });
            
            showToast("Running structural integrity check...", "info");
            const backfillRes = await sheetsApiRequest({ action: 'backfillIds' });

            const r = repairRes.stats || {};
            const b = backfillRes.stats || {};
            showToast(
                `Repair Complete! Names Fixed: (Acc: ${r.accounts||0}, Kar: ${r.karigars||0}, Ord: ${r.orders||0}) | ` +
                `IDs Fixed: (Acc: ${b.accounts||0}, Ord: ${b.orders||0})`, 
                "success"
            );
            
            await fetchAccounts();
            renderAccountsList();
            try { await fetchAllCompaniesData(); } catch (e) {}
        } catch (err) {
            console.error(err);
            showToast("Migration error: " + err.message, "error");
        } finally {
            hideLoader();
        }
    });

    if (syncSheetBtn) syncSheetBtn.addEventListener('click', async () => {
        if (!isDevUser()) {
            showToast("Only dev can run replace tools", "error");
            return;
        }
        const selection = await openReplaceFromSheetModal();
        if (!selection) return;
        const selectedLabels = getReplaceSheetSelectionLabels(selection);
        if (!selectedLabels.length) {
            showToast("Select at least one data block", "error");
            return;
        }
        const confirmText = `This will replace ONLY selected Firebase data from Google Sheets:\n\n- ${selectedLabels.join('\n- ')}\n\nContinue?`;
        if (!confirm(confirmText)) return;

        showLoader();
        try {
            showToast("Reading full sheet data...", "info");
            const fullRes = await sheetsApiRequest({ action: 'getAllSheetData' });
            const sheetData = fullRes?.data || null;
            if (!fullRes?.success || !sheetData) {
                throw new Error(fullRes?.message || 'Failed to read sheet data');
            }

            showToast(`Replacing selected data: ${selectedLabels.join(', ')}...`, "info");
            const replaceRes = await Promise.race([
                FirebaseService.replaceFromSheetsSelective(sheetData, {
                    accounts: selection.accounts,
                    orders: selection.orders,
                    karigars: selection.karigars,
                    karigarTransactions: selection.karigarTransactions,
                    sizePrices: selection.sizePrices
                }, { fast: true }),
                new Promise((_, reject) => setTimeout(() => reject(new Error('Replace timed out. Please try again.')), 180000))
            ]);
            if (replaceRes?.success === false) {
                throw new Error(replaceRes?.message || 'Firebase replace failed');
            }
            await loadInitialData();
            const syncStats = replaceRes?.stats || {};
            const statusParts = [];
            if (selection.accounts) statusParts.push(`Accounts: ${syncStats.accounts || 0}`);
            if (selection.orders) statusParts.push(`Orders: ${syncStats.orders || 0}`);
            if (selection.karigars) statusParts.push(`Karigars: ${syncStats.karigars || 0}`);
            if (selection.karigarTransactions) statusParts.push(`Karigar Tx: ${syncStats.karigarTxs || 0}`);
            if (selection.sizePrices) statusParts.push(`Prices: ${syncStats.designPrices || 0}`);
            showToast(`Selected replace complete. ${statusParts.join(' | ')}`, "success");
        } catch (err) {
            console.error(err);
            showToast("Sync Failed: " + err.message, "error");
        } finally {
            hideLoader();
        }
    });

    const today = getTodayISODate();
    setOrderDateDefaults(true);
    
    const savedCompany = localStorage.getItem('selectedCompany');
    if (savedCompany) {
        AppState.currentCompany = savedCompany;
    }
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
            if (!isCompanyAllowed(companyId)) {
                showToast("You don't have access to this company", "error");
                return;
            }
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
        if (!merged.has(key)) {
            merged.set(key, {
                date,
                accountId: accountId || accountName,
                accountName: accountName || accountId,
                meesho: parseInt(row?.meesho, 10) || 0,
                total: parseInt(row?.total, 10) || (parseInt(row?.meesho, 10) || 0),
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

function setPendingOrderOverride(date, accountId, accountName, meesho, companyId) {
    const safeCompanyId = String(companyId || '').trim();
    const safeDate = normalizeToISODate(date);
    if (!safeCompanyId || !safeDate) return;
    const safeAccountId = String(accountId || '').trim();
    const safeName = String(accountName || '').trim();
    const qty = parseInt(meesho) || 0;
    const key = buildPendingOrderOverrideKey(safeCompanyId, safeDate, safeAccountId, safeName);
    _pendingOrderOverrides.set(key, {
        companyId: safeCompanyId,
        date: safeDate,
        accountId: safeAccountId,
        accountName: safeName,
        meesho: qty,
        total: qty,
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
            const serverQty = parseInt(target[idx]?.meesho) || 0;
            if (serverQty === ov.meesho) {
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

async function refreshAppDataManually() {
    showLoader('Checking server status...');
    try {
        const serverResponsive = await probeFirebaseWithCountdown(5);
        if (!serverResponsive) {
            enableSheetsFallbackMode();
            updateBackendModeBanner();
            setLoaderStatus('Firebase server down. Please use the app from tomorrow 1:00 PM. Loading from Google Sheets (Slow)...');
            await loadRecentDataFromSheets(SHEETS_FALLBACK_DAYS);
            await loadAvailableSheetMonths(true);
            if (AppState.currentSection === 'data-sheet') renderDataSheet();
            else if (AppState.currentSection === 'dashboard') renderDashboard();
            else if (AppState.currentSection === 'daily-order') renderOrderEntryTable();
            showToast('Server down. Loaded latest Google Sheet data.', 'info');
            return;
        }

        if (_useSheetsFallbackMode) {
            disableSheetsFallbackMode();
            updateBackendModeBanner();
        }

        showToast('Refreshing from Google Sheet first, then Firebase...', 'info');
        await FirebaseService.flushWrites();
        await syncFirebaseFromSheetsSourceOnRefresh();
        setLoaderStatus('Applying latest data to website...');
        await loadInitialData();
        await mergeRecentSheetRowsIntoState(SHEETS_FALLBACK_DAYS);
        await Promise.all([
            loadMoneyBackups(true),
            loadAvailableSheetMonths(true)
        ]);
        if (AppState.currentSection === 'karigar') {
            invalidateKarigarCache();
            await renderKarigarPage(true);
        } else if (AppState.currentSection === 'money-backup') {
            await renderMoneyBackupPage(true);
        } else if (AppState.currentSection === 'size-prices') {
            await renderSizePricesPage();
        } else if (AppState.currentSection === 'data-sheet') {
            renderDataSheet();
        } else if (AppState.currentSection === 'dashboard') {
            renderDashboard();
        }
        showToast('Data refreshed', 'success');
    } catch (e) {
        console.error('Manual refresh failed:', e);
        showToast('Refresh failed', 'error');
    } finally {
        hideLoader();
    }
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
        else if (AppState.currentSection === 'daily-order') {
            setOrderDateDefaults();
            renderOrderEntryTable();
            checkExistingOrdersForDate();
        }
        else if (AppState.currentSection === 'add-account') renderAccountsList();
        else if (AppState.currentSection === 'money-management') renderMoneyManagement();
        else if (AppState.currentSection === 'money-backup') renderMoneyBackupPage();
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
        else if (AppState.currentSection === 'daily-order') {
            setOrderDateDefaults();
            renderOrderEntryTable();
            checkExistingOrdersForDate();
        }
        else if (AppState.currentSection === 'add-account') renderAccountsList();
        else if (AppState.currentSection === 'money-management') renderMoneyManagement();
        else if (AppState.currentSection === 'money-backup') renderMoneyBackupPage();
        else if (AppState.currentSection === 'karigar') await renderKarigarPage(true);
        else if (AppState.currentSection === 'size-prices') renderSizePricesPage();
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
        if (!isAdminUser()) clearSheetsCaches();

        if (!isCompanyAllowed(AppState.currentCompany)) {
            AppState.currentCompany = getAllowedCompanies()[0] || 'company1';
            localStorage.setItem('selectedCompany', AppState.currentCompany);
        }
        document.getElementById('login-screen').classList.add('hidden');
        document.getElementById('app-screen').classList.remove('hidden');
        applyRolePermissions();
        updateBackendModeBanner();
        startAdminAutoSync();
        loadInitialData();

    } else {
        document.getElementById('login-screen').classList.remove('hidden');
        document.getElementById('app-screen').classList.add('hidden');
        updateBackendModeBanner();
        if (_adminAutoSyncTimer) {
            clearInterval(_adminAutoSyncTimer);
            _adminAutoSyncTimer = null;
        }
        if (_adminRealtimeRefreshTimer) {
            clearInterval(_adminRealtimeRefreshTimer);
            _adminRealtimeRefreshTimer = null;
        }
    }
}

function applyRolePermissions() {
    const role = AppState.currentUser?.role;
    const userInfo = document.getElementById('sidebar-user-info');
    
    // Show user info in sidebar - Simplified as requested (Removed avatar with "A")
    userInfo.innerHTML = `<span>${AppState.currentUser.displayName}</span>`;
    
    if (role === 'order' || role === 'order_c2') {
        // Order role: allow Daily Order + Data Sheet only
        document.getElementById('nav-dashboard').style.display = 'none';
        document.getElementById('nav-data-sheet').style.display = '';
        document.getElementById('nav-manage-accounts').style.display = 'none';
        document.getElementById('nav-money-management').style.display = 'none';
    } else {
        document.getElementById('nav-dashboard').style.display = '';
        document.getElementById('nav-data-sheet').style.display = '';
        document.getElementById('nav-manage-accounts').style.display = '';
        document.getElementById('nav-money-management').style.display = '';
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
}

function attachEventListeners() {
    // Login
    document.getElementById('login-form').addEventListener('submit', (e) => {
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
            if (foundUser.role !== 'admin') clearSheetsCaches();
            if (!isCompanyAllowed(AppState.currentCompany)) {
                AppState.currentCompany = getAllowedCompanies()[0] || 'company1';
                localStorage.setItem('selectedCompany', AppState.currentCompany);
            }
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
                localStorage.removeItem('userUsername');
                localStorage.removeItem('userAllowedCompanies');
                AppState.currentUser = null;
                checkAuth();
                return;
            }
            e.preventDefault();
            const target = btn.getAttribute('data-target');
            
            // Check permissions
            if (target === 'add-account' && (AppState.currentUser?.role === 'order' || AppState.currentUser?.role === 'order_c2')) {
                showToast("You don't have permission to manage accounts", "error");
                return;
            }
            
            navigateTo(target);
            const sidebar = document.getElementById('sidebar');
            if(window.innerWidth <= 768 && sidebar.classList.contains('open')) closeMobileSidebar();
        });
    });

    document.getElementById('menu-toggle').addEventListener('click', toggleMobileSidebar);
    document.getElementById('sidebar-overlay')?.addEventListener('click', closeMobileSidebar);
    window.addEventListener('resize', () => {
        if (window.innerWidth > 768) closeMobileSidebar();
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
        await saveEditAccount();
    });

    const closeDeleteModalBtn = document.getElementById('close-delete-modal-btn');
    if (closeDeleteModalBtn) {
        closeDeleteModalBtn.addEventListener('click', () => document.getElementById('delete-account-modal').classList.remove('show'));
    }
    document.getElementById('delete-account-modal').addEventListener('click', (e) => { if(e.target.id === 'delete-account-modal') e.target.classList.remove('show'); });
    document.getElementById('cancel-delete-btn').addEventListener('click', () => document.getElementById('delete-account-modal').classList.remove('show'));
    document.getElementById('confirm-delete-btn').addEventListener('click', async () => {
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
}

function navigateTo(sectionId) {
    
    // Permission check for order role
    if ((AppState.currentUser?.role === 'order' || AppState.currentUser?.role === 'order_c2') && (sectionId === 'add-account' || sectionId === 'dashboard' || sectionId === 'money-management' || sectionId === 'money-backup')) {
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
    showLoader('Connecting to server...');
    try {
        console.log("🚀 [INITIALIZATION] Starting web app...");
        AppState.karigarCacheByCompany = {};
        FirebaseService.init();
        const serverResponsive = await probeFirebaseWithCountdown(5);
        if (!serverResponsive) {
            enableSheetsFallbackMode();
            setLoaderStatus('Firebase server down. Please use the app from tomorrow 1:00 PM. Loading from Google Sheets (Slow)...');
        } else {
            await tryRecoverFirestoreMode();
            if (_useSheetsFallbackMode) disableSheetsFallbackMode();
        }
        updateBackendModeBanner();
        
        console.log("🚀 [INITIALIZATION] Checking legacy migration and seeds...");
        await ensureFirebaseSeeded();
        
        if (_useSheetsFallbackMode || isFirestoreQuotaBlocked()) {
            setLoaderStatus('Firebase server down. Please use the app from tomorrow 1:00 PM. Loading from Google Sheets (Slow)...');
            console.log("🚀 [INITIALIZATION] Firestore quota mode active. Loading recent Sheet data...");
            await loadRecentDataFromSheets(SHEETS_FALLBACK_DAYS);
            updateBackendModeBanner();
        } else {
            setLoaderStatus('Loading Firebase data...');
            console.log("🚀 [INITIALIZATION] Fetching fresh data instantly from Firestore...");
            await Promise.all([
                fetchAccounts(),
                fetchAllCompaniesData({ refreshArchiveMonths: false })
            ]);
        }
        
        console.log("🚀 [INITIALIZATION] Local data ready. Booting UI in background...");
        loadAvailableSheetMonths(true).catch(e => console.warn('Background meta fetch:', e));
        checkAutoBackup();
        resumeSheetSyncIfPending();
        if (!_pendingDataChangesForBackup) {
            const staleMs = _lastSheetBackupSuccessMs > 0 ? (Date.now() - _lastSheetBackupSuccessMs) : Number.MAX_SAFE_INTEGER;
            if (staleMs >= SHEET_RECONCILE_MAX_STALE_MS) {
                _pendingDataChangesForBackup = true;
                scheduleBackgroundSheetBackup('startup_reconcile');
            }
        }

        console.log("🚀 [INITIALIZATION] Finished completely. Routing to UI.");
        if (AppState.currentUser?.role === 'order' || AppState.currentUser?.role === 'order_c2') {
            navigateTo('daily-order');
        } else {
            navigateTo('data-sheet');
        }
        runStartupMaintenanceInBackground();
    } catch (err) {
        console.error(err);
        if (isFirestoreQuotaError(err) || isFirestoreUnavailableError(err)) {
            try {
                setLoaderStatus('Firebase server down. Please use the app from tomorrow 1:00 PM. Loading from Google Sheets (Slow)...');
                enableSheetsFallbackMode();
                await loadRecentDataFromSheets(SHEETS_FALLBACK_DAYS);
                updateBackendModeBanner();
                if (AppState.currentUser?.role === 'order' || AppState.currentUser?.role === 'order_c2') {
                    navigateTo('daily-order');
                } else {
                    navigateTo('data-sheet');
                }
                showToast('Firebase server down. Please use the app from tomorrow 1:00 PM. Running in Sheet Mode (Slow).', 'info');
            } catch (sheetErr) {
                console.error('Sheet mode bootstrap failed:', sheetErr);
                showToast("Error loading data: " + (sheetErr?.message || err?.message || 'Unknown error'), "error");
            }
        } else {
            showToast("Error loading data: " + err.message, "error");
        }
    }
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

    const normalized = {
        ...nextRow,
        date: safeDate,
        accountId: safeId,
        meesho: parseInt(nextRow?.meesho) || 0,
        total: parseInt(nextRow?.total) || (parseInt(nextRow?.meesho) || 0)
    };

    if (idx >= 0) {
        target[idx] = { ...target[idx], ...normalized };
    } else {
        target.push(normalized);
    }
    return target;
}

function applyOptimisticOrderUpdate(date, accountId, accountName, meesho, companyId) {
    const safeCompanyId = String(companyId || AppState.currentCompany || '').trim() || 'company1';
    const safeDate = normalizeToISODate(date);
    if (!safeDate) return;
    const safeAccountId = String(accountId || '').trim();

    const detailsList = safeCompanyId === 'company1' ? AppState.company1Details : AppState.company2Details;
    const resolvedNameFromDetails = (detailsList || []).find(d => String(d.accountId || '').trim() === safeAccountId)?.name || '';
    const resolvedName = String(accountName || resolvedNameFromDetails || safeAccountId).trim();
    const safeQty = parseInt(meesho) || 0;
    setPendingOrderOverride(safeDate, safeAccountId, resolvedName, safeQty, safeCompanyId);
    const row = {
        date: safeDate,
        accountId: safeAccountId,
        accountName: resolvedName,
        meesho: safeQty,
        total: safeQty,
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
        applyOptimisticOrderUpdate(date, o.accountId, o.accountName, o.meesho, companyId);
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
    const isAdmin = AppState.currentUser?.role === 'admin';
    
    // Add Sortable to container if not initialized
    if (isAdmin && !container._sortableInstance) {
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
        const extraHtml = `
            <div class="account-meta">
                <span><i class='bx bx-phone'></i> Mobile: ${details.mobile || 'Not added'}</span>
                <span><i class='bx bx-calendar'></i> Recharge: ${rechargeTxt}</span>
            </div>`;

        return `
        <div class="account-item" data-account-id="${id}" data-account-name="${acc.replace(/"/g, '&quot;')}">
            <div style="display: flex; align-items: center; gap: 10px;">
                ${isAdmin ? `<i class='bx bx-menu drag-handle' style="cursor: grab; color: #a0aec0;"></i>` : ''}
                <div class="account-position-badge">${idx + 1}</div>
                <div>
                    <span class="account-name font-bold">${acc}</span>
                    ${extraHtml}
                </div>
            </div>
            ${isAdmin ? `<div class="account-actions">
                <button class="btn btn-outline btn-sm edit-btn" onclick="openEditAccount('${id}', '${acc.replace(/'/g, "\\'")}')" title="Edit details"><i class='bx bx-edit-alt'></i></button>
                <button class="btn btn-outline btn-sm delete-btn" onclick="openDeleteAccount('${id}', '${acc.replace(/'/g, "\\'")}')" title="Delete"><i class='bx bx-trash'></i></button>
            </div>` : ''}
        </div>
    `}).join('');
}

function openEditAccount(id, name) {
    const details = AppState.accountDetails.find(d => d.accountId === id);
    if (!details) return;
    document.getElementById('edit-account-id').value = id;
    document.getElementById('edit-account-old-name').value = name;
    document.getElementById('edit-account-name').value = name;
    document.getElementById('edit-account-mobile').value = details.mobile || '';
    document.getElementById('edit-account-recharge').value = details.rechargeDate || '';
    document.getElementById('edit-account-modal').classList.add('show');
}

async function saveEditAccount() {
    const id = document.getElementById('edit-account-id').value;
    const oldName = document.getElementById('edit-account-old-name').value;
    const newName = document.getElementById('edit-account-name').value.trim();
    const mobile = document.getElementById('edit-account-mobile').value.trim();
    const recharge = document.getElementById('edit-account-recharge').value;
    const compId = AppState.currentCompany;
    
    if (!newName) return showToast("Name cannot be empty!", "error");
    
    showToast("Updating account...", "info");
    const res = await FirebaseService.editAccount(id, newName, compId, mobile, '', recharge);
    if (res.success) {
        showToast("Account updated!", "success");
        document.getElementById('edit-account-modal').classList.remove('show');
        await fetchAccounts();
        if (AppState.currentSection === 'daily-order' || AppState.currentSection === 'order-entry') renderOrderEntryTable();
        if (AppState.currentSection === 'add-account' || AppState.currentSection === 'accounts') renderAccountsList();
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
    if (!accTbody) return;
    accTbody.innerHTML = '';
    
    const buildAccTotals = (accountDetails, filtered) => {
        const totals = {};
        // Use accountId as the key for reliable aggregation
        accountDetails.forEach(ad => {
            const id = ad.accountId;
            totals[id] = { name: ad.name, meesho: 0, total: 0 };
        });
        
        filtered.forEach(row => {
            const id = row.accountId;
            if (id && totals[id]) {
                totals[id].meesho += parseInt(row.meesho) || 0;
                totals[id].total += parseInt(row.total) || 0;
            } else if (row.accountName) {
                // Fallback for legacy name-based records
                const idFromName = Object.keys(totals).find(k => totals[k].name === row.accountName);
                if (idFromName) {
                    totals[idFromName].meesho += parseInt(row.meesho) || 0;
                    totals[idFromName].total += parseInt(row.total) || 0;
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
            html += `<tr><td class="font-medium">${d.name}</td><td class="text-center"><span class="dot dot-a"></span></td><td class="text-right text-muted">${d.meesho}</td><td class="text-right font-bold text-main">${d.total}</td></tr>`;
        });
    }

    // Company B Header
    const c2Keys = Object.keys(c2AccTotals);
    if (c2Keys.length > 0) {
        html += `<tr class="table-divider-row"><td colspan="5">Company B Accounts</td></tr>`;
        c2Keys.forEach(id => {
            const d = c2AccTotals[id];
            html += `<tr><td class="font-medium">${d.name}</td><td class="text-center"><span class="dot dot-b"></span></td><td class="text-right text-muted">${d.meesho}</td><td class="text-right font-bold text-main">${d.total}</td></tr>`;
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
    const readActions = new Set(['getAccounts', 'getDashboardData', 'getRemarks', 'getMoneyBackups', 'getKarigars', 'getKarigarTransactions', 'getDesignPrices']);
    const cacheKey = `${action}::${companyId}::${String(payload?.month || '')}`;
    if (readActions.has(action)) {
        const cached = _apiReadCache.get(cacheKey);
        if (cached && (Date.now() - cached.ts) < READ_CACHE_TTL_MS) {
            return cached.value;
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
                result = await FirebaseService.addAccount(payload.accountName, companyId, payload.mobile, payload.gstin, payload.rechargeDate);
                if (result && result.success !== false) {
                    try {
                        await sheetsApiRequest({ action: 'addAccount', accountName: payload.accountName, companyId, mobile: payload.mobile, gstin: payload.gstin, rechargeDate: payload.rechargeDate });
                    } catch (e) { scheduleBackgroundSheetBackup('addAccount'); }
                }
                break;
            case 'editAccount':
                result = await FirebaseService.editAccount(payload.accountId, payload.newName, companyId, payload.mobile, payload.gstin, payload.rechargeDate);
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
async function sheetsApiRequest(payload) {
    const MAX_RETRIES = 2;
    const BASE_TIMEOUT_MS = 45000;
    let lastErr = null;

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
        const controller = new AbortController();
        const timeoutMs = BASE_TIMEOUT_MS + (attempt * 10000);
        const timeout = setTimeout(() => controller.abort(), timeoutMs);
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
            lastErr = e;
            const isTimeout = e && e.name === 'AbortError';
            if (attempt < MAX_RETRIES && (isTimeout || /network|timeout|fetch/i.test(String(e.message || '')))) {
                await new Promise(r => setTimeout(r, 800 * (attempt + 1)));
                continue;
            }
            if (isTimeout) throw new Error(`Sheets API timeout (${Math.round(timeoutMs/1000)}s)`);
            throw e;
        }
    }

    throw lastErr || new Error('Sheets request failed');
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
        showToast(`Fetching ${month} data from Firestore…`, 'info');
        const res = await apiRequest({ action: 'getDashboardData', companyId, month });
        const list = Array.isArray(res?.data) ? res.data : [];
        if (companyId === 'company1') AppState.company1Data = list;
        else AppState.company2Data = list;
        await loadAvailableSheetMonths(true);
        await renderDataSheet();
        showToast(`Loaded ${list.length} records from Firestore.`, 'success');
    } catch (e) {
        console.error('Load historical failed:', e);
        showToast('Failed to load from Firestore', 'error');
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
        accounts = getOrderedCompanyAccounts('company1').map(ad => ({ id: ad.accountId, name: ad.name, company: 'company1', label: getCompanyDisplayName('company1') }));
        rawData = [...AppState.company1Data];
    } else if (companyFilter === 'company2') {
        accounts = getOrderedCompanyAccounts('company2').map(ad => ({ id: ad.accountId, name: ad.name, company: 'company2', label: getCompanyDisplayName('company2') }));
        rawData = [...AppState.company2Data];
    } else {
        getOrderedCompanyAccounts('company1').forEach(ad => accounts.push({ id: ad.accountId, name: ad.name, company: 'company1', label: getCompanyDisplayName('company1') }));
        getOrderedCompanyAccounts('company2').forEach(ad => accounts.push({ id: ad.accountId, name: ad.name, company: 'company2', label: getCompanyDisplayName('company2') }));
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
        
        // If a specific month is selected and no data locally, offer one-click Firestore reload.
        if (monthFilter && monthFilter !== 'all' && monthFilter !== currentMonth) {
            const compId = companyFilter === 'all' ? 'company1' : companyFilter; // Default to comp1 if all
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
    
    // Build lookup: { date -> { accountId -> { meesho, total, company } } }
    const lookup = {};
    rawData.forEach(r => {
        const d = normalizeToISODate(r.date);
        if (!d) return;
        if (!lookup[d]) lookup[d] = {};
        const key = r.accountId || r.accountName;
        if (!lookup[d][key]) lookup[d][key] = { meesho: 0, total: 0 };
        lookup[d][key].meesho += parseInt(r.meesho) || 0;
        lookup[d][key].total += parseInt(r.total) || 0;
    });
    
    // Load remarks (from backend first time, then cached)
    const savedRemarks = await loadRemarksFromBackend();
    if (renderSeq !== _renderDataSheetSeq) return;
    
    const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    
    // === HEADER ===
    let headerRow1 = '<tr><th class="sheet-date-col">Date</th>';
    accounts.forEach((acc, idx) => {
        const compTag = companyFilter === 'all' ? `<span class="sheet-company-tag">${acc.label}</span>` : '';
        headerRow1 += `<th class="sheet-acct-group sheet-acct-border" title="${String(acc.name).replace(/"/g, '&quot;')}"><span class="sheet-header-name">${acc.name}</span>${compTag}</th>`;
    });
    headerRow1 += '<th class="sheet-acct-group sheet-total-col">Total</th>';
    headerRow1 += '<th class="sheet-acct-group sheet-remarks-col">Remarks</th>';
    headerRow1 += '</tr>';
    
    let headerRow2 = ''; // Removed second header row for headers
    thead.innerHTML = headerRow1 + headerRow2;
    
    // === BODY ===
    let bodyHtml = '';
    const grandTotals = { meesho: new Array(accounts.length).fill(0), total: 0 };
    const monthTotals = {};
    dates.forEach(date => {
        const monthKey = date.substring(0, 7);
        if (!monthTotals[monthKey]) monthTotals[monthKey] = { total: 0, accountTotals: new Array(accounts.length).fill(0) };
        accounts.forEach((acc, idx) => {
            const cell = lookup[date]?.[acc.id] || lookup[date]?.[acc.name] || { meesho: 0, total: 0 };
            const qty = parseInt(cell.meesho, 10) || 0;
            monthTotals[monthKey].accountTotals[idx] += qty;
            monthTotals[monthKey].total += parseInt(cell.total, 10) || 0;
        });
    });
    let activeMonth = '';
    const fullColspan = accounts.length + 3;
    
    dates.forEach(date => {
        const [y, m, d] = date.split('-');
        const monthKey = `${y}-${m}`;
        if (monthKey !== activeMonth) {
            activeMonth = monthKey;
            const monthSummary = monthTotals[monthKey] || { total: 0 };
            bodyHtml += `<tr class="sheet-month-row" data-month="${monthKey}"><td colspan="${fullColspan}"><span>${getMonthLabelFromKey(monthKey)}</span><strong>Total Orders: <b class="sheet-month-total">${monthSummary.total}</b></strong></td></tr>`;
        }
        const dateObj = new Date(parseInt(y), parseInt(m)-1, parseInt(d));
        const dayName = dayNames[dateObj.getDay()];
        const isToday = date === todayStr;
        const dayChipClass = isToday ? 'sheet-day-chip sheet-today' : 'sheet-day-chip';
        const formattedDate = `${d}/${m}/${y}`;
        
        bodyHtml += `<tr data-month="${monthKey}">`;
        bodyHtml += `<td class="sheet-date-cell">${formattedDate} <span class="${dayChipClass}">${dayName}</span></td>`;
        
        let rowTotal = 0;
        accounts.forEach((acc, idx) => {
            const cell = lookup[date]?.[acc.id] || lookup[date]?.[acc.name] || { meesho: 0, total: 0 };
            const meeshoVal = parseInt(cell.meesho, 10) || 0;
            const cellTotal = parseInt(cell.total, 10) || 0;
            rowTotal += cellTotal;
            grandTotals.meesho[idx] += meeshoVal;
            
            bodyHtml += `<td class="sheet-editable sheet-acct-border"><input type="number" class="sheet-cell-input" value="${meeshoVal}" data-date="${date}" data-account-id="${escapeHtml(acc.id)}" data-account-name="${escapeHtml(acc.name)}" data-company="${acc.company}" data-field="meesho" min="0" placeholder="0"></td>`;
        });
        grandTotals.total += rowTotal;
        
        bodyHtml += `<td class="sheet-total-cell">${rowTotal}</td>`;
        
        const remarkVal = savedRemarks[date] || '';
        bodyHtml += `<td class="sheet-editable"><input type="text" class="sheet-cell-input sheet-remark-input" value="${escapeHtml(remarkVal)}" data-date="${date}" data-field="remark" placeholder="Add note..."></td>`;
        bodyHtml += '</tr>';
    });
    
    // Grand total row (hide for order users; they only need daily totals)
    if (!isOrderUser()) {
        bodyHtml += '<tr class="sheet-grand-row">';
        bodyHtml += '<td class="sheet-date-cell">TOTAL</td>';
        accounts.forEach((_, idx) => {
            bodyHtml += `<td class="sheet-acct-border">${grandTotals.meesho[idx]}</td>`;
        });
        bodyHtml += `<td class="sheet-total-cell">${grandTotals.total}</td>`;
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
    tbody.querySelectorAll('input[data-field="meesho"]').forEach(inp => {
        inp.addEventListener('change', () => {
            const { date, accountId, company, field } = inp.dataset;
            const value = parseInt(inp.value) || 0;
            
            // Update local state + local cache immediately (prevents temporary row disappear during refresh)
            applyOptimisticOrderUpdate(date, accountId, inp.dataset.accountName, value, company);
            
            // Recalculate totals in UI immediately
            recalcSheetRowTotal(inp);
            
            // Buffer the write to Firebase
            FirebaseService.bufferWrite(`order_${date}_${accountId}_${field}`, () =>
                apiRequest({ action: 'updateOrder', date, accountId, field, value, companyId: company })
            );
            scheduleQuickWriteFlush(1200);
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
    const monthKey = tr.dataset.month;
    if (monthKey) {
        const monthRow = tbody.querySelector(`.sheet-month-row[data-month="${monthKey}"]`);
        const monthTotalEl = monthRow?.querySelector('.sheet-month-total');
        if (monthTotalEl) {
            let monthTotal = 0;
            tbody.querySelectorAll(`tr[data-month="${monthKey}"]:not(.sheet-month-row)`).forEach(row => {
                const tc = row.querySelector('.sheet-total-cell');
                if (tc) monthTotal += parseInt(tc.textContent) || 0;
            });
            monthTotalEl.textContent = monthTotal;
        }
    }

    const grandRow = tbody.querySelector('.sheet-grand-row');
    if (!grandRow) return;
    const allRows = tbody.querySelectorAll('tr:not(.sheet-grand-row):not(.sheet-month-row)');
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
    
    table.querySelectorAll('tbody tr:not(.sheet-grand-row):not(.sheet-month-row)').forEach(tr => {
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
    const startLabel = formatKarigarBackupPoint(backup?.periodStartAt || backup?.snapshotAt || backup?.createdAt, '-');
    const endRaw = backup?.periodEndAt || '';
    const endLabel = endRaw ? formatKarigarBackupPoint(endRaw, '-') : 'Present';
    const rangeText = `${startLabel} → ${endLabel}`;
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
        AppState.designPricesByCompany[companyId] = map;
        AppState.designPrices = map;
    } catch (e) {
        console.warn('Size prices fallback: using cached/empty state', e);
        map = AppState.designPricesByCompany[companyId] || AppState.designPrices || {};
        rawHistoryMap = {};
    }

    const tbody = document.getElementById('size-price-tbody');
    const chart = document.getElementById('size-price-chart');
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
    if (!tbody || !chart || !form || !keyInput || !valueInput || !effectiveDateInput || !saveBtn || !recycleOpenBtn || !recycleTbody || !recycleModal || !recycleEmpty || !recycleCloseBtn || !historyModal || !historyTitle || !historyBody || !historyEmpty || !historyCloseBtn) return;

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

    tbody.innerHTML = rows.length ? rows.map(r => `
        <tr>
            <td>${r.key}</td>
            <td class=\"text-right\">₹${r.value.toFixed(2)}</td>
            <td class=\"text-center\">${formatISODateTimeForDisplay(r.activeFrom) || '-'}</td>
            <td class=\"text-center\">
                <button class=\"btn btn-outline btn-sm size-edit-btn\" data-key=\"${r.key}\" data-value=\"${r.value}\" data-effective=\"${r.activeFrom}\" ${isAdmin ? '' : 'disabled'}>
                    <i class='bx bx-edit'></i> Edit
                </button>
                <button class=\"btn btn-danger btn-sm size-delete-btn\" data-key=\"${r.key}\" ${isAdmin ? '' : 'disabled'}>
                    <i class='bx bx-trash'></i> Delete
                </button>
                <button class=\"btn btn-outline btn-sm size-history-btn\" data-key=\"${r.key}\">
                    <i class='bx bx-history'></i> History
                </button>
            </td>
        </tr>
    `).join('') : '<tr><td colspan=\"4\" class=\"text-center text-muted p-3\">No size/design prices saved yet.</td></tr>';

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

    const maxVal = rows.reduce((m, r) => Math.max(m, r.value), 0) || 1;
    chart.innerHTML = rows.slice(0, 20).map(r => `
        <div style=\"display:grid;grid-template-columns:120px 1fr 80px;gap:0.5rem;align-items:center;\">
            <div class=\"text-xs text-muted\">${r.key}</div>
            <div style=\"height:10px;background:#e2e8f0;border-radius:999px;overflow:hidden;\">
                <div style=\"height:100%;width:${Math.max(4, (r.value / maxVal) * 100)}%;background:#135bec;\"></div>
            </div>
            <div class=\"text-right text-xs font-bold\">₹${r.value.toFixed(2)}</div>
        </div>
    `).join('');

    if (!form.dataset.bound) {
        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            if (!isAdminUser()) return showToast('Only admin can update prices', 'error');
            const k = String(keyInput.value || '').trim().toUpperCase();
            const p = parseFloat(valueInput.value);
            const requestedEffectiveFrom = normalizeToISODateTime(effectiveDateInput.value, today) || `${today}T00:00:00`;
            const nowEffectiveFrom = normalizeToISODateTime(getCurrentLocalDateTimeInput(), today) || `${today}T00:00:00`;
            const effectiveFrom = requestedEffectiveFrom < nowEffectiveFrom ? nowEffectiveFrom : requestedEffectiveFrom;
            if (!k) return showToast('Enter size/design', 'error');
            if (!Number.isFinite(p) || p < 0) return showToast('Enter valid price', 'error');
            showLoader();
            try {
                if (requestedEffectiveFrom < nowEffectiveFrom) {
                    showToast('Past timeline is locked. Applied from current time.', 'info');
                }
                const r = await FirebaseService.upsertDesignPrice(k, p, buildAuditActor(), companyId, effectiveFrom, null);
                if (!r || r.success === false) throw new Error(r?.message || 'Failed');
                _pendingDataChangesForBackup = true;
                scheduleBackgroundSheetBackup('upsertDesignPrice');
                invalidateAllKarigarCaches();
                showToast('Price saved', 'success');
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

    tbody.querySelectorAll('.size-edit-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            if (!isAdminUser()) return;
            form.dataset.editMode = '1';
            form.dataset.editFromEffective = '';
            form.dataset.editFromKey = '';
            keyInput.value = btn.dataset.key || '';
            valueInput.value = btn.dataset.value || '';
            effectiveDateInput.value = getCurrentLocalDateTimeInput();
            saveBtn.innerHTML = "<i class='bx bx-save'></i> Save New Price";
            showToast('Past history is locked. New price will apply from selected time onward.', 'info');
            keyInput.focus();
        });
    });

    tbody.querySelectorAll('.size-delete-btn').forEach(btn => {
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
                invalidateAllKarigarCaches();
                showToast('Price deleted from active list', 'success');
                await renderSizePricesPage();
            } catch (err) {
                console.error(err);
                showToast(err.message || 'Failed to delete price', 'error');
            } finally {
                hideLoader();
            }
        });
    });

    tbody.querySelectorAll('.size-history-btn').forEach(btn => {
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
                invalidateAllKarigarCaches();
                showToast('Size price restored', 'success');
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
            AppState.designPrices = pRes.data || {};
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
    renderKarigarGrid();
    setupKarigarListeners();
}

function resolveKarigarIdFromState(karigarId, karigarName) {
    const rawId = String(karigarId || '').trim();
    if (rawId.startsWith('kar_')) return rawId;
    return '';
}

function rebuildKarigarComputedCache() {
    const txs = Array.isArray(AppState.karigarTransactions) ? AppState.karigarTransactions : [];
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
            s.totalUpad = round2(s.totalUpad + toAmount(t.amount));
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
        if (t.type === 'upad') totalUpad = round2(totalUpad + toAmount(t.amount));
    });
    const balance = round2(totalJama - totalUpad);
    return { totalJama, totalUpad, totalPics, balance, lastTx: tx[0] };
}

function renderKarigarGrid() {
    const grid = document.getElementById('karigar-grid');
    const searchInput = document.getElementById('karigar-search-input');
    const query = (searchInput.value || '').trim().toLowerCase();
    const btnCreate = document.getElementById('btn-create-karigar');
    const isAdmin = isAdminUser();
    
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
    
    const resetBtn = document.getElementById('btn-reset-karigar');
    
    if (resetBtn) {
        if (!isAdmin) {
            resetBtn.style.display = 'none';
        } else {
            const targetCompanyId = String(AppState.currentCompany || 'company1').trim() || 'company1';
            const targetCompanyName = getCompanyDisplayName(targetCompanyId);
            resetBtn.style.display = 'block';
            resetBtn.classList.remove('btn-danger', 'text-white');
            resetBtn.classList.add('btn-outline', 'text-danger');
            resetBtn.innerHTML = `<i class='bx bx-refresh'></i> Reset ${targetCompanyName} Karigar Data`;

            const newResetBtn = resetBtn.cloneNode(true);
            resetBtn.parentNode.replaceChild(newResetBtn, resetBtn);
            newResetBtn.addEventListener('click', () => confirmKarigarMonthlyReset(targetCompanyId));
        }
    }
    
    let html = '';
    let globalBalanceSum = 0;
    let globalPicsSum = 0;
    
    filtered.forEach(k => {
        const stats = calculateKarigarBalance(k.id, k.name);
        globalBalanceSum += stats.balance;
        globalPicsSum += (parseInt(stats.totalPics, 10) || 0);
        
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
                    <div class="karigar-balance-val" style="background: ${isNegative ? '#fee2e2' : '#dcfce7'}; color: ${balColor}; padding: 0.5rem 0.75rem; border-radius: 8px; font-weight: bold; text-align: right;">
                        <span style="font-size: 0.7rem; display:block; text-transform:uppercase; margin-bottom: -2px;">Balance</span>
                        <span class="actual-bal">₹${Math.abs(stats.balance).toFixed(2)}${isNegative ? ' Due' : ' Clear'}</span>
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
                    <div style="width: 1px; background: var(--border-color);"></div>
                    <div>
                        <span class="text-xs text-muted" style="display:block;">Total Pics</span>
                        <strong class="text-primary">${stats.totalPics}</strong>
                    </div>
                </div>

                <div class="flex gap-2 w-100" style="margin-top: auto; display:grid; grid-template-columns: 1fr 1fr; gap:0.5rem;">
                    <button class="btn btn-outline btn-karigar-upad ${isAdmin ? '' : 'disabled'}" data-id="${k.id}" data-name="${k.name.replace(/"/g, '&quot;')}" ${isAdmin ? '' : 'disabled title="Only admin can add Borrow (Upad)"'}>
                        <i class='bx bx-minus-circle'></i> Borrow (Upad)
                    </button>
                    <button class="btn btn-primary btn-karigar-jama" data-id="${k.id}" data-name="${k.name.replace(/"/g, '&quot;')}">
                        <i class='bx bx-plus-circle'></i> Maal (Jama)
                    </button>
                    <button class="btn btn-outline btn-sm btn-karigar-history" data-id="${k.id}" data-name="${k.name.replace(/"/g, '&quot;')}" style="grid-column: span 2;">
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
}

function setupKarigarListeners() {
    const searchInput = document.getElementById('karigar-search-input');
    if (searchInput && !searchInput.dataset.bound) {
        searchInput.addEventListener('input', renderKarigarGrid);
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
                await FirebaseService.addKarigarJama({ 
                    karigarId, karigarName, date, transactionDateTime, designName, size, pic, price, upadAmount, 
                    companyId: AppState.currentCompany,
                    addedBy: AppState.currentUser?.role || 'admin',
                    actor: buildAuditActor()
                });
                _pendingDataChangesForBackup = true;
                scheduleBackgroundSheetBackup('addKarigarJama');
                showToast("Maal (Jama) saved!", "success");

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
    bindKarigarFormEnterToSubmit(jamaForm, document.getElementById('karigar-jama-modal'));
    
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
            const transactionDateTime = combineDateAndTimeToISO(date, time);
            if (!transactionDateTime) {
                showToast('Invalid date/time', 'error');
                upadForm.dataset.submitting = 'false';
                if (submitBtn) submitBtn.disabled = false;
                return;
            }
            
            showLoader();
            try {
                await FirebaseService.addKarigarUpad({ 
                    karigarId, karigarName, date, transactionDateTime, amount, 
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
            } catch (err) { showToast("Failed to add", "error"); }
            finally {
                hideLoader();
                upadForm.dataset.submitting = 'false';
                if (submitBtn) submitBtn.disabled = false;
            }
        });
        upadForm.dataset.bound = 'true';
    }
    bindKarigarFormEnterToSubmit(upadForm, document.getElementById('karigar-upad-modal'));
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
        const karigars = Array.isArray(AppState.karigars) ? [...AppState.karigars] : [];
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

    // Bulk entry UX: focus design field immediately.
    setTimeout(() => {
        document.getElementById('karigar-jama-design')?.focus();
    }, 0);
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
        const karigars = Array.isArray(AppState.karigars) ? [...AppState.karigars] : [];
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

function openKarigarHistoryModal(id, name) {
    const resolvedId = resolveKarigarIdFromState(id, name);
    if (!resolvedId) return showToast('Invalid karigar ID', 'error');
    const txs = getKarigarTransactionsFor(resolvedId);
    document.getElementById('karigar-history-name-display').textContent = name;
    const tbody = document.getElementById('karigar-history-tbody');
    tbody.innerHTML = '';
    
    let totalJama = 0;
    let totalUpad = 0;
    
    if (txs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="text-center p-4 text-muted">No transactions recorded for this karigar.</td></tr>';
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

async function deleteKarigarHistory(txId, karigarId, karigarName) {
    if (!isAdminUser()) return showToast('Only admin can delete transaction', 'error');
    if (!confirm('Are you certain you want to specifically delete this record? This cannot be undone.')) return;
    
    showLoader();
    try {
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
    const tx = (AppState.karigarTransactions || []).find(t => t.id === txId);
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
        updates.price = parseFloat(newPrice) || 0;
        updates.upadAmount = parseFloat(newUpad) || 0;
        updates.type = 'jama';
    } else {
        const newAmount = prompt('Borrow Amount:', String(tx.amount || 0));
        if (newAmount === null) return;
        updates.amount = parseFloat(newAmount) || 0;
        updates.type = 'upad';
    }

    showLoader();
    try {
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
    if (!confirm(`This will backup ALL ${targetCompanyName} Karigar data to Google Sheets and instantly clear ONLY ${targetCompanyName} Karigar transactions for new month.\n\nAre you sure you want to proceed?`)) return;
    
    showLoader();
    try {
        showToast(`Initiating ${targetCompanyName} backup to Google Sheets...`, 'info');
        
        // Ensure all pending writes are flushed first
        await FirebaseService.flushWrites();
        
        // Full database backup is the safest way so nothing else gets missed
        const allData = await FirebaseService.getAllDataForBackup();
        const response = await sheetsApiRequest({ action: 'saveFullBackup', data: allData });
        
        if (response && response.success) {
            showToast(`Backup successful. Clearing ${targetCompanyName} data...`, 'info');
            const txRes = await FirebaseService.getKarigarTransactions(safeCompanyId);
            const backupRows = (Array.isArray(txRes?.data) ? txRes.data : [])
                .filter(tx => String(tx.companyId || '').trim() === safeCompanyId)
                .map(tx => ({ ...tx }));
            await FirebaseService.createKarigarResetBackup({
                companyId: safeCompanyId,
                rows: backupRows,
                actor: buildAuditActor()
            });
            
            // Delete from firestore
            const deleted = await FirebaseService.clearKarigarMonthlyData(safeCompanyId, buildAuditActor());
            _pendingDataChangesForBackup = true;
            scheduleBackgroundSheetBackup(`clearKarigarMonthlyData_${safeCompanyId}`);
            
            // Update UI & memory state
            if (String(AppState.currentCompany || '').trim() === safeCompanyId) {
                AppState.karigarTransactions = [];
                rebuildKarigarComputedCache();
            }
            invalidateKarigarCache(safeCompanyId);
            const currentMonth = getTodayISODate().substring(0, 7);
            localStorage.setItem(`karigar_last_reset_month_${safeCompanyId}`, currentMonth);
            if (AppState.karigarResetBackupsByCompany && typeof AppState.karigarResetBackupsByCompany === 'object') {
                delete AppState.karigarResetBackupsByCompany[safeCompanyId];
            }
            if (AppState.selectedKarigarResetBackupIdByCompany && typeof AppState.selectedKarigarResetBackupIdByCompany === 'object') {
                delete AppState.selectedKarigarResetBackupIdByCompany[safeCompanyId];
            }
            if (String(AppState.currentCompany || '').trim() === safeCompanyId) {
                AppState.karigarResetBackups = [];
                AppState.selectedKarigarResetBackupId = '';
            }
            
            showToast(`${targetCompanyName} reset complete! Cleared ${deleted} records.`, 'success');
            if (String(AppState.currentCompany || '').trim() === safeCompanyId) {
                renderKarigarGrid();
            }
        } else {
            showToast(`Backup failed for ${targetCompanyName}. Local data was kept safe.`, 'error');
        }
    } catch (e) {
        console.error('Reset error:', e);
        showToast(`System error during ${targetCompanyName} reset.`, 'error');
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

    settingsBtn.addEventListener('click', () => modal.classList.add('show'));
    indicator.addEventListener('click', () => {
        if (LanStorageService.isReadOnly()) {
            LanStorageService.requestUnlock().then(success => {
                if (success) showToast("LAN folder unlocked successfully!", "success");
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

    fileInput.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (!file) return;

        const reader = new FileReader();
        reader.onload = async (evt) => {
            showLoader("Bootstrapping LAN folder...");
            try {
                const payload = JSON.parse(evt.target.result);
                await LanStorageService.bootstrapFolderFromBackup(payload);
                showToast("LAN folder successfully bootstrapped from backup!", "success");
                loadInitialData();
            } catch (err) {
                showToast("Import failed: " + err.message, "error");
            } finally {
                hideLoader();
                fileInput.value = "";
            }
        };
        reader.readAsText(file);
    });

    if (LanStorageService.hasSavedHandle()) {
        LanStorageService.autoReconnect().then(success => {
            if (success) {
                console.log("LAN shared folder auto-reconnected successfully.");
            } else {
                console.log("LAN shared folder locked. Click indicator to authorize.");
            }
        });
    } else {
        syncUI("disconnected");
    }
}
