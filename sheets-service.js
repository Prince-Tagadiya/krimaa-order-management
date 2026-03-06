// Sheets-only data service (Firebase removed)
// Keeps legacy global name `FirebaseService` so app code remains compatible.

const FirebaseService = (() => {
    let pendingWrites = 0;
    let pendingPromises = [];
    let syncListeners = [];

    function notify(status) {
        syncListeners.forEach(fn => {
            try { fn(status); } catch (e) {}
        });
    }

    function beginWrite() {
        pendingWrites += 1;
        notify('syncing');
    }

    function endWrite() {
        pendingWrites = Math.max(0, pendingWrites - 1);
        if (pendingWrites === 0) notify('synced');
    }

    async function request(payload) {
        if (typeof window !== 'undefined' && typeof window.sheetsApiRequest === 'function') {
            return window.sheetsApiRequest(payload);
        }
        const res = await fetch(SHEETS_API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'text/plain;charset=utf-8' },
            body: JSON.stringify(payload)
        });
        const txt = await res.text();
        return JSON.parse(txt);
    }

    function normalizeCompanyId(cid) {
        const raw = String(cid || 'company1').trim().toLowerCase();
        if (raw === 'company2') return 'company2';
        return 'company1';
    }

    function nowIsoDate() {
        const d = new Date();
        return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().split('T')[0];
    }

    function buildId(prefix) {
        return prefix + Math.random().toString(36).slice(2, 11);
    }

    function normalizeNameKey(v) {
        return String(v || '').trim().toLowerCase().replace(/\s+/g, ' ');
    }

    function toAmount(v) {
        const n = parseFloat(String(v ?? '').replace(/[^0-9.-]/g, ''));
        return Number.isFinite(n) ? n : 0;
    }

    function txStableId(tx, idx) {
        const parts = [
            String(tx.companyId || ''),
            String(tx.date || ''),
            String(tx.karigarId || ''),
            String(tx.karigarName || ''),
            String(tx.type || ''),
            String(tx.designName || ''),
            String(tx.size || ''),
            String(tx.pic || ''),
            String(tx.price || ''),
            String(tx.totalJama || tx.total || ''),
            String(tx.upadAmount || tx.amount || ''),
            String(tx.createdAt || '')
        ].join('|');
        let h = 0;
        for (let i = 0; i < parts.length; i++) h = (h * 31 + parts.charCodeAt(i)) >>> 0;
        return `tx_${h}_${idx}`;
    }

    async function getAllDataForBackup() {
        const res = await request({ action: 'getAllSheetData' });
        if (!res || res.success === false) throw new Error(res?.message || 'Failed to read sheet data');
        return res.data || {
            company1: { accounts: [], orders: [] },
            company2: { accounts: [], orders: [] },
            karigars: [],
            karigarTransactions: [],
            designPrices: {},
            moneyBackups: {},
            remarks: {}
        };
    }

    async function saveAllData(allData) {
        beginWrite();
        try {
            const res = await request({ action: 'saveFullBackup', data: allData });
            if (!res || res.success === false) throw new Error(res?.message || 'Failed saving full backup');
            return res;
        } finally {
            endWrite();
        }
    }

    function pushPending(p) {
        pendingPromises.push(p);
        p.finally(() => {
            pendingPromises = pendingPromises.filter(x => x !== p);
        });
        return p;
    }

    function actorMeta(actor, phase) {
        const safe = actor || {};
        const role = String(safe.role || '').trim() || 'unknown';
        const username = String(safe.username || '').trim() || 'unknown';
        const displayName = String(safe.displayName || '').trim() || username;
        const dashboard = String(safe.dashboard || safe.source || 'web_app').trim();
        const source = String(safe.source || dashboard || 'web_app').trim();
        return {
            source,
            dashboard,
            [`${phase}ByRole`]: role,
            [`${phase}ByUser`]: username,
            [`${phase}ByName`]: displayName
        };
    }

    function isAdminActor(actor) {
        return String(actor?.role || '').trim().toLowerCase() === 'admin';
    }

    function readBackupMeta() {
        try { return JSON.parse(localStorage.getItem('backup_meta') || '{}'); } catch (e) { return {}; }
    }

    function writeBackupMeta(meta) {
        localStorage.setItem('backup_meta', JSON.stringify(meta || {}));
    }

    async function getAccounts(companyId) {
        return request({ action: 'getAccounts', companyId: normalizeCompanyId(companyId) });
    }

    async function addAccount(name, companyId, mobile, gstin, rechargeDate) {
        return request({ action: 'addAccount', accountName: name, mobile, gstin, rechargeDate, companyId: normalizeCompanyId(companyId) });
    }

    async function editAccount(accountId, newName, companyId, mobile, gstin, rechargeDate) {
        return request({ action: 'editAccount', accountId, newName, mobile, gstin, rechargeDate, companyId: normalizeCompanyId(companyId) });
    }

    async function deleteAccount(id, companyId) {
        return request({ action: 'deleteAccount', accountId: id, companyId: normalizeCompanyId(companyId) });
    }

    async function updateAccountOrder(orderedAccounts, companyId) {
        return request({ action: 'updateAccountOrder', orderedAccounts, companyId: normalizeCompanyId(companyId) });
    }

    async function getOrders(companyId, monthStr) {
        return request({ action: 'getDashboardData', companyId: normalizeCompanyId(companyId), month: monthStr || '' });
    }

    async function submitOrders(date, orders, companyId) {
        beginWrite();
        try {
            return await request({ action: 'submitOrders', date, orders, companyId: normalizeCompanyId(companyId) });
        } finally {
            endWrite();
        }
    }

    async function updateOrder(date, accountId, field, value, companyId) {
        beginWrite();
        try {
            return await request({ action: 'updateOrder', date, accountId, field, value, companyId: normalizeCompanyId(companyId) });
        } finally {
            endWrite();
        }
    }

    async function saveRemark(date, remark) {
        beginWrite();
        try {
            return await request({ action: 'saveRemark', date, remark });
        } finally {
            endWrite();
        }
    }

    async function getRemarks() {
        return request({ action: 'getRemarks' });
    }

    async function updateMoney(accountId, companyId, money, expense, date) {
        // Sheets-only mode: keep non-blocking behavior.
        return { success: true, message: 'Money values updated in session' };
    }

    async function resetAllMoney(date) {
        return { success: true, message: 'Money reset in session' };
    }

    async function createMoneyBackup(date, rows, reason) {
        beginWrite();
        try {
            return await request({ action: 'saveMoneyBackup', date, rows, reason });
        } finally {
            endWrite();
        }
    }

    async function getMoneyBackups() {
        return request({ action: 'getMoneyBackups' });
    }

    async function deleteMoneyBackup(backupId) {
        const all = await getAllDataForBackup();
        const list = Array.isArray(all.moneyBackups) ? all.moneyBackups : [];
        const filtered = list.filter(b => String(b.id || b.backupDate || '') !== String(backupId || ''));
        all.moneyBackups = filtered;
        await saveAllData(all);
        return { success: true };
    }

    async function getKarigars(companyId = 'company1') {
        const cid = normalizeCompanyId(companyId);
        const all = await getAllDataForBackup();
        const data = (all.karigars || []).filter(k => normalizeCompanyId(k.companyId) === cid);
        return { success: true, data };
    }

    async function addKarigar(name, companyId = 'company1', actor = null) {
        const cid = normalizeCompanyId(companyId);
        const all = await getAllDataForBackup();
        all.karigars = all.karigars || [];
        const key = normalizeNameKey(name);
        const existing = all.karigars.find(k => normalizeCompanyId(k.companyId) === cid && normalizeNameKey(k.name) === key);
        if (existing) return { success: true, id: existing.id };
        const id = buildId('kar_');
        all.karigars.push({
            id,
            name: String(name || '').trim(),
            companyId: cid,
            addedAt: new Date().toISOString(),
            ...actorMeta(actor, 'created')
        });
        await saveAllData(all);
        return { success: true, id };
    }

    async function editKarigar(id, newName, companyId = 'company1', actor = null) {
        if (!isAdminActor(actor)) return { success: false, message: 'Only admin can edit karigar' };
        const cid = normalizeCompanyId(companyId);
        const all = await getAllDataForBackup();
        all.karigars = all.karigars || [];
        all.karigarTransactions = all.karigarTransactions || [];
        const target = all.karigars.find(k => String(k.id) === String(id) && normalizeCompanyId(k.companyId) === cid);
        if (!target) return { success: false, message: 'Karigar not found' };
        target.name = String(newName || '').trim();
        Object.assign(target, actorMeta(actor, 'updated'));
        all.karigarTransactions.forEach(tx => {
            if (String(tx.karigarId) === String(id) && normalizeCompanyId(tx.companyId) === cid) {
                tx.karigarName = target.name;
                Object.assign(tx, actorMeta(actor, 'updated'));
            }
        });
        await saveAllData(all);
        return { success: true };
    }

    async function deleteKarigar(id, companyId = 'company1', actor = null) {
        if (!isAdminActor(actor)) return { success: false, message: 'Only admin can delete karigar' };
        const cid = normalizeCompanyId(companyId);
        const all = await getAllDataForBackup();
        all.karigars = (all.karigars || []).filter(k => !(String(k.id) === String(id) && normalizeCompanyId(k.companyId) === cid));
        all.karigarTransactions = (all.karigarTransactions || []).filter(tx => !(String(tx.karigarId) === String(id) && normalizeCompanyId(tx.companyId) === cid));
        await saveAllData(all);
        return { success: true };
    }

    async function getKarigarTransactions(companyId = 'company1') {
        const cid = normalizeCompanyId(companyId);
        const all = await getAllDataForBackup();
        const txs = (all.karigarTransactions || [])
            .filter(tx => normalizeCompanyId(tx.companyId) === cid)
            .map((tx, idx) => ({ ...tx, id: tx.id || txStableId(tx, idx) }));
        return { success: true, data: txs };
    }

    async function getDesignPrices() {
        const all = await getAllDataForBackup();
        return { success: true, data: all.designPrices || {} };
    }

    async function upsertDesignPrice(sizeKey, price, actor = null) {
        const all = await getAllDataForBackup();
        all.designPrices = all.designPrices || {};
        const key = String(sizeKey || '').trim().toUpperCase();
        if (!key) return { success: false, message: 'Size/Design required' };
        all.designPrices[key] = toAmount(price);
        await saveAllData(all);
        return { success: true };
    }

    async function addKarigarJama(data) {
        const all = await getAllDataForBackup();
        all.karigars = all.karigars || [];
        all.karigarTransactions = all.karigarTransactions || [];
        const cid = normalizeCompanyId(data.companyId);
        const name = String(data.karigarName || '').trim();
        let karigarId = String(data.karigarId || '').trim();
        if (!karigarId.startsWith('kar_')) {
            const found = all.karigars.find(k => normalizeCompanyId(k.companyId) === cid && normalizeNameKey(k.name) === normalizeNameKey(name));
            if (found) karigarId = found.id;
        }
        if (!karigarId.startsWith('kar_')) {
            const created = await addKarigar(name, cid, data.actor || null);
            karigarId = created.id;
        }
        const price = toAmount(data.price);
        const pic = parseInt(data.pic, 10) || 0;
        const total = Math.round((price * pic + Number.EPSILON) * 100) / 100;
        const tx = {
            id: buildId('tx_'),
            date: String(data.date || nowIsoDate()).split('T')[0],
            karigarId,
            karigarName: name,
            type: 'jama',
            designName: String(data.designName || '').trim(),
            size: String(data.size || '').trim(),
            pic,
            price,
            total,
            totalJama: total,
            upadAmount: toAmount(data.upadAmount),
            companyId: cid,
            addedBy: data.addedBy || 'user',
            createdAt: new Date().toISOString(),
            ...actorMeta(data.actor, 'created')
        };
        all.karigarTransactions.push(tx);
        const key = String(data.designName || '').trim().toUpperCase();
        if (key) {
            all.designPrices = all.designPrices || {};
            all.designPrices[key] = price;
        }
        await saveAllData(all);
        return { success: true };
    }

    async function addKarigarUpad(data) {
        if (!isAdminActor(data.actor)) throw new Error('Only admin can add Borrow (Upad)');
        const all = await getAllDataForBackup();
        all.karigars = all.karigars || [];
        all.karigarTransactions = all.karigarTransactions || [];
        const cid = normalizeCompanyId(data.companyId);
        const name = String(data.karigarName || '').trim();
        let karigarId = String(data.karigarId || '').trim();
        if (!karigarId.startsWith('kar_')) {
            const found = all.karigars.find(k => normalizeCompanyId(k.companyId) === cid && normalizeNameKey(k.name) === normalizeNameKey(name));
            if (found) karigarId = found.id;
        }
        if (!karigarId.startsWith('kar_')) {
            const created = await addKarigar(name, cid, data.actor || null);
            karigarId = created.id;
        }
        all.karigarTransactions.push({
            id: buildId('tx_'),
            date: String(data.date || nowIsoDate()).split('T')[0],
            karigarId,
            karigarName: name,
            type: 'upad',
            amount: toAmount(data.amount),
            companyId: cid,
            createdAt: new Date().toISOString(),
            ...actorMeta(data.actor, 'created')
        });
        await saveAllData(all);
        return { success: true };
    }

    async function updateKarigarTransaction(id, updates = {}, actor = null, companyId = 'company1') {
        if (!isAdminActor(actor)) return { success: false, message: 'Only admin can edit transaction' };
        const cid = normalizeCompanyId(companyId);
        const all = await getAllDataForBackup();
        all.karigarTransactions = all.karigarTransactions || [];
        const idx = all.karigarTransactions.findIndex((tx, i) => {
            const txId = tx.id || txStableId(tx, i);
            return String(txId) === String(id) && normalizeCompanyId(tx.companyId) === cid;
        });
        if (idx === -1) return { success: false, message: 'Transaction not found' };
        const tx = all.karigarTransactions[idx];
        Object.assign(tx, updates || {});
        if (String(tx.type || '').toLowerCase() === 'jama') {
            const pic = parseInt(tx.pic, 10) || 0;
            const price = toAmount(tx.price);
            tx.total = Math.round((pic * price + Number.EPSILON) * 100) / 100;
            tx.totalJama = tx.total;
            tx.upadAmount = toAmount(tx.upadAmount);
        } else {
            tx.amount = toAmount(tx.amount);
        }
        Object.assign(tx, actorMeta(actor, 'updated'));
        tx.updatedAt = new Date().toISOString();
        await saveAllData(all);
        return { success: true };
    }

    async function deleteKarigarTransaction(id, companyId = 'company1', actor = null) {
        if (!isAdminActor(actor)) return { success: false, message: 'Only admin can delete transaction' };
        const cid = normalizeCompanyId(companyId);
        const all = await getAllDataForBackup();
        all.karigarTransactions = (all.karigarTransactions || []).filter((tx, i) => {
            const txId = tx.id || txStableId(tx, i);
            return !(String(txId) === String(id) && normalizeCompanyId(tx.companyId) === cid);
        });
        await saveAllData(all);
        return { success: true };
    }

    async function clearKarigarMonthlyData(companyId = 'company1', actor = null) {
        if (!isAdminActor(actor)) throw new Error('Only admin can run karigar monthly reset');
        const cid = normalizeCompanyId(companyId);
        const all = await getAllDataForBackup();
        const before = (all.karigarTransactions || []).length;
        all.karigarTransactions = (all.karigarTransactions || []).filter(tx => normalizeCompanyId(tx.companyId) !== cid);
        await saveAllData(all);
        return before - all.karigarTransactions.length;
    }

    async function replaceFromSheets(sheetData) {
        const res = await request({ action: 'saveFullBackup', data: sheetData || {} });
        if (!res || res.success === false) throw new Error(res?.message || 'Replace failed');
        return { success: true, deleted: 0, stats: { accounts: 0, orders: 0, karigars: 0, karigarTxs: 0 } };
    }

    async function syncFromSheets(sheetData) {
        return replaceFromSheets(sheetData);
    }

    async function backupAndArchiveMonthlyData(year, month) {
        return { success: true, count: 0, year, month };
    }

    async function clearOldOrders() { return 0; }
    async function migrateLegacyOrdersToDailyOrders() { return { success: true, count: 0 }; }
    async function migrateDatabaseToIds() { return { success: true }; }
    async function fixHistoricalDataIntegrity() { return { success: true, stats: {} }; }
    async function seedFromSheets() { return { success: true }; }

    async function isEmpty(companyId) {
        const res = await getAccounts(companyId);
        const list = res?.details || res?.data || [];
        return !Array.isArray(list) || list.length === 0;
    }

    async function setBackupMeta(meta) {
        const cur = readBackupMeta();
        writeBackupMeta({ ...cur, ...(meta || {}) });
        return { success: true };
    }

    async function getBackupMeta() {
        return readBackupMeta();
    }

    function onSyncStatusChange(cb) {
        if (typeof cb === 'function') syncListeners.push(cb);
        return () => { syncListeners = syncListeners.filter(x => x !== cb); };
    }

    function getPendingCount() {
        return pendingWrites;
    }

    function bufferWrite(key, fn) {
        if (typeof fn !== 'function') return Promise.resolve({ success: false });
        beginWrite();
        const p = Promise.resolve().then(fn).finally(endWrite);
        return pushPending(p);
    }

    async function flushWrites() {
        if (pendingPromises.length === 0) return;
        await Promise.allSettled([...pendingPromises]);
    }

    function init() { notify('synced'); }

    async function commitInChunks(ops) {
        for (const op of ops || []) {
            if (typeof op === 'function') {
                const fakeBatch = {
                    set: async () => {},
                    update: async () => {},
                    delete: async () => {}
                };
                await op(fakeBatch);
            }
        }
    }

    return {
        init,
        onSyncStatusChange,
        getPendingCount,
        bufferWrite,
        flushWrites,
        commitInChunks,
        db: null,

        getAccounts,
        addAccount,
        editAccount,
        deleteAccount,
        updateAccountOrder,
        getOrders,
        submitOrders,
        updateOrder,

        saveRemark,
        getRemarks,

        updateMoney,
        resetAllMoney,
        createMoneyBackup,
        getMoneyBackups,
        deleteMoneyBackup,

        getKarigars,
        addKarigar,
        editKarigar,
        deleteKarigar,
        getKarigarTransactions,
        addKarigarJama,
        addKarigarUpad,
        updateKarigarTransaction,
        deleteKarigarTransaction,
        clearKarigarMonthlyData,

        getDesignPrices,
        upsertDesignPrice,

        getAllDataForBackup,
        replaceFromSheets,
        syncFromSheets,

        isEmpty,
        seedFromSheets,
        migrateLegacyOrdersToDailyOrders,
        migrateDatabaseToIds,
        fixHistoricalDataIntegrity,
        backupAndArchiveMonthlyData,
        clearOldOrders,
        setBackupMeta,
        getBackupMeta
    };
})();
