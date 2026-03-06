// ===== FIREBASE SERVICE =====
// All Firestore CRUD operations for accounts, orders, remarks, and backups.
// Uses firebase-compat SDK loaded via CDN.

const FirebaseService = (() => {
    let db = null;
    let _initialized = false;

    // Sync status: idle | pending | syncing | saved | error
    let _syncStatus = 'idle';
    const _syncListeners = [];

    // Write buffer for Data Sheet edits
    const _pendingWrites = new Map();
    let _flushTimer = null;

    // ───── INIT ─────
    function init() {
        if (_initialized) return;
        firebase.initializeApp(FIREBASE_CONFIG);
        db = firebase.firestore();
        
        // Enable offline persistence for anti-data loss
        db.enablePersistence({ synchronizeTabs: true })
          .catch(err => console.warn('Persistence error:', err));
          
        _initialized = true;
    }

    function getDb() { init(); return db; }

    // ───── SYNC STATUS ─────
    function setSyncStatus(s) {
        _syncStatus = s;
        _syncListeners.forEach(fn => fn(s));
    }
    function onSyncStatusChange(fn) { _syncListeners.push(fn); }
    function getSyncStatus() { return _syncStatus; }

    // ───── HELPERS ─────
    // Firestore batch limit is 500 ops. This helper splits automatically.
    async function commitInChunks(operations) {
        const CHUNK = 450;
        for (let i = 0; i < operations.length; i += CHUNK) {
            const batch = db.batch();
            operations.slice(i, i + CHUNK).forEach(op => op(batch));
            await batch.commit();
        }
    }

    // Maps for global ID-to-name resolution
    let accountNameIdMap = {}; // name -> id
    let accountIdNameMap = {}; // id -> name
    let _allAccountsMap = {}; // id -> data
    let _allKarigarsMap = {}; // id -> data

    function buildMoneyRecordDocId(date, companyId, accountId) {
        return [date, companyId, accountId]
            .map(v => encodeURIComponent(String(v || '').trim()))
            .join('_');
    }

    function buildPrefixedId(prefix) {
        return `${prefix}${Math.random().toString(36).substr(2, 9)}`;
    }

    function isPrefixedId(value, prefix) {
        return String(value || '').trim().startsWith(prefix);
    }

    function normalizeNameKey(value) {
        return String(value || '').trim().toLowerCase();
    }

    function parseFlexibleDateValue(rawValue) {
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

        const parsed = new Date(str);
        if (!isNaN(parsed.getTime())) return parsed;
        return null;
    }

    async function getAccounts(companyId) {
        init();
        let snap;
        try {
            snap = await db.collection('accounts')
                .where('companyId', '==', companyId)
                .orderBy('position')
                .get();
        } catch (e) {
            console.warn('Index missing for accounts, using fallback sort:', e.message);
            snap = await db.collection('accounts')
                .where('companyId', '==', companyId)
                .get();
        }
        const docs = [];
        snap.forEach(doc => {
            const data = doc.data();
            const id = data.accountId || doc.id;
            accountNameIdMap[data.name.trim()] = id;
            accountIdNameMap[id] = data.name.trim();
            _allAccountsMap[id] = data;
            docs.push(data);
        });
        docs.sort((a, b) => (a.position || 0) - (b.position || 0));
        return { success: true, data: docs.map(d => d.name), details: docs };
    }

    async function addAccount(name, companyId, mobile, gstin, rechargeDate) {
        init();
        name = (name || '').trim();
        if (!name) return { success: false, message: 'Account name cannot be empty' };
        // Duplicate check
        const dup = await db.collection('accounts')
            .where('companyId', '==', companyId)
            .where('nameLower', '==', name.toLowerCase())
            .limit(1).get();
        if (!dup.empty) return { success: false, message: 'Account already exists' };
        // Next position
        const all = await db.collection('accounts').where('companyId', '==', companyId).get();
        const accId = buildPrefixedId('acc_');
        await db.collection('accounts').doc(accId).set({
            accountId: accId,
            name,
            nameLower: name.toLowerCase(),
            companyId,
            position: all.size,
            mobile: mobile || '',
            gstin: gstin || '',
            rechargeDate: rechargeDate || '',
            money: 0, expense: 0,
            addedDate: firebase.firestore.FieldValue.serverTimestamp()
        });
        return { success: true, message: 'Account added successfully', id: accId };
    }

    async function editAccount(accountId, newName, companyId, mobile, gstin, rechargeDate) {
        init();
        newName = (newName || '').trim();
        if (!newName) return { success: false, message: 'Account name cannot be empty' };
        
        // 1. Check for duplicate names (excluding current account)
        const dup = await db.collection('accounts')
            .where('companyId', '==', companyId)
            .where('nameLower', '==', newName.toLowerCase())
            .get();
        
        if (!dup.empty && dup.docs.some(d => d.id !== accountId)) {
            return { success: false, message: 'Account name already exists' };
        }

        // 2. Update account doc
        await db.collection('accounts').doc(accountId).update({ 
            name: newName, 
            nameLower: newName.toLowerCase(),
            mobile: mobile || '',
            gstin: gstin || '',
            rechargeDate: rechargeDate || ''
        });

        // Update local maps immediately if they exist
        if (accountIdNameMap[accountId]) {
            delete accountNameIdMap[accountIdNameMap[accountId]];
            accountIdNameMap[accountId] = newName;
            accountNameIdMap[newName] = accountId;
        }

        return { success: true, message: 'Account updated successfully' };
    }

    async function deleteAccount(id, companyId) {
        init();
        if (id) {
            // Prioritize ID-based deletion
            await db.collection('accounts').doc(id).delete();
            return { success: true, message: 'Account deleted successfully' };
        }
        // Fallback for name-based delete if no ID provided (legacy)
        // Note: The original function accepted 'name' as the first argument.
        // If 'id' is actually a name, this fallback will handle it.
        const snap = await db.collection('accounts')
            .where('companyId', '==', companyId)
            .where('name', '==', String(id || '').trim()) // Assuming 'id' might be a name in legacy calls
            .get();
        if (!snap.empty) {
            // If multiple accounts with the same name exist (shouldn't happen with current addAccount logic),
            // this will only delete the first one found. The original code deleted all.
            // For refactoring, we'll delete all found by name for consistency with original behavior.
            const ops = [];
            snap.forEach(doc => ops.push(b => b.delete(doc.ref)));
            await commitInChunks(ops);
            return { success: true, message: 'Account deleted successfully' };
        }
        return { success: false, message: 'Account not found' };
    }

    async function updateAccountOrder(orderedAccounts, companyId) {
        init();
        if (!orderedAccounts?.length) return { success: false, message: 'No accounts provided' };
        const snap = await db.collection('accounts').where('companyId', '==', companyId).get();
        const ops = [];
        snap.forEach(doc => {
            const idx = orderedAccounts.indexOf(doc.data().name);
            if (idx !== -1) ops.push(b => b.update(doc.ref, { position: idx }));
        });
        await commitInChunks(ops);
        return { success: true, message: 'Account order updated' };
    }

    async function updateMoney(accountId, companyId, money, expense, date) {
        init();
        const accountDoc = await db.collection('accounts').doc(accountId).get();
        if (!accountDoc.exists) return { success: false, message: 'Account not found' };
        
        const ops = [];
        ops.push(b => b.update(accountDoc.ref, { 
            money: money || 0, 
            expense: expense || 0, 
            moneyDate: date || '' 
        }));

        // Save historic date wise record
        if (date) {
            const docId = buildMoneyRecordDocId(date, companyId, accountId);
            const accountName = accountDoc.data().name;
            ops.push(b => b.set(db.collection('money_records').doc(docId), {
                date, 
                accountId, 
                accountName, // Still keep name for easier reading but link by ID
                companyId, 
                money: money || 0, 
                expense: expense || 0, 
                updatedAt: firebase.firestore.FieldValue.serverTimestamp()
            }, { merge: true }));
        }
        await commitInChunks(ops);
        return { success: true, message: 'Money updated successfully' };
    }

    async function resetAllMoney(date) {
        init();
        const ops = [];
        const snap1 = await db.collection('accounts').where('companyId', '==', 'company1').get();
        snap1.forEach(doc => {
            ops.push(b => b.update(doc.ref, { money: 0, expense: 0, moneyDate: date || '' }));
            if (date) {
                const accId = doc.data().accountId || doc.id;
                const docId = buildMoneyRecordDocId(date, 'company1', accId);
                ops.push(b => b.set(db.collection('money_records').doc(docId), { date, accountId: accId, accountName: doc.data().name, companyId: 'company1', money: 0, expense: 0, updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true }));
            }
        });
        const snap2 = await db.collection('accounts').where('companyId', '==', 'company2').get();
        snap2.forEach(doc => {
            ops.push(b => b.update(doc.ref, { money: 0, expense: 0, moneyDate: date || '' }));
            if (date) {
                const accId = doc.data().accountId || doc.id;
                const docId = buildMoneyRecordDocId(date, 'company2', accId);
                ops.push(b => b.set(db.collection('money_records').doc(docId), { date, accountId: accId, accountName: doc.data().name, companyId: 'company2', money: 0, expense: 0, updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true }));
            }
        });
        
        await commitInChunks(ops);
        return { success: true, message: 'All money reset successfully' };
    }

    async function createMoneyBackup(backupDate, rows, reason) {
        init();
        const safeRows = Array.isArray(rows) ? rows : [];
        if (safeRows.length === 0) {
            return { success: false, message: 'No rows provided for backup' };
        }

        let totalMoney = 0;
        let totalExpense = 0;
        safeRows.forEach(r => {
            totalMoney += parseInt(r.money) || 0;
            totalExpense += parseInt(r.expense) || 0;
        });

        const docRef = await db.collection('money_backups').add({
            backupDate: backupDate || '',
            reason: reason || 'manual',
            rows: safeRows.map(r => ({
                companyId: r.companyId || '',
                companyName: r.companyName || '',
                accountName: r.accountName || '',
                money: parseInt(r.money) || 0,
                expense: parseInt(r.expense) || 0,
                balance: (parseInt(r.money) || 0) - (parseInt(r.expense) || 0)
            })),
            totals: {
                money: totalMoney,
                expense: totalExpense,
                balance: totalMoney - totalExpense
            },
            createdAt: firebase.firestore.FieldValue.serverTimestamp()
        });

        return { success: true, message: 'Money backup created', id: docRef.id };
    }

    async function getMoneyBackups() {
        init();
        let snap;
        try {
            snap = await db.collection('money_backups').orderBy('createdAt', 'desc').get();
        } catch (e) {
            // Fallback for environments where createdAt ordering may fail.
            snap = await db.collection('money_backups').get();
        }

        const data = [];
        snap.forEach(doc => {
            const d = doc.data() || {};
            data.push({
                id: doc.id,
                backupDate: d.backupDate || '',
                reason: d.reason || '',
                rows: Array.isArray(d.rows) ? d.rows : [],
                totals: d.totals || { money: 0, expense: 0, balance: 0 },
                createdAt: d.createdAt || null
            });
        });

        data.sort((a, b) => {
            const ad = a.backupDate || '';
            const bd = b.backupDate || '';
            if (ad === bd) return (String(b.id)).localeCompare(String(a.id));
            return bd.localeCompare(ad);
        });

        return { success: true, data };
    }

    async function deleteMoneyBackup(backupId) {
        init();
        if (!backupId) return { success: false, message: 'Backup ID is required' };
        try {
            await db.collection('money_backups').doc(backupId).delete();
            return { success: true, message: 'Money backup deleted successfully' };
        } catch (e) {
            console.error('Error deleting money backup:', e);
            return { success: false, message: 'Failed to delete money backup: ' + e.message };
        }
    }

    // ───── ORDERS ─────
    async function getOrders(companyId, monthStr) {
        init();
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
        const thirtyDaysAgoStr = thirtyDaysAgo.toISOString().split('T')[0];

        // 1. Fetch valid Account IDs for this company
        const accSnap = await db.collection('accounts').where('companyId', '==', companyId).get();
        const validAccIds = new Set();
        accSnap.forEach(doc => {
             validAccIds.add(doc.data().accountId || doc.id);
        });

        // 2. Fetch from fast Daily Summary layer!
        let query = db.collection('daily_summary').where('date', '>=', monthStr ? `${monthStr}-01` : thirtyDaysAgoStr);
        if (monthStr) query = query.where('date', '<=', `${monthStr}-31`);

        try {
            const snap = await query.get();
            const records = [];
            snap.forEach(doc => {
                const d = doc.data();
                for (const [key, val] of Object.entries(d)) {
                    if (key !== 'date' && key !== 'masterCompany' && validAccIds.has(key)) {
                        const resolvedName = accountIdNameMap[key] || key;
                        records.push({
                            date: d.date,
                            accountId: key,
                            accountName: resolvedName,
                            meesho: val,
                            total: val,
                            companyId
                        });
                    }
                }
            });
            return { success: true, data: records };
        } catch (e) {
            console.error("Firebase fetch error: ", e);
            throw e;
        }
    }

    async function submitOrders(date, orders, companyId) {
        init();
        if (!orders?.length) return { success: false, message: 'No orders provided' };
        
        const dStr = String(date).split('T')[0];
        const year = dStr.substring(0, 4);
        const month = dStr.substring(5, 7);
        const partition = `orders_${year}_${month}`;

        const ops = [];
        const summaryUpdates = {};
        
        orders.forEach(o => {
            const accId = o.accountId || accountNameIdMap[o.accountName.trim()] || o.accountName;
            const quantity = parseInt(o.meesho) || 0;
            
            const docId = encodeURIComponent(`${date}_${accId}`);
            const orderRef = db.collection(partition).doc(docId);
            
            ops.push(b => b.set(orderRef, {
                orderId: docId,
                accountId: accId,
                masterCompany: companyId,
                quantity,
                date: dStr,
                createdAt: firebase.firestore.FieldValue.serverTimestamp()
            }, { merge: true }));
            
            summaryUpdates[accId] = quantity;
        });
        
        const summaryRef = db.collection('daily_summary').doc(dStr);
        ops.push(b => b.set(summaryRef, {
            date: dStr,
            ...summaryUpdates
        }, { merge: true }));
        
        await commitInChunks(ops);
        return { success: true, message: 'Orders submitted' };
    }

    async function updateOrder(date, accountId, field, value, companyId) {
        init();
        const dStr = String(date).split('T')[0];
        const year = dStr.substring(0, 4);
        const month = dStr.substring(5, 7);
        const partition = `orders_${year}_${month}`;
        const numVal = parseInt(value) || 0;
        
        const accId = accountId;
        const docId = encodeURIComponent(`${date}_${accId}`);
        const orderRef = db.collection(partition).doc(docId);
        
        const ops = [];
        ops.push(b => b.set(orderRef, {
            orderId: docId,
            accountId: accId,
            masterCompany: companyId,
            quantity: numVal,
            date: dStr,
            updatedAt: firebase.firestore.FieldValue.serverTimestamp()
        }, { merge: true }));
        
        const summaryRef = db.collection('daily_summary').doc(dStr);
        ops.push(b => b.set(summaryRef, {
            date: dStr,
            [accId]: numVal
        }, { merge: true }));
        
        await commitInChunks(ops);
        return { success: true, message: 'Order updated' };
    }

    // ───── REMARKS ─────
    async function saveRemark(date, remark) {
        init();
        await db.collection('remarks').doc(date).set(
            { remark: remark || '', updatedAt: firebase.firestore.FieldValue.serverTimestamp() },
            { merge: true }
        );
        return { success: true, message: 'Remark saved' };
    }

    async function getRemarks() {
        init();
        const snap = await db.collection('remarks').get();
        const map = {};
        snap.forEach(doc => { map[doc.id] = doc.data().remark || ''; });
        return { success: true, data: map };
    }

    // ───── BACKUP / ARCHIVE ─────
    async function getAllDataForBackup() {
        init();
        const result = { 
            company1: { accounts: [], orders: [] }, 
            company2: { accounts: [], orders: [] }, 
            remarks: {},
            karigars: [],
            karigarTransactions: [],
            designPrices: {},
            moneyBackups: []
        };
        
        const [c1a, c2a, rem, kars, txs, dps, mbks] = await Promise.all([
            db.collection('accounts').where('companyId', '==', 'company1').get(),
            db.collection('accounts').where('companyId', '==', 'company2').get(),
            db.collection('remarks').get(),
            db.collection('karigars').get(),
            db.collection('karigar_transactions').orderBy('date', 'desc').get(),
            db.collection('design_prices').get(),
            db.collection('money_backups').get()
        ]);

        const serializeTs = (ts) => {
            if (!ts) return '';
            if (ts.toDate) return ts.toDate().toISOString();
            if (ts.seconds) return new Date(ts.seconds * 1000).toISOString();
            return String(ts);
        };
        c1a.forEach(d => {
             const raw = d.data();
             result.company1.accounts.push({
                 ...raw,
                 accountId: raw.accountId || d.id,
                 name: accountIdNameMap[d.id] || raw.name,
                 addedDate: serializeTs(raw.addedDate)
             });
        });
        c2a.forEach(d => {
             const raw = d.data();
             result.company2.accounts.push({
                 ...raw,
                 accountId: raw.accountId || d.id,
                 name: accountIdNameMap[d.id] || raw.name,
                 addedDate: serializeTs(raw.addedDate)
             });
        });
        result.company1.accounts.sort((a, b) => (a.position || 0) - (b.position || 0));
        result.company2.accounts.sort((a, b) => (a.position || 0) - (b.position || 0));
        
        rem.forEach(d => { result.remarks[d.id] = d.data().remark; });
        kars.forEach(d => result.karigars.push(d.data()));
        txs.forEach(d => result.karigarTransactions.push(d.data()));
        dps.forEach(d => result.designPrices[d.id] = d.data().price);
        mbks.forEach(d => result.moneyBackups.push(d.data()));
        
        return result;
    }

    async function backupAndArchiveMonthlyData(year, monthStr) {
        init();
        const partitionName = `orders_${year}_${monthStr}`;
        const sheetTabName = `Archive_${year}_${monthStr}`;

        console.log(`[VERIFY] Checking Firestore partition ${partitionName} for backup...`);
        const oldOrdersSnap = await db.collection(partitionName).get();
        
        if (oldOrdersSnap.empty) {
            console.log(`[VERIFY] No orders found in ${partitionName}. Nothing to archive.`);
            return { success: true, count: 0, message: "No data to archive" };
        }

        const rowsToExport = [];
        oldOrdersSnap.forEach(doc => {
            const o = doc.data();
            rowsToExport.push([
                o.orderId,
                o.masterCompany || 'company1',
                accountIdNameMap[o.accountId] || o.accountId,
                o.quantity || 0,
                o.date
            ]);
        });

        console.log(`[NETWORK] Sending ${rowsToExport.length} rows to Google Sheets for archiving...`);
        // We will call the global sheetsApiRequest dynamically because this is the frontend
        const response = await window.sheetsApiRequest({
            action: 'archiveMonthlyData',
            sheetName: sheetTabName,
            data: rowsToExport
        });

        if (response && response.success && response.rowsWritten === rowsToExport.length) {
            console.log(`[VERIFY] Success! Sheets confirmed write of ${response.rowsWritten} rows. Commencing Firestore purge for partition ${partitionName}...`);
            const ops = [];
            oldOrdersSnap.forEach(doc => {
                 ops.push(b => b.delete(doc.ref));
            });
            await commitInChunks(ops);
            console.log(`[CLEANUP] Successfully backed up and purged ${rowsToExport.length} old orders!`);
            return { success: true, count: rowsToExport.length, message: "Archive succeeded" };
        } else {
            console.error("[CRITICAL] Archival verification failed. No Firestore data was deleted to prevent data loss.", response);
            return { success: false, message: response?.message || "Verification failed" };
        }
    }

    async function getBackupMeta() {
        init();
        const doc = await db.collection('system').doc('backupMeta').get();
        return doc.exists ? doc.data() : {};
    }

    async function setBackupMeta(data) {
        init();
        await db.collection('system').doc('backupMeta').set(data, { merge: true });
    }

    // ───── SEED from Sheets (one-time migration) ─────
    async function seedFromSheets(companyId, accounts, orders) {
        init();
        const ops = [];
        (accounts || []).forEach((name, idx) => {
            ops.push(b => b.set(db.collection('accounts').doc(), {
                name, nameLower: name.toLowerCase(), companyId, position: idx,
                addedDate: firebase.firestore.FieldValue.serverTimestamp()
            }));
        });
        
        const dailyMap = {};
        (orders || []).forEach(o => {
            const d = o.date;
            if (!dailyMap[d]) dailyMap[d] = { accounts: [], orders: [] };
            dailyMap[d].accounts.push(o.accountName);
            dailyMap[d].orders.push(parseInt(o.meesho) || 0);
        });
        
        for (const [date, data] of Object.entries(dailyMap)) {
            ops.push(b => b.set(db.collection('daily_orders').doc(`${companyId}_${date}`), {
                date, companyId,
                accounts: data.accounts,
                orders: data.orders,
                totals: data.orders,
                createdAt: firebase.firestore.FieldValue.serverTimestamp()
            }));
        }
        await commitInChunks(ops);
    }

    async function isEmpty(companyId) {
        init();
        const snap = await db.collection('accounts')
            .where('companyId', '==', companyId).limit(1).get();
        return snap.empty;
    }

    function buildActorMeta(actor, phase) {
        const safe = actor || {};
        const role = String(safe.role || '').trim() || 'unknown';
        const username = String(safe.username || '').trim() || 'unknown';
        const displayName = String(safe.displayName || '').trim() || username;
        const dashboard = String(safe.dashboard || safe.source || 'web_app').trim();
        const source = String(safe.source || dashboard || 'web_app').trim();
        const dashboardId = String(safe.dashboardId || '').trim();
        const base = {
            source,
            dashboard,
            dashboardId,
            [`${phase}ByRole`]: role,
            [`${phase}ByUser`]: username,
            [`${phase}ByName`]: displayName
        };
        return base;
    }

    function isAdminActor(actor) {
        const role = String(actor?.role || '').trim().toLowerCase();
        return role === 'admin';
    }

    // ───── KARIGAR ─────
    async function getKarigars(companyId = 'company1') {
        init();
        const safeCompanyId = String(companyId || 'company1').trim();
        const snap = await db.collection('karigars')
            .where('companyId', '==', safeCompanyId)
            .get();
        const docs = snap.docs || [];
        const canonicalByName = {};

        docs.forEach(doc => {
            const data = doc.data() || {};
            const nameKey = normalizeNameKey(data.name);
            const dataId = String(data.id || '').trim();
            const docId = String(doc.id || '').trim();
            const existingCanonical = isPrefixedId(dataId, 'kar_')
                ? dataId
                : (isPrefixedId(docId, 'kar_') ? docId : '');
            if (nameKey && existingCanonical && !canonicalByName[nameKey]) {
                canonicalByName[nameKey] = existingCanonical;
            }
        });

        const resultMap = new Map();
        _allKarigarsMap = {};

        for (const doc of docs) {
            const data = doc.data() || {};
            const name = String(data.name || '').trim();
            const nameKey = normalizeNameKey(name);
            const dataId = String(data.id || '').trim();
            const docId = String(doc.id || '').trim();

            let canonicalId = isPrefixedId(dataId, 'kar_') ? dataId : '';
            if (!canonicalId && isPrefixedId(docId, 'kar_')) canonicalId = docId;
            if (!canonicalId && nameKey && canonicalByName[nameKey]) canonicalId = canonicalByName[nameKey];
            if (!canonicalId) canonicalId = buildPrefixedId('kar_');

            if (nameKey && !canonicalByName[nameKey]) canonicalByName[nameKey] = canonicalId;

            if (doc.id !== canonicalId) {
                await db.collection('karigars').doc(canonicalId).set({ ...data, id: canonicalId, name, companyId: safeCompanyId }, { merge: true });
                await doc.ref.delete();
            } else if (dataId !== canonicalId || data.companyId !== safeCompanyId) {
                await doc.ref.update({ id: canonicalId, companyId: safeCompanyId });
            }

            const normalized = { ...data, id: canonicalId, name, companyId: safeCompanyId };
            _allKarigarsMap[canonicalId] = normalized;
            resultMap.set(canonicalId, normalized);
        }

        const results = Array.from(resultMap.values()).sort((a, b) =>
            String(a.name || '').localeCompare(String(b.name || ''))
        );
        return { success: true, data: results };
    }

    async function addKarigar(name, companyId = 'company1', actor = null) {
        init();
        name = name.trim();
        const safeCompanyId = String(companyId || 'company1').trim();
        // Check if exists
        const snap = await db.collection('karigars')
            .where('name', '==', name).where('companyId', '==', safeCompanyId)
            .get();
        if (!snap.empty) {
            const data = snap.docs[0].data();
            let id = String(data.id || snap.docs[0].id || '').trim();
            if (!isPrefixedId(id, 'kar_')) {
                id = buildPrefixedId('kar_');
                await db.collection('karigars').doc(id).set({ ...data, id, name, companyId: safeCompanyId }, { merge: true });
                await snap.docs[0].ref.delete();
            } else if (data.id !== id || data.companyId !== safeCompanyId) {
                await snap.docs[0].ref.update({ id, companyId: safeCompanyId });
            }
            return { success: true, id };
        }
        
        const karigarId = buildPrefixedId('kar_');
        await db.collection('karigars').doc(karigarId).set({
            name: name,
            id: karigarId,
            companyId: safeCompanyId,
            ...buildActorMeta(actor, 'created'),
            createdAt: firebase.firestore.FieldValue.serverTimestamp()
        });
        return { success: true, id: karigarId };
    }

    async function editKarigar(id, newName, companyId = 'company1', actor = null) {
        init();
        if (!isAdminActor(actor)) return { success: false, message: 'Only admin can edit karigar' };
        const safeId = String(id || '').trim();
        const safeName = String(newName || '').trim();
        const safeCompanyId = String(companyId || 'company1').trim();
        if (!safeId || !safeName) return { success: false, message: 'Karigar id and name required' };

        const dup = await db.collection('karigars')
            .where('companyId', '==', safeCompanyId)
            .where('name', '==', safeName)
            .limit(1).get();
        if (!dup.empty && dup.docs[0].id !== safeId) {
            return { success: false, message: 'Karigar name already exists in this company' };
        }

        const docRef = db.collection('karigars').doc(safeId);
        const doc = await docRef.get();
        if (!doc.exists) return { success: false, message: 'Karigar not found' };

        await docRef.update({
            name: safeName,
            companyId: safeCompanyId,
            ...buildActorMeta(actor, 'updated'),
            updatedAt: firebase.firestore.FieldValue.serverTimestamp()
        });

        const txSnap = await db.collection('karigar_transactions')
            .where('karigarId', '==', safeId)
            .where('companyId', '==', safeCompanyId)
            .get();
        if (!txSnap.empty) {
            const ops = [];
            txSnap.forEach(t => ops.push(b => b.update(t.ref, {
                karigarName: safeName,
                ...buildActorMeta(actor, 'updated'),
                updatedAt: firebase.firestore.FieldValue.serverTimestamp()
            })));
            await commitInChunks(ops);
        }
        return { success: true };
    }

    async function deleteKarigar(id, companyId = 'company1', actor = null) {
        init();
        if (!isAdminActor(actor)) return { success: false, message: 'Only admin can delete karigar' };
        const safeId = String(id || '').trim();
        const safeCompanyId = String(companyId || 'company1').trim();
        if (!safeId) return { success: false, message: 'Karigar id required' };

        const karDoc = await db.collection('karigars').doc(safeId).get();
        if (karDoc.exists) {
            const data = karDoc.data() || {};
            if (!data.companyId || data.companyId === safeCompanyId) {
                await karDoc.ref.delete();
            }
        }

        // Also remove linked transactions for this company.
        const txSnap = await db.collection('karigar_transactions')
            .where('karigarId', '==', safeId)
            .where('companyId', '==', safeCompanyId)
            .get();
        if (!txSnap.empty) {
            const ops = [];
            txSnap.forEach(doc => ops.push(b => b.delete(doc.ref)));
            await commitInChunks(ops);
        }
        return { success: true };
    }

    async function getKarigarTransactions(companyId = 'company1') {
        init();
        const safeCompanyId = String(companyId || 'company1').trim();
        if (Object.keys(_allKarigarsMap).length === 0) {
            try {
                await getKarigars(safeCompanyId);
            } catch (e) {
                console.warn('Karigar map bootstrap failed:', e);
            }
        }

        const karigarNameToId = {};
        const karigarIdToName = {};
        Object.values(_allKarigarsMap).forEach(k => {
            if (!k || typeof k !== 'object') return;
            const id = String(k.id || '').trim();
            const name = String(k.name || '').trim();
            if (!id) return;
            if (name) karigarNameToId[normalizeNameKey(name)] = id;
            if (name) karigarIdToName[id] = name;
        });

        const snap = await db.collection('karigar_transactions')
            .where('companyId', '==', safeCompanyId)
            .get();
        const results = [];
        for (const doc of snap.docs) {
            const data = doc.data() || {};
            const rawId = String(data.karigarId || '').trim();
            const rawName = String(data.karigarName || '').trim();
            const nameKey = normalizeNameKey(rawName);

            let resolvedId = rawId;
            if (!isPrefixedId(resolvedId, 'kar_') && nameKey && karigarNameToId[nameKey]) {
                resolvedId = karigarNameToId[nameKey];
            }

            let resolvedName = rawName;
            if (!resolvedName && resolvedId && karigarIdToName[resolvedId]) {
                resolvedName = karigarIdToName[resolvedId];
            }

            const patch = {};
            if (resolvedId && resolvedId !== rawId) patch.karigarId = resolvedId;
            if (resolvedName && resolvedName !== rawName) patch.karigarName = resolvedName;
            if (Object.keys(patch).length > 0) {
                await doc.ref.update(patch);
            }

            results.push({
                id: doc.id,
                ...data,
                companyId: safeCompanyId,
                karigarId: resolvedId || rawId,
                karigarName: resolvedName || rawName
            });
        }
        // Sorting will be done locally for better performance
        return { success: true, data: results };
    }

    async function resolveKarigarIdForWrite(karigarId, karigarName, companyId = 'company1', actor = null) {
        const rawId = String(karigarId || '').trim();
        if (isPrefixedId(rawId, 'kar_')) return rawId;

        const name = String(karigarName || '').trim();
        const safeCompanyId = String(companyId || 'company1').trim();
        if (!name) return buildPrefixedId('kar_');

        const snap = await db.collection('karigars')
            .where('name', '==', name)
            .where('companyId', '==', safeCompanyId)
            .limit(1).get();

        if (!snap.empty) {
            const doc = snap.docs[0];
            const data = doc.data() || {};
            let canonicalId = String(data.id || '').trim();
            if (!isPrefixedId(canonicalId, 'kar_')) {
                canonicalId = isPrefixedId(doc.id, 'kar_') ? doc.id : buildPrefixedId('kar_');
            }

            if (doc.id !== canonicalId) {
                await db.collection('karigars').doc(canonicalId).set({ ...data, id: canonicalId, name, companyId: safeCompanyId }, { merge: true });
                await doc.ref.delete();
            } else if (data.id !== canonicalId || data.companyId !== safeCompanyId) {
                await doc.ref.update({ id: canonicalId, companyId: safeCompanyId });
            }
            return canonicalId;
        }

        const newId = buildPrefixedId('kar_');
        await db.collection('karigars').doc(newId).set({
            name,
            id: newId,
            companyId: safeCompanyId,
            ...buildActorMeta(actor, 'created'),
            createdAt: firebase.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        return newId;
    }

    async function addKarigarJama(data) {
        init();
        const docRef = db.collection('karigar_transactions').doc();
        const price = parseFloat(data.price) || 0;
        const pic = parseInt(data.pic) || 0;
        const total = Math.round(((price * pic) + Number.EPSILON) * 100) / 100;
        const upadAmount = parseFloat(data.upadAmount) || 0;
        const safeCompanyId = String(data.companyId || 'company1').trim();
        const actor = data.actor || null;
        const resolvedKarigarId = await resolveKarigarIdForWrite(data.karigarId, data.karigarName, safeCompanyId, actor);
        const safeDate = String(data.date || '').split('T')[0];

        await docRef.set({
            type: 'jama',
            karigarId: resolvedKarigarId,
            karigarName: String(data.karigarName || '').trim(),
            date: safeDate,
            designName: data.designName,
            size: data.size || '',
            pic: pic,
            price: price,
            total: total,
            upadAmount: upadAmount,
            companyId: safeCompanyId,
            addedBy: data.addedBy || 'admin',
            ...buildActorMeta(actor, 'created'),
            createdAt: firebase.firestore.FieldValue.serverTimestamp()
        });
        
        if (data.designName && data.price) {
            await db.collection('design_prices').doc(data.designName.toString().trim().toUpperCase()).set({
                price: price,
                updatedAt: firebase.firestore.FieldValue.serverTimestamp()
            });
        }
        return { success: true };
    }

    async function addKarigarUpad(data) {
        init();
        if (!isAdminActor(data.actor)) throw new Error('Only admin can add Borrow (Upad)');
        const docRef = db.collection('karigar_transactions').doc();
        const safeCompanyId = String(data.companyId || 'company1').trim();
        const actor = data.actor || null;
        const resolvedKarigarId = await resolveKarigarIdForWrite(data.karigarId, data.karigarName, safeCompanyId, actor);
        const safeDate = String(data.date || '').split('T')[0];
        await docRef.set({
            type: 'upad',
            karigarId: resolvedKarigarId,
            karigarName: String(data.karigarName || '').trim(),
            date: safeDate,
            amount: parseFloat(data.amount),
            companyId: safeCompanyId,
            addedBy: data.addedBy || 'admin',
            ...buildActorMeta(actor, 'created'),
            createdAt: firebase.firestore.FieldValue.serverTimestamp()
        });
        return { success: true };
    }

    async function updateKarigarTransaction(id, updates = {}, actor = null, companyId = 'company1') {
        init();
        if (!isAdminActor(actor)) return { success: false, message: 'Only admin can edit transaction' };
        const safeId = String(id || '').trim();
        const safeCompanyId = String(companyId || 'company1').trim();
        if (!safeId) return { success: false, message: 'Transaction id required' };

        const txRef = db.collection('karigar_transactions').doc(safeId);
        const txDoc = await txRef.get();
        if (!txDoc.exists) return { success: false, message: 'Transaction not found' };

        const current = txDoc.data() || {};
        if (current.companyId && current.companyId !== safeCompanyId) {
            return { success: false, message: 'Company mismatch for transaction update' };
        }

        const patch = {
            ...buildActorMeta(actor, 'updated'),
            updatedAt: firebase.firestore.FieldValue.serverTimestamp()
        };

        if (typeof updates.date !== 'undefined') patch.date = String(updates.date || '').split('T')[0];
        if (typeof updates.designName !== 'undefined') patch.designName = String(updates.designName || '').trim();
        if (typeof updates.size !== 'undefined') patch.size = String(updates.size || '').trim();
        if (typeof updates.pic !== 'undefined') patch.pic = parseInt(updates.pic, 10) || 0;
        if (typeof updates.price !== 'undefined') patch.price = parseFloat(updates.price) || 0;
        if (typeof updates.upadAmount !== 'undefined') patch.upadAmount = parseFloat(updates.upadAmount) || 0;
        if (typeof updates.amount !== 'undefined') patch.amount = parseFloat(updates.amount) || 0;

        const finalType = String(updates.type || current.type || '').trim().toLowerCase();
        if (finalType) patch.type = finalType;
        if (typeof updates.karigarName !== 'undefined') patch.karigarName = String(updates.karigarName || '').trim();
        if (typeof updates.karigarId !== 'undefined') {
            const resolvedKId = await resolveKarigarIdForWrite(updates.karigarId, patch.karigarName || current.karigarName, safeCompanyId, actor);
            patch.karigarId = resolvedKId;
        }

        const shouldRecalcTotal = patch.type === 'jama' || current.type === 'jama';
        if (shouldRecalcTotal) {
            const pic = typeof patch.pic !== 'undefined' ? patch.pic : (parseInt(current.pic, 10) || 0);
            const price = typeof patch.price !== 'undefined' ? patch.price : (parseFloat(current.price) || 0);
            patch.total = price * pic;
        }

        await txRef.update(patch);
        return { success: true };
    }

    async function deleteKarigarTransaction(id, companyId = 'company1', actor = null) {
        init();
        if (!isAdminActor(actor)) return { success: false, message: 'Only admin can delete transaction' };
        const safeId = String(id || '').trim();
        const safeCompanyId = String(companyId || 'company1').trim();
        const txRef = db.collection('karigar_transactions').doc(safeId);
        const txDoc = await txRef.get();
        if (!txDoc.exists) return { success: true };
        const tx = txDoc.data() || {};
        if (!tx.companyId || tx.companyId === safeCompanyId) {
            await txRef.delete();
        }
        return { success: true };
    }

    async function clearKarigarMonthlyData(companyId = 'company1', actor = null) {
        init();
        if (!isAdminActor(actor)) throw new Error('Only admin can run karigar monthly reset');
        const safeCompanyId = String(companyId || 'company1').trim();
        const snap = await db.collection('karigar_transactions')
            .where('companyId', '==', safeCompanyId)
            .get();
        if (snap.empty) return 0;
        let deletedCount = 0;
        for (const doc of snap.docs) {
            await doc.ref.delete();
            deletedCount++;
        }
        return deletedCount;
    }

    async function getDesignPrices() {
        init();
        const snap = await db.collection('design_prices').get();
        const map = {};
        snap.forEach(doc => { map[doc.id] = doc.data().price; });
        return { success: true, data: map };
    }

    // ───── MIGRATE LEGACY ORDERS ─────
    async function migrateLegacyOrdersToDailyOrders() {
        init();
        const legacySnap = await db.collection('orders').limit(500).get();
        if (legacySnap.empty) return { success: true, message: 'No legacy orders found.' };
        
        const dailyMap = {};
        const ops = [];
        
        legacySnap.forEach(doc => {
            const data = doc.data();
            const date = data.date;
            const companyId = data.companyId || 'company1';
            const key = `${companyId}_${date}`;
            
            if (!dailyMap[key]) {
                dailyMap[key] = { date, companyId, accounts: [], orders: [], refsToDelete: [] };
            }
            
            dailyMap[key].accounts.push(data.accountName || '');
            dailyMap[key].orders.push(parseInt(data.meesho) || parseInt(data.total) || 0);
            dailyMap[key].refsToDelete.push(doc.ref);
        });

        // First, apply all merges to daily_orders safely
        for (const [key, d] of Object.entries(dailyMap)) {
            const docRef = db.collection('daily_orders').doc(key);
            const docSnap = await docRef.get();
            
            if (docSnap.exists) {
                // Merge into existing daily document
                const existData = docSnap.data();
                const existAccounts = existData.accounts || [];
                const existOrders = existData.orders || [];
                
                d.accounts.forEach((acc, i) => {
                    const existIdx = existAccounts.indexOf(acc);
                    if (existIdx === -1) {
                        existAccounts.push(acc);
                        existOrders.push(d.orders[i]);
                    } else {
                         // Only update if current is 0 to be safe
                         if (existOrders[existIdx] === 0) existOrders[existIdx] = d.orders[i];
                    }
                });
                
                ops.push(b => b.update(docRef, {
                    accounts: existAccounts,
                    orders: existOrders,
                    totals: existOrders, // Assuming equals
                    updatedAt: firebase.firestore.FieldValue.serverTimestamp()
                }));
            } else {
                // Create brand new daily_order
                ops.push(b => b.set(docRef, {
                    date: d.date,
                    companyId: d.companyId,
                    accounts: d.accounts,
                    orders: d.orders,
                    totals: d.orders,
                    createdAt: firebase.firestore.FieldValue.serverTimestamp()
                }));
            }
        }
        
        // After successfully staging writes, stage deletes of old records
        for (const [key, d] of Object.entries(dailyMap)) {
            d.refsToDelete.forEach(ref => {
                ops.push(b => b.delete(ref));
            });
        }
        
        await commitInChunks(ops);
        return { success: true, count: legacySnap.size, message: `Migrated ${legacySnap.size} legacy orders to daily array format.` };
    }

    // ───── WRITE BUFFER (for Data Sheet edits) ─────
    function bufferWrite(key, writeFn) {
        _pendingWrites.set(key, writeFn);
        setSyncStatus('pending');
        if (_flushTimer) clearTimeout(_flushTimer);
        _flushTimer = setTimeout(() => flushWrites(), APP_CONFIG.writeBufferMs);
    }

    async function flushWrites() {
        if (_pendingWrites.size === 0) { setSyncStatus('saved'); return; }
        setSyncStatus('syncing');
        const entries = [..._pendingWrites.values()];
        _pendingWrites.clear();
        try {
            await Promise.all(entries.map(fn => fn()));
            setSyncStatus('saved');
        } catch (e) {
            console.error('Flush error:', e);
            setSyncStatus('error');
        }
    }

    // ───── MIGRATION: NAMES TO UNIQUE IDS ─────
    async function migrateDatabaseToIds() {
        if (localStorage.getItem('migrated_orders_to_ids_v1')) return;
        init();
        console.log("Migrating all historical orders to use unique IDs...");
        const ops = [];
        
        // 1. Ensure all accounts have an ID
        const accSnap = await db.collection('accounts').get();
        const mapNameId = {};
        accSnap.forEach(doc => {
            let accId = doc.data().accountId;
            if (!accId) { 
                accId = doc.id; 
                ops.push(b => b.update(doc.ref, { accountId: accId })); 
            }
            mapNameId[doc.data().name.trim()] = accId;
        });
        
        // 2. Scan legacy daily_orders and convert them into Monthly Partitions + Daily Summary
        const ordSnap = await db.collection('daily_orders').get();
        ordSnap.forEach(doc => {
            const data = doc.data();
            if (data.accounts) {
                const date = data.date;
                const dStr = String(date).split('T')[0];
                const year = dStr.substring(0, 4);
                const month = dStr.substring(5, 7);
                const partition = `orders_${year}_${month}`;
                
                const summaryUpdates = {};
                for (let i = 0; i < data.accounts.length; i++) {
                    const originalStr = data.accounts[i].trim();
                    const accId = mapNameId[originalStr] || originalStr;
                    const quantity = data.orders ? (data.orders[i] || 0) : 0;
                    
                    const docId = encodeURIComponent(`${date}_${accId}`);
                    const orderRef = db.collection(partition).doc(docId);
                    ops.push(b => b.set(orderRef, {
                        orderId: docId,
                        accountId: accId,
                        masterCompany: data.companyId,
                        quantity,
                        date: dStr,
                        createdAt: data.createdAt || firebase.firestore.FieldValue.serverTimestamp()
                    }, { merge: true }));
                    
                    summaryUpdates[accId] = quantity;
                }
                
                ops.push(b => b.set(db.collection('daily_summary').doc(dStr), {
                    date: dStr,
                    ...summaryUpdates
                }, { merge: true }));
                
                ops.push(b => b.delete(doc.ref)); // Delete old massive document
            }
        });
        
        await commitInChunks(ops);
        localStorage.setItem('migrated_orders_to_ids_v1', 'true');
        console.log("Migration finished.");
    }

    async function fixHistoricalDataIntegrity() {
        init();
        const stats = { accounts: 0, karigars: 0, orders: 0, karigarTxs: 0, summaries: 0 };
        const nameToAccId = {};
        const nameToKarigarId = {};
        const karigarIdToName = {};

        function isRealId(val) {
            if (!val) return false;
            const s = String(val).trim();
            return s.startsWith('acc_') || s.startsWith('kar_');
        }

        // 1. ACCOUNTS: Ensure all use acc_ IDs as Document Keys
        const accSnap = await db.collection('accounts').get();
        for (const doc of accSnap.docs) {
            const data = doc.data();
            let aid = data.accountId || (doc.id.startsWith('acc_') ? doc.id : null);
            const name = (data.name || '').trim();
            const kCompanyId = String(data.companyId || 'company1').trim();
            
            if (!isRealId(aid)) {
                aid = buildPrefixedId('acc_');
            }
            
            // If the Doc ID itself is not the Real ID, re-key it
            if (doc.id !== aid) {
                await db.collection('accounts').doc(aid).set({ ...data, accountId: aid });
                await doc.ref.delete();
                stats.accounts++;
            } else if (!data.accountId) {
                await doc.ref.update({ accountId: aid });
                stats.accounts++;
            }
            if (name) nameToAccId[name.toLowerCase()] = aid;
            nameToAccId[doc.id] = aid; // store mapping for old doc ids
        }

        // 2. KARIGARS: Ensure all use kar_ IDs as Document Keys
        const karSnap = await db.collection('karigars').get();
        for (const doc of karSnap.docs) {
            const data = doc.data();
            let kid = data.id || (doc.id.startsWith('kar_') ? doc.id : null);
            const name = (data.name || '').trim();
            const kCompanyId = String(data.companyId || 'company1').trim();
            
            if (!isRealId(kid)) {
                kid = buildPrefixedId('kar_');
            }

            if (doc.id !== kid) {
                await db.collection('karigars').doc(kid).set({ ...data, id: kid, companyId: kCompanyId });
                await doc.ref.delete();
                stats.karigars++;
            } else if (!data.id) {
                await doc.ref.update({ id: kid, companyId: kCompanyId });
                stats.karigars++;
            }
            if (name) nameToKarigarId[`${kCompanyId}|${name.toLowerCase()}`] = kid;
            nameToKarigarId[doc.id] = kid;
            if (name) karigarIdToName[kid] = name;
        }

        // 3. Karigar Transactions
        const ktxSnap = await db.collection('karigar_transactions').get();
        for (const doc of ktxSnap.docs) {
            const data = doc.data();
            const currentKId = (data.karigarId || '').trim();
            const kName = (data.karigarName || '').trim();
            const txCompanyId = String(data.companyId || 'company1').trim();
            
            let resolvedId = currentKId;
            if (!isRealId(resolvedId) && kName) resolvedId = nameToKarigarId[`${txCompanyId}|${kName.toLowerCase()}`] || resolvedId;
            if (!isRealId(resolvedId) && currentKId) resolvedId = nameToKarigarId[currentKId] || nameToKarigarId[currentKId.toLowerCase()] || resolvedId;

            const resolvedName = kName || (resolvedId && karigarIdToName[resolvedId] ? karigarIdToName[resolvedId] : '');
            const patch = {};
            if (resolvedId && currentKId !== resolvedId) patch.karigarId = resolvedId;
            if (resolvedName && kName !== resolvedName) patch.karigarName = resolvedName;

            if (Object.keys(patch).length > 0) {
                await doc.ref.update(patch);
                stats.karigarTxs++;
            }
        }

        // 4. Daily Summary & Order Partitions
        const summarySnap = await db.collection('daily_summary').get();
        const partitionsProcessed = new Set();
        
        for (const doc of summarySnap.docs) {
            const data = doc.data();
            const date = doc.id;
            let changedSummary = false;
            const newSummary = { ...data };

            for (const [key, val] of Object.entries(data)) {
                if (key === 'date' || key === 'masterCompany') continue;
                let aid = nameToAccId[key.toLowerCase().trim()];
                if (!aid) aid = nameToAccId[key]; 
                
                if (aid && aid !== key) {
                    newSummary[aid] = (newSummary[aid] || 0) + val;
                    delete newSummary[key];
                    changedSummary = true;
                }
            }
            if (changedSummary) {
                await doc.ref.set(newSummary);
                stats.summaries++;
            }

            // Fix order partitions
            const dStr = String(date);
            if (dStr.length >= 7) {
                const partition = `orders_${dStr.substring(0, 4)}_${dStr.substring(5, 7)}`;
                if (!partitionsProcessed.has(partition)) {
                    const ordSnap = await db.collection(partition).get();
                    for (const oDoc of ordSnap.docs) {
                        const oData = oDoc.data();
                        const currentAccId = (oData.accountId || '').trim();
                        const oName = (oData.accountName || '').trim();
                        
                        let resolvedId = oName ? nameToAccId[oName.toLowerCase()] : null;
                        if (!resolvedId) resolvedId = nameToAccId[currentAccId]; 
                        
                        if (resolvedId && resolvedId !== currentAccId) {
                            await oDoc.ref.update({ accountId: resolvedId });
                            stats.orders++;
                        }
                    }
                    partitionsProcessed.add(partition);
                }
            }
        }

        return { success: true, stats };
    }

    async function clearCollectionDocs(collectionName) {
        init();
        let totalDeleted = 0;
        const PAGE_SIZE = 400;

        while (true) {
            const snap = await db.collection(collectionName).limit(PAGE_SIZE).get();
            if (snap.empty) break;

            const ops = [];
            snap.forEach(doc => ops.push(b => b.delete(doc.ref)));
            await commitInChunks(ops);
            totalDeleted += snap.size;

            if (snap.size < PAGE_SIZE) break;
        }
        return totalDeleted;
    }

    async function replaceFromSheets(sheetData) {
        init();

        const orderPartitions = new Set();

        // Derive order partitions from existing Firebase summaries.
        const summarySnap = await db.collection('daily_summary').get();
        summarySnap.forEach(doc => {
            const d = String(doc.id || '').trim();
            if (/^\d{4}-\d{2}-\d{2}$/.test(d)) {
                orderPartitions.add(`orders_${d.substring(0, 4)}_${d.substring(5, 7)}`);
            }
        });

        // Derive order partitions from incoming sheet data too.
        for (const cid of ['company1', 'company2']) {
            const orders = (sheetData?.[cid]?.orders) || [];
            for (const o of orders) {
                const d = String(o?.date || '').trim();
                if (/^\d{4}-\d{2}-\d{2}$/.test(d)) {
                    orderPartitions.add(`orders_${d.substring(0, 4)}_${d.substring(5, 7)}`);
                }
            }
        }

        // Safety sweep for unknown historical monthly partitions.
        const currentYear = new Date().getFullYear();
        for (let y = 2018; y <= currentYear + 3; y++) {
            for (let m = 1; m <= 12; m++) {
                orderPartitions.add(`orders_${y}_${String(m).padStart(2, '0')}`);
            }
        }

        const baseCollections = [
            'accounts',
            'remarks',
            'karigars',
            'karigar_transactions',
            'design_prices',
            'money_backups',
            'money_records',
            'daily_summary',
            'daily_orders',
            'orders'
        ];

        let deleted = 0;
        for (const collectionName of baseCollections) {
            deleted += await clearCollectionDocs(collectionName);
        }
        for (const partitionName of orderPartitions) {
            deleted += await clearCollectionDocs(partitionName);
        }

        // Reset in-memory maps after purge.
        accountNameIdMap = {};
        accountIdNameMap = {};
        _allAccountsMap = {};
        _allKarigarsMap = {};

        const syncResult = await syncFromSheets(sheetData || {});
        return { ...syncResult, deleted };
    }

    /**
     * Sync FROM Google Sheets TO Firebase. Sheets = source of truth.
     * Upserts all data: existing Firebase records are updated, new ones are created.
     * Does NOT delete Firebase records that aren't in Sheets.
     */
    async function syncFromSheets(sheetData) {
        init();
        const stats = { accounts: 0, orders: 0, karigars: 0, karigarTxs: 0, designPrices: 0 };
        
        // 1. Sync Accounts (per company)
        for (const cid of ['company1', 'company2']) {
            const accounts = (sheetData[cid] && sheetData[cid].accounts) || [];
            for (const acc of accounts) {
                if (!acc.name) continue;
                
                const sheetAccId = acc.accountId && acc.accountId.startsWith('acc_') ? acc.accountId : null;

                // Find existing doc by name + companyId
                const snap = await db.collection('accounts')
                    .where('name', '==', acc.name)
                    .where('companyId', '==', cid)
                    .limit(1).get();
                
                const accId = sheetAccId || (snap.empty ? buildPrefixedId('acc_') : (snap.docs[0].data().accountId || snap.docs[0].id));

                if (!snap.empty) {
                    const existingDoc = snap.docs[0];
                    const existingData = existingDoc.data();
                    
                    // IF we are re-keying (doc id matches sheet but doc itself is random id)
                    if (existingDoc.id !== accId) {
                        await db.collection('accounts').doc(accId).set({ ...existingData, accountId: accId, position: acc.position || 0 });
                        await existingDoc.ref.delete();
                    } else {
                        await existingDoc.ref.update({ position: acc.position || 0, accountId: accId });
                    }
                } else {
                    await db.collection('accounts').doc(accId).set({
                        name: acc.name,
                        nameLower: acc.name.toLowerCase(),
                        accountId: accId,
                        companyId: cid,
                        position: acc.position || 0,
                        addedDate: firebase.firestore.FieldValue.serverTimestamp(),
                        money: 0, expense: 0
                    });
                }
                stats.accounts++;
            }
        }
        
        // 2. Sync Orders (per company)
        for (const cid of ['company1', 'company2']) {
            const orders = (sheetData[cid] && sheetData[cid].orders) || [];
            // Group orders by month partition
            const byPartition = {};
            for (const o of orders) {
                if (!o.date || !o.accountId) continue;
                const parts = o.date.split('-');
                const partition = `orders_${parts[0]}_${parts[1]}`;
                if (!byPartition[partition]) byPartition[partition] = [];
                byPartition[partition].push(o);
            }
            
            for (const [partition, partOrders] of Object.entries(byPartition)) {
                // Get existing docs for dedup
                const existSnap = await db.collection(partition).get();
                const existMap = {};
                existSnap.forEach(doc => {
                    const d = doc.data();
                    existMap[`${d.date}_${d.accountId}`] = doc;
                });
                
                for (const o of partOrders) {
                    const key = `${o.date}_${o.accountId}`;
                    const quantity = parseInt(o.meesho) || 0;
                    
                    if (existMap[key]) {
                        // Update if different
                        const existing = existMap[key].data();
                        if ((parseInt(existing.quantity) || 0) !== quantity) {
                            await existMap[key].ref.update({ quantity, meesho: quantity });
                            stats.orders++;
                        }
                    } else {
                        // Create new
                        await db.collection(partition).add({
                            date: o.date,
                            accountId: o.accountId,
                            accountName: o.accountName || '',
                            quantity,
                            meesho: quantity,
                            masterCompany: cid,
                            synced: true
                        });
                        stats.orders++;
                    }
                }
                
                // Also update daily_summary
                const dateMap = {};
                for (const o of partOrders) {
                    if (!dateMap[o.date]) dateMap[o.date] = {};
                    dateMap[o.date][o.accountId] = parseInt(o.meesho) || 0;
                }
                for (const [date, accOrders] of Object.entries(dateMap)) {
                    await db.collection('daily_summary').doc(date).set(
                        { ...accOrders, date, masterCompany: 'all' },
                        { merge: true }
                    );
                }
            }
        }
        
        // 3. Sync Karigars
        const karigars = sheetData.karigars || [];
        const karigarNameToId = {};
        for (const k of karigars) {
            if (!k.name) continue;
            const kCompanyId = String(k.companyId || 'company1').trim();
            const karigarAudit = {
                createdByName: String(k.createdByName || '').trim(),
                createdByUser: String(k.createdByUser || '').trim(),
                createdByRole: String(k.createdByRole || '').trim(),
                source: String(k.source || '').trim(),
                dashboard: String(k.dashboard || '').trim()
            };
            const snap = await db.collection('karigars')
                .where('name', '==', k.name)
                .where('companyId', '==', kCompanyId)
                .limit(1).get();
            
            const sheetKarId = k.id && k.id.startsWith('kar_') ? k.id : null;
            const karId = sheetKarId || (!snap.empty ? (snap.docs[0].data().id || snap.docs[0].id) : buildPrefixedId('kar_'));

            if (!snap.empty) {
                const existingDoc = snap.docs[0];
                const existingData = existingDoc.data();
                if (existingDoc.id !== karId) {
                    await db.collection('karigars').doc(karId).set({ ...existingData, ...karigarAudit, id: karId, companyId: kCompanyId });
                    await existingDoc.ref.delete();
                } else {
                    await existingDoc.ref.update({ ...karigarAudit, id: karId, companyId: kCompanyId });
                }
            } else {
                await db.collection('karigars').doc(karId).set({
                    name: k.name,
                    id: karId,
                    companyId: kCompanyId,
                    ...karigarAudit,
                    addedDate: parseFlexibleDateValue(k.addedAt || k.addedDate) || firebase.firestore.FieldValue.serverTimestamp()
                });
            }
            karigarNameToId[`${kCompanyId}|${normalizeNameKey(k.name)}`] = karId;
            stats.karigars++;
        }
        
        // 4. Sync Karigar Transactions
        const txs = sheetData.karigarTransactions || [];
        for (const tx of txs) {
            if (!tx.date) continue;
            const txName = String(tx.karigarName || '').trim();
            const txNameKey = normalizeNameKey(txName);
            const txCompanyId = String(tx.companyId || 'company1').trim();
            const rawTxId = String(tx.karigarId || '').trim();
            let txKarigarId = rawTxId;
            if (!isPrefixedId(txKarigarId, 'kar_') && txNameKey && karigarNameToId[`${txCompanyId}|${txNameKey}`]) {
                txKarigarId = karigarNameToId[`${txCompanyId}|${txNameKey}`];
            }
            if (!isPrefixedId(txKarigarId, 'kar_')) continue;

            const safeDate = String(tx.date || '').split('T')[0];

            // Dedup safely with broad query, then strict local compare.
            const snap = await db.collection('karigar_transactions')
                .where('date', '==', safeDate)
                .where('karigarId', '==', txKarigarId)
                .where('companyId', '==', txCompanyId)
                .where('type', '==', tx.type || 'jama')
                .get();

            const txDesign = String(tx.designName || '').trim();
            const txSize = String(tx.size || '').trim();
            const txPic = parseInt(tx.pic, 10) || 0;
            const txPrice = parseFloat(tx.price) || 0;
            const txTotal = parseFloat(tx.totalJama) || 0;
            const txUpad = parseFloat(tx.upadAmount) || 0;
            const isDuplicate = snap.docs.some(doc => {
                const d = doc.data() || {};
                return String(d.designName || '').trim() === txDesign &&
                    String(d.size || '').trim() === txSize &&
                    (parseInt(d.pic, 10) || 0) === txPic &&
                    (parseFloat(d.price) || 0) === txPrice &&
                    (parseFloat(d.total) || 0) === txTotal &&
                    (parseFloat(d.upadAmount) || 0) === txUpad;
            });
            
            if (!isDuplicate) {
                const createdAtDate = parseFlexibleDateValue(tx.createdAt);
                await db.collection('karigar_transactions').add({
                    date: safeDate,
                    karigarId: txKarigarId,
                    karigarName: txName || '',
                    companyId: txCompanyId,
                    type: tx.type || 'jama',
                    designName: txDesign,
                    size: txSize,
                    pic: txPic,
                    price: txPrice,
                    total: txTotal,
                    upadAmount: txUpad,
                    createdByName: String(tx.createdByName || '').trim(),
                    createdByUser: String(tx.createdByUser || '').trim(),
                    createdByRole: String(tx.createdByRole || '').trim(),
                    updatedByName: String(tx.updatedByName || '').trim(),
                    updatedByUser: String(tx.updatedByUser || '').trim(),
                    updatedByRole: String(tx.updatedByRole || '').trim(),
                    source: String(tx.source || '').trim(),
                    dashboard: String(tx.dashboard || '').trim(),
                    createdAt: createdAtDate || firebase.firestore.FieldValue.serverTimestamp()
                });
                stats.karigarTxs++;
            }
        }
        
        // 5. Sync Design Prices
        const prices = sheetData.designPrices || {};
        for (const [name, price] of Object.entries(prices)) {
            await db.collection('design_prices').doc(name).set({
                price: parseInt(price) || 0,
                updatedAt: firebase.firestore.FieldValue.serverTimestamp()
            }, { merge: true });
            stats.designPrices++;
        }
        
        return { success: true, stats };
    }

    function getPendingCount() { return _pendingWrites.size; }

    // ───── PUBLIC API ─────
    return {
        init, getDb,
        // Accounts
        getAccounts, addAccount, editAccount, deleteAccount, updateAccountOrder, updateMoney, resetAllMoney, createMoneyBackup, getMoneyBackups, deleteMoneyBackup,
        // Orders
        getOrders, submitOrders, updateOrder,
        // Remarks
        saveRemark, getRemarks,
        // Backup
        getAllDataForBackup, backupAndArchiveMonthlyData, getBackupMeta, setBackupMeta,
        // Migration
        seedFromSheets, isEmpty, migrateLegacyOrdersToDailyOrders, migrateDatabaseToIds,
        // Karigar
        getKarigars, addKarigar, editKarigar, deleteKarigar, getKarigarTransactions,
        addKarigarJama,
        addKarigarUpad,
        updateKarigarTransaction,
        deleteKarigarTransaction,
        clearKarigarMonthlyData,
        getDesignPrices,
        // Write buffer
        bufferWrite, flushWrites, getPendingCount,
        // Integrity Fix
        fixHistoricalDataIntegrity,
        // Sync from Sheets
        syncFromSheets, replaceFromSheets,
        // Sync status
        onSyncStatusChange, getSyncStatus, setSyncStatus
    };
})();
