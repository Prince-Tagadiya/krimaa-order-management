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
        
        // Keep default online mode to avoid deprecated persistence API warnings.
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

    function buildDesignPriceDocId(companyId, designKey) {
        const safeCompanyId = String(companyId || 'global').trim() || 'global';
        const safeKey = String(designKey || '').trim().toUpperCase();
        return `${safeCompanyId}__${safeKey}`;
    }

    function buildDesignPriceHistoryDocId(companyId, designKey, effectiveFrom) {
        const safeCompanyId = String(companyId || 'global').trim() || 'global';
        const safeKey = String(designKey || '').trim().toUpperCase();
        const safeDateTime = normalizeISODateTime(effectiveFrom) || normalizeISODateTime(new Date());
        return `${safeCompanyId}__${safeKey}__${safeDateTime.replace(/[:]/g, '-')}`;
    }

    async function saveDesignPricePoint(companyId, designKey, price, effectiveFrom, actor = null) {
        const safeCompanyId = String(companyId || 'global').trim() || 'global';
        const safeKey = String(designKey || '').trim().toUpperCase();
        const safePrice = Math.round(((parseFloat(price) || 0) + Number.EPSILON) * 100) / 100;
        const safeEffectiveFrom = normalizeISODateTime(effectiveFrom) || normalizeISODateTime(new Date());
        const docId = buildDesignPriceDocId(safeCompanyId, safeKey);
        await db.collection('design_prices').doc(docId).set({
            companyId: safeCompanyId,
            key: safeKey,
            price: safePrice,
            effectiveFromLatest: safeEffectiveFrom,
            ...buildActorMeta(actor, 'updated'),
            updatedAt: firebase.firestore.FieldValue.serverTimestamp()
        }, { merge: true });

        const historyDocId = buildDesignPriceHistoryDocId(safeCompanyId, safeKey, safeEffectiveFrom);
        await db.collection('design_price_history').doc(historyDocId).set({
            companyId: safeCompanyId,
            key: safeKey,
            price: safePrice,
            effectiveFrom: safeEffectiveFrom,
            ...buildActorMeta(actor, 'updated'),
            updatedAt: firebase.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        return { safeCompanyId, safeKey, safePrice, safeEffectiveFrom };
    }

    async function deleteDesignPricePoint(companyId, designKey, effectiveFrom) {
        const safeCompanyId = String(companyId || 'global').trim() || 'global';
        const safeKey = String(designKey || '').trim().toUpperCase();
        const safeEffectiveFrom = normalizeISODateTime(effectiveFrom);
        if (!safeKey || !safeEffectiveFrom) return 0;

        let deleted = 0;
        const deterministicDocId = buildDesignPriceHistoryDocId(safeCompanyId, safeKey, safeEffectiveFrom);
        const directRef = db.collection('design_price_history').doc(deterministicDocId);
        const directDoc = await directRef.get();
        if (directDoc.exists) {
            await directRef.delete();
            deleted += 1;
        }

        const snap = await db.collection('design_price_history')
            .where('companyId', '==', safeCompanyId)
            .where('key', '==', safeKey)
            .where('effectiveFrom', '==', safeEffectiveFrom)
            .get();
        if (!snap.empty) {
            const ops = [];
            snap.forEach(doc => ops.push(b => b.delete(doc.ref)));
            if (ops.length > 0) {
                await commitInChunks(ops);
                deleted += ops.length;
            }
        }
        return deleted;
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

    function normalizeISODate(rawValue) {
        if (!rawValue) return '';
        const d = parseFlexibleDateValue(rawValue);
        if (!d) return '';
        const local = new Date(d.getTime() - (d.getTimezoneOffset() * 60000));
        return local.toISOString().split('T')[0];
    }

    function normalizeISODateTime(rawValue, fallbackDate = '') {
        if (!rawValue && fallbackDate) return `${normalizeISODate(fallbackDate)}T00:00:00`;
        if (!rawValue) return '';
        if (typeof rawValue === 'string') {
            const str = rawValue.trim().replace(' ', 'T');
            const match = str.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})(:\d{2})?$/);
            if (match) {
                const secs = match[3] ? match[3] : ':00';
                return `${match[1]}T${match[2]}${secs}`;
            }
            if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
                return `${str}T00:00:00`;
            }
        }
        const d = parseFlexibleDateValue(rawValue);
        if (!d) return '';
        const local = new Date(d.getTime() - (d.getTimezoneOffset() * 60000));
        return local.toISOString().slice(0, 19);
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
        const safeId = String(id || '').trim();
        if (!isPrefixedId(safeId, 'acc_')) return { success: false, message: 'Valid account ID required' };
        const doc = await db.collection('accounts').doc(safeId).get();
        if (!doc.exists) return { success: false, message: 'Account not found' };
        const data = doc.data() || {};
        if (data.companyId && data.companyId !== companyId) return { success: false, message: 'Company mismatch' };
        await doc.ref.delete();
        return { success: true, message: 'Account deleted successfully' };
    }

    async function updateAccountOrder(orderedAccounts, companyId) {
        init();
        if (!orderedAccounts?.length) return { success: false, message: 'No accounts provided' };
        const snap = await db.collection('accounts').where('companyId', '==', companyId).get();
        const ops = [];
        snap.forEach(doc => {
            const accountId = String(doc.data().accountId || doc.id || '').trim();
            const idx = orderedAccounts.indexOf(accountId);
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

    async function createKarigarResetBackup(payload = {}) {
        init();
        const safeCompanyId = String(payload.companyId || 'company1').trim();
        const actor = payload.actor || null;
        const rows = Array.isArray(payload.rows) ? payload.rows : [];
        const normalizedRows = rows.map(tx => {
            const safeType = String(tx.type || 'jama').trim().toLowerCase();
            return {
                id: String(tx.id || '').trim(),
                companyId: String(tx.companyId || safeCompanyId).trim(),
                karigarId: String(tx.karigarId || '').trim(),
                karigarName: String(tx.karigarName || '').trim(),
                type: safeType || 'jama',
                date: normalizeISODate(tx.date) || '',
                transactionDateTime: normalizeISODateTime(tx.transactionDateTime || tx.dateTime || tx.createdAt || tx.date) || '',
                designName: String(tx.designName || '').trim(),
                size: String(tx.size || '').trim(),
                pic: parseInt(tx.pic, 10) || 0,
                price: parseFloat(tx.price) || 0,
                total: parseFloat(tx.total) || 0,
                upadAmount: parseFloat(tx.upadAmount) || 0,
                amount: parseFloat(tx.amount) || 0,
                createdFrom: String(tx.createdFrom || tx.source || tx.dashboard || tx.addedBy || '').trim(),
                source: String(tx.source || tx.createdFrom || tx.dashboard || tx.addedBy || '').trim(),
                addedBy: String(tx.addedBy || '').trim()
            };
        });

        const summary = normalizedRows.reduce((acc, tx) => {
            if (tx.type === 'jama') {
                acc.totalJama += parseFloat(tx.total) || 0;
                acc.totalUpad += parseFloat(tx.upadAmount) || 0;
            } else {
                acc.totalUpad += parseFloat(tx.amount) || 0;
            }
            return acc;
        }, { totalJama: 0, totalUpad: 0, count: normalizedRows.length });
        summary.netBalance = Math.round(((summary.totalJama - summary.totalUpad) + Number.EPSILON) * 100) / 100;
        summary.totalJama = Math.round((summary.totalJama + Number.EPSILON) * 100) / 100;
        summary.totalUpad = Math.round((summary.totalUpad + Number.EPSILON) * 100) / 100;

        const snapshotAt = new Date().toISOString();
        const docRef = await db.collection('karigar_reset_backups').add({
            companyId: safeCompanyId,
            rows: normalizedRows,
            summary,
            snapshotAt,
            ...buildActorMeta(actor, 'created'),
            createdAt: firebase.firestore.FieldValue.serverTimestamp()
        });
        return { success: true, id: docRef.id, snapshotAt };
    }

    async function getKarigarResetBackups(companyId = 'company1', limitCount = 60) {
        init();
        const safeCompanyId = String(companyId || 'company1').trim();
        const safeLimit = Math.max(1, parseInt(limitCount, 10) || 60);

        let snap;
        try {
            snap = await db.collection('karigar_reset_backups')
                .where('companyId', '==', safeCompanyId)
                .orderBy('createdAt', 'desc')
                .limit(safeLimit)
                .get();
        } catch (e) {
            snap = await db.collection('karigar_reset_backups')
                .where('companyId', '==', safeCompanyId)
                .limit(safeLimit)
                .get();
        }

        const data = [];
        snap.forEach(doc => {
            const d = doc.data() || {};
            data.push({
                id: doc.id,
                companyId: String(d.companyId || safeCompanyId).trim(),
                rows: Array.isArray(d.rows) ? d.rows : [],
                summary: d.summary || { totalJama: 0, totalUpad: 0, netBalance: 0, count: 0 },
                snapshotAt: d.snapshotAt || '',
                createdAt: d.createdAt || null,
                createdFrom: String(d.createdFrom || d.source || '').trim()
            });
        });

        data.sort((a, b) => {
            const ad = String(a.snapshotAt || '').trim();
            const bd = String(b.snapshotAt || '').trim();
            return bd.localeCompare(ad);
        });
        return { success: true, data };
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
            designPriceHistory: [],
            moneyBackups: []
        };
        
        const [c1a, c2a, rem, kars, txs, dps, dph, mbks] = await Promise.all([
            db.collection('accounts').where('companyId', '==', 'company1').get(),
            db.collection('accounts').where('companyId', '==', 'company2').get(),
            db.collection('remarks').get(),
            db.collection('karigars').get(),
            db.collection('karigar_transactions').orderBy('date', 'desc').get(),
            db.collection('design_prices').get(),
            db.collection('design_price_history').get(),
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
        const historyMap = new Map();
        dph.forEach(d => {
            const raw = d.data() || {};
            const key = String(raw.key || '').trim().toUpperCase();
            if (!key) return;
            const effectiveFrom = normalizeISODateTime(raw.effectiveFrom || raw.effectiveFromLatest || raw.updatedAt || raw.createdAt || new Date());
            if (!effectiveFrom) return;
            const price = parseFloat(raw.price) || 0;
            const updatedAtIso = serializeTs(raw.updatedAt || raw.createdAt || new Date());
            const uniq = `global|${key}|${effectiveFrom}`;
            const prev = historyMap.get(uniq);
            const prevMs = prev ? (parseFlexibleDateValue(prev.updatedAt)?.getTime() || 0) : -1;
            const curMs = parseFlexibleDateValue(raw.updatedAt || raw.createdAt)?.getTime() || 0;
            if (!prev || curMs >= prevMs) {
                historyMap.set(uniq, {
                    companyId: 'global',
                    key,
                    price,
                    effectiveFrom,
                    updatedAt: updatedAtIso
                });
            }
        });
        result.designPriceHistory = Array.from(historyMap.values())
            .sort((a, b) => {
                const k = String(a.key || '').localeCompare(String(b.key || ''));
                if (k !== 0) return k;
                return String(a.effectiveFrom || '').localeCompare(String(b.effectiveFrom || ''));
            });

        const nowIso = normalizeISODateTime(new Date());
        const historyByKey = {};
        result.designPriceHistory.forEach(point => {
            const key = String(point.key || '').trim().toUpperCase();
            if (!key) return;
            if (!historyByKey[key]) historyByKey[key] = [];
            historyByKey[key].push(point);
        });
        Object.keys(historyByKey).forEach(key => {
            const points = historyByKey[key].sort((a, b) => String(a.effectiveFrom).localeCompare(String(b.effectiveFrom)));
            let selected = points[points.length - 1];
            points.forEach(p => {
                if (String(p.effectiveFrom || '') <= nowIso) selected = p;
            });
            if (selected) result.designPrices[key] = parseFloat(selected.price) || 0;
        });

        dps.forEach(d => {
            const raw = d.data() || {};
            const cid = String(raw.companyId || '').trim().toLowerCase();
            if (cid && cid !== 'global') return;
            const key = String(raw.key || '').trim().toUpperCase() || String(d.id || '').split('__').slice(1).join('__').trim().toUpperCase();
            if (!key) return;
            if (typeof result.designPrices[key] === 'undefined') {
                result.designPrices[key] = parseFloat(raw.price) || 0;
            }
        });
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

    async function clearOldOrders(keepDays = 30) {
        init();
        const safeKeepDays = Math.max(1, parseInt(keepDays, 10) || 30);
        const cutoff = new Date();
        cutoff.setDate(cutoff.getDate() - safeKeepDays);
        const cutoffISO = new Date(cutoff.getTime() - (cutoff.getTimezoneOffset() * 60000)).toISOString().split('T')[0];

        let totalDeleted = 0;
        const oldDates = [];
        const summarySnap = await db.collection('daily_summary').where('date', '<', cutoffISO).get();
        if (!summarySnap.empty) {
            const ops = [];
            summarySnap.forEach(doc => {
                oldDates.push(String(doc.id || '').trim());
                ops.push(b => b.delete(doc.ref));
            });
            await commitInChunks(ops);
            totalDeleted += summarySnap.size;
        }

        const partitions = new Set();
        oldDates.forEach(d => {
            if (/^\d{4}-\d{2}-\d{2}$/.test(d)) {
                partitions.add(`orders_${d.substring(0, 4)}_${d.substring(5, 7)}`);
            }
        });

        for (const partition of partitions) {
            const snap = await db.collection(partition).where('date', '<', cutoffISO).get();
            if (snap.empty) continue;
            const ops = [];
            snap.forEach(doc => ops.push(b => b.delete(doc.ref)));
            await commitInChunks(ops);
            totalDeleted += snap.size;
        }

        // Legacy fallback collection
        try {
            const legacySnap = await db.collection('daily_orders').where('date', '<', cutoffISO).get();
            if (!legacySnap.empty) {
                const ops = [];
                legacySnap.forEach(doc => ops.push(b => b.delete(doc.ref)));
                await commitInChunks(ops);
                totalDeleted += legacySnap.size;
            }
        } catch (e) {
            console.warn('clearOldOrders legacy cleanup skipped:', e);
        }

        return totalDeleted;
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
        const actorAccount = String(safe.actorAccount || safe.source || '').trim();
        const dashboard = String(safe.dashboard || '').trim();
        const dashboardId = String(safe.dashboardId || '').trim();
        const fromValue = actorAccount || dashboard || 'unknown';
        return {
            source: fromValue,
            dashboard: fromValue,
            dashboardId,
            [`${phase}From`]: fromValue
        };
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
                await Promise.all([getKarigars('company1'), getKarigars('company2')]);
            } catch (e) {
                console.warn('Karigar map bootstrap failed:', e);
            }
        }

        const karigarNameToIdByCompany = { company1: {}, company2: {} };
        const karigarIdToName = {};
        Object.values(_allKarigarsMap).forEach(k => {
            if (!k || typeof k !== 'object') return;
            const id = String(k.id || '').trim();
            const name = String(k.name || '').trim();
            const cid = String(k.companyId || 'company1').trim();
            if (!id) return;
            if (name) karigarNameToIdByCompany[cid][normalizeNameKey(name)] = id;
            if (name) karigarIdToName[id] = name;
        });

        const snap = await db.collection('karigar_transactions').get();
        const results = [];
        for (const doc of snap.docs) {
            const data = doc.data() || {};
            const rawId = String(data.karigarId || '').trim();
            const rawName = String(data.karigarName || '').trim();
            const rawCompanyId = String(data.companyId || '').trim();
            const nameKey = normalizeNameKey(rawName);

            let resolvedId = rawId;
            let resolvedCompanyId = rawCompanyId;

            if (!isPrefixedId(resolvedId, 'kar_')) {
                if (resolvedCompanyId && karigarNameToIdByCompany[resolvedCompanyId] && nameKey) {
                    resolvedId = karigarNameToIdByCompany[resolvedCompanyId][nameKey] || resolvedId;
                } else if (nameKey) {
                    const c1Id = karigarNameToIdByCompany.company1[nameKey] || '';
                    const c2Id = karigarNameToIdByCompany.company2[nameKey] || '';
                    if (c1Id && !c2Id) {
                        resolvedId = c1Id;
                        resolvedCompanyId = 'company1';
                    } else if (c2Id && !c1Id) {
                        resolvedId = c2Id;
                        resolvedCompanyId = 'company2';
                    }
                }
            }

            if (!resolvedCompanyId && isPrefixedId(resolvedId, 'kar_')) {
                resolvedCompanyId = String(_allKarigarsMap[resolvedId]?.companyId || '').trim();
            }
            if (!resolvedCompanyId) resolvedCompanyId = safeCompanyId;
            if (resolvedCompanyId !== safeCompanyId) continue;

            let resolvedName = rawName;
            if (!resolvedName && resolvedId && karigarIdToName[resolvedId]) {
                resolvedName = karigarIdToName[resolvedId];
            }

            const patch = {};
            if (resolvedId && resolvedId !== rawId) patch.karigarId = resolvedId;
            if (resolvedName && resolvedName !== rawName) patch.karigarName = resolvedName;
            if (resolvedCompanyId && resolvedCompanyId !== rawCompanyId) patch.companyId = resolvedCompanyId;
            if (Object.keys(patch).length > 0) {
                await doc.ref.update(patch);
            }

            results.push({
                id: doc.id,
                ...data,
                companyId: resolvedCompanyId,
                karigarId: resolvedId || rawId,
                karigarName: resolvedName || rawName
            });
        }
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
        const pic = parseInt(data.pic, 10) || 0;
        const safeCompanyId = String(data.companyId || 'company1').trim();
        const actor = data.actor || null;
        const safeDate = String(data.date || '').split('T')[0];
        const transactionDateTime = normalizeISODateTime(data.transactionDateTime || data.date, safeDate) || `${safeDate}T00:00:00`;
        const safeTxDate = normalizeISODate(transactionDateTime) || safeDate;
        const designKey = String(data.designName || '').trim().toUpperCase();
        let price = parseFloat(data.price);
        if (!Number.isFinite(price) || price < 0) price = 0;
        if (!isAdminActor(actor)) {
            const resolvedPrices = await getDesignPrices('global', transactionDateTime);
            const autoPrice = parseFloat(resolvedPrices?.data?.[designKey]);
            if (!Number.isFinite(autoPrice) || autoPrice < 0) {
                throw new Error('Saved design price is required for non-admin Jama entry');
            }
            price = autoPrice;
        }
        const total = Math.round(((price * pic) + Number.EPSILON) * 100) / 100;
        const upadAmount = isAdminActor(actor) ? (parseFloat(data.upadAmount) || 0) : 0;
        const resolvedKarigarId = await resolveKarigarIdForWrite(data.karigarId, data.karigarName, safeCompanyId, actor);

        await docRef.set({
            type: 'jama',
            karigarId: resolvedKarigarId,
            karigarName: String(data.karigarName || '').trim(),
            date: safeTxDate,
            transactionDateTime: transactionDateTime,
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
        
        if (data.designName && Number.isFinite(price) && price >= 0) {
            const key = data.designName.toString().trim().toUpperCase();
            await saveDesignPricePoint('global', key, price, transactionDateTime, actor);
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
        const transactionDateTime = normalizeISODateTime(data.transactionDateTime || data.date, safeDate) || `${safeDate}T00:00:00`;
        const safeTxDate = normalizeISODate(transactionDateTime) || safeDate;
        await docRef.set({
            type: 'upad',
            karigarId: resolvedKarigarId,
            karigarName: String(data.karigarName || '').trim(),
            date: safeTxDate,
            transactionDateTime: transactionDateTime,
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

        if (typeof updates.transactionDateTime !== 'undefined') {
            const normalizedDT = normalizeISODateTime(updates.transactionDateTime, updates.date || current.date);
            if (normalizedDT) {
                patch.transactionDateTime = normalizedDT;
                patch.date = normalizeISODate(normalizedDT);
            }
        } else if (typeof updates.date !== 'undefined') {
            patch.date = String(updates.date || '').split('T')[0];
            const currentTime = String(current.transactionDateTime || '').split('T')[1] || '00:00:00';
            patch.transactionDateTime = `${patch.date}T${currentTime}`;
        }
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

    async function getDesignPrices(companyId = 'global', asOfDate = '') {
        init();
        const safeCompanyId = String(companyId || 'global').trim() || 'global';
        const safeAsOfDate = normalizeISODateTime(asOfDate) || normalizeISODateTime(new Date());
        const map = {};
        const historyByKey = {};
        const pointMapByKey = {};
        const historySnap = safeCompanyId === 'global'
            ? await db.collection('design_price_history').get()
            : await db.collection('design_price_history').where('companyId', '==', safeCompanyId).get();
        historySnap.forEach(doc => {
            const data = doc.data() || {};
            if (safeCompanyId !== 'global' && String(data.companyId || '').trim() !== safeCompanyId) return;
            const key = String(data.key || '').trim().toUpperCase();
            const eff = normalizeISODateTime(data.effectiveFrom || data.updatedAt) || '';
            if (!key || !eff) return;
            const price = parseFloat(data.price) || 0;
            const updatedAtMs = parseFlexibleDateValue(data.updatedAt)?.getTime() || 0;
            if (!pointMapByKey[key]) pointMapByKey[key] = {};
            const prev = pointMapByKey[key][eff];
            if (!prev || updatedAtMs >= (prev.updatedAtMs || 0)) {
                pointMapByKey[key][eff] = { effectiveFrom: eff, price, updatedAtMs };
            }
        });

        Object.keys(pointMapByKey).forEach(key => {
            historyByKey[key] = Object.values(pointMapByKey[key])
                .map(p => ({ effectiveFrom: p.effectiveFrom, price: p.price }))
                .sort((a, b) => String(a.effectiveFrom).localeCompare(String(b.effectiveFrom)));
            let chosen = null;
            historyByKey[key].forEach(point => {
                if (point.effectiveFrom <= safeAsOfDate) chosen = point;
            });
            if (chosen) map[key] = chosen.price;
        });

        const scopedSnap = safeCompanyId === 'global'
            ? await db.collection('design_prices').get()
            : await db.collection('design_prices').where('companyId', '==', safeCompanyId).get();
        scopedSnap.forEach(doc => {
            const data = doc.data() || {};
            if (safeCompanyId !== 'global' && String(data.companyId || '').trim() !== safeCompanyId) return;
            const key = String(data.key || '').trim().toUpperCase() || String(doc.id || '').split('__').slice(1).join('__').trim().toUpperCase();
            if (!key) return;
            if (typeof map[key] === 'undefined') map[key] = parseFloat(data.price) || 0;
        });

        // Backward compatibility for old global records.
        if (Object.keys(map).length === 0) {
            const legacySnap = await db.collection('design_prices').get();
            legacySnap.forEach(doc => {
                const data = doc.data() || {};
                if (data.companyId) return;
                const key = String(doc.id || '').trim().toUpperCase();
                if (!key) return;
                if (typeof map[key] === 'undefined') map[key] = parseFloat(data.price) || 0;
            });
        }
        return { success: true, data: map, history: historyByKey, companyId: safeCompanyId, asOfDate: safeAsOfDate };
    }

    async function upsertDesignPrice(designKey, price, actor = null, companyId = 'global', effectiveFrom = '', options = null) {
        init();
        if (!isAdminActor(actor)) return { success: false, message: 'Only admin can update prices' };
        const safeKey = String(designKey || '').trim().toUpperCase();
        const safePrice = parseFloat(price);
        const safeCompanyId = String(companyId || 'global').trim() || 'global';
        if (!safeKey) return { success: false, message: 'Design/Size key required' };
        if (!Number.isFinite(safePrice) || safePrice < 0) return { success: false, message: 'Invalid price' };
        const safeOptions = (options && typeof options === 'object') ? options : {};
        const replaceFromEffectiveFrom = normalizeISODateTime(safeOptions.replaceFromEffectiveFrom || '');
        if (replaceFromEffectiveFrom) {
            const replaceFromKey = String(safeOptions.replaceFromKey || safeKey).trim().toUpperCase();
            await deleteDesignPricePoint(safeCompanyId, replaceFromKey, replaceFromEffectiveFrom);
        }
        const point = await saveDesignPricePoint(safeCompanyId, safeKey, safePrice, effectiveFrom, actor);
        return { success: true, companyId: safeCompanyId, key: safeKey, effectiveFrom: point.safeEffectiveFrom };
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

    async function replaceFromSheets(sheetData, opts = {}) {
        init();
        const fastMode = opts.fast !== false;

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

        // Fast mode avoids brute-force sweeping hundreds of monthly collections.
        // It clears only partitions discoverable from existing summaries + incoming sheet rows.
        if (!fastMode) {
            const currentYear = new Date().getFullYear();
            for (let y = 2018; y <= currentYear + 3; y++) {
                for (let m = 1; m <= 12; m++) {
                    orderPartitions.add(`orders_${y}_${String(m).padStart(2, '0')}`);
                }
            }
        }

        const baseCollections = [
            'accounts',
            'remarks',
            'karigars',
            'karigar_transactions',
            'design_prices',
            'design_price_history',
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
        return { ...syncResult, deleted, fastMode };
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
            const createdFrom = String(k.createdFrom || k.dashboard || k.source || k.addedBy || '').trim();
            const updatedFrom = String(k.updatedFrom || '').trim();
            const karigarAudit = {
                createdFrom: createdFrom,
                updatedFrom: updatedFrom,
                source: createdFrom || '',
                dashboard: createdFrom || ''
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
            const normalizedTxType = String(tx.type || 'jama').trim().toLowerCase() || 'jama';
            let txKarigarId = rawTxId;
            if (!isPrefixedId(txKarigarId, 'kar_') && txNameKey && karigarNameToId[`${txCompanyId}|${txNameKey}`]) {
                txKarigarId = karigarNameToId[`${txCompanyId}|${txNameKey}`];
            }
            if (!isPrefixedId(txKarigarId, 'kar_') && txName) {
                // Never drop valid sheet rows just because old IDs are malformed.
                txKarigarId = await resolveKarigarIdForWrite(rawTxId, txName, txCompanyId, null);
                if (isPrefixedId(txKarigarId, 'kar_')) {
                    karigarNameToId[`${txCompanyId}|${txNameKey}`] = txKarigarId;
                }
            }
            if (!isPrefixedId(txKarigarId, 'kar_')) continue;

            const safeDate = String(tx.date || '').split('T')[0];
            const safeDateTime = normalizeISODateTime(tx.transactionDateTime || tx.dateTime || tx.createdAt || tx.date, safeDate) || `${safeDate}T00:00:00`;
            const createdAtDate = parseFlexibleDateValue(tx.createdAt);

            // Dedup safely with broad query, then strict local compare.
            const snap = await db.collection('karigar_transactions')
                .where('date', '==', safeDate)
                .where('karigarId', '==', txKarigarId)
                .where('companyId', '==', txCompanyId)
                .where('type', '==', normalizedTxType)
                .get();

            const txDesign = String(tx.designName || '').trim();
            const txSize = String(tx.size || '').trim();
            const txPic = parseInt(tx.pic, 10) || 0;
            const txPrice = parseFloat(tx.price) || 0;
            const txTotal = parseFloat(tx.totalJama) || 0;
            const txUpad = parseFloat(tx.upadAmount) || 0;
            const isDuplicate = snap.docs.some(doc => {
                const d = doc.data() || {};
                const existingCreatedAt = parseFlexibleDateValue(d.createdAt);
                const createdAtMatches = createdAtDate && existingCreatedAt
                    ? Math.abs(existingCreatedAt.getTime() - createdAtDate.getTime()) < 1000
                    : false;
                return String(d.designName || '').trim() === txDesign &&
                    String(d.size || '').trim() === txSize &&
                    (parseInt(d.pic, 10) || 0) === txPic &&
                    (parseFloat(d.price) || 0) === txPrice &&
                    (parseFloat(d.total) || 0) === txTotal &&
                    (parseFloat(d.upadAmount) || 0) === txUpad &&
                    createdAtMatches;
            });
            
            if (!isDuplicate) {
                const txCreatedFrom = String(tx.createdFrom || tx.dashboard || tx.source || tx.addedBy || '').trim();
                await db.collection('karigar_transactions').add({
                    date: safeDate,
                    transactionDateTime: safeDateTime,
                    karigarId: txKarigarId,
                    karigarName: txName || '',
                    companyId: txCompanyId,
                    type: normalizedTxType,
                    designName: txDesign,
                    size: txSize,
                    pic: txPic,
                    price: txPrice,
                    total: txTotal,
                    upadAmount: txUpad,
                    createdFrom: txCreatedFrom,
                    updatedFrom: String(tx.updatedFrom || '').trim(),
                    source: txCreatedFrom,
                    dashboard: txCreatedFrom,
                    createdAt: createdAtDate || firebase.firestore.FieldValue.serverTimestamp()
                });
                stats.karigarTxs++;
            }
        }
        
        // 5. Sync Design Price History (date-effective, key-based)
        const priceHistory = sheetData.designPriceHistory || [];
        for (const point of priceHistory) {
            const key = String(point.key || point.designKey || '').trim().toUpperCase();
            const eff = normalizeISODateTime(point.effectiveFrom || point.updatedAt) || normalizeISODateTime(new Date());
            const price = parseFloat(point.price) || 0;
            if (!key) continue;
            await saveDesignPricePoint('global', key, price, eff, null);
            stats.designPrices++;
        }

        // 6. Sync latest Design Prices (legacy map)
        const prices = sheetData.designPrices || {};
        for (const [name, price] of Object.entries(prices)) {
            const key = String(name || '').trim().toUpperCase();
            if (!key) continue;
            await saveDesignPricePoint('global', key, parseFloat(price) || 0, normalizeISODateTime(new Date()), null);
            stats.designPrices += 1;
        }
        
        return { success: true, stats };
    }

    function getPendingCount() { return _pendingWrites.size; }

    // ───── PUBLIC API ─────
    return {
        init, getDb,
        // Accounts
        getAccounts, addAccount, editAccount, deleteAccount, updateAccountOrder, updateMoney, resetAllMoney, createMoneyBackup, getMoneyBackups, deleteMoneyBackup,
        createKarigarResetBackup, getKarigarResetBackups,
        // Orders
        getOrders, submitOrders, updateOrder,
        // Remarks
        saveRemark, getRemarks,
        // Backup
        getAllDataForBackup, backupAndArchiveMonthlyData, clearOldOrders, getBackupMeta, setBackupMeta,
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
        upsertDesignPrice,
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
