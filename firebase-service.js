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

    // ───── ACCOUNTS ─────
    async function getAccounts(companyId) {
        init();
        let snap;
        try {
            // Requires composite index (companyId ASC, position ASC)
            snap = await db.collection('accounts')
                .where('companyId', '==', companyId)
                .orderBy('position')
                .get();
        } catch (e) {
            // Fallback if index doesn't exist yet — sort client-side
            console.warn('Index missing for accounts, using fallback sort:', e.message);
            snap = await db.collection('accounts')
                .where('companyId', '==', companyId)
                .get();
        }
        const docs = [];
        snap.forEach(doc => docs.push(doc.data()));
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
        await db.collection('accounts').add({
            name,
            nameLower: name.toLowerCase(),
            companyId,
            position: all.size,
            mobile: mobile || '',
            gstin: gstin || '',
            rechargeDate: rechargeDate || '',
            addedDate: firebase.firestore.FieldValue.serverTimestamp()
        });
        return { success: true, message: 'Account added successfully' };
    }

    async function editAccount(oldName, newName, companyId, mobile, gstin, rechargeDate) {
        init();
        newName = (newName || '').trim();
        if (!newName) return { success: false, message: 'Account name cannot be empty' };
        if (oldName.toLowerCase() !== newName.toLowerCase()) {
            const dup = await db.collection('accounts')
                .where('companyId', '==', companyId)
                .where('nameLower', '==', newName.toLowerCase())
                .limit(1).get();
            if (!dup.empty) return { success: false, message: 'Account name already exists' };
        }
        // Update account doc
        const snap = await db.collection('accounts')
            .where('companyId', '==', companyId)
            .where('name', '==', oldName.trim())
            .get();
        if (snap.empty) return { success: false, message: 'Account not found' };
        const ops = [];
        snap.forEach(doc => {
            ops.push(b => b.update(doc.ref, { 
                name: newName, 
                nameLower: newName.toLowerCase(),
                mobile: mobile || '',
                gstin: gstin || '',
                rechargeDate: rechargeDate || ''
            }));
        });
        // Also rename in orders
        const ordSnap = await db.collection('orders')
            .where('companyId', '==', companyId)
            .where('accountName', '==', oldName.trim())
            .get();
        ordSnap.forEach(doc => {
            ops.push(b => b.update(doc.ref, { accountName: newName }));
        });
        await commitInChunks(ops);
        return { success: true, message: 'Account updated successfully' };
    }

    async function deleteAccount(name, companyId) {
        init();
        const snap = await db.collection('accounts')
            .where('companyId', '==', companyId)
            .where('name', '==', (name || '').trim())
            .get();
        if (snap.empty) return { success: false, message: 'Account not found' };
        const ops = [];
        snap.forEach(doc => ops.push(b => b.delete(doc.ref)));
        await commitInChunks(ops);
        return { success: true, message: 'Account deleted successfully' };
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

    async function updateMoney(accountName, companyId, money, expense, date) {
        init();
        const snap = await db.collection('accounts')
            .where('companyId', '==', companyId)
            .where('name', '==', (accountName || '').trim())
            .get();
        if (snap.empty) return { success: false, message: 'Account not found' };
        const ops = [];
        snap.forEach(doc => {
            ops.push(b => b.update(doc.ref, { money: money || 0, expense: expense || 0, moneyDate: date || '' }));
        });
        // Save historic date wise record
        if (date) {
            ops.push(b => b.set(db.collection('money_records').doc(`${date}_${companyId}_${accountName}`), {
                date, accountName, companyId, money: money || 0, expense: expense || 0, updatedAt: firebase.firestore.FieldValue.serverTimestamp()
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
            if (date) ops.push(b => b.set(db.collection('money_records').doc(`${date}_company1_${doc.data().name}`), { date, accountName: doc.data().name, companyId: 'company1', money: 0, expense: 0, updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true }));
        });
        const snap2 = await db.collection('accounts').where('companyId', '==', 'company2').get();
        snap2.forEach(doc => {
            ops.push(b => b.update(doc.ref, { money: 0, expense: 0, moneyDate: date || '' }));
            if (date) ops.push(b => b.set(db.collection('money_records').doc(`${date}_company2_${doc.data().name}`), { date, accountName: doc.data().name, companyId: 'company2', money: 0, expense: 0, updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true }));
        });
        
        await commitInChunks(ops);
        return { success: true, message: 'All money reset successfully' };
    }

    // ───── ORDERS ─────
    async function getOrders(companyId, monthStr) {
        init();
        let query = db.collection('orders').where('companyId', '==', companyId);
        if (monthStr) {
            // Filter to a specific month e.g. "2026-03"
            query = query.where('date', '>=', monthStr + '-01')
                         .where('date', '<=', monthStr + '-31');
        }
        const snap = await query.get();
        const records = [];
        snap.forEach(doc => {
            const d = doc.data();
            records.push({
                date: d.date,
                accountName: d.accountName,
                meesho: d.meesho || 0,
                flipkart: d.flipkart || 0,
                total: d.total || 0
            });
        });
        return { success: true, data: records };
    }

    async function submitOrders(date, orders, companyId) {
        init();
        if (!orders?.length) return { success: false, message: 'No orders provided' };
        const ops = orders.map(o => batch => {
            batch.set(db.collection('orders').doc(), {
                date,
                accountName: o.accountName,
                meesho: o.meesho || 0,
                flipkart: o.flipkart || 0,
                total: o.total || 0,
                companyId,
                createdAt: firebase.firestore.FieldValue.serverTimestamp()
            });
        });
        await commitInChunks(ops);
        return { success: true, message: 'Orders submitted' };
    }

    async function updateOrder(date, accountName, field, value, companyId) {
        init();
        const snap = await db.collection('orders')
            .where('companyId', '==', companyId)
            .where('date', '==', date)
            .where('accountName', '==', (accountName || '').trim())
            .get();
        if (snap.empty) return { success: false, message: 'Order not found' };
        const numVal = parseInt(value) || 0;
        const ops = [];
        snap.forEach(doc => {
            const d = doc.data();
            const updates = { [field]: numVal };
            updates.total = (field === 'meesho' ? numVal : (parseInt(d.meesho) || 0))
                          + (field === 'flipkart' ? numVal : (parseInt(d.flipkart) || 0));
            ops.push(b => b.update(doc.ref, updates));
        });
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
        const result = { company1: { accounts: [], orders: [] }, company2: { accounts: [], orders: [] }, remarks: {} };
        const [c1a, c2a, c1o, c2o, rem] = await Promise.all([
            db.collection('accounts').where('companyId', '==', 'company1').get(),
            db.collection('accounts').where('companyId', '==', 'company2').get(),
            db.collection('orders').where('companyId', '==', 'company1').get(),
            db.collection('orders').where('companyId', '==', 'company2').get(),
            db.collection('remarks').get()
        ]);
        c1a.forEach(d => result.company1.accounts.push(d.data()));
        c2a.forEach(d => result.company2.accounts.push(d.data()));
        result.company1.accounts.sort((a, b) => (a.position || 0) - (b.position || 0));
        result.company2.accounts.sort((a, b) => (a.position || 0) - (b.position || 0));
        c1o.forEach(d => result.company1.orders.push(d.data()));
        c2o.forEach(d => result.company2.orders.push(d.data()));
        rem.forEach(d => { result.remarks[d.id] = d.data().remark; });
        return result;
    }

    async function clearOldOrders(keepCurrentMonth) {
        init();
        const currentMonth = new Date().toISOString().substring(0, 7);
        const snap = await db.collection('orders').get();
        const ops = [];
        snap.forEach(doc => {
            if (keepCurrentMonth && doc.data().date?.startsWith(currentMonth)) return;
            ops.push(b => b.delete(doc.ref));
        });
        await commitInChunks(ops);
        return ops.length;
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
        (orders || []).forEach(o => {
            ops.push(b => b.set(db.collection('orders').doc(), {
                date: o.date, accountName: o.accountName,
                meesho: parseInt(o.meesho) || 0, flipkart: parseInt(o.flipkart) || 0,
                total: parseInt(o.total) || 0, companyId,
                createdAt: firebase.firestore.FieldValue.serverTimestamp()
            }));
        });
        await commitInChunks(ops);
    }

    async function isEmpty(companyId) {
        init();
        const snap = await db.collection('accounts')
            .where('companyId', '==', companyId).limit(1).get();
        return snap.empty;
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

    function getPendingCount() { return _pendingWrites.size; }

    // ───── PUBLIC API ─────
    return {
        init, getDb,
        // Accounts
        getAccounts, addAccount, editAccount, deleteAccount, updateAccountOrder, updateMoney, resetAllMoney,
        // Orders
        getOrders, submitOrders, updateOrder,
        // Remarks
        saveRemark, getRemarks,
        // Backup
        getAllDataForBackup, clearOldOrders, getBackupMeta, setBackupMeta,
        // Migration
        seedFromSheets, isEmpty,
        // Write buffer
        bufferWrite, flushWrites, getPendingCount,
        // Sync status
        onSyncStatusChange, getSyncStatus, setSyncStatus
    };
})();
