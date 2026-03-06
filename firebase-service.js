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

    function buildMoneyRecordDocId(date, companyId, accountName) {
        return [date, companyId, accountName]
            .map(v => encodeURIComponent(String(v || '').trim()))
            .join('_');
    }

    let accountNameIdMap = {};
    let accountIdNameMap = {};

    function buildMoneyRecordDocId(date, companyId, accountName) {
        return [date, companyId, accountName]
            .map(v => encodeURIComponent(String(v || '').trim()))
            .join('_');
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
        snap.forEach(doc => {
            const data = doc.data();
            const id = data.accountId || doc.id;
            accountNameIdMap[data.name.trim()] = id;
            accountIdNameMap[id] = data.name.trim();
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
        const docRef = db.collection('accounts').doc();
        await docRef.set({
            accountId: docRef.id,
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
        // Thanks to our ID-based system, we NO LONGER need to manually update thousands
        // of orders exactly right here! It will resolve globally and automatically.
        
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
            const docId = buildMoneyRecordDocId(date, companyId, accountName);
            ops.push(b => b.set(db.collection('money_records').doc(docId), {
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
            if (date) {
                const docId = buildMoneyRecordDocId(date, 'company1', doc.data().name);
                ops.push(b => b.set(db.collection('money_records').doc(docId), { date, accountName: doc.data().name, companyId: 'company1', money: 0, expense: 0, updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true }));
            }
        });
        const snap2 = await db.collection('accounts').where('companyId', '==', 'company2').get();
        snap2.forEach(doc => {
            ops.push(b => b.update(doc.ref, { money: 0, expense: 0, moneyDate: date || '' }));
            if (date) {
                const docId = buildMoneyRecordDocId(date, 'company2', doc.data().name);
                ops.push(b => b.set(db.collection('money_records').doc(docId), { date, accountName: doc.data().name, companyId: 'company2', money: 0, expense: 0, updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true }));
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
            const accId = accountNameIdMap[o.accountName.trim()] || o.accountName;
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

    async function updateOrder(date, accountName, field, value, companyId) {
        init();
        const dStr = String(date).split('T')[0];
        const year = dStr.substring(0, 4);
        const month = dStr.substring(5, 7);
        const partition = `orders_${year}_${month}`;
        const numVal = parseInt(value) || 0;
        
        const accId = accountNameIdMap[(accountName || '').trim()] || accountName;
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
            db.collection('money_backups').orderBy('timestamp', 'desc').get()
        ]);

        c1a.forEach(d => result.company1.accounts.push({
             ...d.data(), 
             name: accountIdNameMap[d.id] || d.data().name 
        }));
        c2a.forEach(d => result.company2.accounts.push({
             ...d.data(),
             name: accountIdNameMap[d.id] || d.data().name 
        }));
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

    // ───── KARIGAR ─────
    async function getKarigars() {
        init();
        const snap = await db.collection('karigars').get();
        const results = [];
        snap.forEach(doc => results.push({ id: doc.id, ...doc.data() }));
        return { success: true, data: results };
    }

    async function addKarigar(name) {
        init();
        // Check if exists
        const snap = await db.collection('karigars').where('name', '==', name.trim()).get();
        if (!snap.empty) return { success: true, id: snap.docs[0].id };
        const docRef = db.collection('karigars').doc();
        await docRef.set({
            name: name.trim(),
            createdAt: firebase.firestore.FieldValue.serverTimestamp()
        });
        return { success: true, id: docRef.id };
    }

    async function deleteKarigar(name) {
        init();
        const snap = await db.collection('karigars').where('name', '==', name.trim()).get();
        if (!snap.empty) {
            await snap.docs[0].ref.delete();
        }
        return { success: true };
    }

    async function getKarigarTransactions() {
        init();
        const snap = await db.collection('karigar_transactions').get();
        const results = [];
        snap.forEach(doc => results.push({ id: doc.id, ...doc.data() }));
        // Sorting will be done locally for better performance
        return { success: true, data: results };
    }

    async function addKarigarJama(data) {
        init();
        const docRef = db.collection('karigar_transactions').doc();
        const price = parseFloat(data.price) || 0;
        const pic = parseInt(data.pic) || 0;
        const total = price * pic;
        const upadAmount = parseFloat(data.upadAmount) || 0;

        await docRef.set({
            type: 'jama',
            karigarName: data.karigarName,
            date: data.date,
            designName: data.designName,
            size: data.size || '',
            pic: pic,
            price: price,
            total: total,
            upadAmount: upadAmount,
            companyId: data.companyId || 'company1',
            addedBy: data.addedBy || 'admin',
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
        const docRef = db.collection('karigar_transactions').doc();
        await docRef.set({
            type: 'upad',
            karigarName: data.karigarName,
            date: data.date,
            amount: parseFloat(data.amount),
            companyId: data.companyId || 'company1',
            addedBy: data.addedBy || 'admin',
            createdAt: firebase.firestore.FieldValue.serverTimestamp()
        });
        return { success: true };
    }

    async function deleteKarigarTransaction(id) {
        init();
        await db.collection('karigar_transactions').doc(id).delete();
        return { success: true };
    }

    async function clearKarigarMonthlyData() {
        init();
        const snap = await db.collection('karigar_transactions').get();
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
        getKarigars, addKarigar, deleteKarigar, getKarigarTransactions,
        addKarigarJama,
        addKarigarUpad,
        deleteKarigarTransaction,
        clearKarigarMonthlyData,
        getDesignPrices,
        // Write buffer
        bufferWrite, flushWrites, getPendingCount,
        // Sync status
        onSyncStatusChange, getSyncStatus, setSyncStatus
    };
})();
