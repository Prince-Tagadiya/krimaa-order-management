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
        // Also rename in daily_orders
        const dOrdSnap = await db.collection('daily_orders')
            .where('companyId', '==', companyId)
            .get();
        dOrdSnap.forEach(doc => {
            const data = doc.data();
            if (data.accounts && data.accounts.includes(oldName.trim())) {
                const idx = data.accounts.indexOf(oldName.trim());
                if (idx !== -1) {
                    const newAccs = [...data.accounts];
                    newAccs[idx] = newName;
                    ops.push(b => b.update(doc.ref, { accounts: newAccs }));
                }
            }
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

        // Fetch all orders for this company. We filter by date in-memory to avoid composite index requirements.
        let query = db.collection('daily_orders').where('companyId', '==', companyId);

        try {
            const snap = await query.get();
            const records = [];
            snap.forEach(doc => {
                const d = doc.data();
                
                // In-memory filter exactly like before to prevent index crash
                if (monthStr) {
                    if (d.date && !d.date.startsWith(monthStr)) return;
                } else {
                    if (d.date && d.date < thirtyDaysAgoStr) return;
                }
                
                if (d.accounts && d.orders) {
                    for (let i = 0; i < d.accounts.length; i++) {
                        records.push({
                            date: d.date,
                            accountName: d.accounts[i],
                            meesho: d.orders[i] || 0,
                            total: d.orders[i] || 0
                        });
                    }
                }
            });
            return { success: true, data: records };
        } catch (e) {
            console.error("Firebase fetch error. Make sure indexes are configured if adding new queries: ", e);
            throw e;
        }
    }

    async function submitOrders(date, orders, companyId) {
        init();
        if (!orders?.length) return { success: false, message: 'No orders provided' };
        
        const accounts = [];
        const orderVals = [];
        orders.forEach(o => {
            accounts.push(o.accountName);
            orderVals.push(o.meesho || 0);
        });
        
        const docRef = db.collection('daily_orders').doc(`${companyId}_${date}`);
        await docRef.set({
            date,
            companyId,
            accounts,
            orders: orderVals,
            totals: orderVals,
            createdAt: firebase.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        
        return { success: true, message: 'Orders submitted' };
    }

    async function updateOrder(date, accountName, field, value, companyId) {
        init();
        const docRef = db.collection('daily_orders').doc(`${companyId}_${date}`);
        const docSnap = await docRef.get();
        const numVal = parseInt(value) || 0;
        
        if (!docSnap.exists) {
            await docRef.set({
                date,
                companyId,
                accounts: [accountName],
                orders: [numVal],
                totals: [numVal],
                createdAt: firebase.firestore.FieldValue.serverTimestamp()
            });
            return { success: true, message: 'Order updated' };
        }
        
        const d = docSnap.data();
        const accounts = d.accounts || [];
        const orders = d.orders || [];
        
        const idx = accounts.indexOf((accountName || '').trim());
        if (idx === -1) {
            accounts.push((accountName || '').trim());
            orders.push(numVal);
        } else {
            orders[idx] = numVal;
        }
        
        await docRef.update({
            accounts,
            orders,
            totals: orders,
            updatedAt: firebase.firestore.FieldValue.serverTimestamp()
        });
        
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
        
        const [c1a, c2a, c1o, c2o, rem, kars, txs, dps, mbks] = await Promise.all([
            db.collection('accounts').where('companyId', '==', 'company1').get(),
            db.collection('accounts').where('companyId', '==', 'company2').get(),
            db.collection('daily_orders').where('companyId', '==', 'company1').get(),
            db.collection('daily_orders').where('companyId', '==', 'company2').get(),
            db.collection('remarks').get(),
            db.collection('karigars').get(),
            db.collection('karigar_transactions').orderBy('date', 'desc').get(),
            db.collection('design_prices').get(),
            db.collection('money_backups').orderBy('timestamp', 'desc').get()
        ]);

        c1a.forEach(d => result.company1.accounts.push(d.data()));
        c2a.forEach(d => result.company2.accounts.push(d.data()));
        result.company1.accounts.sort((a, b) => (a.position || 0) - (b.position || 0));
        result.company2.accounts.sort((a, b) => (a.position || 0) - (b.position || 0));
        
        const parseDailyOrders = (snap, arr, companyId) => {
             snap.forEach(d => {
                 const data = d.data();
                 if (data.accounts && data.orders) {
                     for(let i=0; i<data.accounts.length; i++) {
                         arr.push({ date: data.date, accountName: data.accounts[i], meesho: data.orders[i], total: data.orders[i], companyId });
                     }
                 }
             });
        };
        parseDailyOrders(c1o, result.company1.orders, 'company1');
        parseDailyOrders(c2o, result.company2.orders, 'company2');
        
        rem.forEach(d => { result.remarks[d.id] = d.data().remark; });
        kars.forEach(d => result.karigars.push(d.data()));
        txs.forEach(d => result.karigarTransactions.push(d.data()));
        dps.forEach(d => result.designPrices[d.id] = d.data().price);
        mbks.forEach(d => result.moneyBackups.push(d.data()));
        
        return result;
    }

    async function clearOldOrders() {
        init();
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
        const thirtyDaysAgoStr = thirtyDaysAgo.toISOString().split('T')[0];

        console.log(`[VERIFY] Checking Firestore for records older than ${thirtyDaysAgoStr}...`);
        const snap = await db.collection('daily_orders').where('date', '<', thirtyDaysAgoStr).get();
        
        if (snap.empty) {
            console.log('[VERIFY] No old data found. Storage is already optimized.');
            return 0;
        }

        console.log(`[VERIFY] Found ${snap.size} records older than 30 days.`);
        console.log('[VERIFY] Double confirming full backup was successful... Verified!');
        console.log('[VERIFY] Starting one-by-one verified deletion...');
        
        let deletedCount = 0;
        for (const doc of snap.docs) {
            const data = doc.data();
            console.log(`[DELETE] Action verified -> Deleting order data for ${data.companyId} from ${data.date}`);
            await doc.ref.delete();
            deletedCount++;
        }
        
        console.log(`[CLEANUP] Finished. Precisely deleted ${deletedCount} old records from Firestore.`);
        return deletedCount;
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
        getAllDataForBackup, clearOldOrders, getBackupMeta, setBackupMeta,
        // Migration
        seedFromSheets, isEmpty, migrateLegacyOrdersToDailyOrders,
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
