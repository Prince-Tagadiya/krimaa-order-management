// ===== LAN-FIRST OFFLINE STORAGE SERVICE =====
// Uses File System Access API (Chrome/Edge) to read/write raw JSON files
// on a shared local-network drive. Firebase is used ONLY as a background
// write-only sync target — never read on startup.

const LanStorageService = (() => {
    let _dirHandle = null;
    let _status = "disconnected"; // disconnected | connected | read-only
    let _listeners = [];
    let _pendingCount = 0;

    const DB_NAME = "KrimaaLanStore";
    const STORE_NAME = "handles";
    const HANDLE_KEY = "directory_handle"; // legacy default key (Krimaa admin — backward compat)

    // ───── PER-PROFILE HANDLE KEY SUPPORT ─────
    // Each user login profile can have its own separate LAN folder.
    // Krimaa / admin uses the original legacy key for full backward compatibility.
    // Other users (Dhyan_Order, Krimaa_Users, etc.) get their own key.
    let _activeProfile = null; // username string, null = use default

    // Profiles that share the legacy default key (no isolation needed)
    const LEGACY_PROFILE_USERS = ['Krimaa', 'dev'];

    function _getHandleKey() {
        if (!_activeProfile || LEGACY_PROFILE_USERS.includes(_activeProfile)) {
            return HANDLE_KEY; // backward compat — Krimaa admin key unchanged
        }
        return `${HANDLE_KEY}_${_activeProfile}`;
    }

    function _getLanConfiguredKey() {
        if (!_activeProfile || LEGACY_PROFILE_USERS.includes(_activeProfile)) {
            return 'lan_configured_v1'; // legacy key unchanged
        }
        return `lan_configured_${_activeProfile}`;
    }

    function setActiveProfile(username) {
        _activeProfile = username || null;
        console.log('[LAN] Active profile set to:', _activeProfile, '→ handle key:', _getHandleKey());
    }

    function getActiveProfile() {
        return _activeProfile;
    }

    // ───── INDEXEDDB HANDLE PERSISTENCE ─────
    function _openIdb() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, 1);
            request.onupgradeneeded = (e) => {
                const db = e.target.result;
                if (!db.objectStoreNames.contains(STORE_NAME)) {
                    db.createObjectStore(STORE_NAME);
                }
            };
            request.onsuccess = (e) => resolve(e.target.result);
            request.onerror = (e) => reject(e.target.error);
        });
    }

    async function saveHandle(handle) {
        const db = await _openIdb();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readwrite");
            const store = tx.objectStore(STORE_NAME);
            const request = store.put(handle, _getHandleKey());
            request.onsuccess = () => resolve(true);
            request.onerror = () => reject(request.error);
        });
    }

    async function loadHandle() {
        const db = await _openIdb();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readonly");
            const store = tx.objectStore(STORE_NAME);
            const request = store.get(_getHandleKey());
            request.onsuccess = () => resolve(request.result || null);
            request.onerror = () => reject(request.error);
        });
    }

    async function clearHandle() {
        const db = await _openIdb();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readwrite");
            const store = tx.objectStore(STORE_NAME);
            const request = store.delete(_getHandleKey());
            request.onsuccess = () => resolve(true);
            request.onerror = () => reject(request.error);
        });
    }

    // ───── STATUS MANAGEMENT ─────
    function setStatus(s) {
        _status = s;
        _listeners.forEach(fn => fn(s));
        _notifyPendingBadge();
    }

    function onStatusChange(fn) {
        _listeners.push(fn);
    }

    async function verifyPermission(fileHandle, readWrite) {
        const options = {};
        if (readWrite) options.mode = "readwrite";
        if ((await fileHandle.queryPermission(options)) === "granted") return true;
        if ((await fileHandle.requestPermission(options)) === "granted") return true;
        return false;
    }

    // ───── CONNECT / DISCONNECT ─────
    async function connect() {
        try {
            _dirHandle = await window.showDirectoryPicker({ mode: 'readwrite' });
            await saveHandle(_dirHandle);
            localStorage.setItem(_getLanConfiguredKey(), "true");

            const hasAccess = await verifyPermission(_dirHandle, true);
            if (!hasAccess) {
                setStatus("read-only");
                return { success: true, status: "read-only" };
            }

            // Write setup token
            await writeFile(".krimaa_lan_lock.json", { active: true, updatedAt: new Date().toISOString() });
            setStatus("connected");
            await _refreshPendingCount();
            return { success: true, status: "connected" };
        } catch (e) {
            console.error("LAN Connection Failed:", e);
            setStatus("disconnected");
            return { success: false, error: e.message };
        }
    }

    async function autoReconnect() {
        try {
            _dirHandle = await loadHandle();
            if (!_dirHandle) {
                setStatus("disconnected");
                return false;
            }
            const options = { mode: "readwrite" };
            const perm = await _dirHandle.queryPermission(options);
            if (perm === "granted") {
                setStatus("connected");
                await _refreshPendingCount();
                return true;
            } else {
                // Has handle but permission not granted yet (need user gesture)
                setStatus("read-only");
                return false;
            }
        } catch (e) {
            console.warn("LAN Reconnect skipped:", e);
            setStatus("disconnected");
            return false;
        }
    }

    async function requestUnlock() {
        if (!_dirHandle) return false;
        try {
            const hasAccess = await verifyPermission(_dirHandle, true);
            if (hasAccess) {
                setStatus("connected");
                return true;
            }
        } catch (e) {
            console.error(e);
        }
        return false;
    }

    function disconnect() {
        _dirHandle = null;
        clearHandle();
        localStorage.removeItem(_getLanConfiguredKey());
        _pendingCount = 0;
        setStatus("disconnected");
    }

    // ───── FILE READS / WRITES ─────
    // Normalizes filenames: accepts "foo", "foo.json", or "foo.enc" → always uses "foo.json"
    function _normalize(filename) {
        return filename.replace(/\.enc$/, '').replace(/\.json$/, '') + '.json';
    }

    async function writeFile(filename, data) {
        if (!_dirHandle) throw new Error("No folder selected");
        if (_status === "read-only") throw new Error("LAN folder is read-only.");
        const fname = _normalize(filename);
        
        let cleanedData = data;
        // Enforce Database Unique Constraint: (date + company + accountName) for order arrays
        if (Array.isArray(data) && (fname.startsWith('orders_') || fname === 'daily_orders.json')) {
            const dedupMap = new Map();
            data.forEach(o => {
                const d = o.date;
                const comp = o.companyId || o.masterCompany || 'company1';
                const accName = String(o.accountName || o.accountId || '').toLowerCase().trim();
                if (d && accName) {
                    const key = `${d}__${comp}__${accName}`;
                    // Keep the latest submitted entry for that unique key
                    dedupMap.set(key, o);
                }
            });
            cleanedData = Array.from(dedupMap.values());
        }

        const fileHandle = await _dirHandle.getFileHandle(fname, { create: true });
        const writable = await fileHandle.createWritable();
        await writable.write(JSON.stringify(cleanedData, null, 2));
        await writable.close();
    }

    // Alias for backward compat with firebase-service.js which calls writeEncryptedFile
    async function writeEncryptedFile(filename, data) {
        return writeFile(filename, data);
    }

    async function readFile(filename, fallback = null) {
        if (!_dirHandle) return fallback;
        try {
            const fname = _normalize(filename);
            const fileHandle = await _dirHandle.getFileHandle(fname);
            const file = await fileHandle.getFile();
            const text = await file.text();
            if (!text || !text.trim()) return fallback;
            return JSON.parse(text);
        } catch (e) {
            if (e.name === "NotFoundError") return fallback;
            console.warn('[LAN] readFile error for', filename, e.message);
            return fallback;
        }
    }

    // Alias for backward compat
    async function readEncryptedFile(filename, fallback = null) {
        return readFile(filename, fallback);
    }

    // ───── SYNC QUEUE ─────
    // Every Firebase write is queued here and pushed when online.
    const SYNC_QUEUE_FILE = 'sync_queue';

    async function _loadQueue() {
        return readFile(SYNC_QUEUE_FILE, []);
    }

    async function _saveQueue(queue) {
        await writeFile(SYNC_QUEUE_FILE, queue);
    }

    async function enqueueSyncOp(colName, docId, data, op) {
        // Firebase disabled for offline LAN mode
        return;
    }

    async function getPendingCount() {
        return 0;
    }

    async function _refreshPendingCount() {
        _pendingCount = 0;
        _notifyPendingBadge();
    }

    function _notifyPendingBadge() {
        if (typeof window !== 'undefined' && typeof window.updateFirebasePendingBadge === 'function') {
            window.updateFirebasePendingBadge(0, _status);
        }
    }

    async function flushSyncQueue() {
        // Firebase disabled for offline LAN mode
        return;
    }

    // ───── FIREBASE → LAN PULL SYNC ─────
    // Fetches the latest data from Firebase and merges it into the local LAN folder.
    // This keeps the admin's LAN folder in sync with changes made by other profiles (e.g. Dhyan).
    // - Never wipes local data. Uses upsert / merge logic only.
    // - Runs silently in background — no UI blocking.
    // - Only touches recent months (current + previous) for orders, for performance.
    let _pullInProgress = false;
    let _lastPullMs = 0;
    const PULL_COOLDOWN_MS = 30 * 1000; // minimum 30s between pulls

    async function pullLatestFromFirebase(options = {}) {
        // In LAN offline/local mode, the LAN drive is the authoritative database for the client.
        // Prevent remote unmigrated Firestore from polluting the clean LAN drive.
        return { pulled: 0, skipped: 'lan_is_authoritative' };
    }


    async function generateBackupPayload() {
        const collections = [
            "accounts", "daily_orders", "daily_summary", "karigars", "karigar_reset_backups",
            "karigar_transactions", "money_records", "money_backups", "design_prices",
            "design_price_history", "remarks", "system"
        ];
        const years = ["2024", "2025", "2026", "2027", "2028"];
        const months = ["01","02","03","04","05","06","07","08","09","10","11","12"];
        for (const y of years) {
            for (const m of months) {
                collections.push(`orders_${y}_${m}`);
            }
        }

        const zip = new JSZip();
        if (typeof firebase !== 'undefined' && firebase.firestore) {
            const db = firebase.firestore();
            for (const colName of collections) {
                try {
                    const snap = await db.collection(colName).get();
                    if (!snap.empty) {
                        const docs = [];
                        snap.forEach(doc => docs.push({ id: doc.id, ...doc.data() }));
                        zip.file(`${colName}.json`, JSON.stringify(docs, null, 2));
                    }
                } catch (e) {
                    console.warn(`Export skipped for ${colName}:`, e.message);
                }
            }
        } else {
            for (const colName of collections) {
                try {
                    const data = await readFile(colName, null);
                    if (data !== null) {
                        zip.file(`${colName}.json`, JSON.stringify(data, null, 2));
                    }
                } catch (e) {}
            }
        }

        zip.file(".krimaa_lan_lock.json", JSON.stringify({
            active: true,
            updatedAt: new Date().toISOString(),
            bootstrapped: true
        }, null, 2));

        return zip.generateAsync({ type: "blob" });
    }

    // ───── IMPORT ZIP → LAN FOLDER ─────
    async function bootstrapFolderFromZip(zipFile) {
        if (!_dirHandle) throw new Error("No folder selected");
        const zip = await JSZip.loadAsync(zipFile);
        for (const [filename, file] of Object.entries(zip.files)) {
            if (file.dir) continue;
            const text = await file.async('text');
            try {
                const data = JSON.parse(text);
                const fileHandle = await _dirHandle.getFileHandle(filename, { create: true });
                const writable = await fileHandle.createWritable();
                await writable.write(JSON.stringify(data, null, 2));
                await writable.close();
            } catch (e) {
                console.warn('[LAN] Bootstrap error for', filename, e.message);
            }
        }
    }

    // Legacy import from JSON payload (not zip)
    async function bootstrapFolderFromBackup(backupPayload) {
        if (!_dirHandle) throw new Error("No folder selected");
        if (backupPayload && typeof backupPayload.data === "object") {
            const dataMap = backupPayload.data;
            for (const [colName, docs] of Object.entries(dataMap)) {
                await writeFile(colName, docs);
            }
        }
        await writeFile(".krimaa_lan_lock", { active: true, updatedAt: new Date().toISOString(), bootstrapped: true });
    }

    // ───── OFFLINE DATA FETCHING ─────
    async function fetchFromLocal(payload) {
        if (!_dirHandle || _status !== 'connected') return undefined;
        const action = payload?.action;
        const companyId = payload?.companyId || 'company1';

        try {
            if (action === 'getAccounts') {
                const all = await readFile('accounts', []);
                const filtered = all.filter(a => a.companyId === companyId);
                filtered.sort((a, b) => (a.position || 0) - (b.position || 0));
                return { success: true, data: filtered.map(a => a.name), details: filtered };
            }
            if (action === 'getKarigars') {
                const all = await readFile('karigars', []);
                const filtered = all.filter(k => k.companyId === companyId);
                return { success: true, data: filtered };
            }
            if (action === 'getDesignPrices') {
                const all = await readFile('design_prices', []);
                const filtered = all.filter(d => (d.companyId || 'global') === (companyId || 'global'));
                return { success: true, data: filtered };
            }
            if (action === 'getRemarks') {
                const all = await readFile('remarks', []);
                return { success: true, data: all };
            }
            if (action === 'getKarigarTransactions') {
                const all = await readFile('karigar_transactions', []);
                const filtered = all.filter(t => t.companyId === companyId);
                if (payload.month) {
                    return { success: true, data: filtered.filter(t => (t.date || '').startsWith(payload.month)) };
                }
                return { success: true, data: filtered };
            }
            if (action === 'getDashboardData') {
                const month = payload.month; // e.g. "2024-03"
                let all = [];
                if (month) {
                    const y = month.split('-')[0];
                    const m = month.split('-')[1];
                    all = await readFile(`orders_${y}_${m}`, []);
                } else {
                    try {
                        for await (const entry of _dirHandle.values()) {
                            if (entry.kind === 'file' && entry.name.startsWith('orders_') && entry.name.endsWith('.json')) {
                                const fileData = await readFile(entry.name, []);
                                if (Array.isArray(fileData)) {
                                    all.push(...fileData);
                                }
                            }
                        }
                    } catch (err) {
                        console.warn('[LAN] Error reading all monthly files:', err);
                    }
                }
                

                // Realistic Decoding: Group duplicate records, parse meesho and flipkart, compute total
                const dedupMap = new Map();
                all.forEach(o => {
                    const m = parseInt(o.meesho, 10) || 0;
                    const f = parseInt(o.flipkart, 10) || 0;
                    const t = typeof o.total !== 'undefined' ? (parseInt(o.total, 10) || 0) : (m + f);
                    o.meesho = m;
                    o.flipkart = f;
                    o.total = t;
                    o.quantity = t;
                    const d = o.date;
                    const comp = o.companyId || o.masterCompany || 'company1';
                    const accKey = String(o.accountId || o.accountName || '').toLowerCase().trim();
                    if (!d || !accKey) return;
                    if ((t === 40 || m === 40) && accKey.startsWith('acc_') && !o.accountName) return;
                    
                    const key = `${d}__${comp}__${accKey}`;
                    const existing = dedupMap.get(key);
                    if (existing) {
                        if ((existing.total || 0) === 0 && t > 0) {
                            dedupMap.set(key, o);
                        } else if (t > 0 && existing.total > 0 && (o.flipkart > 0 && (existing.flipkart || 0) === 0)) {
                            existing.flipkart = o.flipkart;
                            existing.total = (existing.meesho || 0) + existing.flipkart;
                            existing.quantity = existing.total;
                        }
                    } else {
                        dedupMap.set(key, o);
                    }
                });
                
                const uniqueOrders = Array.from(dedupMap.values());
                const filtered = uniqueOrders.filter(o => (o.companyId || o.masterCompany) === companyId);
                return { success: true, data: filtered };
            }
            if (action === 'getOrders') {
                const all = await readFile('daily_orders', []);
                const dedupMap = new Map();
                all.forEach(o => {
                    const d = o.date;
                    const comp = o.companyId || o.masterCompany || 'company1';
                    const accName = String(o.accountName || o.accountId || '').toLowerCase().trim();
                    if (!d || !accName) return;
                    const key = `${d}__${comp}__${accName}`;
                                        if ((parseInt(o.meesho, 10) === 40 || parseInt(o.quantity, 10) === 40) && String(o.accountName || o.accountId || '').startsWith('acc_')) return;
                    o.meesho = parseInt(o.meesho, 10) || parseInt(o.quantity, 10) || parseInt(o.total, 10) || 0;
                    const val = o.meesho;
                    
                    const existing = dedupMap.get(key);
                    if (existing) {
                        const existingVal = parseInt(existing.meesho, 10) || 0;
                        if (existingVal === 0) {
                            dedupMap.set(key, o);
                        } else if (val > 0 && val < existingVal) {
                            dedupMap.set(key, o);
                        }
                    } else {
                        dedupMap.set(key, o);
                    }
                });
                const unique = Array.from(dedupMap.values());
                return { success: true, data: unique.filter(o => (o.companyId || o.masterCompany) === companyId) };
            }
        } catch (e) {
            console.error('[LAN] fetchFromLocal failed:', e);
        }
        return undefined;
    }


    async function writeToLocal(payload) {
        if (!_dirHandle || _status !== 'connected') return { success: false, message: 'Offline storage not connected' };
        const action = payload?.action;
        const companyId = payload?.companyId || 'company1';

        try {
            if (action === 'submitOrders') {
                const date = payload.date; 
                const orders = payload.orders || [];
                const y = date.split('-')[0];
                const m = date.split('-')[1];
                const filename = `orders_${y}_${m}`;
                
                const existing = await readFile(filename, []);
                orders.forEach(newOrder => {
                    if ((parseInt(newOrder.meesho, 10) === 40 || parseInt(newOrder.quantity, 10) === 40) && String(newOrder.accountId || '').startsWith('acc_')) return;
                    newOrder.meesho = parseInt(newOrder.meesho, 10) || parseInt(newOrder.quantity, 10) || 0;
                    newOrder.companyId = companyId;
                    newOrder.date = date;
                    const idx = existing.findIndex(o => o.date === date && (o.accountId === newOrder.accountId || o.accountName === newOrder.accountName) && (o.companyId || o.masterCompany) === companyId);
                    if (idx >= 0) {
                        existing[idx] = newOrder;
                    } else {
                        existing.push(newOrder);
                    }
                });
                await writeFile(filename, existing);
                
                const daily = await readFile('daily_orders', []);
                orders.forEach(newOrder => {
                    if ((parseInt(newOrder.meesho, 10) === 40 || parseInt(newOrder.quantity, 10) === 40) && String(newOrder.accountId || '').startsWith('acc_')) return;
                    newOrder.meesho = parseInt(newOrder.meesho, 10) || parseInt(newOrder.quantity, 10) || 0;
                    const idx = daily.findIndex(o => o.date === date && (o.accountId === newOrder.accountId || o.accountName === newOrder.accountName) && (o.companyId || o.masterCompany) === companyId);
                    if (idx >= 0) {
                        daily[idx] = newOrder;
                    } else {
                        daily.push(newOrder);
                    }
                });
                await writeFile('daily_orders', daily);
                return { success: true };
            }
            if (action === 'updateOrder') {
                const date = payload.date;
                const accountId = payload.accountId;
                const accountName = payload.accountName || accountId;
                const field = payload.field || 'meesho';
                const val = parseInt(payload.value, 10) || 0;
                
                const y = date.split('-')[0];
                const m = date.split('-')[1];
                const filename = `orders_${y}_${m}`;
                
                const existing = await readFile(filename, []);
                let updated = false;
                existing.forEach(o => {
                    if (o.date === date && (o.accountId === accountId || o.accountName === accountId || o.accountName === accountName) && (o.companyId || o.masterCompany) === companyId) {
                        o[field] = val;
                        o.meesho = parseInt(o.meesho, 10) || 0;
                        o.flipkart = parseInt(o.flipkart, 10) || 0;
                        o.total = o.meesho + o.flipkart;
                        o.quantity = o.total;
                        updated = true;
                    }
                });
                if (!updated) {
                    const newOrder = {
                        orderId: `${date}_${accountId}`,
                        id: `${date}_${accountId}`,
                        date,
                        accountId,
                        accountName,
                        meesho: field === 'meesho' ? val : 0,
                        flipkart: field === 'flipkart' ? val : 0,
                        companyId,
                        masterCompany: companyId
                    };
                    newOrder.total = newOrder.meesho + newOrder.flipkart;
                    newOrder.quantity = newOrder.total;
                    existing.push(newOrder);
                }
                await writeFile(filename, existing);

                const daily = await readFile('daily_orders', []);
                let dUpdated = false;
                daily.forEach(o => {
                    if (o.date === date && (o.accountId === accountId || o.accountName === accountId || o.accountName === accountName) && (o.companyId || o.masterCompany) === companyId) {
                        o[field] = val;
                        o.meesho = parseInt(o.meesho, 10) || 0;
                        o.flipkart = parseInt(o.flipkart, 10) || 0;
                        o.total = o.meesho + o.flipkart;
                        o.quantity = o.total;
                        dUpdated = true;
                    }
                });
                if (!dUpdated) {
                    const newOrder = {
                        orderId: `${date}_${accountId}`,
                        id: `${date}_${accountId}`,
                        date,
                        accountId,
                        accountName,
                        meesho: field === 'meesho' ? val : 0,
                        flipkart: field === 'flipkart' ? val : 0,
                        companyId,
                        masterCompany: companyId
                    };
                    newOrder.total = newOrder.meesho + newOrder.flipkart;
                    newOrder.quantity = newOrder.total;
                    daily.push(newOrder);
                }
                await writeFile('daily_orders', daily);
                return { success: true };
            }
            if (action === 'addAccount') {
                const accounts = await readFile('accounts', []);
                accounts.push({
                    id: payload.accountName,
                    accountId: payload.accountName,
                    name: payload.accountName,
                    nameLower: String(payload.accountName).toLowerCase(),
                    companyId: companyId,
                    position: accounts.filter(a => a.companyId === companyId).length,
                    mobile: payload.mobile || '',
                    gstin: payload.gstin || '',
                    rechargeDate: payload.rechargeDate || '',
                    hasMeesho: payload.hasMeesho !== false,
                    hasFlipkart: !!payload.hasFlipkart
                });
                await writeFile('accounts', accounts);
                return { success: true };
            }
            if (action === 'editAccount') {
                const accounts = await readFile('accounts', []);
                const acc = accounts.find(a => (a.name === payload.accountId || a.id === payload.accountId || a.accountId === payload.accountId) && a.companyId === companyId);
                if (acc) {
                    if (payload.newName) {
                        acc.name = payload.newName;
                        acc.nameLower = String(payload.newName).toLowerCase();
                    }
                    if (payload.mobile !== undefined) acc.mobile = payload.mobile || '';
                    if (payload.gstin !== undefined) acc.gstin = payload.gstin || '';
                    if (payload.rechargeDate !== undefined) acc.rechargeDate = payload.rechargeDate || '';
                    if (payload.hasMeesho !== undefined) acc.hasMeesho = !!payload.hasMeesho;
                    if (payload.hasFlipkart !== undefined) acc.hasFlipkart = !!payload.hasFlipkart;
                    await writeFile('accounts', accounts);
                }
                return { success: true };
            }
            if (action === 'deleteAccount') {
                const accounts = await readFile('accounts', []);
                const filtered = accounts.filter(a => !((a.name === payload.accountId || a.id === payload.accountId) && a.companyId === companyId));
                await writeFile('accounts', filtered);
                return { success: true };
            }
            if (action === 'saveRemark') {
                const remarks = await readFile('remarks', []);
                const existing = remarks.find(r => r.date === payload.date);
                if (existing) {
                    existing.remark = payload.remark;
                } else {
                    remarks.push({ date: payload.date, remark: payload.remark });
                }
                await writeFile('remarks', remarks);
                return { success: true };
            }
        } catch (e) {
            console.error('[LAN] writeToLocal error:', e);
            return { success: false, error: e.message };
        }
        return { success: false, message: 'Action not supported offline' };
    }

    async function getFolderHash() {
        if (!_dirHandle || _status !== 'connected') return '';
        try {
            let hashParts = [];
            for await (const entry of _dirHandle.values()) {
                if (entry.kind === 'file') {
                    const file = await entry.getFile();
                    hashParts.push(`${entry.name}:${file.lastModified}`);
                }
            }
            return hashParts.sort().join('|');
        } catch (e) {
            return '';
        }
    }

    return {
        getFolderHash,
        connect,
        autoReconnect,
        requestUnlock,
        disconnect,
        writeFile,
        readFile,
        writeEncryptedFile,  // compat alias
        readEncryptedFile,   // compat alias
        enqueueSyncOp,
        flushSyncQueue,
        getPendingCount,
        generateBackupPayload,
        bootstrapFolderFromZip,
        bootstrapFolderFromBackup,
        fetchFromLocal,
        writeToLocal,
        onStatusChange,
        getStatus: () => _status,
        isConnected: () => _status === "connected",
        isReadOnly: () => _status === "read-only",
        hasSavedHandle: () => localStorage.getItem(_getLanConfiguredKey()) === "true",
        // Per-profile support
        setActiveProfile,
        getActiveProfile,
        // Cross-profile Firebase→LAN pull sync
        pullLatestFromFirebase,
    };
})();

window.LanStorageService = LanStorageService;
