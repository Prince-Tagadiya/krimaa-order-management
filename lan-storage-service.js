// ===== LAN-FIRST SECURE STORAGE & SYNC SERVICE =====
// Uses File System Access API for local LAN shared folder reading/writing.
// Stores all database collections as raw, readable JSON files on the shared drive.

const LanStorageService = (() => {
    let _dirHandle = null;
    let _status = "disconnected"; // disconnected | connected | read-only
    let _listeners = [];

    const DB_NAME = "KrimaaLanStore";
    const STORE_NAME = "handles";
    const HANDLE_KEY = "directory_handle";

    // ───── INDEXEDDB HANDLE PERSISTENCE ─────
    function _openDb() {
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
        const db = await _openDb();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readwrite");
            const store = tx.objectStore(STORE_NAME);
            const request = store.put(handle, HANDLE_KEY);
            request.onsuccess = () => resolve(true);
            request.onerror = () => reject(request.error);
        });
    }

    async function loadHandle() {
        const db = await _openDb();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readonly");
            const store = tx.objectStore(STORE_NAME);
            const request = store.get(HANDLE_KEY);
            request.onsuccess = () => resolve(request.result || null);
            request.onerror = () => reject(request.error);
        });
    }

    async function clearHandle() {
        const db = await _openDb();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readwrite");
            const store = tx.objectStore(STORE_NAME);
            const request = store.delete(HANDLE_KEY);
            request.onsuccess = () => resolve(true);
            request.onerror = () => reject(request.error);
        });
    }

    // ───── DIRECTORY / FILE MANAGEMENT ─────
    function setStatus(s) {
        _status = s;
        _listeners.forEach(fn => fn(s));
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

    async function connect() {
        try {
            _dirHandle = await window.showDirectoryPicker();
            await saveHandle(_dirHandle);
            localStorage.setItem("lan_configured_v1", "true");
            
            // Verify writing permission
            const hasAccess = await verifyPermission(_dirHandle, true);
            if (!hasAccess) {
                setStatus("read-only");
                return { success: true, status: "read-only" };
            }

            // Write verification token to shared folder
            await writeEncryptedFile(".krimaa_lan_lock", { active: true, updatedAt: new Date().toISOString() });
            setStatus("connected");
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
            
            // Check permission silently
            const options = { mode: "readwrite" };
            if ((await _dirHandle.queryPermission(options)) === "granted") {
                setStatus("connected");
                return true;
            } else {
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
        localStorage.removeItem("lan_configured_v1");
        setStatus("disconnected");
    }

    // ───── HIGH LEVEL FILE READS / WRITES ─────
    // Kept method name as 'EncryptedFile' to preserve routing compatibility with firebase-service.js,
    // but reads/writes raw unencrypted .json files directly for simplicity!
    async function writeEncryptedFile(filename, data) {
        if (!_dirHandle) throw new Error("No folder selected");
        if (_status === "read-only") throw new Error("LAN folder is currently read-only. Cannot write data.");
        
        const jsonFilename = filename.replace(/\.enc$/, '.json');
        const fileHandle = await _dirHandle.getFileHandle(jsonFilename, { create: true });
        const plainText = JSON.stringify(data, null, 2);
        
        const writable = await fileHandle.createWritable();
        await writable.write(plainText);
        await writable.close();
    }

    async function readEncryptedFile(filename, fallbackData = null) {
        if (!_dirHandle) throw new Error("No folder selected");
        try {
            const jsonFilename = filename.replace(/\.enc$/, '.json');
            const fileHandle = await _dirHandle.getFileHandle(jsonFilename);
            const file = await fileHandle.getFile();
            const plainText = await file.text();
            if (!plainText.trim()) return fallbackData;
            return JSON.parse(plainText);
        } catch (e) {
            if (e.name === "NotFoundError") {
                return fallbackData;
            }
            throw e;
        }
    }

    // ───── EXPORT / IMPORT (CONSOLIDATED DATABASE FILE) ─────
    async function generateBackupPayload() {
        const collections = [
            "accounts", "daily_orders", "daily_summary", "karigars", "karigar_reset_backups",
            "karigar_transactions", "money_records", "money_backups", "design_prices", 
            "design_price_history", "remarks", "system"
        ];
        
        // Dynamically add YYYY_MM orders partition tables for 2024 to 2028
        const years = ["2024", "2025", "2026", "2027", "2028"];
        const months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"];
        for (const y of years) {
            for (const m of months) {
                collections.push(`orders_${y}_${m}`);
            }
        }

        const payload = {
            exportedAt: new Date().toISOString(),
            data: {}
        };

        const db = firebase.firestore();
        for (const colName of collections) {
            try {
                const snap = await db.collection(colName).get();
                if (!snap.empty) {
                    const docs = [];
                    snap.forEach(doc => {
                        docs.push({ id: doc.id, ...doc.data() });
                    });
                    payload.data[colName] = docs;
                }
            } catch (e) {
                console.warn(`Export skipped for ${colName}:`, e.message);
            }
        }
        return payload;
    }

    async function bootstrapFolderFromBackup(backupPayload) {
        if (!_dirHandle) throw new Error("No folder selected");
        if (!backupPayload || typeof backupPayload.data !== "object") {
            throw new Error("Invalid backup payload structure");
        }

        const dataMap = backupPayload.data;
        for (const [colName, docs] of Object.entries(dataMap)) {
            const filename = `${colName}.json`;
            await writeEncryptedFile(filename, docs);
        }

        // Write setup marker
        await writeEncryptedFile(".krimaa_lan_lock", {
            active: true,
            updatedAt: new Date().toISOString(),
            bootstrapped: true
        });
    }

    return {
        connect,
        autoReconnect,
        requestUnlock,
        disconnect,
        writeEncryptedFile,
        readEncryptedFile,
        generateBackupPayload,
        bootstrapFolderFromBackup,
        onStatusChange,
        getStatus: () => _status,
        isConnected: () => _status === "connected",
        isReadOnly: () => _status === "read-only",
        hasSavedHandle: () => localStorage.getItem("lan_configured_v1") === "true"
    };
})();

window.LanStorageService = LanStorageService;
