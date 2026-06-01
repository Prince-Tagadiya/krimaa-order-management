// ===== LAN-FIRST SECURE STORAGE & SYNC SERVICE =====
// Uses File System Access API for local LAN shared folder reading/writing,
// and browser native Web Crypto API (AES-GCM 256-bit) for file-level encryption.

const LanStorageService = (() => {
    let _dirHandle = null;
    let _cryptoKey = null;
    let _password = "";
    let _status = "disconnected"; // disconnected | connected | read-only
    let _listeners = [];

    const DB_NAME = "KrimaaLanStore";
    const STORE_NAME = "handles";
    const HANDLE_KEY = "directory_handle";
    const PASSWORD_SALT = "krimaa_lan_salt_v1";

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

    // ───── NATIVE WEB CRYPTO API (AES-GCM 256-bit) ─────
    async function _deriveKey(password) {
        const enc = new TextEncoder();
        const keyMaterial = await crypto.subtle.importKey(
            "raw",
            enc.encode(password),
            { name: "PBKDF2" },
            false,
            ["deriveBits", "deriveKey"]
        );
        return crypto.subtle.deriveKey(
            {
                name: "PBKDF2",
                salt: enc.encode(PASSWORD_SALT),
                iterations: 100000,
                hash: "SHA-256"
            },
            keyMaterial,
            { name: "AES-GCM", length: 256 },
            false,
            ["encrypt", "decrypt"]
        );
    }

    async function encrypt(plainText, password) {
        const key = await _deriveKey(password);
        const enc = new TextEncoder();
        const iv = crypto.getRandomValues(new Uint8Array(12));
        const encrypted = await crypto.subtle.encrypt(
            { name: "AES-GCM", iv: iv },
            key,
            enc.encode(plainText)
        );

        // Package as: IV (12 bytes) + Encrypted Data
        const ivHex = Array.from(iv).map(b => b.toString(16).padStart(2, '0')).join('');
        const dataB64 = btoa(String.fromCharCode(...new Uint8Array(encrypted)));
        return JSON.stringify({ iv: ivHex, data: dataB64 });
    }

    async function decrypt(cipherJson, password) {
        const key = await _deriveKey(password);
        const { iv: ivHex, data: dataB64 } = JSON.parse(cipherJson);

        const iv = new Uint8Array(ivHex.match(/.{1,2}/g).map(byte => parseInt(byte, 16)));
        const encrypted = new Uint8Array(
            atob(dataB64).split("").map(c => c.charCodeAt(0))
        );

        const decrypted = await crypto.subtle.decrypt(
            { name: "AES-GCM", iv: iv },
            key,
            encrypted
        );
        return new TextDecoder().decode(decrypted);
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

    async function connect(password) {
        try {
            _password = password;
            _dirHandle = await window.showDirectoryPicker();
            await saveHandle(_dirHandle);
            localStorage.setItem("lan_password_configured", "true");
            
            // Derive and verify writing permission
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

    async function autoReconnect(password) {
        try {
            _dirHandle = await loadHandle();
            if (!_dirHandle) {
                setStatus("disconnected");
                return false;
            }
            _password = password;
            
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
        _cryptoKey = null;
        _password = "";
        clearHandle();
        localStorage.removeItem("lan_password_configured");
        setStatus("disconnected");
    }

    // ───── HIGH LEVEL FILE READS / WRITES ─────
    async function writeEncryptedFile(filename, data) {
        if (!_dirHandle) throw new Error("No folder selected");
        if (_status === "read-only") throw new Error("LAN folder is currently read-only. Cannot write data.");
        
        const fileHandle = await _dirHandle.getFileHandle(filename, { create: true });
        const plainText = JSON.stringify(data);
        const cipherText = await encrypt(plainText, _password);
        
        const writable = await fileHandle.createWritable();
        await writable.write(cipherText);
        await writable.close();
    }

    async function readEncryptedFile(filename, fallbackData = null) {
        if (!_dirHandle) throw new Error("No folder selected");
        try {
            const fileHandle = await _dirHandle.getFileHandle(filename);
            const file = await fileHandle.getFile();
            const cipherText = await file.text();
            if (!cipherText.trim()) return fallbackData;
            
            const plainText = await decrypt(cipherText, _password);
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
            "accounts", "daily_orders", "karigars", "karigar_transactions",
            "money_records", "money_backups", "design_prices", "design_price_history", "remarks"
        ];
        const payload = {
            exportedAt: new Date().toISOString(),
            data: {}
        };

        const db = FirebaseService.getDb();
        for (const colName of collections) {
            try {
                const snap = await db.collection(colName).get();
                const docs = [];
                snap.forEach(doc => {
                    docs.push({ id: doc.id, ...doc.data() });
                });
                payload.data[colName] = docs;
            } catch (e) {
                console.warn(`Export failed for ${colName}:`, e);
                payload.data[colName] = [];
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
        
        // Save each collection to the local encrypted files
        for (const [colName, docs] of Object.entries(dataMap)) {
            const filename = `${colName}.enc`;
            // Transform array list to object storage format for easy direct shimming if needed,
            // or store directly as list. Let's store directly as list database dump.
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
        hasSavedHandle: () => localStorage.getItem("lan_password_configured") === "true"
    };
})();

window.LanStorageService = LanStorageService;
