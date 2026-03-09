(function () {
    // Safe defaults so app never crashes if runtime env endpoint fails.
    if (!window.FIREBASE_CONFIG) window.FIREBASE_CONFIG = {};
    if (!window.SHEETS_API_URL) window.SHEETS_API_URL = "";
    if (!window.CLIENT_MODE) window.CLIENT_MODE = "multi_company";
    if (!window.CLIENT_COMPANY_NAME) window.CLIENT_COMPANY_NAME = "";
    if (!Array.isArray(window.AUTH_USERS)) window.AUTH_USERS = [];
    if (!window.APP_CONFIG) {
        window.APP_CONFIG = {
            writeBufferMs: 600000,
            dailyBackupEnabled: true,
            monthlyCleanup: true,
            backupCheckIntervalMs: 3600000,
            clientMode: window.CLIENT_MODE,
            clientCompanyName: window.CLIENT_COMPANY_NAME
        };
    }

    const endpoints = [
        '/.netlify/functions/env',
        '/api/env.js',
        '/env.js'
    ];

    async function tryLoadRuntimeEnv() {
        for (const url of endpoints) {
            try {
                const res = await fetch(url, { cache: 'no-store' });
                if (!res.ok) continue;
                const js = await res.text();
                if (!js || (!js.includes('window.APP_CONFIG') && !js.includes('window.FIREBASE_CONFIG'))) {
                    continue;
                }
                // Same-origin runtime config script execution.
                (0, eval)(js);
                return true;
            } catch (e) {}
        }
        return false;
    }

    window.__envReady = tryLoadRuntimeEnv();
})();
