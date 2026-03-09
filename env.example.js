// ===== KRIMAA APP ENV (EXAMPLE) =====
// This file is only a reference. Real runtime file is generated as env.js from .env/.env.client*.

const FIREBASE_CONFIG = window.FIREBASE_CONFIG = {
    apiKey: "YOUR_FIREBASE_API_KEY",
    authDomain: "YOUR_PROJECT.firebaseapp.com",
    projectId: "YOUR_PROJECT_ID",
    storageBucket: "YOUR_PROJECT.firebasestorage.app",
    messagingSenderId: "YOUR_SENDER_ID",
    appId: "YOUR_APP_ID",
    measurementId: "YOUR_MEASUREMENT_ID"
};

const SHEETS_API_URL = window.SHEETS_API_URL = "YOUR_GOOGLE_APPS_SCRIPT_WEB_APP_URL";
const CLIENT_MODE = window.CLIENT_MODE = "multi_company"; // or single_company
const CLIENT_COMPANY_NAME = window.CLIENT_COMPANY_NAME = "";
const AUTH_USERS = window.AUTH_USERS = [
    { username: "admin_user", password: "admin_pass", role: "admin", displayName: "Admin", allowedCompanies: ["company1", "company2"] }
];

const APP_CONFIG = window.APP_CONFIG = {
    writeBufferMs: 600000,
    dailyBackupEnabled: true,
    monthlyCleanup: true,
    backupCheckIntervalMs: 3600000,
    clientMode: CLIENT_MODE,
    clientCompanyName: CLIENT_COMPANY_NAME
};
