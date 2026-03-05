// ===== KRIMAA APP CONFIGURATION (EXAMPLE) =====
// Copy this file to config.js and fill in your values.

const FIREBASE_CONFIG = {
    apiKey: "YOUR_FIREBASE_API_KEY",
    authDomain: "YOUR_PROJECT.firebaseapp.com",
    projectId: "YOUR_PROJECT_ID",
    storageBucket: "YOUR_PROJECT.firebasestorage.app",
    messagingSenderId: "YOUR_SENDER_ID",
    appId: "YOUR_APP_ID",
    measurementId: "YOUR_MEASUREMENT_ID"
};

// Google Sheets API (used only for backup/archive)
const SHEETS_API_URL = "YOUR_GOOGLE_APPS_SCRIPT_WEB_APP_URL";

// App behaviour
const APP_CONFIG = {
    writeBufferMs: 60000,       // 1 minute buffer before flushing edits to Firestore
    dailyBackupEnabled: true,   // Auto-backup to Sheets daily
    monthlyCleanup: true,       // Monthly: move all data to Sheets + clear Firestore
    backupCheckIntervalMs: 3600000 // Check backup status every hour
};
