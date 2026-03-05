// ===== KRIMAA APP CONFIGURATION =====
// NOTE: Firebase client config values are NOT secrets - security is enforced
// via Firestore Security Rules, not by hiding these values.
// For production, configure proper Firestore Security Rules in Firebase Console.

const FIREBASE_CONFIG = {
    apiKey: "AIzaSyAZtVy7azaf6dLcc7FvMZ4MWIEv1ExwsjM",
    authDomain: "krimaa-f7fc3.firebaseapp.com",
    projectId: "krimaa-f7fc3",
    storageBucket: "krimaa-f7fc3.firebasestorage.app",
    messagingSenderId: "74955019006",
    appId: "1:74955019006:web:8050648012cb37f0134baf",
    measurementId: "G-XQMYDH5BH7"
};

// Google Sheets API (used only for backup/archive)
const SHEETS_API_URL = "https://script.google.com/macros/s/AKfycbwj9oL5WWKMGzSGYv3llJTjbPcHg8z2DvCdtquDIvmMAlsEt01mDvd0_IFdzSRVvPgT/exec";

// App behaviour
const APP_CONFIG = {
    writeBufferMs: 60000,       // 1 minute buffer before flushing edits to Firestore
    dailyBackupEnabled: true,   // Auto-backup to Sheets daily
    monthlyCleanup: true,       // Monthly: move all data to Sheets + clear Firestore
    backupCheckIntervalMs: 3600000 // Check backup status every hour
};
