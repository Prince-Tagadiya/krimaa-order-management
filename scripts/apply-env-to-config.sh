#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: ./scripts/apply-env-to-config.sh .env.client1"
  exit 1
fi

ENV_FILE="$1"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Env file not found: $ENV_FILE"
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

: "${FIREBASE_API_KEY:?Missing FIREBASE_API_KEY}"
: "${FIREBASE_AUTH_DOMAIN:?Missing FIREBASE_AUTH_DOMAIN}"
: "${FIREBASE_PROJECT_ID:?Missing FIREBASE_PROJECT_ID}"
: "${FIREBASE_STORAGE_BUCKET:?Missing FIREBASE_STORAGE_BUCKET}"
: "${FIREBASE_MESSAGING_SENDER_ID:?Missing FIREBASE_MESSAGING_SENDER_ID}"
: "${FIREBASE_APP_ID:?Missing FIREBASE_APP_ID}"
: "${FIREBASE_MEASUREMENT_ID:?Missing FIREBASE_MEASUREMENT_ID}"
: "${SHEETS_API_URL:?Missing SHEETS_API_URL}"

CLIENT_MODE_VALUE="${CLIENT_MODE:-multi_company}"
CLIENT_COMPANY_NAME_VALUE="${CLIENT_COMPANY_NAME:-}"
AUTH_USERS_JSON_VALUE="${AUTH_USERS_JSON:-[]}"

cat > env.js <<EOF
// ===== KRIMAA APP ENV =====
// Auto-generated from ${ENV_FILE}

const FIREBASE_CONFIG = window.FIREBASE_CONFIG = {
    apiKey: "${FIREBASE_API_KEY}",
    authDomain: "${FIREBASE_AUTH_DOMAIN}",
    projectId: "${FIREBASE_PROJECT_ID}",
    storageBucket: "${FIREBASE_STORAGE_BUCKET}",
    messagingSenderId: "${FIREBASE_MESSAGING_SENDER_ID}",
    appId: "${FIREBASE_APP_ID}",
    measurementId: "${FIREBASE_MEASUREMENT_ID}"
};

// Google Sheets API (used only for backup/archive)
const SHEETS_API_URL = window.SHEETS_API_URL = "${SHEETS_API_URL}";

const CLIENT_MODE = window.CLIENT_MODE = "${CLIENT_MODE_VALUE}";
const CLIENT_COMPANY_NAME = window.CLIENT_COMPANY_NAME = "${CLIENT_COMPANY_NAME_VALUE}";
const AUTH_USERS = window.AUTH_USERS = ${AUTH_USERS_JSON_VALUE};

// App behaviour
const APP_CONFIG = window.APP_CONFIG = {
    writeBufferMs: ${APP_WRITE_BUFFER_MS:-600000},
    dailyBackupEnabled: ${APP_DAILY_BACKUP_ENABLED:-true},
    monthlyCleanup: ${APP_MONTHLY_CLEANUP:-true},
    backupCheckIntervalMs: ${APP_BACKUP_CHECK_INTERVAL_MS:-3600000},
    clientMode: CLIENT_MODE,
    clientCompanyName: CLIENT_COMPANY_NAME
};
EOF

echo "env.js generated from ${ENV_FILE}"
