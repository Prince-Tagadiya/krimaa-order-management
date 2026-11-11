# Krimaa Order Management

Online, mobile-compatible order and karigar management using Firebase Cloud Firestore. The app no longer requires a local folder, shared drive or Google Sheets. Business data lives in Firestore; browser storage only retains login/UI preferences and quota cooldown. Unsaved edits stay in memory, so keep the tab open until the Saved indicator appears.

## Run

1. Keep Firebase configuration and the existing login configuration in the ignored `.env` file, or the Netlify environment. Never commit credentials.
2. Generate local config with `./scripts/apply-env-to-config.sh .env`.
3. Run `python3 serve.py` and open the address it prints. Opening HTML directly is unsupported.
4. Netlify deploys this repository and reads runtime settings from `/.netlify/functions/env`.

The existing login system uses browser-side user configuration. Moving this to Firebase Authentication and deploying restrictive security rules requires the Firebase project owner's credentials; this is a separate access-dependent step and must not be mistaken for production-grade access control.

## Data and saves

- Orders use `daily_orders/{companyId}__{YYYY-MM-DD}`. Each document contains account rows with independent `meesho` and `flipkart` quantities. Transactions preserve concurrent edits to other cells.
- The app coalesces edits to the same cell, serializes pending writes and retains failed writes for manual retry. Changing companies and logging out wait for pending saves.
- Full-history polling, LAN synchronization, Sheets fallbacks and automatic archival deletion are disabled. Click Refresh to retrieve updates from other devices. The datasheet initially shows the current month; previous months and All Time remain available.
- Karigar paid statuses and cycle settings use `cloud_state`. Reports download as A4 PDFs. Print opens the PDF viewer (or downloads when popups are blocked), supporting mobile sharing/printing.
- Both supplied original backups and the previous cloud snapshot are stored in `migration_backups`. Migration verification is retained privately in `.migration/`.

## Checks

Run `node --test tests/firestore.test.cjs`. Syntax-check the JavaScript with `node --check app.js` and `node --check firebase-service.js`.

Firestore limits cannot be eliminated. The current implementation reduces duplicate reads/writes and pauses on quota errors without falsely reporting a successful save. Monitor project-wide usage in Firebase; billing and budget configuration require the project owner.
