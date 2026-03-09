# Krimaa Order Management

Welcome to the Krimaa Order Management App setup. This project contains a modern, simple frontend (HTML/CSS/JS) and a Google Apps Script backend.

## 1. Setup Google Sheets & Backend
1. Go to [Google Sheets](https://sheets.google.com) and create a new blank spreadsheet.
2. In the top menu, click **Extensions** -> **Apps Script**.
3. A new tab will open with a file named `Code.gs`.
4. Open the `Code.gs` file from this project folder (`/Applications/PRINCE/PROJECTS/KRIMAA/OrderApp/Code.gs`), copy all of its contents, and paste it into the Google Apps Script editor (replacing whatever is there).
5. Click the **Save** icon (or press `Cmd+S`).

## 2. Deploy Google Apps Script API
1. On the top right corner of the Apps Script editor, click the blue **Deploy** button.
2. Select **New deployment**.
3. Under "Select type" (the gear icon), choose **Web app**.
4. Fill in the deployment details:
   - **Description**: Add any description (e.g. `Order API v1`).
   - **Execute as**: Select `Me (<your_email>)`.
   - **Who has access**: Select `Anyone`. *(Important for the API to work from your website)*
5. Click **Deploy**.
6. Google will ask for Authorization. Click **Authorize access**, select your Google account, click **Advanced**, and then click **Go to Untitled project (unsafe)**. Finally, click **Allow**.
7. Once deployed, you will get a **Web app URL** (looks like `https://script.google.com/macros/s/AKfyc.../exec`).
8. Copy this URL.

## 3. Connect Frontend
Use env files and generate `env.js` (do not hardcode in app files).

1. Fill `.env` (or `.env.client1` / `.env.client2`) with Firebase + Sheets values.
   - Add dashboard users in `AUTH_USERS_JSON` (username/password/role/displayName/allowedCompanies).
   - Do not commit real credentials in repo files; keep them only in local env or Vercel Environment Variables.
2. Generate runtime config:
   - `./scripts/apply-env-to-config.sh .env`
3. Start the app.
4. On Vercel, set the same env variables in Project Settings → Environment Variables.
   - App loads runtime env from `/api/env.js` on deploy.
5. On Netlify, set the same env variables in Site settings → Environment variables.
   - App loads runtime env from `/.netlify/functions/env`.

## 4. Run the Application
1. Open the `index.html` file in your preferred web browser. (Double click on the file).
2. Login with credentials defined in `AUTH_USERS_JSON` in your selected env file.
3. Start adding Accounts like "Anandi Fashion". The API will automatically create the required Sheets and columns in your Google Sheet!

Enjoy!

## Multi-client config (separate Firebase + Sheets)
Use separate env files per client and generate `env.js` before deploy/run.

1. Fill values in:
   - `.env.client1`
   - `.env.client2`
2. Generate config for client 1:
   - `./scripts/apply-env-to-config.sh .env.client1`
3. Generate config for client 2:
   - `./scripts/apply-env-to-config.sh .env.client2`

Client mode options:
- `CLIENT_MODE=multi_company` for normal 2-company mode
- `CLIENT_MODE=single_company` for one-company client
- `CLIENT_COMPANY_NAME=...` to set single-company display name

This keeps the same code logic but isolates each client on their own Firebase project and Google Sheet.
