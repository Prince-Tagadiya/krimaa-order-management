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
1. Open up `app.js` in this project folder.
2. Find the very first line:
   ```javascript
   const API_URL = "YOUR_GOOGLE_APPS_SCRIPT_WEB_APP_URL";
   ```
3. Replace the text inside quotes with the **Web app URL** you copied in the previous step.
4. Save `app.js`.

## 4. Run the Application
1. Open the `index.html` file in your preferred web browser. (Double click on the file).
2. Login with the hardcoded credentials:
   - **Username**: `admin`
   - **Password**: `admin123`
3. Start adding Accounts like "Anandi Fashion". The API will automatically create the required Sheets and columns in your Google Sheet!

Enjoy!
