// KRIMAA ORDER MANAGEMENT v4.0 - MULTI-COMPANY + EDIT/DELETE ACCOUNTS
var SCRIPT_NAME = "Order Management API";

// Company configuration - define your companies here
var COMPANIES = {
  'company1': { name: 'Company 1', accountsSheet: 'Accounts_C1', ordersSheet: 'Orders_C1' },
  'company2': { name: 'Company 2', accountsSheet: 'Accounts_C2', ordersSheet: 'Orders_C2' }
};
var REMARKS_SHEET = 'Remarks';
var MONEY_BACKUP_SHEET = 'Money_Backups';
var KARIGAR_SHEET = 'Karigars'; // legacy fallback
var KARIGAR_TX_SHEET = 'Karigar_Transactions'; // legacy fallback
var KARIGAR_SHEETS = {
  company1: 'Karigars_C1',
  company2: 'Karigars_C2'
};
var KARIGAR_TX_SHEETS = {
  company1: 'Karigar_Transactions_C1',
  company2: 'Karigar_Transactions_C2'
};
var DESIGN_PRICES_SHEET = 'Design_Prices';
var _sheetsInitialised = {};

function normalizeCompanyId(companyId) {
  var raw = (companyId || 'company1').toString().trim().toLowerCase();
  var alias = {
    'company 1': 'company1',
    'comp 1': 'company1',
    'c1': 'company1',
    '1': 'company1',
    'company 2': 'company2',
    'comp 2': 'company2',
    'c2': 'company2',
    '2': 'company2'
  };
  var normalized = alias[raw] || raw;
  if (!COMPANIES[normalized]) return 'company1';
  return normalized;
}

function generatePrefixedId(prefix) {
  return prefix + Math.random().toString(36).substr(2, 9);
}

function isPrefixedId(value, prefix) {
  return String(value || '').trim().indexOf(prefix) === 0;
}

function parseFlexibleDateTime(value, fallbackDateStr) {
  if (value instanceof Date && !isNaN(value.getTime())) return value;

  if (typeof value === 'number' && !isNaN(value)) {
    if (value > 1000000000000) {
      var fromMillis = new Date(value);
      if (!isNaN(fromMillis.getTime())) return fromMillis;
    } else if (value > 1000000000) {
      var fromUnix = new Date(value * 1000);
      if (!isNaN(fromUnix.getTime())) return fromUnix;
    } else if (value > 10000 && value < 60000) {
      var fromSerial = new Date((value - 25569) * 86400000);
      if (!isNaN(fromSerial.getTime())) return fromSerial;
    }
  }

  if (value && typeof value === 'object') {
    if (typeof value.toDate === 'function') {
      try {
        var fromToDate = value.toDate();
        if (fromToDate instanceof Date && !isNaN(fromToDate.getTime())) return fromToDate;
      } catch (e) {}
    }
    if (typeof value.seconds === 'number') {
      var fromSeconds = new Date(value.seconds * 1000);
      if (!isNaN(fromSeconds.getTime())) return fromSeconds;
    }
    if (typeof value._seconds === 'number') {
      var fromUnderscoreSeconds = new Date(value._seconds * 1000);
      if (!isNaN(fromUnderscoreSeconds.getTime())) return fromUnderscoreSeconds;
    }
  }

  var str = String(value || '').trim();
  if (str) {
    if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$/.test(str)) {
      var fromSpaceTs = new Date(str.replace(' ', 'T'));
      if (!isNaN(fromSpaceTs.getTime())) return fromSpaceTs;
    }
    var parsed = new Date(str);
    if (!isNaN(parsed.getTime())) return parsed;
  }

  if (fallbackDateStr) {
    var fallback = new Date(String(fallbackDateStr).split('T')[0] + 'T00:00:00');
    if (!isNaN(fallback.getTime())) return fallback;
  }
  return new Date();
}

function getKarigarSheetName(companyId) {
  var cid = normalizeCompanyId(companyId);
  return KARIGAR_SHEETS[cid] || KARIGAR_SHEET;
}

function getKarigarTxSheetName(companyId) {
  var cid = normalizeCompanyId(companyId);
  return KARIGAR_TX_SHEETS[cid] || KARIGAR_TX_SHEET;
}

function getKarigarHeaderRow() {
  return ['Karigar ID', 'Name', 'Added At', 'Created By Name', 'Created By User', 'Created By Role', 'Source', 'Dashboard'];
}

function getKarigarTxHeaderRow() {
  return [
    'Date', 'Karigar ID', 'Karigar Name', 'Type', 'Design', 'Size', 'Pic', 'Price', 'Total Jama', 'Upad Amount', 'Created At',
    'Created By Name', 'Created By User', 'Created By Role',
    'Updated By Name', 'Updated By User', 'Updated By Role',
    'Source', 'Dashboard'
  ];
}

// Function to automatically create the required Google Sheets if missing
function setupSheets(companyId) {
  companyId = normalizeCompanyId(companyId);
  if (_sheetsInitialised[companyId]) return;
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var company = COMPANIES[companyId];
  if (!company) {
    for (var cid in COMPANIES) {
      setupSheetsForCompany(ss, COMPANIES[cid]);
      _sheetsInitialised[cid] = true;
    }
    setupRemarksSheet(ss);
    return;
  }
  setupSheetsForCompany(ss, company);
  setupRemarksSheet(ss);
  setupMoneyBackupSheet(ss);
  setupKarigarSheets(ss);
  _sheetsInitialised[companyId] = true;
}

function setupKarigarSheets(ss) {
  function ensureKarigarSheet(sheetName) {
    var sheet = ss.getSheetByName(sheetName);
    if (!sheet) sheet = ss.insertSheet(sheetName);
    var headers = getKarigarHeaderRow();
    if (sheet.getMaxColumns() < headers.length) {
      sheet.insertColumnsAfter(sheet.getMaxColumns(), headers.length - sheet.getMaxColumns());
    }
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(1, 1, 1, headers.length).setFontWeight('bold');
    sheet.setFrozenRows(1);
  }

  function ensureKarigarTxSheet(sheetName) {
    var txSheet = ss.getSheetByName(sheetName);
    if (!txSheet) txSheet = ss.insertSheet(sheetName);
    var headers = getKarigarTxHeaderRow();
    if (txSheet.getMaxColumns() < headers.length) {
      txSheet.insertColumnsAfter(txSheet.getMaxColumns(), headers.length - txSheet.getMaxColumns());
    }
    txSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    txSheet.getRange(1, 1, 1, headers.length).setFontWeight('bold');
    txSheet.setFrozenRows(1);
  }

  // New per-company sheets
  ensureKarigarSheet(KARIGAR_SHEETS.company1);
  ensureKarigarSheet(KARIGAR_SHEETS.company2);
  ensureKarigarTxSheet(KARIGAR_TX_SHEETS.company1);
  ensureKarigarTxSheet(KARIGAR_TX_SHEETS.company2);

  // Keep legacy sheets available for old backups/migrations
  ensureKarigarSheet(KARIGAR_SHEET);
  ensureKarigarTxSheet(KARIGAR_TX_SHEET);

  var dpSheet = ss.getSheetByName(DESIGN_PRICES_SHEET);
  if (!dpSheet) {
    dpSheet = ss.insertSheet(DESIGN_PRICES_SHEET);
    dpSheet.appendRow(['Design Name', 'Latest Price', 'Updated At']);
    dpSheet.getRange('A1:C1').setFontWeight('bold');
    dpSheet.setFrozenRows(1);
  }
}

function setupRemarksSheet(ss) {
  var sheet = ss.getSheetByName(REMARKS_SHEET);
  if (!sheet) {
    sheet = ss.insertSheet(REMARKS_SHEET);
    sheet.appendRow(['Date', 'Remark', 'Last Updated']);
    sheet.getRange('A1:C1').setFontWeight('bold');
    sheet.setFrozenRows(1);
  }
}

function setupMoneyBackupSheet(ss) {
  var sheet = ss.getSheetByName(MONEY_BACKUP_SHEET);
  if (!sheet) {
    sheet = ss.insertSheet(MONEY_BACKUP_SHEET);
    sheet.appendRow(['Backup Date', 'Company Id', 'Account ID', 'Account Name', 'Money', 'Expense', 'Balance', 'Reason', 'Saved At']);
    sheet.getRange('A1:I1').setFontWeight('bold');
    sheet.setFrozenRows(1);
  }
}

function setupSheetsForCompany(ss, company) {
  var accountsSheet = ss.getSheetByName(company.accountsSheet);
  if (!accountsSheet) accountsSheet = ss.insertSheet(company.accountsSheet);
  // Unconditionally ensure valid headers exist
  accountsSheet.getRange(1, 1, 1, 4).setValues([['Account ID', 'Account Name', 'Added Date', 'Position']]).setFontWeight("bold");
  accountsSheet.setFrozenRows(1);
  
  var ordersSheet = ss.getSheetByName(company.ordersSheet);
  if (!ordersSheet) ordersSheet = ss.insertSheet(company.ordersSheet);
  // Unconditionally ensure valid headers exist
  ordersSheet.getRange(1, 1, 1, 5).setValues([['Date', 'Account ID', 'Account Name', 'Meesho', 'Synced At']]).setFontWeight("bold");
  ordersSheet.setFrozenRows(1);
}

// Migrate existing data from old sheets (Accounts/Orders) to Company 1 sheets
// Run this function ONCE manually to migrate your existing data
function migrateExistingData() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // Setup new sheets first
  setupSheets();
  
  var c1 = COMPANIES['company1'];
  
  // Migrate Accounts
  var oldAccounts = ss.getSheetByName('Accounts');
  if (oldAccounts) {
    var newAccounts = ss.getSheetByName(c1.accountsSheet);
    var data = oldAccounts.getDataRange().getValues();
    if (data.length > 1) {
      // Skip header, copy data
      var dataOnly = data.slice(1);
      if (dataOnly.length > 0) {
        newAccounts.getRange(2, 1, dataOnly.length, dataOnly[0].length).setValues(dataOnly);
      }
    }
    Logger.log('Accounts migrated to ' + c1.accountsSheet);
  }
  
  // Migrate Orders
  var oldOrders = ss.getSheetByName('Orders');
  if (oldOrders) {
    var newOrders = ss.getSheetByName(c1.ordersSheet);
    var data = oldOrders.getDataRange().getValues();
    if (data.length > 1) {
      var dataOnly = data.slice(1);
      if (dataOnly.length > 0) {
        newOrders.getRange(2, 1, dataOnly.length, dataOnly[0].length).setValues(dataOnly);
      }
    }
    Logger.log('Orders migrated to ' + c1.ordersSheet);
  }
  
  Logger.log('Migration complete! You can now rename/delete the old Accounts and Orders sheets.');
}

// Handle all POST requests
function doPost(e) {
  var output = ContentService.createTextOutput();
  output.setMimeType(ContentService.MimeType.JSON);
  
  try {
    var data;
    if (e.postData) {
      if (typeof e.postData.contents === 'string') {
        data = JSON.parse(e.postData.contents);
      } else {
        data = e.postData.contents;
      }
    }
    
    var action = data ? data.action : null;
    var companyId = normalizeCompanyId(data ? data.companyId : 'company1');
    setupSheets(companyId);
    
    if (action === 'addAccount') {
      return output.setContent(JSON.stringify(addAccount(data.accountName, companyId)));
    } else if (action === 'editAccount') {
      return output.setContent(JSON.stringify(editAccount(data.accountId, data.newName, companyId)));
    } else if (action === 'deleteAccount') {
      return output.setContent(JSON.stringify(deleteAccount(data.accountId, companyId)));
    } else if (action === 'submitOrders') {
      return output.setContent(JSON.stringify(submitOrders(data.date, data.orders, companyId)));
    } else if (action === 'getAccounts') {
      return output.setContent(JSON.stringify(getAccounts(companyId)));
    } else if (action === 'getDashboardData') {
      return output.setContent(JSON.stringify(getDashboardData(companyId, data.month)));
    } else if (action === 'getAvailableOrderMonths') {
      return output.setContent(JSON.stringify(getAvailableOrderMonths(companyId)));
    } else if (action === 'updateAccountOrder') {
      return output.setContent(JSON.stringify(updateAccountOrder(data.orderedAccounts, companyId)));
    } else if (action === 'getCompanies') {
      return output.setContent(JSON.stringify(getCompaniesInfo()));
    } else if (action === 'saveRemark') {
      return output.setContent(JSON.stringify(saveRemark(data.date, data.remark)));
    } else if (action === 'getRemarks') {
      return output.setContent(JSON.stringify(getRemarks()));
    } else if (action === 'updateOrder') {
      return output.setContent(JSON.stringify(updateSingleOrder(data.date, data.accountId, data.field, data.value, companyId)));
    } else if (action === 'bulkBackup') {
      return output.setContent(JSON.stringify(bulkBackup(data.orders, data.accounts, companyId, data.append, data.clearMonth)));
    } else if (action === 'saveMoneyBackup') {
      return output.setContent(JSON.stringify(saveMoneyBackup(data.date, data.rows, data.reason)));
    } else if (action === 'saveFullBackup') {
      return output.setContent(JSON.stringify(saveFullBackup(data.data)));
    } else if (action === 'getGlobalReport') {
      return output.setContent(JSON.stringify(getGlobalReport(data.fromDate, data.toDate)));
    } else if (action === 'archiveMonthlyData') {
      return output.setContent(JSON.stringify(archiveMonthlyData(data.sheetName, data.data)));
    } else if (action === 'getMoneyBackups') {
      return output.setContent(JSON.stringify(getMoneyBackups()));
    } else if (action === 'backfillIds') {
      return output.setContent(JSON.stringify(backfillMissingIds()));
    } else if (action === 'repairFromFirebase') {
      var fbData = JSON.parse(data.firebaseData || '{}');
      return output.setContent(JSON.stringify(repairFromFirebase(fbData)));
    } else if (action === 'getAllSheetData') {
      return output.setContent(JSON.stringify(getAllSheetData()));
    }
    
    return output.setContent(JSON.stringify({success: false, message: 'Invalid action: ' + action}));
  } catch(error) {
    return output.setContent(JSON.stringify({success: false, message: error.toString()}));
  }
}

function saveMoneyBackup(backupDate, rows, reason) {
  setupSheets('company1');
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(MONEY_BACKUP_SHEET);
  if (!sheet) {
    setupMoneyBackupSheet(ss);
    sheet = ss.getSheetByName(MONEY_BACKUP_SHEET);
  }

  if (!rows || !rows.length) {
    return { success: false, message: 'No backup rows provided' };
  }

  var nowTs = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  var safeDate = backupDate || Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
  var safeReason = reason || 'manual';

  var values = [];
  for (var i = 0; i < rows.length; i++) {
    var row = rows[i] || {};
    var money = parseInt(row.money, 10) || 0;
    var expense = parseInt(row.expense, 10) || 0;
    values.push([
      safeDate,
      row.companyId || '',
      row.accountId || '',
      row.accountName || '',
      money,
      expense,
      money - expense,
      safeReason,
      nowTs
    ]);
  }

  if (values.length > 0) {
    sheet.insertRowsAfter(1, values.length);
    sheet.getRange(2, 1, values.length, values[0].length).setValues(values);
  }
  return { success: true, message: 'Money backup saved to sheet', rows: values.length, sheet: MONEY_BACKUP_SHEET };
}

function getMoneyBackups() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(MONEY_BACKUP_SHEET);
  if (!sheet) return { success: true, data: [] };
  
  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) return { success: true, data: [] };
  
  var values = sheet.getRange(2, 1, lastRow - 1, 9).getValues();
  var backupsMap = {};
  
  for (var i = 0; i < values.length; i++) {
    var r = values[i];
    var bDate = r[0] ? String(r[0]) : '';
    if (!bDate) continue; // Skip empty rows
    
    // We group by backup date + reason
    var reason = r[7] || '';
    var key = bDate + '_' + reason;
    
    if (!backupsMap[key]) {
      backupsMap[key] = {
        id: 'sheet_' + key,
        backupDate: bDate,
        reason: reason,
        createdAt: r[8] || '', // the 'Saved At' string
        rows: [],
        totals: { money: 0, expense: 0, balance: 0 }
      };
    }
    
    var money = parseInt(r[4], 10) || 0;
    var exp = parseInt(r[5], 10) || 0;
    backupsMap[key].rows.push({
      companyId: r[1],
      accountId: r[2],
      accountName: r[3],
      money: money,
      expense: exp,
      balance: money - exp
    });
    
    backupsMap[key].totals.money += money;
    backupsMap[key].totals.expense += exp;
    backupsMap[key].totals.balance += (money - exp);
  }
  
  var resultKeys = Object.keys(backupsMap).sort(function(a, b) {
    return a > b ? -1 : 1; // Descending by key string
  });
  var result = resultKeys.map(function(k) { return backupsMap[k]; });
  
  return { success: true, data: result };
}

// Handle all GET requests
function doGet(e) {
  var output = ContentService.createTextOutput();
  output.setMimeType(ContentService.MimeType.JSON);
  
  try {
    var action = e.parameter.action;
    var companyId = normalizeCompanyId(e.parameter.companyId || 'company1');
    
    setupSheets(companyId);
    
    if (action === 'getAccounts') {
      return output.setContent(JSON.stringify(getAccounts(companyId)));
    } else if (action === 'getDashboardData') {
      return output.setContent(JSON.stringify(getDashboardData(companyId, e.parameter.month)));
    } else if (action === 'getAvailableOrderMonths') {
      return output.setContent(JSON.stringify(getAvailableOrderMonths(companyId)));
    } else if (action === 'getCompanies') {
      return output.setContent(JSON.stringify(getCompaniesInfo()));
    } else if (action === 'getRemarks') {
      return output.setContent(JSON.stringify(getRemarks()));
    } else if (action === 'getAvailableOrderMonths') {
      return output.setContent(JSON.stringify(getAvailableOrderMonths(companyId)));
    }
    
    return output.setContent(JSON.stringify({success: false, message: 'Invalid GET action'}));
  } catch(error) {
    return output.setContent(JSON.stringify({success: false, message: error.toString()}));
  }
}

// Return list of companies
function getCompaniesInfo() {
  var result = [];
  for (var cid in COMPANIES) {
    result.push({ id: cid, name: COMPANIES[cid].name });
  }
  return { success: true, data: result };
}

// Logic to add a new account to the sheet
function addAccount(accountName, companyId, accountId) {
  companyId = normalizeCompanyId(companyId);
  if (!accountName || accountName.trim() === '') {
    return {success: false, message: 'Account name cannot be empty'};
  }
  
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.accountsSheet);
  
  var data = sheet.getDataRange().getValues();
  if (data.length > 0) {
    var startIdx = (data[0][0] === 'Account ID' || data[0][1] === 'Account Name') ? 1 : 0;
    for (var i = startIdx; i < data.length; i++) {
        var existingName = (data[i][1] || '').toString().trim().toLowerCase();
        if (existingName === accountName.trim().toLowerCase()) {
            return {success: false, message: 'Account already exists'};
        }
    }
  }
  
  var id = isPrefixedId(accountId, 'acc_') ? String(accountId).trim() : generatePrefixedId('acc_');
  var date = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  sheet.appendRow([id, accountName.trim(), date]);
  return {success: true, message: 'Account added successfully', accountId: id};
}

// Logic to edit an existing account name
function editAccount(accountId, newName, companyId) {
  companyId = normalizeCompanyId(companyId);
  if (!accountId || !newName || newName.trim() === '') {
    return {success: false, message: 'ID and new name required'};
  }
  
  newName = newName.trim();
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  
  // Update in Accounts sheet
  var accountsSheet = ss.getSheetByName(company.accountsSheet);
  var accData = accountsSheet.getDataRange().getValues();
  var startIdx = (accData[0][0] === 'Account ID') ? 1 : 0;
  
  var accountFound = false;
  for (var i = startIdx; i < accData.length; i++) {
    if (accData[i][0].toString().trim() === accountId) {
      accountsSheet.getRange(i + 1, 2).setValue(newName); // Column B: Name
      accountFound = true;
      break;
    }
  }
  
  if (!accountFound) {
    // Attempt fallback by name? No, primary key is ID now.
    return {success: false, message: 'Account not found by ID'};
  }
  
  // Also update account name in Orders sheet for this ID
  var ordersSheet = ss.getSheetByName(company.ordersSheet);
  if (ordersSheet) {
    var ordData = ordersSheet.getDataRange().getValues();
    var ordStartIdx = (ordData.length > 0 && ordData[0][0] === 'Date') ? 1 : 0;
    
    for (var j = ordStartIdx; j < ordData.length; j++) {
      if (ordData[j][1] && ordData[j][1].toString().trim() === accountId) {
        ordersSheet.getRange(j + 1, 3).setValue(newName); // Column C: Name
      }
    }
  }
  
  return {success: true, message: 'Account updated successfully'};
}

// Logic to delete an account
function deleteAccount(accountId, companyId) {
  companyId = normalizeCompanyId(companyId);
  if (!accountId) return {success: false, message: 'Account ID required'};
  
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  
  var accountsSheet = ss.getSheetByName(company.accountsSheet);
  var accData = accountsSheet.getDataRange().getValues();
  var startIdx = (accData[0][0] === 'Account ID') ? 1 : 0;
  
  var rowToDelete = -1;
  for (var i = startIdx; i < accData.length; i++) {
    if (accData[i][0].toString().trim() === accountId) {
      rowToDelete = i + 1;
      break;
    }
  }
  
  if (rowToDelete === -1) return {success: false, message: 'Account not found'};
  accountsSheet.deleteRow(rowToDelete);
  return {success: true, message: 'Account deleted'};
}

// Logic to get all accounts from the sheet (sorted by Position column)
function getAccounts(companyId) {
  companyId = normalizeCompanyId(companyId);
  setupSheets(companyId);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.accountsSheet);
  if (!sheet) return {success: true, data: [], details: []};
  
  var data = sheet.getDataRange().getValues();
  var accounts = [];
  if (data.length > 0) {
    var startIdx = (data[0][0] === 'Account ID') ? 1 : 0;
    for (var i = startIdx; i < data.length; i++) {
      if (data[i][0]) {
        var pos = data[i][3]; // Position column (D)
        accounts.push({ 
          accountId: data[i][0].toString(), 
          name: data[i][1].toString(), 
          addedDate: data[i][2],
          position: (pos !== '' && pos !== undefined) ? parseInt(pos) : 9999 
        });
      }
    }
  }
  
  accounts.sort(function(a, b) { return a.position - b.position; });
  return {success: true, data: accounts.map(function(a){ return a.name; }), details: accounts};
}

// Logic to update account order/position in the sheet
function updateAccountOrder(orderedAccountIds, companyId) {
  companyId = normalizeCompanyId(companyId);
  if (!orderedAccountIds || orderedAccountIds.length === 0) return {success: false, message: 'No IDs provided'};
  
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.accountsSheet);
  var data = sheet.getDataRange().getValues();
  var startIdx = (data[0][0] === 'Account ID') ? 1 : 0;
  
  for (var i = startIdx; i < data.length; i++) {
    var id = data[i][0].toString().trim();
    var posIndex = orderedAccountIds.indexOf(id);
    if (posIndex !== -1) {
      sheet.getRange(i + 1, 4).setValue(posIndex); // Position in Column D
    }
  }
  return {success: true};
}

// Logic to submit daily orders
function submitOrders(dateStr, orders, companyId) {
  companyId = normalizeCompanyId(companyId);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.ordersSheet);
  
  if (!orders || orders.length === 0) return {success: false, message: 'No orders'};
  
  var now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  var newRows = orders.map(function(o) {
    return [dateStr, o.accountId, o.accountName, o.meesho || 0, now];
  });
  
  var lastRow = sheet.getLastRow();
  sheet.getRange(lastRow + 1, 1, newRows.length, 5).setValues(newRows);
  return {success: true};
}

// Logic to load order data for the dashboard, optionally filtered by month (YYYY-MM)
function getDashboardData(companyId, monthFilter) {
  companyId = normalizeCompanyId(companyId);
  setupSheets(companyId);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.ordersSheet);
  if (!sheet) return {success: true, data: []};
  
  var lastRow = sheet.getLastRow();
  var startRow = (lastRow >= 1 && sheet.getRange(1, 1).getValue().toString().indexOf('Date') !== -1) ? 2 : 1;
  if (lastRow < startRow) return {success: true, data: []};
  
  var data = sheet.getRange(startRow, 1, lastRow - (startRow - 1), 5).getValues();
  var records = [];
  for (var i = 0; i < data.length; i++) {
    var d = data[i][0] instanceof Date ? Utilities.formatDate(data[i][0], Session.getScriptTimeZone(), "yyyy-MM-dd") : data[i][0];
    if (monthFilter && d.substring(0, 7) !== monthFilter) continue;
    var meeshoVal = parseInt(data[i][3]) || 0;
    records.push({ date: d, accountId: data[i][1], accountName: data[i][2], meesho: meeshoVal, total: meeshoVal });
  }
  return {success: true, data: records};
}

function getAvailableOrderMonths(companyId) {
  companyId = normalizeCompanyId(companyId);
  setupSheets(companyId);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.ordersSheet);
  if (!sheet) return {success: true, data: []};
  
  var lastRow = sheet.getLastRow();
  if (lastRow < 2) return {success: true, data: []};
  
  var data = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
  var months = new Set();
  for (var i = 0; i < data.length; i++) {
    var rawDate = data[i][0];
    if (rawDate instanceof Date) {
      months.add(Utilities.formatDate(rawDate, Session.getScriptTimeZone(), "yyyy-MM"));
    } else if (typeof rawDate === 'string' && rawDate.length >= 7) {
      months.add(rawDate.substring(0, 7));
    }
  }
  
  var sorted = Array.from(months).sort().reverse();
  return {success: true, data: sorted};
}

function saveRemark(dateStr, remark) {
  if (!dateStr) return {success: false, message: 'Date required'};
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupRemarksSheet(ss);
  var sheet = ss.getSheetByName(REMARKS_SHEET);
  var data = sheet.getDataRange().getValues();
  var startIdx = (data.length > 0 && (data[0][0] === 'Date' || data[0][0] === 'Account ID')) ? 1 : 0;
  
  // Find existing row for this date
  for (var i = startIdx; i < data.length; i++) {
    var rowDate = data[i][0];
    if (rowDate instanceof Date) {
      rowDate = Utilities.formatDate(rowDate, Session.getScriptTimeZone(), 'yyyy-MM-dd');
    }
    if (rowDate.toString().trim() === dateStr.trim()) {
      sheet.getRange(i + 1, 2).setValue(remark || '');
      sheet.getRange(i + 1, 3).setValue(new Date());
      return {success: true, message: 'Remark updated'};
    }
  }
  
  // New row
  sheet.appendRow([dateStr, remark || '', new Date()]);
  return {success: true, message: 'Remark saved'};
}

function getRemarks() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupRemarksSheet(ss);
  var sheet = ss.getSheetByName(REMARKS_SHEET);
  if (!sheet) return {success: true, data: {}};
  var data = sheet.getDataRange().getValues();
  var startIdx = (data.length > 0 && data[0][0] === 'Date') ? 1 : 0;
  var remarks = {};
  for (var i = startIdx; i < data.length; i++) {
    var rawDate = data[i][0];
    if (rawDate instanceof Date) {
      rawDate = Utilities.formatDate(rawDate, Session.getScriptTimeZone(), 'yyyy-MM-dd');
    }
    remarks[rawDate.toString().trim()] = (data[i][1] || '').toString();
  }
  return {success: true, data: remarks};
}

// ===== UPDATE SINGLE ORDER CELL =====
function updateSingleOrder(dateStr, accountId, field, value, companyId) {
  companyId = normalizeCompanyId(companyId);
  if (!dateStr || !accountId || !field) return {success: false, message: 'Missing parameters'};
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.ordersSheet);
  var data = sheet.getDataRange().getValues();
  var startIdx = (data.length > 0 && data[0][0].indexOf('Date') !== -1) ? 1 : 0;
  
  var colIndex = (field === 'meesho') ? 4 : -1;
  if (colIndex === -1) return {success: false, message: 'Invalid field'};
  
  var numVal = parseInt(value) || 0;
  for (var i = startIdx; i < data.length; i++) {
    var d = data[i][0] instanceof Date ? Utilities.formatDate(data[i][0], Session.getScriptTimeZone(), "yyyy-MM-dd") : data[i][0];
    if (d.toString().trim() === dateStr.trim() && data[i][1].toString().trim() === accountId.trim()) {
      sheet.getRange(i + 1, colIndex).setValue(numVal);
      return {success: true, message: 'Updated'};
    }
  }
  return {success: false, message: 'Order row not found'};
}

function archiveMonthlyData(sheetName, rows) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(sheetName);

  
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
    sheet.appendRow(["OrderID", "CompanyID", "CompanyName", "Quantity", "Date"]);
    sheet.getRange(1, 1, 1, 5).setFontWeight("bold").setBackground("#f3f4f6");
    sheet.setFrozenRows(1);
    Logger.log("Created missing archive sheet: " + sheetName);
  }

  if (rows && rows.length > 0) {
    var existDates = sheet.getLastRow() > 1 ? sheet.getRange(2, 1, sheet.getLastRow()-1, 1).getValues().map(function(r){ return String(r[0]); }) : [];
    var newRows = [];
    
    rows.forEach(function(row) {
      // row structure: [OrderID, CompanyID, CompanyName, Quantity, Date]
      if (existDates.indexOf(String(row[0])) === -1) {
        newRows.push(row);
      }
    });

    if (newRows.length > 0) {
      sheet.insertRowsAfter(1, newRows.length);
      sheet.getRange(2, 1, newRows.length, newRows[0].length).setValues(newRows);
      return { success: true, rowsWritten: newRows.length, message: "Archived directly successfully." };
    }
  }
  return { success: true, rowsWritten: 0, message: "No new rows to archive." };
}

// ===== BULK BACKUP (receives Firebase data and writes to Sheets) =====
function saveFullBackup(data) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets('all');
  
  var totalRows = 0;
  var companyIds = ['company1', 'company2'];
  var karigarCols = getKarigarHeaderRow().length;
  var karigarTxCols = getKarigarTxHeaderRow().length;
  
  // 1. Sync Remarks
  if (data.remarks) {
    var sheet = ss.getSheetByName(REMARKS_SHEET);
    sheet.clear();
    sheet.appendRow(['Date', 'Remark', 'Last Updated']);
    sheet.getRange('A1:C1').setFontWeight('bold');
    var rows = [];
    for (var date in data.remarks) {
      if (date === 'last_update') continue;
      rows.push([date, data.remarks[date], new Date()]);
    }
    if (rows.length > 0) {
      sheet.getRange(2, 1, rows.length, 3).setValues(rows);
      totalRows += rows.length;
    }
  }

  var karigarNameToIdByCompany = { company1: {}, company2: {} };
  var legacyKarigarIdToPrefixedIdByCompany = { company1: {}, company2: {} };

  // 2. Sync Karigars (overwrite style by company while preserving proper kar_ IDs)
  if (data.karigars) {
    var karigarRowsByCompany = { company1: [], company2: [] };
    companyIds.forEach(function(cid) {
      var sheet = ss.getSheetByName(getKarigarSheetName(cid));
      if (sheet && sheet.getLastRow() > 1) {
        sheet.getRange(2, 1, sheet.getLastRow() - 1, karigarCols).clearContent();
      }
    });

    data.karigars.forEach(function(k) {
      var cid = normalizeCompanyId(k.companyId || 'company1');
      var kName = String(k.name || '').trim();
      var rawId = String(k.id || '').trim();
      var kId = isPrefixedId(rawId, 'kar_') ? rawId : generatePrefixedId('kar_');
      var addedAt = parseFlexibleDateTime(k.addedDate || k.addedAt || k.createdAt, Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd'));

      if (kName) karigarNameToIdByCompany[cid][kName.toLowerCase()] = kId;
      if (rawId && !isPrefixedId(rawId, 'kar_')) legacyKarigarIdToPrefixedIdByCompany[cid][rawId] = kId;
      karigarRowsByCompany[cid].push([
        kId, kName, addedAt,
        String(k.createdByName || '').trim(),
        String(k.createdByUser || '').trim(),
        String(k.createdByRole || '').trim(),
        String(k.source || '').trim(),
        String(k.dashboard || '').trim()
      ]);
    });

    companyIds.forEach(function(cid) {
      var rows = karigarRowsByCompany[cid];
      if (!rows || rows.length === 0) return;
      var sheet = ss.getSheetByName(getKarigarSheetName(cid));
      sheet.getRange(2, 1, rows.length, karigarCols).setValues(rows);
      totalRows += rows.length;
    });
  }

  // 3. Sync Karigar Transactions (UPSERT logic + company-wise ID normalization)
  if (data.karigarTransactions) {
    var existMapByCompany = { company1: {}, company2: {} };
    var newRowsByCompany = { company1: [], company2: [] };

    companyIds.forEach(function(cid) {
      var sheet = ss.getSheetByName(getKarigarTxSheetName(cid));
      var existData = (sheet && sheet.getLastRow() > 1) ? sheet.getRange(2, 1, sheet.getLastRow() - 1, karigarTxCols).getValues() : [];
      existData.forEach(function(r) {
        var dStr = r[0] instanceof Date ? Utilities.formatDate(r[0], Session.getScriptTimeZone(), 'yyyy-MM-dd') : String(r[0]).split(' ')[0];
        var key = dStr + "_" + r[1] + "_" + r[3] + "_" + r[4] + "_" + r[6];
        existMapByCompany[cid][key] = true;
      });
    });

    data.karigarTransactions.forEach(function(tx) {
      var txCompanyId = normalizeCompanyId(tx.companyId || 'company1');
      var txName = String(tx.karigarName || '').trim();
      var txNameKey = txName.toLowerCase();
      var rawTxId = String(tx.karigarId || '').trim();
      var normalizedTxId = rawTxId;

      if (!isPrefixedId(normalizedTxId, 'kar_')) {
        if (txNameKey && karigarNameToIdByCompany[txCompanyId][txNameKey]) {
          normalizedTxId = karigarNameToIdByCompany[txCompanyId][txNameKey];
        } else if (rawTxId && legacyKarigarIdToPrefixedIdByCompany[txCompanyId][rawTxId]) {
          normalizedTxId = legacyKarigarIdToPrefixedIdByCompany[txCompanyId][rawTxId];
        } else {
          normalizedTxId = generatePrefixedId('kar_');
          if (txNameKey) karigarNameToIdByCompany[txCompanyId][txNameKey] = normalizedTxId;
          if (rawTxId) legacyKarigarIdToPrefixedIdByCompany[txCompanyId][rawTxId] = normalizedTxId;
        }
      }

      if (txNameKey && !karigarNameToIdByCompany[txCompanyId][txNameKey]) {
        karigarNameToIdByCompany[txCompanyId][txNameKey] = normalizedTxId;
      }

      var parsedTxDate = parseFlexibleDateTime(tx.date, Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd'));
      var dStr = Utilities.formatDate(parsedTxDate, Session.getScriptTimeZone(), 'yyyy-MM-dd');
      var createdAtDate = parseFlexibleDateTime(tx.createdAt || tx.addedAt || tx.updatedAt, dStr);
      var dateWithTime = Utilities.formatDate(createdAtDate, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');

      var key = dStr + "_" + normalizedTxId + "_" + tx.type + "_" + (tx.designName || '') + "_" + (tx.pic || '0');
      if (!existMapByCompany[txCompanyId][key]) {
        newRowsByCompany[txCompanyId].push([
          dateWithTime,
          normalizedTxId,
          txName,
          tx.type, 
          tx.designName || '', 
          tx.size || '', 
          tx.pic || 0, 
          tx.price || 0, 
          tx.total || 0, 
          tx.amount || tx.upadAmount || 0,
          createdAtDate,
          String(tx.createdByName || '').trim(),
          String(tx.createdByUser || '').trim(),
          String(tx.createdByRole || tx.addedBy || '').trim(),
          String(tx.updatedByName || '').trim(),
          String(tx.updatedByUser || '').trim(),
          String(tx.updatedByRole || '').trim(),
          String(tx.source || '').trim(),
          String(tx.dashboard || '').trim()
        ]);
      }
    });

    companyIds.forEach(function(cid) {
      var rows = newRowsByCompany[cid];
      if (!rows || rows.length === 0) return;
      var sheet = ss.getSheetByName(getKarigarTxSheetName(cid));
      sheet.insertRowsAfter(1, rows.length);
      sheet.getRange(2, 1, rows.length, karigarTxCols).setValues(rows);
      totalRows += rows.length;
    });
  }

  // 4. Sync Design Prices
  if (data.designPrices) {
    var sheet = ss.getSheetByName(DESIGN_PRICES_SHEET);
    sheet.clear();
    sheet.appendRow(['Design Name', 'Latest Price', 'Updated At']);
    sheet.getRange('A1:C1').setFontWeight('bold');
    var rows = [];
    for (var design in data.designPrices) {
      rows.push([design, data.designPrices[design], new Date()]);
    }
    if (rows.length > 0) {
      sheet.getRange(2, 1, rows.length, 3).setValues(rows);
      totalRows += rows.length;
    }
  }

  if (data.moneyBackups) {
    var sheet = ss.getSheetByName(MONEY_BACKUP_SHEET);
    var existDates = sheet.getLastRow() > 1 ? sheet.getRange(2, 1, sheet.getLastRow()-1, 1).getValues().map(function(r){ return String(r[0]).split(' ')[0]; }) : [];
    var newRows = [];
    data.moneyBackups.forEach(function(b) {
      if (existDates.indexOf(String(b.backupDate).split(' ')[0]) === -1) {
        (b.rows || []).forEach(function(r) {
           newRows.push([b.backupDate, r.companyId, r.companyName, r.accountName, r.money, r.expense, r.balance, b.reason || '', b.timestamp || new Date()]);
        });
      }
    });
    if (newRows.length > 0) {
      sheet.insertRowsAfter(1, newRows.length);
      sheet.getRange(2, 1, newRows.length, 9).setValues(newRows);
      totalRows += newRows.length;
    }
  }

  // 6. Sync Company 1 & 2 Orders and Accounts
  totalRows += syncCompanyData(ss, 'company1', data.company1);
  totalRows += syncCompanyData(ss, 'company2', data.company2);

  return { success: true, message: "Full database backup to Sheets successful.", totalRows: totalRows };
}

function syncCompanyData(ss, companyId, compData) {
  var count = 0;
  if (!compData) return count;
  var company = COMPANIES[companyId];
  if (!company) return count;

  // Sync Accounts — PRESERVE existing acc_ IDs from Sheets
  if (compData.accounts && compData.accounts.length > 0) {
    var sheet = ss.getSheetByName(company.accountsSheet);
    
    // Build a map of existing acc_ IDs by account name BEFORE clearing
    var existingAccIds = {}; // name_lower -> acc_xxxxx
    if (sheet.getLastRow() > 1) {
      var existData = sheet.getDataRange().getValues();
      for (var i = 1; i < existData.length; i++) {
        var existId = String(existData[i][0] || '').trim();
        var existName = String(existData[i][1] || '').trim();
        if (existName && existId.indexOf('acc_') === 0) {
          existingAccIds[existName.toLowerCase()] = existId;
        }
      }
    }
    
    sheet.getRange(2, 1, Math.max(1, sheet.getLastRow()), 4).clearContent();
    var rows = compData.accounts.map(function(acc, idx) {
      // Convert Firebase Timestamp objects to readable strings
      var addedDate = acc.addedDate;
      if (addedDate && typeof addedDate === 'object' && addedDate.seconds) {
        addedDate = Utilities.formatDate(new Date(addedDate.seconds * 1000), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
      } else if (!addedDate || String(addedDate).indexOf('seconds') !== -1) {
        addedDate = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
      }
      
      // Use existing acc_ ID if available, otherwise generate a new one
      var accName = acc.name || '';
      var accId = existingAccIds[accName.toLowerCase()];
      if (!accId) {
        accId = generatePrefixedId('acc_');
      }
      
      return [accId, accName, addedDate, acc.position !== undefined ? acc.position : idx];
    });
    sheet.getRange(2, 1, rows.length, 4).setValues(rows);
    count += rows.length;
  }

  // Sync Orders is now handled by the new partition archive architecture
  return count;
}

function syncOrdersToSheet(sheet, orders) {
  var lastRow = sheet.getLastRow();
  var existData = lastRow > 1 ? sheet.getRange(2, 1, lastRow-1, 5).getValues() : [];
  var existMap = {};
  existData.forEach(function(r, idx) {
    var dStr = r[0] instanceof Date ? Utilities.formatDate(r[0], Session.getScriptTimeZone(), 'yyyy-MM-dd') : String(r[0]).split(' ')[0];
    var key = dStr + "_" + r[1]; // Date_ID
    existMap[key] = { row: idx + 2, data: r };
  });

  var now = new Date();
  var timeStr = Utilities.formatDate(now, Session.getScriptTimeZone(), "HH:mm:ss");
  var newRows = [];
  var updatedRowsCount = 0;
  
  orders.forEach(function(o) {
    var key = o.date + "_" + o.accountId;
    if (existMap[key]) {
      var exist = existMap[key].data;
      if (parseInt(exist[3]) !== parseInt(o.meesho)) {
        sheet.getRange(existMap[key].row, 4, 1, 2).setValues([[o.meesho, now]]);
        updatedRowsCount++;
      }
    } else {
      newRows.push([o.date + ' ' + timeStr, o.accountId, o.accountName, o.meesho, now]);
    }
  });

  if (newRows.length > 0) {
    sheet.insertRowsAfter(1, newRows.length);
    sheet.getRange(2, 1, newRows.length, 5).setValues(newRows);
  }
  
  return newRows.length + updatedRowsCount;
}

// Keep for compatibility
function bulkBackup(orders, accounts, companyId) {
  var data = { 
    company1: companyId === 'company1' ? { orders: orders, accounts: accounts } : { orders: [], accounts: [] },
    company2: companyId === 'company2' ? { orders: orders, accounts: accounts } : { orders: [], accounts: [] }
  };
  return saveFullBackup(data);
}

function getGlobalReport(fromDate, toDate) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets('all');
  
  var result = {
    success: true,
    orders: [],
    karigarTransactions: []
  };

  // 1. Fetch Orders from C1 and C2
  ['company1', 'company2'].forEach(function(cid) {
    var cSheet = ss.getSheetByName(COMPANIES[cid].ordersSheet);
    if(cSheet && cSheet.getLastRow() > 1) {
      var data = cSheet.getRange(2, 1, cSheet.getLastRow()-1, 5).getValues();
      data.forEach(function(r) {
        var dStr = r[0] instanceof Date ? Utilities.formatDate(r[0], Session.getScriptTimeZone(), 'yyyy-MM-dd') : String(r[0]).split(' ')[0];
        if (dStr >= fromDate && dStr <= toDate) {
          var mVal = parseInt(r[3]) || 0;
          result.orders.push({ date: dStr, companyId: cid, accountId: r[1], accountName: r[2], meesho: mVal, total: mVal });
        }
      });
    }
  });

  // 2. Fetch Karigar Transactions from C1 and C2 sheets
  ['company1', 'company2'].forEach(function(cid) {
    var kxSheet = ss.getSheetByName(getKarigarTxSheetName(cid));
    if (!kxSheet || kxSheet.getLastRow() <= 1) return;
    var data = kxSheet.getRange(2, 1, kxSheet.getLastRow() - 1, 11).getValues();
    data.forEach(function(r) {
      var dStr = r[0] instanceof Date ? Utilities.formatDate(r[0], Session.getScriptTimeZone(), 'yyyy-MM-dd') : String(r[0]).split(' ')[0];
      if (dStr >= fromDate && dStr <= toDate) {
        result.karigarTransactions.push({
          id: 'gs_' + Math.random().toString(36).substr(2, 9),
          karigarId: r[1],
          karigarName: r[2],
          type: r[3],
          amount: parseFloat(r[8]) > 0 ? r[8] : r[9],
          total: r[8],
          date: dStr,
          designName: r[4],
          companyId: cid,
          addedBy: 'Archive'
        });
      }
    });
  });

  // Sort by date
  result.orders.sort(function(a, b) { return a.date > b.date ? 1 : -1; });
  result.karigarTransactions.sort(function(a, b) { return a.date > b.date ? 1 : -1; });

  return result;
}

/**
 * ONE-TIME MIGRATION: Backfills missing IDs for accounts and Karigars 
 * and links them with existing orders/transactions.
 * 
 * KEY LOGIC: A "real" unique ID starts with a known prefix like 'acc_' or 'kar_'.
 * Any value in the ID column that does NOT start with such a prefix is treated
 * as a name and gets replaced with a proper unique ID.
 */
function backfillMissingIds() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets('all');
  
  var results = { accounts: 0, orders: 0, karigars: 0, transactions: 0, moneyBackups: 0 };
  
  // Helper: check if a value looks like a real generated ID
  function isRealId(val) {
    if (!val) return false;
    var s = String(val).trim();
    return s.indexOf('acc_') === 0 || s.indexOf('kar_') === 0;
  }

  function pickBestName(primary, fallback) {
    var p = String(primary || '').trim();
    var f = String(fallback || '').trim();
    if (!p) return f;
    if (isRealId(p)) return f || p;
    if (p.toLowerCase() === 'null' || p.toLowerCase() === 'undefined') return f || '';
    if (p.indexOf('seconds') !== -1 || p.indexOf('{') === 0) return f || '';
    return p;
  }
  
  // ==========================================
  // 1. KARIGARS: Generate real IDs
  // ==========================================
  var karigarNameToIdByCompany = { company1: {}, company2: {} };
  ['company1', 'company2'].forEach(function(cid) {
    var kSheet = ss.getSheetByName(getKarigarSheetName(cid));
    if (!kSheet || kSheet.getLastRow() <= 1) return;
    var karigarNameToId = karigarNameToIdByCompany[cid];
    var kData = kSheet.getDataRange().getValues();

    for (var i = 1; i < kData.length; i++) {
      var colA = kData[i][0] ? String(kData[i][0]).trim() : '';
      var colB = kData[i][1] ? String(kData[i][1]).trim() : '';

      if (!colA && !colB) continue;

      if (isRealId(colA)) {
        if (colB) karigarNameToId[colB.toLowerCase()] = colA;
        continue;
      }

      var realName = pickBestName(colB, colA);
      var newId = generatePrefixedId('kar_');
      var now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
      kSheet.getRange(i + 1, 1, 1, 3).setValues([[newId, realName, now]]);

      if (realName) karigarNameToId[realName.toLowerCase()] = newId;
      if (colA) karigarNameToId[colA.toLowerCase()] = newId;
      if (colB) karigarNameToId[colB.toLowerCase()] = newId;
      results.karigars++;
    }
  });

  // ==========================================
  // 2. KARIGAR TRANSACTIONS: Fix IDs
  // ==========================================
  ['company1', 'company2'].forEach(function(cid) {
    var kTxSheet = ss.getSheetByName(getKarigarTxSheetName(cid));
    if (!kTxSheet || kTxSheet.getLastRow() <= 1) return;
    var karigarNameToId = karigarNameToIdByCompany[cid];
    var kTxData = kTxSheet.getDataRange().getValues();

    for (var j = 1; j < kTxData.length; j++) {
      var txColB = kTxData[j][1] ? String(kTxData[j][1]).trim() : '';
      var txColC = kTxData[j][2] ? String(kTxData[j][2]).trim() : '';

      if (isRealId(txColB)) continue;

      var resolvedKId = karigarNameToId[txColB.toLowerCase()];
      if (resolvedKId) {
        var types = ['jama', 'upad', 'penalty', 'bonus'];
        if (types.indexOf(txColC.toLowerCase()) !== -1) {
          var kName = txColB;
          var kType = txColC;
          var kDesign = kTxData[j][3] || '';
          var kSize = kTxData[j][4] || '';
          var kPic = kTxData[j][5] || 0;
          var kPrice = kTxData[j][6] || 0;
          var kTotal = kTxData[j][7] || 0;
          var kUpad = kTxData[j][8] || 0;
          var kCreated = kTxData[j][9] || '';
          kTxSheet.getRange(j + 1, 2, 1, 10).setValues([[resolvedKId, kName, kType, kDesign, kSize, kPic, kPrice, kTotal, kUpad, kCreated]]);
        } else {
          kTxSheet.getRange(j + 1, 2).setValue(resolvedKId);
        }
        results.transactions++;
      } else if (!txColB && txColC && karigarNameToId[txColC.toLowerCase()]) {
        kTxSheet.getRange(j + 1, 2).setValue(karigarNameToId[txColC.toLowerCase()]);
        results.transactions++;
      }
    }
  });

  // ==========================================
  // 3. ACCOUNTS + ORDERS: Generate real IDs per company
  // ==========================================
  ['company1', 'company2'].forEach(function(cid) {
    var company = COMPANIES[cid];
    var accNameToId = {}; // name -> real unique ID
    
    // --- 3a. Fix Accounts sheet ---
    var accSheet = ss.getSheetByName(company.accountsSheet);
    if (accSheet && accSheet.getLastRow() > 1) {
      var accData = accSheet.getDataRange().getValues();
      for (var i = 1; i < accData.length; i++) {
        var colA = accData[i][0] ? String(accData[i][0]).trim() : '';
        var colB = accData[i][1] ? String(accData[i][1]).trim() : '';
        var colD = accData[i][3]; // Position
        
        if (!colA && !colB) continue; // Empty row
        
        if (isRealId(colA)) {
          // Already has a proper ID — Column B should be the name
          accNameToId[colB.toLowerCase()] = colA;
          continue;
        }
        
        // Column A may be old Firebase doc ID; prefer Column B when it looks like a real name.
        var realName = pickBestName(colB, colA);
        var newAccId = generatePrefixedId('acc_');
        var now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
        var pos = (colD !== '' && colD !== undefined && !isNaN(parseInt(colD))) ? parseInt(colD) : i - 1;
        
        // Rewrite the entire row: [ID, Name, AddedDate, Position]
        accSheet.getRange(i + 1, 1, 1, 4).setValues([[newAccId, realName, now, pos]]);
        
        if (realName) accNameToId[realName.toLowerCase()] = newAccId;
        // Also map legacy values so orders referencing old IDs/names get fixed
        if (colA) accNameToId[colA.toLowerCase()] = newAccId;
        if (colB) accNameToId[colB.toLowerCase()] = newAccId;
        results.accounts++;
      }
    }

    // --- 3b. Fix Orders sheet ---
    var ordSheet = ss.getSheetByName(company.ordersSheet);
    if (ordSheet && ordSheet.getLastRow() > 1) {
      var ordData = ordSheet.getDataRange().getValues();
      for (var j = 1; j < ordData.length; j++) {
        var oColB = ordData[j][1] ? String(ordData[j][1]).trim() : '';
        var oColC = ordData[j][2] ? String(ordData[j][2]).trim() : '';
        
        if (isRealId(oColB)) continue; // Already has proper ID, skip
        
        // oColB might be an account name
        var resolvedAccId = accNameToId[oColB.toLowerCase()];
        if (resolvedAccId) {
          // Name in the ID column — Col C likely has the Meesho number
          if (oColC === '' || !isNaN(parseFloat(oColC))) {
            // Misaligned: [Date, Name, Meesho, Synced] -> [Date, ID, Name, Meesho, Synced]
            var rName = oColB;
            var rMeesho = oColC || 0;
            var rSynced = ordData[j][3] || '';
            ordSheet.getRange(j + 1, 2, 1, 4).setValues([[resolvedAccId, rName, rMeesho, rSynced]]);
          } else {
            // Col C has a name too? Just replace ID
            ordSheet.getRange(j + 1, 2).setValue(resolvedAccId);
          }
          results.orders++;
        } else if (!oColB && oColC && accNameToId[oColC.toLowerCase()]) {
          // ID is empty, name is in col C
          ordSheet.getRange(j + 1, 2).setValue(accNameToId[oColC.toLowerCase()]);
          results.orders++;
        }
      }
    }
  });

  // ==========================================
  // 4. MONEY BACKUPS: Fix Account IDs
  // ==========================================
  var mbSheet = ss.getSheetByName(MONEY_BACKUP_SHEET);
  if (mbSheet && mbSheet.getLastRow() > 1) {
    var mbData = mbSheet.getDataRange().getValues();
    var globalAccMap = {};
    ['company1', 'company2'].forEach(function(cid) {
      var s = ss.getSheetByName(COMPANIES[cid].accountsSheet);
      if (s && s.getLastRow() > 1) {
        var d = s.getDataRange().getValues();
        for (var i = 1; i < d.length; i++) {
          var accId = String(d[i][0] || '').trim();
          var accName = String(d[i][1] || '').trim();
          if (accName && isRealId(accId)) {
            globalAccMap[accName.toLowerCase()] = accId;
          }
        }
      }
    });

    for (var k = 1; k < mbData.length; k++) {
      var mbId = mbData[k][2] ? String(mbData[k][2]).trim() : '';
      var mbName = mbData[k][3] ? String(mbData[k][3]).trim() : '';
      if (mbName && !isRealId(mbId) && globalAccMap[mbName.toLowerCase()]) {
        mbSheet.getRange(k + 1, 3).setValue(globalAccMap[mbName.toLowerCase()]);
        results.moneyBackups++;
      }
    }
  }
  // ==========================================
  // 5. CLEANUP: Fix missing names, missing IDs, and number formatting in Orders
  // ==========================================
  ['company1', 'company2'].forEach(function(cid) {
    var company = COMPANIES[cid];
    
    // Re-read the now-fixed Accounts to build complete maps
    var idToName = {};
    var nameToId = {};
    var accSheet = ss.getSheetByName(company.accountsSheet);
    if (accSheet && accSheet.getLastRow() > 1) {
      var accData = accSheet.getDataRange().getValues();
      for (var i = 1; i < accData.length; i++) {
        var aid = String(accData[i][0] || '').trim();
        var aname = String(accData[i][1] || '').trim();
        if (aid && aname) {
          idToName[aid] = aname;
          nameToId[aname.toLowerCase()] = aid;
        }
      }
    }
    
    var ordSheet = ss.getSheetByName(company.ordersSheet);
    if (ordSheet && ordSheet.getLastRow() > 1) {
      var lastRow = ordSheet.getLastRow();
      
      // Force Meesho (Col D) to plain number format
      ordSheet.getRange(2, 4, lastRow - 1, 1).setNumberFormat('0');
      
      // Fix any remaining missing IDs or Names row by row
      var ordData = ordSheet.getRange(2, 1, lastRow - 1, 5).getValues();
      for (var j = 0; j < ordData.length; j++) {
        var rowNum = j + 2;
        var oId = String(ordData[j][1] || '').trim();
        var oName = String(ordData[j][2] || '').trim();
        var oMeesho = ordData[j][3];
        
        var changed = false;
        var newId = oId;
        var newName = oName;
        var newMeesho = oMeesho;
        
        // Fix missing Account ID
        if (!isRealId(oId) && oName && nameToId[oName.toLowerCase()]) {
          newId = nameToId[oName.toLowerCase()];
          changed = true;
        }
        
        // Fix missing Account Name
        if (!oName && isRealId(oId) && idToName[oId]) {
          newName = idToName[oId];
          changed = true;
        }
        
        // Fix Meesho if it's a Date object
        if (oMeesho instanceof Date) {
          newMeesho = 0;
          changed = true;
        }
        
        if (changed) {
          ordSheet.getRange(rowNum, 2, 1, 4).setValues([[newId, newName, parseInt(newMeesho) || 0, ordData[j][4] || '']]);
          results.orders++;
        }
      }
    }
  });

  return { success: true, message: 'Backfill completed successfully.', stats: results };
}

/**
 * REPAIR: Uses Firebase data (passed from frontend) to fix corrupted names in Sheets.
 * Firebase doc IDs that were incorrectly stored as "names" get replaced with real names.
 */
function repairFromFirebase(fbData) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets('all');
  
  var results = { accounts: 0, karigars: 0, orders: 0, transactions: 0 };
  
  // Fix karigar sheet headers first
  ['company1', 'company2'].forEach(function(cid) {
    var kSheet = ss.getSheetByName(getKarigarSheetName(cid));
    if (!kSheet) return;
    var kHeaders = getKarigarHeaderRow();
    var h = kSheet.getRange('A1').getValue();
    if (h !== 'Karigar ID') {
      if (kSheet.getMaxColumns() < kHeaders.length) {
        kSheet.insertColumnsAfter(kSheet.getMaxColumns(), kHeaders.length - kSheet.getMaxColumns());
      }
      kSheet.getRange(1, 1, 1, kHeaders.length).setValues([kHeaders]);
      kSheet.getRange(1, 1, 1, kHeaders.length).setFontWeight('bold');
    }
  });
  
  // Build Firebase doc ID -> real name maps
  var fbAccIdToName = {};   // Firebase doc id -> real account name
  var fbAccIdToCompany = {}; // Firebase doc id -> companyId
  var fbKarIdToNameByCompany = { company1: {}, company2: {} };
  
  if (fbData.accounts) {
    fbData.accounts.forEach(function(a) {
      if (a.docId && a.name) {
        fbAccIdToName[a.docId] = a.name;
        fbAccIdToCompany[a.docId] = a.companyId || 'company1';
      }
    });
  }
  
  if (fbData.karigars) {
    fbData.karigars.forEach(function(k) {
      if (k.docId && k.name) {
        var cid = normalizeCompanyId(k.companyId || 'company1');
        fbKarIdToNameByCompany[cid][k.docId] = k.name;
      }
    });
  }
  
  // ==========================================
  // 1. FIX ACCOUNTS: Replace Firebase doc IDs in Name column with real names
  // ==========================================
  ['company1', 'company2'].forEach(function(cid) {
    var company = COMPANIES[cid];
    var accSheet = ss.getSheetByName(company.accountsSheet);
    if (!accSheet || accSheet.getLastRow() <= 1) return;
    
    var accData = accSheet.getDataRange().getValues();
    for (var i = 1; i < accData.length; i++) {
      var colA = String(accData[i][0] || '').trim(); // Account ID
      var colB = String(accData[i][1] || '').trim(); // Account Name (might be Firebase doc ID)
      
      // Check if Column B is a Firebase doc ID (has a real name mapping)
      if (colB && fbAccIdToName[colB]) {
        accSheet.getRange(i + 1, 2).setValue(fbAccIdToName[colB]);
        results.accounts++;
      }
      // Also check if Column A is a Firebase doc ID (not acc_ prefixed)
      else if (colA && colA.indexOf('acc_') !== 0 && fbAccIdToName[colA]) {
        // Column A has Firebase doc ID as the "ID" — fix the whole row
        var realName = fbAccIdToName[colA];
        var newId = generatePrefixedId('acc_');
        var now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
        var pos = accData[i][3] || (i - 1);
        accSheet.getRange(i + 1, 1, 1, 4).setValues([[newId, realName, now, pos]]);
        results.accounts++;
      }
    }
  });
  
  // ==========================================  
  // 2. FIX KARIGARS: Replace Firebase doc IDs with real names
  // ==========================================
  ['company1', 'company2'].forEach(function(cid) {
    var kSheet = ss.getSheetByName(getKarigarSheetName(cid));
    if (!kSheet || kSheet.getLastRow() <= 1) return;
    var fbKarIdToName = fbKarIdToNameByCompany[cid];
    var kData = kSheet.getDataRange().getValues();
    for (var i = 1; i < kData.length; i++) {
      var colA = String(kData[i][0] || '').trim();
      var colB = String(kData[i][1] || '').trim();

      if (colB && fbKarIdToName[colB]) {
        kSheet.getRange(i + 1, 2).setValue(fbKarIdToName[colB]);
        results.karigars++;
      }
      else if (colA && colA.indexOf('kar_') !== 0 && fbKarIdToName[colA]) {
        var realName = fbKarIdToName[colA];
        var newId = generatePrefixedId('kar_');
        var now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
        kSheet.getRange(i + 1, 1, 1, 3).setValues([[newId, realName, now]]);
        results.karigars++;
      }
    }
  });
  
  // ==========================================
  // 3. FIX ORDERS: Fill missing data, fix formats, remove duplicates
  // ==========================================
  var globalIdToName = {};
  var globalNameToId = {};
  ['company1', 'company2'].forEach(function(cid) {
    var company = COMPANIES[cid];
    var accSheet = ss.getSheetByName(company.accountsSheet);
    if (accSheet && accSheet.getLastRow() > 1) {
      var accData = accSheet.getDataRange().getValues();
      for (var ai = 1; ai < accData.length; ai++) {
        var aid = String(accData[ai][0] || '').trim();
        var aname = String(accData[ai][1] || '').trim();
        if (aid && aname) {
          globalIdToName[aid] = aname;
          globalNameToId[aname.toLowerCase()] = aid;
        }
      }
    }
    
    var ordSheet = ss.getSheetByName(company.ordersSheet);
    if (ordSheet && ordSheet.getLastRow() > 1) {
      var lastRow = ordSheet.getLastRow();
      var ordData = ordSheet.getRange(2, 1, lastRow - 1, 5).getValues();
      
      // Build a set of good rows (with acc_ IDs) keyed by date+accId
      var goodRowKeys = {};
      ordData.forEach(function(row) {
        var rId = String(row[1] || '').trim();
        var rDate = row[0] instanceof Date ? Utilities.formatDate(row[0], Session.getScriptTimeZone(), 'yyyy-MM-dd') : String(row[0]).split(' ')[0];
        if (rId.indexOf('acc_') === 0) {
          goodRowKeys[rDate + '_' + rId] = true;
        }
      });
      
      var cleanRows = [];
      var removedCount = 0;
      
      for (var j = 0; j < ordData.length; j++) {
        var oDate = ordData[j][0];
        var oId = String(ordData[j][1] || '').trim();
        var oName = String(ordData[j][2] || '').trim();
        var oMeesho = ordData[j][3];
        var oSynced = ordData[j][4];
        
        var dateStr = oDate instanceof Date ? Utilities.formatDate(oDate, Session.getScriptTimeZone(), 'yyyy-MM-dd') : String(oDate).split(' ')[0];
        
        // SKIP garbage rows: date as Account ID, or completely empty
        if (oId && /^\d{4}-\d{2}-\d{2}/.test(oId)) {
          removedCount++;
          continue;
        }
        
        // SKIP old-format duplicate rows:
        // - No acc_ ID AND Synced At is a serial number (46087 etc.)
        // - These are old backup entries that duplicate the acc_-keyed rows
        var isSyncedSerial = (typeof oSynced === 'number' && oSynced > 10000);
        if (!oId || oId.indexOf('acc_') !== 0) {
          // This row has no proper ID — check if it's a known duplicate
          var nameInIdCol = oId; // name might be in the ID column
          if (nameInIdCol && isSyncedSerial && !globalNameToId[nameInIdCol.toLowerCase()]) {
            // Old-format backup row with a name that doesn't match any current account
            removedCount++;
            continue;
          }
          
          // Try to fill in the ID
          if (nameInIdCol && globalNameToId[nameInIdCol.toLowerCase()]) {
            // Name is in ID column, shift data
            var resolvedId = globalNameToId[nameInIdCol.toLowerCase()];
            // Check if a good row already exists for this date+id
            if (goodRowKeys[dateStr + '_' + resolvedId]) {
              removedCount++; // Duplicate
              continue;
            }
            oId = resolvedId;
            oName = nameInIdCol;
            // oMeesho is already in the right column (was Account Name col)
          } else if (!oId && oName && globalNameToId[oName.toLowerCase()]) {
            oId = globalNameToId[oName.toLowerCase()];
          }
        }
        
        // Fix missing Account Name by ID lookup
        if (!oName && oId && globalIdToName[oId]) {
          oName = globalIdToName[oId];
        }
        
        // Fix Account Name if it's a Firebase doc ID
        if (oName && fbAccIdToName[oName]) {
          oName = fbAccIdToName[oName];
        }
        
        // Fix Meesho if it's a Date
        var mVal = (oMeesho instanceof Date) ? 0 : (parseInt(oMeesho) || 0);
        
        // Fix Synced At
        var syncVal = oSynced;
        if (isSyncedSerial) {
          syncVal = Utilities.formatDate(new Date((oSynced - 25569) * 86400000), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
        } else if (oSynced instanceof Date) {
          syncVal = Utilities.formatDate(oSynced, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
        }
        
        cleanRows.push([dateStr, oId || '', oName || '', mVal, syncVal || '']);
      }
      
      // Rewrite the entire sheet with clean data
      ordSheet.getRange(2, 1, Math.max(1, lastRow - 1), 5).clearContent();
      if (cleanRows.length > 0) {
        ordSheet.getRange(2, 1, cleanRows.length, 5).setValues(cleanRows);
        // Force number format on Meesho column
        ordSheet.getRange(2, 4, cleanRows.length, 1).setNumberFormat('0');
      }
      
      results.orders += removedCount; // Track cleaned rows
    }
  });
  
  // ==========================================
  // 4. FIX KARIGAR TRANSACTIONS: Fill missing names
  // ==========================================
  ['company1', 'company2'].forEach(function(cid) {
    var globalKarIdToName = {};
    var kSheet = ss.getSheetByName(getKarigarSheetName(cid));
    if (kSheet && kSheet.getLastRow() > 1) {
      var kData2 = kSheet.getDataRange().getValues();
      for (var ki = 1; ki < kData2.length; ki++) {
        var kid = String(kData2[ki][0] || '').trim();
        var kname = String(kData2[ki][1] || '').trim();
        if (kid && kname) globalKarIdToName[kid] = kname;
      }
    }

    var fbKarIdToName = fbKarIdToNameByCompany[cid];
    var kTxSheet = ss.getSheetByName(getKarigarTxSheetName(cid));
    if (!kTxSheet || kTxSheet.getLastRow() <= 1) return;
    var kTxData = kTxSheet.getDataRange().getValues();
    for (var ti = 1; ti < kTxData.length; ti++) {
      var txId = String(kTxData[ti][1] || '').trim();
      var txName = String(kTxData[ti][2] || '').trim();

      if (txName && fbKarIdToName[txName]) {
        kTxSheet.getRange(ti + 1, 3).setValue(fbKarIdToName[txName]);
        results.transactions++;
      }
      else if (!txName && txId && globalKarIdToName[txId]) {
        kTxSheet.getRange(ti + 1, 3).setValue(globalKarIdToName[txId]);
        results.transactions++;
      }
    }
  });
  
  return { success: true, message: 'Repair from Firebase completed.', stats: results };
}

/**
 * Read ALL data from Google Sheets and return as structured JSON.
 * Used by "Sync to Firebase" to push Sheet data as source of truth.
 */
function getAllSheetData() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets('all');
  
  var result = {
    company1: { accounts: [], orders: [] },
    company2: { accounts: [], orders: [] },
    karigars: [],
    karigarTransactions: [],
    designPrices: {}
  };
  var karigarCols = getKarigarHeaderRow().length;
  var karigarTxCols = getKarigarTxHeaderRow().length;
  
  // Read Accounts + Orders per company
  ['company1', 'company2'].forEach(function(cid) {
    var company = COMPANIES[cid];
    
    var accSheet = ss.getSheetByName(company.accountsSheet);
    if (accSheet && accSheet.getLastRow() > 1) {
      var data = accSheet.getDataRange().getValues();
      for (var i = 1; i < data.length; i++) {
        var accId = String(data[i][0] || '').trim();
        var accName = String(data[i][1] || '').trim();
        if (!accName) continue;
        result[cid].accounts.push({
          accountId: accId,
          name: accName,
          addedDate: data[i][2] ? String(data[i][2]) : '',
          position: parseInt(data[i][3]) || (i - 1)
        });
      }
    }
    
    var ordSheet = ss.getSheetByName(company.ordersSheet);
    if (ordSheet && ordSheet.getLastRow() > 1) {
      var data = ordSheet.getRange(2, 1, ordSheet.getLastRow() - 1, 5).getValues();
      for (var i = 0; i < data.length; i++) {
        var dateStr = data[i][0] instanceof Date ? 
          Utilities.formatDate(data[i][0], Session.getScriptTimeZone(), 'yyyy-MM-dd') : 
          String(data[i][0]).split(' ')[0];
        var accId = String(data[i][1] || '').trim();
        var accName = String(data[i][2] || '').trim();
        if (!dateStr || !accId) continue;
        result[cid].orders.push({
          date: dateStr,
          accountId: accId,
          accountName: accName,
          meesho: parseInt(data[i][3]) || 0
        });
      }
    }
  });
  
  // Read Karigars + Karigar Transactions per company
  ['company1', 'company2'].forEach(function(cid) {
    var kSheet = ss.getSheetByName(getKarigarSheetName(cid));
    if (kSheet && kSheet.getLastRow() > 1) {
      var data = kSheet.getRange(2, 1, kSheet.getLastRow() - 1, karigarCols).getValues();
      for (var i = 0; i < data.length; i++) {
        var kid = String(data[i][0] || '').trim();
        var kname = String(data[i][1] || '').trim();
        if (!kname) continue;
        result.karigars.push({
          id: kid,
          name: kname,
          companyId: cid,
          addedAt: data[i][2] ? String(data[i][2]) : '',
          createdByName: String(data[i][3] || '').trim(),
          createdByUser: String(data[i][4] || '').trim(),
          createdByRole: String(data[i][5] || '').trim(),
          source: String(data[i][6] || '').trim(),
          dashboard: String(data[i][7] || '').trim()
        });
      }
    }

    var ktSheet = ss.getSheetByName(getKarigarTxSheetName(cid));
    if (ktSheet && ktSheet.getLastRow() > 1) {
      var txData = ktSheet.getRange(2, 1, ktSheet.getLastRow() - 1, karigarTxCols).getValues();
      for (var j = 0; j < txData.length; j++) {
        var parsedDate = parseFlexibleDateTime(txData[j][0], Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd'));
        var dateOnly = Utilities.formatDate(parsedDate, Session.getScriptTimeZone(), 'yyyy-MM-dd');
        var createdAtDate = parseFlexibleDateTime(txData[j][10] || txData[j][0], dateOnly);
        var createdAtStr = Utilities.formatDate(createdAtDate, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
        result.karigarTransactions.push({
          date: dateOnly,
          karigarId: String(txData[j][1] || '').trim(),
          karigarName: String(txData[j][2] || '').trim(),
          type: String(txData[j][3] || '').trim(),
          designName: String(txData[j][4] || '').trim(),
          size: String(txData[j][5] || '').trim(),
          pic: parseInt(txData[j][6]) || 0,
          price: parseInt(txData[j][7]) || 0,
          totalJama: parseInt(txData[j][8]) || 0,
          upadAmount: parseInt(txData[j][9]) || 0,
          companyId: cid,
          createdAt: createdAtStr,
          createdByName: String(txData[j][11] || '').trim(),
          createdByUser: String(txData[j][12] || '').trim(),
          createdByRole: String(txData[j][13] || '').trim(),
          updatedByName: String(txData[j][14] || '').trim(),
          updatedByUser: String(txData[j][15] || '').trim(),
          updatedByRole: String(txData[j][16] || '').trim(),
          source: String(txData[j][17] || '').trim(),
          dashboard: String(txData[j][18] || '').trim()
        });
      }
    }
  });
  
  // Read Design Prices
  var dpSheet = ss.getSheetByName(DESIGN_PRICES_SHEET);
  if (dpSheet && dpSheet.getLastRow() > 1) {
    var data = dpSheet.getDataRange().getValues();
    for (var i = 1; i < data.length; i++) {
      var designName = String(data[i][0] || '').trim();
      if (designName) result.designPrices[designName] = parseInt(data[i][1]) || 0;
    }
  }
  
  return { success: true, data: result };
}
