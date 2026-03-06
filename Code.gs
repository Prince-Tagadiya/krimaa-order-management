// KRIMAA ORDER MANAGEMENT v4.0 - MULTI-COMPANY + EDIT/DELETE ACCOUNTS
var SCRIPT_NAME = "Order Management API";

// Company configuration - define your companies here
var COMPANIES = {
  'company1': { name: 'Company 1', accountsSheet: 'Accounts_C1', ordersSheet: 'Orders_C1' },
  'company2': { name: 'Company 2', accountsSheet: 'Accounts_C2', ordersSheet: 'Orders_C2' }
};
var REMARKS_SHEET = 'Remarks';
var MONEY_BACKUP_SHEET = 'Money_Backups';
var KARIGAR_SHEET = 'Karigars';
var KARIGAR_TX_SHEET = 'Karigar_Transactions';
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
  var kSheet = ss.getSheetByName(KARIGAR_SHEET);
  if (!kSheet) {
    kSheet = ss.insertSheet(KARIGAR_SHEET);
    kSheet.appendRow(['Name', 'Added At']);
    kSheet.getRange('A1:B1').setFontWeight('bold');
    kSheet.setFrozenRows(1);
  }

  var txSheet = ss.getSheetByName(KARIGAR_TX_SHEET);
  if (!txSheet) {
    txSheet = ss.insertSheet(KARIGAR_TX_SHEET);
    txSheet.appendRow(['Date', 'Karigar', 'Type', 'Design', 'Size', 'Pic', 'Price', 'Total Jama', 'Upad Amount', 'Created At']);
    txSheet.getRange('A1:J1').setFontWeight('bold');
    txSheet.setFrozenRows(1);
  }

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
    sheet.appendRow(['Date', 'Remark', 'Updated']);
    sheet.getRange('A1:C1').setFontWeight('bold');
    sheet.setFrozenRows(1);
  }
}

function setupMoneyBackupSheet(ss) {
  var sheet = ss.getSheetByName(MONEY_BACKUP_SHEET);
  if (!sheet) {
    sheet = ss.insertSheet(MONEY_BACKUP_SHEET);
    sheet.appendRow(['Backup Date', 'Company Id', 'Company Name', 'Account Name', 'Money', 'Expense', 'Balance', 'Reason', 'Saved At']);
    sheet.getRange('A1:I1').setFontWeight('bold');
    sheet.setFrozenRows(1);
  }
}

function setupSheetsForCompany(ss, company) {
  var accountsSheet = ss.getSheetByName(company.accountsSheet);
  if (!accountsSheet) accountsSheet = ss.insertSheet(company.accountsSheet);
  // Unconditionally ensure valid headers exist
  accountsSheet.getRange(1, 1, 1, 3).setValues([['Account Name', 'Added Date', 'Position']]).setFontWeight("bold");
  accountsSheet.setFrozenRows(1);
  
  var ordersSheet = ss.getSheetByName(company.ordersSheet);
  if (!ordersSheet) ordersSheet = ss.insertSheet(company.ordersSheet);
  // Unconditionally ensure valid headers exist
  ordersSheet.getRange(1, 1, 1, 5).setValues([['Date', 'Account Name', 'Meesho', 'Total', 'Synced At']]).setFontWeight("bold");
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
      return output.setContent(JSON.stringify(editAccount(data.oldName, data.newName, companyId)));
    } else if (action === 'deleteAccount') {
      return output.setContent(JSON.stringify(deleteAccount(data.accountName, companyId)));
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
      return output.setContent(JSON.stringify(updateSingleOrder(data.date, data.accountName, data.field, data.value, companyId)));
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
      row.companyName || '',
      row.accountName || '',
      money,
      expense,
      money - expense,
      safeReason,
      nowTs
    ]);
  }

  sheet.getRange(sheet.getLastRow() + 1, 1, values.length, values[0].length).setValues(values);
  return { success: true, message: 'Money backup saved to sheet', rows: values.length, sheet: MONEY_BACKUP_SHEET };
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
function addAccount(accountName, companyId) {
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
    var startIdx = (data[0][0] === 'Account Name') ? 1 : 0;
    for (var i = startIdx; i < data.length; i++) {
      if (data[i][0].toString().trim().toLowerCase() === accountName.trim().toLowerCase()) {
        return {success: false, message: 'Account already exists'};
      }
    }
  }
  
  var date = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
  sheet.appendRow([accountName.trim(), date]);
  return {success: true, message: 'Account added successfully'};
}

// Logic to edit an existing account name
function editAccount(oldName, newName, companyId) {
  companyId = normalizeCompanyId(companyId);
  if (!oldName || !newName || newName.trim() === '') {
    return {success: false, message: 'Account name cannot be empty'};
  }
  
  oldName = oldName.trim();
  newName = newName.trim();
  
  if (oldName.toLowerCase() === newName.toLowerCase()) {
    // Same name but possibly different case, allow it
  }
  
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  
  // Update in Accounts sheet
  var accountsSheet = ss.getSheetByName(company.accountsSheet);
  var accData = accountsSheet.getDataRange().getValues();
  var startIdx = (accData[0][0] === 'Account Name') ? 1 : 0;
  
  // Check if new name already exists (different from old)
  if (oldName.toLowerCase() !== newName.toLowerCase()) {
    for (var i = startIdx; i < accData.length; i++) {
      if (accData[i][0].toString().trim().toLowerCase() === newName.toLowerCase()) {
        return {success: false, message: 'Account name already exists'};
      }
    }
  }
  
  var accountFound = false;
  for (var i = startIdx; i < accData.length; i++) {
    if (accData[i][0].toString().trim() === oldName) {
      accountsSheet.getRange(i + 1, 1).setValue(newName);
      accountFound = true;
      break;
    }
  }
  
  if (!accountFound) {
    return {success: false, message: 'Account not found'};
  }
  
  // Also update account name in Orders sheet
  var ordersSheet = ss.getSheetByName(company.ordersSheet);
  if (ordersSheet) {
    var ordData = ordersSheet.getDataRange().getValues();
    var ordStartIdx = 0;
    if (ordData.length > 0 && /^Date(\s*\/\s*.+)?$/.test(String(ordData[0][0] || '').trim())) {
      ordStartIdx = 1;
    }
    
    for (var j = ordStartIdx; j < ordData.length; j++) {
      if (ordData[j][1] && ordData[j][1].toString().trim() === oldName) {
        ordersSheet.getRange(j + 1, 2).setValue(newName);
      }
    }
  }
  
  return {success: true, message: 'Account updated successfully'};
}

// Logic to delete an account
function deleteAccount(accountName, companyId) {
  companyId = normalizeCompanyId(companyId);
  if (!accountName || accountName.trim() === '') {
    return {success: false, message: 'Account name cannot be empty'};
  }
  
  accountName = accountName.trim();
  
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  
  // Delete from Accounts sheet
  var accountsSheet = ss.getSheetByName(company.accountsSheet);
  var accData = accountsSheet.getDataRange().getValues();
  var startIdx = (accData[0][0] === 'Account Name') ? 1 : 0;
  
  var rowToDelete = -1;
  for (var i = startIdx; i < accData.length; i++) {
    if (accData[i][0].toString().trim() === accountName) {
      rowToDelete = i + 1; // 1-indexed for sheet
      break;
    }
  }
  
  if (rowToDelete === -1) {
    return {success: false, message: 'Account not found'};
  }
  
  accountsSheet.deleteRow(rowToDelete);
  
  return {success: true, message: 'Account deleted successfully'};
}

// Logic to get all accounts from the sheet (sorted by Position column)
function getAccounts(companyId) {
  companyId = normalizeCompanyId(companyId);
  setupSheets(companyId);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.accountsSheet);
  if (!sheet) return {success: true, data: []};
  
  var data = sheet.getDataRange().getValues();
  var accounts = [];
  if (data.length > 0) {
    var startIdx = (data[0][0] === 'Account Name') ? 1 : 0;
    for (var i = startIdx; i < data.length; i++) {
      if (data[i][0]) {
        var pos = data[i][2]; // Position column (C)
        accounts.push({ name: data[i][0], position: (pos !== '' && pos !== undefined) ? parseInt(pos) : 9999 });
      }
    }
  }
  
  // Sort by position
  accounts.sort(function(a, b) { return a.position - b.position; });
  
  // Return just the names in sorted order
  var sortedNames = accounts.map(function(a) { return a.name; });
  return {success: true, data: sortedNames};
}

// Logic to update account order/position in the sheet
function updateAccountOrder(orderedAccounts, companyId) {
  companyId = normalizeCompanyId(companyId);
  if (!orderedAccounts || orderedAccounts.length === 0) {
    return {success: false, message: 'No accounts provided'};
  }
  
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.accountsSheet);
  var data = sheet.getDataRange().getValues();
  
  var startIdx = (data[0][0] === 'Account Name') ? 1 : 0;
  
  for (var i = startIdx; i < data.length; i++) {
    var accountName = data[i][0].toString().trim();
    var posIndex = orderedAccounts.indexOf(accountName);
    if (posIndex !== -1) {
      sheet.getRange(i + 1, 3).setValue(posIndex);
    }
  }
  
  return {success: true, message: 'Account order updated successfully'};
}

// Logic to submit daily orders
function submitOrders(dateStr, orders, companyId) {
  companyId = normalizeCompanyId(companyId);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.ordersSheet);
  var data = sheet.getDataRange().getValues();
  
  if (!orders || orders.length === 0) {
    return {success: false, message: 'No orders provided'};
  }
  
  var newRows = [];
  for (var j = 0; j < orders.length; j++) {
    var order = orders[j];
    newRows.push([dateStr, order.accountName, order.meesho || 0, order.total || 0]);
  }
  
  if(newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
  }
  
  return {success: true, message: 'Orders submitted successfully'};
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
  
  var startRow = 1;
  if (lastRow >= 1) {
    var firstCell = sheet.getRange(1, 1).getValue().toString();
    if (/^Date(\s*\/\s*.+)?$/.test(firstCell.trim())) {
      startRow = 2;
    }
  }

  if (lastRow < startRow) return {success: true, data: []};
  var data = sheet.getRange(startRow, 1, lastRow - (startRow - 1), 4).getValues();
  var records = [];
  
  for (var i = 0; i < data.length; i++) {
    var rawDate = data[i][0];
    var formattedDate = rawDate;
    if (rawDate instanceof Date) {
      formattedDate = Utilities.formatDate(rawDate, Session.getScriptTimeZone(), "yyyy-MM-dd");
    }
    records.push({
      date: formattedDate,
      accountName: data[i][1],
      meesho: data[i][2],
      total: data[i][3]
    });
  }
  
  if (monthFilter && monthFilter.length === 7) {
    records = records.filter(function(r) {
      return r.date.substring(0, 7) === monthFilter;
    });
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

// ===== REMARKS =====
function saveRemark(dateStr, remark) {
  if (!dateStr) return {success: false, message: 'Date required'};
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupRemarksSheet(ss);
  var sheet = ss.getSheetByName(REMARKS_SHEET);
  var data = sheet.getDataRange().getValues();
  var startIdx = (data.length > 0 && data[0][0] === 'Date') ? 1 : 0;
  
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
function updateSingleOrder(dateStr, accountName, field, value, companyId) {
  companyId = normalizeCompanyId(companyId);
  if (!dateStr || !accountName || !field) return {success: false, message: 'Missing parameters'};
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.ordersSheet);
  var data = sheet.getDataRange().getValues();
  var startIdx = (data.length > 0 && (data[0][0] === 'Date' || data[0][0] === 'Date / \u0CA4\u0CBE\u0CB0\u0CC0\u0C96')) ? 1 : 0;
  
  var colIndex = -1;
  if (field === 'meesho') colIndex = 3;
  else return {success: false, message: 'Invalid field'};
  
  var numVal = parseInt(value) || 0;
  
  for (var i = startIdx; i < data.length; i++) {
    var rowDate = data[i][0];
    if (rowDate instanceof Date) {
      rowDate = Utilities.formatDate(rowDate, Session.getScriptTimeZone(), 'yyyy-MM-dd');
    }
    if (rowDate.toString().trim() === dateStr.trim() && data[i][1].toString().trim() === accountName.trim()) {
      sheet.getRange(i + 1, colIndex).setValue(numVal);
      // Recalculate total
      sheet.getRange(i + 1, 4).setValue(numVal);
      return {success: true, message: 'Order updated'};
    }
  }
  
  return {success: false, message: 'Order row not found for ' + dateStr + ' / ' + accountName};
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
      sheet.getRange(sheet.getLastRow() + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
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
  
  // 1. Sync Remarks
  if (data.remarks) {
    var sheet = ss.getSheetByName(REMARKS_SHEET);
    sheet.clear();
    sheet.appendRow(['Account ID', 'Remark', 'Last Updated']);
    sheet.getRange('A1:C1').setFontWeight('bold');
    var rows = [];
    for (var id in data.remarks) {
      if (id === 'last_update') continue;
      rows.push([id, data.remarks[id], new Date()]);
    }
    if (rows.length > 0) {
      sheet.getRange(2, 1, rows.length, 3).setValues(rows);
      totalRows += rows.length;
    }
  }

  // 2. Sync Karigars (Overwrite style for simplicity or UPSERT)
  if (data.karigars) {
    var sheet = ss.getSheetByName(KARIGAR_SHEET);
    sheet.getRange(2, 1, Math.max(1, sheet.getLastRow()), 2).clearContent();
    var rows = data.karigars.map(function(k) { return [k.name, k.addedDate || new Date()]; });
    if (rows.length > 0) {
      sheet.getRange(2, 1, rows.length, 2).setValues(rows);
      totalRows += rows.length;
    }
  }

  // 3. Sync Karigar Transactions (UPSERT logic to prevent data loss)
  if (data.karigarTransactions) {
    var sheet = ss.getSheetByName(KARIGAR_TX_SHEET);
    var existData = sheet.getLastRow() > 1 ? sheet.getRange(2, 1, sheet.getLastRow()-1, 10).getValues() : [];
    var existMap = {};
    existData.forEach(function(r) {
      var dStr = r[0] instanceof Date ? Utilities.formatDate(r[0], Session.getScriptTimeZone(), 'yyyy-MM-dd') : r[0];
      var key = dStr + "_" + r[1] + "_" + r[2] + "_" + r[3] + "_" + r[5]; // Date_Karigar_Type_Design_Pic
      existMap[key] = true;
    });

    var newRows = [];
    data.karigarTransactions.forEach(function(tx) {
      var dStr = tx.date;
      var key = dStr + "_" + tx.karigarName + "_" + tx.type + "_" + (tx.designName || '') + "_" + (tx.pic || '0');
      if (!existMap[key]) {
        newRows.push([
          tx.date, 
          tx.karigarName, 
          tx.type, 
          tx.designName || '', 
          tx.size || '', 
          tx.pic || 0, 
          tx.price || 0, 
          tx.total || 0, 
          tx.amount || tx.upadAmount || 0,
          new Date()
        ]);
      }
    });
    if (newRows.length > 0) {
      sheet.getRange(sheet.getLastRow() + 1, 1, newRows.length, 10).setValues(newRows);
      totalRows += newRows.length;
    }
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

  // 5. Sync Money Backups
  if (data.moneyBackups) {
    var sheet = ss.getSheetByName(MONEY_BACKUP_SHEET);
    var existDates = sheet.getLastRow() > 1 ? sheet.getRange(2, 1, sheet.getLastRow()-1, 1).getValues().map(function(r){ return String(r[0]); }) : [];
    var newRows = [];
    data.moneyBackups.forEach(function(b) {
      if (existDates.indexOf(String(b.backupDate)) === -1) {
        (b.rows || []).forEach(function(r) {
           newRows.push([b.backupDate, r.companyId, r.companyName, r.accountName, r.money, r.expense, r.balance, b.reason || '', b.timestamp || new Date()]);
        });
      }
    });
    if (newRows.length > 0) {
      sheet.getRange(sheet.getLastRow() + 1, 1, newRows.length, 9).setValues(newRows);
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

  // Sync Accounts
  if (compData.accounts && compData.accounts.length > 0) {
    var sheet = ss.getSheetByName(company.accountsSheet);
    sheet.getRange(2, 1, Math.max(1, sheet.getLastRow()), 3).clearContent();
    var rows = compData.accounts.map(function(acc, idx) {
      return [acc.name, acc.addedDate || new Date(), acc.position || idx];
    });
    sheet.getRange(2, 1, rows.length, 3).setValues(rows);
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
    var dStr = r[0] instanceof Date ? Utilities.formatDate(r[0], Session.getScriptTimeZone(), 'yyyy-MM-dd') : r[0];
    var key = dStr + "_" + r[1];
    existMap[key] = { row: idx + 2, data: r };
  });

  var now = new Date();
  var newRows = [];
  var updatedRowsCount = 0;
  
  orders.forEach(function(o) {
    var key = o.date + "_" + o.accountName;
    if (existMap[key]) {
      var exist = existMap[key].data;
      if (parseInt(exist[2]) !== parseInt(o.meesho) || parseInt(exist[3]) !== parseInt(o.total)) {
        sheet.getRange(existMap[key].row, 3, 1, 3).setValues([[o.meesho, o.total, now]]);
        updatedRowsCount++;
      }
    } else {
      newRows.push([o.date, o.accountName, o.meesho, o.total, now]);
    }
  });

  if (newRows.length > 0) {
    sheet.getRange(sheet.getLastRow() + 1, 1, newRows.length, 5).setValues(newRows);
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
        var dStr = r[0] instanceof Date ? Utilities.formatDate(r[0], Session.getScriptTimeZone(), 'yyyy-MM-dd') : r[0];
        if (dStr >= fromDate && dStr <= toDate) {
          result.orders.push({ date: dStr, companyId: cid, accountName: r[1], meesho: r[2], total: r[3] });
        }
      });
    }
  });

  // 2. Fetch Karigar Transactions
  var kxSheet = ss.getSheetByName(KARIGAR_TX_SHEET);
  if (kxSheet && kxSheet.getLastRow() > 1) {
    var data = kxSheet.getRange(2, 1, kxSheet.getLastRow()-1, 10).getValues();
    data.forEach(function(r) {
      var dStr = r[0] instanceof Date ? Utilities.formatDate(r[0], Session.getScriptTimeZone(), 'yyyy-MM-dd') : r[0]; // Date is col 0
      if (dStr >= fromDate && dStr <= toDate) {
        result.karigarTransactions.push({
          id: 'gs_' + Math.random().toString(36).substr(2, 9),
          karigarName: r[1],
          type: r[2],
          amount: parseFloat(r[7]) > 0 ? r[7] : r[8], // Jama total or Upad amount
          total: r[7],
          date: dStr,
          designName: r[3],
          companyId: 'History',
          addedBy: 'Archive'
        });
      }
    });
  }

  // Sort by date
  result.orders.sort(function(a, b) { return a.date > b.date ? 1 : -1; });
  result.karigarTransactions.sort(function(a, b) { return a.date > b.date ? 1 : -1; });

  return result;
}
