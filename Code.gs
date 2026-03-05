// KRIMAA ORDER MANAGEMENT v4.0 - MULTI-COMPANY + EDIT/DELETE ACCOUNTS
var SCRIPT_NAME = "Order Management API";

// Company configuration - define your companies here
var COMPANIES = {
  'company1': { name: 'Company 1', accountsSheet: 'Accounts_C1', ordersSheet: 'Orders_C1' },
  'company2': { name: 'Company 2', accountsSheet: 'Accounts_C2', ordersSheet: 'Orders_C2' }
};
var REMARKS_SHEET = 'Remarks';
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
  _sheetsInitialised[companyId] = true;
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

function setupSheetsForCompany(ss, company) {
  var accountsSheet = ss.getSheetByName(company.accountsSheet);
  if (!accountsSheet) {
    accountsSheet = ss.insertSheet(company.accountsSheet);
    accountsSheet.appendRow(['Account Name', 'Added Date', 'Position']);
    accountsSheet.getRange("A1:C1").setFontWeight("bold");
    accountsSheet.setFrozenRows(1);
  } else {
    // Ensure Position column header exists for older sheets
    var header = accountsSheet.getRange(1, 3).getValue();
    if (header !== 'Position') {
      accountsSheet.getRange(1, 3).setValue('Position').setFontWeight('bold');
    }
  }
  
  var ordersSheet = ss.getSheetByName(company.ordersSheet);
  if (!ordersSheet) {
    ordersSheet = ss.insertSheet(company.ordersSheet);
    ordersSheet.appendRow(['Date', 'Account Name', 'Meesho', 'Flipkart', 'Total']);
    ordersSheet.getRange("A1:E1").setFontWeight("bold");
    ordersSheet.setFrozenRows(1);
  }
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
      return output.setContent(JSON.stringify(getDashboardData(companyId)));
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
      return output.setContent(JSON.stringify(bulkBackup(data.orders, data.accounts, companyId)));
    }
    
    return output.setContent(JSON.stringify({success: false, message: 'Invalid action: ' + action}));
  } catch(error) {
    return output.setContent(JSON.stringify({success: false, message: error.toString()}));
  }
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
      return output.setContent(JSON.stringify(getDashboardData(companyId)));
    } else if (action === 'getCompanies') {
      return output.setContent(JSON.stringify(getCompaniesInfo()));
    } else if (action === 'getRemarks') {
      return output.setContent(JSON.stringify(getRemarks()));
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
    newRows.push([dateStr, order.accountName, order.meesho || 0, order.flipkart || 0, order.total || 0]);
  }
  
  if(newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
  }
  
  return {success: true, message: 'Orders submitted successfully'};
}

// Logic to load all order data for the dashboard
function getDashboardData(companyId) {
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
  var data = sheet.getRange(startRow, 1, lastRow - (startRow - 1), 5).getValues();
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
      flipkart: data[i][3],
      total: data[i][4]
    });
  }
  return {success: true, data: records};
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
  else if (field === 'flipkart') colIndex = 4;
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
      var meesho = (colIndex === 3) ? numVal : (parseInt(data[i][2]) || 0);
      var flipkart = (colIndex === 4) ? numVal : (parseInt(data[i][3]) || 0);
      sheet.getRange(i + 1, 5).setValue(meesho + flipkart);
      return {success: true, message: 'Order updated'};
    }
  }
  
  return {success: false, message: 'Order row not found for ' + dateStr + ' / ' + accountName};
}

// ===== BULK BACKUP (receives Firebase data and writes to Sheets) =====
function bulkBackup(orders, accounts, companyId) {
  companyId = normalizeCompanyId(companyId);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets(companyId);
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  
  // --- Backup Orders ---
  if (orders && orders.length > 0) {
    var ordersSheet = ss.getSheetByName(company.ordersSheet);
    // Clear existing data (keep header)
    var lastRow = ordersSheet.getLastRow();
    if (lastRow > 1) {
      ordersSheet.getRange(2, 1, lastRow - 1, 5).clearContent();
    }
    // Write new data
    var rows = [];
    for (var i = 0; i < orders.length; i++) {
      var o = orders[i];
      rows.push([
        o.date || '',
        o.accountName || '',
        parseInt(o.meesho) || 0,
        parseInt(o.flipkart) || 0,
        parseInt(o.total) || 0
      ]);
    }
    if (rows.length > 0) {
      ordersSheet.getRange(2, 1, rows.length, 5).setValues(rows);
    }
  }
  
  // --- Backup Accounts ---
  if (accounts && accounts.length > 0) {
    var accountsSheet = ss.getSheetByName(company.accountsSheet);
    var accLastRow = accountsSheet.getLastRow();
    if (accLastRow > 1) {
      accountsSheet.getRange(2, 1, accLastRow - 1, 3).clearContent();
    }
    var accRows = [];
    var now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
    for (var j = 0; j < accounts.length; j++) {
      accRows.push([accounts[j], now, j]);
    }
    if (accRows.length > 0) {
      accountsSheet.getRange(2, 1, accRows.length, 3).setValues(accRows);
    }
  }
  
  return {success: true, message: 'Backup completed for ' + company.name + ': ' + (orders ? orders.length : 0) + ' orders'};
}
