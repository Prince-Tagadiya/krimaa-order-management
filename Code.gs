// KRIMAA ORDER MANAGEMENT v3.0 - MULTI-COMPANY SUPPORT
var SCRIPT_NAME = "Order Management API";

// Company configuration - define your companies here
var COMPANIES = {
  'company1': { name: 'Company 1', accountsSheet: 'Accounts_C1', ordersSheet: 'Orders_C1' },
  'company2': { name: 'Company 2', accountsSheet: 'Accounts_C2', ordersSheet: 'Orders_C2' }
};

// Function to automatically create the required Google Sheets if missing
function setupSheets(companyId) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var company = COMPANIES[companyId];
  if (!company) {
    // Fallback: setup all companies
    for (var cid in COMPANIES) {
      setupSheetsForCompany(ss, COMPANIES[cid]);
    }
    return;
  }
  setupSheetsForCompany(ss, company);
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
    var companyId = data ? (data.companyId || 'company1') : 'company1';
    
    if (action === 'addAccount') {
      return output.setContent(JSON.stringify(addAccount(data.accountName, companyId)));
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
    var companyId = e.parameter.companyId || 'company1';
    
    setupSheets(companyId);
    
    if (action === 'getAccounts') {
      return output.setContent(JSON.stringify(getAccounts(companyId)));
    } else if (action === 'getDashboardData') {
      return output.setContent(JSON.stringify(getDashboardData(companyId)));
    } else if (action === 'getCompanies') {
      return output.setContent(JSON.stringify(getCompaniesInfo()));
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

// Logic to get all accounts from the sheet (sorted by Position column)
function getAccounts(companyId) {
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
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var company = COMPANIES[companyId] || COMPANIES['company1'];
  var sheet = ss.getSheetByName(company.ordersSheet);
  
  if (!sheet) return {success: true, data: []};
  var lastRow = sheet.getLastRow();
  
  var startRow = 1;
  if (lastRow >= 1) {
    var firstCell = sheet.getRange(1, 1).getValue().toString();
    if (firstCell === "Date" || firstCell === "Date / તારીખ") {
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
