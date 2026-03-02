// KRIMAA ORDER MANAGEMENT v2.0 - NO DUPLICATE DATE RESTRICTION - ALLOWS MULTIPLE SUBMISSIONS PER DAY
var SCRIPT_NAME = "Order Management API";

// Function to automatically create the required Google Sheets if missing
function setupSheets() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  
  var accountsSheet = ss.getSheetByName('Accounts');
  if (!accountsSheet) {
    accountsSheet = ss.insertSheet('Accounts');
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
  
  var ordersSheet = ss.getSheetByName('Orders');
  if (!ordersSheet) {
    ordersSheet = ss.insertSheet('Orders');
    ordersSheet.appendRow(['Date', 'Account Name', 'Meesho', 'Flipkart', 'Total']);
    ordersSheet.getRange("A1:E1").setFontWeight("bold");
    ordersSheet.setFrozenRows(1);
  }
}

// Handle all POST requests (Submit orders, Add account, fetch data)
function doPost(e) {
  var output = ContentService.createTextOutput();
  
  // Important: Use text representation of JSON as MimeType to resolve CORS
  output.setMimeType(ContentService.MimeType.JSON);
  
  try {
    var data;
    
    // Important: check postData contents instead of parameters
    if (e.postData) {
        if (typeof e.postData.contents === 'string') {
            data = JSON.parse(e.postData.contents);
        } else {
            data = e.postData.contents;
        }
    }
    
    var action = data ? data.action : null;
    
    if (action === 'addAccount') {
      return output.setContent(JSON.stringify(addAccount(data.accountName)));
    } else if (action === 'submitOrders') {
      return output.setContent(JSON.stringify(submitOrders(data.date, data.orders)));
    } else if (action === 'getAccounts') {
      return output.setContent(JSON.stringify(getAccounts()));
    } else if (action === 'getDashboardData') {
      return output.setContent(JSON.stringify(getDashboardData()));
    } else if (action === 'updateAccountOrder') {
      return output.setContent(JSON.stringify(updateAccountOrder(data.orderedAccounts)));
    }
    
    return output.setContent(JSON.stringify({success: false, message: 'Invalid action: ' + action}));
  } catch(error) {
    return output.setContent(JSON.stringify({success: false, message: error.toString()}));
  }
}

// Handle all GET requests (Fetch accounts, Fetch dashboard info)
function doGet(e) {
  var output = ContentService.createTextOutput();
  output.setMimeType(ContentService.MimeType.JSON);
  
  try {
    setupSheets();
    var action = e.parameter.action;
    
    if (action === 'getAccounts') {
      return output.setContent(JSON.stringify(getAccounts()));
    } else if (action === 'getDashboardData') {
      return output.setContent(JSON.stringify(getDashboardData()));
    }
    
    return output.setContent(JSON.stringify({success: false, message: 'Invalid GET action'}));
  } catch(error) {
    return output.setContent(JSON.stringify({success: false, message: error.toString()}));
  }
}

// Logic to add a new account to the sheet
function addAccount(accountName) {
  if (!accountName || accountName.trim() === '') {
     return {success: false, message: 'Account name cannot be empty'};
  }
  
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets();
  var sheet = ss.getSheetByName('Accounts');
  
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
function getAccounts() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName('Accounts');
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
function updateAccountOrder(orderedAccounts) {
  if (!orderedAccounts || orderedAccounts.length === 0) {
    return {success: false, message: 'No accounts provided'};
  }
  
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets();
  var sheet = ss.getSheetByName('Accounts');
  var data = sheet.getDataRange().getValues();
  
  var startIdx = (data[0][0] === 'Account Name') ? 1 : 0;
  
  // Build a map: accountName -> row index (1-based for sheet)
  for (var i = startIdx; i < data.length; i++) {
    var accountName = data[i][0].toString().trim();
    var posIndex = orderedAccounts.indexOf(accountName);
    if (posIndex !== -1) {
      // Write position (0-based index) to column C of this row
      sheet.getRange(i + 1, 3).setValue(posIndex);
    }
  }
  
  return {success: true, message: 'Account order updated successfully'};
}

// Logic to submit daily orders
function submitOrders(dateStr, orders) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  setupSheets();
  var sheet = ss.getSheetByName('Orders');
  var data = sheet.getDataRange().getValues();
  
  if (!orders || orders.length === 0) {
    return {success: false, message: 'No orders provided'};
  }
  
  var newRows = [];
  for (var j = 0; j < orders.length; j++) {
    var order = orders[j];
    newRows.push([dateStr, order.accountName, order.meesho || 0, order.flipkart || 0, order.total || 0]);
  }
  
  // Efficient batch insert
  if(newRows.length > 0) {
     var lastRow = sheet.getLastRow();
     sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
  }
  
  return {success: true, message: 'Orders submitted successfully'};
}

// Logic to load all order data for the dashboard
function getDashboardData() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName('Orders');
  
  if (!sheet) return {success: true, data: []};
  var lastRow = sheet.getLastRow();
  
  // Smart Header Detection
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
