/**
 * Highly optimized script to sync Airtable base metadata
 * Efficiently processes bases, tables, fields, and views with minimal API calls
 */

const apiKey = PropertiesService.getScriptProperties().getProperty("airtableId");
const headers = {
  "Authorization": "Bearer " + apiKey,
  "Content-Type": "application/json"
};

const headersWithTypecast = {
  "Authorization": "Bearer " + apiKey,
  "Content-Type": "application/json"
};

const targetBaseId = "app4xfaklobVjLusr";
const baseMetaUrl = "https://api.airtable.com/v0/meta/bases";
const baseTableUrl = "https://api.airtable.com/v0/app4xfaklobVjLusr/tbl9iqLQftc3EsgNw";
const tableTableUrl = "https://api.airtable.com/v0/app4xfaklobVjLusr/tblwFYv49fZDUfyXE";
const fieldTableUrl = "https://api.airtable.com/v0/app4xfaklobVjLusr/tblLcwcFbjLyQXdl6";
const viewTableUrl = "https://api.airtable.com/v0/app4xfaklobVjLusr/tblLm8VJO7A6AGyKB";

// Constants for optimization
const MAX_BATCH_SIZE = 10; // Airtable now only allows up to 10 records per batch (previously was 50)
const MAX_FORMULA_LENGTH = 90000; // Airtable formula character limit (~100KB)

function upsertAirtableBasesAndTables() {
  Logger.log("Starting optimized Airtable base sync process...");
  const timestamp = new Date().toISOString();
  
  try {
    // Step 1: Find the target base in the list of bases
    // This approach is more reliable than directly accessing the base metadata
    Logger.log("Locating target base in base list...");
    const targetBase = findTargetBase(targetBaseId);
    
    if (!targetBase) {
      Logger.log("Target base not found or insufficient permissions. Aborting.");
      return;
    }
    
    // Step 2: Prepare and upsert base record
    Logger.log("Found target base: " + targetBase.name);
    const baseRecord = {
      "fields": {
        "Base ID": targetBase.id,
        "Name": targetBase.name,
        "Last Updated": timestamp,
        "Application": ["recSxsOB2DkPmiCqW"]
      }
    };
    
    // Upsert base record
    Logger.log("Upserting base record...");
    upsertRecords(baseTableUrl, [baseRecord], "Base ID", headers);
    
    // Step 3: Get the Airtable record ID for this base
    const baseRecordId = getBaseRecordIdOptimized(targetBaseId);
    if (!baseRecordId) {
      Logger.log("Could not find base record ID after upsert, aborting");
      return;
    }
    
    // Step 4: Get tables for this base
    Logger.log("Fetching tables for base...");
    const tablesUrl = baseMetaUrl + "/" + targetBaseId + "/tables";
    const tablesResponse = UrlFetchApp.fetch(tablesUrl, { 
      method: "get", 
      headers: headers,
      muteHttpExceptions: true
    });
    
    if (tablesResponse.getResponseCode() !== 200) {
      Logger.log("Error fetching tables: " + tablesResponse.getContentText());
      return;
    }
    
    const tablesData = JSON.parse(tablesResponse.getContentText());
    if (!tablesData.tables || tablesData.tables.length === 0) {
      Logger.log("No tables found for base");
      return;
    }
    
    Logger.log("Found " + tablesData.tables.length + " tables in base");
    
    // Step 5: Process all tables, fields, and views in an optimized way
    processAllTablesBulk(tablesData.tables, baseRecordId, timestamp);
    
    // Step 6: Final cleanup - delete records in specified views
    Logger.log("Deleting records in specified views...");
    deleteRecordsInView("app4xfaklobVjLusr", "tblwFYv49fZDUfyXE", "viwfIWcsR2RxSyXyE");
    deleteRecordsInView("app4xfaklobVjLusr", "tblLm8VJO7A6AGyKB", "viwCgvSnviMd6Za8T");
    
    // New Step 7: Delete all records in the specified view
    Logger.log("Deleting all records in viwVRkZdlC7dbTHYC...");
    deleteRecordsInView("app4xfaklobVjLusr", "tblLcwcFbjLyQXdl6", "viwVRkZdlC7dbTHYC");
    
    Logger.log("Airtable base sync completed successfully!");
    
  } catch (e) {
    Logger.log("Critical error in sync process: " + e.toString());
  }
}

/**
 * Find the target base in the list of bases
 */
function findTargetBase(baseId) {
  let url = baseMetaUrl;
  
  try {
    // Iterate through pages of bases until we find our target
    while (url) {
      const response = UrlFetchApp.fetch(url, { 
        method: "get", 
        headers: headers,
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() !== 200) {
        Logger.log("Error fetching bases: " + response.getContentText());
        return null;
      }
      
      const data = JSON.parse(response.getContentText());
      
      if (data.bases) {
        // Look for our target base in this batch
        const targetBase = data.bases.find(base => base.id === baseId);
        if (targetBase) {
          return targetBase;
        }
      }
      
      // Move to next page if we haven't found our target
      url = data.offset ? (baseMetaUrl + "?offset=" + data.offset) : null;
    }
    
    Logger.log("Target base not found in available bases list");
    return null;
  } catch (err) {
    Logger.log("Error finding target base: " + err.toString());
    return null;
  }
}

/**
 * Optimized version of getBaseRecordId that uses a direct lookup
 */
function getBaseRecordIdOptimized(baseId) {
  try {
    const filterFormula = encodeURIComponent("{Base ID}=\"" + baseId + "\"");
    const url = baseTableUrl + "?filterByFormula=" + filterFormula + "&fields%5B%5D=Base%20ID";
    
    const response = UrlFetchApp.fetch(url, { 
      method: "get", 
      headers: headers,
      muteHttpExceptions: true
    });
    
    if (response.getResponseCode() !== 200) {
      Logger.log("Error in base record lookup: " + response.getContentText());
      return null;
    }
    
    const data = JSON.parse(response.getContentText());
    
    if (data.records && data.records.length > 0) {
      return data.records[0].id;
    }
    
    Logger.log("No base record found with ID: " + baseId);
    return null;
  } catch (err) {
    Logger.log("Error in base record lookup: " + err.toString());
    return null;
  }
}

/**
 * Process all tables, fields and views in the most efficient way possible
 */
function processAllTablesBulk(tables, baseRecordId, timestamp) {
  // Step 1: Prepare all table records for bulk upsert
  const tableRecords = tables.map(table => ({
    "fields": {
      "Table ID": table.id,
      "Name": table.name,
      "Last Updated": timestamp,
      "Base": baseRecordId ? [baseRecordId] : [],
      "Application": ["recSxsOB2DkPmiCqW"]
    }
  }));
  
  // Step 2: Upsert all table records in optimized batches
  Logger.log(`Upserting ${tableRecords.length} table records in batches of ${MAX_BATCH_SIZE}...`);
  upsertRecordsBulk(tableTableUrl, tableRecords, "Table ID", headers);
  
  // Step 3: Get all table record IDs in an optimized operation
  Logger.log("Getting table record IDs...");
  const tableIds = tables.map(t => t.id);
  const tableRecordMap = getTableRecordMapOptimized(tableIds);
  
  // Step 4: Pre-process field and view data to minimize memory usage
  // Process tables in batches to avoid memory issues with large bases
  const tableBatchSize = 20; // Process 20 tables at a time to manage memory
  const totalTableBatches = Math.ceil(tables.length / tableBatchSize);
  
  for (let tableBatchIndex = 0; tableBatchIndex < tables.length; tableBatchIndex += tableBatchSize) {
    const currentBatchNumber = Math.floor(tableBatchIndex / tableBatchSize) + 1;
    const tableBatch = tables.slice(tableBatchIndex, tableBatchIndex + tableBatchSize);
    Logger.log(`Processing fields and views for table batch ${currentBatchNumber}/${totalTableBatches}...`);
    
    const batchFieldRecords = [];
    const batchViewRecords = [];
    
    // Prepare all fields and views for this batch of tables
    tableBatch.forEach(table => {
      const tableRecordId = tableRecordMap[table.id];
      if (!tableRecordId) {
        Logger.log(`Table record ID not found for table: ${table.id}, skipping fields and views`);
        return;
      }
      
      // Add all fields for this table to the batch
      if (table.fields && table.fields.length > 0) {
        const tableFieldRecords = table.fields.map(field => ({
          "fields": {
            "Field ID": field.id,
            "Name": field.name,
            "Field Type": field.type,
            "Field Type Lookup": field.type, // Add field type lookup with typecast
            "Last Updated": timestamp,
            "Table": [tableRecordId],
            "Application": ["recSxsOB2DkPmiCqW"]
          }
        }));
        
        batchFieldRecords.push(...tableFieldRecords);
      }
      
      // Add all views for this table to the batch
      if (table.views && table.views.length > 0) {
        const tableViewRecords = table.views.map(view => ({
          "fields": {
            "View ID": view.id,
            "Name": view.name,
            "View Type": view.type,
            "Last Updated": timestamp,
            "Table": [tableRecordId],
            "Application": ["recSxsOB2DkPmiCqW"]
          }
        }));
        
        batchViewRecords.push(...tableViewRecords);
      }
    });
    
    // Process fields for this batch of tables with typecast enabled
    if (batchFieldRecords.length > 0) {
      Logger.log(`Upserting ${batchFieldRecords.length} field records from table batch ${currentBatchNumber}...`);
      upsertRecordsBulkWithTypecast(fieldTableUrl, batchFieldRecords, "Field ID", headersWithTypecast);
    }
    
    // Process views for this batch of tables
    if (batchViewRecords.length > 0) {
      Logger.log(`Upserting ${batchViewRecords.length} view records from table batch ${currentBatchNumber}...`);
      upsertRecordsBulk(viewTableUrl, batchViewRecords, "View ID", headers);
    }
    
    // Free memory by removing references to processed data
    if (tableBatchIndex + tableBatchSize < tables.length) {
      Logger.log(`Completed batch ${currentBatchNumber}/${totalTableBatches}. Moving to next batch...`);
      Utilities.sleep(100); // Brief pause between batches
    }
  }
  
  Logger.log("All tables, fields, and views processed successfully.");
}

/**
 * Get table record IDs with minimal API calls by using chunked OR formulas
 * with URL length monitoring to avoid exceeding limits
 */
function getTableRecordMapOptimized(tableIds) {
  const result = {};
  
  if (!tableIds || tableIds.length === 0) {
    return result;
  }
  
  Logger.log(`Optimizing lookup for ${tableIds.length} table IDs...`);
  
  // Use a hybrid approach - start with small batches and increase if successful
  // Start with a conservative batch size to ensure success
  let optimalBatchSize = 5;
  const maxUrlLength = 16000; // Safe URL length limit (actual limit is around 16-20K)
  
  // Process tables in adaptive batches
  for (let i = 0; i < tableIds.length; i += optimalBatchSize) {
    const batchIds = tableIds.slice(i, i + optimalBatchSize);
    const formulaParts = batchIds.map(id => `{Table ID}="${id}"`);
    const formula = formulaParts.length > 1 ? `OR(${formulaParts.join(",")})` : formulaParts[0];
    const encodedFormula = encodeURIComponent(formula);
    
    // Check if URL would be too long and reduce batch size if needed
    const urlWithFormula = `${tableTableUrl}?filterByFormula=${encodedFormula}&fields%5B%5D=Table%20ID`;
    
    if (urlWithFormula.length > maxUrlLength && optimalBatchSize > 1) {
      // URL too long, reduce batch size and retry this batch
      optimalBatchSize = Math.max(1, Math.floor(optimalBatchSize / 2));
      Logger.log(`URL length exceeded, reducing batch size to ${optimalBatchSize}`);
      i -= optimalBatchSize; // Retry this batch with smaller size
      continue;
    }
    
    try {
      const response = UrlFetchApp.fetch(urlWithFormula, { 
        method: "get", 
        headers: headers,
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() !== 200) {
        if (response.getResponseCode() === 414 || response.getResponseCode() === 413 || 
            response.getContentText().includes("URL") || response.getContentText().includes("uri")) {
          // URL too long error, reduce batch size and retry
          optimalBatchSize = Math.max(1, Math.floor(optimalBatchSize / 2));
          Logger.log(`URL length error (${response.getResponseCode()}), reducing batch size to ${optimalBatchSize}`);
          i -= optimalBatchSize; // Retry this batch with smaller size
          continue;
        }
        
        Logger.log(`Error in table lookup batch ${Math.floor(i/optimalBatchSize) + 1}: ${response.getContentText()}`);
        continue;
      }
      
      const data = JSON.parse(response.getContentText());
      
      if (data.records && data.records.length > 0) {
        data.records.forEach(record => {
          if (record.fields["Table ID"]) {
            result[record.fields["Table ID"]] = record.id;
          }
        });
        
        // If this succeeded with current batch size and we're below 10, 
        // we can cautiously increase batch size for future batches
        if (optimalBatchSize < 10 && data.records.length === batchIds.length) {
          optimalBatchSize = Math.min(10, optimalBatchSize + 1);
        }
      }
      
      // Log progress at reasonable intervals
      const processedCount = Math.min(i + optimalBatchSize, tableIds.length);
      if (i % 20 === 0 || processedCount === tableIds.length) {
        Logger.log(`Processed ${processedCount}/${tableIds.length} tables with batch size ${optimalBatchSize}, found ${Object.keys(result).length} IDs`);
      }
      
    } catch (err) {
      if (err.toString().includes("URL") || err.toString().includes("Length")) {
        // URL length error, reduce batch size and retry
        optimalBatchSize = Math.max(1, Math.floor(optimalBatchSize / 2));
        Logger.log(`URL length exception, reducing batch size to ${optimalBatchSize}`);
        i -= optimalBatchSize; // Retry this batch with smaller size
        continue;
      }
      
      Logger.log(`Error in table lookup batch ${Math.floor(i/optimalBatchSize) + 1}: ${err.toString()}`);
    }
    
    // Small delay to avoid rate limits
    if (i + optimalBatchSize < tableIds.length) {
      Utilities.sleep(50);
    }
  }
  
  Logger.log(`Retrieved ${Object.keys(result).length}/${tableIds.length} table record IDs`);
  return result;
}

/**
 * Optimized function for upserting records with maximum batch size and typecast
 */
function upsertRecordsBulkWithTypecast(url, records, mergeField, headers) {
  if (!records || records.length === 0) {
    return;
  }
  
  // Use maximum allowed batch size for efficiency
  const totalBatches = Math.ceil(records.length / MAX_BATCH_SIZE);
  
  for (let i = 0; i < records.length; i += MAX_BATCH_SIZE) {
    const batch = records.slice(i, i + MAX_BATCH_SIZE);
    const batchNumber = Math.floor(i / MAX_BATCH_SIZE) + 1;
    
    const payload = JSON.stringify({
      "performUpsert": {
        "fieldsToMergeOn": [mergeField]
      },
      "records": batch,
      "typecast": true
    });

    try {
      const response = UrlFetchApp.fetch(url, { 
        method: "PATCH", 
        headers: headers, 
        payload: payload,
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() >= 400) {
        Logger.log(`Error upserting batch with typecast ${batchNumber}: ${response.getContentText()}`);
      } else {
        Logger.log(`Successfully upserted batch with typecast ${batchNumber}/${totalBatches}, ${batch.length} records`);
      }
    } catch (err) {
      Logger.log(`Critical error upserting batch with typecast ${batchNumber}: ${err.toString()}`);
    }

    // Small delay between batches to prevent rate limiting
    if (i + MAX_BATCH_SIZE < records.length) {
      Utilities.sleep(100);
    }
  }
}

/**
 * Optimized function for upserting records with maximum batch size
 */
function upsertRecordsBulk(url, records, mergeField, headers) {
  if (!records || records.length === 0) {
    return;
  }
  
  // Use maximum allowed batch size for efficiency
  const totalBatches = Math.ceil(records.length / MAX_BATCH_SIZE);
  
  for (let i = 0; i < records.length; i += MAX_BATCH_SIZE) {
    const batch = records.slice(i, i + MAX_BATCH_SIZE);
    const batchNumber = Math.floor(i / MAX_BATCH_SIZE) + 1;
    
    const payload = JSON.stringify({
      "performUpsert": {
        "fieldsToMergeOn": [mergeField]
      },
      "records": batch
    });

    try {
      const response = UrlFetchApp.fetch(url, { 
        method: "PATCH", 
        headers: headers, 
        payload: payload,
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() >= 400) {
        Logger.log(`Error upserting batch ${batchNumber}: ${response.getContentText()}`);
      } else {
        Logger.log(`Successfully upserted batch ${batchNumber}/${totalBatches}, ${batch.length} records`);
      }
    } catch (err) {
      Logger.log(`Critical error upserting batch ${batchNumber}: ${err.toString()}`);
    }

    // Small delay between batches to prevent rate limiting
    if (i + MAX_BATCH_SIZE < records.length) {
      Utilities.sleep(100);
    }
  }
}

/**
 * Legacy upsert function for backward compatibility
 */
function upsertRecords(url, records, mergeField, headers) {
  return upsertRecordsBulk(url, records, mergeField, headers);
}

/**
 * Helper function to chunk formula parts by length
 */
function chunkFormulasByLength(formulaParts, maxLength) {
  const chunks = [];
  let currentChunk = [];
  let currentLength = 0;
  
  for (const part of formulaParts) {
    // Check if adding this part would exceed the max length
    const newLength = currentLength + part.length + (currentChunk.length > 0 ? 1 : 0); // +1 for comma
    
    if (newLength > maxLength || currentChunk.length >= 50) { // 50 is a safe batch size
      chunks.push(currentChunk);
      currentChunk = [part];
      currentLength = part.length;
    } else {
      currentChunk.push(part);
      currentLength = newLength;
    }
  }
  
  if (currentChunk.length > 0) {
    chunks.push(currentChunk);
  }
  
  return chunks;
}

/**
 * Function to delete records in a specific view
 */
function deleteRecordsInView(baseId, tableId, viewId) {
  try {
    Logger.log(`Deleting records in view ${viewId}...`);
    
    // Build the URL to fetch records from the view
    const url = `https://api.airtable.com/v0/${baseId}/${tableId}?view=${viewId}`;
    
    // Fetch records in the view
    const response = UrlFetchApp.fetch(url, {
      method: "get",
      headers: headers,
      muteHttpExceptions: true
    });
    
    if (response.getResponseCode() !== 200) {
      Logger.log(`Error fetching records for view ${viewId}: ${response.getContentText()}`);
      return;
    }
    
    const data = JSON.parse(response.getContentText());
    
    if (!data.records || data.records.length === 0) {
      Logger.log(`No records found in view ${viewId}`);
      return;
    }
    
    // Extract record IDs
    const recordIds = data.records.map(record => record.id);
    Logger.log(`Found ${recordIds.length} records to delete in view ${viewId}`);
    
    // Delete records in smaller batches of 10
    const deleteUrl = `https://api.airtable.com/v0/${baseId}/${tableId}`;
    const totalBatches = Math.ceil(recordIds.length / MAX_BATCH_SIZE);
    
    for (let i = 0; i < recordIds.length; i += MAX_BATCH_SIZE) {
      const batch = recordIds.slice(i, i + MAX_BATCH_SIZE);
      const batchNumber = Math.floor(i / MAX_BATCH_SIZE) + 1;
      
      // For delete operations, we need a different format
      // The 'deleted: true' property indicates we want to delete these records
      const deletePayload = {
        records: batch.map(id => ({ id, deleted: true }))
      };
      
      const deleteResponse = UrlFetchApp.fetch(deleteUrl, {
        method: "patch",
        headers: headers,
        payload: JSON.stringify(deletePayload),
        muteHttpExceptions: true
      });
      
      if (deleteResponse.getResponseCode() >= 400) {
        Logger.log(`Error deleting batch in view ${viewId}: ${deleteResponse.getContentText()}`);
      } else {
        Logger.log(`Successfully deleted batch ${batchNumber}/${totalBatches}, ${batch.length} records`);
      }
      
      // Small delay between batches
      if (i + MAX_BATCH_SIZE < recordIds.length) {
        Utilities.sleep(100);
      }
    }
    
    Logger.log(`Completed deletion of records in view ${viewId}`);
    
  } catch (err) {
    Logger.log(`Error deleting records in view ${viewId}: ${err.toString()}`);
  }
}