/**
 * Merged sync script that runs all scripts in sequence
 * 1. AirtableTracker runs first to sync Airtable base metadata
 * 2. Zoho-Airtable(Sync) syncs Zoho CRM modules and fields
 * 3. ZohoBooksSync syncs Zoho Books entities and fields
 * 4. ZohoProjectsSync syncs Zoho Projects task fields
 * 
 * Script Properties Required:
 * - airtableId / airtableApiKey
 * - clientIdCRM, clientSecretCRM, refreshTokenCRM
 * - clientIdBooks, clientSecretBooks, refreshTokenBooks
 * - clientIdProjects, clientSecretProjects, refreshTokenProjects
 */

function runMergedSyncScript() {
  Logger.log("=== Starting Merged Sync Script ===");
  
  try {
    // Step 1: Run AirtableTracker first
    Logger.log("Step 1: Running AirtableTracker...");
    upsertAirtableBasesAndTables();
    Logger.log("Step 1: AirtableTracker completed successfully");
    
    // Small delay between scripts
    Utilities.sleep(2000);
    
    // Step 2: Run Zoho CRM to Airtable sync
    Logger.log("Step 2: Running Zoho CRM to Airtable sync...");
    syncZohoModulesToAirtable();
    Logger.log("Step 2: Zoho CRM sync completed successfully");
    
    // Small delay between scripts
    Utilities.sleep(2000);
    
    // Step 3: Run Zoho Books sync
    Logger.log("Step 3: Running Zoho Books sync...");
    fetchFieldsFromZohoBooks();
    Logger.log("Step 3: Zoho Books sync completed successfully");
    
    // Small delay between scripts
    Utilities.sleep(2000);
    
    // Step 4: Run Zoho Projects sync
    Logger.log("Step 4: Running Zoho Projects sync...");
    runStandaloneZohoTaskFetch();
    Logger.log("Step 4: Zoho Projects sync completed successfully");
    
    Logger.log("=== All sync scripts completed successfully ===");
    
  } catch (error) {
    Logger.log("=== Error in merged sync script ===");
    Logger.log("Error: " + error.toString());
    Logger.log("Stack trace: " + error.stack);
    throw error;
  }
}

// ===== AIRTABLE TRACKER FUNCTIONS =====

/**
 * Highly optimized script to sync Airtable base metadata
 * Efficiently processes bases, tables, fields, and views with minimal API calls
 */

const apiKey = PropertiesService.getScriptProperties().getProperty("airtableApiKey") || PropertiesService.getScriptProperties().getProperty("airtableId");
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
 * Search for matching Zoho fields by name to auto-link them (case-insensitive)
 */
function findMatchingZohoField(fieldName, headers) {
  try {
    const zohoFieldsTableId = 'tbl0JfUjWhV4TvLz2'; // Zoho Fields table
    // Use UPPER() function for case-insensitive matching
    const filterFormula = encodeURIComponent(`UPPER({api_name})=UPPER('${fieldName.replace(/'/g, "\\'")}')`);
    const url = `https://api.airtable.com/v0/${targetBaseId}/${zohoFieldsTableId}?filterByFormula=${filterFormula}&fields%5B%5D=api_name&maxRecords=1`;
    
    const response = UrlFetchApp.fetch(url, { 
      method: "get", 
      headers: headers,
      muteHttpExceptions: true
    });
    
    if (response.getResponseCode() !== 200) {
      return null;
    }
    
    const data = JSON.parse(response.getContentText());
    
    if (data.records && data.records.length > 0) {
      Logger.log(`Found matching Zoho field for ${fieldName}: ${data.records[0].id}`);
      return data.records[0].id;
    }
    
    return null;
  } catch (err) {
    Logger.log(`Error searching for Zoho field ${fieldName}: ${err.toString()}`);
    return null;
  }
}

/**
 * Get existing field records with their Zoho field links
 */
function getExistingFieldsWithZohoLinks(fieldIds, headers) {
  const result = {};
  
  if (!fieldIds || fieldIds.length === 0) {
    return result;
  }
  
  try {
    // Process field IDs in batches
    for (let i = 0; i < fieldIds.length; i += 10) {
      const batchIds = fieldIds.slice(i, i + 10);
      const formulaParts = batchIds.map(id => `{Field ID}="${id}"`);
      const formula = formulaParts.length > 1 ? `OR(${formulaParts.join(",")})` : formulaParts[0];
      const encodedFormula = encodeURIComponent(formula);
      
      const url = `${fieldTableUrl}?filterByFormula=${encodedFormula}&fields%5B%5D=Field%20ID&fields%5B%5D=Zoho%20Fields`;
      
      const response = UrlFetchApp.fetch(url, { 
        method: "get", 
        headers: headers,
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() === 200) {
        const data = JSON.parse(response.getContentText());
        
        if (data.records) {
          data.records.forEach(record => {
            const fieldId = record.fields["Field ID"];
            const zohoFields = record.fields["Zoho Fields"] || [];
            if (fieldId && zohoFields.length > 0) {
              result[fieldId] = zohoFields;
            }
          });
        }
      }
    }
  } catch (err) {
    Logger.log(`Error fetching existing field Zoho links: ${err.toString()}`);
  }
  
  return result;
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
  
  // Track which Zoho fields have already been linked to prevent duplicates
  const usedZohoFieldIds = new Set();
  
  // First, get all existing field records to check which Zoho fields are already linked
  try {
    Logger.log("Fetching existing field records to check Zoho field links...");
    let offset = null;
    let hasMore = true;
    
    while (hasMore) {
      const url = fieldTableUrl + (offset ? `?offset=${offset}` : '');
      const response = UrlFetchApp.fetch(url, { 
        method: "get", 
        headers: headers,
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() === 200) {
        const data = JSON.parse(response.getContentText());
        
        // Check each existing field record for Zoho field links
        if (data.records) {
          data.records.forEach(record => {
            if (record.fields["Zoho Fields"] && record.fields["Zoho Fields"].length > 0) {
              record.fields["Zoho Fields"].forEach(zohoFieldId => {
                usedZohoFieldIds.add(zohoFieldId);
              });
            }
          });
        }
        
        // Check if there are more records
        offset = data.offset;
        hasMore = !!offset;
      } else {
        Logger.log("Error fetching existing field records: " + response.getContentText());
        hasMore = false;
      }
    }
    
    Logger.log(`Found ${usedZohoFieldIds.size} Zoho fields already linked in existing records`);
  } catch (error) {
    Logger.log("Error checking existing Zoho field links: " + error.toString());
  }
  
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
        // First, get existing field records to check their current Zoho field links
        const fieldIds = table.fields.map(f => f.id);
        const existingFieldsWithLinks = getExistingFieldsWithZohoLinks(fieldIds, headers);
        
        const tableFieldRecords = table.fields.map(field => {
          const fieldRecord = {
            "fields": {
              "Field ID": field.id,
              "Name": field.name,
              "Field Type": field.type,
              "Field Type Lookup": field.type, // Add field type lookup with typecast
              "Last Updated": timestamp,
              "Table": [tableRecordId],
              "Application": ["recSxsOB2DkPmiCqW"]
            }
          };
          
          // Check if this field already has a Zoho field link
          const existingZohoLinks = existingFieldsWithLinks[field.id];
          if (existingZohoLinks && existingZohoLinks.length > 0) {
            // Preserve existing Zoho field links
            fieldRecord.fields["Zoho Fields"] = existingZohoLinks;
            existingZohoLinks.forEach(linkId => usedZohoFieldIds.add(linkId));
            Logger.log(`Preserving existing Zoho field links for ${field.name}: ${existingZohoLinks.join(', ')}`);
          } else {
            // Only search for new matching if no existing link
            const matchingZohoFieldId = findMatchingZohoField(field.name, headers);
            
            // Add Zoho Fields link if match found and not already used
            if (matchingZohoFieldId && !usedZohoFieldIds.has(matchingZohoFieldId)) {
              fieldRecord.fields["Zoho Fields"] = [matchingZohoFieldId];
              usedZohoFieldIds.add(matchingZohoFieldId);
              Logger.log(`Linked field ${field.name} to Zoho field ${matchingZohoFieldId}`);
            } else if (matchingZohoFieldId && usedZohoFieldIds.has(matchingZohoFieldId)) {
              Logger.log(`Skipped duplicate link: Zoho field ${matchingZohoFieldId} already linked to another field`);
            }
          }
          
          return fieldRecord;
        });
        
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
      
      // For delete operations, use query parameters with record IDs
      const deleteUrlWithIds = deleteUrl + "?" + batch.map(id => `records[]=${id}`).join("&");
      
      const deleteResponse = UrlFetchApp.fetch(deleteUrlWithIds, {
        method: "DELETE",
        headers: headers,
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

// ===== ZOHO CRM TO AIRTABLE SYNC FUNCTIONS =====

/**
 * Script to sync Zoho CRM modules and their fields into Airtable
 * Updates or inserts each module and its fields with full JSON
 */

function syncZohoModulesToAirtable() {
  const airtablePAT = PropertiesService.getScriptProperties().getProperty('airtableApiKey');
  const clientId = PropertiesService.getScriptProperties().getProperty('clientIdCRM');
  const clientSecret = PropertiesService.getScriptProperties().getProperty('clientSecretCRM');
  const refreshToken = PropertiesService.getScriptProperties().getProperty('refreshTokenCRM');

  const airtableBaseId = 'app4xfaklobVjLusr';
  const moduleTableId = 'tbl2HlEPyESvXUXHN';
  const fieldTableId = 'tbl0JfUjWhV4TvLz2';
  const moduleUrl = `https://api.airtable.com/v0/${airtableBaseId}/${moduleTableId}`;
  const fieldUrl = `https://api.airtable.com/v0/${airtableBaseId}/${fieldTableId}`;

  const headers = {
    'Authorization': 'Bearer ' + airtablePAT,
    'Content-Type': 'application/json'
  };

  const headersWithTypecast = {
    'Authorization': 'Bearer ' + airtablePAT,
    'Content-Type': 'application/json'
  };

  console.log('Fetching Zoho access token...');
  const accessToken = getZohoAccessToken(clientId, clientSecret, refreshToken);
  console.log('Fetching modules from Zoho CRM...');
  const modules = getZohoModules(accessToken);
  console.log(`Fetched ${modules.length} modules.`);

  const filterFormulas = modules.map(m => `{api_name}='${m.api_name}'`);
  const existingModuleMap = {};

  // Fetch existing modules in batches
  for (let i = 0; i < filterFormulas.length; i += 10) {
    const batch = filterFormulas.slice(i, i + 10);
    const formula = `OR(${batch.join(',')})`;
    const response = UrlFetchApp.fetch(`${moduleUrl}?filterByFormula=${encodeURIComponent(formula)}&pageSize=100`, { headers });
    const data = JSON.parse(response.getContentText());
    for (const record of data.records) {
      existingModuleMap[record.fields.api_name] = record.id;
    }
  }

  const moduleUpdates = [];
  const moduleCreates = [];
  const timestamp = new Date().toISOString();

  // First pass: Handle modules and collect field data
  const moduleFieldsMap = {}; // Store fields by module
  
  modules.forEach(module => {
    const apiName = module.api_name;
    const moduleJson = JSON.stringify(module);

    const moduleFields = {
      'api_name': apiName,
      'JSON': moduleJson,
      'Last Automation Time': timestamp,
      'Application': ['recQ0e0nQ4kI50kcc']
    };

    // Add to update or create array
    if (existingModuleMap[apiName]) {
      moduleUpdates.push({ id: existingModuleMap[apiName], fields: moduleFields });
    } else {
      moduleCreates.push({ fields: moduleFields });
    }
    
    // Get fields for this module
    const zohoFields = getZohoModuleFields(accessToken, apiName);
    if (zohoFields.length) {
      moduleFieldsMap[apiName] = zohoFields;
    }
  });

  // Process module updates and creates
  if (moduleUpdates.length > 0) {
    console.log(`Updating ${moduleUpdates.length} existing modules...`);
    for (let i = 0; i < moduleUpdates.length; i += 10) {
      const payload = JSON.stringify({ records: moduleUpdates.slice(i, i + 10) });
      UrlFetchApp.fetch(moduleUrl, { method: 'PATCH', headers, payload });
    }
  }

  if (moduleCreates.length > 0) {
    console.log(`Creating ${moduleCreates.length} new modules...`);
    for (let i = 0; i < moduleCreates.length; i += 10) {
      const payload = JSON.stringify({ records: moduleCreates.slice(i, i + 10) });
      const response = UrlFetchApp.fetch(moduleUrl, { method: 'POST', headers, payload });
      const data = JSON.parse(response.getContentText());
      data.records.forEach((rec, idx) => {
        const fields = moduleCreates[i + idx].fields;
        existingModuleMap[fields.api_name] = rec.id;
      });
    }
  }

  // Now process fields by module
  for (const [moduleApiName, fields] of Object.entries(moduleFieldsMap)) {
    const moduleId = existingModuleMap[moduleApiName];
    if (!moduleId) {
      console.warn(`Module ID not found for ${moduleApiName}, skipping fields`);
      continue;
    }
    
    // Create upsertion keys for all fields in this module
    const upsertionKeys = fields.map(field => `${moduleApiName}-${field.api_name}`);
    console.log(`Processing ${upsertionKeys.length} fields for module ${moduleApiName}`);
    
    // Batch lookup existing fields by Upsertion Check in efficient batches
    const existingFieldMap = {};
    
    for (let i = 0; i < upsertionKeys.length; i += 10) {
      const keyBatch = upsertionKeys.slice(i, i + 10);
      const formula = `OR(${keyBatch.map(key => `{Upsertion Check}='${key}'`).join(',')})`;      
      try {
        const response = UrlFetchApp.fetch(`${fieldUrl}?filterByFormula=${encodeURIComponent(formula)}`, { headers });
        const data = JSON.parse(response.getContentText());
        
        for (const record of data.records) {
          if (record.fields['Upsertion Check']) {
            existingFieldMap[record.fields['Upsertion Check']] = record.id;
          }
        }
      } catch (error) {
        console.warn(`Error fetching fields with upsertion keys: ${error.message}`);
        // Continue with next batch if one fails
      }
    }
    
    // Search for matching fields in tblLcwcFbjLyQXdl6 by UI name
    const matchingFieldsMap = searchForMatchingFields(fields, moduleApiName, headers);
    
    // Prepare updates and creates for this module's fields
    const fieldUpdates = [];
    const fieldCreates = [];
    
    fields.forEach(field => {
      const fieldApiName = field.api_name;
      const fieldJson = JSON.stringify(field);
      const upsertionKey = `${moduleApiName}-${fieldApiName}`;
      
      // Extract the UI name from the field data
      const uiName = field.display_label || field.field_label || fieldApiName;
      
      // Extract the Zoho field type
      const zohoFieldType = field.data_type || '';
      
      if (existingFieldMap[upsertionKey]) {
        // Field exists - update timestamp, JSON, UI Name, and Zoho Field Type
        fieldUpdates.push({
          id: existingFieldMap[upsertionKey],
          fields: {
            'Last Automation Time': timestamp,
            'JSON': fieldJson,
            'UI Name': uiName,
            'Zoho Field Type': zohoFieldType
          }
        });
      } else {
        // Check if we found a matching field in tblLcwcFbjLyQXdl6
        const matchingFieldId = matchingFieldsMap[uiName];
        
        if (matchingFieldId) {
          // Link to existing field instead of creating new
          fieldCreates.push({
            fields: {
              'api_name': fieldApiName, // Keep original API name
              'JSON': fieldJson,
              'Last Automation Time': timestamp,
              'Application': ['recQ0e0nQ4kI50kcc'],
              'Module': [moduleId],
              'Upsertion Check': upsertionKey,
              'UI Name': uiName,
              'Zoho Field Type': zohoFieldType,
              'Field': [matchingFieldId] // Link to the matching field
            }
          });
        } else {
          // Field doesn't exist - create new with UI name and field type
          fieldCreates.push({
            fields: {
              'api_name': fieldApiName, // Keep original API name
              'JSON': fieldJson,
              'Last Automation Time': timestamp,
              'Application': ['recQ0e0nQ4kI50kcc'],
              'Module': [moduleId],
              'Upsertion Check': upsertionKey,
              'UI Name': uiName,
              'Zoho Field Type': zohoFieldType
            }
          });
        }
      }
    });
    
    // Process updates for this module
    if (fieldUpdates.length > 0) {
      console.log(`Updating ${fieldUpdates.length} existing fields for module ${moduleApiName}...`);
      for (let i = 0; i < fieldUpdates.length; i += 10) {
        const payload = JSON.stringify({ 
          records: fieldUpdates.slice(i, i + 10),
          typecast: true
        });
        UrlFetchApp.fetch(fieldUrl, { method: 'PATCH', headers: headersWithTypecast, payload });
      }
    }
    
    // Process creates for this module
    if (fieldCreates.length > 0) {
      console.log(`Creating ${fieldCreates.length} new fields for module ${moduleApiName}...`);
      for (let i = 0; i < fieldCreates.length; i += 10) {
        const payload = JSON.stringify({ 
          records: fieldCreates.slice(i, i + 10),
          typecast: true
        });
        UrlFetchApp.fetch(fieldUrl, { method: 'POST', headers: headersWithTypecast, payload });
      }
    }
  }

  console.log('Module and field sync complete.');
}

function getZohoAccessToken(clientId, clientSecret, refreshToken) {
  const tokenUrl = `https://accounts.zoho.com/oauth/v2/token?refresh_token=${refreshToken}&client_id=${clientId}&client_secret=${clientSecret}&grant_type=refresh_token`;
  const response = UrlFetchApp.fetch(tokenUrl, { method: 'POST' });
  const json = JSON.parse(response.getContentText());
  if (!json.access_token) throw new Error('Zoho access token fetch failed');
  return json.access_token;
}

function getZohoModules(accessToken) {
  const url = 'https://www.zohoapis.com/crm/v2/settings/modules';
  const headers = {
    'Authorization': `Zoho-oauthtoken ${accessToken}`
  };
  const response = UrlFetchApp.fetch(url, { headers });
  const json = JSON.parse(response.getContentText());
  return json.modules || [];
}

function getZohoModuleFields(accessToken, moduleApiName) {
  const url = `https://www.zohoapis.com/crm/v2/settings/fields?module=${moduleApiName}`;
  const headers = {
    'Authorization': `Zoho-oauthtoken ${accessToken}`
  };
  try {
    const response = UrlFetchApp.fetch(url, { headers });
    const json = JSON.parse(response.getContentText());
    return json.fields || [];
  } catch (error) {
    console.warn(`Failed to fetch fields for module '${moduleApiName}': ${error.message}`);
    return [];
  }
}

function searchForMatchingFields(fields, moduleApiName, headers) {
  const lookupTableId = 'tblLcwcFbjLyQXdl6';
  const airtableBaseId = 'app4xfaklobVjLusr';
  const lookupUrl = `https://api.airtable.com/v0/${airtableBaseId}/${lookupTableId}`;
  
  const matchingFieldsMap = {};
  
  // Extract unique UI names from the fields
  const uiNames = [...new Set(fields.map(field => 
    field.display_label || field.field_label || field.api_name
  ))];
  
  console.log(`Searching for ${uiNames.length} field names in lookup table for module ${moduleApiName}`);
  
  // Search in batches to avoid URL length limits
  for (let i = 0; i < uiNames.length; i += 10) {
    const namesBatch = uiNames.slice(i, i + 10);
    
    // Create filter formula to search for fields with matching names and table
    const nameConditions = namesBatch.map(name => `{Name}='${name.replace(/'/g, "\\'")}'`);
    const tableCondition = `{Table}='${moduleApiName}'`;
    const formula = `AND(${tableCondition},OR(${nameConditions.join(',')}))`;
    
    try {
      const response = UrlFetchApp.fetch(`${lookupUrl}?filterByFormula=${encodeURIComponent(formula)}`, { headers });
      const data = JSON.parse(response.getContentText());
      
      // Map the found records by their Name field
      for (const record of data.records) {
        if (record.fields.Name && record.fields.Table === moduleApiName) {
          matchingFieldsMap[record.fields.Name] = record.id;
          console.log(`Found matching field: ${record.fields.Name} -> ${record.id}`);
        }
      }
    } catch (error) {
      console.warn(`Error searching for matching fields: ${error.message}`);
      // Continue with next batch if one fails
    }
  }
  
  return matchingFieldsMap;
}

// ===== ZOHO BOOKS SYNC FUNCTIONS =====

/**
 * Fetches and inserts sample record fields into Airtable for various Zoho Books entities
 */

const ZOHO_ORGANIZATION_ID = '732022817';
const AIRTABLE_BASE_ID = 'app4xfaklobVjLusr';
const AIRTABLE_TABLE_ID = 'tbl0JfUjWhV4TvLz2';
const AIRTABLE_VIEW_ID = 'viw19wmgmu9FDbr5m';
const APPLICATION_ID = 'recUwrBtC1DZBT0CL';

// These should be actual module record IDs from the Zoho Modules table, not hardcoded
// We'll look them up dynamically instead
const MODULE_NAMES = {
  'Items': 'Items',
  'Customers': 'Customers', 
  'Vendors': 'Vendors',
  'Expenses': 'Expenses',
  'Bills': 'Bills',
  'Invoices': 'Invoices'
};

function getZohoBooksAccessToken() {
  const clientId = PropertiesService.getScriptProperties().getProperty('clientIdBooks');
  const clientSecret = PropertiesService.getScriptProperties().getProperty('clientSecretBooks');
  const refreshToken = PropertiesService.getScriptProperties().getProperty('refreshTokenBooks');

  const url = `https://accounts.zoho.com/oauth/v2/token?refresh_token=${refreshToken}&client_id=${clientId}&client_secret=${clientSecret}&grant_type=refresh_token`;

  const response = UrlFetchApp.fetch(url, { method: 'POST' });
  const data = JSON.parse(response.getContentText());

  if (!data.access_token) throw new Error("Failed to get access token: " + JSON.stringify(data));
  return data.access_token;
}

function getModuleIdByName(moduleName, airtableHeaders) {
  try {
    const moduleTableId = 'tbl2HlEPyESvXUXHN';
    const filterFormula = encodeURIComponent(`{api_name}='${moduleName}'`);
    const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${moduleTableId}?filterByFormula=${filterFormula}&fields%5B%5D=api_name`;
    
    const response = UrlFetchApp.fetch(url, { headers: airtableHeaders });
    const data = JSON.parse(response.getContentText());
    
    if (data.records && data.records.length > 0) {
      return data.records[0].id;
    }
    
    Logger.log(`No module record found for ${moduleName}`);
    return null;
  } catch (err) {
    Logger.log(`Error looking up module ${moduleName}: ${err}`);
    return null;
  }
}

function fetchFieldsFromZohoBooks() {
  const accessToken = getZohoBooksAccessToken();
  const headers = {
    'Authorization': 'Zoho-oauthtoken ' + accessToken,
    'Content-Type': 'application/json'
  };

  const airtableToken = PropertiesService.getScriptProperties().getProperty('airtableApiKey');
  const airtableHeaders = {
    'Authorization': 'Bearer ' + airtableToken,
    'Content-Type': 'application/json'
  };

  const entities = [
    { name: 'Items', url: `https://www.zohoapis.com/books/v3/items?per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'items' },
    { name: 'Customers', url: `https://www.zohoapis.com/books/v3/contacts?type=customer&per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'contacts' },
    { name: 'Vendors', url: `https://www.zohoapis.com/books/v3/contacts?type=vendor&per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'contacts' },
    { name: 'Expenses', url: `https://www.zohoapis.com/books/v3/expenses?per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'expenses' },
    { name: 'Bills', url: `https://www.zohoapis.com/books/v3/bills?per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'bills' },
    { name: 'Invoices', url: `https://www.zohoapis.com/books/v3/invoices?per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'invoices' }
  ];

  entities.forEach(entity => {
    try {
      const res = UrlFetchApp.fetch(entity.url, { headers });
      const data = JSON.parse(res.getContentText());

      if (data[entity.key] && data[entity.key].length > 0) {
        const record = data[entity.key][0];
        Logger.log(`Adding ${Object.keys(record).length} fields for ${entity.name}`);

        const timestamp = new Date().toISOString();
        
        // Get the module ID dynamically
        const moduleId = getModuleIdByName(entity.name, airtableHeaders);
        
        const fieldPayloads = Object.keys(record).map(field => {
          const value = record[field];
          const isCustom = typeof field === 'string' && field.startsWith('cf_');
          let fieldMeta = null;

          if (isCustom && value && typeof value === 'object') {
            fieldMeta = value;
          }

          return {
            fields: {
              "Application": [APPLICATION_ID],
              "api_name": field,
              "Module": moduleId ? [moduleId] : [],
              "JSON": JSON.stringify(fieldMeta || {}),
              "Last Automation Time": timestamp,
              "Upsertion Check": `${entity.name} - ${field}`
            }
          };
        });

        // For proper upserting, we need to fetch all records for this module
        // and then match on the Upsertion Check field in memory
        let existingRecords = [];
        try {
          // Instead of filtering by module ID (which might still get too many records),
          // fetch records using a FIND() operation on the entity name part of Upsertion Check
          const filterFormula = encodeURIComponent(`FIND("${entity.name} - ", {Upsertion Check})=1`);
          const existingUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}?filterByFormula=${filterFormula}&fields%5B%5D=Upsertion+Check`;
          
          Logger.log(`Fetching existing records for ${entity.name} with URL: ${existingUrl}`);
          const existingRes = UrlFetchApp.fetch(existingUrl, { headers: airtableHeaders });
          const existingData = JSON.parse(existingRes.getContentText());
          existingRecords = existingData.records || [];
          Logger.log(`Found ${existingRecords.length} existing records for ${entity.name}`);
        } catch (filterErr) {
          Logger.log(`Error fetching existing records: ${filterErr}`);
          // Continue with empty existing records
        }

        // Build a map of Upsertion Check values to Airtable record IDs
        const existingMap = {};
        existingRecords.forEach(rec => {
          if (rec.fields && rec.fields["Upsertion Check"]) {
            existingMap[rec.fields["Upsertion Check"]] = rec.id;
          }
        });

        const createRecords = [];
        const updateRecords = [];

        // Process each field and determine if it needs to be created or updated
        fieldPayloads.forEach(payload => {
          const key = payload.fields["Upsertion Check"];
          if (existingMap[key]) {
            // This record exists, add to update list
            updateRecords.push({ id: existingMap[key], fields: payload.fields });
            Logger.log(`Will update existing record for ${key}`);
          } else {
            // This is a new record, add to create list
            createRecords.push(payload);
            Logger.log(`Will create new record for ${key}`);
          }
        });

        // Batch create new records
        for (let i = 0; i < createRecords.length; i += 10) {
          const batch = createRecords.slice(i, i + 10);
          const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}`;
          const options = {
            method: 'post',
            contentType: 'application/json',
            headers: airtableHeaders,
            payload: JSON.stringify({ records: batch }),
            muteHttpExceptions: true
          };
          const airtableRes = UrlFetchApp.fetch(url, options);
          const responseCode = airtableRes.getResponseCode();
          Logger.log(`Created ${batch.length} records for ${entity.name}, response code: ${responseCode}`);
          if (responseCode !== 200) {
            Logger.log(`Error response: ${airtableRes.getContentText()}`);
          }
        }

        // Batch update existing records
        for (let i = 0; i < updateRecords.length; i += 10) {
          const batch = updateRecords.slice(i, i + 10);
          const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}`;
          const options = {
            method: 'patch',
            contentType: 'application/json',
            headers: airtableHeaders,
            payload: JSON.stringify({ records: batch }),
            muteHttpExceptions: true
          };
          const airtableRes = UrlFetchApp.fetch(url, options);
          const responseCode = airtableRes.getResponseCode();
          Logger.log(`Updated ${batch.length} records for ${entity.name}, response code: ${responseCode}`);
          if (responseCode !== 200) {
            Logger.log(`Error response: ${airtableRes.getContentText()}`);
          }
        }
      } else {
        Logger.log(`No ${entity.name} records found.`);
      }
    } catch (err) {
      Logger.log(`Error processing ${entity.name}: ${err}`);
    }
  });
}

// ===== ZOHO PROJECTS SYNC FUNCTIONS =====

/**
 * Standalone version of fetchZohoTaskByIdFromAirtable for environments where functions are not selectable
 */

function runStandaloneZohoTaskFetch() {
  const clientId = PropertiesService.getScriptProperties().getProperty('clientIdProjects');
  const clientSecret = PropertiesService.getScriptProperties().getProperty('clientSecretProjects');
  const refreshToken = PropertiesService.getScriptProperties().getProperty('refreshTokenProjects');

  const tokenUrl = `https://accounts.zoho.com/oauth/v2/token?refresh_token=${refreshToken}&client_id=${clientId}&client_secret=${clientSecret}&grant_type=refresh_token`;
  const tokenResponse = UrlFetchApp.fetch(tokenUrl, { method: 'post' });
  const tokenData = JSON.parse(tokenResponse.getContentText());
  if (!tokenData.access_token) throw new Error("Failed to get access token: " + JSON.stringify(tokenData));
  Logger.log("Successfully obtained Zoho Projects access token");
  const accessToken = tokenData.access_token;

  const airtablePAT = PropertiesService.getScriptProperties().getProperty('airtableApiKey');
  const airtableUrl = 'https://api.airtable.com/v0/app4xfaklobVjLusr/tblYqgemNWDIL2Zbv?view=viwVzBUrAoAFdiq2K&maxRecords=1';
  const airtableOptions = {
    method: 'get',
    headers: { Authorization: `Bearer ${airtablePAT}` }
  };
  const airtableResponse = UrlFetchApp.fetch(airtableUrl, airtableOptions);
  const airtableData = JSON.parse(airtableResponse.getContentText());
  const airtableRecord = airtableData.records[0];

  const fields = airtableRecord.fields;
  const taskId = fields['Zoho Projects ID'];
  const projectId = fields['Zoho Projects ID (from Project)'];
  const portalId = 'greenlighteducation791';

  const headers = {
    'Authorization': 'Zoho-oauthtoken ' + accessToken,
    'Content-Type': 'application/json'
  };

  const taskUrl = `https://projectsapi.zoho.com/restapi/portal/${portalId}/projects/${projectId}/tasks/${taskId}/`;
  try {
    const taskResp = UrlFetchApp.fetch(taskUrl, { headers });
    const taskData = JSON.parse(taskResp.getContentText());
    const task = taskData.tasks[0];

    Logger.log("=== Task Fields ===");
    const keys = Object.keys(task);
    Logger.log(`Found ${keys.length} fields in the task`);

    const now = new Date().toISOString();
    const createBatch = [];
    const updateBatch = [];

    keys.forEach(k => {
      const value = task[k];
      Logger.log(`Field: ${k}`);
      Logger.log(`Type: ${typeof value}`);
      Logger.log(`Value: ${JSON.stringify(value)}`);
      Logger.log("---");

      const fieldsPayload = {
        "Field Type": typeof value,
        "JSON": JSON.stringify({ name: k, value }),
        "Last Automation Time": now,
        "api_name": k,
        "Module": ["recyHbidcT8ptu2U5"],
        "Application": ["recXkmU64mdG4S4eM"]
      };

      const filterFormula = `AND(api_name='${k}', FIND('recXkmU64mdG4S4eM', ARRAYJOIN(Application)))`;
      const filterUrl = `https://api.airtable.com/v0/app4xfaklobVjLusr/tbl0JfUjWhV4TvLz2?filterByFormula=${encodeURIComponent(filterFormula)}`;

      const filterResp = UrlFetchApp.fetch(filterUrl, {
        method: 'get',
        headers: { Authorization: `Bearer ${airtablePAT}` }
      });
      const filterData = JSON.parse(filterResp.getContentText());

      if (filterData.records.length > 0) {
        updateBatch.push({
          id: filterData.records[0].id,
          fields: fieldsPayload
        });
      } else {
        createBatch.push({ fields: fieldsPayload });
      }
    });

    if (createBatch.length > 0) {
      for (let i = 0; i < createBatch.length; i += 10) {
        const chunk = createBatch.slice(i, i + 10);
        const createResp = UrlFetchApp.fetch('https://api.airtable.com/v0/app4xfaklobVjLusr/tbl0JfUjWhV4TvLz2', {
          method: 'post',
          contentType: 'application/json',
          headers: { Authorization: `Bearer ${airtablePAT}` },
          payload: JSON.stringify({ records: chunk })
        });
        Logger.log(`Created batch chunk: ${createResp.getContentText()}`);
      }
    }

    if (updateBatch.length > 0) {
      for (let i = 0; i < updateBatch.length; i += 10) {
        const chunk = updateBatch.slice(i, i + 10);
        const updateResp = UrlFetchApp.fetch('https://api.airtable.com/v0/app4xfaklobVjLusr/tbl0JfUjWhV4TvLz2', {
          method: 'patch',
          contentType: 'application/json',
          headers: { Authorization: `Bearer ${airtablePAT}` },
          payload: JSON.stringify({ records: chunk })
        });
        Logger.log(`Updated batch chunk: ${updateResp.getContentText()}`);
      }
    }

  } catch (err) {
    Logger.log(`Error fetching task details: ${err}`);
  }

  // === Fetch Custom Fields Again ===
  try {
    const customFieldsUrl = `https://projectsapi.zoho.com/restapi/portal/${portalId}/projects/${projectId}/tasks/customfields/`;
    const customResp = UrlFetchApp.fetch(customFieldsUrl, { headers });
    const customData = JSON.parse(customResp.getContentText());

    if (customData && customData.customfields) {
      Logger.log("=== Custom Fields ===");
      Logger.log(`Found ${customData.customfields.length} custom fields`);

      customData.customfields.forEach(field => {
        Logger.log(`ID: ${field.id}`);
        Logger.log(`Field: ${field.field_name}`);
        Logger.log(`Label: ${field.label_name}`);
        Logger.log(`Type: ${field.custom_field_type}`);
        Logger.log("---");
      });
    } else {
      Logger.log("No custom fields found");
    }
  } catch (err) {
    Logger.log(`Error fetching custom fields: ${err}`);
  }
}