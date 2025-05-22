const { getLeadsModifiedSince, getMultipleLeadDetails, getLeadDetails } = require('./zohoService');
const { 
  getRecordsModifiedSince, 
  getAllRecordsForSync, 
  getRecordById,
  findAirtableRecordByZohoId,
  getFieldIdToNameMapping 
} = require('./airtableService');
const { 
  createAirtableRecordFromZohoLead, 
  syncFieldFromZohoToAirtable,
  syncFieldFromAirtableToZoho 
} = require('./syncService');
const fieldMappingCache = require('../utils/fieldMappingCache');
const { filterIgnoredFields } = require('../config/config');

// Track last sync timestamp - start from when script is initiated
let lastSyncTimestamp = Date.now();

// Main polling sync function
async function performSync() {
  try {
    console.log(`\n🔄 Starting sync cycle at ${new Date().toISOString()}`);
    console.log(`📅 Looking for changes since: ${new Date(lastSyncTimestamp).toISOString()}`);
    
    // Step 1: Get changes from both systems
    const [zohoChanges, airtableChanges] = await Promise.all([
      getZohoChanges(lastSyncTimestamp),
      getAirtableChanges(lastSyncTimestamp)
    ]);

    console.log(`📊 Found ${zohoChanges.length} Zoho changes, ${airtableChanges.length} Airtable changes`);

    // Step 2: Process conflicts and determine sync direction
    const syncPlan = await createSyncPlan(zohoChanges, airtableChanges);
    
    console.log(`📋 Sync plan: ${syncPlan.zohoToAirtable.length} Zoho→Airtable, ${syncPlan.airtableToZoho.length} Airtable→Zoho, ${syncPlan.conflicts.length} conflicts`);

    // Step 3: Execute sync operations
    const results = await executeSyncPlan(syncPlan);
    
    // Step 4: Update last sync timestamp
    lastSyncTimestamp = Date.now();
    
    console.log(`✅ Sync completed successfully: ${results.successful} successful, ${results.failed} failed`);
    return results;
    
  } catch (error) {
    console.error('❌ Sync failed:', error.message);
    return { successful: 0, failed: 1, error: error.message };
  }
}

// Get Zoho changes since last sync
async function getZohoChanges(sinceTimestamp) {
  const response = await getLeadsModifiedSince(sinceTimestamp);
  if (!response || !response.data) {
    return [];
  }

  // Get full details for changed leads
  const leadIds = response.data.map(lead => lead.id);
  if (leadIds.length === 0) {
    return [];
  }

  const detailsResponse = await getMultipleLeadDetails(leadIds);
  if (!detailsResponse || !detailsResponse.data) {
    return [];
  }

  return detailsResponse.data.map(lead => ({
    id: lead.id,
    modifiedTime: new Date(lead.Modified_Time).getTime(),
    data: lead,
    source: 'zoho'
  }));
}

// Get Airtable changes since last sync
async function getAirtableChanges(sinceTimestamp) {
  const response = await getRecordsModifiedSince(sinceTimestamp);
  if (!response || !response.records) {
    return [];
  }

  return response.records
    .filter(record => record.fields['Zoho CRM ID']) // Only records linked to Zoho
    .map(record => ({
      id: record.id,
      zohoId: record.fields['Zoho CRM ID'],
      modifiedTime: new Date(record.fields['Last Modified Time']).getTime(),
      data: record,
      source: 'airtable'
    }));
}

// Create sync plan based on modification times
async function createSyncPlan(zohoChanges, airtableChanges) {
  const plan = {
    zohoToAirtable: [],
    airtableToZoho: [],
    conflicts: []
  };

  // Create maps for quick lookup
  const zohoMap = new Map(zohoChanges.map(change => [change.id, change]));
  const airtableMap = new Map(airtableChanges.map(change => [change.zohoId, change]));

  // Process Zoho changes
  for (const zohoChange of zohoChanges) {
    const airtableChange = airtableMap.get(zohoChange.id);
    
    if (!airtableChange) {
      // No corresponding Airtable record - sync from Zoho
      plan.zohoToAirtable.push(zohoChange);
    } else {
      // Both changed - compare timestamps
      const timeDiff = Math.abs(zohoChange.modifiedTime - airtableChange.modifiedTime);
      
      if (timeDiff < 60000) { // Less than 1 minute difference - potential conflict
        plan.conflicts.push({
          zoho: zohoChange,
          airtable: airtableChange,
          timeDiff
        });
      } else if (zohoChange.modifiedTime > airtableChange.modifiedTime) {
        // Zoho is newer
        plan.zohoToAirtable.push(zohoChange);
      } else {
        // Airtable is newer
        plan.airtableToZoho.push(airtableChange);
      }
    }
  }

  // Process Airtable-only changes
  for (const airtableChange of airtableChanges) {
    if (!zohoMap.has(airtableChange.zohoId)) {
      plan.airtableToZoho.push(airtableChange);
    }
  }

  return plan;
}

// Execute the sync plan
async function executeSyncPlan(plan) {
  let successful = 0;
  let failed = 0;

  // Handle conflicts (for now, Zoho wins in conflicts)
  for (const conflict of plan.conflicts) {
    console.log(`⚠️  Conflict detected for lead ${conflict.zoho.id}, Zoho wins (${conflict.timeDiff}ms apart)`);
    plan.zohoToAirtable.push(conflict.zoho);
  }

  // Sync Zoho → Airtable
  for (const change of plan.zohoToAirtable) {
    try {
      const success = await syncZohoToAirtable(change);
      if (success) {
        successful++;
        console.log(`✅ Synced Zoho lead ${change.id} → Airtable`);
      } else {
        failed++;
        console.log(`❌ Failed to sync Zoho lead ${change.id} → Airtable`);
      }
    } catch (error) {
      failed++;
      console.log(`❌ Error syncing Zoho lead ${change.id} → Airtable:`, error.message);
    }
  }

  // Sync Airtable → Zoho
  for (const change of plan.airtableToZoho) {
    try {
      const success = await syncAirtableToZoho(change);
      if (success) {
        successful++;
        console.log(`✅ Synced Airtable record ${change.id} → Zoho ${change.zohoId}`);
      } else {
        failed++;
        console.log(`❌ Failed to sync Airtable record ${change.id} → Zoho ${change.zohoId}`);
      }
    } catch (error) {
      failed++;
      console.log(`❌ Error syncing Airtable record ${change.id} → Zoho:`, error.message);
    }
  }

  return { successful, failed };
}

// Sync a Zoho lead to Airtable
async function syncZohoToAirtable(change) {
  const leadData = change.data;
  
  // Check if Airtable record exists
  let airtableRecordId = await findAirtableRecordByZohoId(change.id);
  
  if (!airtableRecordId) {
    // Create new Airtable record
    const createdRecord = await createAirtableRecordFromZohoLead(change.id, leadData);
    return !!createdRecord;
  }

  // Get current Airtable record to compare values
  const currentAirtableRecord = await getRecordById(airtableRecordId);
  if (!currentAirtableRecord) {
    console.log(`⚠️  Could not fetch current Airtable record ${airtableRecordId}`);
    return false;
  }

  // Update existing record - sync only fields with different values
  const fieldMapping = fieldMappingCache.getFieldMapping();
  if (!fieldMapping) {
    console.log('⚠️  No field mapping available, skipping detailed sync');
    return false;
  }

  let syncSuccess = true;
  const syncPromises = [];
  let fieldsToSync = 0;

  // Compare and sync only changed fields
  for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
    if (leadData[zohoField] !== undefined && !shouldIgnoreField(zohoField)) {
      const zohoValue = leadData[zohoField];
      const airtableValue = currentAirtableRecord.fields[mapping.airtable];
      
      // Compare values - only sync if different
      if (!areValuesEqual(zohoValue, airtableValue)) {
        fieldsToSync++;
        console.log(`🔄 Field ${zohoField}: "${airtableValue}" → "${zohoValue}"`);
        syncPromises.push(
          syncFieldFromZohoToAirtable(change.id, zohoField, zohoValue, mapping)
            .catch(error => {
              console.log(`❌ Failed to sync field ${zohoField}:`, error.message);
              syncSuccess = false;
            })
        );
      }
    }
  }

  if (fieldsToSync === 0) {
    console.log(`⏭️  No field differences found for Zoho lead ${change.id}, skipping update`);
    return true; // No changes needed is still success
  }

  await Promise.allSettled(syncPromises);
  return syncSuccess;
}

// Sync an Airtable record to Zoho
async function syncAirtableToZoho(change) {
  const recordData = change.data;
  const fieldMapping = fieldMappingCache.getFieldMapping();
  
  if (!fieldMapping) {
    console.log('⚠️  No field mapping available, skipping detailed sync');
    return false;
  }

  // Get field ID to name mapping to resolve field IDs to actual field names (CRITICAL FIX)
  const fieldIdToName = await getFieldIdToNameMapping();

  // Get current Zoho lead to compare values
  const currentZohoLead = await getLeadDetails(change.zohoId);
  if (!currentZohoLead || !currentZohoLead.data || !currentZohoLead.data[0]) {
    console.log(`⚠️  Could not fetch current Zoho lead ${change.zohoId}`);
    return false;
  }

  const currentZohoData = currentZohoLead.data[0];
  let syncSuccess = true;
  const syncPromises = [];
  let fieldsToSync = 0;

  // Compare and sync only changed fields
  for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
    const airtableFieldId = mapping.airtable;
    
    // Resolve field ID to field name (CRITICAL FIX from bulk sync)
    const airtableFieldName = fieldIdToName[airtableFieldId] || airtableFieldId;
    const airtableValue = recordData.fields[airtableFieldName];
    
    if (airtableValue !== undefined && !shouldIgnoreField(zohoField)) {
      const zohoValue = currentZohoData[zohoField];
      
      // Compare values - only sync if different
      if (!areValuesEqual(airtableValue, zohoValue)) {
        fieldsToSync++;
        console.log(`🔄 Field ${zohoField} (${airtableFieldName}): "${zohoValue}" → "${airtableValue}"`);
        syncPromises.push(
          syncFieldFromAirtableToZoho(change.id, zohoField, airtableValue, mapping)
            .catch(error => {
              console.log(`❌ Failed to sync field ${zohoField}:`, error.message);
              syncSuccess = false;
            })
        );
      }
    }
  }

  if (fieldsToSync === 0) {
    console.log(`⏭️  No field differences found for Airtable record ${change.id}, skipping update`);
    return true; // No changes needed is still success
  }

  await Promise.allSettled(syncPromises);
  return syncSuccess;
}

// Helper function to check if field should be ignored
function shouldIgnoreField(fieldName) {
  const ignoredZohoFields = [
    'Modified_Time', 'Created_Time', 'Modified_By', 'Created_By', 'id',
    'smsmagic4__Plain_Phone', 'smsmagic4__Plain_Mobile',
    'Lead_Conversion_Time', 'Data_Processing_Basis_Details',
    'Approval', 'Data_Source', 'Process_Flow'
  ];
  
  return ignoredZohoFields.includes(fieldName);
}

// Helper function to compare values for equality
function areValuesEqual(value1, value2) {
  // Handle null/undefined cases
  if (value1 == null && value2 == null) return true;
  if (value1 == null || value2 == null) return false;
  
  // Convert both to strings for comparison to handle type differences
  const str1 = String(value1).trim();
  const str2 = String(value2).trim();
  
  return str1 === str2;
}

// Get sync status
function getSyncStatus() {
  return {
    lastSyncTimestamp,
    lastSyncTime: new Date(lastSyncTimestamp).toISOString(),
    fieldMappingReady: fieldMappingCache.isReady(),
    fieldMappingCount: fieldMappingCache.getStatus().mappingCount
  };
}

// Set last sync timestamp (useful for testing)
function setLastSyncTimestamp(timestamp) {
  lastSyncTimestamp = timestamp;
}

module.exports = {
  performSync,
  getSyncStatus,
  setLastSyncTimestamp
};