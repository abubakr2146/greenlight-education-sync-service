const { getLeadsModifiedSince, getMultipleLeadDetails } = require('./zohoService');
const { 
  getRecordsModifiedSince, 
  getAllRecordsForSync, 
  getRecordById,
  findAirtableRecordByZohoId 
} = require('./airtableService');
const { 
  createAirtableRecordFromZohoLead, 
  syncFieldFromZohoToAirtable,
  syncFieldFromAirtableToZoho 
} = require('./syncService');
const fieldMappingCache = require('../utils/fieldMappingCache');
const { filterIgnoredFields } = require('../config/config');
const { 
  shouldSkipRecordSync, 
  recordRecordSync, 
  getRecordSyncStatus 
} = require('../utils/syncTracker');

// Track last sync timestamp
let lastSyncTimestamp = Date.now() - (24 * 60 * 60 * 1000); // Start with 24 hours ago

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
    
    // Check if we should skip this record due to recent sync
    if (shouldSkipRecordSync('zoho', zohoChange.id)) {
      console.log(`🔄 Skipping Zoho lead ${zohoChange.id} - recently synced`);
      continue;
    }
    
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
      // Check if we should skip this record due to recent sync
      if (shouldSkipRecordSync('airtable', airtableChange.id)) {
        console.log(`🔄 Skipping Airtable record ${airtableChange.id} - recently synced`);
        continue;
      }
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
        // Record that we synced this record to prevent loops
        recordRecordSync('zoho', change.id);
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
        // Record that we synced this record to prevent loops
        recordRecordSync('airtable', change.id);
        recordRecordSync('zoho', change.zohoId); // Also track the Zoho side
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

  // Update existing record - sync all mapped fields
  const fieldMapping = fieldMappingCache.getFieldMapping();
  if (!fieldMapping) {
    console.log('⚠️  No field mapping available, skipping detailed sync');
    return false;
  }

  let syncSuccess = true;
  const syncPromises = [];

  // Sync all mapped fields
  for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
    if (leadData[zohoField] !== undefined && !shouldIgnoreField(zohoField)) {
      syncPromises.push(
        syncFieldFromZohoToAirtable(change.id, zohoField, leadData[zohoField], mapping)
          .catch(error => {
            console.log(`❌ Failed to sync field ${zohoField}:`, error.message);
            syncSuccess = false;
          })
      );
    }
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

  let syncSuccess = true;
  const syncPromises = [];

  // Sync all mapped fields
  for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
    const airtableField = mapping.airtable;
    const fieldValue = recordData.fields[airtableField];
    
    if (fieldValue !== undefined && !shouldIgnoreField(zohoField)) {
      syncPromises.push(
        syncFieldFromAirtableToZoho(change.id, zohoField, fieldValue, mapping)
          .catch(error => {
            console.log(`❌ Failed to sync field ${zohoField}:`, error.message);
            syncSuccess = false;
          })
      );
    }
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

// Get sync status
function getSyncStatus() {
  const recordSyncStatus = getRecordSyncStatus();
  return {
    lastSyncTimestamp,
    lastSyncTime: new Date(lastSyncTimestamp).toISOString(),
    fieldMappingReady: fieldMappingCache.isReady(),
    fieldMappingCount: fieldMappingCache.getStatus().mappingCount,
    activeRecordSyncs: recordSyncStatus.length,
    recentlySyncedRecords: recordSyncStatus
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