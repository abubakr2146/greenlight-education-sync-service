const { getLeadsModifiedSince, getMultipleLeadDetails, getLeadDetails } = require('./zohoService');
const { 
  getRecordsModifiedSince, 
  getAllRecordsForSync, 
  getRecordById,
  findAirtableRecordByZohoId,
  findZohoLeadByAirtableId,
  getFieldIdToNameMapping 
} = require('./airtableService');
const { 
  createAirtableRecordFromZohoLead,
  createZohoLeadFromAirtableRecord 
} = require('./syncService');
const { 
  syncZohoToAirtable, 
  syncAirtableToZoho 
} = require('./syncExecutionService');
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

  return response.records.map(record => ({
      id: record.id,
      zohoId: record.fields['Zoho CRM ID'] || null, // Allow null for new records
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
  const airtableMap = new Map(airtableChanges.filter(c => c.zohoId).map(change => [change.zohoId, change]));

  // Process Zoho changes
  for (const zohoChange of zohoChanges) {
    const airtableChange = airtableMap.get(zohoChange.id);
    
    if (!airtableChange) {
      // No corresponding Airtable record - sync from Zoho (will create if needed)
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

  // Process Airtable changes
  for (const airtableChange of airtableChanges) {
    if (!airtableChange.zohoId) {
      // Airtable record has no Zoho CRM ID - create new Zoho lead
      plan.airtableToZoho.push(airtableChange);
    } else if (!zohoMap.has(airtableChange.zohoId)) {
      // Airtable record references non-existent Zoho lead
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
      const success = await syncZohoToAirtable(change, { 
        compareValues: true, 
        verbose: false,
        createMissing: true 
      });
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
      const success = await syncAirtableToZoho(change, { 
        compareValues: true, 
        verbose: false,
        createMissing: true 
      });
      if (success) {
        successful++;
        const target = change.zohoId ? `Zoho ${change.zohoId}` : 'new Zoho lead';
        console.log(`✅ Synced Airtable record ${change.id} → ${target}`);
      } else {
        failed++;
        const target = change.zohoId ? `Zoho ${change.zohoId}` : 'new Zoho lead';
        console.log(`❌ Failed to sync Airtable record ${change.id} → ${target}`);
      }
    } catch (error) {
      failed++;
      console.log(`❌ Error syncing Airtable record ${change.id} → Zoho:`, error.message);
    }
  }

  return { successful, failed };
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