const { getLeadDetails } = require('./zohoService');
const { 
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

/**
 * Unified Sync Execution Service
 * 
 * Provides common sync logic for both bulk sync and polling operations.
 * Handles field-level synchronization between Zoho and Airtable.
 */

// Sync options interface
const SYNC_OPTIONS = {
  compareValues: true,      // Compare values before syncing (polling mode)
  verbose: false,          // Detailed logging
  skipIgnoredFields: true, // Skip system/computed fields
  createMissing: true      // Create records if they don't exist
};

// Fields to ignore during sync (computed fields, timestamps, etc.)
const IGNORED_FIELDS = [
  'Modified_Time', 'Created_Time', 'Modified_By', 'Created_By', 'id',
  'smsmagic4__Plain_Phone', 'smsmagic4__Plain_Mobile',
  'Lead_Conversion_Time', 'Data_Processing_Basis_Details',
  'Approval', 'Data_Source', 'Process_Flow'
];

/**
 * Helper function to check if field should be ignored
 */
function shouldIgnoreField(fieldName) {
  return IGNORED_FIELDS.includes(fieldName);
}

/**
 * Helper function to compare values for equality
 */
function areValuesEqual(value1, value2) {
  if (value1 == null && value2 == null) return true;
  if (value1 == null || value2 == null) return false;
  
  const str1 = String(value1).trim();
  const str2 = String(value2).trim();
  
  return str1 === str2;
}

/**
 * Sync a Zoho lead to Airtable
 * 
 * @param {Object} zohoData - Zoho lead data
 * @param {Object} options - Sync options
 * @returns {Promise<boolean>} Success status
 */
async function syncZohoToAirtable(zohoData, options = {}) {
  const opts = { ...SYNC_OPTIONS, ...options };
  const { id: zohoId, data: leadData } = zohoData;
  
  try {
    // Check if Airtable record exists
    let airtableRecordId = await findAirtableRecordByZohoId(zohoId);
    
    if (!airtableRecordId) {
      if (!opts.createMissing) {
        if (opts.verbose) console.log(`⏭️  Skipping creation of Airtable record for Zoho lead ${zohoId}`);
        return false;
      }
      
      // Create new Airtable record
      if (opts.verbose) console.log(`📝 Creating new Airtable record for Zoho lead ${zohoId}`);
      const createdRecord = await createAirtableRecordFromZohoLead(zohoId, leadData);
      return !!createdRecord;
    }

    // Get field mapping
    const fieldMapping = fieldMappingCache.getFieldMapping();
    if (!fieldMapping) {
      if (opts.verbose) console.log('⚠️  No field mapping available, skipping detailed sync');
      return false;
    }

    // Get current Airtable record for comparison (if needed)
    let currentAirtableRecord = null;
    if (opts.compareValues) {
      currentAirtableRecord = await getRecordById(airtableRecordId);
      if (!currentAirtableRecord) {
        if (opts.verbose) console.log(`⚠️  Could not fetch current Airtable record ${airtableRecordId}`);
        return false;
      }
    }

    const syncPromises = [];
    let fieldsToSync = 0;
    
    // Process each mapped field
    for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
      if (leadData[zohoField] === undefined) continue;
      if (opts.skipIgnoredFields && shouldIgnoreField(zohoField)) continue;
      
      const zohoValue = leadData[zohoField];
      
      // Compare values if in comparison mode
      if (opts.compareValues && currentAirtableRecord) {
        const airtableValue = currentAirtableRecord.fields[mapping.airtable];
        if (areValuesEqual(zohoValue, airtableValue)) {
          continue; // Skip if values are the same
        }
        
        if (opts.verbose) {
          console.log(`🔄 Field ${zohoField}: "${airtableValue}" → "${zohoValue}"`);
        }
      }
      
      fieldsToSync++;
      syncPromises.push(
        syncFieldFromZohoToAirtable(zohoId, zohoField, zohoValue, mapping)
          .catch(error => {
            if (opts.verbose) {
              console.log(`❌ Failed to sync field ${zohoField}: ${error.message}`);
            }
            throw error;
          })
      );
    }

    if (fieldsToSync === 0) {
      if (opts.verbose) {
        const reason = opts.compareValues ? 'no field differences found' : 'no fields to sync';
        console.log(`⏭️  Skipping update for Zoho lead ${zohoId}, ${reason}`);
      }
      return true; // No changes needed is still success
    }

    const results = await Promise.allSettled(syncPromises);
    const successful = results.filter(r => r.status === 'fulfilled').length;
    const failed = results.filter(r => r.status === 'rejected').length;
    
    if (opts.verbose) {
      console.log(`📊 Sync results: ${successful} successful, ${failed} failed out of ${fieldsToSync} fields`);
    }
    
    return successful > 0;
    
  } catch (error) {
    if (opts.verbose) {
      console.log(`❌ Error syncing Zoho lead ${zohoId} to Airtable: ${error.message}`);
    }
    return false;
  }
}

/**
 * Sync an Airtable record to Zoho
 * 
 * @param {Object} airtableData - Airtable record data
 * @param {Object} options - Sync options
 * @returns {Promise<boolean>} Success status
 */
async function syncAirtableToZoho(airtableData, options = {}) {
  const opts = { ...SYNC_OPTIONS, ...options };
  const { id: recordId, zohoId, data: recordData } = airtableData;
  
  try {
    // Get field mapping
    const fieldMapping = fieldMappingCache.getFieldMapping();
    if (!fieldMapping) {
      if (opts.verbose) console.log('⚠️  No field mapping available, skipping detailed sync');
      return false;
    }

    // Get field ID to name mapping
    const fieldIdToName = await getFieldIdToNameMapping();

    // Get current Zoho lead for comparison (if needed)
    let currentZohoLead = null;
    if (opts.compareValues) {
      const zohoResponse = await getLeadDetails(zohoId);
      if (!zohoResponse || !zohoResponse.data || !zohoResponse.data[0]) {
        if (opts.verbose) console.log(`⚠️  Could not fetch current Zoho lead ${zohoId}`);
        return false;
      }
      currentZohoLead = zohoResponse.data[0];
    }

    const syncPromises = [];
    let fieldsToSync = 0;
    
    // Process each mapped field
    for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
      if (opts.skipIgnoredFields && shouldIgnoreField(zohoField)) continue;
      
      const airtableFieldId = mapping.airtable;
      const airtableFieldName = fieldIdToName[airtableFieldId] || airtableFieldId;
      const airtableValue = recordData.fields[airtableFieldName];
      
      if (airtableValue === undefined) continue;
      
      // Compare values if in comparison mode
      if (opts.compareValues && currentZohoLead) {
        const zohoValue = currentZohoLead[zohoField];
        if (areValuesEqual(airtableValue, zohoValue)) {
          continue; // Skip if values are the same
        }
        
        if (opts.verbose) {
          console.log(`🔄 Field ${zohoField} (${airtableFieldName}): "${zohoValue}" → "${airtableValue}"`);
        }
      }
      
      fieldsToSync++;
      syncPromises.push(
        syncFieldFromAirtableToZoho(recordId, zohoField, airtableValue, mapping)
          .catch(error => {
            if (opts.verbose) {
              console.log(`❌ Failed to sync field ${zohoField}: ${error.message}`);
            }
            throw error;
          })
      );
    }

    if (fieldsToSync === 0) {
      if (opts.verbose) {
        const reason = opts.compareValues ? 'no field differences found' : 'no fields to sync';
        console.log(`⏭️  Skipping update for Airtable record ${recordId}, ${reason}`);
      }
      return true; // No changes needed is still success
    }

    const results = await Promise.allSettled(syncPromises);
    const successful = results.filter(r => r.status === 'fulfilled').length;
    const failed = results.filter(r => r.status === 'rejected').length;
    
    if (opts.verbose) {
      console.log(`📊 Sync results: ${successful} successful, ${failed} failed out of ${fieldsToSync} fields`);
    }
    
    return successful > 0;
    
  } catch (error) {
    if (opts.verbose) {
      console.log(`❌ Error syncing Airtable record ${recordId} to Zoho: ${error.message}`);
    }
    return false;
  }
}

module.exports = {
  syncZohoToAirtable,
  syncAirtableToZoho,
  SYNC_OPTIONS,
  shouldIgnoreField,
  areValuesEqual
};