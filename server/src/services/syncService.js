const { FIELD_MAPPING } = require('../config/config');
const fieldMappingCache = require('../utils/fieldMappingCache');
const { shouldSkipSync } = require('../utils/syncTracker');
const { getLeadDetails, updateZohoLead } = require('./zohoService');
const { 
  updateAirtableRecord, 
  createAirtableRecord, 
  findAirtableRecordByZohoId, 
  findZohoLeadByAirtableId 
} = require('./airtableService');

// Constants
const SYNC_DIRECTIONS = {
  ZOHO_TO_AIRTABLE: 'zoho_to_airtable',
  AIRTABLE_TO_ZOHO: 'airtable_to_zoho'
};

// Helper function to get field mapping for a specific field using cache
function getFieldMappingFor(zohoFieldName) {
  const fieldMapping = fieldMappingCache.getFieldMapping();
  
  if (!fieldMapping) {
    console.warn(`⚠️  Field mapping cache not ready, fallback to static mapping for ${zohoFieldName}`);
    // Fallback to static mapping
    if (FIELD_MAPPING[zohoFieldName]) {
      return FIELD_MAPPING[zohoFieldName];
    }
    return null;
  }
  
  // Handle both static (FIELD_MAPPING) and dynamic mapping formats
  if (fieldMapping[zohoFieldName]) {
    return fieldMapping[zohoFieldName];
  }
  
  // Fallback: check if it's the old static format
  for (const [key, mapping] of Object.entries(fieldMapping)) {
    if (mapping.zoho === zohoFieldName) {
      return mapping;
    }
  }
  
  return null;
}

// Helper function to get Zoho CRM ID field mapping using cache
function getZohoCrmIdMapping() {
  const fieldMapping = fieldMappingCache.getFieldMapping();
  
  if (!fieldMapping) {
    console.warn(`⚠️  Field mapping cache not ready, fallback to static mapping for Zoho CRM ID`);
    return FIELD_MAPPING.ZOHO_ID;
  }
  
  // Look for ZOHO_ID or equivalent in dynamic mapping
  if (fieldMapping.ZOHO_ID || fieldMapping['Zoho CRM ID']) {
    return fieldMapping.ZOHO_ID || fieldMapping['Zoho CRM ID'];
  }
  
  // Fallback to static mapping
  return FIELD_MAPPING.ZOHO_ID;
}

// Helper function to find or create Airtable record for a Zoho lead
async function findOrCreateAirtableRecord(leadId) {
  let airtableRecordId = await findAirtableRecordByZohoId(leadId);
  
  if (!airtableRecordId) {
    console.log(`⚠️  No corresponding Airtable record found for Zoho lead ${leadId}`);
    console.log(`📝 Creating Airtable record for this lead...`);
    
    const leadDetails = await getLeadDetails(leadId);
    if (leadDetails && leadDetails.data && leadDetails.data[0]) {
      const createdRecord = await createAirtableRecordFromZohoLead(leadId, leadDetails.data[0]);
      if (createdRecord) {
        airtableRecordId = createdRecord.id;
        console.log(`✅ Created Airtable record: ${airtableRecordId}`);
      } else {
        console.log(`❌ Failed to create Airtable record`);
        return null;
      }
    } else {
      console.log(`❌ Could not fetch lead details to create Airtable record`);
      return null;
    }
  }
  
  return airtableRecordId;
}

// Helper function to record sync to prevent loops
function recordSyncToPreventLoop(system, recordId, fieldId, value) {
  shouldSkipSync(system, recordId, fieldId, value);
  console.log(`📝 Pre-recorded sync to prevent webhook loop: ${system}:${recordId}:${fieldId}`);
}

// Generic sync function that handles both directions
async function syncField(params) {
  const {
    direction,
    sourceId,
    fieldName,
    value,
    mapping
  } = params;

  if (direction === SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE) {
    return await syncFromZohoToAirtable(sourceId, fieldName, value, mapping);
  } else if (direction === SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO) {
    return await syncFromAirtableToZoho(sourceId, fieldName, value, mapping);
  } else {
    throw new Error(`Invalid sync direction: ${direction}`);
  }
}

// Internal function to sync from Zoho to Airtable
async function syncFromZohoToAirtable(leadId, zohoFieldName, newValue, mapping) {
  console.log(`\n🔄 Syncing ${zohoFieldName} from Zoho lead ${leadId} to Airtable...`);
  
  try {
    // Find or create corresponding Airtable record
    const airtableRecordId = await findOrCreateAirtableRecord(leadId);
    if (!airtableRecordId) {
      return false;
    }
    
    // Pre-record sync to prevent webhook loop
    const airtableField = mapping.airtable;
    recordSyncToPreventLoop('airtable', airtableRecordId, airtableField, newValue);
    
    // Prepare and execute update
    const fieldUpdates = { [airtableField]: newValue };
    await updateAirtableRecord(airtableRecordId, fieldUpdates);
    
    console.log(`✅ Successfully synced ${zohoFieldName} to Airtable`);
    return true;
  } catch (error) {
    console.error(`❌ Error syncing ${zohoFieldName} from Zoho to Airtable:`, error.message);
    return false;
  }
}

// Internal function to sync from Airtable to Zoho
async function syncFromAirtableToZoho(recordId, zohoFieldName, newValue, mapping) {
  console.log(`\n🔄 Syncing ${zohoFieldName} from Airtable record ${recordId} to Zoho...`);
  
  try {
    // Find corresponding Zoho lead
    const zohoLeadId = await findZohoLeadByAirtableId(recordId);
    if (!zohoLeadId) {
      console.log(`⚠️  No corresponding Zoho lead found for Airtable record ${recordId}`);
      return false;
    }
    
    // Pre-record sync to prevent webhook loop
    const zohoField = mapping.zoho;
    recordSyncToPreventLoop('zoho', zohoLeadId, zohoField, newValue);
    
    // Prepare and execute update
    const fieldUpdates = {
      id: zohoLeadId,
      [zohoField]: newValue
    };
    await updateZohoLead(zohoLeadId, fieldUpdates);
    
    console.log(`✅ Successfully synced ${zohoFieldName} to Zoho`);
    return true;
  } catch (error) {
    console.error(`❌ Error syncing ${zohoFieldName} from Airtable to Zoho:`, error.message);
    return false;
  }
}

// Simplified phone sync functions (now just thin wrappers)
async function syncPhoneFromZohoToAirtable(leadId, newPhoneValue) {
  const phoneMapping = getFieldMappingFor('Phone');
  if (!phoneMapping) {
    console.log(`⚠️  No field mapping found for Phone field`);
    return false;
  }
  
  return await syncField({
    direction: SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE,
    sourceId: leadId,
    fieldName: 'Phone',
    value: newPhoneValue,
    mapping: phoneMapping
  });
}

async function syncPhoneFromAirtableToZoho(recordId, newPhoneValue) {
  const phoneMapping = getFieldMappingFor('Phone');
  if (!phoneMapping) {
    console.log(`⚠️  No field mapping found for Phone field`);
    return false;
  }
  
  return await syncField({
    direction: SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO,
    sourceId: recordId,
    fieldName: 'Phone',
    value: newPhoneValue,
    mapping: phoneMapping
  });
}

// Generic field sync functions (updated to use new internal functions)
async function syncFieldFromZohoToAirtable(leadId, zohoFieldName, newValue, mapping) {
  return await syncFromZohoToAirtable(leadId, zohoFieldName, newValue, mapping);
}

async function syncFieldFromAirtableToZoho(recordId, zohoFieldName, newValue, mapping) {
  return await syncFromAirtableToZoho(recordId, zohoFieldName, newValue, mapping);
}

// Create Airtable record when new Zoho lead is created
async function createAirtableRecordFromZohoLead(leadId, leadData) {
  console.log(`\n📝 Creating Airtable record for new Zoho lead ${leadId}...`);
  
  try {
    const recordData = { fields: {} };
    
    // Get Zoho CRM ID mapping
    const zohoCrmIdMapping = getZohoCrmIdMapping();
    if (zohoCrmIdMapping) {
      recordData.fields[zohoCrmIdMapping.airtable] = leadId;
      console.log(`📋 Adding Zoho CRM ID: ${leadId}`);
    }
    
    // Add phone if available and mapped
    const phoneMapping = getFieldMappingFor('Phone');
    if (phoneMapping && leadData[phoneMapping.zoho]) {
      recordData.fields[phoneMapping.airtable] = leadData[phoneMapping.zoho];
      console.log(`📞 Adding Phone: ${leadData[phoneMapping.zoho]}`);
    } else {
      console.log(`📞 No phone number available or mapped`);
    }
    
    console.log(`📝 Creating Airtable record with minimal fields:`, recordData.fields);
    
    const createdRecord = await createAirtableRecord(recordData);
    
    if (createdRecord) {
      console.log(`   - Zoho CRM ID: ${leadId}`);
      console.log(`   - Phone: ${phoneMapping ? leadData[phoneMapping.zoho] || 'Not provided' : 'Not mapped'}`);
    }
    
    return createdRecord;
  } catch (error) {
    console.error('❌ Error in createAirtableRecordFromZohoLead:', error.message);
    return null;
  }
}

// Handle Zoho lead update - check for field changes and sync
async function handleZohoLeadUpdate(leadId, leadData, changedFieldsInfo) {
  if (!changedFieldsInfo || !changedFieldsInfo.changedFields) {
    return;
  }
  
  try {
    // Check each changed field to see if it should be synced
    const syncPromises = [];
    
    for (const changedField of changedFieldsInfo.changedFields) {
      const mapping = getFieldMappingFor(changedField);
      
      if (mapping) {
        const newValue = changedFieldsInfo.currentValues[changedField];
        console.log(`🔄 ${changedField} field changed in Zoho lead ${leadId}: ${newValue}`);
        
        // Check if we should skip this sync to prevent loops
        if (!shouldSkipSync('zoho', leadId, changedField, newValue)) {
          // Add sync operation to promises array for parallel execution
          syncPromises.push(
            syncField({
              direction: SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE,
              sourceId: leadId,
              fieldName: changedField,
              value: newValue,
              mapping: mapping
            })
          );
        }
      }
    }
    
    // Execute all syncs in parallel
    if (syncPromises.length > 0) {
      const results = await Promise.allSettled(syncPromises);
      const successful = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
      const failed = results.filter(r => r.status === 'rejected' || r.value === false).length;
      
      console.log(`📊 Sync summary: ${successful} successful, ${failed} failed`);
      
      // Log any failures
      results.forEach((result, index) => {
        if (result.status === 'rejected') {
          console.error(`❌ Sync ${index} failed:`, result.reason);
        }
      });
    }
  } catch (error) {
    console.error('❌ Error in handleZohoLeadUpdate:', error.message);
  }
}

// Handle Airtable record update - check for field changes and sync
async function handleAirtableRecordUpdate(recordId, changedFieldsInfo) {
  console.log(`\n🔍 Processing Airtable record update for ${recordId}`);
  
  try {
    // Get cached field mappings
    const fieldMapping = fieldMappingCache.getFieldMapping();
    if (!fieldMapping) {
      console.warn(`⚠️  Field mapping cache not ready, skipping Airtable record update`);
      return;
    }
    console.log(`📋 Available field mappings: ${Object.keys(fieldMapping).join(', ')}`);
    
    const syncPromises = [];
    
    // Look for mapped field changes
    for (const fieldInfo of changedFieldsInfo) {
      console.log(`\n🔎 Checking field: ${fieldInfo.fieldName} (ID: ${fieldInfo.fieldId})`);
      
      // Find if this Airtable field is mapped to a Zoho field
      let mappedZohoField = null;
      let mapping = null;
      
      for (const [zohoField, fieldMap] of Object.entries(fieldMapping)) {
        // Match against the field ID (primary) or field name (fallback)
        if (fieldMap.airtable === fieldInfo.fieldId || fieldMap.airtable === fieldInfo.fieldName) {
          mappedZohoField = zohoField;
          mapping = fieldMap;
          console.log(`✅ Found mapping: Airtable ${fieldInfo.fieldName} (${fieldInfo.fieldId}) → Zoho ${zohoField}`);
          break;
        }
      }
      
      if (mappedZohoField && mapping) {
        console.log(`🔄 Syncing ${mappedZohoField} from Airtable to Zoho: ${JSON.stringify(fieldInfo.currentValue)}`);
        
        // Check if we should skip this sync to prevent loops
        const airtableFieldKey = fieldInfo.fieldId || fieldInfo.fieldName;
        if (!shouldSkipSync('airtable', recordId, airtableFieldKey, fieldInfo.currentValue)) {
          // Add sync operation to promises array for parallel execution
          syncPromises.push(
            syncField({
              direction: SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO,
              sourceId: recordId,
              fieldName: mappedZohoField,
              value: fieldInfo.currentValue,
              mapping: mapping
            })
          );
        } else {
          console.log(`⏭️  Skipping sync due to cooldown`);
        }
      } else {
        console.log(`⚠️  No Zoho mapping found for Airtable field: ${fieldInfo.fieldName || fieldInfo.fieldId}`);
      }
    }
    
    // Execute all syncs in parallel
    if (syncPromises.length > 0) {
      const results = await Promise.allSettled(syncPromises);
      const successful = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
      const failed = results.filter(r => r.status === 'rejected' || r.value === false).length;
      
      console.log(`📊 Sync summary: ${successful} successful, ${failed} failed`);
      
      // Log any failures
      results.forEach((result, index) => {
        if (result.status === 'rejected') {
          console.error(`❌ Sync ${index} failed:`, result.reason);
        }
      });
    }
  } catch (error) {
    console.error('❌ Error in handleAirtableRecordUpdate:', error.message);
  }
}

module.exports = {
  // Public API - simplified sync functions
  syncPhoneFromZohoToAirtable,
  syncPhoneFromAirtableToZoho,
  syncField,
  
  // Backward compatibility - generic functions
  syncFieldFromZohoToAirtable,
  syncFieldFromAirtableToZoho,
  
  // Record management
  createAirtableRecordFromZohoLead,
  findOrCreateAirtableRecord,
  
  // Update handlers
  handleZohoLeadUpdate,
  handleAirtableRecordUpdate,
  
  // Utilities
  getFieldMappingFor,
  getZohoCrmIdMapping,
  
  // Constants
  SYNC_DIRECTIONS
};