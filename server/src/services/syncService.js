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
    const leadDetails = await getLeadDetails(leadId);
    if (leadDetails && leadDetails.data && leadDetails.data[0]) {
      const createdRecord = await createAirtableRecordFromZohoLead(leadId, leadDetails.data[0]);
      if (createdRecord) {
        airtableRecordId = createdRecord.id;
      } else {
        return null;
      }
    } else {
      return null;
    }
  }
  
  return airtableRecordId;
}

// Helper function to record sync to prevent loops
function recordSyncToPreventLoop(system, recordId, fieldId, value) {
  shouldSkipSync(system, recordId, fieldId, value);
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
    
    return true;
  } catch (error) {
    return false;
  }
}

// Internal function to sync from Airtable to Zoho
async function syncFromAirtableToZoho(recordId, zohoFieldName, newValue, mapping) {
  try {
    // Find corresponding Zoho lead
    const zohoLeadId = await findZohoLeadByAirtableId(recordId);
    if (!zohoLeadId) {
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
    
    return true;
  } catch (error) {
    return false;
  }
}

// Simplified phone sync functions (now just thin wrappers)
async function syncPhoneFromZohoToAirtable(leadId, newPhoneValue) {
  const phoneMapping = getFieldMappingFor('Phone');
  if (!phoneMapping) {
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
  try {
    const recordData = { fields: {} };
    
    // Get Zoho CRM ID mapping
    const zohoCrmIdMapping = getZohoCrmIdMapping();
    if (zohoCrmIdMapping) {
      recordData.fields[zohoCrmIdMapping.airtable] = leadId;
    }
    
    // Add phone if available and mapped
    const phoneMapping = getFieldMappingFor('Phone');
    if (phoneMapping && leadData[phoneMapping.zoho]) {
      recordData.fields[phoneMapping.airtable] = leadData[phoneMapping.zoho];
    }
    
    const createdRecord = await createAirtableRecord(recordData);
    
    return createdRecord;
  } catch (error) {
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
      
      // Log any failures
      results.forEach((result, index) => {
        if (result.status === 'rejected') {
          // Sync failed
        }
      });
    }
  } catch (error) {
    // Error in sync
  }
}

// Handle Airtable record update - check for field changes and sync
async function handleAirtableRecordUpdate(recordId, changedFieldsInfo) {
  try {
    // Get cached field mappings
    const fieldMapping = fieldMappingCache.getFieldMapping();
    if (!fieldMapping) {
      return;
    }
    
    const syncPromises = [];
    
    // Look for mapped field changes
    for (const fieldInfo of changedFieldsInfo) {
      // Find if this Airtable field is mapped to a Zoho field
      let mappedZohoField = null;
      let mapping = null;
      
      for (const [zohoField, fieldMap] of Object.entries(fieldMapping)) {
        // Match against the field ID (primary) or field name (fallback)
        if (fieldMap.airtable === fieldInfo.fieldId || fieldMap.airtable === fieldInfo.fieldName) {
          mappedZohoField = zohoField;
          mapping = fieldMap;
          break;
        }
      }
      
      if (mappedZohoField && mapping) {
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
        }
      }
    }
    
    // Execute all syncs in parallel
    if (syncPromises.length > 0) {
      const results = await Promise.allSettled(syncPromises);
      const successful = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
      const failed = results.filter(r => r.status === 'rejected' || r.value === false).length;
      
      // Log any failures
      results.forEach((result, index) => {
        if (result.status === 'rejected') {
          // Sync failed
        }
      });
    }
  } catch (error) {
    // Error in sync
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