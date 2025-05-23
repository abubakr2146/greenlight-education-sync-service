const { FIELD_MAPPING } = require('../config/config');
const fieldMappingCache = require('../utils/fieldMappingCache');
const { shouldSkipSync } = require('../utils/syncTracker');
const { getLeadDetails, updateZohoLead, createZohoLead } = require('./zohoService');
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

// Helper function to get Airtable ID field mapping using cache
function getAirtableIdMapping() {
  const fieldMapping = fieldMappingCache.getFieldMapping();
  
  if (!fieldMapping) {
    return FIELD_MAPPING.AIRTABLE_ID;
  }
  
  // Look for AIRTABLE_ID or equivalent in dynamic mapping
  if (fieldMapping.AIRTABLE_ID || fieldMapping['Airtable_Record_ID']) {
    return fieldMapping.AIRTABLE_ID || fieldMapping['Airtable_Record_ID'];
  }
  
  // Fallback to static mapping
  return FIELD_MAPPING.AIRTABLE_ID;
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
    
    // Get field mapping and populate mapped fields from Zoho lead data
    const fieldMapping = fieldMappingCache.getFieldMapping();
    if (fieldMapping) {
      for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
        if (leadData[zohoField] !== undefined && leadData[zohoField] !== null && leadData[zohoField] !== '') {
          // Skip the Zoho CRM ID field as we already handled it
          if (mapping.airtable !== (zohoCrmIdMapping?.airtable)) {
            let value = leadData[zohoField];
            
            // Transform complex objects to strings or skip problematic fields
            if (typeof value === 'object' && value !== null) {
              if (zohoField === 'Owner') {
                // Skip Owner field for now as it's likely a linked record field in Airtable
                continue;
              } else if (Array.isArray(value)) {
                // For arrays, join with commas
                value = value.join(', ');
              } else {
                // For other objects, use JSON string or specific property
                value = value.name || value.value || JSON.stringify(value);
              }
            }
            
            // Additional validation for field types that might cause issues
            if (typeof value === 'string' && value.trim() === '') {
              continue;
            }
            
            recordData.fields[mapping.airtable] = value;
          }
        }
      }
    } else {
      // Fallback: add phone if available and mapped
      const phoneMapping = getFieldMappingFor('Phone');
      if (phoneMapping && leadData[phoneMapping.zoho]) {
        recordData.fields[phoneMapping.airtable] = leadData[phoneMapping.zoho];
      }
    }
    
    const createdRecord = await createAirtableRecord(recordData);
    
    return createdRecord;
  } catch (error) {
    console.log(`❌ Error in createAirtableRecordFromZohoLead: ${error.message}`);
    return null;
  }
}

// Create Zoho lead from Airtable record
async function createZohoLeadFromAirtableRecord(recordId, recordData) {
  try {
    const leadData = {};
    
    // Get field mapping
    const fieldMapping = fieldMappingCache.getFieldMapping();
    if (!fieldMapping) {
      return null;
    }
    
    // Get field ID to name mapping for translating Airtable field IDs
    const { getFieldIdToNameMapping } = require('./airtableService');
    const fieldIdToName = await getFieldIdToNameMapping();
    
    // Map Airtable fields to Zoho fields
    for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
      const airtableFieldId = mapping.airtable;
      const airtableFieldName = fieldIdToName[airtableFieldId] || airtableFieldId;
      const airtableValue = recordData.fields[airtableFieldName];
      
      if (airtableValue !== undefined && airtableValue !== null && airtableValue !== '') {
        leadData[zohoField] = airtableValue;
      }
    }
    
    // Add the Airtable record ID to the lead data before creating
    const airtableIdMapping = getAirtableIdMapping();
    if (airtableIdMapping) {
      leadData[airtableIdMapping.zoho] = recordId;
    }
    
    // Create the lead in Zoho
    console.log(`🔄 Creating Zoho lead with data:`, Object.keys(leadData).length, 'fields');
    const createResponse = await createZohoLead(leadData);
    
    if (createResponse && createResponse.data && createResponse.data[0]) {
      const newZohoLead = createResponse.data[0];
      const newZohoLeadId = newZohoLead.details.id;
      
      // Update the Airtable record with the new Zoho CRM ID
      const zohoCrmIdMapping = getZohoCrmIdMapping();
      if (zohoCrmIdMapping && newZohoLeadId) {
        const fieldUpdates = {
          [zohoCrmIdMapping.airtable]: newZohoLeadId
        };
        console.log(`📝 Updating Airtable record ${recordId} with Zoho CRM ID ${newZohoLeadId}`);
        const updateResult = await updateAirtableRecord(recordId, fieldUpdates);
        if (!updateResult) {
          console.log(`❌ Failed to update Airtable record with Zoho CRM ID`);
        }
      }
      
      console.log(`✅ Created Zoho lead ${newZohoLeadId} from Airtable record ${recordId}`);
      return {
        id: newZohoLeadId,
        data: newZohoLead.details
      };
    }
    
    console.log(`❌ Failed to create Zoho lead - no valid response`);
    return null;
  } catch (error) {
    console.log(`❌ Error creating Zoho lead from Airtable:`, error.message);
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
  createZohoLeadFromAirtableRecord,
  findOrCreateAirtableRecord,
  
  // Update handlers
  handleZohoLeadUpdate,
  handleAirtableRecordUpdate,
  
  // Utilities
  getFieldMappingFor,
  getZohoCrmIdMapping,
  getAirtableIdMapping,
  
  // Constants
  SYNC_DIRECTIONS
};