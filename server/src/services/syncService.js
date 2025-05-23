// server/src/services/syncService_new.js

// Config and Utils
const { FIELD_MAPPING: STATIC_FIELD_MAPPING } = require('../config/config'); // Keep for fallback
const fieldMappingCache = require('../utils/fieldMappingCache');
const { shouldSkipSync, recordSyncedValue } = require('../utils/syncTracker'); // Assuming recordSyncedValue exists or is part of shouldSkipSync logic

// Module-aware services
const {
  getRecordDetails,
  updateZohoRecord,
  createZohoRecord
} = require('./zohoService');

const {
  updateModuleRecord,
  createModuleRecord,
  findModuleRecordByZohoId,
  findZohoIdByModuleRecord,
  getFieldIdToNameMapping, // Assuming this is made module-aware or used with moduleConfig
  // getModuleConfig // Not directly used here, but services use it
} = require('./airtableService');

const SYNC_DIRECTIONS = {
  ZOHO_TO_AIRTABLE: 'zoho_to_airtable',
  AIRTABLE_TO_ZOHO: 'airtable_to_zoho'
};

// Helper function to get field mapping for a specific field using cache (module-aware)
async function getFieldMappingFor(zohoFieldName, module = 'Leads') {
  await fieldMappingCache.ensureModuleInitialized(module);
  const fieldMapping = fieldMappingCache.getFieldMapping(module);

  if (!fieldMapping) {
    if (STATIC_FIELD_MAPPING[zohoFieldName]) {
      return STATIC_FIELD_MAPPING[zohoFieldName];
    }
    console.warn(`[SyncService][${module}] No field mapping found in cache or static config for Zoho field: ${zohoFieldName}`);
    return null;
  }

  if (fieldMapping[zohoFieldName]) {
    return fieldMapping[zohoFieldName];
  }

  for (const [_key, mappingEntry] of Object.entries(fieldMapping)) {
    if (mappingEntry.zoho === zohoFieldName) {
      return mappingEntry;
    }
  }
  console.warn(`[SyncService][${module}] Field mapping for Zoho field '${zohoFieldName}' not found.`);
  return null;
}

// Helper function to get Zoho CRM ID field mapping using cache (module-aware)
async function getZohoCrmIdMapping(module = 'Leads') {
  await fieldMappingCache.ensureModuleInitialized(module);
  const fieldMapping = fieldMappingCache.getFieldMapping(module);
  const zohoCrmIdKey = 'ZOHO_ID';

  if (!fieldMapping) {
    console.warn(`[SyncService][${module}] Field mapping cache not available for Zoho CRM ID.`);
    return STATIC_FIELD_MAPPING.ZOHO_ID;
  }

  if (fieldMapping[zohoCrmIdKey] || fieldMapping['Zoho CRM ID']) {
    return fieldMapping[zohoCrmIdKey] || fieldMapping['Zoho CRM ID'];
  }
  console.warn(`[SyncService][${module}] Zoho CRM ID mapping not found in dynamic cache. Falling back to static.`);
  return STATIC_FIELD_MAPPING.ZOHO_ID;
}

// Helper function to get Airtable ID field mapping using cache (module-aware)
async function getAirtableIdMapping(module = 'Leads') {
  await fieldMappingCache.ensureModuleInitialized(module);
  const fieldMapping = fieldMappingCache.getFieldMapping(module);
  const airtableIdKey = 'AIRTABLE_ID';

  if (!fieldMapping) {
    console.warn(`[SyncService][${module}] Field mapping cache not available for Airtable Record ID.`);
    return STATIC_FIELD_MAPPING.AIRTABLE_ID;
  }

  if (fieldMapping[airtableIdKey] || fieldMapping['Airtable_Record_ID']) {
    return fieldMapping[airtableIdKey] || fieldMapping['Airtable_Record_ID'];
  }
  console.warn(`[SyncService][${module}] Airtable Record ID mapping not found in dynamic cache. Falling back to static.`);
  return STATIC_FIELD_MAPPING.AIRTABLE_ID;
}

// Helper function to find or create Airtable record for a Zoho record (module-aware)
async function findOrCreateAirtableRecord(zohoRecordId, module = 'Leads') {
  let airtableRecordId = await findModuleRecordByZohoId(zohoRecordId, module);

  if (!airtableRecordId) {
    const recordDetails = await getRecordDetails(zohoRecordId, module);
    if (recordDetails && recordDetails.data && recordDetails.data[0]) {
      const createdRecord = await createAirtableRecordFromZoho(zohoRecordId, recordDetails.data[0], module);
      if (createdRecord && createdRecord.id) {
        airtableRecordId = createdRecord.id;
      } else {
        console.error(`[SyncService][${module}] Failed to create Airtable record for Zoho ID ${zohoRecordId}.`);
        return null;
      }
    } else {
      console.error(`[SyncService][${module}] Could not fetch details for Zoho ID ${zohoRecordId} to create Airtable record.`);
      return null;
    }
  }
  return airtableRecordId;
}

// Generic sync function that handles both directions (module-aware)
async function syncField(params) {
  const {
    direction,
    sourceId,
    fieldName, // Zoho Field Name
    value,
    mapping,
    module = 'Leads'
  } = params;

  if (!mapping || !mapping.zoho || !mapping.airtable) {
    console.error(`[SyncService][${module}] Invalid mapping provided for field ${fieldName}. Cannot sync. Mapping:`, mapping);
    return false;
  }

  // Record sync intent before actual operation
  // For ZOHO_TO_AIRTABLE, sourceId is Zoho ID, target is Airtable. We record for Airtable.
  // For AIRTABLE_TO_ZOHO, sourceId is Airtable ID, target is Zoho. We record for Zoho.
  if (direction === SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE) {
    // We need the target Airtable ID to record sync. This is tricky if it's a new record.
    // This pre-emptive recording might need to happen inside syncFromZohoToAirtable after ID is known.
    // For now, we rely on shouldSkipSync check before calling syncField.
  } else if (direction === SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO) {
    const zohoTargetId = await findZohoIdByModuleRecord(sourceId, module);
    if (zohoTargetId) {
        recordSyncedValue('zoho', zohoTargetId, mapping.zoho, value);
    }
  }


  if (direction === SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE) {
    return await syncFromZohoToAirtable(sourceId, fieldName, value, mapping, module);
  } else if (direction === SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO) {
    return await syncFromAirtableToZoho(sourceId, fieldName, value, mapping, module);
  } else {
    console.error(`[SyncService][${module}] Invalid sync direction: ${direction}`);
    throw new Error(`Invalid sync direction: ${direction}`);
  }
}

// Internal function to sync from Zoho to Airtable (module-aware)
async function syncFromZohoToAirtable(zohoRecordId, zohoFieldName, newValue, mapping, module = 'Leads') {
  try {
    const airtableRecordId = await findOrCreateAirtableRecord(zohoRecordId, module);
    if (!airtableRecordId) {
      console.error(`[SyncService][${module}] Could not find or create Airtable record for Zoho ID ${zohoRecordId}. Sync failed for field ${zohoFieldName}.`);
      return false;
    }

    const airtableField = mapping.airtable;
    recordSyncedValue('airtable', airtableRecordId, airtableField, newValue); // Record before update

    const fieldUpdates = { [airtableField]: newValue };
    await updateModuleRecord(airtableRecordId, fieldUpdates, module);
    console.log(`[SyncService][${module}] Synced Zoho field '${zohoFieldName}' (ID: ${zohoRecordId}) to Airtable record ${airtableRecordId}, field '${airtableField}'.`);
    return true;
  } catch (error) {
    console.error(`[SyncService][${module}] Error syncing Zoho field '${zohoFieldName}' (ID: ${zohoRecordId}) to Airtable: ${error.message}`, error.stack);
    return false;
  }
}

// Internal function to sync from Airtable to Zoho (module-aware)
async function syncFromAirtableToZoho(airtableRecordId, zohoFieldName, newValue, mapping, module = 'Leads') {
  try {
    const zohoRecordId = await findZohoIdByModuleRecord(airtableRecordId, module);
    if (!zohoRecordId) {
      // Option: Create Zoho record if it doesn't exist?
      // For now, assume it must exist for field-level sync from Airtable.
      // Creation is handled by createZohoRecordFromAirtable.
      console.warn(`[SyncService][${module}] Zoho record not found for Airtable ID ${airtableRecordId}. Cannot sync field ${zohoFieldName}.`);
      return false;
    }

    const zohoApiName = mapping.zoho; // This should be the zohoFieldName
    // recordSyncedValue('zoho', zohoRecordId, zohoApiName, newValue); // Already done in syncField

    const fieldUpdates = { [zohoApiName]: newValue };
    await updateZohoRecord(zohoRecordId, fieldUpdates, module);
    console.log(`[SyncService][${module}] Synced Airtable record ${airtableRecordId} (field for Zoho: '${zohoApiName}') to Zoho record ${zohoRecordId}.`);
    return true;
  } catch (error) {
    console.error(`[SyncService][${module}] Error syncing Airtable record ${airtableRecordId} (field for Zoho: '${zohoFieldName}') to Zoho: ${error.message}`, error.stack);
    return false;
  }
}

// Simplified generic field sync functions (module-aware)
async function syncPhoneFromZohoToAirtable(zohoRecordId, newPhoneValue, module = 'Leads') {
  const phoneMapping = await getFieldMappingFor('Phone', module);
  if (!phoneMapping) {
    console.error(`[SyncService][${module}] Phone mapping not found. Cannot sync phone from Zoho ${zohoRecordId}.`);
    return false;
  }
  if (shouldSkipSync('zoho', zohoRecordId, 'Phone', newPhoneValue)) return true; // Already synced

  return await syncField({
    direction: SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE,
    sourceId: zohoRecordId,
    fieldName: 'Phone',
    value: newPhoneValue,
    mapping: phoneMapping,
    module: module
  });
}

async function syncPhoneFromAirtableToZoho(airtableRecordId, newPhoneValue, module = 'Leads') {
  const phoneMapping = await getFieldMappingFor('Phone', module);
  if (!phoneMapping) {
    console.error(`[SyncService][${module}] Phone mapping not found. Cannot sync phone from Airtable ${airtableRecordId}.`);
    return false;
  }
  // Assuming mapping.airtable is the Airtable field name for Phone
  if (shouldSkipSync('airtable', airtableRecordId, phoneMapping.airtable, newPhoneValue)) return true;

  return await syncField({
    direction: SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO,
    sourceId: airtableRecordId,
    fieldName: 'Phone', // Target Zoho Field Name
    value: newPhoneValue,
    mapping: phoneMapping,
    module: module
  });
}

// Generic field sync functions (module-aware wrappers)
async function syncFieldFromZohoToAirtable(zohoRecordId, zohoFieldName, newValue, mapping, module = 'Leads') {
  if (shouldSkipSync('zoho', zohoRecordId, zohoFieldName, newValue)) return true;
  return await syncFromZohoToAirtable(zohoRecordId, zohoFieldName, newValue, mapping, module);
}

async function syncFieldFromAirtableToZoho(airtableRecordId, zohoFieldName, newValue, mapping, module = 'Leads') {
  if (shouldSkipSync('airtable', airtableRecordId, mapping.airtable, newValue)) return true;
  return await syncFromAirtableToZoho(airtableRecordId, zohoFieldName, newValue, mapping, module);
}

// Create Airtable record when new Zoho record is created (module-aware)
async function createAirtableRecordFromZoho(zohoRecordId, zohoRecordData, module = 'Leads') {
  try {
    const recordDataToCreate = { fields: {} };
    await fieldMappingCache.ensureModuleInitialized(module);

    const zohoCrmIdMap = await getZohoCrmIdMapping(module);
    if (zohoCrmIdMap && zohoCrmIdMap.airtable) {
      recordDataToCreate.fields[zohoCrmIdMap.airtable] = zohoRecordId;
    } else {
      console.warn(`[SyncService][${module}] Zoho CRM ID mapping to Airtable not found. Cannot set Zoho ID on new Airtable record.`);
    }

    const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(module);
    if (currentModuleFieldMapping) {
      for (const [zohoFieldApiName, mapping] of Object.entries(currentModuleFieldMapping)) {
        if (zohoRecordData[zohoFieldApiName] !== undefined &&
            zohoRecordData[zohoFieldApiName] !== null &&
            mapping.airtable && // Ensure airtable field is defined in mapping
            (mapping.airtable !== (zohoCrmIdMap?.airtable))) { // Don't map ZOHO_ID again

          let value = zohoRecordData[zohoFieldApiName];
          if (typeof value === 'object' && value !== null) {
            if (value.name) value = value.name;
            else if (Array.isArray(value)) value = value.map(item => (typeof item === 'object' && item.name) ? item.name : String(item)).join(', ');
            else if (mapping.isLookupToString || Object.keys(value).length > 0) { // Heuristic for complex objects
                 // Skip complex objects like Layout, $line_tax etc. unless specifically mapped as string
                if ((zohoFieldApiName.startsWith('$') || ['Owner', 'Layout'].includes(zohoFieldApiName)) && !mapping.isLookupToString) {
                    console.log(`[SyncService][${module}] Skipping complex Zoho field '${zohoFieldApiName}' during Airtable creation for Zoho ID ${zohoRecordId}.`);
                    continue;
                }
                value = JSON.stringify(value);
            } else {
                value = null;
            }
          }
          if (value !== null && String(value).trim() !== '') {
            recordDataToCreate.fields[mapping.airtable] = value;
          }
        }
      }
    } else {
      console.warn(`[SyncService][${module}] No field mapping found for module. Airtable record for Zoho ID ${zohoRecordId} might be incomplete.`);
    }
    
    if (Object.keys(recordDataToCreate.fields).length === 0 || (Object.keys(recordDataToCreate.fields).length === 1 && recordDataToCreate.fields[zohoCrmIdMap?.airtable])) {
        console.warn(`[SyncService][${module}] No mappable fields (besides Zoho ID) for Zoho ID ${zohoRecordId}. Airtable record not created.`);
        return null;
    }

    const createdRecord = await createModuleRecord(recordDataToCreate, module);
    console.log(`[SyncService][${module}] Created Airtable record ${createdRecord?.id} from Zoho record ${zohoRecordId}.`);
    return createdRecord;
  } catch (error) {
    console.error(`[SyncService][${module}] Error in createAirtableRecordFromZoho for Zoho ID ${zohoRecordId}: ${error.message}`, error.stack);
    return null;
  }
}

// Create Zoho record from Airtable record (module-aware)
async function createZohoRecordFromAirtable(airtableRecordId, airtableRecordData, module = 'Leads') {
  try {
    const zohoRecordPayload = {};
    await fieldMappingCache.ensureModuleInitialized(module);
    const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(module);

    if (!currentModuleFieldMapping) {
      console.error(`[SyncService][${module}] No field mapping found. Cannot create Zoho record from Airtable ID ${airtableRecordId}.`);
      return null;
    }

    // airtableService.getFieldIdToNameMapping might need module context if Airtable base structure differs
    const airtableFieldIdToName = await getFieldIdToNameMapping(null, module); // Pass module

    for (const [zohoFieldApiName, mapping] of Object.entries(currentModuleFieldMapping)) {
      if (!mapping.airtable || !mapping.zoho) continue; // Skip incomplete mappings

      // Airtable data in webhooks usually comes with field names.
      // If mapping.airtable is an ID, we'd need to resolve it. Assuming it's field name for now.
      const airtableFieldName = mapping.airtable;
      const airtableValue = airtableRecordData.fields[airtableFieldName];

      if (airtableValue !== undefined && airtableValue !== null && String(airtableValue).trim() !== '') {
        // TODO: Add data type transformation if needed based on Zoho field type
        zohoRecordPayload[mapping.zoho] = airtableValue;
      }
    }
    
    const airtableIdMap = await getAirtableIdMapping(module);
    if (airtableIdMap && airtableIdMap.zoho) {
      zohoRecordPayload[airtableIdMap.zoho] = airtableRecordId;
    } else {
      console.warn(`[SyncService][${module}] Airtable Record ID mapping to Zoho not found. Cannot set Airtable ID on new Zoho record.`);
    }

    if (Object.keys(zohoRecordPayload).length === 0 || (Object.keys(zohoRecordPayload).length === 1 && zohoRecordPayload[airtableIdMap?.zoho])) {
        console.warn(`[SyncService][${module}] No mappable fields (besides Airtable ID) for Airtable ID ${airtableRecordId}. Zoho record not created.`);
        return null;
    }
    
    const createResponse = await createZohoRecord(zohoRecordPayload, module);

    if (createResponse && createResponse.data && createResponse.data[0] && createResponse.data[0].details) {
      const newZohoRecordDetails = createResponse.data[0].details;
      const newZohoRecordId = newZohoRecordDetails.id;

      const zohoCrmIdMap = await getZohoCrmIdMapping(module);
      if (zohoCrmIdMap && zohoCrmIdMap.airtable && newZohoRecordId) {
        const airtableUpdatePayload = { [zohoCrmIdMap.airtable]: newZohoRecordId };
        // Record sync before updating Airtable to prevent loop from this update
        recordSyncedValue('airtable', airtableRecordId, zohoCrmIdMap.airtable, newZohoRecordId);
        await updateModuleRecord(airtableRecordId, airtableUpdatePayload, module);
        console.log(`[SyncService][${module}] Updated Airtable record ${airtableRecordId} with new Zoho ID ${newZohoRecordId}.`);
      }
      console.log(`[SyncService][${module}] Created Zoho record ${newZohoRecordId} from Airtable record ${airtableRecordId}.`);
      return { id: newZohoRecordId, data: newZohoRecordDetails };
    }
    console.error(`[SyncService][${module}] Failed to create Zoho record from Airtable ID ${airtableRecordId}. Response:`, JSON.stringify(createResponse));
    return null;
  } catch (error) {
    console.error(`[SyncService][${module}] Error creating Zoho record from Airtable ID ${airtableRecordId}: ${error.message}`, error.stack);
    return null;
  }
}

// Handle Zoho record update - check for field changes and sync (module-aware)
async function handleZohoRecordUpdate(zohoRecordId, _zohoRecordData, changedFieldsInfo, module = 'Leads') {
  if (!changedFieldsInfo || !changedFieldsInfo.changedFields || !changedFieldsInfo.currentValues) {
    console.warn(`[SyncService][${module}] Insufficient data in changedFieldsInfo for Zoho record ${zohoRecordId}. Cannot process update.`);
    return;
  }

  try {
    const syncPromises = [];
    await fieldMappingCache.ensureModuleInitialized(module);

    for (const zohoFieldApiName of changedFieldsInfo.changedFields) {
      const mapping = await getFieldMappingFor(zohoFieldApiName, module);
      if (mapping) {
        const newValue = changedFieldsInfo.currentValues[zohoFieldApiName];
        if (!shouldSkipSync('zoho', zohoRecordId, zohoFieldApiName, newValue)) {
          syncPromises.push(
            syncField({ // syncField itself will call syncFromZohoToAirtable
              direction: SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE,
              sourceId: zohoRecordId,
              fieldName: zohoFieldApiName,
              value: newValue,
              mapping: mapping,
              module: module
            })
          );
        } else {
          console.log(`[SyncService][${module}] Skipping sync for Zoho field ${zohoFieldApiName} (ID: ${zohoRecordId}) due to recent sync.`);
        }
      }
    }
    
    if (syncPromises.length > 0) {
      const results = await Promise.allSettled(syncPromises);
      results.forEach((result, index) => {
        if (result.status === 'rejected' || (result.status === 'fulfilled' && !result.value)) {
          console.error(`[SyncService][${module}] Failed or unsuccessful sync operation for Zoho record ${zohoRecordId}, field index ${index}. Reason/Result:`, result.status === 'rejected' ? result.reason : result.value);
        }
      });
    }
  } catch (error) {
    console.error(`[SyncService][${module}] Error in handleZohoRecordUpdate for ID ${zohoRecordId}: ${error.message}`, error.stack);
  }
}

// Handle Airtable record update - check for field changes and sync (module-aware)
async function handleAirtableRecordUpdate(airtableRecordId, changedFieldsInfo, module = 'Leads') {
  // changedFieldsInfo is an array: [{ fieldId: 'fldxxxx', fieldName: 'Airtable Field Name', currentValue: 'new value' }, ...]
  try {
    await fieldMappingCache.ensureModuleInitialized(module);
    const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(module);
    if (!currentModuleFieldMapping) {
      console.error(`[SyncService][${module}] Field mapping not available. Cannot handle Airtable record update for ${airtableRecordId}.`);
      return;
    }

    const syncPromises = [];

    for (const fieldInfo of changedFieldsInfo) {
      let mappedZohoFieldApiName = null;
      let mappingDetails = null;

      for (const [zohoApiName, map] of Object.entries(currentModuleFieldMapping)) {
        if (map.airtable === fieldInfo.fieldName) { // Assuming mapping.airtable stores Airtable Field Name
          mappedZohoFieldApiName = zohoApiName;
          mappingDetails = map;
          break;
        }
      }

      if (mappedZohoFieldApiName && mappingDetails) {
        if (!shouldSkipSync('airtable', airtableRecordId, fieldInfo.fieldName, fieldInfo.currentValue)) {
          syncPromises.push(
            syncField({ // syncField itself will call syncFromAirtableToZoho
              direction: SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO,
              sourceId: airtableRecordId,
              fieldName: mappedZohoFieldApiName, // Target Zoho Field API Name
              value: fieldInfo.currentValue,
              mapping: mappingDetails,
              module: module
            })
          );
        } else {
          console.log(`[SyncService][${module}] Skipping sync for Airtable field ${fieldInfo.fieldName} (ID: ${airtableRecordId}) due to recent sync.`);
        }
      }
    }
    
    if (syncPromises.length > 0) {
      const results = await Promise.allSettled(syncPromises);
      results.forEach((result, index) => {
         if (result.status === 'rejected' || (result.status === 'fulfilled' && !result.value)) {
          console.error(`[SyncService][${module}] Failed or unsuccessful sync operation for Airtable record ${airtableRecordId}, field index ${index}. Reason/Result:`, result.status === 'rejected' ? result.reason : result.value);
        }
      });
    }
  } catch (error) {
    console.error(`[SyncService][${module}] Error in handleAirtableRecordUpdate for ID ${airtableRecordId}: ${error.message}`, error.stack);
  }
}

module.exports = {
  syncPhoneFromZohoToAirtable,
  syncPhoneFromAirtableToZoho,
  syncField, // Core generic sync function
  
  syncFieldFromZohoToAirtable, // Specific direction, useful for direct calls
  syncFieldFromAirtableToZoho, // Specific direction, useful for direct calls
  
  createAirtableRecordFromZoho,
  createZohoRecordFromAirtable,
  findOrCreateAirtableRecord,
  
  handleZohoRecordUpdate,
  handleAirtableRecordUpdate,
  
  getFieldMappingFor, // Utility
  getZohoCrmIdMapping, // Utility
  getAirtableIdMapping, // Utility
  
  SYNC_DIRECTIONS // Constants
};
