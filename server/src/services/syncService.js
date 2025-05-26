// server/src/services/syncService_new.js

// Config and Utils
const { FIELD_MAPPING: STATIC_FIELD_MAPPING } = require('../config/config'); // Keep for fallback
const fieldMappingCache = require('../utils/fieldMappingCache');
const { shouldSkipSync, recordSyncedValue } = require('../utils/syncTracker');

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
  getFieldIdToNameMapping,
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
    fieldName, 
    value,
    mapping,
    module = 'Leads',
    targetRecordId = null // New parameter for known target record ID
  } = params;

  if (!mapping || !mapping.zoho || !mapping.airtable) {
    console.error(`[SyncService][${module}] Invalid mapping provided for field ${fieldName}. Cannot sync. Mapping:`, mapping);
    return false;
  }

  if (direction === SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE) {
    // Recording for ZOHO_TO_AIRTABLE is handled within syncFromZohoToAirtable
  } else if (direction === SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO) {
    const zohoTargetId = targetRecordId || await findZohoIdByModuleRecord(sourceId, module);
    if (zohoTargetId) {
        recordSyncedValue('zoho', zohoTargetId, mapping.zoho, value);
    } else {
        // If zohoTargetId is not found, it implies a new record creation scenario or an issue.
        // For updates, zohoTargetId should exist. For creations, this pre-emptive recordSyncedValue might not be applicable here.
        // Creation path (createZohoRecordFromAirtable) handles its own logic.
    }
  }

  if (direction === SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE) {
    return await syncFromZohoToAirtable(sourceId, fieldName, value, mapping, module, targetRecordId);
  } else if (direction === SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO) {
    return await syncFromAirtableToZoho(sourceId, fieldName, value, mapping, module, targetRecordId);
  } else {
    console.error(`[SyncService][${module}] Invalid sync direction: ${direction}`);
    throw new Error(`Invalid sync direction: ${direction}`);
  }
}

// Internal function to sync from Zoho to Airtable (module-aware)
async function syncFromZohoToAirtable(zohoRecordId, zohoFieldName, newValue, mapping, module = 'Leads', knownAirtableRecordId = null) {
  try {
    // Use the known Airtable record ID if provided, otherwise look it up
    const airtableRecordId = knownAirtableRecordId || await findOrCreateAirtableRecord(zohoRecordId, module);
    if (!airtableRecordId) {
      console.error(`[SyncService][${module}] Could not find or create Airtable record for Zoho ID ${zohoRecordId}. Sync failed for field ${zohoFieldName}.`);
      return false;
    }

    const airtableFieldIdOrName = mapping.airtable; 
    recordSyncedValue('airtable', airtableRecordId, airtableFieldIdOrName, newValue); 

    const fieldUpdates = { [airtableFieldIdOrName]: newValue };
    await updateModuleRecord(airtableRecordId, fieldUpdates, module);
    console.log(`[SyncService][${module}] Synced Zoho field '${zohoFieldName}' (ID: ${zohoRecordId}) to Airtable record ${airtableRecordId}, field '${airtableFieldIdOrName}'.`);
    return true;
  } catch (error) {
    console.error(`[SyncService][${module}] Error syncing Zoho field '${zohoFieldName}' (ID: ${zohoRecordId}) to Airtable: ${error.message}`, error.stack);
    return false;
  }
}

// Internal function to sync from Airtable to Zoho (module-aware)
async function syncFromAirtableToZoho(airtableRecordId, zohoFieldName, newValue, mapping, module = 'Leads', knownZohoRecordId = null) {
  try {
    const zohoRecordId = knownZohoRecordId || await findZohoIdByModuleRecord(airtableRecordId, module);
    if (!zohoRecordId) {
      // This case should ideally be handled by createZohoRecordFromAirtable if the record is new.
      // If we reach here, it means we expected an existing Zoho record to update.
      console.warn(`[SyncService][${module}] Zoho record not found for Airtable ID ${airtableRecordId} during field sync. Cannot sync field ${zohoFieldName}. This might indicate an issue if an update was expected.`);
      return false; 
    }

    const zohoApiName = mapping.zoho;
    // recordSyncedValue is called in syncField for A->Z

    const fieldUpdates = { [zohoApiName]: newValue };
    await updateZohoRecord(zohoRecordId, fieldUpdates, module);
    console.log(`[SyncService][${module}] Synced Airtable record ${airtableRecordId} (field for Zoho: '${zohoApiName}') to Zoho record ${zohoRecordId}.`);
    return true;
  } catch (error) {
    console.error(`[SyncService][${module}] Error syncing Airtable record ${airtableRecordId} (field for Zoho: '${zohoFieldName}') to Zoho: ${error.message}`, error.stack);
    return false;
  }
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
    const { IGNORED_FIELDS } = require('../config/config');
    const { getFieldIdToNameMapping } = require('./airtableService');
    const recordDataToCreate = { fields: {} };
    await fieldMappingCache.ensureModuleInitialized(module);

    const zohoCrmIdMap = await getZohoCrmIdMapping(module); 
    const airtableZohoIdField = zohoCrmIdMap?.airtable;

    if (airtableZohoIdField) {
      recordDataToCreate.fields[airtableZohoIdField] = zohoRecordId;
    } else {
      console.warn(`[SyncService][${module}] Zoho CRM ID mapping to Airtable not found. Cannot set Zoho ID on new Airtable record.`);
    }

    // Get field ID to name mapping for checking ignored fields
    const airtableFieldIdToNameMap = await getFieldIdToNameMapping(null, module);

    const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(module);
    if (currentModuleFieldMapping) {
      for (const [zohoFieldApiName, mapping] of Object.entries(currentModuleFieldMapping)) {
        if (zohoFieldApiName === 'ZOHO_ID' || zohoFieldApiName === 'AIRTABLE_ID') continue; // Skip special mapping keys
        
        // Skip ignored fields - check both by ID and by name
        let airtableFieldName = mapping.airtable;
        if (mapping.airtable && mapping.airtable.startsWith('fld') && airtableFieldIdToNameMap[mapping.airtable]) {
          airtableFieldName = airtableFieldIdToNameMap[mapping.airtable];
        }
        
        if (IGNORED_FIELDS.zoho.includes(zohoFieldApiName) || 
            IGNORED_FIELDS.airtable.includes(mapping.airtable) ||
            IGNORED_FIELDS.airtable.includes(airtableFieldName)) {
          continue;
        }

        if (zohoRecordData[zohoFieldApiName] !== undefined &&
            zohoRecordData[zohoFieldApiName] !== null &&
            mapping.airtable && 
            (mapping.airtable !== airtableZohoIdField)) { 

          let value = zohoRecordData[zohoFieldApiName];
          if (typeof value === 'object' && value !== null) {
            if (value.name) value = value.name; 
            else if (Array.isArray(value)) value = value.map(item => (typeof item === 'object' && item.name) ? item.name : String(item)).join(', ');
            else if (mapping.isLookupToString || Object.keys(value).length > 0) {
                if ((zohoFieldApiName.startsWith('$') || ['Owner', 'Layout'].includes(zohoFieldApiName)) && !mapping.isLookupToString) {
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
    
    const payloadKeys = Object.keys(recordDataToCreate.fields);
    if (payloadKeys.length === 0 || (payloadKeys.length === 1 && airtableZohoIdField && payloadKeys[0] === airtableZohoIdField)) {
        console.warn(`[SyncService][${module}] No mappable data fields (only Zoho ID field '${airtableZohoIdField || 'unknown'}') for Zoho ID ${zohoRecordId}. Airtable record not created.`);
        return null;
    }

    const createdRecord = await createModuleRecord(recordDataToCreate, module);
    if (createdRecord) {
        console.log(`[SyncService][${module}] Created Airtable record ${createdRecord.id} from Zoho record ${zohoRecordId}.`);
    } else {
        console.error(`[SyncService][${module}] Failed to create Airtable record from Zoho record ${zohoRecordId} (createModuleRecord returned null/undefined).`);
    }
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

    const airtableFieldIdToNameMap = await getFieldIdToNameMapping(null, module);
    if (!airtableFieldIdToNameMap || Object.keys(airtableFieldIdToNameMap).length === 0) {
        console.warn(`[SyncService][${module}] Airtable field ID to Name map is empty or unavailable for module ${module}. Field lookup by ID will fail.`);
    }

    for (const [zohoFieldMapKey, mapping] of Object.entries(currentModuleFieldMapping)) {
      if (!mapping.airtable || !mapping.zoho) continue;
      if (zohoFieldMapKey === 'ZOHO_ID' || zohoFieldMapKey === 'AIRTABLE_ID') {
        continue;
      }
      
      let airtableFieldName = mapping.airtable;
      if (mapping.airtable.startsWith('fld')) { // Indicates it's an Airtable Field ID
        if (airtableFieldIdToNameMap && airtableFieldIdToNameMap[mapping.airtable]) {
            airtableFieldName = airtableFieldIdToNameMap[mapping.airtable];
        } else {
            console.warn(`[SyncService][${module}] Airtable field name not found for ID '${mapping.airtable}' (mapped to Zoho field '${mapping.zoho}'). Skipping this field.`);
            continue;
        }
      }
      // If not starting with 'fld', assume mapping.airtable is already a field name (e.g. from static config)

      const airtableValue = airtableRecordData.fields[airtableFieldName];

      if (airtableValue !== undefined && airtableValue !== null && String(airtableValue).trim() !== '') {
        zohoRecordPayload[mapping.zoho] = airtableValue;
      }
    }
    
    const airtableIdMap = await getAirtableIdMapping(module); 
    const zohoAirtableIdField = airtableIdMap?.zoho;

    if (zohoAirtableIdField) {
      zohoRecordPayload[zohoAirtableIdField] = airtableRecordId;
    } else {
      console.warn(`[SyncService][${module}] Airtable Record ID mapping to Zoho not found. Cannot set Airtable ID on new Zoho record.`);
    }

    const payloadKeys = Object.keys(zohoRecordPayload);
    if (payloadKeys.length === 0 || (payloadKeys.length === 1 && zohoAirtableIdField && payloadKeys[0] === zohoAirtableIdField)) {
        console.warn(`[SyncService][${module}] No mappable data fields (only Airtable ID field '${zohoAirtableIdField || 'unknown'}') for Airtable ID ${airtableRecordId}. Zoho record not created.`);
        return null;
    }
    
    const createResponse = await createZohoRecord(zohoRecordPayload, module);

    if (createResponse && createResponse.data && createResponse.data[0] && createResponse.data[0].details) {
      const newZohoRecordDetails = createResponse.data[0].details;
      const newZohoRecordId = newZohoRecordDetails.id;

      const zohoCrmIdMap = await getZohoCrmIdMapping(module);
      if (zohoCrmIdMap && zohoCrmIdMap.airtable && newZohoRecordId) {
        const airtableUpdatePayload = { [zohoCrmIdMap.airtable]: newZohoRecordId };
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
async function handleZohoRecordUpdate(zohoRecordId, _zohoRecordData, changedFieldsInfo, module = 'Leads', knownAirtableRecordId = null) {
  let overallSuccess = true;
  try {
    const syncPromises = [];
    await fieldMappingCache.ensureModuleInitialized(module);

    for (const zohoFieldApiName of changedFieldsInfo.changedFields) {
      const mapping = await getFieldMappingFor(zohoFieldApiName, module);
      if (mapping) {
        const newValue = changedFieldsInfo.currentValues[zohoFieldApiName];
        if (!shouldSkipSync('zoho', zohoRecordId, zohoFieldApiName, newValue)) {
          syncPromises.push(
            syncField({ 
              direction: SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE,
              sourceId: zohoRecordId,
              fieldName: zohoFieldApiName,
              value: newValue,
              mapping: mapping,
              module: module,
              targetRecordId: knownAirtableRecordId // Pass the known Airtable record ID
            })
          );
        }
      }
    }
    
    if (syncPromises.length > 0) {
      const results = await Promise.allSettled(syncPromises);
      results.forEach((result, index) => {
        if (result.status === 'rejected' || (result.status === 'fulfilled' && !result.value)) {
          console.error(`[SyncService][${module}] Failed or unsuccessful Z->A sync for Zoho record ${zohoRecordId}, field index ${index}. Reason/Result:`, result.status === 'rejected' ? result.reason : result.value);
          overallSuccess = false; 
        }
      });
    }
  } catch (error) {
    console.error(`[SyncService][${module}] Error in handleZohoRecordUpdate for ID ${zohoRecordId}: ${error.message}`, error.stack);
    overallSuccess = false;
  }
  return overallSuccess;
}

// Handle Airtable record update - check for field changes and sync (module-aware)
async function handleAirtableRecordUpdate(airtableRecordId, changedFieldsInfo, module = 'Leads', knownZohoRecordId = null) {
  let overallSuccess = true; 
  try {
    await fieldMappingCache.ensureModuleInitialized(module);
    const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(module);
    if (!currentModuleFieldMapping) {
      console.error(`[SyncService][${module}] Field mapping not available. Cannot handle Airtable record update for ${airtableRecordId}.`);
      return false; 
    }
    
    const airtableFieldIdToNameMap = await getFieldIdToNameMapping(null, module);
    if (!airtableFieldIdToNameMap) {
        console.warn(`[SyncService][${module}] Airtable field ID to Name map is unavailable for module ${module} during update. Field lookup by ID might fail if mappings use IDs.`);
    }


    const syncPromises = [];

    for (const fieldInfo of changedFieldsInfo) { 
      let mappedZohoKey = null; // The key from currentModuleFieldMapping (e.g., "First_Name")
      let mappingDetails = null;

      for (const [keyInMap, mapEntry] of Object.entries(currentModuleFieldMapping)) {
        if (!mapEntry.airtable || !mapEntry.zoho) continue; // Skip incomplete map entries

        let airtableIdentifierInMap = mapEntry.airtable;
        if (mapEntry.airtable.startsWith('fld') && airtableFieldIdToNameMap && airtableFieldIdToNameMap[mapEntry.airtable]) {
            airtableIdentifierInMap = airtableFieldIdToNameMap[mapEntry.airtable];
        }
        // If mapEntry.airtable is already a name, or ID-to-Name lookup failed, airtableIdentifierInMap remains as mapEntry.airtable

        if (airtableIdentifierInMap === fieldInfo.fieldName) { 
          mappedZohoKey = keyInMap; 
          mappingDetails = mapEntry; 
          break;
        }
      }

      if (mappedZohoKey && mappingDetails) {
        if (!shouldSkipSync('airtable', airtableRecordId, fieldInfo.fieldName, fieldInfo.currentValue)) {
          syncPromises.push(
            syncField({ 
              direction: SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO,
              sourceId: airtableRecordId,
              fieldName: mappingDetails.zoho, 
              value: fieldInfo.currentValue,
              mapping: mappingDetails, 
              module: module,
              targetRecordId: knownZohoRecordId // Pass the known Zoho record ID
            })
          );
        }
      }
    }
    
    if (syncPromises.length > 0) {
      const results = await Promise.allSettled(syncPromises);
      results.forEach((result, index) => {
         if (result.status === 'rejected' || (result.status === 'fulfilled' && !result.value)) {
          console.error(`[SyncService][${module}] Failed or unsuccessful A->Z sync for Airtable record ${airtableRecordId}, field index ${index}. Reason/Result:`, result.status === 'rejected' ? result.reason : result.value);
          overallSuccess = false;
        }
      });
    }
  } catch (error) {
    console.error(`[SyncService][${module}] Error in handleAirtableRecordUpdate for ID ${airtableRecordId}: ${error.message}`, error.stack);
    overallSuccess = false;
  }
  return overallSuccess;
}

module.exports = {

  syncField, 
  
  syncFieldFromZohoToAirtable, 
  syncFieldFromAirtableToZoho, 
  
  createAirtableRecordFromZoho,
  createZohoRecordFromAirtable,
  findOrCreateAirtableRecord,
  
  handleZohoRecordUpdate,
  handleAirtableRecordUpdate,
  
  getFieldMappingFor, 
  getZohoCrmIdMapping, 
  getAirtableIdMapping, 
  
  SYNC_DIRECTIONS
};
