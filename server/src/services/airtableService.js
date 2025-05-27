const axios = require('axios');
const { loadAirtableConfig, loadAirtableConfigForModule, shouldIgnoreField, FIELD_MAPPING } = require('../config/config');
const { getModuleTableConfig, ZOHO_FIELDS_TABLE_ID, ZOHO_MODULES_TABLE_ID } = require('./moduleConfigService');

// Helper function to get module-specific config
async function getModuleConfig(config, module) {
  if (!module || module === 'Leads') {
    return config; // Default behavior for backward compatibility
  }
  
  try {
    const moduleTableConfig = await getModuleTableConfig(module, config);
    
    // Create a new config object with module-specific overrides
    return {
      ...config,
      tableName: moduleTableConfig.tableName,
      tableId: moduleTableConfig.tableId,
      currentModule: module
    };
  } catch (error) {
    console.error(`Error getting module config for ${module}:`, error.message);
    return config; // Fallback to default config
  }
}

// Get field names from field IDs using Airtable API
async function getFieldNames(config, fieldIds) {
  try {
    const response = await axios.get(
      `${config.apiUrl}/meta/bases/${config.baseId}/tables`,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    const table = response.data.tables.find(t => t.id === config.tableId || t.name === config.tableName);
    if (!table) {
      return {};
    }
    
    const fieldMapping = {};
    table.fields.forEach(field => {
      fieldMapping[field.id] = field.name;
    });
    
    const result = {};
    fieldIds.forEach(fieldId => {
      result[fieldId] = fieldMapping[fieldId] || fieldId;
    });
    
    return result;
  } catch (error) {
    return {};
  }
}

// Get Airtable field ID from field name
async function getAirtableFieldId(config, fieldName) {
  try {
    const response = await axios.get(
      `${config.apiUrl}/meta/bases/${config.baseId}/tables`,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    const table = response.data.tables.find(t => t.id === config.tableId || t.name === config.tableName);
    if (!table) {
      return null;
    }
    
    const field = table.fields.find(f => f.name === fieldName);
    return field ? field.id : null;
  } catch (error) {
    return null;
  }
}

// Update Airtable record
async function updateAirtableRecord(recordId, fieldUpdates, config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }

  try {
    const response = await axios.patch(
      `${config.apiUrl}/${config.baseId}/${config.tableId}/${recordId}`,
      {
        fields: fieldUpdates
      },
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    return response.data;
  } catch (error) {
    return null;
  }
}

// Create Airtable record
async function createAirtableRecord(recordData, config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      console.log(`❌ No Airtable config found`);
      return null;
    }
  }

  try {
    const url = `${config.apiUrl}/${config.baseId}/${config.tableId}`;
    
    const response = await axios.post(url, recordData, {
      headers: {
        'Authorization': `Bearer ${config.apiToken}`,
        'Content-Type': 'application/json'
      }
    });
    
    return response.data;
  } catch (error) {
    console.log(`❌ Error creating Airtable record:`, error.message);
    if (error.response) {
      console.log(`❌ Response status:`, error.response.status);
      console.log(`❌ Response data:`, JSON.stringify(error.response.data, null, 2));
    }
    return null;
  }
}

// Find Airtable record by Zoho lead ID using the "Zoho CRM ID" field
async function findAirtableRecordByZohoId(zohoLeadId, config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }

  try {
    // Search for records where "Zoho CRM ID" field equals the Zoho lead ID
    const response = await axios.get(
      `${config.apiUrl}/${config.baseId}/${config.tableId}`,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        },
        params: {
          filterByFormula: `{${FIELD_MAPPING.ZOHO_ID.airtable}} = "${zohoLeadId}"`
        }
      }
    );
    
    if (response.data.records && response.data.records.length > 0) {
      const recordId = response.data.records[0].id;
      return recordId;
    } else {
      return null;
    }
  } catch (error) {
    return null;
  }
}

// Find Zoho lead ID by Airtable record ID using the "Zoho CRM ID" field
async function findZohoLeadByAirtableId(airtableRecordId, config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }
  
  try {
    // Get the specific Airtable record to read the "Zoho CRM ID" field
    const response = await axios.get(
      `${config.apiUrl}/${config.baseId}/${config.tableId}/${airtableRecordId}`,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    const zohoId = response.data.fields[FIELD_MAPPING.ZOHO_ID.airtable];
    if (zohoId) {
      return zohoId;
    } else {
      return null;
    }
  } catch (error) {
    return null;
  }
}



// Fetch dynamic field mapping from Zoho Fields table
async function fetchDynamicFieldMapping(config = null, module = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }

  try {
    let allRecords = [];
    let offset = null;
    let pageCount = 0;
    
    // Fetch all records with pagination
    do {
      pageCount++;
      
      const params = {};
      if (offset) {
        params.offset = offset;
      }
      
      const response = await axios.get(
        `${config.apiUrl}/${config.baseId}/tbl0JfUjWhV4TvLz2`, // Zoho Fields table ID
        {
          headers: {
            'Authorization': `Bearer ${config.apiToken}`,
            'Content-Type': 'application/json'
          },
          params: params
        }
      );
      
      const pageRecords = response.data.records || [];
      allRecords.push(...pageRecords);
      offset = response.data.offset;
      
    } while (offset);
    
    // If module is specified, get the module record ID by api_name
    let moduleRecordId = null;
    if (module) {
      moduleRecordId = await getModuleRecordIdByApiName(config, module);
      if (!moduleRecordId) {
        // No module found with this api_name, return empty mapping
        return {};
      }
    }
    
    const fieldMapping = {};
    
    for (const record of allRecords) {
      const fields = record.fields;
      
      // Check if module filtering is needed
      if (module && moduleRecordId) {
        const recordModules = fields['Module'] || [];
        const recordModuleIds = Array.isArray(recordModules) ? recordModules : [recordModules];
        
        // Skip if this record doesn't match the specified module record ID
        if (!recordModuleIds.includes(moduleRecordId)) {
          continue;
        }
      }
      
      // Use correct field names from your Zoho Fields table
      const zohoFieldName = fields['api_name']; // Zoho field name
      
      // Try multiple field sources for the Airtable field ID
      let airtableFieldId = fields['Airtable Field ID'] || fields['Field ID'] || fields['Airtable Field'];
      
      if (zohoFieldName && airtableFieldId) {
        // Handle both string and array values for airtableFieldId
        const finalAirtableFieldId = Array.isArray(airtableFieldId) ? airtableFieldId[0] : airtableFieldId;
        
        // Only use if it looks like a field ID (starts with "fld")
        if (typeof finalAirtableFieldId === 'string' && finalAirtableFieldId.startsWith('fld')) {
          fieldMapping[zohoFieldName] = {
            zoho: zohoFieldName,
            airtable: finalAirtableFieldId,
            recordId: record.id
          };
        }
      }
    }
    
    return fieldMapping;
  } catch (error) {
    return null;
  }
}

// Helper function to get module record ID by api_name
async function getModuleRecordIdByApiName(config, apiName) {
  try {
    const moduleTableId = 'tbl2HlEPyESvXUXHN'; // Zoho Modules table ID
    
    const response = await axios.get(
      `${config.apiUrl}/${config.baseId}/${moduleTableId}`,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        },
        params: {
          filterByFormula: `{api_name} = "${apiName}"`, // Search by api_name field
          fields: ['api_name'] // Only fetch the api_name field
        }
      }
    );
    
    const records = response.data.records || [];
    if (records.length > 0) {
      return records[0].id; // Return the record ID
    }
    
    return null;
  } catch (error) {
    return null;
  }
}

// Fetch Airtable records modified since a specific timestamp
async function getRecordsModifiedSince(sinceTimestamp, config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }

  try {
    let allRecords = [];
    let offset = null;
    
    // Format timestamp for Airtable API (ISO format)
    const sinceDate = new Date(sinceTimestamp).toISOString();
    
    do {
      const params = {
        sort: [{ field: 'Last Modified Time', direction: 'desc' }],
        pageSize: 100, // Max records per page
        filterByFormula: `IS_AFTER({Last Modified Time}, '${sinceDate}')`
      };
      
      if (offset) {
        params.offset = offset;
      }

      const response = await axios.get(
        `${config.apiUrl}/${config.baseId}/${config.tableId}`,
        {
          headers: {
            'Authorization': `Bearer ${config.apiToken}`,
            'Content-Type': 'application/json'
          },
          params: params
        }
      );
      
      const pageRecords = response.data.records || [];
      allRecords.push(...pageRecords);
      
      // Update offset for next iteration
      offset = response.data.offset;
      
      // Stop if no more records or no offset
      if (!offset || pageRecords.length === 0) {
        break;
      }
      
    } while (offset);
    
    return { records: allRecords };
  } catch (error) {
    return null;
  }
}

// Get all records with their Zoho CRM IDs and Last Modified Times for comparison
async function getAllRecordsForSync(config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }

  try {
    let allRecords = [];
    let offset = null;
    
    do {
      const params = {
        fields: ['Zoho CRM ID', 'Last Modified Time'],
        pageSize: 100,
        sort: [{ field: 'Last Modified Time', direction: 'desc' }]
      };
      
      if (offset) {
        params.offset = offset;
      }

      const response = await axios.get(
        `${config.apiUrl}/${config.baseId}/${config.tableId}`,
        {
          headers: {
            'Authorization': `Bearer ${config.apiToken}`,
            'Content-Type': 'application/json'
          },
          params: params
        }
      );
      
      const pageRecords = response.data.records || [];
      allRecords.push(...pageRecords);
      
      // Update offset for next iteration
      offset = response.data.offset;
      
      // Stop if no more records or no offset
      if (!offset || pageRecords.length === 0) {
        break;
      }
      
    } while (offset);
    
    return { records: allRecords };
  } catch (error) {
    return null;
  }
}

// Get full record details for a specific record by ID
async function getRecordById(recordId, config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }

  try {
    const response = await axios.get(
      `${config.apiUrl}/${config.baseId}/${config.tableId}/${recordId}`,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    return response.data;
  } catch (error) {
    return null;
  }
}

// Cache for field ID to name mappings by module
const fieldIdToNameCache = new Map();

// Get complete field ID to name mapping for the table (module-aware)
async function getFieldIdToNameMapping(config = null, module = 'Leads') {
  // Check cache first
  if (fieldIdToNameCache.has(module)) {
    return fieldIdToNameCache.get(module);
  }

  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }

  try {
    // Get the module table configuration
    const moduleTableConfig = await getModuleTableConfig(module, config);
    if (!moduleTableConfig) {
      console.error(`[AirtableService] Module ${module} not found in Airtable`);
      return {};
    }
    
    const tableId = moduleTableConfig.tableId;
    const tableName = moduleTableConfig.tableName;
    
    const response = await axios.get(
      `${config.apiUrl}/meta/bases/${config.baseId}/tables`,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    const table = response.data.tables.find(t => t.id === tableId || t.name === tableName);
    if (!table) {
      console.error(`[AirtableService] Table not found for module ${module} (tableId: ${tableId}, tableName: ${tableName})`);
      return {};
    }
    
    const fieldMapping = {};
    table.fields.forEach(field => {
      fieldMapping[field.id] = field.name;
    });
    
    // Cache the result
    fieldIdToNameCache.set(module, fieldMapping);
    
    return fieldMapping;
  } catch (error) {
    console.error(`[AirtableService] Error getting field mapping for module ${module}:`, error.message);
    return {};
  }
}

// Update a single field in Airtable record
async function updateAirtableField(recordId, fieldName, value, config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return false;
    }
  }

  try {
    // First get the field ID for the field name
    const fieldId = await getAirtableFieldId(config, fieldName);
    const fieldToUpdate = fieldId || fieldName; // Use field ID if found, otherwise use name
    
    const response = await axios.patch(
      `${config.apiUrl}/${config.baseId}/${config.tableId}/${recordId}`,
      {
        fields: {
          [fieldToUpdate]: value
        }
      },
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    return !!response.data;
  } catch (error) {
    return false;
  }
}

// ========== MODULE-AWARE WRAPPER FUNCTIONS ==========

// Module-aware update record function
async function updateModuleRecord(recordId, fieldUpdates, module = 'Leads', config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }
  
  const moduleConfig = await getModuleConfig(config, module);
  return updateAirtableRecord(recordId, fieldUpdates, moduleConfig);
}

// Module-aware create record function
async function createModuleRecord(recordData, module = 'Leads', config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }
  
  const moduleConfig = await getModuleConfig(config, module);
  return createAirtableRecord(recordData, moduleConfig);
}

// Module-aware find record by Zoho ID
async function findModuleRecordByZohoId(zohoId, module = 'Leads', config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }
  
  const moduleConfig = await getModuleConfig(config, module);
  return findAirtableRecordByZohoId(zohoId, moduleConfig);
}

// Module-aware find Zoho ID by Airtable record
async function findZohoIdByModuleRecord(recordId, module = 'Leads', config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }
  
  const moduleConfig = await getModuleConfig(config, module);
  return findZohoLeadByAirtableId(recordId, moduleConfig);
}

// Module-aware get records modified since
async function getModuleRecordsModifiedSince(sinceTimestamp, module = 'Leads', config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }
  
  const moduleConfig = await getModuleConfig(config, module);
  return getRecordsModifiedSince(sinceTimestamp, moduleConfig);
}

// Module-aware get all records for sync
async function getAllModuleRecordsForSync(module = 'Leads', config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }
  
  const moduleConfig = await getModuleConfig(config, module);
  return getAllRecordsForSync(moduleConfig);
}

// Module-aware get record by ID
async function getModuleRecordById(recordId, module = 'Leads', config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }
  
  const moduleConfig = await getModuleConfig(config, module);
  return getRecordById(recordId, moduleConfig);
}

// Module-aware update field
async function updateModuleField(recordId, fieldName, value, module = 'Leads', config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return false;
    }
  }
  
  const moduleConfig = await getModuleConfig(config, module);
  return updateAirtableField(recordId, fieldName, value, moduleConfig);
}

// ========== BULK OPERATIONS ==========

// Bulk create Airtable records (max 10 per request)
// IMPORTANT: recordsData should be array of {fields: {...}} objects
async function createAirtableRecordsBulk(recordsData, config = null) {
  if (!recordsData || recordsData.length === 0) return [];
  
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      console.log(`❌ No Airtable config found`);
      return [];
    }
  }
  
  const results = [];
  const errors = [];
  const batchSize = 10; // Airtable API limit
  
  for (let i = 0; i < recordsData.length; i += batchSize) {
    const batch = recordsData.slice(i, i + batchSize);
    
    try {
      const url = `${config.apiUrl}/${config.baseId}/${config.tableId}`;
      const response = await axios.post(
        url,
        { records: batch },
        {
          headers: {
            'Authorization': `Bearer ${config.apiToken}`,
            'Content-Type': 'application/json'
          }
        }
      );
      
      if (response.data && response.data.records) {
        results.push(...response.data.records);
      }
    } catch (error) {
      console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, error.message);
      if (error.response) {
        console.error(`❌ Response:`, JSON.stringify(error.response.data, null, 2));
      }
      errors.push({
        batch: Math.floor(i/batchSize) + 1,
        error: error.message,
        records: batch
      });
    }
    
    // Rate limiting: wait 200ms between requests (5 req/sec)
    if (i + batchSize < recordsData.length) {
      await new Promise(resolve => setTimeout(resolve, 200));
    }
  }
  
  return { success: results, errors };
}

// Bulk update Airtable records (max 10 per request)
// IMPORTANT: updates should be array of {id: recordId, fields: {...}} objects
async function updateAirtableRecordsBulk(updates, config = null) {
  if (!updates || updates.length === 0) return [];
  
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      console.log(`❌ No Airtable config found`);
      return [];
    }
  }
  
  const results = [];
  const errors = [];
  const batchSize = 10; // Airtable API limit
  
  for (let i = 0; i < updates.length; i += batchSize) {
    const batch = updates.slice(i, i + batchSize);
    
    try {
      const url = `${config.apiUrl}/${config.baseId}/${config.tableId}`;
      const response = await axios.patch(
        url,
        { records: batch },
        {
          headers: {
            'Authorization': `Bearer ${config.apiToken}`,
            'Content-Type': 'application/json'
          }
        }
      );
      
      if (response.data && response.data.records) {
        results.push(...response.data.records);
      }
    } catch (error) {
      console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, error.message);
      if (error.response) {
        console.error(`❌ Response:`, JSON.stringify(error.response.data, null, 2));
      }
      errors.push({
        batch: Math.floor(i/batchSize) + 1,
        error: error.message,
        records: batch
      });
    }
    
    // Rate limiting: wait 200ms between requests (5 req/sec)
    if (i + batchSize < updates.length) {
      await new Promise(resolve => setTimeout(resolve, 200));
    }
  }
  
  return { success: results, errors };
}

// Module-aware bulk create function
async function createModuleRecordsBulk(recordsData, module = 'Leads', config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return { success: [], errors: [] };
    }
  }
  
  const moduleConfig = await getModuleConfig(config, module);
  return createAirtableRecordsBulk(recordsData, moduleConfig);
}

// Module-aware bulk update function
async function updateModuleRecordsBulk(updates, module = 'Leads', config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return { success: [], errors: [] };
    }
  }
  
  const moduleConfig = await getModuleConfig(config, module);
  return updateAirtableRecordsBulk(updates, moduleConfig);
}

module.exports = {
  // Core functions (still available for backward compatibility)
  getFieldNames,
  getAirtableFieldId,
  updateAirtableRecord,
  updateAirtableField,
  createAirtableRecord,
  findAirtableRecordByZohoId,
  findZohoLeadByAirtableId,
  fetchDynamicFieldMapping,
  getRecordsModifiedSince,
  getAllRecordsForSync,
  getRecordById,
  getFieldIdToNameMapping,
  getModuleRecordIdByApiName,
  
  // Module-aware functions
  updateModuleRecord,
  createModuleRecord,
  findModuleRecordByZohoId,
  findZohoIdByModuleRecord,
  getModuleRecordsModifiedSince,
  getAllModuleRecordsForSync,
  getModuleRecordById,
  updateModuleField,
  
  // Bulk operations
  createAirtableRecordsBulk,
  updateAirtableRecordsBulk,
  createModuleRecordsBulk,
  updateModuleRecordsBulk,
  
  // Helper functions
  getModuleConfig
};