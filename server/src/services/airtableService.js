const axios = require('axios');
const { loadAirtableConfig, shouldIgnoreField, FIELD_MAPPING } = require('../config/config');

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
      `${config.apiUrl}/${config.baseId}/${encodeURIComponent(config.tableName)}/${recordId}`,
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
      return null;
    }
  }

  try {
    const response = await axios.post(
      `${config.apiUrl}/${config.baseId}/${encodeURIComponent(config.tableName)}`,
      recordData,
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
      `${config.apiUrl}/${config.baseId}/${encodeURIComponent(config.tableName)}`,
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
      `${config.apiUrl}/${config.baseId}/${encodeURIComponent(config.tableName)}/${airtableRecordId}`,
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

// Fetch webhook payloads from Airtable API with cursor-based pagination for latest payloads
async function fetchWebhookPayloads(config, webhookId, maxPayloads = 200) {
  try {
    let allPayloads = [];
    let cursor = null;
    let iterations = 0;
    const maxIterations = Math.ceil(maxPayloads / 50); // Prevent infinite loops
    
    do {
      const params = {};
      if (cursor) {
        params.cursor = cursor;
      }
      // Limit to 50 per request (Airtable's max)
      params.limit = Math.min(50, maxPayloads - allPayloads.length);
      
      const response = await axios.get(
        `${config.apiUrl}/bases/${config.baseId}/webhooks/${webhookId}/payloads`,
        {
          headers: {
            'Authorization': `Bearer ${config.apiToken}`,
            'Content-Type': 'application/json'
          },
          params: params
        }
      );
      
      const payloads = response.data.payloads || [];
      allPayloads.push(...payloads);
      
      // Update cursor for next iteration
      cursor = response.data.cursor;
      iterations++;
      
      // Stop if we have enough payloads or no more cursor
      if (allPayloads.length >= maxPayloads || !cursor || payloads.length === 0) {
        break;
      }
      
    } while (cursor && iterations < maxIterations);
    
    // Sort by timestamp descending to get newest payloads first
    allPayloads.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    return allPayloads;
  } catch (error) {
    return [];
  }
}

// Extract actual field data from Airtable payload structure
function extractFieldData(recordData) {
  // Handle new payload format where fields are nested under cellValuesByFieldId
  if (recordData.cellValuesByFieldId) {
    return recordData.cellValuesByFieldId;
  }
  
  // Handle old format where fields are directly on the record
  return recordData;
}

// Compare current and previous values to identify changed fields
function getChangedFieldsFromRecord(current, previous) {
  // Extract the actual field data
  const currentFields = extractFieldData(current);
  const previousFields = previous ? extractFieldData(previous) : {};
  
  if (!previous) {
    return Object.keys(currentFields); // All fields are "changed" for new records
  }
  
  const changedFields = [];
  Object.keys(currentFields).forEach(fieldId => {
    if (JSON.stringify(currentFields[fieldId]) !== JSON.stringify(previousFields[fieldId])) {
      changedFields.push(fieldId);
    }
  });
  
  return changedFields;
}

// Process changed tables data
async function processChangedTables(changedTablesById, config) {
  for (const tableId of Object.keys(changedTablesById)) {
    const table = changedTablesById[tableId];
    
    // Process changed records
    if (table.changedRecordsById) {
      for (const recordId of Object.keys(table.changedRecordsById)) {
        const record = table.changedRecordsById[recordId];
        
        // Extract field data from the record structure
        const currentFields = extractFieldData(record.current);
        const previousFields = record.previous ? extractFieldData(record.previous) : {};
        
        // Identify which fields actually changed
        const changedFieldIds = getChangedFieldsFromRecord(record.current, record.previous);
        
        if (changedFieldIds.length > 0) {
          // Get field names for the changed fields
          const fieldNames = await getFieldNames(config, changedFieldIds);
          
          const changedFieldsInfo = [];
          
          for (const fieldId of changedFieldIds) {
            const fieldName = fieldNames[fieldId] || fieldId;
            const currentValue = currentFields[fieldId];
            const previousValue = previousFields[fieldId];
            
            // Check if this field should be ignored
            if (shouldIgnoreField(fieldName, 'airtable')) {
              continue;
            }
            
            changedFieldsInfo.push({
              fieldId,
              fieldName,
              currentValue,
              previousValue
            });
          }
          
          // Return processed change information for sync handling
          return {
            recordId,
            changedFields: changedFieldsInfo,
            allCurrentFields: currentFields
          };
        }
      }
    }
    
    // Process created records
    if (table.createdRecordsById) {
      for (const recordId of Object.keys(table.createdRecordsById)) {
        const record = table.createdRecordsById[recordId];
        
        const currentFields = extractFieldData(record.current);
        const allFieldIds = Object.keys(currentFields);
      }
    }
    
    // Process deleted records
    if (table.destroyedRecordIds) {
      for (const recordId of table.destroyedRecordIds) {
        // Record deleted
      }
    }
  }
  
  return null;
}

// Fetch dynamic field mapping from Zoho Fields table
async function fetchDynamicFieldMapping(config = null) {
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
    
    const fieldMapping = {};
    
    for (const record of allRecords) {
      const fields = record.fields;
      
      // Use correct field names from your Zoho Fields table
      const zohoFieldName = fields['Field Name']; // Zoho field name
      
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
        `${config.apiUrl}/${config.baseId}/${encodeURIComponent(config.tableName)}`,
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
        `${config.apiUrl}/${config.baseId}/${encodeURIComponent(config.tableName)}`,
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
      `${config.apiUrl}/${config.baseId}/${encodeURIComponent(config.tableName)}/${recordId}`,
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

// Get complete field ID to name mapping for the table
async function getFieldIdToNameMapping(config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      return null;
    }
  }

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
    
    return fieldMapping;
  } catch (error) {
    return {};
  }
}

module.exports = {
  getFieldNames,
  getAirtableFieldId,
  updateAirtableRecord,
  createAirtableRecord,
  findAirtableRecordByZohoId,
  findZohoLeadByAirtableId,
  fetchWebhookPayloads,
  extractFieldData,
  getChangedFieldsFromRecord,
  processChangedTables,
  fetchDynamicFieldMapping,
  getRecordsModifiedSince,
  getAllRecordsForSync,
  getRecordById,
  getFieldIdToNameMapping
};