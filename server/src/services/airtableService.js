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
  fetchDynamicFieldMapping,
  getRecordsModifiedSince,
  getAllRecordsForSync,
  getRecordById,
  getFieldIdToNameMapping,
  getModuleRecordIdByApiName
};