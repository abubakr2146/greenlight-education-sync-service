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
      console.log('Table not found in metadata');
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
    console.error('Error getting field names:', error.response?.data || error.message);
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
      console.log('Table not found in metadata');
      return null;
    }
    
    const field = table.fields.find(f => f.name === fieldName);
    return field ? field.id : null;
  } catch (error) {
    console.error('Error getting Airtable field ID:', error.response?.data || error.message);
    return null;
  }
}

// Update Airtable record
async function updateAirtableRecord(recordId, fieldUpdates, config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      console.error('Failed to load Airtable config');
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
    
    console.log('✅ Airtable record updated successfully');
    return response.data;
  } catch (error) {
    console.error('❌ Error updating Airtable record:', error.response?.data || error.message);
    return null;
  }
}

// Create Airtable record
async function createAirtableRecord(recordData, config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      console.error('Failed to load Airtable config');
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
    
    console.log(`✅ Airtable record created successfully: ${response.data.id}`);
    return response.data;
  } catch (error) {
    console.error('❌ Error creating Airtable record:', error.response?.data || error.message);
    return null;
  }
}

// Find Airtable record by Zoho lead ID using the "Zoho CRM ID" field
async function findAirtableRecordByZohoId(zohoLeadId, config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      console.error('Failed to load Airtable config');
      return null;
    }
  }

  console.log(`🔍 Looking for Airtable record with Zoho CRM ID: ${zohoLeadId}`);
  
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
      console.log(`✅ Found Airtable record: ${recordId}`);
      return recordId;
    } else {
      console.log(`❌ No Airtable record found with Zoho CRM ID: ${zohoLeadId}`);
      return null;
    }
  } catch (error) {
    console.error('Error finding Airtable record:', error.response?.data || error.message);
    return null;
  }
}

// Find Zoho lead ID by Airtable record ID using the "Zoho CRM ID" field
async function findZohoLeadByAirtableId(airtableRecordId, config = null) {
  console.log(`🔍 Looking for Zoho lead ID in Airtable record: ${airtableRecordId}`);
  
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      console.error('Failed to load Airtable config');
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
      console.log(`✅ Found Zoho lead ID: ${zohoId}`);
      return zohoId;
    } else {
      console.log(`❌ No Zoho CRM ID found in Airtable record: ${airtableRecordId}`);
      return null;
    }
  } catch (error) {
    console.error('Error finding Zoho lead ID:', error.response?.data || error.message);
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
    
    console.log(`📡 Fetching webhook payloads (max: ${maxPayloads})...`);
    
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
      
      console.log(`   Batch ${iterations}: ${payloads.length} payloads (total: ${allPayloads.length})`);
      
      // Stop if we have enough payloads or no more cursor
      if (allPayloads.length >= maxPayloads || !cursor || payloads.length === 0) {
        break;
      }
      
    } while (cursor && iterations < maxIterations);
    
    // Sort by timestamp descending to get newest payloads first
    allPayloads.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    console.log(`✅ Retrieved ${allPayloads.length} total payloads`);
    if (allPayloads.length > 0) {
      console.log(`   Newest: ${allPayloads[0].timestamp}`);
      console.log(`   Oldest: ${allPayloads[allPayloads.length - 1].timestamp}`);
    }
    
    return allPayloads;
  } catch (error) {
    console.error('Error fetching webhook payloads:', error.response?.data || error.message);
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
  console.log('\n=== Changed Tables ===');
  
  for (const tableId of Object.keys(changedTablesById)) {
    const table = changedTablesById[tableId];
    console.log(`Table ID: ${tableId}`);
    console.log(`Table Name: ${table.name || 'Unknown'}`);
    
    // Process changed records
    if (table.changedRecordsById) {
      console.log('\n=== Changed Records ===');
      
      for (const recordId of Object.keys(table.changedRecordsById)) {
        const record = table.changedRecordsById[recordId];
        console.log(`Record ID: ${recordId}`);
        
        // Extract field data from the record structure
        const currentFields = extractFieldData(record.current);
        const previousFields = record.previous ? extractFieldData(record.previous) : {};
        
        // Identify which fields actually changed
        const changedFieldIds = getChangedFieldsFromRecord(record.current, record.previous);
        console.log('Changed Field IDs:', changedFieldIds);
        
        if (changedFieldIds.length > 0) {
          // Get field names for the changed fields
          const fieldNames = await getFieldNames(config, changedFieldIds);
          console.log('Changed Field Names:');
          
          const changedFieldsInfo = [];
          
          for (const fieldId of changedFieldIds) {
            const fieldName = fieldNames[fieldId] || fieldId;
            const currentValue = currentFields[fieldId];
            const previousValue = previousFields[fieldId];
            
            console.log(`  ${fieldName} (${fieldId}):`);
            console.log(`    From: ${JSON.stringify(previousValue)}`);
            console.log(`    To: ${JSON.stringify(currentValue)}`);
            
            // Check if this field should be ignored
            if (shouldIgnoreField(fieldName, 'airtable')) {
              console.log(`⏭️  Skipping ignored field: ${fieldName}`);
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
        
        console.log('All Current Fields:', currentFields);
        if (record.previous) {
          console.log('All Previous Values:', record.previous);
        }
        console.log('---');
      }
    }
    
    // Process created records
    if (table.createdRecordsById) {
      console.log('\n=== Created Records ===');
      
      for (const recordId of Object.keys(table.createdRecordsById)) {
        const record = table.createdRecordsById[recordId];
        console.log(`Record ID: ${recordId}`);
        
        const currentFields = extractFieldData(record.current);
        const allFieldIds = Object.keys(currentFields);
        if (allFieldIds.length > 0) {
          const fieldNames = await getFieldNames(config, allFieldIds);
          console.log('Field Values:');
          for (const fieldId of allFieldIds) {
            const fieldName = fieldNames[fieldId] || fieldId;
            const value = currentFields[fieldId];
            console.log(`  ${fieldName} (${fieldId}): ${JSON.stringify(value)}`);
          }
        }
        
        console.log('All Values:', currentFields);
        console.log('---');
      }
    }
    
    // Process deleted records
    if (table.destroyedRecordIds) {
      console.log('\n=== Deleted Records ===');
      for (const recordId of table.destroyedRecordIds) {
        console.log(`Deleted Record ID: ${recordId}`);
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
      console.error('Failed to load Airtable config');
      return null;
    }
  }

  console.log('📋 Fetching dynamic field mapping from Zoho Fields table...');
  
  try {
    const response = await axios.get(
      `${config.apiUrl}/${config.baseId}/tbl0JfUjWhV4TvLz2`, // Zoho Fields table ID
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        },
      }
    );
    
    const fieldMapping = {};
    const records = response.data.records || [];
    
    console.log(`✅ Found ${records.length} active field mappings`);
    
    for (const record of records) {
      const fields = record.fields;
      
      // Use correct field names from your Zoho Fields table
      const zohoFieldName = fields['Field Name']; // Zoho field name
      const airtableFieldId = fields['Airtable Field ID']; // Airtable field ID
      
      console.log(`   Record ${record.id}:`);
      console.log(`     - All fields: ${JSON.stringify(Object.keys(fields))}`);
      console.log(`     - Zoho field: ${zohoFieldName}`);
      console.log(`     - Airtable field ID: ${airtableFieldId}`);
      
      if (zohoFieldName && airtableFieldId) {
        // Handle both string and array values for airtableFieldId
        const finalAirtableFieldId = Array.isArray(airtableFieldId) ? airtableFieldId[0] : airtableFieldId;
        
        console.log(`   ✅ Mapping: ${zohoFieldName} → ${finalAirtableFieldId}`);
        
        fieldMapping[zohoFieldName] = {
          zoho: zohoFieldName,
          airtable: finalAirtableFieldId, // Use field ID directly
          recordId: record.id
        };
      } else {
        console.log(`   ❌ Skipping record - missing data`);
      }
    }
    
    return fieldMapping;
  } catch (error) {
    console.error('Error fetching dynamic field mapping:', error.response?.data || error.message);
    return null;
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
  fetchDynamicFieldMapping
};