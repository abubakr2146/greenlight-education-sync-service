const fs = require('fs');
const path = require('path');

// Field mapping configuration
const FIELD_MAPPING = {
  PHONE: {
    zoho: 'Phone',        // Zoho field name for phone
    airtable: 'Phone'     // Airtable field name for phone (we'll get the field ID dynamically)
  },
  ZOHO_ID: {
    airtable: 'Zoho CRM ID'  // Airtable field name that stores Zoho lead ID
  },
  AIRTABLE_ID: {
    zoho: 'Airtable_Record_ID'  // Zoho field name that stores Airtable record ID
  }
};

// Fields to ignore during sync (computed fields, timestamps, etc.)
const IGNORED_FIELDS = {
  zoho: [
    'Modified_Time',
    'Created_Time', 
    'Modified_By',
    'Created_By',
    'id',
    'smsmagic4__Plain_Phone',    // SMS Magic computed field
    'smsmagic4__Plain_Mobile',   // SMS Magic computed field
    'Lead_Conversion_Time',
    'Data_Processing_Basis_Details',
    'Approval',
    'Data_Source',
    'Process_Flow'
  ],
  airtable: [
    'Last Modified Time',
    'Created Time',
    'Last Modified By',
    'Auto Number',
    'Record ID'
  ]
};

// Load Zoho config
function loadZohoConfig() {
  try {
    const configPath = path.join(__dirname, '../../../setup/zoho-config.json');
    const configData = fs.readFileSync(configPath, 'utf8');
    return JSON.parse(configData);
  } catch (error) {
    return null;
  }
}

// Load Airtable config
function loadAirtableConfig() {
  try {
    const configPath = path.join(__dirname, '../../../setup/airtable-config.json');
    const configData = fs.readFileSync(configPath, 'utf8');
    return JSON.parse(configData);
  } catch (error) {
    return null;
  }
}

// Save Zoho config
function saveZohoConfig(config) {
  try {
    const configPath = path.join(__dirname, '../../../setup/zoho-config.json');
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
    return true;
  } catch (error) {
    return false;
  }
}

// Helper function to check if a field should be ignored
function shouldIgnoreField(fieldName, system) {
  const ignoredFields = IGNORED_FIELDS[system] || [];
  return ignoredFields.includes(fieldName);
}

// Filter out ignored fields from a list
function filterIgnoredFields(fieldNames, system) {
  return fieldNames.filter(fieldName => !shouldIgnoreField(fieldName, system));
}

// Get dynamic field mapping (or fall back to static mapping)
async function getFieldMapping(module = 'Leads') {
  try {
    const { fetchDynamicFieldMapping } = require('../services/airtableService');
    const dynamicMapping = await fetchDynamicFieldMapping(null, module);
    
    if (dynamicMapping && Object.keys(dynamicMapping).length > 0) {
      return dynamicMapping;
    }
  } catch (error) {
    // Failed to fetch dynamic mapping
  }
  
  return FIELD_MAPPING;
}

module.exports = {
  FIELD_MAPPING,
  IGNORED_FIELDS,
  loadZohoConfig,
  loadAirtableConfig,
  saveZohoConfig,
  shouldIgnoreField,
  filterIgnoredFields,
  getFieldMapping
};