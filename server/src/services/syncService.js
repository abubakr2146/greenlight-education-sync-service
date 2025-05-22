const { FIELD_MAPPING, getFieldMapping } = require('../config/config');
const { shouldSkipSync } = require('../utils/syncTracker');
const { getLeadDetails, updateZohoLead } = require('./zohoService');
const { 
  updateAirtableRecord, 
  createAirtableRecord, 
  findAirtableRecordByZohoId, 
  findZohoLeadByAirtableId 
} = require('./airtableService');

// Helper function to get field mapping for a specific field
async function getFieldMappingFor(zohoFieldName) {
  const fieldMapping = await getFieldMapping();
  
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

// Helper function to get Zoho CRM ID field mapping
async function getZohoCrmIdMapping() {
  const fieldMapping = await getFieldMapping();
  
  // Look for ZOHO_ID or equivalent in dynamic mapping
  if (fieldMapping.ZOHO_ID || fieldMapping['Zoho CRM ID']) {
    return fieldMapping.ZOHO_ID || fieldMapping['Zoho CRM ID'];
  }
  
  // Fallback to static mapping
  return FIELD_MAPPING.ZOHO_ID;
}

// Sync phone field from Zoho to Airtable
async function syncPhoneFromZohoToAirtable(leadId, newPhoneValue) {
  console.log(`\n🔄 Syncing phone from Zoho lead ${leadId} to Airtable...`);
  
  // Get field mapping for Phone field
  const phoneMapping = await getFieldMappingFor('Phone');
  if (!phoneMapping) {
    console.log(`⚠️  No field mapping found for Phone field`);
    return;
  }
  
  // For now, we'll need to implement a mapping between Zoho lead IDs and Airtable record IDs
  let airtableRecordId = await findAirtableRecordByZohoId(leadId);
  
  if (!airtableRecordId) {
    console.log(`⚠️  No corresponding Airtable record found for Zoho lead ${leadId}`);
    console.log(`📝 Creating Airtable record for this lead...`);
    
    // Get the lead details to create the Airtable record
    const leadDetails = await getLeadDetails(leadId);
    if (leadDetails && leadDetails.data && leadDetails.data[0]) {
      const createdRecord = await createAirtableRecordFromZohoLead(leadId, leadDetails.data[0]);
      if (createdRecord) {
        airtableRecordId = createdRecord.id;
        console.log(`✅ Created Airtable record: ${airtableRecordId}`);
      } else {
        console.log(`❌ Failed to create Airtable record`);
        return;
      }
    } else {
      console.log(`❌ Could not fetch lead details to create Airtable record`);
      return;
    }
  }
  
  // IMPORTANT: Record this sync BEFORE making the update to prevent webhook loop
  const { shouldSkipSync } = require('../utils/syncTracker');
  const airtableField = phoneMapping.airtable;
  const syncKey = `airtable:${airtableRecordId}:${airtableField}`;
  
  // Record that we're about to sync this value to Airtable
  // This will prevent processing the resulting webhook
  shouldSkipSync('airtable', airtableRecordId, airtableField, newPhoneValue);
  console.log(`📝 Pre-recorded sync to prevent webhook loop: ${syncKey}`);
  
  const fieldUpdates = {};
  fieldUpdates[airtableField] = newPhoneValue;
  
  await updateAirtableRecord(airtableRecordId, fieldUpdates);
}

// Sync phone field from Airtable to Zoho  
async function syncPhoneFromAirtableToZoho(recordId, newPhoneValue) {
  console.log(`\n🔄 Syncing phone from Airtable record ${recordId} to Zoho...`);
  
  // Get field mapping for Phone field
  const phoneMapping = await getFieldMappingFor('Phone');
  if (!phoneMapping) {
    console.log(`⚠️  No field mapping found for Phone field`);
    return;
  }
  
  // Find the corresponding Zoho lead ID
  const zohoLeadId = await findZohoLeadByAirtableId(recordId);
  
  if (!zohoLeadId) {
    console.log(`⚠️  No corresponding Zoho lead found for Airtable record ${recordId}`);
    return;
  }
  
  // IMPORTANT: Record this sync BEFORE making the update to prevent webhook loop
  const { shouldSkipSync } = require('../utils/syncTracker');
  const zohoField = phoneMapping.zoho;
  const syncKey = `zoho:${zohoLeadId}:${zohoField}`;
  
  // Record that we're about to sync this value to Zoho
  // This will prevent processing the resulting webhook
  shouldSkipSync('zoho', zohoLeadId, zohoField, newPhoneValue);
  console.log(`📝 Pre-recorded sync to prevent webhook loop: ${syncKey}`);
  
  const fieldUpdates = {
    id: zohoLeadId
  };
  fieldUpdates[zohoField] = newPhoneValue;
  
  await updateZohoLead(zohoLeadId, fieldUpdates);
}

// Create Airtable record when new Zoho lead is created
async function createAirtableRecordFromZohoLead(leadId, leadData) {
  console.log(`\n📝 Creating Airtable record for new Zoho lead ${leadId}...`);
  
  try {
    const recordData = {
      fields: {}
    };
    
    // Get Zoho CRM ID mapping
    const zohoCrmIdMapping = await getZohoCrmIdMapping();
    if (zohoCrmIdMapping) {
      recordData.fields[zohoCrmIdMapping.airtable] = leadId;
      console.log(`📋 Adding Zoho CRM ID: ${leadId}`);
    }
    
    // Add phone if available and mapped
    const phoneMapping = await getFieldMappingFor('Phone');
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
  
  // Get all field mappings to check for changes
  const fieldMapping = await getFieldMapping();
  
  // Check each changed field to see if it should be synced
  for (const changedField of changedFieldsInfo.changedFields) {
    const mapping = await getFieldMappingFor(changedField);
    
    if (mapping) {
      const newValue = changedFieldsInfo.currentValues[changedField];
      console.log(`🔄 ${changedField} field changed in Zoho lead ${leadId}: ${newValue}`);
      
      // Check if we should skip this sync to prevent loops
      if (!shouldSkipSync('zoho', leadId, changedField, newValue)) {
        await syncFieldFromZohoToAirtable(leadId, changedField, newValue, mapping);
      }
    }
  }
}

// Handle Airtable record update - check for field changes and sync
async function handleAirtableRecordUpdate(recordId, changedFieldsInfo) {
  console.log(`\n🔍 Processing Airtable record update for ${recordId}`);
  
  // Get dynamic field mappings
  const fieldMapping = await getFieldMapping();
  console.log(`📋 Available field mappings: ${Object.keys(fieldMapping).join(', ')}`);
  
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
    
    // Debug: Show what we're comparing
    if (!mappedZohoField) {
      console.log(`🔍 Comparing against stored mappings:`);
      for (const [zohoField, fieldMap] of Object.entries(fieldMapping)) {
        console.log(`   ${zohoField}: "${fieldMap.airtable}" (${typeof fieldMap.airtable}) === "${fieldInfo.fieldId}" (${typeof fieldInfo.fieldId})? ${fieldMap.airtable === fieldInfo.fieldId}`);
        console.log(`   Length check: ${fieldMap.airtable.length} vs ${fieldInfo.fieldId.length}`);
        console.log(`   String match: ${JSON.stringify(fieldMap.airtable)} === ${JSON.stringify(fieldInfo.fieldId)}`);
      }
    }
    
    if (mappedZohoField && mapping) {
      console.log(`🔄 Syncing ${mappedZohoField} from Airtable to Zoho: ${JSON.stringify(fieldInfo.currentValue)}`);
      
      // Check if we should skip this sync to prevent loops
      const airtableFieldKey = fieldInfo.fieldId || fieldInfo.fieldName;
      if (!shouldSkipSync('airtable', recordId, airtableFieldKey, fieldInfo.currentValue)) {
        await syncFieldFromAirtableToZoho(recordId, mappedZohoField, fieldInfo.currentValue, mapping);
      } else {
        console.log(`⏭️  Skipping sync due to cooldown`);
      }
    } else {
      console.log(`⚠️  No Zoho mapping found for Airtable field: ${fieldInfo.fieldName || fieldInfo.fieldId}`);
    }
  }
}

// Generic function to sync any field from Zoho to Airtable
async function syncFieldFromZohoToAirtable(leadId, zohoFieldName, newValue, mapping) {
  console.log(`\n🔄 Syncing ${zohoFieldName} from Zoho lead ${leadId} to Airtable...`);
  
  // Find corresponding Airtable record
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
        return;
      }
    } else {
      console.log(`❌ Could not fetch lead details to create Airtable record`);
      return;
    }
  }
  
  // Pre-record sync to prevent webhook loop
  const { shouldSkipSync } = require('../utils/syncTracker');
  const airtableField = mapping.airtable;
  shouldSkipSync('airtable', airtableRecordId, airtableField, newValue);
  console.log(`📝 Pre-recorded sync to prevent webhook loop`);
  
  const fieldUpdates = {};
  fieldUpdates[airtableField] = newValue;
  
  await updateAirtableRecord(airtableRecordId, fieldUpdates);
}

// Generic function to sync any field from Airtable to Zoho
async function syncFieldFromAirtableToZoho(recordId, zohoFieldName, newValue, mapping) {
  console.log(`\n🔄 Syncing ${zohoFieldName} from Airtable record ${recordId} to Zoho...`);
  
  const zohoLeadId = await findZohoLeadByAirtableId(recordId);
  
  if (!zohoLeadId) {
    console.log(`⚠️  No corresponding Zoho lead found for Airtable record ${recordId}`);
    return;
  }
  
  // Pre-record sync to prevent webhook loop
  const { shouldSkipSync } = require('../utils/syncTracker');
  const zohoField = mapping.zoho;
  shouldSkipSync('zoho', zohoLeadId, zohoField, newValue);
  console.log(`📝 Pre-recorded sync to prevent webhook loop`);
  
  const fieldUpdates = {
    id: zohoLeadId
  };
  fieldUpdates[zohoField] = newValue;
  
  await updateZohoLead(zohoLeadId, fieldUpdates);
}

module.exports = {
  syncPhoneFromZohoToAirtable,
  syncPhoneFromAirtableToZoho,
  syncFieldFromZohoToAirtable,
  syncFieldFromAirtableToZoho,
  createAirtableRecordFromZohoLead,
  handleZohoLeadUpdate,
  handleAirtableRecordUpdate
};