#!/usr/bin/env node

/**
 * Diagnostics - Check Field Mappings and Data
 */

const { getAllRecordsForSync, getRecordById } = require('./src/services/airtableService');
const { getMultipleLeadDetails } = require('./src/services/zohoService');
const fieldMappingCache = require('./src/utils/fieldMappingCache');

async function diagnose() {
  console.log('🔍 Diagnostic Analysis');
  console.log('======================\n');
  
  try {
    // Initialize field mapping cache
    console.log('🔧 Initializing field mapping cache...');
    await fieldMappingCache.initialize();
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    const fieldMapping = fieldMappingCache.getFieldMapping();
    console.log(`✅ Field mapping loaded: ${Object.keys(fieldMapping).length} mappings\n`);
    
    // Get sample Airtable record
    console.log('📥 Fetching sample Airtable record...');
    const airtableData = await getAllRecordsForSync();
    
    if (airtableData && airtableData.records && airtableData.records.length > 0) {
      const sampleRecord = airtableData.records[0];
      console.log(`✅ Found sample record: ${sampleRecord.id}\n`);
      
      console.log('📋 Available Airtable Fields:');
      Object.entries(sampleRecord.fields).forEach(([field, value]) => {
        console.log(`   - ${field}: ${typeof value} = "${value}"`);
      });
      
      console.log('\n🗺️  First 10 Field Mappings:');
      Object.entries(fieldMapping).slice(0, 10).forEach(([zohoField, mapping]) => {
        const airtableValue = sampleRecord.fields[mapping.airtable];
        const hasValue = airtableValue !== undefined ? '✅' : '❌';
        console.log(`   ${hasValue} ${zohoField} → ${mapping.airtable} (${typeof airtableValue})`);
      });
      
      // Count matches
      let hasValue = 0;
      let undefined = 0;
      
      Object.entries(fieldMapping).forEach(([zohoField, mapping]) => {
        if (sampleRecord.fields[mapping.airtable] !== undefined) {
          hasValue++;
        } else {
          undefined++;
        }
      });
      
      console.log(`\n📊 Field Mapping Summary:`);
      console.log(`   - Total mappings: ${Object.keys(fieldMapping).length}`);
      console.log(`   - Fields with values: ${hasValue}`);
      console.log(`   - Fields undefined: ${undefined}`);
      console.log(`   - Success rate: ${Math.round((hasValue / Object.keys(fieldMapping).length) * 100)}%`);
      
      if (hasValue === 0) {
        console.log('\n❌ PROBLEM: No field mappings match the actual Airtable data!');
        console.log('💡 This suggests the field mappings are for a different table or base.');
      }
      
    } else {
      console.log('❌ No Airtable records found');
    }
    
    // Get sample Zoho lead
    console.log('\n📥 Fetching sample Zoho lead...');
    const zohoData = await getMultipleLeadDetails(['4717452000028122037']);
    
    if (zohoData && zohoData.data && zohoData.data.length > 0) {
      const sampleLead = zohoData.data[0];
      console.log(`✅ Found sample lead: ${sampleLead.id}\n`);
      
      console.log('📋 Sample Zoho Lead Fields:');
      Object.entries(sampleLead).slice(0, 15).forEach(([field, value]) => {
        console.log(`   - ${field}: ${typeof value} = "${value}"`);
      });
    }
    
  } catch (error) {
    console.error('❌ Diagnostic failed:', error.message);
  } finally {
    fieldMappingCache.destroy();
  }
}

diagnose();