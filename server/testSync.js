#!/usr/bin/env node

/**
 * Test Sync Script
 * 
 * Quick test to verify all components are working
 */

const { loadZohoConfig, loadAirtableConfig } = require('./src/config/config');
const { getLeadsModifiedSince } = require('./src/services/zohoService');
const { getRecordsModifiedSince } = require('./src/services/airtableService');
const fieldMappingCache = require('./src/utils/fieldMappingCache');

async function testConnections() {
  console.log('🧪 Testing Sync Components');
  console.log('===========================\n');
  
  try {
    // Test 1: Configuration
    console.log('1️⃣  Testing Configuration...');
    const zohoConfig = loadZohoConfig();
    const airtableConfig = loadAirtableConfig();
    
    if (!zohoConfig) {
      console.log('❌ Zoho config not found');
      return;
    }
    if (!airtableConfig) {
      console.log('❌ Airtable config not found');
      return;
    }
    
    console.log('✅ Configurations loaded');
    console.log(`   - Zoho Domain: ${zohoConfig.apiDomain}`);
    console.log(`   - Airtable Base: ${airtableConfig.baseId}`);
    console.log(`   - Table: ${airtableConfig.tableName}\n`);
    
    // Test 2: Field Mapping Cache
    console.log('2️⃣  Testing Field Mapping Cache...');
    await fieldMappingCache.initialize();
    await new Promise(resolve => setTimeout(resolve, 3000)); // Wait for cache
    
    const status = fieldMappingCache.getStatus();
    console.log(`✅ Field mapping cache: ${status.mappingCount} fields mapped`);
    console.log(`   - Last updated: ${status.lastUpdated}\n`);
    
    // Test 3: Zoho API
    console.log('3️⃣  Testing Zoho API...');
    const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);
    const zohoResponse = await getLeadsModifiedSince(oneDayAgo);
    
    if (zohoResponse) {
      console.log(`✅ Zoho API working: ${zohoResponse.data?.length || 0} leads found in last 24h`);
    } else {
      console.log('❌ Zoho API failed');
    }
    console.log('');
    
    // Test 4: Airtable API
    console.log('4️⃣  Testing Airtable API...');
    const airtableResponse = await getRecordsModifiedSince(oneDayAgo);
    
    if (airtableResponse) {
      console.log(`✅ Airtable API working: ${airtableResponse.records?.length || 0} records found in last 24h`);
    } else {
      console.log('❌ Airtable API failed');
    }
    console.log('');
    
    // Test 5: Field Mappings
    console.log('5️⃣  Testing Field Mappings...');
    const mappings = fieldMappingCache.getFieldMapping();
    if (mappings && Object.keys(mappings).length > 0) {
      console.log('✅ Field mappings available:');
      Object.entries(mappings).forEach(([zohoField, mapping]) => {
        console.log(`   - ${zohoField} → ${mapping.airtable}`);
      });
    } else {
      console.log('❌ No field mappings found');
    }
    
    console.log('\n🎉 All tests completed!');
    console.log('\n📋 Ready to run sync:');
    console.log('   npm run sync          # Run sync now');
    console.log('   npm run sync:status    # Check status');
    console.log('   npm run sync:full      # Full 30-day sync');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  } finally {
    fieldMappingCache.destroy();
  }
}

testConnections();