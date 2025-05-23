#!/usr/bin/env node

/**
 * Test Module Configuration
 * 
 * Tests the dynamic module configuration system
 * 
 * Usage:
 * - node testModuleConfig.js          # Test all modules
 * - node testModuleConfig.js Partners # Test specific module
 */

const { 
  getModuleConfig, 
  getModuleTableConfig, 
  getAvailableModules,
  isValidModule,
  getZohoModulePluralName 
} = require('./src/services/moduleConfigService');

const fieldMappingCache = require('./src/utils/fieldMappingCache');
const { getRecordDetails, getRecordsModifiedSince } = require('./src/services/zohoService');
const { getModuleRecordsModifiedSince, getAllModuleRecordsForSync } = require('./src/services/airtableService');

async function testModule(moduleName) {
  console.log(`\n🧪 Testing module: ${moduleName}`);
  console.log('='.repeat(50));
  
  try {
    // Test 1: Module validation
    console.log('\n1️⃣  Validating module...');
    const isValid = await isValidModule(moduleName);
    console.log(`   ✅ Module is valid: ${isValid}`);
    
    if (!isValid) {
      console.log(`   ❌ Module '${moduleName}' is not configured in Airtable`);
      return;
    }
    
    // Test 2: Get module configuration
    console.log('\n2️⃣  Getting module configuration...');
    const moduleConfig = await getModuleConfig(moduleName);
    console.log(`   ✅ Module record ID: ${moduleConfig.id}`);
    console.log(`   ✅ API Name: ${moduleConfig.fields['api_name']}`);
    console.log(`   ✅ Display Name: ${moduleConfig.fields['Name'] || 'N/A'}`);
    
    // Test 3: Get table configuration
    console.log('\n3️⃣  Getting table configuration...');
    const tableConfig = await getModuleTableConfig(moduleName);
    console.log(`   ✅ Airtable Table Name: ${tableConfig.tableName}`);
    console.log(`   ✅ Airtable Table ID: ${tableConfig.tableId}`);
    console.log(`   ✅ Zoho Module Name: ${tableConfig.zohoModuleName}`);
    
    // Test 4: Get Zoho plural name
    console.log('\n4️⃣  Getting Zoho module plural name...');
    const pluralName = getZohoModulePluralName(moduleName);
    console.log(`   ✅ Zoho API endpoint: /crm/v2/${pluralName}`);
    
    // Test 5: Initialize field mapping cache
    console.log('\n5️⃣  Initializing field mapping cache...');
    await fieldMappingCache.initialize(moduleName);
    
    // Wait for cache to populate
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    const cacheStatus = fieldMappingCache.getStatus(moduleName);
    console.log(`   ✅ Cache initialized: ${cacheStatus.initialized}`);
    console.log(`   ✅ Field mappings loaded: ${cacheStatus.mappingCount}`);
    
    if (cacheStatus.mappingCount > 0) {
      const fieldMapping = fieldMappingCache.getFieldMapping(moduleName);
      console.log(`   📋 Sample field mappings:`);
      const sampleFields = Object.entries(fieldMapping).slice(0, 3);
      for (const [zohoField, mapping] of sampleFields) {
        console.log(`      - ${zohoField} → ${mapping.airtable}`);
      }
    }
    
    // Test 6: Test Zoho API call
    console.log('\n6️⃣  Testing Zoho API...');
    try {
      const recentRecords = await getRecordsModifiedSince(0, moduleName);
      if (recentRecords && recentRecords.data) {
        console.log(`   ✅ Zoho API working - Found ${recentRecords.data.length} ${moduleName} records`);
      } else {
        console.log(`   ⚠️  No ${moduleName} records found in Zoho`);
      }
    } catch (error) {
      console.log(`   ❌ Zoho API error: ${error.message}`);
    }
    
    // Test 7: Test Airtable API call
    console.log('\n7️⃣  Testing Airtable API...');
    try {
      const airtableRecords = await getAllModuleRecordsForSync(moduleName);
      if (airtableRecords && airtableRecords.records) {
        console.log(`   ✅ Airtable API working - Found ${airtableRecords.records.length} records in ${tableConfig.tableName} table`);
      } else {
        console.log(`   ⚠️  No records found in Airtable ${tableConfig.tableName} table`);
      }
    } catch (error) {
      console.log(`   ❌ Airtable API error: ${error.message}`);
    }
    
    console.log(`\n✅ Module '${moduleName}' is ready for sync!`);
    
  } catch (error) {
    console.error(`\n❌ Error testing module '${moduleName}':`, error.message);
    if (error.stack) {
      console.error('Stack trace:', error.stack);
    }
  }
}

async function main() {
  const args = process.argv.slice(2);
  const specificModule = args[0];
  
  console.log('🚀 Module Configuration Test');
  console.log('============================');
  
  try {
    if (specificModule) {
      // Test specific module
      await testModule(specificModule);
    } else {
      // Test all available modules
      console.log('\n📋 Fetching available modules...');
      const modules = await getAvailableModules();
      console.log(`✅ Found ${modules.length} configured modules: ${modules.join(', ')}`);
      
      // Test each module
      for (const module of modules) {
        await testModule(module);
      }
    }
    
    console.log('\n🎉 Module configuration test completed!');
    
  } catch (error) {
    console.error('\n❌ Fatal error:', error.message);
    process.exit(1);
  } finally {
    // Cleanup
    fieldMappingCache.destroy();
  }
}

// Handle process termination
process.on('SIGINT', () => {
  console.log('\n\n🛑 Test interrupted');
  fieldMappingCache.destroy();
  process.exit(0);
});

// Run the test
main();