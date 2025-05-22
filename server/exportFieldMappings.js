#!/usr/bin/env node

/**
 * Field Mapping Export Tool
 * 
 * Exports field mappings from Airtable "Zoho Fields" table to a JSON file.
 * This allows the sync service to work with saved mappings instead of 
 * fetching them dynamically every time.
 * 
 * Usage:
 * - Export to default file: node exportFieldMappings.js
 * - Export to custom file: node exportFieldMappings.js --output custom-mappings.json
 * - Export for specific module: node exportFieldMappings.js --module Leads
 * - Preview only (no save): node exportFieldMappings.js --preview
 * - Combined options: node exportFieldMappings.js --module Leads --output leads-mappings.json
 */

const fs = require('fs');
const path = require('path');
const { fetchDynamicFieldMapping } = require('./src/services/airtableService');

async function main() {
  const args = process.argv.slice(2);
  
  // Parse command line arguments
  const outputIndex = args.indexOf('--output');
  const moduleIndex = args.indexOf('--module');
  const previewMode = args.includes('--preview');
  
  const module = moduleIndex !== -1 && args[moduleIndex + 1] 
    ? args[moduleIndex + 1] 
    : null;
  
  const defaultOutputFile = module 
    ? path.join(__dirname, `field-mappings-${module.toLowerCase()}.json`)
    : path.join(__dirname, 'field-mappings.json');
    
  const outputFile = outputIndex !== -1 && args[outputIndex + 1] 
    ? path.join(__dirname, args[outputIndex + 1])
    : defaultOutputFile;

  console.log('🔍 Field Mapping Export Tool');
  console.log('=============================');
  
  if (module) {
    console.log(`🎯 Filtering for module: ${module}`);
  }
  
  try {
    console.log('📡 Fetching field mappings from Airtable...');
    
    // Fetch the dynamic field mapping with optional module filter
    const fieldMapping = await fetchDynamicFieldMapping(null, module);
    
    if (!fieldMapping || Object.keys(fieldMapping).length === 0) {
      console.error('❌ No field mappings found or failed to fetch from Airtable');
      if (module) {
        console.error(`   No mappings found for module "${module}"`);
        console.error('   Please check that the module exists and has associated field mappings');
      } else {
        console.error('   Please check your Airtable configuration and "Zoho Fields" table');
      }
      process.exit(1);
    }
    
    const mappingCount = Object.keys(fieldMapping).length;
    const moduleMsg = module ? ` for module "${module}"` : '';
    console.log(`✅ Successfully fetched ${mappingCount} field mappings${moduleMsg}`);
    
    // Display the mappings
    console.log('\n📋 Field Mappings:');
    console.log('==================');
    
    for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
      console.log(`├─ ${zohoField}`);
      console.log(`│  ├─ Zoho: ${mapping.zoho}`);
      console.log(`│  ├─ Airtable: ${mapping.airtable}`);
      console.log(`│  └─ Record ID: ${mapping.recordId}`);
    }
    
    if (previewMode) {
      console.log('\n👁️  Preview mode - not saving to file');
      return;
    }
    
    // Prepare the output data with metadata
    const outputData = {
      exportedAt: new Date().toISOString(),
      exportedBy: 'Field Mapping Export Tool',
      module: module || 'all',
      mappingCount: Object.keys(fieldMapping).length,
      fieldMappings: fieldMapping
    };
    
    // Save to file
    console.log(`\n💾 Saving field mappings to: ${outputFile}`);
    fs.writeFileSync(outputFile, JSON.stringify(outputData, null, 2));
    
    console.log('✅ Field mappings exported successfully!');
    console.log('\n📊 Export Summary:');
    console.log(`├─ Module: ${module || 'all'}`);
    console.log(`├─ Total mappings: ${Object.keys(fieldMapping).length}`);
    console.log(`├─ Output file: ${path.basename(outputFile)}`);
    console.log(`├─ File size: ${Math.round(fs.statSync(outputFile).size / 1024 * 100) / 100} KB`);
    console.log(`└─ Exported at: ${new Date().toLocaleString()}`);
    
    console.log('\n💡 Usage Tips:');
    console.log('  • Use this file as a backup of your field mappings');
    console.log('  • Re-export whenever you change field mappings in Airtable');
    console.log('  • Use --module parameter to export only specific module mappings');
    console.log('  • The sync service can optionally use this file as fallback');
    
  } catch (error) {
    console.error('❌ Export failed:', error.message);
    console.error('\n🔍 Troubleshooting:');
    console.error('  • Check your Airtable configuration in ../setup/airtable-config.json');
    console.error('  • Verify your API token has access to the "Zoho Fields" table');
    console.error('  • Ensure the "Zoho Fields" table exists and has the correct structure');
    process.exit(1);
  }
}

// Handle process termination gracefully
process.on('SIGINT', () => {
  console.log('\n\n🛑 Export interrupted by user');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n\n🛑 Export terminated');
  process.exit(0);
});

// Run the main function
main();