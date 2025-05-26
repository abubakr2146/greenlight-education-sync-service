#!/usr/bin/env node

/**
 * Duplicate Removal Script - Standalone
 * 
 * Removes duplicate records from both Zoho CRM and Airtable for a specified module,
 * keeping the most recent records based on modification timestamps.
 * 
 * Finds duplicates separately in each system:
 * - Zoho: Records with same Zoho ID
 * - Airtable: Records with same Zoho CRM ID value
 * 
 * Usage:
 * node removeDuplicates.js --module Leads
 * node removeDuplicates.js --module Partners --dry-run
 * node removeDuplicates.js --module Contacts --verbose
 */

const axios = require('axios');
const yargs = require('yargs');
const { loadZohoConfig, loadAirtableConfig } = require('./src/config/config');
const { 
  getRecordsModifiedSince, 
  deleteZohoRecord, 
  getZohoModulePluralName,
  refreshZohoToken 
} = require('./src/services/zohoService');
const { 
  getModuleTableConfig 
} = require('./src/services/moduleConfigService');
const fieldMappingCache = require('./src/utils/fieldMappingCache');

class DuplicateRemover {
  constructor(options = {}) {
    this.moduleName = options.module || 'Leads';
    this.dryRun = options.dryRun || false;
    this.verbose = options.verbose || false;
    this.stats = {
      zohoRecordsProcessed: 0,
      airtableRecordsProcessed: 0,
      zohoRecordsDeleted: 0,
      airtableRecordsDeleted: 0,
      zohoDuplicateGroups: 0,
      airtableDuplicateGroups: 0,
      errors: 0
    };
    this.zohoConfig = null;
    this.airtableConfig = null;
    this.moduleTableConfig = null;
    this.zohoCrmIdField = null; // Will store the Airtable field name/ID for Zoho CRM ID
  }

  async initialize() {
    console.log('🚀 Duplicate Removal Script');
    console.log('============================');
    console.log(`📋 Module: ${this.moduleName}`);
    console.log(`🔍 Detection: Zoho ID duplicates in both systems`);
    console.log(`🎯 Mode: ${this.dryRun ? 'DRY RUN (no deletions)' : 'LIVE DELETION'}`);
    console.log(`📊 Verbose: ${this.verbose ? 'ON' : 'OFF'}\n`);

    // Load configurations
    this.zohoConfig = loadZohoConfig();
    this.airtableConfig = loadAirtableConfig();

    if (!this.zohoConfig || !this.airtableConfig) {
      throw new Error('Missing Zoho or Airtable configuration');
    }

    // Get module-specific table configuration
    try {
      this.moduleTableConfig = await getModuleTableConfig(this.moduleName);
      console.log(`✅ Module configuration loaded:`);
      console.log(`   Airtable Table: ${this.moduleTableConfig.tableName} (${this.moduleTableConfig.tableId})`);
      console.log(`   Zoho Module: ${this.moduleTableConfig.zohoModuleName}\n`);
    } catch (error) {
      throw new Error(`Failed to load module configuration: ${error.message}`);
    }

    // Initialize field mapping cache to get Zoho CRM ID field
    console.log('🔧 Initializing field mapping cache...');
    await fieldMappingCache.initialize(this.moduleName);
    
    const fieldStatus = fieldMappingCache.getStatus(this.moduleName);
    if (!fieldStatus || !fieldStatus.initialized) {
      throw new Error(`Field mapping cache failed to initialize for module ${this.moduleName}`);
    }
    
    // Get the Zoho CRM ID field mapping
    const { getZohoCrmIdMapping } = require('./src/services/syncService');
    const zohoCrmIdMapping = await getZohoCrmIdMapping(this.moduleName);
    if (!zohoCrmIdMapping || !zohoCrmIdMapping.airtable) {
      throw new Error(`Could not find Zoho CRM ID field mapping for module ${this.moduleName}`);
    }
    this.zohoCrmIdField = zohoCrmIdMapping.airtable;
    
    console.log(`✅ Field mapping ready: ${fieldStatus.mappingCount} fields mapped`);
    console.log(`ℹ️  Using Airtable field '${this.zohoCrmIdField}' for Zoho CRM ID\n`);
  }

  async run() {
    try {
      await this.initialize();

      console.log('📥 Fetching records from both systems...');
      const [zohoRecords, airtableRecords] = await Promise.all([
        this.getAllZohoRecords(),
        this.getAllAirtableRecords()
      ]);

      this.stats.zohoRecordsProcessed = zohoRecords.length;
      this.stats.airtableRecordsProcessed = airtableRecords.length;

      console.log(`📊 Found ${zohoRecords.length} Zoho records, ${airtableRecords.length} Airtable records\n`);

      // Process Zoho duplicates
      console.log('🔍 Analyzing Zoho duplicates (same Zoho ID)...');
      const zohoDuplicateGroups = this.identifyZohoDuplicates(zohoRecords);
      this.stats.zohoDuplicateGroups = zohoDuplicateGroups.length;
      
      console.log(`📋 Found ${zohoDuplicateGroups.length} Zoho duplicate groups`);
      if (this.verbose) {
        this.logDuplicateGroups(zohoDuplicateGroups, 'Zoho');
      }

      // Process Airtable duplicates
      console.log('\n🔍 Analyzing Airtable duplicates (same Zoho CRM ID)...');
      const airtableDuplicateGroups = this.identifyAirtableDuplicates(airtableRecords);
      this.stats.airtableDuplicateGroups = airtableDuplicateGroups.length;
      
      console.log(`📋 Found ${airtableDuplicateGroups.length} Airtable duplicate groups`);
      if (this.verbose) {
        this.logDuplicateGroups(airtableDuplicateGroups, 'Airtable');
      }

      if (this.dryRun) {
        console.log('\n🔍 DRY RUN - No records will be deleted');
        this.showDryRunSummary(zohoDuplicateGroups, airtableDuplicateGroups);
      } else {
        console.log('\n⚡ Removing duplicates...');
        await this.removeDuplicates(zohoDuplicateGroups, airtableDuplicateGroups);
      }

      this.showResults();

    } catch (error) {
      console.error(`❌ Duplicate removal failed: ${error.message}`);
      if (this.verbose) console.error(error.stack);
      process.exit(1);
    } finally {
      // Cleanup
      fieldMappingCache.destroyModule(this.moduleName);
    }
  }

  async getAllZohoRecords() {
    try {
      // Refresh token if needed
      if (Date.now() >= this.zohoConfig.tokenExpiry) {
        const refreshed = await refreshZohoToken(this.zohoConfig);
        if (!refreshed) {
          throw new Error('Failed to refresh Zoho token');
        }
      }

      const modulePlural = getZohoModulePluralName(this.moduleName);
      let allRecords = [];
      let page = 1;
      const perPage = 200;

      while (true) {
        const response = await axios.get(
          `${this.zohoConfig.apiDomain}/crm/v2/${modulePlural}`,
          {
            headers: {
              'Authorization': `Zoho-oauthtoken ${this.zohoConfig.accessToken}`,
              'Content-Type': 'application/json'
            },
            params: {
              page: page,
              per_page: perPage
            }
          }
        );

        const records = response.data.data || [];
        allRecords.push(...records);

        if (records.length < perPage) {
          break;
        }
        page++;
      }

      return allRecords;
    } catch (error) {
      throw new Error(`Failed to fetch Zoho records: ${error.message}`);
    }
  }

  async getAllAirtableRecords() {
    try {
      let allRecords = [];
      let offset = null;

      do {
        const params = {};
        if (offset) params.offset = offset;

        const response = await axios.get(
          `${this.airtableConfig.apiUrl}/${this.airtableConfig.baseId}/${this.moduleTableConfig.tableId}`,
          {
            headers: {
              'Authorization': `Bearer ${this.airtableConfig.apiToken}`,
              'Content-Type': 'application/json'
            },
            params: params
          }
        );

        const records = response.data.records || [];
        allRecords.push(...records);
        offset = response.data.offset;

      } while (offset);

      return allRecords;
    } catch (error) {
      throw new Error(`Failed to fetch Airtable records: ${error.message}`);
    }
  }

  identifyZohoDuplicates(zohoRecords) {
    const duplicateGroups = [];
    const idGroups = new Map(); // Map: Zoho ID -> [records with that ID]

    // Group Zoho records by their ID
    for (const record of zohoRecords) {
      const zohoId = record.id;
      if (!zohoId) continue; // Skip records without IDs
      
      if (!idGroups.has(zohoId)) {
        idGroups.set(zohoId, []);
      }
      idGroups.get(zohoId).push(record);
    }

    // Find groups with more than one record (duplicates)
    for (const [zohoId, recordGroup] of idGroups) {
      if (recordGroup.length > 1) {
        // Sort by modification time (newest first)
        recordGroup.sort((a, b) => {
          const timeA = new Date(a.Modified_Time).getTime();
          const timeB = new Date(b.Modified_Time).getTime();
          return timeB - timeA;
        });

        duplicateGroups.push(recordGroup);
      }
    }

    return duplicateGroups;
  }

  identifyAirtableDuplicates(airtableRecords) {
    const duplicateGroups = [];
    const idGroups = new Map(); // Map: Zoho CRM ID -> [records with that Zoho CRM ID]

    // Group Airtable records by their Zoho CRM ID
    for (const record of airtableRecords) {
      const zohoCrmId = record.fields[this.zohoCrmIdField];
      if (!zohoCrmId) continue; // Skip records without Zoho CRM IDs
      
      if (!idGroups.has(zohoCrmId)) {
        idGroups.set(zohoCrmId, []);
      }
      idGroups.get(zohoCrmId).push(record);
    }

    // Find groups with more than one record (duplicates)
    for (const [zohoCrmId, recordGroup] of idGroups) {
      if (recordGroup.length > 1) {
        // Sort by modification time (newest first)
        recordGroup.sort((a, b) => {
          const timeA = new Date(a.fields['Last Modified Time'] || a.createdTime).getTime();
          const timeB = new Date(b.fields['Last Modified Time'] || b.createdTime).getTime();
          return timeB - timeA;
        });

        duplicateGroups.push(recordGroup);
      }
    }

    return duplicateGroups;
  }

  getModificationTime(record, system) {
    if (system === 'zoho') {
      return new Date(record.Modified_Time).getTime();
    } else {
      return new Date(record.fields['Last Modified Time'] || record.createdTime).getTime();
    }
  }

  logDuplicateGroups(groups, systemName) {
    console.log(`\n📄 ${systemName} Duplicate Groups:`);
    groups.forEach((group, index) => {
      const duplicateId = systemName === 'Zoho' ? group[0].id : group[0].fields[this.zohoCrmIdField];
      console.log(`\n  Group ${index + 1} - ${systemName === 'Zoho' ? 'Zoho ID' : 'Zoho CRM ID'}: ${duplicateId} (${group.length} records):`);
      
      group.forEach((record, recordIndex) => {
        const recordId = systemName === 'Zoho' ? record.id : record.id;
        const modTime = systemName === 'Zoho' ? 
          new Date(record.Modified_Time).getTime() : 
          new Date(record.fields['Last Modified Time'] || record.createdTime).getTime();
        const isKeep = recordIndex === 0;
        const action = isKeep ? '🟢 KEEP' : '🔴 DELETE';
        
        console.log(`    ${action} Record ${recordId} - Modified: ${new Date(modTime).toISOString()}`);
        
        if (this.verbose && systemName === 'Zoho') {
          // Show some key Zoho fields
          const fields = ['Email', 'Phone', 'First_Name', 'Last_Name', 'Company'].filter(f => record[f]);
          if (fields.length > 0) {
            const fieldInfo = fields.map(f => `${f}: ${record[f]}`).join(', ');
            console.log(`      ${fieldInfo}`);
          }
        } else if (this.verbose && systemName === 'Airtable') {
          // Show the Zoho CRM ID
          console.log(`      Zoho CRM ID: ${record.fields[this.zohoCrmIdField]}`);
        }
      });
    });
  }

  showDryRunSummary(zohoDuplicateGroups, airtableDuplicateGroups) {
    const zohoToDelete = zohoDuplicateGroups.reduce((sum, group) => sum + (group.length - 1), 0);
    const airtableToDelete = airtableDuplicateGroups.reduce((sum, group) => sum + (group.length - 1), 0);

    console.log('\n📊 Dry Run Summary:');
    console.log(`├─ Zoho records to delete: ${zohoToDelete} (from ${zohoDuplicateGroups.length} groups)`);
    console.log(`└─ Airtable records to delete: ${airtableToDelete} (from ${airtableDuplicateGroups.length} groups)`);
  }

  async removeDuplicates(zohoDuplicateGroups, airtableDuplicateGroups) {
    // Remove Zoho duplicates
    for (const group of zohoDuplicateGroups) {
      const [keep, ...toDelete] = group;
      console.log(`\n🔄 Processing Zoho group - keeping ${keep.id}, deleting ${toDelete.length} duplicates`);

      for (const record of toDelete) {
        try {
          const success = await this.deleteZohoRecord(record.id);
          if (success) {
            this.stats.zohoRecordsDeleted++;
            console.log(`  ✅ Deleted Zoho record ${record.id}`);
          } else {
            this.stats.errors++;
            console.log(`  ❌ Failed to delete Zoho record ${record.id}`);
          }
        } catch (error) {
          this.stats.errors++;
          console.log(`  ❌ Error deleting Zoho record ${record.id}: ${error.message}`);
        }
      }
    }

    // Remove Airtable duplicates
    for (const group of airtableDuplicateGroups) {
      const [keep, ...toDelete] = group;
      console.log(`\n🔄 Processing Airtable group - keeping ${keep.id}, deleting ${toDelete.length} duplicates`);

      for (const record of toDelete) {
        try {
          const success = await this.deleteAirtableRecord(record.id);
          if (success) {
            this.stats.airtableRecordsDeleted++;
            console.log(`  ✅ Deleted Airtable record ${record.id}`);
          } else {
            this.stats.errors++;
            console.log(`  ❌ Failed to delete Airtable record ${record.id}`);
          }
        } catch (error) {
          this.stats.errors++;
          console.log(`  ❌ Error deleting Airtable record ${record.id}: ${error.message}`);
        }
      }
    }
  }

  async deleteZohoRecord(recordId) {
    try {
      return await deleteZohoRecord(recordId, this.moduleName, this.zohoConfig);
    } catch (error) {
      console.error(`Failed to delete Zoho record ${recordId}:`, error.message);
      return false;
    }
  }

  async deleteAirtableRecord(recordId) {
    try {
      const response = await axios.delete(
        `${this.airtableConfig.apiUrl}/${this.airtableConfig.baseId}/${this.moduleTableConfig.tableId}/${recordId}`,
        {
          headers: {
            'Authorization': `Bearer ${this.airtableConfig.apiToken}`,
            'Content-Type': 'application/json'
          }
        }
      );

      return response.status === 200;
    } catch (error) {
      console.error(`Failed to delete Airtable record ${recordId}:`, error.message);
      return false;
    }
  }

  showResults() {
    console.log('\n🎯 Duplicate Removal Results');
    console.log('============================');
    console.log(`📊 Zoho Records Processed: ${this.stats.zohoRecordsProcessed}`);
    console.log(`📊 Airtable Records Processed: ${this.stats.airtableRecordsProcessed}`);
    console.log(`🗑️  Zoho Records Deleted: ${this.stats.zohoRecordsDeleted}`);
    console.log(`🗑️  Airtable Records Deleted: ${this.stats.airtableRecordsDeleted}`);
    console.log(`📈 Zoho Duplicate Groups: ${this.stats.zohoDuplicateGroups}`);
    console.log(`📈 Airtable Duplicate Groups: ${this.stats.airtableDuplicateGroups}`);
    console.log(`❌ Errors: ${this.stats.errors}`);
    
    const totalDeleted = this.stats.zohoRecordsDeleted + this.stats.airtableRecordsDeleted;
    console.log(`\n✅ Total records removed: ${totalDeleted}`);
  }
}

// CLI setup
const argv = yargs
  .option('module', {
    alias: 'm',
    description: 'Module name (Leads, Partners, Contacts, etc.)',
    type: 'string',
    demandOption: true
  })
  .option('dry-run', {
    alias: 'd',
    description: 'Preview duplicates without deleting',
    type: 'boolean',
    default: false
  })
  .option('verbose', {
    alias: 'v',
    description: 'Verbose logging',
    type: 'boolean',
    default: false
  })
  .help()
  .alias('help', 'h')
  .argv;

// Run the script
const remover = new DuplicateRemover({
  module: argv.module,
  dryRun: argv.dryRun || argv.d,
  verbose: argv.verbose || argv.v
});

remover.run().catch(error => {
  console.error('❌ Script failed:', error.message);
  process.exit(1);
});