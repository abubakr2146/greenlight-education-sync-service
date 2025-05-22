#!/usr/bin/env node

/**
 * Bulk Sync - Complete Lead Synchronization
 * 
 * Fetches ALL leads from both systems and syncs based on latest modified time
 * 
 * Usage:
 * - Full bulk sync: node bulkSync.js
 * - Dry run: node bulkSync.js --dry-run
 * - Verbose: node bulkSync.js --verbose
 */

const axios = require('axios');
const { loadZohoConfig, loadAirtableConfig } = require('./src/config/config');
const { getMultipleLeadDetails, refreshZohoToken } = require('./src/services/zohoService');
const { 
  getAllRecordsForSync, 
  getRecordById,
  findAirtableRecordByZohoId,
  getFieldIdToNameMapping 
} = require('./src/services/airtableService');
const { 
  createAirtableRecordFromZohoLead, 
  syncFieldFromZohoToAirtable,
  syncFieldFromAirtableToZoho 
} = require('./src/services/syncService');
const fieldMappingCache = require('./src/utils/fieldMappingCache');

class BulkSync {
  constructor(options = {}) {
    this.dryRun = options.dryRun || false;
    this.verbose = options.verbose || false;
    this.stats = {
      totalZohoLeads: 0,
      totalAirtableRecords: 0,
      zohoToAirtable: 0,
      airtableToZoho: 0,
      newAirtableRecords: 0,
      conflicts: 0,
      errors: 0,
      skipped: 0
    };
  }

  async run() {
    try {
      console.log('🚀 Starting Bulk Lead Synchronization');
      console.log('=====================================');
      console.log(`🎯 Mode: ${this.dryRun ? 'DRY RUN (no changes)' : 'LIVE SYNC'}`);
      console.log(`📊 Verbose: ${this.verbose ? 'ON' : 'OFF'}\n`);

      // Initialize field mapping cache
      console.log('🔧 Initializing field mapping cache...');
      await fieldMappingCache.initialize('Leads');
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      const fieldStatus = fieldMappingCache.getStatus();
      console.log(`✅ Field mapping ready: ${fieldStatus.mappingCount} fields mapped\n`);

      // Step 1: Fetch all data
      console.log('📥 Fetching all leads from both systems...');
      const [zohoLeads, airtableRecords] = await Promise.all([
        this.getAllZohoLeads(),
        this.getAllAirtableRecords()
      ]);

      this.stats.totalZohoLeads = zohoLeads.length;
      this.stats.totalAirtableRecords = airtableRecords.length;

      console.log(`📊 Found ${zohoLeads.length} Zoho leads, ${airtableRecords.length} Airtable records\n`);

      // Step 2: Create sync plan
      console.log('📋 Creating sync plan...');
      const syncPlan = this.createBulkSyncPlan(zohoLeads, airtableRecords);
      
      console.log('📈 Sync Plan Summary:');
      console.log(`├─ Zoho → Airtable: ${syncPlan.zohoToAirtable.length}`);
      console.log(`├─ Airtable → Zoho: ${syncPlan.airtableToZoho.length}`);
      console.log(`├─ New Airtable records: ${syncPlan.newRecords.length}`);
      console.log(`├─ Conflicts (manual review): ${syncPlan.conflicts.length}`);
      console.log(`└─ No sync needed: ${syncPlan.noSyncNeeded.length}\n`);

      if (this.dryRun) {
        console.log('🔍 DRY RUN - No changes will be made\n');
        this.showDetailedPlan(syncPlan);
        return;
      }

      // Step 3: Execute sync
      console.log('⚡ Executing sync plan...');
      await this.executeBulkSyncPlan(syncPlan);

      // Step 4: Show results
      this.showResults();

    } catch (error) {
      console.error('❌ Bulk sync failed:', error.message);
      if (this.verbose) {
        console.error(error.stack);
      }
    } finally {
      fieldMappingCache.destroy();
    }
  }

  async getAllZohoLeads() {
    const config = loadZohoConfig();
    if (!config) {
      throw new Error('Zoho config not found');
    }

    try {
      // Check if token needs refresh
      if (Date.now() >= config.tokenExpiry) {
        await refreshZohoToken(config);
      }

      let allLeadIds = [];
      let page = 1;
      const perPage = 200;

      console.log('📥 Fetching Zoho lead IDs...');

      // First, get all lead IDs with timestamps
      while (true) {
        const response = await axios.get(
          `${config.apiDomain}/crm/v2/Leads`,
          {
            headers: {
              'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
              'Content-Type': 'application/json'
            },
            params: {
              page,
              per_page: perPage,
              fields: 'id,Modified_Time,Created_Time', // Only get IDs and timestamps first
              sort_by: 'Modified_Time',
              sort_order: 'desc'
            }
          }
        );

        const pageLeads = response.data.data || [];
        allLeadIds.push(...pageLeads);

        if (this.verbose) {
          console.log(`   Page ${page}: ${pageLeads.length} lead IDs (total: ${allLeadIds.length})`);
        } else {
          process.stdout.write('.');
        }

        if (pageLeads.length < perPage) {
          break; // Last page
        }

        page++;
      }

      if (!this.verbose) {
        console.log(''); // New line after dots
      }

      console.log('📥 Fetching full lead details...');

      // Now get full details for all leads in batches
      const allLeads = [];
      const batchSize = 100; // Zoho allows up to 100 per request
      
      for (let i = 0; i < allLeadIds.length; i += batchSize) {
        const batch = allLeadIds.slice(i, i + batchSize);
        const leadIds = batch.map(lead => lead.id);
        
        if (this.verbose) {
          console.log(`   Fetching details for leads ${i + 1}-${Math.min(i + batchSize, allLeadIds.length)}`);
        }
        
        const detailsResponse = await getMultipleLeadDetails(leadIds, config);
        if (detailsResponse && detailsResponse.data) {
          allLeads.push(...detailsResponse.data);
        }
        
        if (!this.verbose) {
          process.stdout.write('.');
        }
      }

      if (!this.verbose) {
        console.log(''); // New line after dots
      }

      return allLeads.map(lead => {
        // Try different timestamp fields in order of preference
        let modifiedTime = new Date(lead.Modified_Time).getTime();
        let timestampSource = 'Modified_Time';
        
        if (isNaN(modifiedTime) && lead.Last_Activity_Time) {
          modifiedTime = new Date(lead.Last_Activity_Time).getTime();
          timestampSource = 'Last_Activity_Time';
        }
        
        if (isNaN(modifiedTime) && lead.Created_Time) {
          modifiedTime = new Date(lead.Created_Time).getTime();
          timestampSource = 'Created_Time';
        }
        
        if (isNaN(modifiedTime)) {
          modifiedTime = Date.now();
          timestampSource = 'current_time';
        }
        
        if (this.verbose) {
          console.log(`📅 Lead ${lead.id}: Using ${timestampSource} = ${new Date(modifiedTime).toISOString()}`);
        }
        
        return {
          id: lead.id,
          modifiedTime,
          data: lead,
          source: 'zoho',
          timestampSource
        };
      });

    } catch (error) {
      throw new Error(`Failed to fetch Zoho leads: ${error.message}`);
    }
  }

  async getAllAirtableRecords() {
    const config = loadAirtableConfig();
    if (!config) {
      throw new Error('Airtable config not found');
    }

    try {
      let allRecords = [];
      let offset = null;
      
      console.log('📥 Fetching complete Airtable records...');

      do {
        const params = {
          pageSize: 100,
          sort: [{ field: 'Last Modified Time', direction: 'desc' }]
          // Get ALL fields, not just Zoho CRM ID and Last Modified Time
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
        
        if (this.verbose) {
          console.log(`   Page: ${pageRecords.length} records (total: ${allRecords.length})`);
        } else {
          process.stdout.write('.');
        }
        
        // Update offset for next iteration
        offset = response.data.offset;
        
        // Stop if no more records or no offset
        if (!offset || pageRecords.length === 0) {
          break;
        }
        
      } while (offset);

      if (!this.verbose) {
        console.log(''); // New line after dots
      }

      return allRecords
        .filter(record => record.fields['Zoho CRM ID']) // Only records with Zoho ID
        .map(record => {
          const lastModified = record.fields['Last Modified Time'];
          const modifiedTime = new Date(lastModified).getTime();
          
          if (this.verbose && isNaN(modifiedTime)) {
            console.log(`⚠️  Invalid timestamp for record ${record.id}: "${lastModified}"`);
          }
          
          return {
            id: record.id,
            zohoId: record.fields['Zoho CRM ID'],
            modifiedTime: isNaN(modifiedTime) ? Date.now() : modifiedTime,
            data: record,
            source: 'airtable'
          };
        });

    } catch (error) {
      throw new Error(`Failed to fetch Airtable records: ${error.message}`);
    }
  }

  createBulkSyncPlan(zohoLeads, airtableRecords) {
    const plan = {
      zohoToAirtable: [],
      airtableToZoho: [],
      newRecords: [],
      conflicts: [],
      noSyncNeeded: []
    };

    // Create maps for quick lookup
    const zohoMap = new Map(zohoLeads.map(lead => [lead.id, lead]));
    const airtableMap = new Map(airtableRecords.map(record => [record.zohoId, record]));

    // Process Zoho leads
    for (const zohoLead of zohoLeads) {
      const airtableRecord = airtableMap.get(zohoLead.id);

      if (!airtableRecord) {
        // No Airtable record exists - create new one
        plan.newRecords.push(zohoLead);
      } else {
        // Both exist - compare timestamps
        const zohoTime = zohoLead.modifiedTime;
        const airtableTime = airtableRecord.modifiedTime;
        
        // Debug invalid timestamps
        if (this.verbose && (isNaN(zohoTime) || isNaN(airtableTime))) {
          console.log(`⚠️  Invalid timestamp comparison - Zoho: ${zohoTime} (${zohoLead.data.Modified_Time}), Airtable: ${airtableTime} (${airtableRecord.data.fields['Last Modified Time']})`);
        }
        
        const timeDiff = Math.abs(zohoTime - airtableTime);
        
        if (timeDiff < 30000) { // Less than 30 seconds - consider same time
          plan.noSyncNeeded.push({
            zoho: zohoLead,
            airtable: airtableRecord,
            reason: 'Times too close'
          });
        } else if (zohoTime > airtableTime) {
          // Zoho is newer
          plan.zohoToAirtable.push({
            zoho: zohoLead,
            airtable: airtableRecord,
            timeDiff,
            zohoTime: new Date(zohoTime).toISOString(),
            airtableTime: new Date(airtableTime).toISOString()
          });
        } else {
          // Airtable is newer
          plan.airtableToZoho.push({
            airtable: airtableRecord,
            zoho: zohoLead,
            timeDiff,
            zohoTime: new Date(zohoTime).toISOString(),
            airtableTime: new Date(airtableTime).toISOString()
          });
        }
      }
    }

    // Process Airtable records that don't have corresponding Zoho leads
    for (const airtableRecord of airtableRecords) {
      if (!zohoMap.has(airtableRecord.zohoId)) {
        plan.conflicts.push({
          type: 'orphaned_airtable',
          airtable: airtableRecord,
          reason: 'Airtable record references non-existent Zoho lead'
        });
      }
    }

    return plan;
  }

  async executeBulkSyncPlan(plan) {
    let processed = 0;
    const total = plan.zohoToAirtable.length + plan.airtableToZoho.length + plan.newRecords.length;

    // Create new Airtable records
    for (const zohoLead of plan.newRecords) {
      try {
        processed++;
        console.log(`[${processed}/${total}] Creating new Airtable record for Zoho lead ${zohoLead.id}`);
        
        const created = await createAirtableRecordFromZohoLead(zohoLead.id, zohoLead.data);
        if (created) {
          this.stats.newAirtableRecords++;
        } else {
          this.stats.errors++;
        }
      } catch (error) {
        this.stats.errors++;
        console.log(`❌ Error creating record: ${error.message}`);
      }
    }

    // Sync Zoho → Airtable
    for (const syncItem of plan.zohoToAirtable) {
      try {
        processed++;
        const ageMinutes = isNaN(syncItem.timeDiff) ? 'unknown' : Math.round(syncItem.timeDiff / 60000);
        console.log(`[${processed}/${total}] Syncing Zoho lead ${syncItem.zoho.id} → Airtable (Zoho ${ageMinutes}min newer)`);
        
        const success = await this.syncZohoToAirtable(syncItem.zoho, syncItem.airtable);
        if (success) {
          this.stats.zohoToAirtable++;
        } else {
          this.stats.errors++;
        }
      } catch (error) {
        this.stats.errors++;
        console.log(`❌ Error syncing Zoho→Airtable: ${error.message}`);
      }
    }

    // Sync Airtable → Zoho
    for (const syncItem of plan.airtableToZoho) {
      try {
        processed++;
        const ageMinutes = isNaN(syncItem.timeDiff) ? 'unknown' : Math.round(syncItem.timeDiff / 60000);
        console.log(`[${processed}/${total}] Syncing Airtable record ${syncItem.airtable.id} → Zoho ${syncItem.zoho.id} (Airtable ${ageMinutes}min newer)`);
        
        const success = await this.syncAirtableToZoho(syncItem.airtable, syncItem.zoho);
        if (success) {
          this.stats.airtableToZoho++;
        } else {
          this.stats.errors++;
          console.log(`❌ Failed to sync Airtable record ${syncItem.airtable.id} → Zoho ${syncItem.zoho.id}`);
        }
      } catch (error) {
        this.stats.errors++;
        console.log(`❌ Error syncing Airtable→Zoho: ${error.message}`);
        if (this.verbose) {
          console.log(`   Stack trace: ${error.stack}`);
        }
      }
    }
  }

  async syncZohoToAirtable(zohoLead, airtableRecord) {
    const fieldMapping = fieldMappingCache.getFieldMapping();
    if (!fieldMapping) {
      return false;
    }

    const syncPromises = [];
    
    // Sync all mapped fields
    for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
      if (zohoLead.data[zohoField] !== undefined && !this.shouldIgnoreField(zohoField)) {
        syncPromises.push(
          syncFieldFromZohoToAirtable(zohoLead.id, zohoField, zohoLead.data[zohoField], mapping)
        );
      }
    }

    const results = await Promise.allSettled(syncPromises);
    const successful = results.filter(r => r.status === 'fulfilled').length;
    
    return successful > 0;
  }

  async syncAirtableToZoho(airtableRecord, zohoLead) {
    const fieldMapping = fieldMappingCache.getFieldMapping();
    if (!fieldMapping) {
      console.log(`❌ No field mapping available for sync`);
      return false;
    }

    // Get field ID to name mapping to resolve field IDs to actual field names
    const fieldIdToName = await getFieldIdToNameMapping();
    
    const syncPromises = [];
    let fieldsToSync = 0;
    let fieldsChecked = 0;
    let fieldsIgnored = 0;
    let fieldsUndefined = 0;
    
    if (this.verbose) {
      console.log(`   🔍 Available Airtable fields: ${Object.keys(airtableRecord.data.fields).slice(0, 10).join(', ')}...`);
      console.log(`   🗺️  Field mappings to check: ${Object.keys(fieldMapping).length}`);
    }
    
    // Sync all mapped fields
    for (const [zohoField, mapping] of Object.entries(fieldMapping)) {
      fieldsChecked++;
      const airtableFieldId = mapping.airtable;
      
      // Resolve field ID to field name
      const airtableFieldName = fieldIdToName[airtableFieldId] || airtableFieldId;
      const fieldValue = airtableRecord.data.fields[airtableFieldName];
      
      if (this.shouldIgnoreField(zohoField)) {
        fieldsIgnored++;
        continue;
      }
      
      if (fieldValue === undefined) {
        fieldsUndefined++;
        if (this.verbose && fieldsChecked <= 5) {
          console.log(`   ⚪ ${zohoField} (${airtableFieldName}): undefined`);
        }
        continue;
      }
      
      if (this.verbose && fieldsToSync < 5) {
        console.log(`   ✅ ${zohoField} (${airtableFieldName}): "${fieldValue}"`);
      }
      
      fieldsToSync++;
      syncPromises.push(
        syncFieldFromAirtableToZoho(airtableRecord.id, zohoField, fieldValue, mapping)
          .catch(error => {
            if (this.verbose) {
              console.log(`   ❌ Failed to sync field ${zohoField}: ${error.message}`);
            }
            throw error;
          })
      );
    }

    if (fieldsToSync === 0) {
      if (this.verbose) {
        console.log(`   ⚠️  No fields to sync for record ${airtableRecord.id}`);
        console.log(`   📊 Summary: ${fieldsChecked} checked, ${fieldsIgnored} ignored, ${fieldsUndefined} undefined, ${fieldsToSync} to sync`);
      }
      return false;
    }

    const results = await Promise.allSettled(syncPromises);
    const successful = results.filter(r => r.status === 'fulfilled').length;
    const failed = results.filter(r => r.status === 'rejected').length;
    
    if (this.verbose) {
      console.log(`   📊 Field sync results: ${successful} successful, ${failed} failed out of ${fieldsToSync} total`);
    }
    
    return successful > 0;
  }

  shouldIgnoreField(fieldName) {
    const ignoredFields = [
      'Modified_Time', 'Created_Time', 'Modified_By', 'Created_By', 'id',
      'smsmagic4__Plain_Phone', 'smsmagic4__Plain_Mobile',
      'Lead_Conversion_Time', 'Data_Processing_Basis_Details',
      'Approval', 'Data_Source', 'Process_Flow'
    ];
    
    return ignoredFields.includes(fieldName);
  }

  showDetailedPlan(plan) {
    if (plan.newRecords.length > 0) {
      console.log('🆕 New Airtable records to create:');
      plan.newRecords.slice(0, 5).forEach(lead => {
        console.log(`   - Zoho lead ${lead.id}`);
      });
      if (plan.newRecords.length > 5) {
        console.log(`   ... and ${plan.newRecords.length - 5} more`);
      }
      console.log('');
    }

    if (plan.zohoToAirtable.length > 0) {
      console.log('🔄 Zoho → Airtable syncs:');
      plan.zohoToAirtable.slice(0, 5).forEach(item => {
        const ageMinutes = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
        console.log(`   - Lead ${item.zoho.id} (Zoho ${ageMinutes}min newer)`);
        if (this.verbose) {
          console.log(`     Zoho: ${item.zohoTime}, Airtable: ${item.airtableTime}`);
        }
      });
      if (plan.zohoToAirtable.length > 5) {
        console.log(`   ... and ${plan.zohoToAirtable.length - 5} more`);
      }
      console.log('');
    }

    if (plan.airtableToZoho.length > 0) {
      console.log('🔄 Airtable → Zoho syncs:');
      plan.airtableToZoho.slice(0, 5).forEach(item => {
        const ageMinutes = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
        console.log(`   - Record ${item.airtable.id} → Lead ${item.zoho.id} (Airtable ${ageMinutes}min newer)`);
        if (this.verbose) {
          console.log(`     Zoho: ${item.zohoTime}, Airtable: ${item.airtableTime}`);
        }
      });
      if (plan.airtableToZoho.length > 5) {
        console.log(`   ... and ${plan.airtableToZoho.length - 5} more`);
      }
      console.log('');
    }
  }

  showResults() {
    console.log('\n🎉 Bulk Sync Completed!');
    console.log('========================');
    console.log(`📊 Total Leads Processed: ${this.stats.totalZohoLeads}`);
    console.log(`📊 Total Airtable Records: ${this.stats.totalAirtableRecords}`);
    console.log('');
    console.log('📈 Sync Results:');
    console.log(`├─ New Airtable Records: ${this.stats.newAirtableRecords}`);
    console.log(`├─ Zoho → Airtable: ${this.stats.zohoToAirtable}`);
    console.log(`├─ Airtable → Zoho: ${this.stats.airtableToZoho}`);
    console.log(`├─ Errors: ${this.stats.errors}`);
    console.log(`└─ Total Operations: ${this.stats.newAirtableRecords + this.stats.zohoToAirtable + this.stats.airtableToZoho}`);
    
    const successRate = this.stats.errors > 0 ? 
      Math.round(((this.stats.newAirtableRecords + this.stats.zohoToAirtable + this.stats.airtableToZoho) / 
        (this.stats.newAirtableRecords + this.stats.zohoToAirtable + this.stats.airtableToZoho + this.stats.errors)) * 100) : 100;
    
    console.log(`\n✨ Success Rate: ${successRate}%`);
  }
}

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {};
  
  if (args.includes('--dry-run')) {
    options.dryRun = true;
  }
  
  if (args.includes('--verbose') || args.includes('-v')) {
    options.verbose = true;
  }
  
  if (args.includes('--help') || args.includes('-h')) {
    console.log(`
🚀 Bulk Sync Usage:

  node bulkSync.js               # Run full bulk sync
  node bulkSync.js --dry-run     # Preview changes without executing
  node bulkSync.js --verbose     # Detailed logging
  node bulkSync.js --dry-run -v  # Preview with detailed logs

Description:
  Fetches ALL leads from both Zoho and Airtable, compares modification times,
  and syncs the newer version to the other system. Creates new Airtable 
  records for Zoho leads that don't exist in Airtable.

Safety:
  - Uses existing field mappings from your Zoho Fields table
  - Respects ignored field configurations  
  - Shows detailed preview in dry-run mode
`);
    process.exit(0);
  }
  
  return options;
}

// Main execution
async function main() {
  const options = parseArgs();
  const bulkSync = new BulkSync(options);
  await bulkSync.run();
}

if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
  });
}

module.exports = BulkSync;