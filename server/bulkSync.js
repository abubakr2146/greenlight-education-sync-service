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
const { getMultipleLeadDetails, refreshZohoToken, deleteZohoLead } = require('./src/services/zohoService');
const { 
  getAllRecordsForSync, 
  getRecordById,
  findAirtableRecordByZohoId,
  getFieldIdToNameMapping,
  updateAirtableField 
} = require('./src/services/airtableService');
const { createAirtableRecordFromZohoLead } = require('./src/services/syncService');
const { 
  syncZohoToAirtable, 
  syncAirtableToZoho 
} = require('./src/services/syncExecutionService');
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
      newZohoLeads: 0,
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
      console.log(`├─ New Airtable records: ${syncPlan.newAirtableRecords.length}`);
      console.log(`├─ New Zoho leads: ${syncPlan.newZohoLeads.length}`);
      console.log(`├─ Conflicts (manual review): ${syncPlan.conflicts.length}`);
      console.log(`└─ No sync needed: ${syncPlan.noSyncNeeded.length}\n`);

      if (this.dryRun) {
        console.log('🔍 DRY RUN - No changes will be made\n');
        this.showDetailedPlan(syncPlan);
        return;
      }

      // Step 3: Execute sync
      console.log('⚡ Executing sync plan...');
      await this.executeBulkSyncPlan(syncPlan, zohoLeads, airtableRecords);

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

      return allRecords.map(record => {
          const lastModified = record.fields['Last Modified Time'];
          const modifiedTime = new Date(lastModified).getTime();
          
          if (this.verbose && isNaN(modifiedTime)) {
            console.log(`⚠️  Invalid timestamp for record ${record.id}: "${lastModified}"`);
          }
          
          return {
            id: record.id,
            zohoId: record.fields['Zoho CRM ID'] || null, // Allow null for new records
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
      newAirtableRecords: [], // New Airtable records from Zoho leads
      newZohoLeads: [],       // New Zoho leads from Airtable records
      conflicts: [],
      noSyncNeeded: []
    };

    // Create maps for quick lookup
    const zohoMap = new Map(zohoLeads.map(lead => [lead.id, lead]));
    const airtableMap = new Map(airtableRecords.filter(r => r.zohoId).map(record => [record.zohoId, record]));

    // Process Zoho leads
    for (const zohoLead of zohoLeads) {
      const airtableRecord = airtableMap.get(zohoLead.id);

      if (!airtableRecord) {
        // No Airtable record exists - create new one
        plan.newAirtableRecords.push(zohoLead);
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

    // Process Airtable records
    for (const airtableRecord of airtableRecords) {
      if (!airtableRecord.zohoId) {
        // Airtable record has no Zoho CRM ID - create new Zoho lead
        plan.newZohoLeads.push(airtableRecord);
      } else if (!zohoMap.has(airtableRecord.zohoId)) {
        // Airtable record references non-existent Zoho lead - will be marked as deleted
        // This is handled in the deletion check at the end
      }
    }

    return plan;
  }

  async executeBulkSyncPlan(plan, zohoLeads, airtableRecords) {
    let processed = 0;
    const total = plan.zohoToAirtable.length + plan.airtableToZoho.length + plan.newAirtableRecords.length + plan.newZohoLeads.length;

    // Create new Airtable records from Zoho leads
    for (const zohoLead of plan.newAirtableRecords) {
      try {
        processed++;
        console.log(`[${processed}/${total}] Creating new Airtable record for Zoho lead ${zohoLead.id}`);
        
        const created = await createAirtableRecordFromZohoLead(zohoLead.id, zohoLead.data);
        if (created) {
          this.stats.newAirtableRecords++;
          if (this.verbose) {
            console.log(`✅ Successfully created Airtable record ${created.id}`);
          }
        } else {
          this.stats.errors++;
          console.log(`❌ Failed to create Airtable record for Zoho lead ${zohoLead.id} - function returned null`);
        }
      } catch (error) {
        this.stats.errors++;
        console.log(`❌ Error creating Airtable record: ${error.message}`);
        if (this.verbose) {
          console.log(`❌ Stack trace: ${error.stack}`);
        }
      }
    }

    // Create new Zoho leads from Airtable records
    for (const airtableRecord of plan.newZohoLeads) {
      try {
        processed++;
        console.log(`[${processed}/${total}] Creating new Zoho lead for Airtable record ${airtableRecord.id}`);
        
        const success = await syncAirtableToZoho(airtableRecord, { 
          compareValues: false, 
          verbose: this.verbose,
          createMissing: true // Enable creation of missing Zoho leads
        });
        if (success) {
          this.stats.newZohoLeads = (this.stats.newZohoLeads || 0) + 1;
        } else {
          this.stats.errors++;
        }
      } catch (error) {
        this.stats.errors++;
        console.log(`❌ Error creating Zoho lead: ${error.message}`);
      }
    }

    // Sync Zoho → Airtable
    for (const syncItem of plan.zohoToAirtable) {
      try {
        processed++;
        const ageMinutes = isNaN(syncItem.timeDiff) ? 'unknown' : Math.round(syncItem.timeDiff / 60000);
        console.log(`[${processed}/${total}] Syncing Zoho lead ${syncItem.zoho.id} → Airtable (Zoho ${ageMinutes}min newer)`);
        
        const success = await syncZohoToAirtable(syncItem.zoho, { 
          compareValues: false, 
          verbose: this.verbose,
          createMissing: false 
        });
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
        
        const success = await syncAirtableToZoho(syncItem.airtable, { 
          compareValues: false, 
          verbose: this.verbose,
          createMissing: false 
        });
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
    
    // Simple deletion check
    console.log('\n🗑️  Checking for deletions...');
    
    const zohoIds = new Set(zohoLeads.map(lead => lead.id));
    const airtableWithZohoId = airtableRecords.filter(r => r.zohoId);
    
    let deletedZoho = 0;
    let markedDeleted = 0;
    
    // Delete Zoho leads not in Airtable (only if they're old enough)
    for (const zohoLead of zohoLeads) {
      const hasAirtable = airtableRecords.some(r => r.zohoId === zohoLead.id);
      if (!hasAirtable) {
        // Check if lead is older than 24 hours
        const createdTime = new Date(zohoLead.data.Created_Time).getTime();
        const ageInHours = (Date.now() - createdTime) / (1000 * 60 * 60);
        
        if (ageInHours > 24) {
          processed++;
          if (!this.dryRun) {
            const deleted = await deleteZohoLead(zohoLead.id);
            if (deleted) deletedZoho++;
          } else {
            deletedZoho++; // Count for dry run
          }
          console.log(`[${processed}/${total + zohoLeads.length + airtableWithZohoId.length}] ${this.dryRun ? '[DRY RUN] Would delete' : 'Deleted'} Zoho lead ${zohoLead.id} (${Math.round(ageInHours)} hours old)`);
        } else {
          console.log(`⏭️  Skipping deletion of new Zoho lead ${zohoLead.id} (only ${Math.round(ageInHours)} hours old)`);
        }
      }
    }
    
    // Mark Airtable records as deleted
    for (const airtableRecord of airtableWithZohoId) {
      if (!zohoIds.has(airtableRecord.zohoId)) {
        processed++;
        if (!this.dryRun) {
          const updated = await updateAirtableField(airtableRecord.id, 'Lead Status', 'Deleted Lead');
          if (updated) markedDeleted++;
        } else {
          markedDeleted++; // Count for dry run
        }
        console.log(`[${processed}/${total + zohoLeads.length + airtableWithZohoId.length}] ${this.dryRun ? '[DRY RUN] Would mark' : 'Marked'} Airtable record ${airtableRecord.id} as deleted`);
      }
    }
    
    console.log(`\n🗑️  Deletion check completed: ${deletedZoho} Zoho deleted, ${markedDeleted} Airtable marked as deleted`);
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
    if (plan.newAirtableRecords.length > 0) {
      console.log('🆕 New Airtable records to create:');
      plan.newAirtableRecords.slice(0, 5).forEach(lead => {
        console.log(`   - Zoho lead ${lead.id}`);
      });
      if (plan.newAirtableRecords.length > 5) {
        console.log(`   ... and ${plan.newAirtableRecords.length - 5} more`);
      }
      console.log('');
    }

    if (plan.newZohoLeads.length > 0) {
      console.log('🆕 New Zoho leads to create:');
      plan.newZohoLeads.slice(0, 5).forEach(record => {
        console.log(`   - Airtable record ${record.id}`);
      });
      if (plan.newZohoLeads.length > 5) {
        console.log(`   ... and ${plan.newZohoLeads.length - 5} more`);
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
    console.log(`├─ New Zoho Leads: ${this.stats.newZohoLeads}`);
    console.log(`├─ Zoho → Airtable: ${this.stats.zohoToAirtable}`);
    console.log(`├─ Airtable → Zoho: ${this.stats.airtableToZoho}`);
    console.log(`├─ Errors: ${this.stats.errors}`);
    console.log(`└─ Total Operations: ${this.stats.newAirtableRecords + this.stats.newZohoLeads + this.stats.zohoToAirtable + this.stats.airtableToZoho}`);
    
    const totalSuccessful = this.stats.newAirtableRecords + this.stats.newZohoLeads + this.stats.zohoToAirtable + this.stats.airtableToZoho;
    const successRate = this.stats.errors > 0 ? 
      Math.round((totalSuccessful / (totalSuccessful + this.stats.errors)) * 100) : 100;
    
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