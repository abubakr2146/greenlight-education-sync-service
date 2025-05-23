#!/usr/bin/env node

/**
 * Test Bulk Sync - Single Lead Synchronization
 * 
 * Test sync for a specific Zoho lead by ID
 * 
 * Usage:
 * - node testBulkSync.js <zoho-lead-id>
 * - node testBulkSync.js <zoho-lead-id> --dry-run
 * - node testBulkSync.js <zoho-lead-id> --verbose
 */

const { loadZohoConfig, loadAirtableConfig } = require('./src/config/config');
const { getLeadDetails, deleteZohoLead } = require('./src/services/zohoService');
const { 
  findAirtableRecordByZohoId,
  getRecordById,
  updateAirtableField 
} = require('./src/services/airtableService');
const { createAirtableRecordFromZohoLead } = require('./src/services/syncService');
const { 
  syncZohoToAirtable, 
  syncAirtableToZoho 
} = require('./src/services/syncExecutionService');
const fieldMappingCache = require('./src/utils/fieldMappingCache');

class TestBulkSync {
  constructor(zohoLeadId, options = {}) {
    this.zohoLeadId = zohoLeadId;
    this.dryRun = options.dryRun || false;
    this.verbose = options.verbose || false;
    this.withDeletion = options.withDeletion || false;
  }

  async run() {
    try {
      console.log('🧪 Test Bulk Sync - Single Lead');
      console.log('================================');
      console.log(`📍 Zoho Lead ID: ${this.zohoLeadId}`);
      console.log(`🎯 Mode: ${this.dryRun ? 'DRY RUN (no changes)' : 'LIVE SYNC'}`);
      console.log(`📊 Verbose: ${this.verbose ? 'ON' : 'OFF'}`);
      console.log(`🗑️  Deletion check: ${this.withDeletion ? 'ON' : 'OFF'}\n`);

      // Initialize field mapping cache
      console.log('🔧 Initializing field mapping cache...');
      await fieldMappingCache.initialize('Leads');
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      const fieldStatus = fieldMappingCache.getStatus();
      console.log(`✅ Field mapping ready: ${fieldStatus.mappingCount} fields mapped\n`);

      // Step 1: Fetch the specific Zoho lead
      console.log(`📥 Fetching Zoho lead ${this.zohoLeadId}...`);
      const zohoResponse = await getLeadDetails(this.zohoLeadId);
      
      if (!zohoResponse || !zohoResponse.data || !zohoResponse.data[0]) {
        console.log(`❌ Zoho lead ${this.zohoLeadId} not found!`);
        
        // Always check if there's an Airtable record for this missing Zoho lead
        console.log(`\n🔍 Checking for orphaned Airtable record...`);
        const airtableRecordId = await findAirtableRecordByZohoId(this.zohoLeadId);
        
        if (airtableRecordId) {
          console.log(`📝 Found Airtable record ${airtableRecordId} for missing Zoho lead`);
          if (!this.dryRun) {
            const updated = await updateAirtableField(airtableRecordId, 'Lead Status', 'Deleted Lead');
            if (updated) {
              console.log(`✅ Marked Airtable record as "Deleted Lead"`);
            } else {
              console.log(`❌ Failed to update Airtable record`);
            }
          } else {
            console.log(`[DRY RUN] Would mark Airtable record ${airtableRecordId} as "Deleted Lead"`);
          }
        } else {
          console.log(`✅ No Airtable record found for this Zoho lead`);
        }
        
        return;
      }
      
      const zohoLead = zohoResponse.data[0];
      console.log(`✅ Found Zoho lead: ${zohoLead.Full_Name || zohoLead.Email || 'No name'}`);
      
      if (this.verbose) {
        console.log(`   Created: ${zohoLead.Created_Time}`);
        console.log(`   Modified: ${zohoLead.Modified_Time}`);
        console.log(`   Email: ${zohoLead.Email || 'N/A'}`);
        console.log(`   Phone: ${zohoLead.Phone || 'N/A'}`);
      }

      // Step 2: Check for corresponding Airtable record
      console.log(`\n🔍 Checking for Airtable record...`);
      const airtableRecordId = await findAirtableRecordByZohoId(this.zohoLeadId);
      
      let airtableRecord = null;
      if (airtableRecordId) {
        airtableRecord = await getRecordById(airtableRecordId);
        console.log(`✅ Found Airtable record: ${airtableRecordId}`);
        
        if (this.verbose && airtableRecord) {
          console.log(`   Last Modified: ${airtableRecord.fields['Last Modified Time'] || 'N/A'}`);
        }
      } else {
        console.log(`❌ No Airtable record found for Zoho lead ${this.zohoLeadId}`);
      }

      // Step 3: Determine sync action
      console.log(`\n📋 Sync Plan:`);
      
      if (!airtableRecord) {
        // Create new Airtable record
        console.log(`   🆕 Create new Airtable record from Zoho lead`);
        
        if (!this.dryRun) {
          console.log(`\n⚡ Creating Airtable record...`);
          const created = await createAirtableRecordFromZohoLead(this.zohoLeadId, zohoLead);
          if (created) {
            console.log(`✅ Successfully created Airtable record ${created.id}`);
          } else {
            console.log(`❌ Failed to create Airtable record`);
          }
        } else {
          console.log(`\n[DRY RUN] Would create new Airtable record`);
        }
      } else {
        // Compare modification times
        let zohoTime, airtableTime;
        
        try {
          zohoTime = new Date(zohoLead.Modified_Time).getTime();
          if (isNaN(zohoTime)) {
            console.log(`   ⚠️  Invalid Zoho timestamp: ${zohoLead.Modified_Time}`);
            zohoTime = Date.now();
          }
        } catch (e) {
          console.log(`   ⚠️  Error parsing Zoho timestamp: ${e.message}`);
          zohoTime = Date.now();
        }
        
        try {
          airtableTime = new Date(airtableRecord.fields['Last Modified Time']).getTime();
          if (isNaN(airtableTime)) {
            console.log(`   ⚠️  Invalid Airtable timestamp: ${airtableRecord.fields['Last Modified Time']}`);
            airtableTime = Date.now();
          }
        } catch (e) {
          console.log(`   ⚠️  Error parsing Airtable timestamp: ${e.message}`);
          airtableTime = Date.now();
        }
        
        const timeDiff = Math.abs(zohoTime - airtableTime);
        
        console.log(`   📅 Zoho modified: ${new Date(zohoTime).toISOString()}`);
        console.log(`   📅 Airtable modified: ${new Date(airtableTime).toISOString()}`);
        console.log(`   ⏱️  Time difference: ${Math.round(timeDiff / 1000)} seconds`);
        
        if (timeDiff < 30000) { // Less than 30 seconds
          console.log(`   ✅ Records are in sync (times too close)`);
        } else if (zohoTime > airtableTime) {
          console.log(`   🔄 Sync Zoho → Airtable (Zoho is newer)`);
          
          if (!this.dryRun) {
            console.log(`\n⚡ Syncing Zoho to Airtable...`);
            const success = await syncZohoToAirtable(
              { id: this.zohoLeadId, data: zohoLead }, 
              { compareValues: true, verbose: this.verbose }
            );
            if (success) {
              console.log(`✅ Successfully synced Zoho → Airtable`);
            } else {
              console.log(`❌ Failed to sync Zoho → Airtable`);
            }
          } else {
            console.log(`\n[DRY RUN] Would sync Zoho → Airtable`);
          }
        } else {
          console.log(`   🔄 Sync Airtable → Zoho (Airtable is newer)`);
          
          if (!this.dryRun) {
            console.log(`\n⚡ Syncing Airtable to Zoho...`);
            const success = await syncAirtableToZoho(
              { 
                id: airtableRecordId, 
                zohoId: this.zohoLeadId, 
                data: airtableRecord 
              }, 
              { compareValues: true, verbose: this.verbose }
            );
            if (success) {
              console.log(`✅ Successfully synced Airtable → Zoho`);
            } else {
              console.log(`❌ Failed to sync Airtable → Zoho`);
            }
          } else {
            console.log(`\n[DRY RUN] Would sync Airtable → Zoho`);
          }
        }
      }

      // Step 4: Deletion check (if enabled)
      if (this.withDeletion && !airtableRecord) {
        console.log(`\n🗑️  Deletion Check:`);
        console.log(`   Zoho lead exists but has no Airtable record`);
        
        if (!this.dryRun) {
          const deleted = await deleteZohoLead(this.zohoLeadId);
          if (deleted) {
            console.log(`   ✅ Deleted Zoho lead ${this.zohoLeadId}`);
          } else {
            console.log(`   ❌ Failed to delete Zoho lead`);
          }
        } else {
          console.log(`   [DRY RUN] Would delete Zoho lead ${this.zohoLeadId}`);
        }
      }

      console.log(`\n✨ Test sync completed!`);

    } catch (error) {
      console.error('❌ Test sync failed:', error.message);
      if (this.verbose) {
        console.error(error.stack);
      }
    } finally {
      fieldMappingCache.destroy();
    }
  }
}

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  
  if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
    console.log(`
🧪 Test Bulk Sync Usage:

  node testBulkSync.js <zoho-lead-id>                    # Sync specific lead
  node testBulkSync.js <zoho-lead-id> --dry-run          # Preview changes
  node testBulkSync.js <zoho-lead-id> --verbose          # Detailed logging
  node testBulkSync.js <zoho-lead-id> --with-deletion    # Include deletion check

Examples:
  node testBulkSync.js 123456789                         # Sync lead 123456789
  node testBulkSync.js 123456789 --dry-run --verbose     # Preview with details
  node testBulkSync.js 123456789 --with-deletion         # Sync and check deletions

Description:
  Tests the sync process for a single Zoho lead. Useful for debugging
  sync issues or testing changes before running full bulk sync.
`);
    process.exit(0);
  }
  
  const zohoLeadId = args[0];
  const options = {
    dryRun: args.includes('--dry-run'),
    verbose: args.includes('--verbose') || args.includes('-v'),
    withDeletion: args.includes('--with-deletion')
  };
  
  return { zohoLeadId, options };
}

// Main execution
async function main() {
  const { zohoLeadId, options } = parseArgs();
  const testSync = new TestBulkSync(zohoLeadId, options);
  await testSync.run();
}

if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
  });
}

module.exports = TestBulkSync;