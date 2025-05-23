#!/usr/bin/env node

/**
 * Bulk Sync - Complete Record Synchronization for a specific module
 *
 * Fetches ALL records from both systems for the specified module and syncs based on latest modified time
 *
 * Usage:
 * - Full bulk sync for Leads (default): node bulkSync.js
 * - Full bulk sync for Contacts: node bulkSync.js --module Contacts
 * - Dry run: node bulkSync.js --module Leads --dry-run
 * - Verbose: node bulkSync.js --verbose
 */

const axios = require('axios');
const { loadZohoConfig, loadAirtableConfig } = require('./src/config/config');
const {
  getMultipleRecordDetails, // Changed from getMultipleLeadDetails
  refreshZohoToken,
  deleteZohoRecord, // Changed from deleteZohoLead
  getZohoModulePluralName // Added
} = require('./src/services/zohoService');
const {
  getAllModuleRecordsForSync, // Changed from getAllRecordsForSync
  getModuleRecordById, // Changed from getRecordById
  findModuleRecordByZohoId, // Changed from findAirtableRecordByZohoId
  // getFieldIdToNameMapping, // This is complex, Airtable field names are used directly from mapping
  updateModuleField // Changed from updateAirtableField
} = require('./src/services/airtableService');
const {
  createAirtableRecordFromZoho, // Changed from createAirtableRecordFromZohoLead
  createZohoRecordFromAirtable, // Added for new Zoho records
  syncFieldFromZohoToAirtable, // To replace syncExecutionService
  syncFieldFromAirtableToZoho, // To replace syncExecutionService
  getFieldMappingFor, // Added
  getZohoCrmIdMapping, // Added
  getAirtableIdMapping // Added
} = require('./src/services/syncService');
const fieldMappingCache = require('./src/utils/fieldMappingCache');
// Assuming syncExecutionService is not used anymore, relying on syncService for module-aware sync logic

class BulkSync {
  constructor(options = {}) {
    this.moduleName = options.moduleName || 'Leads';
    this.dryRun = options.dryRun || false;
    this.verbose = options.verbose || false;
    this.stats = {
      totalZohoRecords: 0,
      totalAirtableRecords: 0,
      zohoToAirtable: 0,
      airtableToZoho: 0,
      newAirtableRecords: 0,
      newZohoRecords: 0,
      conflicts: 0,
      errors: 0,
      skipped: 0,
      deletedZohoRecords: 0,
      markedAirtableAsDeleted: 0
    };
    this.zohoCrmIdAirtableField = null; // To be fetched from mapping
    this.airtableLastModifiedField = 'Last Modified Time'; // Common convention, but could be mapped
  }

  async run() {
    try {
      console.log(`🚀 Starting Bulk Synchronization for ${this.moduleName}`);
      console.log('=====================================');
      console.log(`🎯 Mode: ${this.dryRun ? 'DRY RUN (no changes)' : 'LIVE SYNC'}`);
      console.log(`📊 Verbose: ${this.verbose ? 'ON' : 'OFF'}\n`);

      console.log(`🔧 Initializing field mapping cache for ${this.moduleName}...`);
      await fieldMappingCache.initialize(this.moduleName);
      // Wait for cache to potentially populate, adjust as needed
      await new Promise(resolve => setTimeout(resolve, process.env.NODE_ENV === 'test' ? 50 : 3000));

      const fieldStatus = fieldMappingCache.getStatus(this.moduleName);
      if (!fieldStatus || !fieldStatus.initialized || fieldStatus.mappingCount === 0) {
        console.error(`❌ Field mapping cache for ${this.moduleName} failed to initialize or is empty. Aborting.`);
        return;
      }
      console.log(`✅ Field mapping ready for ${this.moduleName}: ${fieldStatus.mappingCount} fields mapped\n`);

      const zohoCrmIdMap = await getZohoCrmIdMapping(this.moduleName);
      if (!zohoCrmIdMap || !zohoCrmIdMap.airtable) {
        console.error(`❌ Critical error: Could not determine Airtable field for Zoho CRM ID for module ${this.moduleName}. Aborting.`);
        return;
      }
      this.zohoCrmIdAirtableField = zohoCrmIdMap.airtable;
      console.log(`ℹ️  Using Airtable field '${this.zohoCrmIdAirtableField}' for Zoho CRM ID.`);


      console.log(`📥 Fetching all ${this.moduleName} records from both systems...`);
      const [zohoRecords, airtableRecords] = await Promise.all([
        this.getAllZohoRecords(),
        this.getAllAirtableRecords()
      ]);

      this.stats.totalZohoRecords = zohoRecords.length;
      this.stats.totalAirtableRecords = airtableRecords.length;

      console.log(`📊 Found ${zohoRecords.length} Zoho ${this.moduleName} records, ${airtableRecords.length} Airtable records for ${this.moduleName}\n`);

      console.log('📋 Creating sync plan...');
      const syncPlan = this.createBulkSyncPlan(zohoRecords, airtableRecords);

      console.log('📈 Sync Plan Summary:');
      console.log(`├─ Zoho → Airtable: ${syncPlan.zohoToAirtable.length}`);
      console.log(`├─ Airtable → Zoho: ${syncPlan.airtableToZoho.length}`);
      console.log(`├─ New Airtable records: ${syncPlan.newAirtableRecords.length}`);
      console.log(`├─ New Zoho records: ${syncPlan.newZohoRecords.length}`);
      console.log(`├─ Conflicts (manual review): ${syncPlan.conflicts.length}`); // Conflict logic not implemented yet
      console.log(`└─ No sync needed: ${syncPlan.noSyncNeeded.length}\n`);

      if (this.dryRun) {
        console.log('🔍 DRY RUN - No changes will be made\n');
        this.showDetailedPlan(syncPlan);
        // Also show planned deletions in dry run
        await this.performDeletionChecks(zohoRecords, airtableRecords, true);
        this.showResults(); // Show stats for dry run
        return;
      }

      console.log('⚡ Executing sync plan...');
      await this.executeBulkSyncPlan(syncPlan);

      console.log('\n🗑️  Performing deletion checks...');
      await this.performDeletionChecks(zohoRecords, airtableRecords, false);


      this.showResults();

    } catch (error) {
      console.error(`❌ Bulk sync for ${this.moduleName} failed:`, error.message);
      if (this.verbose) {
        console.error(error.stack);
      }
      this.stats.errors++; // Increment general error count
    } finally {
      fieldMappingCache.destroyModule(this.moduleName); // Destroy only the current module's cache
      console.log(`\n✅ Field mapping cache for ${this.moduleName} destroyed.`);
    }
  }

  async getAllZohoRecords() {
    const config = loadZohoConfig();
    if (!config) throw new Error('Zoho config not found');

    try {
      if (Date.now() >= config.tokenExpiry) await refreshZohoToken(config);

      let allRecordSummaries = [];
      let page = 1;
      const perPage = 200;
      const zohoModuleApiName = getZohoModulePluralName(this.moduleName);

      console.log(`📥 Fetching Zoho ${this.moduleName} record IDs & timestamps...`);
      while (true) {
        const response = await axios.get(
          `${config.apiDomain}/crm/v2/${zohoModuleApiName}`,
          {
            headers: { 'Authorization': `Zoho-oauthtoken ${config.accessToken}`, 'Content-Type': 'application/json' },
            params: { page, per_page: perPage, fields: 'id,Modified_Time,Created_Time', sort_by: 'Modified_Time', sort_order: 'desc' }
          }
        );
        const pageRecords = response.data.data || [];
        allRecordSummaries.push(...pageRecords);
        this.verbose ? console.log(`   Page ${page}: ${pageRecords.length} ${this.moduleName} IDs (total: ${allRecordSummaries.length})`) : process.stdout.write('.');
        if (pageRecords.length < perPage) break;
        page++;
      }
      if (!this.verbose) console.log('');

      console.log(`📥 Fetching full ${this.moduleName} details...`);
      const allFullRecords = [];
      const batchSize = 100;
      for (let i = 0; i < allRecordSummaries.length; i += batchSize) {
        const batchIds = allRecordSummaries.slice(i, i + batchSize).map(rec => rec.id);
        if (this.verbose) console.log(`   Fetching details for ${this.moduleName} records ${i + 1}-${Math.min(i + batchSize, allRecordSummaries.length)}`);
        const detailsResponse = await getMultipleRecordDetails(batchIds, this.moduleName, config);
        if (detailsResponse && detailsResponse.data) allFullRecords.push(...detailsResponse.data);
        if (!this.verbose) process.stdout.write('.');
      }
      if (!this.verbose) console.log('');

      return allFullRecords.map(record => {
        let modifiedTime = new Date(record.Modified_Time).getTime();
        let timestampSource = 'Modified_Time';
        if (isNaN(modifiedTime) && record.Last_Activity_Time) {
          modifiedTime = new Date(record.Last_Activity_Time).getTime();
          timestampSource = 'Last_Activity_Time';
        }
        if (isNaN(modifiedTime) && record.Created_Time) {
          modifiedTime = new Date(record.Created_Time).getTime();
          timestampSource = 'Created_Time';
        }
        if (isNaN(modifiedTime)) {
          modifiedTime = Date.now(); // Fallback, should be rare
          timestampSource = 'current_time_fallback';
        }
        if (this.verbose && isNaN(new Date(record.Modified_Time).getTime())) {
          console.log(`📅 Record ${record.id} (${this.moduleName}): Using ${timestampSource} = ${new Date(modifiedTime).toISOString()}. Original Modified_Time: ${record.Modified_Time}`);
        }
        return { id: record.id, modifiedTime, data: record, source: 'zoho', timestampSource };
      });
    } catch (error) {
      throw new Error(`Failed to fetch Zoho ${this.moduleName} records: ${error.message}`);
    }
  }

  async getAllAirtableRecords() {
    const config = loadAirtableConfig(); // This might need to be module-specific if base/API token changes
    if (!config) throw new Error('Airtable config not found');

    try {
      // getAllModuleRecordsForSync expects module name and config.
      // The current getAllRecordsForSync in airtableService.js is not module-aware for table name.
      // We'll use the more generic axios call here and make it module-aware.
      // This requires getting the Airtable table name for the module.
      // This info should come from moduleConfigService or a similar place.
      // For now, assuming loadAirtableConfig() provides a `tableName` that is correct for the *default* module,
      // and we'll need to enhance this if `tableName` must change per module.
      // Let's assume `airtableService.getModuleConfig` can provide this.
      const airtableModuleConfig = await require('./src/services/airtableService').getModuleConfig(config, this.moduleName);
      const tableNameToUse = airtableModuleConfig.tableName;


      let allRecords = [];
      let offset = null;
      console.log(`📥 Fetching complete Airtable records for ${this.moduleName} from table '${tableNameToUse}'...`);

      do {
        const params = { pageSize: 100, sort: [{ field: this.airtableLastModifiedField, direction: 'desc' }] };
        if (offset) params.offset = offset;

        const response = await axios.get(
          `${config.apiUrl}/${config.baseId}/${encodeURIComponent(tableNameToUse)}`,
          {
            headers: { 'Authorization': `Bearer ${config.apiToken}`, 'Content-Type': 'application/json' },
            params: params
          }
        );
        const pageRecords = response.data.records || [];
        allRecords.push(...pageRecords);
        this.verbose ? console.log(`   Page: ${pageRecords.length} records (total: ${allRecords.length})`) : process.stdout.write('.');
        offset = response.data.offset;
        if (!offset || pageRecords.length === 0) break;
      } while (offset);
      if (!this.verbose) console.log('');

      return allRecords.map(record => {
        const lastModified = record.fields[this.airtableLastModifiedField];
        const modifiedTime = new Date(lastModified).getTime();
        if (this.verbose && isNaN(modifiedTime)) {
          console.log(`⚠️  Invalid Airtable timestamp for record ${record.id} ('${this.airtableLastModifiedField}'): "${lastModified}"`);
        }
        return {
          id: record.id,
          zohoId: record.fields[this.zohoCrmIdAirtableField] || null,
          modifiedTime: isNaN(modifiedTime) ? Date.now() : modifiedTime, // Fallback for invalid date
          data: record, // Full record data
          source: 'airtable'
        };
      });
    } catch (error) {
      throw new Error(`Failed to fetch Airtable records for ${this.moduleName}: ${error.message}`);
    }
  }

  createBulkSyncPlan(zohoRecords, airtableRecords) {
    const plan = {
      zohoToAirtable: [], airtableToZoho: [],
      newAirtableRecords: [], newZohoRecords: [],
      conflicts: [], noSyncNeeded: []
    };
    const zohoMap = new Map(zohoRecords.map(rec => [rec.id, rec]));
    const airtableMapByZohoId = new Map(airtableRecords.filter(r => r.zohoId).map(record => [record.zohoId, record]));

    for (const zohoRecord of zohoRecords) {
      const airtableRecord = airtableMapByZohoId.get(zohoRecord.id);
      if (!airtableRecord) {
        plan.newAirtableRecords.push(zohoRecord);
      } else {
        const zohoTime = zohoRecord.modifiedTime;
        const airtableTime = airtableRecord.modifiedTime;
        const timeDiff = Math.abs(zohoTime - airtableTime);

        if (this.verbose && (isNaN(zohoTime) || isNaN(airtableTime))) {
          console.log(`⚠️ Invalid timestamp comparison - Zoho ${this.moduleName} ID ${zohoRecord.id}: ${zohoTime} (Source: ${zohoRecord.timestampSource}), Airtable ID ${airtableRecord.id}: ${airtableTime} (Field: ${this.airtableLastModifiedField}=${airtableRecord.data.fields[this.airtableLastModifiedField]})`);
        }

        if (timeDiff < 30000) { // Less than 30 seconds tolerance
          plan.noSyncNeeded.push({ zoho: zohoRecord, airtable: airtableRecord, reason: 'Timestamps too close' });
        } else if (zohoTime > airtableTime) {
          plan.zohoToAirtable.push({ zoho: zohoRecord, airtable: airtableRecord, timeDiff, zohoTime: new Date(zohoTime).toISOString(), airtableTime: new Date(airtableTime).toISOString() });
        } else {
          plan.airtableToZoho.push({ airtable: airtableRecord, zoho: zohoRecord, timeDiff, zohoTime: new Date(zohoTime).toISOString(), airtableTime: new Date(airtableTime).toISOString() });
        }
      }
    }

    for (const airtableRecord of airtableRecords) {
      if (!airtableRecord.zohoId) {
        plan.newZohoRecords.push(airtableRecord);
      }
      // Cases where airtableRecord.zohoId exists but not in zohoMap are handled by deletion check
    }
    return plan;
  }

  async executeSyncOperation(item, direction) {
    const sourceRecord = direction === 'ZOHO_TO_AIRTABLE' ? item.zoho : item.airtable;
    const targetRecord = direction === 'ZOHO_TO_AIRTABLE' ? item.airtable : item.zoho;
    const sourceName = direction === 'ZOHO_TO_AIRTABLE' ? `Zoho ${this.moduleName} ${sourceRecord.id}` : `Airtable ${sourceRecord.id}`;
    const targetName = direction === 'ZOHO_TO_AIRTABLE' ? `Airtable ${targetRecord.id}` : `Zoho ${this.moduleName} ${targetRecord.id}`;

    try {
      await fieldMappingCache.ensureModuleInitialized(this.moduleName);
      const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(this.moduleName);
      if (!currentModuleFieldMapping) {
        console.error(`[BulkSync][${this.moduleName}] No field mapping. Cannot sync ${sourceName} to ${targetName}.`);
        this.stats.errors++;
        return false;
      }

      let successCount = 0;
      let fieldErrorCount = 0;

      for (const [zohoFieldApiName, mapping] of Object.entries(currentModuleFieldMapping)) {
        if (this.shouldIgnoreField(zohoFieldApiName, mapping.airtable)) continue;

        let valueToSync;
        if (direction === 'ZOHO_TO_AIRTABLE') {
          valueToSync = sourceRecord.data[zohoFieldApiName];
        } else { // AIRTABLE_TO_ZOHO
          // mapping.airtable should be the Airtable field name or ID
          valueToSync = sourceRecord.data.fields[mapping.airtable];
        }

        // Skip if value is undefined (null is a valid value to sync for clearing fields)
        if (valueToSync === undefined) continue;

        // Use syncField from syncService for individual field sync
        const syncParams = {
          direction: direction === 'ZOHO_TO_AIRTABLE' ? require('./src/services/syncService').SYNC_DIRECTIONS.ZOHO_TO_AIRTABLE : require('./src/services/syncService').SYNC_DIRECTIONS.AIRTABLE_TO_ZOHO,
          sourceId: sourceRecord.id, // Zoho ID for Z->A, Airtable ID for A->Z
          fieldName: zohoFieldApiName, // Always Zoho field name as the reference key for mapping
          value: valueToSync,
          mapping: mapping,
          module: this.moduleName
        };
        
        // For AIRTABLE_TO_ZOHO, sourceId is Airtable Record ID.
        // syncField's internal syncFromAirtableToZoho needs the Zoho Record ID.
        // It gets this using findZohoIdByModuleRecord(airtableRecordId, module).
        // For ZOHO_TO_AIRTABLE, sourceId is Zoho Record ID.
        // syncField's internal syncFromZohoToAirtable needs Airtable Record ID.
        // It gets this using findOrCreateAirtableRecord(zohoRecordId, module).

        if (this.verbose) {
            console.log(`   Syncing field ${mapping.zoho} (Airtable: ${mapping.airtable}) with value:`, valueToSync);
        }

        const fieldSynced = await require('./src/services/syncService').syncField(syncParams);
        if (fieldSynced) {
          successCount++;
        } else {
          fieldErrorCount++;
          if (this.verbose) console.log(`   ❌ Failed to sync field ${mapping.zoho} for ${sourceName}`);
        }
      }
      if (fieldErrorCount > 0) {
         console.log(`Partial failure syncing ${sourceName} to ${targetName}: ${successCount} fields OK, ${fieldErrorCount} fields FAILED.`);
         this.stats.errors++; // Count as one record error if any field fails
         return false;
      }
      return true;

    } catch (error) {
      console.error(`❌ Error during full sync of ${sourceName} to ${targetName}: ${error.message}`);
      if (this.verbose) console.error(error.stack);
      this.stats.errors++;
      return false;
    }
  }


  async executeBulkSyncPlan(plan) {
    let processed = 0;
    const totalOperations = plan.zohoToAirtable.length + plan.airtableToZoho.length + plan.newAirtableRecords.length + plan.newZohoRecords.length;

    for (const zohoRecord of plan.newAirtableRecords) {
      processed++;
      console.log(`[${processed}/${totalOperations}] Creating new Airtable record for Zoho ${this.moduleName} ${zohoRecord.id}`);
      try {
        const created = await createAirtableRecordFromZoho(zohoRecord.id, zohoRecord.data, this.moduleName);
        created ? this.stats.newAirtableRecords++ : this.stats.errors++;
        if (this.verbose && created) console.log(`✅ Successfully created Airtable record ${created.id}`);
        else if (this.verbose && !created) console.log(`❌ Failed to create Airtable record for Zoho ${this.moduleName} ${zohoRecord.id}`);
      } catch (e) { this.stats.errors++; console.error(`Error creating Airtable record: ${e.message}`);}
    }

    for (const airtableRecord of plan.newZohoRecords) {
      processed++;
      console.log(`[${processed}/${totalOperations}] Creating new Zoho ${this.moduleName} for Airtable record ${airtableRecord.id}`);
      try {
        const created = await createZohoRecordFromAirtable(airtableRecord.id, airtableRecord.data, this.moduleName);
        created ? this.stats.newZohoRecords++ : this.stats.errors++;
         if (this.verbose && created) console.log(`✅ Successfully created Zoho record ${created.id}`);
         else if (this.verbose && !created) console.log(`❌ Failed to create Zoho record for Airtable ${airtableRecord.id}`);
      } catch (e) { this.stats.errors++; console.error(`Error creating Zoho record: ${e.message}`);}
    }

    for (const item of plan.zohoToAirtable) {
      processed++;
      const ageMinutes = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
      console.log(`[${processed}/${totalOperations}] Syncing Zoho ${this.moduleName} ${item.zoho.id} → Airtable (Zoho ${ageMinutes}min newer)`);
      const success = await this.executeSyncOperation(item, 'ZOHO_TO_AIRTABLE');
      success ? this.stats.zohoToAirtable++ : this.stats.errors++;
    }

    for (const item of plan.airtableToZoho) {
      processed++;
      const ageMinutes = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
      console.log(`[${processed}/${totalOperations}] Syncing Airtable record ${item.airtable.id} → Zoho ${this.moduleName} ${item.zoho.id} (Airtable ${ageMinutes}min newer)`);
      const success = await this.executeSyncOperation(item, 'AIRTABLE_TO_ZOHO');
      success ? this.stats.airtableToZoho++ : this.stats.errors++;
    }
  }

  async performDeletionChecks(zohoRecords, airtableRecords, dryRun) {
    const zohoRecordIds = new Set(zohoRecords.map(r => r.id));
    const airtableRecordsWithZohoId = airtableRecords.filter(r => r.zohoId);

    // Check for Zoho records that don't have a corresponding Airtable record (potential deletion in Airtable)
    // This is complex: if a Zoho record is deleted, its Airtable counterpart might need to be marked/deleted.
    // The current plan focuses on Zoho records not in Airtable.

    // Delete Zoho records not represented in Airtable (if they are old)
    for (const zohoRecord of zohoRecords) {
      const hasAirtableCounterpart = airtableRecords.some(ar => ar.zohoId === zohoRecord.id);
      if (!hasAirtableCounterpart) {
        const createdTime = new Date(zohoRecord.data.Created_Time).getTime();
        const ageInHours = (Date.now() - createdTime) / (3600000); // 1000 * 60 * 60
        const ageThresholdHours = 24;

        if (ageInHours > ageThresholdHours) {
          console.log(`${dryRun ? '[DRY RUN] Would delete' : 'Deleting'} Zoho ${this.moduleName} ${zohoRecord.id} (no Airtable link, ${Math.round(ageInHours)} hrs old)`);
          if (!dryRun) {
            try {
              const deleted = await deleteZohoRecord(zohoRecord.id, this.moduleName);
              if (deleted) this.stats.deletedZohoRecords++; else this.stats.errors++;
            } catch (e) { this.stats.errors++; console.error(`Error deleting Zoho ${this.moduleName} ${zohoRecord.id}: ${e.message}`);}
          } else {
            this.stats.deletedZohoRecords++; // Count for dry run
          }
        } else {
          console.log(`⏭️  Skipping deletion of new Zoho ${this.moduleName} ${zohoRecord.id} (only ${Math.round(ageInHours)} hrs old)`);
        }
      }
    }

    // Mark Airtable records as deleted if their Zoho counterpart is gone
    // This requires a "status" field in Airtable for the module.
    const airtableDeletedStatusField = 'Status'; // Example, make this module-configurable
    const airtableDeletedStatusValue = `Deleted ${this.moduleName}`; // Example

    for (const airtableRecord of airtableRecordsWithZohoId) {
      if (!zohoRecordIds.has(airtableRecord.zohoId)) {
        console.log(`${dryRun ? '[DRY RUN] Would mark' : 'Marking'} Airtable record ${airtableRecord.id} (for ${this.moduleName}) as deleted (Zoho ID ${airtableRecord.zohoId} not found)`);
        if (!dryRun) {
          try {
            // Ensure updateModuleField is used correctly
            const updated = await updateModuleField(airtableRecord.id, airtableDeletedStatusField, airtableDeletedStatusValue, this.moduleName);
            if (updated) this.stats.markedAirtableAsDeleted++; else this.stats.errors++;
          } catch (e) { this.stats.errors++; console.error(`Error marking Airtable record ${airtableRecord.id} as deleted: ${e.message}`);}
        } else {
          this.stats.markedAirtableAsDeleted++; // Count for dry run
        }
      }
    }
    console.log(`\n🗑️  Deletion check completed: ${this.stats.deletedZohoRecords} Zoho ${this.moduleName}(s) ${dryRun ? 'would be' : ''} deleted, ${this.stats.markedAirtableAsDeleted} Airtable record(s) ${dryRun ? 'would be' : ''} marked as deleted.`);
  }


  shouldIgnoreField(zohoFieldName, airtableFieldName) {
    // Basic ignored fields by Zoho API name
    const ignoredZohoFields = [
      'Modified_Time', 'Created_Time', 'Modified_By', 'Created_By', 'id',
      'CurrencySymbol', '$currency_symbol', // Common currency fields
      'Exchange_Rate', '$exchange_rate',
      'Last_Activity_Time', 'Layout', '$layout',
      // Add other common system fields for the specific module if necessary
      // Example for Leads (some might be general):
      'Lead_Conversion_Time', 'Data_Processing_Basis_Details',
      'Approval', 'Process_Flow', '$process_flow', '$approved', '$approval',
      '$editable', '$review_process', '$review', '$zia_assign_time', '$in_merge',
      '$locked_for_me', '$lock_source_s', '$orchestration', '$converted', '$converted_detail',
      '$zia_owner_assignment', '$zia_visit_count', '$zia_visit_source', '$zia_contact_source',
      '$zia_event_source', '$zia_event_name', '$zia_event_description', '$zia_event_timestamp',
      'Tag', // Tags are often complex objects or arrays of objects
      'Owner' // Owner is a lookup, often handled specially or by ID
    ];
     if (ignoredZohoFields.includes(zohoFieldName) || (zohoFieldName && zohoFieldName.startsWith('$'))) {
        if(this.verbose) console.log(`   Ignoring Zoho system field: ${zohoFieldName}`);
        return true;
    }

    // Ignoring Airtable system fields (less common to come from mapping this way, but good check)
    const ignoredAirtableFields = ['Zoho CRM ID', 'Last Modified Time', 'Created Time', 'Airtable Record ID']; // Example
    if (airtableFieldName && ignoredAirtableFields.includes(airtableFieldName)) {
        if(this.verbose) console.log(`   Ignoring Airtable system field: ${airtableFieldName}`);
        return true;
    }
    return false;
  }

  showDetailedPlan(plan) {
    const logItem = (item, type) => {
      if (type === 'newAirtable') console.log(`   - Zoho ${this.moduleName} ${item.id}`);
      else if (type === 'newZoho') console.log(`   - Airtable record ${item.id}`);
      else if (type === 'zohoToAirtable') {
        const age = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
        console.log(`   - Zoho ${this.moduleName} ${item.zoho.id} (Zoho ${age}min newer)`);
        if (this.verbose) console.log(`     Zoho: ${item.zohoTime}, Airtable: ${item.airtableTime}`);
      } else if (type === 'airtableToZoho') {
        const age = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
        console.log(`   - Airtable record ${item.airtable.id} → Zoho ${this.moduleName} ${item.zoho.id} (Airtable ${age}min newer)`);
        if (this.verbose) console.log(`     Zoho: ${item.zohoTime}, Airtable: ${item.airtableTime}`);
      }
    };
    const logPlanSection = (title, items, type) => {
      if (items.length > 0) {
        console.log(title);
        items.slice(0, 5).forEach(item => logItem(item, type));
        if (items.length > 5) console.log(`   ... and ${items.length - 5} more`);
        console.log('');
      }
    };
    logPlanSection(`🆕 New Airtable records for ${this.moduleName} to create:`, plan.newAirtableRecords, 'newAirtable');
    logPlanSection(`🆕 New Zoho ${this.moduleName} records to create:`, plan.newZohoRecords, 'newZoho');
    logPlanSection(`🔄 Zoho ${this.moduleName} → Airtable syncs:`, plan.zohoToAirtable, 'zohoToAirtable');
    logPlanSection(`🔄 Airtable → Zoho ${this.moduleName} syncs:`, plan.airtableToZoho, 'airtableToZoho');
  }

  showResults() {
    console.log(`\n🎉 Bulk Sync for ${this.moduleName} Completed!`);
    console.log('========================');
    console.log(`📊 Total Zoho ${this.moduleName} Records: ${this.stats.totalZohoRecords}`);
    console.log(`📊 Total Airtable Records for ${this.moduleName}: ${this.stats.totalAirtableRecords}`);
    console.log('');
    console.log('📈 Sync Results:');
    console.log(`├─ New Airtable Records: ${this.stats.newAirtableRecords}`);
    console.log(`├─ New Zoho ${this.moduleName} Records: ${this.stats.newZohoRecords}`);
    console.log(`├─ Zoho → Airtable Syncs: ${this.stats.zohoToAirtable}`);
    console.log(`├─ Airtable → Zoho Syncs: ${this.stats.airtableToZoho}`);
    console.log(`├─ Zoho ${this.moduleName} Records Deleted: ${this.stats.deletedZohoRecords}`);
    console.log(`├─ Airtable Records Marked Deleted: ${this.stats.markedAirtableAsDeleted}`);
    console.log(`├─ Errors: ${this.stats.errors}`);
    const totalOps = this.stats.newAirtableRecords + this.stats.newZohoRecords + this.stats.zohoToAirtable + this.stats.airtableToZoho + this.stats.deletedZohoRecords + this.stats.markedAirtableAsDeleted;
    console.log(`└─ Total Operations: ${totalOps}`);
    const successRate = (totalOps + this.stats.errors) > 0 ? Math.round((totalOps / (totalOps + this.stats.errors)) * 100) : 100;
    console.log(`\n✨ Success Rate (based on operations vs errors): ${successRate}%`);
  }
}

function parseArgs() {
  const args = process.argv.slice(2);
  const options = { moduleName: 'Leads' }; // Default module
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--module' && args[i+1]) {
      options.moduleName = args[i+1];
      i++;
    } else if (args[i] === '--dry-run') {
      options.dryRun = true;
    } else if (args[i] === '--verbose' || args[i] === '-v') {
      options.verbose = true;
    } else if (args[i] === '--help' || args[i] === '-h') {
      console.log(`
🚀 Bulk Sync Usage:
  node bulkSync.js [--module ModuleName] [--dry-run] [--verbose | -v] [--help | -h]

Options:
  --module <ModuleName>  Specify the Zoho module to sync (e.g., Leads, Contacts, Accounts). Defaults to 'Leads'.
  --dry-run              Preview changes without executing.
  --verbose, -v          Detailed logging.
  --help, -h             Show this help message.

Examples:
  node bulkSync.js                               # Sync Leads module
  node bulkSync.js --module Contacts             # Sync Contacts module
  node bulkSync.js --module Accounts --dry-run   # Dry run for Accounts module

Description:
  Fetches ALL records for the specified module from both Zoho and Airtable,
  compares modification times, and syncs the newer version. Creates new
  records in the target system if they don't exist.
`);
      process.exit(0);
    }
  }
  return options;
}

async function main() {
  const options = parseArgs();
  const bulkSync = new BulkSync(options);
  await bulkSync.run();
}

if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error in bulkSync main:', error.message);
    if (error.stack) console.error(error.stack);
    process.exit(1);
  });
}

module.exports = BulkSync;
