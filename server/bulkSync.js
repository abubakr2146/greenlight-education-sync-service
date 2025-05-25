#!/usr/bin/env node

/**
 * Bulk Sync - Complete Record Synchronization for a specific module
 * Can also sync a single Zoho record if --zoho-id is provided.
 */

const axios = require('axios');
const { loadZohoConfig, loadAirtableConfig } = require('./src/config/config');
const {
  getMultipleRecordDetails,
  getRecordDetails, // For fetching a single Zoho record
  refreshZohoToken,
  deleteZohoRecord,
  getZohoModulePluralName
} = require('./src/services/zohoService');
const {
  // getAllModuleRecordsForSync, // Not directly used if fetching all Airtable records via axios
  getModuleRecordById, 
  findModuleRecordByZohoId,
  updateModuleField,
  getModuleConfig, 
  getFieldIdToNameMapping
} = require('./src/services/airtableService');
const {
  createAirtableRecordFromZoho,
  createZohoRecordFromAirtable,
  handleZohoRecordUpdate, 
  handleAirtableRecordUpdate, 
  getZohoCrmIdMapping,
  getAirtableIdMapping,
  SYNC_DIRECTIONS
} = require('./src/services/syncService');
const fieldMappingCache = require('./src/utils/fieldMappingCache');

class BulkSync {
  constructor(options = {}) {
    this.moduleName = options.moduleName || 'Leads';
    this.specificZohoId = options.zohoId || null; // New option
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
    this.zohoCrmIdAirtableField = null;
    this.airtableIdFieldFromMapping = null; // Cache for Airtable ID field
    this.airtableLastModifiedField = 'Last Modified Time';
  }

  async run() {
    try {
      if (this.specificZohoId) {
        console.log(`🎯 Starting Single Record Sync for Zoho ${this.moduleName} ID: ${this.specificZohoId}`);
      } else {
        console.log(`🚀 Starting Bulk Synchronization for ${this.moduleName}`);
      }
      console.log('=====================================');
      console.log(`🎯 Mode: ${this.dryRun ? 'DRY RUN (no changes)' : 'LIVE SYNC'}`);
      console.log(`📊 Verbose: ${this.verbose ? 'ON' : 'OFF'}\n`);

      console.log(`🔧 Initializing field mapping cache for ${this.moduleName}...`);
      await fieldMappingCache.initialize(this.moduleName);
      await new Promise(resolve => setTimeout(resolve, process.env.NODE_ENV === 'test' ? 50 : 1000));

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
      
      // Cache the Airtable ID mapping to avoid repeated lookups
      const airtableIdMap = await getAirtableIdMapping(this.moduleName);
      this.airtableIdFieldFromMapping = airtableIdMap?.airtable;

      console.log(`📥 Fetching records from both systems...`);
      const [zohoRecords, airtableRecords] = await Promise.all([
        this.getAllZohoRecords(),
        this.getAllAirtableRecords()
      ]);

      if (this.specificZohoId && zohoRecords.length === 0) {
        console.error(`❌ Zoho ${this.moduleName} record with ID ${this.specificZohoId} not found or failed to fetch. Aborting.`);
        return;
      }
      
      this.stats.totalZohoRecords = zohoRecords.length;
      this.stats.totalAirtableRecords = airtableRecords.length;

      console.log(`📊 Found ${zohoRecords.length} Zoho ${this.moduleName} records, ${airtableRecords.length} Airtable records for ${this.moduleName}\n`);

      console.log('📋 Creating sync plan...');
      const syncPlan = await this.createBulkSyncPlan(zohoRecords, airtableRecords);

      console.log('📈 Sync Plan Summary:');
      console.log(`├─ Zoho → Airtable: ${syncPlan.zohoToAirtable.length}`);
      console.log(`├─ Airtable → Zoho: ${syncPlan.airtableToZoho.length}`);
      console.log(`├─ New Airtable records: ${syncPlan.newAirtableRecords.length}`);
      console.log(`├─ New Zoho records: ${syncPlan.newZohoRecords.length}`);
      console.log(`├─ Conflicts (manual review): ${syncPlan.conflicts.length}`);
      console.log(`└─ No sync needed: ${syncPlan.noSyncNeeded.length}\n`);

      if (this.dryRun) {
        console.log('🔍 DRY RUN - No changes will be made\n');
        await this.showDetailedPlan(syncPlan);
        if (!this.specificZohoId) { // Only run deletion checks in full bulk mode
            await this.performDeletionChecks(zohoRecords, airtableRecords, true);
        } else {
            console.log("ℹ️ Deletion checks skipped for single record sync.");
        }
        this.showResults();
        return;
      }

      console.log('⚡ Executing sync plan...');
      await this.executeBulkSyncPlan(syncPlan);

      if (!this.specificZohoId) { // Only run deletion checks in full bulk mode
        console.log('\n🗑️  Performing deletion checks...');
        await this.performDeletionChecks(zohoRecords, airtableRecords, false);
      } else {
        console.log("\nℹ️ Deletion checks skipped for single record sync.");
      }

      this.showResults();

    } catch (error) {
      console.error(`❌ Sync for ${this.moduleName} failed:`, error.message);
      if (this.verbose) console.error(error.stack);
      this.stats.errors++;
    } finally {
      fieldMappingCache.destroyModule(this.moduleName);
      console.log(`\n✅ Field mapping cache for ${this.moduleName} destroyed.`);
    }
  }

  async getAllZohoRecords() {
    const config = loadZohoConfig();
    if (!config) throw new Error('Zoho config not found');
    try {
      if (Date.now() >= config.tokenExpiry) await refreshZohoToken(config);

      if (this.specificZohoId) {
        console.log(`📥 Fetching specific Zoho ${this.moduleName} record ID: ${this.specificZohoId}...`);
        const recordDetailResponse = await getRecordDetails(this.specificZohoId, this.moduleName, config);
        if (recordDetailResponse && recordDetailResponse.data && recordDetailResponse.data.length > 0) {
          const record = recordDetailResponse.data[0];
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
          if (isNaN(modifiedTime)) modifiedTime = Date.now();
          return [{ id: record.id, modifiedTime, data: record, source: 'zoho', timestampSource }];
        } else {
          console.warn(`Specific Zoho ${this.moduleName} record ${this.specificZohoId} not found or error fetching.`);
          return [];
        }
      } else {
        // Fetch all records (existing logic)
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
              params: { page, per_page: perPage, fields: 'id,Modified_Time,Created_Time,Last_Activity_Time', sort_by: 'Modified_Time', sort_order: 'desc' }
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
          if (isNaN(modifiedTime)) modifiedTime = Date.now();
          return { id: record.id, modifiedTime, data: record, source: 'zoho', timestampSource };
        });
      }
    } catch (error) {
      console.error(`Error fetching Zoho ${this.moduleName} records:`, error.response ? error.response.data : error.message);
      throw new Error(`Failed to fetch Zoho ${this.moduleName} records: ${error.message}`);
    }
  }

  async getAllAirtableRecords() {
    const config = loadAirtableConfig();
    if (!config) throw new Error('Airtable config not found');
    try {
      const airtableModuleConfig = await getModuleConfig(config, this.moduleName);
      const tableNameToUse = airtableModuleConfig.tableName;

      if (this.specificZohoId) {
        console.log(`📥 Fetching corresponding Airtable record for Zoho ID: ${this.specificZohoId}...`);
        const airtableRecordId = await findModuleRecordByZohoId(this.specificZohoId, this.moduleName);
        if (airtableRecordId) {
          const record = await getModuleRecordById(airtableRecordId, this.moduleName); // This returns the full record object
          if (record) {
            const lastModified = record.fields[this.airtableLastModifiedField];
            const modifiedTime = new Date(lastModified).getTime();
            return [{
              id: record.id,
              zohoId: record.fields[this.zohoCrmIdAirtableField] || null,
              modifiedTime: isNaN(modifiedTime) ? Date.now() : modifiedTime,
              data: record, // getModuleRecordById returns the full record structure
              source: 'airtable'
            }];
          }
        }
        console.warn(`No corresponding Airtable record found for Zoho ID ${this.specificZohoId}.`);
        return [];
      } else {
        // Fetch all records (existing logic)
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
          return {
            id: record.id,
            zohoId: record.fields[this.zohoCrmIdAirtableField] || null,
            modifiedTime: isNaN(modifiedTime) ? Date.now() : modifiedTime,
            data: record,
            source: 'airtable'
          };
        });
      }
    } catch (error) {
      console.error(`Error fetching Airtable ${this.moduleName} records:`, error.response ? error.response.data : error.message);
      throw new Error(`Failed to fetch Airtable records for ${this.moduleName}: ${error.message}`);
    }
  }

  async compareFieldValues(zohoRecord, airtableRecord) {
    try {
      await fieldMappingCache.ensureModuleInitialized(this.moduleName);
      const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(this.moduleName);
      if (!currentModuleFieldMapping) {
        console.warn(`[BulkSync][${this.moduleName}] No field mapping available for comparison. Assuming fields have changed.`);
        return true; // Assume changed if we can't compare
      }

      const airtableFieldIdToNameMap = await getFieldIdToNameMapping(null, this.moduleName);
      
      let hasChanges = false;
      let checkedFields = 0;

      for (const [zohoFieldMapKey, mapping] of Object.entries(currentModuleFieldMapping)) {
        if (zohoFieldMapKey === 'ZOHO_ID' || zohoFieldMapKey === 'AIRTABLE_ID') continue;
        if (!mapping.airtable || !mapping.zoho) continue;
        
        // Quick check for ID fields without calling shouldIgnoreField
        if (mapping.airtable === this.zohoCrmIdAirtableField || 
            mapping.airtable === this.airtableIdFieldFromMapping ||
            mapping.zoho.startsWith('$') ||
            ['Modified_Time', 'Created_Time', 'Modified_By', 'Created_By', 'id', 'Owner'].includes(mapping.zoho)) {
          continue;
        }

        let airtableFieldName = mapping.airtable;
        if (mapping.airtable.startsWith('fld')) {
          if (airtableFieldIdToNameMap && airtableFieldIdToNameMap[mapping.airtable]) {
            airtableFieldName = airtableFieldIdToNameMap[mapping.airtable];
          } else {
            continue; // Skip if we can't resolve the field name
          }
        }

        const zohoValue = zohoRecord.data[mapping.zoho];
        const airtableValue = airtableRecord.data.fields[airtableFieldName];

        // Normalize values for comparison
        const normalizedZohoValue = this.normalizeValue(zohoValue);
        const normalizedAirtableValue = this.normalizeValue(airtableValue);

        if (normalizedZohoValue !== normalizedAirtableValue) {
          if (this.verbose) {
            console.log(`   Field '${mapping.zoho}' differs: Zoho='${normalizedZohoValue}' vs Airtable='${normalizedAirtableValue}'`);
          }
          hasChanges = true;
          break; // Exit early if we find any change
        }
        checkedFields++;
      }

      if (this.verbose && !hasChanges) {
        console.log(`   No changes detected across ${checkedFields} mapped fields for Zoho ${zohoRecord.id}`);
      }

      return hasChanges;
    } catch (error) {
      console.error(`[BulkSync][${this.moduleName}] Error comparing field values:`, error.message);
      return true; // Assume changed on error
    }
  }

  normalizeValue(value) {
    if (value === null || value === undefined) return '';
    if (typeof value === 'string') return value.trim();
    if (typeof value === 'boolean') return value.toString();
    if (typeof value === 'number') return value.toString();
    if (Array.isArray(value)) {
      return value.map(item => {
        if (typeof item === 'object' && item !== null && item.name) {
          return item.name;
        }
        return String(item);
      }).join(', ');
    }
    if (typeof value === 'object' && value !== null) {
      if (value.name) return value.name;
      // For other objects, use JSON string for comparison
      try {
        return JSON.stringify(value);
      } catch (e) {
        return String(value);
      }
    }
    return String(value);
  }

  async createBulkSyncPlan(zohoRecords, airtableRecords) {
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
        if (timeDiff < 30000) { 
          plan.noSyncNeeded.push({ zoho: zohoRecord, airtable: airtableRecord, reason: 'Timestamps too close' });
        } else {
          // Check if field values have actually changed
          const hasChanges = await this.compareFieldValues(zohoRecord, airtableRecord);
          
          if (!hasChanges) {
            plan.noSyncNeeded.push({ zoho: zohoRecord, airtable: airtableRecord, reason: 'No field changes detected' });
          } else if (zohoTime > airtableTime) {
            plan.zohoToAirtable.push({ zoho: zohoRecord, airtable: airtableRecord, timeDiff, zohoTime: new Date(zohoTime).toISOString(), airtableTime: new Date(airtableTime).toISOString() });
          } else {
            plan.airtableToZoho.push({ airtable: airtableRecord, zoho: zohoRecord, timeDiff, zohoTime: new Date(zohoTime).toISOString(), airtableTime: new Date(airtableTime).toISOString() });
          }
        }
      }
    }
    // If we are syncing a specific Zoho record, we don't want to create a new Zoho record from an unlinked Airtable record.
    // This logic is primarily for full bulk sync.
    if (!this.specificZohoId) {
        for (const airtableRecord of airtableRecords) {
          if (!airtableRecord.zohoId) {
            plan.newZohoRecords.push(airtableRecord);
          }
        }
    } else if (this.specificZohoId && airtableRecords.length > 0 && !airtableRecords[0].zohoId) {
        // If syncing a specific Zoho ID, and we found an Airtable record, but it's NOT linked to ANY Zoho ID,
        // it could be a candidate for linking and then creating in Zoho if it doesn't exist.
        // However, the current plan focuses on the Zoho record as the primary.
        // If the Airtable record found via findModuleRecordByZohoId was indeed linked, airtableRecords[0].zohoId would exist.
        // This case (specificZohoId is set, airtable record exists but has no zohoId) should be rare if findModuleRecordByZohoId was used.
        // For now, we'll assume if specificZohoId is set, newZohoRecords will be empty unless explicitly handled.
    }


    return plan;
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
      } catch (e) { this.stats.errors++; console.error(`Error creating Airtable record: ${e.message}`);}
    }

    if (!this.specificZohoId) { // Only create new Zoho records in full bulk mode
        for (const airtableRecord of plan.newZohoRecords) {
          processed++;
          console.log(`[${processed}/${totalOperations}] Creating new Zoho ${this.moduleName} for Airtable record ${airtableRecord.id}`);
          try {
            const created = await createZohoRecordFromAirtable(airtableRecord.id, airtableRecord.data, this.moduleName);
            created ? this.stats.newZohoRecords++ : this.stats.errors++;
          } catch (e) { this.stats.errors++; console.error(`Error creating Zoho record: ${e.message}`);}
        }
    }


    // ZOHO -> AIRTABLE UPDATES
    for (const item of plan.zohoToAirtable) {
      processed++;
      const ageMinutes = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
      console.log(`[${processed}/${totalOperations}] Syncing Zoho ${this.moduleName} ${item.zoho.id} → Airtable record ${item.airtable.id} (Zoho ${ageMinutes}min newer)`);
      try {
        await fieldMappingCache.ensureModuleInitialized(this.moduleName);
        const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(this.moduleName);
        if (!currentModuleFieldMapping) {
            console.error(`[BulkSync][${this.moduleName}] No field mapping. Cannot sync Zoho ${item.zoho.id}.`);
            this.stats.errors++;
            continue;
        }
        
        const changedFields = [];
        const currentValues = {};
        for (const zohoFieldApiName in item.zoho.data) {
            if (await this.shouldIgnoreField(zohoFieldApiName, null)) continue;
            if (currentModuleFieldMapping[zohoFieldApiName] || Object.values(currentModuleFieldMapping).find(m => m.zoho === zohoFieldApiName)) { 
                changedFields.push(zohoFieldApiName);
                currentValues[zohoFieldApiName] = item.zoho.data[zohoFieldApiName];
            }
        }
        
        const changedFieldsInfo = { changedFields, currentValues };

        const success = await handleZohoRecordUpdate(item.zoho.id, item.zoho.data, changedFieldsInfo, this.moduleName);
        success ? this.stats.zohoToAirtable++ : this.stats.errors++;
      } catch (e) {
        this.stats.errors++;
        console.error(`Error syncing Zoho ${item.zoho.id} to Airtable ${item.airtable.id}: ${e.message}`);
        if (this.verbose) console.error(e.stack);
      }
    }

    // AIRTABLE -> ZOHO UPDATES
    for (const item of plan.airtableToZoho) {
      processed++;
      const ageMinutes = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
      console.log(`[${processed}/${totalOperations}] Syncing Airtable record ${item.airtable.id} → Zoho ${this.moduleName} ${item.zoho.id} (Airtable ${ageMinutes}min newer)`);
      try {
        await fieldMappingCache.ensureModuleInitialized(this.moduleName);
        const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(this.moduleName);
        const airtableFieldIdToNameMap = await getFieldIdToNameMapping(null, this.moduleName);

        if (!currentModuleFieldMapping) {
            console.error(`[BulkSync][${this.moduleName}] No field mapping. Cannot sync Airtable ${item.airtable.id}.`);
            this.stats.errors++;
            continue;
        }
        
        const changedFieldsInfoArray = [];
        for (const [zohoKey, mapping] of Object.entries(currentModuleFieldMapping)) {
            if (await this.shouldIgnoreField(mapping.zoho, mapping.airtable)) continue;

            let airtableFieldName = mapping.airtable;
            if (mapping.airtable.startsWith('fld') && airtableFieldIdToNameMap && airtableFieldIdToNameMap[mapping.airtable]) {
                airtableFieldName = airtableFieldIdToNameMap[mapping.airtable];
            } else if (mapping.airtable.startsWith('fld')) {
                if(this.verbose) console.warn(`   [BulkSync][${this.moduleName}] Airtable field name for ID ${mapping.airtable} not found in map. Skipping field ${mapping.zoho} for A->Z sync of ${item.airtable.id}`);
                continue; 
            }
            
            const airtableValue = item.airtable.data.fields[airtableFieldName];
            if (airtableValue !== undefined) { 
                changedFieldsInfoArray.push({
                    fieldName: airtableFieldName, 
                    currentValue: airtableValue
                });
            }
        }
        
        if (changedFieldsInfoArray.length === 0) {
            if(this.verbose) console.log(`   No mappable fields with values found to sync from Airtable ${item.airtable.id} to Zoho ${item.zoho.id}.`);
            this.stats.skipped++;
            continue;
        }

        const success = await handleAirtableRecordUpdate(item.airtable.id, changedFieldsInfoArray, this.moduleName);
        success ? this.stats.airtableToZoho++ : this.stats.errors++;
      } catch (e) {
        this.stats.errors++;
        console.error(`Error syncing Airtable ${item.airtable.id} to Zoho ${item.zoho.id}: ${e.message}`);
        if (this.verbose) console.error(e.stack);
      }
    }
  }

  async performDeletionChecks(zohoRecords, airtableRecords, dryRun) {
    // This function should only run in full bulk mode, not single record mode.
    if (this.specificZohoId) {
        console.log("ℹ️ Deletion checks skipped for single record sync mode inside performDeletionChecks.");
        return;
    }

    const zohoRecordIds = new Set(zohoRecords.map(r => r.id));
    const airtableRecordsWithZohoId = airtableRecords.filter(r => r.zohoId);

    for (const zohoRecord of zohoRecords) {
      const hasAirtableCounterpart = airtableRecords.some(ar => ar.zohoId === zohoRecord.id);
      if (!hasAirtableCounterpart) {
        const createdTime = new Date(zohoRecord.data.Created_Time).getTime();
        const ageInHours = (Date.now() - createdTime) / (3600000);
        const ageThresholdHours = 24; 
        if (ageInHours > ageThresholdHours) {
          console.log(`${dryRun ? '[DRY RUN] Would delete' : 'Deleting'} Zoho ${this.moduleName} ${zohoRecord.id} (no Airtable link, ${Math.round(ageInHours)} hrs old)`);
          if (!dryRun) {
            try {
              const deleted = await deleteZohoRecord(zohoRecord.id, this.moduleName);
              if (deleted) this.stats.deletedZohoRecords++; else this.stats.errors++;
            } catch (e) { this.stats.errors++; console.error(`Error deleting Zoho ${this.moduleName} ${zohoRecord.id}: ${e.message}`);}
          } else {
            this.stats.deletedZohoRecords++;
          }
        } else {
          if(this.verbose) console.log(`⏭️  Skipping deletion of new Zoho ${this.moduleName} ${zohoRecord.id} (only ${Math.round(ageInHours)} hrs old, no Airtable link)`);
        }
      }
    }

    const airtableModuleConfig = await getModuleConfig(loadAirtableConfig(), this.moduleName);
    const airtableDeletedStatusField = airtableModuleConfig.statusFieldInternal || 'Status'; 
    const airtableDeletedStatusValue = `Deleted in Zoho`; 

    for (const airtableRecord of airtableRecordsWithZohoId) {
      if (!zohoRecordIds.has(airtableRecord.zohoId)) {
        if (airtableRecord.data.fields[airtableDeletedStatusField] === airtableDeletedStatusValue) {
            if(this.verbose) console.log(`ℹ️ Airtable record ${airtableRecord.id} already marked as deleted.`);
            continue;
        }
        console.log(`${dryRun ? '[DRY RUN] Would mark' : 'Marking'} Airtable record ${airtableRecord.id} (for ${this.moduleName}) as '${airtableDeletedStatusValue}' (Zoho ID ${airtableRecord.zohoId} not found)`);
        if (!dryRun) {
          try {
            const updated = await updateModuleField(airtableRecord.id, airtableDeletedStatusField, airtableDeletedStatusValue, this.moduleName);
            if (updated) this.stats.markedAirtableAsDeleted++; else this.stats.errors++;
          } catch (e) { this.stats.errors++; console.error(`Error marking Airtable record ${airtableRecord.id} as deleted: ${e.message}`);}
        } else {
          this.stats.markedAirtableAsDeleted++;
        }
      }
    }
    console.log(`\n🗑️  Deletion check completed: ${this.stats.deletedZohoRecords} Zoho ${this.moduleName}(s) ${dryRun ? 'would be' : ''} deleted, ${this.stats.markedAirtableAsDeleted} Airtable record(s) ${dryRun ? 'would be' : ''} marked as deleted.`);
  }

  async shouldIgnoreField(zohoFieldName, airtableFieldName) {
    const ignoredZohoFields = [
      'Modified_Time', 'Created_Time', 'Modified_By', 'Created_By', 'id',
      'CurrencySymbol', '$currency_symbol', 'Exchange_Rate', '$exchange_rate',
      'Last_Activity_Time', 'Layout', '$layout', 'Lead_Conversion_Time', 
      'Data_Processing_Basis_Details', 'Approval', 'Process_Flow', '$process_flow', 
      '$approved', '$approval', '$editable', '$review_process', '$review', 
      '$zia_assign_time', '$in_merge', '$locked_for_me', '$lock_source_s', 
      '$orchestration', '$converted', '$converted_detail', '$zia_owner_assignment', 
      '$zia_visit_count', '$zia_visit_source', '$zia_contact_source', 
      '$zia_event_source', '$zia_event_name', '$zia_event_description', 
      '$zia_event_timestamp', 'Tag', 'Owner'
    ];
     if (ignoredZohoFields.includes(zohoFieldName) || (zohoFieldName && zohoFieldName.startsWith('$'))) {
        if(this.verbose && zohoFieldName) console.log(`   Ignoring Zoho system field: ${zohoFieldName}`);
        return true;
    }
    
    if (airtableFieldName === this.zohoCrmIdAirtableField || airtableFieldName === this.airtableIdFieldFromMapping) {
        if(this.verbose && airtableFieldName) console.log(`   Ignoring Airtable ID mapping field: ${airtableFieldName}`);
        return true;
    }
    const ignoredAirtableSystemLikeFields = ['Last Modified Time', 'Created Time', 'Record ID', 'Formatted Name']; 
    if (airtableFieldName && ignoredAirtableSystemLikeFields.includes(airtableFieldName)) {
        if(this.verbose) console.log(`   Ignoring Airtable system-like field: ${airtableFieldName}`);
        return true;
    }
    return false;
  }

  async showDetailedPlan(plan) { 
    const logItem = (item, type) => {
      if (type === 'newAirtable') console.log(`   - Zoho ${this.moduleName} ${item.id}`);
      else if (type === 'newZoho') console.log(`   - Airtable record ${item.id}`);
      else if (type === 'zohoToAirtable') {
        const age = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
        console.log(`   - Zoho ${this.moduleName} ${item.zoho.id} (Zoho ${age}min newer) → Airtable ${item.airtable.id}`);
      } else if (type === 'airtableToZoho') {
        const age = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
        console.log(`   - Airtable record ${item.airtable.id} (Airtable ${age}min newer) → Zoho ${this.moduleName} ${item.zoho.id}`);
      }
    };
    const logPlanSection = (title, items, type) => {
      if (items.length > 0) {
        console.log(title);
        items.slice(0, this.verbose ? items.length : 5).forEach(item => logItem(item, type));
        if (!this.verbose && items.length > 5) console.log(`   ... and ${items.length - 5} more`);
        console.log('');
      }
    };
    logPlanSection(`🆕 New Airtable records for ${this.moduleName} to create:`, plan.newAirtableRecords, 'newAirtable');
    if (!this.specificZohoId) { // Only show this section in full bulk mode
        logPlanSection(`🆕 New Zoho ${this.moduleName} records to create:`, plan.newZohoRecords, 'newZoho');
    }
    logPlanSection(`🔄 Zoho ${this.moduleName} → Airtable syncs:`, plan.zohoToAirtable, 'zohoToAirtable');
    logPlanSection(`🔄 Airtable → Zoho ${this.moduleName} syncs:`, plan.airtableToZoho, 'airtableToZoho');
  }

  showResults() {
    console.log(`\n🎉 Sync for ${this.moduleName} ${this.specificZohoId ? `(ID: ${this.specificZohoId}) ` : ''}Completed!`);
    console.log('========================');
    console.log(`📊 Total Zoho ${this.moduleName} Records Processed: ${this.stats.totalZohoRecords}`);
    console.log(`📊 Total Airtable Records for ${this.moduleName} Processed: ${this.stats.totalAirtableRecords}`);
    console.log('');
    console.log('📈 Sync Results:');
    console.log(`├─ New Airtable Records: ${this.stats.newAirtableRecords}`);
    console.log(`├─ New Zoho ${this.moduleName} Records: ${this.stats.newZohoRecords}`);
    console.log(`├─ Zoho → Airtable Syncs: ${this.stats.zohoToAirtable}`);
    console.log(`├─ Airtable → Zoho Syncs: ${this.stats.airtableToZoho}`);
    if (!this.specificZohoId) {
        console.log(`├─ Zoho ${this.moduleName} Records Deleted: ${this.stats.deletedZohoRecords}`);
        console.log(`├─ Airtable Records Marked Deleted: ${this.stats.markedAirtableAsDeleted}`);
    }
    console.log(`├─ Errors: ${this.stats.errors}`);
    const totalOps = this.stats.newAirtableRecords + this.stats.newZohoRecords + this.stats.zohoToAirtable + this.stats.airtableToZoho + 
                     (this.specificZohoId ? 0 : (this.stats.deletedZohoRecords + this.stats.markedAirtableAsDeleted));
    console.log(`└─ Total Operations: ${totalOps}`);
    const successRate = (totalOps + this.stats.errors) > 0 ? Math.round((totalOps / (totalOps + this.stats.errors)) * 100) : 100; 
    console.log(`\n✨ Success Rate (based on operations vs errors): ${successRate}%`);
  }
}

function parseArgs() {
  const args = process.argv.slice(2);
  const options = { moduleName: 'Leads' };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--module' && args[i+1]) {
      options.moduleName = args[i+1];
      i++;
    } else if (args[i] === '--zoho-id' && args[i+1]) { // New option
      options.zohoId = args[i+1];
      i++;
    } else if (args[i] === '--dry-run') {
      options.dryRun = true;
    } else if (args[i] === '--verbose' || args[i] === '-v') {
      options.verbose = true;
    } else if (args[i] === '--help' || args[i] === '-h') {
      console.log(`
🚀 Bulk Sync Usage:
  node bulkSync.js [--module ModuleName] [--zoho-id ZohoRecordId] [--dry-run] [--verbose | -v] [--help | -h]

Options:
  --module <ModuleName>    Specify the Zoho module to sync (e.g., Leads, Contacts). Defaults to 'Leads'.
  --zoho-id <ZohoRecordId> Sync only this specific Zoho record ID for the given module.
  --dry-run                Preview changes without executing.
  --verbose, -v            Detailed logging.
  --help, -h               Show this help message.

Examples:
  node bulkSync.js                                       # Full sync for Leads module
  node bulkSync.js --module Contacts                     # Full sync for Contacts module
  node bulkSync.js --module Leads --zoho-id 123456789    # Sync specific Lead ID 123456789
  node bulkSync.js --module Accounts --dry-run           # Dry run for Accounts module
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
