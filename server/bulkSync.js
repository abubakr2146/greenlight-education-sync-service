#!/usr/bin/env node

/**
 * Bulk Sync - Complete Record Synchronization for a specific module
 * Can also sync a single Zoho record if --zoho-id is provided.
 */

const axios = require('axios');
const { loadZohoConfig, loadAirtableConfig, shouldIgnoreField } = require('./src/config/config');
const {
  getMultipleRecordDetails,
  getRecordDetails, // For fetching a single Zoho record
  refreshZohoToken,
  deleteZohoRecord,
  getZohoModulePluralName,
  createZohoRecordsBulk,
  updateZohoRecordsBulk
} = require('./src/services/zohoService');
const {
  // getAllModuleRecordsForSync, // Not directly used if fetching all Airtable records via axios
  getModuleRecordById, 
  findModuleRecordByZohoId,
  updateModuleField,
  getModuleConfig, 
  getFieldIdToNameMapping,
  createModuleRecordsBulk,
  updateModuleRecordsBulk
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
    this.noDelete = options.noDelete || false;
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
        if (!this.specificZohoId && !this.noDelete) { // Only run deletion checks in full bulk mode and when not disabled
            await this.performDeletionChecks(zohoRecords, airtableRecords, true);
        } else {
            if (this.specificZohoId) {
                console.log("ℹ️ Deletion checks skipped for single record sync.");
            } else if (this.noDelete) {
                console.log("ℹ️ Deletion checks skipped (--no-delete flag).");
            }
        }
        this.showResults();
        return;
      }

      console.log('⚡ Executing sync plan...');
      await this.executeBulkSyncPlan(syncPlan);

      if (!this.specificZohoId && !this.noDelete) { // Only run deletion checks in full bulk mode and when not disabled
        console.log('\n🗑️  Performing deletion checks...');
        await this.performDeletionChecks(zohoRecords, airtableRecords, false);
      } else {
        if (this.specificZohoId) {
            console.log("\nℹ️ Deletion checks skipped for single record sync.");
        } else if (this.noDelete) {
            console.log("\nℹ️ Deletion checks skipped (--no-delete flag).");
        }
      }

      this.showResults();
      
      // Cleanup cache after successful completion
      fieldMappingCache.destroyModule(this.moduleName);
      console.log(`\n✅ Field mapping cache for ${this.moduleName} destroyed.`);

    } catch (error) {
      console.error(`❌ Sync for ${this.moduleName} failed:`, error.message);
      if (this.verbose) console.error(error.stack);
      this.stats.errors++;
      
      // Cleanup cache after error
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
        const zohoModuleApiName = await getZohoModulePluralName(this.moduleName);
        console.log(`🔍 Using Zoho API module name: ${zohoModuleApiName} for ${this.moduleName}`);
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
      // Use cached field mapping if available
      if (!this._cachedFieldMapping) {
        await fieldMappingCache.ensureModuleInitialized(this.moduleName);
        this._cachedFieldMapping = fieldMappingCache.getFieldMapping(this.moduleName);
        this._cachedFieldIdToNameMap = await getFieldIdToNameMapping(null, this.moduleName);
      }
      
      const currentModuleFieldMapping = this._cachedFieldMapping;
      if (!currentModuleFieldMapping) {
        console.warn(`[BulkSync][${this.moduleName}] No field mapping available for comparison. Assuming fields have changed.`);
        return true; // Assume changed if we can't compare
      }

      const airtableFieldIdToNameMap = this._cachedFieldIdToNameMap;
      
      let hasChanges = false;
      let checkedFields = 0;

      for (const [zohoFieldMapKey, mapping] of Object.entries(currentModuleFieldMapping)) {
        if (zohoFieldMapKey === 'ZOHO_ID' || zohoFieldMapKey === 'AIRTABLE_ID') continue;
        if (!mapping.airtable || !mapping.zoho) continue;
        
        // Use proper ignore field checking from config
        if (this.isIdMappingField(mapping.airtable) ||
            shouldIgnoreField(mapping.zoho, 'zoho')) {
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

    // Process records in batches for parallel comparison
    const BATCH_SIZE = 50; // Increased back since we fixed the repeated API calls
    const totalComparisons = airtableMapByZohoId.size;
    let processedComparisons = 0;
    let lastProgressUpdate = Date.now();
    
    // Pre-cache field mappings to avoid repeated API calls
    if (!this._cachedFieldMapping) {
      await fieldMappingCache.ensureModuleInitialized(this.moduleName);
      this._cachedFieldMapping = fieldMappingCache.getFieldMapping(this.moduleName);
      this._cachedFieldIdToNameMap = await getFieldIdToNameMapping(null, this.moduleName);
    }

    // Process Zoho records in batches
    for (let i = 0; i < zohoRecords.length; i += BATCH_SIZE) {
      const batch = zohoRecords.slice(i, Math.min(i + BATCH_SIZE, zohoRecords.length));
      
      // Process batch in parallel
      const batchPromises = batch.map(async (zohoRecord) => {
        const airtableRecord = airtableMapByZohoId.get(zohoRecord.id);
        if (!airtableRecord) {
          return { type: 'newAirtable', record: zohoRecord };
        }

        const zohoTime = zohoRecord.modifiedTime;
        const airtableTime = airtableRecord.modifiedTime;
        const timeDiff = Math.abs(zohoTime - airtableTime);
        
        if (timeDiff < 30000) {
          return { type: 'noSync', data: { zoho: zohoRecord, airtable: airtableRecord, reason: 'Timestamps too close' } };
        }

        // Check if field values have actually changed
        const hasChanges = await this.compareFieldValues(zohoRecord, airtableRecord);
        
        if (!hasChanges) {
          return { type: 'noSync', data: { zoho: zohoRecord, airtable: airtableRecord, reason: 'No field changes detected' } };
        } else if (zohoTime > airtableTime) {
          return { type: 'zohoToAirtable', data: { zoho: zohoRecord, airtable: airtableRecord, timeDiff, zohoTime: new Date(zohoTime).toISOString(), airtableTime: new Date(airtableTime).toISOString() } };
        } else {
          return { type: 'airtableToZoho', data: { airtable: airtableRecord, zoho: zohoRecord, timeDiff, zohoTime: new Date(zohoTime).toISOString(), airtableTime: new Date(airtableTime).toISOString() } };
        }
      });

      // Wait for batch to complete
      const batchResults = await Promise.all(batchPromises);
      
      // Sort results into plan categories
      for (const result of batchResults) {
        switch (result.type) {
          case 'newAirtable':
            plan.newAirtableRecords.push(result.record);
            break;
          case 'noSync':
            plan.noSyncNeeded.push(result.data);
            break;
          case 'zohoToAirtable':
            plan.zohoToAirtable.push(result.data);
            break;
          case 'airtableToZoho':
            plan.airtableToZoho.push(result.data);
            break;
        }
        
        if (result.type !== 'newAirtable') {
          processedComparisons++;
        }
      }

      // Show progress every 2 seconds or every 10%
      const now = Date.now();
      const progressPercent = Math.round((processedComparisons / totalComparisons) * 100);
      if (now - lastProgressUpdate > 2000 || progressPercent % 10 === 0) {
        if (!this.verbose) {
          process.stdout.write(`\r   Comparing records: ${processedComparisons}/${totalComparisons} (${progressPercent}%)`);
        } else {
          console.log(`   Processed batch ${Math.floor(i / BATCH_SIZE) + 1}: ${processedComparisons}/${totalComparisons} comparisons (${progressPercent}%)`);
        }
        lastProgressUpdate = now;
      }
    }
    
    // Clear the progress line
    if (!this.verbose && totalComparisons > 0) {
      process.stdout.write(`\r   Comparing records: ${totalComparisons}/${totalComparisons} (100%)\n`);
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

  // Helper function to prepare Airtable fields from Zoho data (similar to createAirtableRecordFromZoho)
  async prepareAirtableFieldsFromZoho(zohoRecordId, zohoRecordData) {
    const fields = {};
    const currentModuleFieldMapping = this._cachedFieldMapping || fieldMappingCache.getFieldMapping(this.moduleName);
    const airtableFieldIdToNameMap = this._cachedFieldIdToNameMap || await getFieldIdToNameMapping(null, this.moduleName);
    
    // Add Zoho CRM ID
    if (this.zohoCrmIdAirtableField) {
      fields[this.zohoCrmIdAirtableField] = zohoRecordId;
    }
    
    if (currentModuleFieldMapping) {
      for (const [zohoFieldApiName, mapping] of Object.entries(currentModuleFieldMapping)) {
        if (zohoFieldApiName === 'ZOHO_ID' || zohoFieldApiName === 'AIRTABLE_ID') continue;
        
        // Skip ignored fields
        let airtableFieldName = mapping.airtable;
        if (mapping.airtable && mapping.airtable.startsWith('fld') && airtableFieldIdToNameMap[mapping.airtable]) {
          airtableFieldName = airtableFieldIdToNameMap[mapping.airtable];
        }
        
        if (shouldIgnoreField(zohoFieldApiName, 'zoho') || 
            shouldIgnoreField(mapping.airtable, 'airtable') ||
            shouldIgnoreField(airtableFieldName, 'airtable')) {
          continue;
        }
        
        if (zohoRecordData[zohoFieldApiName] !== undefined &&
            zohoRecordData[zohoFieldApiName] !== null &&
            mapping.airtable && 
            (mapping.airtable !== this.zohoCrmIdAirtableField)) {
          
          let value = zohoRecordData[zohoFieldApiName];
          if (typeof value === 'object' && value !== null) {
            if (value.name) value = value.name;
            else if (Array.isArray(value)) value = value.map(item => (typeof item === 'object' && item.name) ? item.name : String(item)).join(', ');
            else if (mapping.isLookupToString || Object.keys(value).length > 0) {
              if ((zohoFieldApiName.startsWith('$') || ['Owner', 'Layout'].includes(zohoFieldApiName)) && !mapping.isLookupToString) {
                continue;
              }
              value = JSON.stringify(value);
            } else {
              value = null;
            }
          }
          if (value !== null && String(value).trim() !== '') {
            fields[mapping.airtable] = value;
          }
        }
      }
    }
    
    return fields;
  }
  
  // Helper function to prepare Zoho data from Airtable record (similar to createZohoRecordFromAirtable)
  async prepareZohoDataFromAirtable(airtableRecordId, airtableRecordData) {
    const zohoData = {};
    const currentModuleFieldMapping = this._cachedFieldMapping || fieldMappingCache.getFieldMapping(this.moduleName);
    const airtableFieldIdToNameMap = this._cachedFieldIdToNameMap || await getFieldIdToNameMapping(null, this.moduleName);
    
    if (currentModuleFieldMapping) {
      for (const [_key, mapping] of Object.entries(currentModuleFieldMapping)) {
        if (!mapping.zoho || !mapping.airtable) continue;
        if (mapping.zoho === 'ZOHO_ID' || mapping.zoho === 'AIRTABLE_ID') continue;
        
        // Skip ignored fields
        if (shouldIgnoreField(mapping.zoho, 'zoho') || 
            shouldIgnoreField(mapping.airtable, 'airtable')) {
          continue;
        }
        
        let airtableFieldName = mapping.airtable;
        if (mapping.airtable.startsWith('fld') && airtableFieldIdToNameMap[mapping.airtable]) {
          airtableFieldName = airtableFieldIdToNameMap[mapping.airtable];
        } else if (mapping.airtable.startsWith('fld')) {
          continue; // Skip if we can't resolve field name
        }
        
        const airtableValue = airtableRecordData.fields[airtableFieldName];
        if (airtableValue !== undefined && airtableValue !== null && String(airtableValue).trim() !== '') {
          zohoData[mapping.zoho] = airtableValue;
        }
      }
    }
    
    // Add Airtable ID to Zoho record
    const airtableIdMapping = await getAirtableIdMapping(this.moduleName);
    if (airtableIdMapping && airtableIdMapping.zoho) {
      zohoData[airtableIdMapping.zoho] = airtableRecordId;
    }
    
    return zohoData;
  }

  async executeBulkSyncPlan(plan) {
    let processed = 0;
    const totalOperations = plan.zohoToAirtable.length + plan.airtableToZoho.length + plan.newAirtableRecords.length + plan.newZohoRecords.length;
    
    // CREATE NEW AIRTABLE RECORDS USING BULK API
    if (plan.newAirtableRecords.length > 0) {
      console.log(`\n📝 Creating ${plan.newAirtableRecords.length} new Airtable records...`);
      
      // Prepare all records for bulk creation
      console.log(`   Preparing ${plan.newAirtableRecords.length} record(s) for creation...`);
      const recordsToCreatePromises = plan.newAirtableRecords.map(async (zohoRecord) => {
        const fields = await this.prepareAirtableFieldsFromZoho(zohoRecord.id, zohoRecord.data);
        return { fields, zohoId: zohoRecord.id };
      });
      
      const recordsToCreate = await Promise.all(recordsToCreatePromises);
      console.log(`   ✅ Prepared ${recordsToCreate.length} records`);
      
      // Filter out any records with empty fields
      const validRecords = recordsToCreate.filter(r => {
        const fieldKeys = Object.keys(r.fields);
        if (fieldKeys.length === 0 || (fieldKeys.length === 1 && fieldKeys[0] === this.zohoCrmIdAirtableField)) {
          console.warn(`   ⚠️  Skipping Zoho record ${r.zohoId} - no mappable fields`);
          this.stats.skipped++;
          return false;
        }
        return true;
      });
      
      if (validRecords.length > 0) {
        console.log(`   📤 Processing ${validRecords.length} valid records in batches of 10...`);
        const result = await createModuleRecordsBulk(
          validRecords.map(r => ({ fields: r.fields })),
          this.moduleName
        );
        
        // Update stats
        this.stats.newAirtableRecords += result.success.length;
        this.stats.errors += result.errors.length;
        
        // Log results
        console.log(`   ✅ Successfully created ${result.success.length} Airtable records`);
        if (result.errors.length > 0) {
          console.error(`   ❌ Failed to create ${result.errors.length} Airtable records`);
          if (this.verbose) {
            result.errors.forEach(err => {
              console.error(`      Batch ${err.batch}: ${err.error}`);
            });
          }
        }
      } else {
        console.log(`   ⚠️  No valid records to create after filtering`);
      }
      
      processed += plan.newAirtableRecords.length;
    }

    // CREATE NEW ZOHO RECORDS USING BULK API
    if (!this.specificZohoId && plan.newZohoRecords.length > 0) { // Only create new Zoho records in full bulk mode
      console.log(`\n📝 Creating ${plan.newZohoRecords.length} new Zoho ${this.moduleName} records...`);
      
      // Prepare all records for bulk creation
      console.log(`   Preparing ${plan.newZohoRecords.length} record(s) for creation...`);
      const recordsToCreatePromises = plan.newZohoRecords.map(async (airtableRecord) => {
        const zohoData = await this.prepareZohoDataFromAirtable(airtableRecord.id, airtableRecord.data);
        return { data: zohoData, airtableId: airtableRecord.id };
      });
      
      const recordsToCreate = await Promise.all(recordsToCreatePromises);
      console.log(`   ✅ Prepared ${recordsToCreate.length} records`);
      
      // Filter out any records with empty data
      const validRecords = recordsToCreate.filter(r => {
        const fieldKeys = Object.keys(r.data);
        if (fieldKeys.length === 0) {
          console.warn(`   ⚠️  Skipping Airtable record ${r.airtableId} - no mappable fields`);
          this.stats.skipped++;
          return false;
        }
        return true;
      });
      
      if (validRecords.length > 0) {
        console.log(`   📤 Processing ${validRecords.length} valid records in batches of 100...`);
        const result = await createZohoRecordsBulk(
          validRecords.map(r => r.data),
          this.moduleName
        );
        
        // Update stats
        this.stats.newZohoRecords += result.success.length;
        this.stats.errors += result.errors.length;
        
        // Log results
        console.log(`   ✅ Successfully created ${result.success.length} Zoho ${this.moduleName} records`);
        if (result.errors.length > 0) {
          console.error(`   ❌ Failed to create ${result.errors.length} Zoho records`);
          if (this.verbose) {
            result.errors.forEach(err => {
              if (err.batch !== undefined) {
                console.error(`      Batch ${err.batch}: ${err.error}`);
              } else if (err.record) {
                console.error(`      Record error: ${err.error}`);
              }
            });
          }
        }
        
        // Update Airtable records with the new Zoho IDs
        if (result.success.length > 0 && this.zohoCrmIdAirtableField) {
          console.log(`\n🔄 Updating ${result.success.length} Airtable records with new Zoho IDs...`);
          let updatedCount = 0;
          for (const zohoResult of result.success) {
            if (zohoResult.details && zohoResult.details.id && zohoResult.originalData) {
              // Find the corresponding Airtable record
              const airtableIdMapping = await getAirtableIdMapping(this.moduleName);
              const airtableInfo = recordsToCreate.find(r => {
                // Match by Airtable ID that was sent to Zoho
                return airtableIdMapping && r.data[airtableIdMapping.zoho] === zohoResult.originalData[airtableIdMapping.zoho];
              });
              
              if (airtableInfo) {
                try {
                  await updateModuleField(
                    airtableInfo.airtableId,
                    this.zohoCrmIdAirtableField,
                    zohoResult.details.id,
                    this.moduleName
                  );
                  updatedCount++;
                  if (this.verbose) {
                    console.log(`   ✅ Updated Airtable ${airtableInfo.airtableId} with Zoho ID ${zohoResult.details.id}`);
                  }
                } catch (e) {
                  console.error(`   ❌ Failed to update Airtable ${airtableInfo.airtableId}: ${e.message}`);
                }
              }
            }
          }
          console.log(`   ✅ Updated ${updatedCount}/${result.success.length} Airtable records with Zoho IDs`);
        }
      } else {
        console.log(`   ⚠️  No valid records to create after filtering`);
      }
      
      processed += plan.newZohoRecords.length;
    }


    // ZOHO -> AIRTABLE UPDATES
    if (plan.zohoToAirtable.length > 0) {
      console.log(`\n🔄 Performing ${plan.zohoToAirtable.length} Zoho → Airtable updates...`);
      
      // Pre-initialize field mapping once
      await fieldMappingCache.ensureModuleInitialized(this.moduleName);
      const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(this.moduleName);
      
      const UPDATE_BATCH_SIZE = 50; // Batch size for parallel updates
      const totalBatches = Math.ceil(plan.zohoToAirtable.length / UPDATE_BATCH_SIZE);
      
      for (let i = 0; i < plan.zohoToAirtable.length; i += UPDATE_BATCH_SIZE) {
        const batchNumber = Math.floor(i / UPDATE_BATCH_SIZE) + 1;
        const batch = plan.zohoToAirtable.slice(i, Math.min(i + UPDATE_BATCH_SIZE, plan.zohoToAirtable.length));
        
        console.log(`   📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} records)...`);
        
        // Process batch in parallel
        const batchPromises = batch.map(async (item, batchIndex) => {
          const currentIndex = processed + batchIndex + 1;
          const ageMinutes = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
          if (this.verbose) {
            console.log(`     [${currentIndex}/${totalOperations}] Syncing Zoho ${this.moduleName} ${item.zoho.id} → Airtable ${item.airtable.id} (Zoho ${ageMinutes}min newer)`);
          }
          
          try {
            if (!currentModuleFieldMapping) {
              console.error(`     [BulkSync][${this.moduleName}] No field mapping. Cannot sync Zoho ${item.zoho.id}.`);
              return { success: false, zohoId: item.zoho.id, error: 'No field mapping' };
            }
            
            const changedFields = [];
            const currentValues = {};
            for (const zohoFieldApiName in item.zoho.data) {
              if (shouldIgnoreField(zohoFieldApiName, 'zoho')) continue;
              if (currentModuleFieldMapping[zohoFieldApiName] || Object.values(currentModuleFieldMapping).find(m => m.zoho === zohoFieldApiName)) { 
                changedFields.push(zohoFieldApiName);
                currentValues[zohoFieldApiName] = item.zoho.data[zohoFieldApiName];
              }
            }
            
            const changedFieldsInfo = { changedFields, currentValues };
            // Pass the known Airtable record ID to avoid unnecessary lookups
            const success = await handleZohoRecordUpdate(item.zoho.id, item.zoho.data, changedFieldsInfo, this.moduleName, item.airtable.id);
            return { success, zohoId: item.zoho.id };
          } catch (e) {
            console.error(`     Error syncing Zoho ${item.zoho.id} to Airtable ${item.airtable.id}: ${e.message}`);
            if (this.verbose) console.error(e.stack);
            return { success: false, zohoId: item.zoho.id, error: e.message };
          }
        });
        
        // Wait for batch to complete
        const results = await Promise.all(batchPromises);
        
        // Update stats and show batch results
        const batchSuccesses = results.filter(r => r.success).length;
        const batchErrors = results.filter(r => !r.success).length;
        
        results.forEach(result => {
          if (result.success) {
            this.stats.zohoToAirtable++;
          } else {
            this.stats.errors++;
          }
        });
        
        console.log(`   ✅ Batch ${batchNumber} complete: ${batchSuccesses} success, ${batchErrors} errors`);
        
        processed += batch.length;
        
        // Add small delay between batches to avoid rate limits
        if (i + UPDATE_BATCH_SIZE < plan.zohoToAirtable.length) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      }
      
      console.log(`   ✅ All Zoho → Airtable updates complete: ${this.stats.zohoToAirtable} successful, ${this.stats.errors - (processed - plan.zohoToAirtable.length)} errors`);
    }

    // AIRTABLE -> ZOHO UPDATES
    if (plan.airtableToZoho.length > 0) {
      console.log(`\n🔄 Performing ${plan.airtableToZoho.length} Airtable → Zoho updates...`);
      
      // Pre-initialize field mapping and field ID to name mapping once
      await fieldMappingCache.ensureModuleInitialized(this.moduleName);
      const currentModuleFieldMapping = fieldMappingCache.getFieldMapping(this.moduleName);
      const airtableFieldIdToNameMap = await getFieldIdToNameMapping(null, this.moduleName);
      
      const UPDATE_BATCH_SIZE = 100; // Batch size for parallel updates
      const totalBatches = Math.ceil(plan.airtableToZoho.length / UPDATE_BATCH_SIZE);
      
      for (let i = 0; i < plan.airtableToZoho.length; i += UPDATE_BATCH_SIZE) {
        const batchNumber = Math.floor(i / UPDATE_BATCH_SIZE) + 1;
        const batch = plan.airtableToZoho.slice(i, Math.min(i + UPDATE_BATCH_SIZE, plan.airtableToZoho.length));
        
        console.log(`   📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} records)...`);
        
        // Process batch in parallel
        const batchPromises = batch.map(async (item, batchIndex) => {
          const currentIndex = processed + batchIndex + 1;
          const ageMinutes = isNaN(item.timeDiff) ? 'unknown' : Math.round(item.timeDiff / 60000);
          if (this.verbose) {
            console.log(`     [${currentIndex}/${totalOperations}] Syncing Airtable ${item.airtable.id} → Zoho ${this.moduleName} ${item.zoho.id} (Airtable ${ageMinutes}min newer)`);
          }
          
          try {
            if (!currentModuleFieldMapping) {
              console.error(`     [BulkSync][${this.moduleName}] No field mapping. Cannot sync Airtable ${item.airtable.id}.`);
              return { success: false, airtableId: item.airtable.id, error: 'No field mapping' };
            }
            
            const changedFieldsInfoArray = [];
            for (const [zohoKey, mapping] of Object.entries(currentModuleFieldMapping)) {
              if (shouldIgnoreField(mapping.zoho, 'zoho') || shouldIgnoreField(mapping.airtable, 'airtable')) continue;

              let airtableFieldName = mapping.airtable;
              if (mapping.airtable.startsWith('fld') && airtableFieldIdToNameMap && airtableFieldIdToNameMap[mapping.airtable]) {
                airtableFieldName = airtableFieldIdToNameMap[mapping.airtable];
              } else if (mapping.airtable.startsWith('fld')) {
                if(this.verbose) console.warn(`     [BulkSync][${this.moduleName}] Airtable field name for ID ${mapping.airtable} not found in map. Skipping field ${mapping.zoho} for A->Z sync of ${item.airtable.id}`);
                continue; 
              }
              
              const airtableValue = item.airtable.data.fields[airtableFieldName];
              const zohoValue = item.zoho.data[mapping.zoho];
              
              // Compare normalized values to check if they're actually different
              const normalizedAirtableValue = this.normalizeValue(airtableValue);
              const normalizedZohoValue = this.normalizeValue(zohoValue);
              
              if (normalizedAirtableValue !== normalizedZohoValue) {
                if (this.verbose) {
                  console.log(`     Field '${mapping.zoho}' differs: Airtable='${normalizedAirtableValue}' vs Zoho='${normalizedZohoValue}'`);
                }
                changedFieldsInfoArray.push({
                  fieldName: airtableFieldName, 
                  currentValue: airtableValue
                });
              }
            }
            
            if (changedFieldsInfoArray.length === 0) {
              if (this.verbose) {
                console.log(`     No field changes detected between Airtable ${item.airtable.id} and Zoho ${item.zoho.id}. Skipping sync.`);
              }
              return { success: true, airtableId: item.airtable.id, skipped: true };
            }

            // Pass the known Zoho record ID to avoid unnecessary lookups
            const success = await handleAirtableRecordUpdate(item.airtable.id, changedFieldsInfoArray, this.moduleName, item.zoho.id);
            return { success, airtableId: item.airtable.id };
          } catch (e) {
            console.error(`     Error syncing Airtable ${item.airtable.id} to Zoho ${item.zoho.id}: ${e.message}`);
            if (this.verbose) console.error(e.stack);
            return { success: false, airtableId: item.airtable.id, error: e.message };
          }
        });
        
        // Wait for batch to complete
        const results = await Promise.all(batchPromises);
        
        // Update stats and show batch results
        const batchSuccesses = results.filter(r => r.success && !r.skipped).length;
        const batchSkipped = results.filter(r => r.skipped).length;
        const batchErrors = results.filter(r => !r.success).length;
        
        results.forEach(result => {
          if (result.skipped) {
            this.stats.skipped++;
          } else if (result.success) {
            this.stats.airtableToZoho++;
          } else {
            this.stats.errors++;
          }
        });
        
        console.log(`   ✅ Batch ${batchNumber} complete: ${batchSuccesses} success, ${batchSkipped} skipped, ${batchErrors} errors`);
        
        processed += batch.length;
        
        // Add small delay between batches to avoid rate limits
        if (i + UPDATE_BATCH_SIZE < plan.airtableToZoho.length) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      }
      
      console.log(`   ✅ All Airtable → Zoho updates complete: ${this.stats.airtableToZoho} successful, ${this.stats.skipped} skipped, ${this.stats.errors - (this.stats.zohoToAirtable ? 0 : this.stats.errors)} errors`);
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

  // Helper method to check if an Airtable field is an ID mapping field (class-specific logic)
  isIdMappingField(airtableFieldName) {
    return airtableFieldName === this.zohoCrmIdAirtableField || 
           airtableFieldName === this.airtableIdFieldFromMapping;
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
    } else if (args[i] === '--no-delete') {
      options.noDelete = true;
    } else if (args[i] === '--verbose' || args[i] === '-v') {
      options.verbose = true;
    } else if (args[i] === '--help' || args[i] === '-h') {
      console.log(`
🚀 Bulk Sync Usage:
  node bulkSync.js [--module ModuleName] [--zoho-id ZohoRecordId] [--dry-run] [--no-delete] [--verbose | -v] [--help | -h]

Options:
  --module <ModuleName>    Specify the Zoho module to sync (e.g., Leads, Contacts). Defaults to 'Leads'.
  --zoho-id <ZohoRecordId> Sync only this specific Zoho record ID for the given module.
  --dry-run                Preview changes without executing.
  --no-delete              Skip deletion checks and operations.
  --verbose, -v            Detailed logging.
  --help, -h               Show this help message.

Examples:
  node bulkSync.js                                       # Full sync for Leads module
  node bulkSync.js --module Contacts                     # Full sync for Contacts module
  node bulkSync.js --module Leads --zoho-id 123456789    # Sync specific Lead ID 123456789
  node bulkSync.js --module Accounts --dry-run           # Dry run for Accounts module
  node bulkSync.js --module Partners --no-delete        # Sync Partners without deletion checks
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
