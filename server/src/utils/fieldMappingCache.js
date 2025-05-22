const { fetchDynamicFieldMapping } = require('../services/airtableService');
const { loadAirtableConfig } = require('../config/config');

class FieldMappingCache {
  constructor() {
    this.cache = null;
    this.lastUpdated = null;
    this.refreshInterval = 5 * 60 * 1000; // 5 minutes in milliseconds
    this.intervalId = null;
    this.isInitializing = false;
  }

  async initialize() {
    if (this.isInitializing) {
      console.log('⏳ Field mapping cache initialization already in progress...');
      return;
    }

    this.isInitializing = true;
    console.log('🚀 Initializing field mapping cache...');
    
    try {
      await this.refreshCache();
      
      // Start periodic refresh
      this.intervalId = setInterval(() => {
        this.refreshCache();
      }, this.refreshInterval);
      
      console.log(`✅ Field mapping cache initialized and will refresh every ${this.refreshInterval / 1000 / 60} minutes`);
    } catch (error) {
      console.error('❌ Failed to initialize field mapping cache:', error);
    } finally {
      this.isInitializing = false;
    }
  }

  async refreshCache() {
    console.log('🔄 Refreshing field mapping cache...');
    
    try {
      const config = loadAirtableConfig();
      if (!config) {
        console.error('❌ Failed to load Airtable config for cache refresh');
        return;
      }

      const fieldMapping = await fetchDynamicFieldMapping(config);
      if (fieldMapping) {
        this.cache = fieldMapping;
        this.lastUpdated = new Date();
        console.log(`✅ Field mapping cache refreshed successfully (${Object.keys(fieldMapping).length} mappings)`);
        console.log(`   Last updated: ${this.lastUpdated.toISOString()}`);
      } else {
        console.error('❌ Failed to fetch field mapping for cache refresh');
      }
    } catch (error) {
      console.error('❌ Error refreshing field mapping cache:', error);
    }
  }

  getFieldMapping() {
    if (!this.cache) {
      console.warn('⚠️  Field mapping cache not initialized or empty');
      return null;
    }
    
    console.log(`📋 Using cached field mapping (${Object.keys(this.cache).length} mappings, last updated: ${this.lastUpdated?.toISOString()})`);
    return this.cache;
  }

  isReady() {
    return this.cache !== null;
  }

  getStatus() {
    return {
      initialized: this.cache !== null,
      lastUpdated: this.lastUpdated,
      mappingCount: this.cache ? Object.keys(this.cache).length : 0,
      refreshInterval: this.refreshInterval
    };
  }

  destroy() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
      console.log('🛑 Field mapping cache refresh interval stopped');
    }
  }
}

// Create singleton instance
const fieldMappingCache = new FieldMappingCache();

module.exports = fieldMappingCache;