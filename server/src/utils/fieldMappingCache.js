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
      return;
    }

    this.isInitializing = true;
    
    try {
      await this.refreshCache();
      
      // Start periodic refresh
      this.intervalId = setInterval(() => {
        this.refreshCache();
      }, this.refreshInterval);
      
    } catch (error) {
      // Initialization failed
    } finally {
      this.isInitializing = false;
    }
  }

  async refreshCache() {
    try {
      const config = loadAirtableConfig();
      if (!config) {
        return;
      }

      const fieldMapping = await fetchDynamicFieldMapping(config);
      if (fieldMapping) {
        this.cache = fieldMapping;
        this.lastUpdated = new Date();
      }
    } catch (error) {
      // Refresh failed
    }
  }

  getFieldMapping() {
    if (!this.cache) {
      return null;
    }
    
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
    }
  }
}

// Create singleton instance
const fieldMappingCache = new FieldMappingCache();

module.exports = fieldMappingCache;