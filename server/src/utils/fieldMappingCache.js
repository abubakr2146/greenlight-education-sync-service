const { fetchDynamicFieldMapping } = require('../services/airtableService');
const { loadAirtableConfig } = require('../config/config');

class FieldMappingCache {
  constructor() {
    this.cacheByModule = new Map(); // Cache per module
    this.lastUpdatedByModule = new Map();
    this.refreshInterval = 5 * 60 * 1000; // 5 minutes in milliseconds
    this.intervalIds = new Map(); // Interval per module
    this.isInitializing = new Set(); // Track initialization per module
  }

  async initialize(module = 'Leads') {
    if (this.isInitializing.has(module)) {
      return;
    }

    this.isInitializing.add(module);
    
    try {
      await this.refreshCache(module);
      
      // Clear any existing interval for this module
      if (this.intervalIds.has(module)) {
        clearInterval(this.intervalIds.get(module));
      }
      
      // Start periodic refresh for this module
      const intervalId = setInterval(() => {
        this.refreshCache(module);
      }, this.refreshInterval);
      
      this.intervalIds.set(module, intervalId);
      
    } catch (error) {
      console.error(`Failed to initialize field mapping cache for module ${module}:`, error.message);
    } finally {
      this.isInitializing.delete(module);
    }
  }

  async refreshCache(module = 'Leads') {
    try {
      const config = loadAirtableConfig();
      if (!config) {
        return;
      }

      const fieldMapping = await fetchDynamicFieldMapping(config, module);
      if (fieldMapping) {
        this.cacheByModule.set(module, fieldMapping);
        this.lastUpdatedByModule.set(module, new Date());
        console.log(`Field mapping cache refreshed for module ${module}: ${Object.keys(fieldMapping).length} mappings`);
      }
    } catch (error) {
      console.error(`Failed to refresh field mapping cache for module ${module}:`, error.message);
    }
  }

  getFieldMapping(module = 'Leads') {
    if (!this.cacheByModule.has(module)) {
      console.warn(`No field mapping cache found for module ${module}`);
      return null;
    }
    
    return this.cacheByModule.get(module);
  }

  isReady(module = 'Leads') {
    return this.cacheByModule.has(module) && this.cacheByModule.get(module) !== null;
  }

  getStatus(module = null) {
    if (module) {
      // Return status for specific module
      return {
        initialized: this.cacheByModule.has(module),
        lastUpdated: this.lastUpdatedByModule.get(module) || null,
        mappingCount: this.cacheByModule.has(module) ? Object.keys(this.cacheByModule.get(module)).length : 0,
        refreshInterval: this.refreshInterval,
        module: module
      };
    }
    
    // Return status for all modules
    const moduleStatuses = {};
    for (const [mod, cache] of this.cacheByModule) {
      moduleStatuses[mod] = {
        initialized: true,
        lastUpdated: this.lastUpdatedByModule.get(mod) || null,
        mappingCount: Object.keys(cache).length
      };
    }
    
    return {
      modules: moduleStatuses,
      refreshInterval: this.refreshInterval,
      totalModules: this.cacheByModule.size
    };
  }

  destroyModule(module) {
    // Clear interval for specific module
    if (this.intervalIds.has(module)) {
      clearInterval(this.intervalIds.get(module));
      this.intervalIds.delete(module);
    }
    
    // Clear cache for module
    this.cacheByModule.delete(module);
    this.lastUpdatedByModule.delete(module);
    this.isInitializing.delete(module);
  }

  destroy() {
    // Clear all intervals
    for (const intervalId of this.intervalIds.values()) {
      clearInterval(intervalId);
    }
    
    // Clear all caches
    this.intervalIds.clear();
    this.cacheByModule.clear();
    this.lastUpdatedByModule.clear();
    this.isInitializing.clear();
  }

  // Method to ensure a module is initialized before use
  async ensureModuleInitialized(module = 'Leads') {
    if (!this.isReady(module) && !this.isInitializing.has(module)) {
      await this.initialize(module);
      
      // Wait a bit for cache to populate
      let attempts = 0;
      while (!this.isReady(module) && attempts < 10) {
        await new Promise(resolve => setTimeout(resolve, 500));
        attempts++;
      }
    }
    
    return this.isReady(module);
  }
}

// Create singleton instance
const fieldMappingCache = new FieldMappingCache();

module.exports = fieldMappingCache;