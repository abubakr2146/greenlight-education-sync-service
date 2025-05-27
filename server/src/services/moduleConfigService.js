const axios = require('axios');
const { loadAirtableConfig } = require('../config/config');

/**
 * Module Configuration Service
 * 
 * Manages dynamic module configurations by fetching module details
 * from Airtable's Zoho Modules table and providing module-specific
 * settings for the sync service.
 */

// Cache for module configurations
const moduleCache = new Map();

// Known table IDs (these should ideally come from config)
const ZOHO_MODULES_TABLE_ID = 'tbl2HlEPyESvXUXHN';
const ZOHO_FIELDS_TABLE_ID = 'tbl0JfUjWhV4TvLz2';

/**
 * Fetch all module configurations from Airtable
 * @param {Object} config - Airtable configuration object
 * @returns {Promise<Array>} Array of module records
 */
async function fetchAllModules(config = null) {
  if (!config) {
    config = loadAirtableConfig();
    if (!config) {
      throw new Error('No Airtable configuration found');
    }
  }

  try {
    let allModules = [];
    let offset = null;

    do {
      const params = {};
      if (offset) {
        params.offset = offset;
      }

      const response = await axios.get(
        `${config.apiUrl}/${config.baseId}/${ZOHO_MODULES_TABLE_ID}`,
        {
          headers: {
            'Authorization': `Bearer ${config.apiToken}`,
            'Content-Type': 'application/json'
          },
          params: params
        }
      );

      const pageRecords = response.data.records || [];
      allModules.push(...pageRecords);
      offset = response.data.offset;

    } while (offset);

    // Cache the modules
    allModules.forEach(module => {
      const apiName = module.fields['api_name'];
      if (apiName) {
        moduleCache.set(apiName, module);
      }
    });

    return allModules;
  } catch (error) {
    console.error('Error fetching modules:', error.message);
    throw new Error(`Failed to fetch module configurations: ${error.message}`);
  }
}

/**
 * Get module configuration by API name
 * @param {string} moduleName - The API name of the module (e.g., 'Leads', 'Partners')
 * @param {Object} config - Optional Airtable configuration
 * @returns {Promise<Object>} Module configuration object
 */
async function getModuleConfig(moduleName, config = null) {
  // Check cache first
  if (moduleCache.has(moduleName)) {
    return moduleCache.get(moduleName);
  }

  // Fetch from Airtable
  const modules = await fetchAllModules(config);
  const module = modules.find(m => m.fields['api_name'] === moduleName);
  
  if (!module) {
    throw new Error(`Module '${moduleName}' not found in Airtable configuration`);
  }

  return module;
}

/**
 * Get Airtable table configuration for a module
 * @param {string} moduleName - The API name of the module
 * @param {Object} config - Optional Airtable configuration
 * @returns {Promise<Object>} Table configuration with tableId and tableName
 */
async function getModuleTableConfig(moduleName, config = null) {
  const module = await getModuleConfig(moduleName, config);
  
  const tableConfig = {
    tableName: module.fields['Airtable Table Name'] || moduleName,
    tableId: Array.isArray(module.fields['Airtable Table ID']) 
      ? module.fields['Airtable Table ID'][0] 
      : module.fields['Airtable Table ID'],
    zohoModuleName: module.fields['api_name'],
    moduleRecordId: module.id
  };

  // Validate required fields
  if (!tableConfig.tableId) {
    throw new Error(`Module '${moduleName}' does not have an Airtable Table ID configured`);
  }

  return tableConfig;
}

/**
 * Get all available modules
 * @param {Object} config - Optional Airtable configuration
 * @returns {Promise<Array>} Array of module names
 */
async function getAvailableModules(config = null) {
  const modules = await fetchAllModules(config);
  return modules
    .map(m => m.fields['api_name'])
    .filter(name => name); // Filter out any empty names
}

/**
 * Validate if a module exists
 * @param {string} moduleName - The API name of the module
 * @param {Object} config - Optional Airtable configuration
 * @returns {Promise<boolean>} True if module exists
 */
async function isValidModule(moduleName, config = null) {
  try {
    await getModuleConfig(moduleName, config);
    return true;
  } catch (error) {
    return false;
  }
}

/**
 * Get the Zoho module plural name (for API endpoints)
 * @param {string} moduleName - The API name of the module
 * @returns {string} Plural form of the module name
 */
async function getZohoModulePluralName(moduleName) {
  // Check if this is a custom module by looking up the configuration
  try {
    let moduleConfig;
    if (moduleCache.has(moduleName)) {
      moduleConfig = moduleCache.get(moduleName);
    } else {
      // Try to fetch the module config if not in cache
      try {
        moduleConfig = await getModuleConfig(moduleName);
      } catch (configError) {
        // Fall through to standard handling if config lookup fails
        moduleConfig = null;
      }
    }
    
    // If it's a custom module, use the api_name field (not module_name)
    if (moduleConfig && moduleConfig.fields && moduleConfig.fields.module_name && 
        moduleConfig.fields.module_name.startsWith('CustomModule')) {
      return moduleConfig.fields.api_name;
    }
  } catch (error) {
    // Fall through to standard handling if config lookup fails
  }

  // Handle special cases for standard modules
  const pluralMap = {
    'Lead': 'Leads',
    'Leads': 'Leads',
    'Partner': 'Partners',
    'Partners': 'Partners',
    'Contact': 'Contacts',
    'Contacts': 'Contacts',
    'Account': 'Accounts',
    'Accounts': 'Accounts',
    'Deal': 'Deals',
    'Deals': 'Deals',
    'Task': 'Tasks',
    'Tasks': 'Tasks',
    'Event': 'Events',
    'Events': 'Events',
    'Call': 'Calls',
    'Calls': 'Calls',
    'Meeting': 'Meetings',
    'Meetings': 'Meetings',
    'Product': 'Products',
    'Products': 'Products',
    'Quote': 'Quotes',
    'Quotes': 'Quotes',
    'SalesOrder': 'Sales_Orders',
    'Sales_Orders': 'Sales_Orders',
    'PurchaseOrder': 'Purchase_Orders',
    'Purchase_Orders': 'Purchase_Orders',
    'Invoice': 'Invoices',
    'Invoices': 'Invoices',
    'Campaign': 'Campaigns',
    'Campaigns': 'Campaigns',
    'Vendor': 'Vendors',
    'Vendors': 'Vendors',
    'Case': 'Cases',
    'Cases': 'Cases',
    'Solution': 'Solutions',
    'Solutions': 'Solutions',
    'Student': 'Students',
    'Students': 'Students',
    'Calls_Meetings': 'Calls_Meetings'
  };

  // If we have a known plural form, use it
  if (pluralMap[moduleName]) {
    return pluralMap[moduleName];
  }

  // Otherwise, try to pluralize intelligently
  if (moduleName.endsWith('y')) {
    return moduleName.slice(0, -1) + 'ies';
  } else if (moduleName.endsWith('s') || moduleName.endsWith('x') || moduleName.endsWith('ch')) {
    return moduleName + 'es';
  } else {
    return moduleName + 's';
  }
}

/**
 * Clear the module cache
 */
function clearModuleCache() {
  moduleCache.clear();
}

module.exports = {
  fetchAllModules,
  getModuleConfig,
  getModuleTableConfig,
  getAvailableModules,
  isValidModule,
  getZohoModulePluralName,
  clearModuleCache,
  ZOHO_MODULES_TABLE_ID,
  ZOHO_FIELDS_TABLE_ID
};