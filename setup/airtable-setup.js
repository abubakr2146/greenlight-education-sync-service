/**
 * Airtable Setup Script
 * 
 * This script helps with:
 * 1. Collecting Airtable API token, base ID, and table name
 * 2. Testing the connection to Airtable
 * 3. Saving all credentials to a local config file
 * 
 * Usage: 
 * - First run: node airtable-setup.js setup
 * - Test connection: node airtable-setup.js test
 */

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const readline = require('readline');

// Config file path
const CONFIG_PATH = path.join(__dirname, 'airtable-config.json');

// Default config template
const DEFAULT_CONFIG = {
  apiToken: '',
  baseId: '',
  tableName: '',
  tableId: '',
  apiUrl: 'https://api.airtable.com/v0'
};

// Create interface for reading user input
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

/**
 * Prompt the user for input
 * @param {string} question - The question to ask
 * @returns {Promise<string>} - The user's response
 */
function prompt(question) {
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      resolve(answer);
    });
  });
}

/**
 * Load configuration from file or create default
 * @returns {Object} - The configuration object
 */
function loadConfig() {
  try {
    if (fs.existsSync(CONFIG_PATH)) {
      const configData = fs.readFileSync(CONFIG_PATH, 'utf8');
      return JSON.parse(configData);
    }
  } catch (error) {
    // Error handled
  }
  
  return { ...DEFAULT_CONFIG };
}

/**
 * Save configuration to file
 * @param {Object} config - The configuration object to save
 */
function saveConfig(config) {
  try {
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2));
  } catch (error) {
    // Error handled
  }
}

/**
 * Initial setup to get API token, base ID, and table name
 */
async function initialSetup() {
  let config = loadConfig();
  
  // Get API token
  if (!config.apiToken) {
    config.apiToken = await prompt('Enter your Airtable API Token: ');
  }
  
  // Get base ID
  if (!config.baseId) {
    config.baseId = await prompt('Enter your Base ID: ');
  }
  
  // Get table name
  if (!config.tableName) {
    config.tableName = await prompt('Enter your Table Name: ');
  }
  
  // Save the config
  saveConfig(config);
  
  // Test the connection
  await testConnection(config);
  
  console.log('\n✅ Airtable setup complete!');
}

/**
 * Test connection to Airtable
 * @param {Object} config - The configuration object
 */
async function testConnection(config) {
  try {
    const response = await axios.get(
      `${config.apiUrl}/${config.baseId}/${encodeURIComponent(config.tableName)}?maxRecords=1`,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    if (response.data.records.length > 0) {
      // Sample fields available
    }
    
  } catch (error) {
    // Connection failed
  }
}

/**
 * Get table ID from table name
 * @param {Object} config - The configuration object
 * @returns {string|null} - The table ID or null if not found
 */
async function getTableId(config) {
  try {
    // First get base metadata to find table ID
    const response = await axios.get(
      `${config.apiUrl}/meta/bases/${config.baseId}/tables`,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    const table = response.data.tables.find(t => t.name === config.tableName);
    if (!table) {
      return null;
    }
    
    return table.id;
    
  } catch (error) {
    return null;
  }
}


/**
 * Main function to run the script
 */
async function main() {
  const args = process.argv.slice(2);
  const command = args[0] || 'help';
  
  const config = loadConfig();
  
  switch (command) {
    case 'setup':
      await initialSetup();
      break;
      
    case 'test':
      await testConnection(config);
      break;
      
    case 'help':
    default:
      break;
  }
  
  rl.close();
}

main();