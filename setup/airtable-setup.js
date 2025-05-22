/**
 * Airtable Setup Script
 * 
 * This script helps with:
 * 1. Collecting Airtable API token, base ID, and table name
 * 2. Setting up webhook notifications in Airtable
 * 3. Testing the connection to Airtable
 * 4. Saving all credentials to a local config file
 * 
 * Usage: 
 * - First run: node airtable-setup.js setup
 * - Setup webhook: node airtable-setup.js webhook
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
  webhookUrl: '',
  webhookId: '',
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
    console.error('Error reading config file:', error.message);
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
    console.log(`Configuration saved to ${CONFIG_PATH}`);
  } catch (error) {
    console.error('Error saving config file:', error.message);
  }
}

/**
 * Initial setup to get API token, base ID, and table name
 */
async function initialSetup() {
  console.log('\\n=== Airtable API Setup ===\\n');
  console.log('This script will guide you through setting up Airtable API credentials.\\n');
  
  let config = loadConfig();
  
  // Get API token
  if (!config.apiToken) {
    console.log('1. Go to https://airtable.com/create/tokens and create a personal access token');
    console.log('2. Make sure to give it the following scopes:');
    console.log('   - data.records:read');
    console.log('   - data.records:write');
    console.log('   - schema.bases:read');
    console.log('   - webhook:manage');
    console.log('3. Copy the token\\n');
    
    config.apiToken = await prompt('Enter your Airtable API Token: ');
  }
  
  // Get base ID
  if (!config.baseId) {
    console.log('\\n1. Go to your Airtable base');
    console.log('2. The base ID is in the URL: https://airtable.com/[BASE_ID]/...');
    console.log('3. It starts with "app" followed by alphanumeric characters\\n');
    
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
  
  // Ask for webhook URL
  config.webhookUrl = await prompt('Enter your webhook URL (leave empty to skip webhook setup): ');
  
  if (config.webhookUrl) {
    saveConfig(config);
    await setupWebhook(config);
  }
}

/**
 * Test connection to Airtable
 * @param {Object} config - The configuration object
 */
async function testConnection(config) {
  console.log('\\n=== Testing Airtable Connection ===');
  
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
    
    console.log('✅ Connection successful!');
    console.log(`Found table "${config.tableName}" with ${response.data.records.length > 0 ? 'records' : 'no records'}`);
    
    if (response.data.records.length > 0) {
      console.log('Sample record fields:', Object.keys(response.data.records[0].fields));
    }
    
  } catch (error) {
    console.error('❌ Connection failed:', error.response?.data || error.message);
    console.log('Please check your API token, base ID, and table name.');
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
      console.error(`Table "${config.tableName}" not found in base`);
      console.log('Available tables:', response.data.tables.map(t => t.name).join(', '));
      return null;
    }
    
    console.log(`Found table "${config.tableName}" with ID: ${table.id}`);
    return table.id;
    
  } catch (error) {
    console.error('Error getting table ID:', error.response?.data || error.message);
    return null;
  }
}

/**
 * Setup webhook in Airtable
 * @param {Object} config - The configuration object
 */
async function setupWebhook(config) {
  if (!config.webhookUrl) {
    config.webhookUrl = await prompt('Enter your webhook URL: ');
    if (!config.webhookUrl) {
      console.log('Webhook URL is required to set up notifications.');
      return;
    }
  }
  
  console.log('\\n=== Setting up Airtable Webhook ===');
  
  // Get table ID first
  const tableId = await getTableId(config);
  if (!tableId) {
    return;
  }
  
  // Create new webhook with proper payload
  try {
    const webhookData = {
      notificationUrl: config.webhookUrl,
      specification: {
        options: {
          filters: {
            dataTypes: ['tableData'],
            recordChangeScope: tableId
          }
        }
      }
    };
    
    console.log('Creating webhook with payload:', JSON.stringify(webhookData, null, 2));
    
    const response = await axios.post(
      `${config.apiUrl}/bases/${config.baseId}/webhooks`,
      webhookData,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    console.log('✅ Webhook created successfully!');
    console.log('Webhook ID:', response.data.id);
    console.log('Webhook will notify on changes to table:', config.tableName);
    console.log('Expiration time:', response.data.expirationTime);
    
    // Save webhook ID to config
    config.webhookId = response.data.id;
    config.tableId = tableId; // Save table ID for future use
    saveConfig(config);
    
  } catch (error) {
    console.error('❌ Error creating webhook:', error.response?.data || error.message);
    
    if (error.response?.status === 422) {
      console.log('\\nTip: Make sure your personal access token has the following scopes:');
      console.log('- data.records:read');
      console.log('- data.records:write'); 
      console.log('- schema.bases:read');
      console.log('- webhook:manage');
    }
  }
}

/**
 * Delete webhook
 * @param {Object} config - The configuration object
 */
async function deleteWebhook(config) {
  if (!config.webhookId) {
    console.log('No webhook ID found in config.');
    return;
  }
  
  try {
    await axios.delete(
      `${config.apiUrl}/bases/${config.baseId}/webhooks/${config.webhookId}`,
      {
        headers: {
          'Authorization': `Bearer ${config.apiToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    console.log('✅ Webhook deleted successfully!');
    config.webhookId = '';
    saveConfig(config);
    
  } catch (error) {
    console.error('❌ Error deleting webhook:', error.response?.data || error.message);
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
      
    case 'webhook':
      await setupWebhook(config);
      break;
      
    case 'delete-webhook':
      await deleteWebhook(config);
      break;
      
    case 'help':
    default:
      console.log('\\nAirtable Setup Script Usage:');
      console.log('  node airtable-setup.js setup         - Run initial setup');
      console.log('  node airtable-setup.js test          - Test connection to Airtable');
      console.log('  node airtable-setup.js webhook       - Setup webhook notification');
      console.log('  node airtable-setup.js delete-webhook - Delete existing webhook');
      console.log('  node airtable-setup.js help          - Show this help message');
      break;
  }
  
  rl.close();
}

main();