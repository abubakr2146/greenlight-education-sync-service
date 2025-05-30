/**
 * Zoho Webhook Setup Script
 * 
 * This script helps with:
 * 1. Creating webhooks for all Zoho CRM modules
 * 2. Listing existing webhooks
 * 3. Deleting webhooks
 * 4. Testing webhook notifications
 * 
 * Usage: 
 * - Create webhooks: node zoho-webhook-setup.js create
 * - List webhooks: node zoho-webhook-setup.js list
 * - Delete webhooks: node zoho-webhook-setup.js delete
 * - Delete all webhooks: node zoho-webhook-setup.js delete-all
 */

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const readline = require('readline');

// Config file paths
const ZOHO_CONFIG_PATH = path.join(__dirname, 'zoho-config.json');
const WEBHOOK_CONFIG_PATH = path.join(__dirname, 'webhook-config.json');

// Default webhook config template
const DEFAULT_WEBHOOK_CONFIG = {
  webhookUrl: '',
  webhookSecret: '',
  createdWebhooks: []
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
 * Load Zoho configuration
 * @returns {Object} - The configuration object
 */
function loadZohoConfig() {
  try {
    if (fs.existsSync(ZOHO_CONFIG_PATH)) {
      const configData = fs.readFileSync(ZOHO_CONFIG_PATH, 'utf8');
      return JSON.parse(configData);
    }
  } catch (error) {
    console.error('Error loading Zoho config:', error.message);
  }
  
  console.error('Zoho config not found. Please run zoho-setup.js first.');
  process.exit(1);
}

/**
 * Load webhook configuration
 * @returns {Object} - The webhook configuration object
 */
function loadWebhookConfig() {
  try {
    if (fs.existsSync(WEBHOOK_CONFIG_PATH)) {
      const configData = fs.readFileSync(WEBHOOK_CONFIG_PATH, 'utf8');
      return JSON.parse(configData);
    }
  } catch (error) {
    console.log('Creating new webhook config...');
  }
  
  return { ...DEFAULT_WEBHOOK_CONFIG };
}

/**
 * Save webhook configuration
 * @param {Object} config - The webhook configuration object to save
 */
function saveWebhookConfig(config) {
  try {
    fs.writeFileSync(WEBHOOK_CONFIG_PATH, JSON.stringify(config, null, 2));
    console.log('Webhook config saved.');
  } catch (error) {
    console.error('Error saving webhook config:', error.message);
  }
}

/**
 * Refresh access token if expired
 * @param {Object} zohoConfig - The Zoho configuration object
 * @returns {Object} - Updated configuration with new access token
 */
async function refreshAccessToken(zohoConfig) {
  if (Date.now() < zohoConfig.tokenExpiry) {
    return zohoConfig;
  }

  console.log('Refreshing access token...');
  
  try {
    const response = await axios.post('https://accounts.zoho.com/oauth/v2/token', null, {
      params: {
        refresh_token: zohoConfig.refreshToken,
        client_id: zohoConfig.clientId,
        client_secret: zohoConfig.clientSecret,
        grant_type: 'refresh_token'
      }
    });
    
    zohoConfig.accessToken = response.data.access_token;
    zohoConfig.tokenExpiry = Date.now() + (response.data.expires_in * 1000);
    
    // Save updated config
    fs.writeFileSync(ZOHO_CONFIG_PATH, JSON.stringify(zohoConfig, null, 2));
    console.log('Access token refreshed.');
    
    return zohoConfig;
  } catch (error) {
    console.error('Error refreshing token:', error.response?.data || error.message);
    throw error;
  }
}

/**
 * Get all available modules from Zoho
 * @param {Object} zohoConfig - The Zoho configuration object
 * @returns {Array} - List of module objects
 */
async function getAllModules(zohoConfig) {
  try {
    const response = await axios.get(
      `${zohoConfig.apiDomain}/crm/v2/settings/modules`,
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${zohoConfig.accessToken}`
        }
      }
    );
    
    console.log('Total modules received:', response.data.modules.length);
    
    // Show all API-supported modules for debugging
    const apiSupportedModules = response.data.modules.filter(module => module.api_supported);
    console.log('API-supported modules:', apiSupportedModules.map(m => m.api_name).join(', '));
    
    // Filter for webhook-capable modules
    // Include all API-supported modules except certain system modules
    const excludedModules = ['Activities', 'Feeds', 'Notes', 'Attachments', 'Actions_Performed'];
    const modules = response.data.modules.filter(module => 
      module.api_supported && 
      module.webhook_supported !== false && // Some modules explicitly don't support webhooks
      !excludedModules.includes(module.api_name)
    );
    
    console.log('Webhook-capable modules:', modules.length);
    
    return modules;
  } catch (error) {
    console.error('Error fetching modules:', error.response?.data || error.message);
    throw error;
  }
}

/**
 * Create a webhook for a specific module
 * @param {Object} zohoConfig - The Zoho configuration object
 * @param {Object} webhookConfig - The webhook configuration object
 * @param {Object} module - The module object
 * @returns {Object} - Created webhook details
 */
async function createWebhook(zohoConfig, webhookConfig, module) {
  try {
    // Generate unique channel ID - must be numeric (bigint)
    // Add random component to ensure uniqueness when creating multiple webhooks quickly
    const channelId = (Date.now() * 1000 + Math.floor(Math.random() * 1000)).toString();
    
    // Define events to watch - using individual events instead of .all
    const events = [
      `${module.api_name}.create`,
      `${module.api_name}.edit`,
      `${module.api_name}.delete`
    ];
    
    // Calculate expiry time (max 1 day for instant notifications)
    const expiryDate = new Date(Date.now() + 23 * 60 * 60 * 1000); // 23 hours to be safe
    
    // Get timezone offset in format ±HH:MM
    const offset = -expiryDate.getTimezoneOffset();
    const offsetHours = Math.floor(Math.abs(offset) / 60);
    const offsetMinutes = Math.abs(offset) % 60;
    const offsetSign = offset >= 0 ? '+' : '-';
    const timezoneOffset = `${offsetSign}${String(offsetHours).padStart(2, '0')}:${String(offsetMinutes).padStart(2, '0')}`;
    
    // Format: YYYY-MM-DDTHH:MM:SS±HH:MM
    const channelExpiry = expiryDate.toISOString().slice(0, -5) + timezoneOffset;
    
    console.log(`Creating webhook for ${module.api_name} with expiry: ${channelExpiry}`);
    
    const webhookData = {
      watch: [
        {
          channel_id: channelId,
          events: events,
          channel_expiry: channelExpiry,
          token: webhookConfig.webhookSecret || "zoho_webhook_token",
          notify_url: webhookConfig.webhookUrl
        }
      ]
    };
    
    const response = await axios.post(
      `${zohoConfig.apiDomain}/crm/v2/actions/watch`,
      webhookData,
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${zohoConfig.accessToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    if (response.data.watch && response.data.watch[0].status === 'success') {
      const webhook = response.data.watch[0].details;
      console.log(`✅ Webhook created for ${module.api_name} (Channel: ${webhook.channel_id})`);
      return {
        module: module.api_name,
        channelId: webhook.channel_id,
        resourceUri: webhook.resource_uri,
        channelExpiry: webhook.channel_expiry,
        events: webhook.events
      };
    } else {
      console.error(`❌ Failed to create webhook for ${module.api_name}:`, response.data);
      // Log more details about the error
      if (response.data.watch && response.data.watch[0].details) {
        console.error('   Error details:', JSON.stringify(response.data.watch[0].details, null, 2));
      }
      return null;
    }
  } catch (error) {
    console.error(`❌ Error creating webhook for ${module.api_name}:`, error.response?.data || error.message);
    if (error.response?.data?.watch?.[0]?.details) {
      console.error('   Error details:', JSON.stringify(error.response.data.watch[0].details, null, 2));
    }
    return null;
  }
}

/**
 * List all existing webhooks
 * @param {Object} zohoConfig - The Zoho configuration object
 * @returns {Array} - List of webhooks
 */
async function listWebhooks(zohoConfig) {
  try {
    const response = await axios.get(
      `${zohoConfig.apiDomain}/crm/v2/actions/watch`,
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${zohoConfig.accessToken}`
        }
      }
    );
    
    if (response.data.watch) {
      return response.data.watch;
    }
    return [];
  } catch (error) {
    console.error('Error listing webhooks:', error.response?.data || error.message);
    return [];
  }
}

/**
 * Delete a specific webhook
 * @param {Object} zohoConfig - The Zoho configuration object
 * @param {string} channelId - The channel ID to delete
 * @returns {boolean} - Success status
 */
async function deleteWebhook(zohoConfig, channelId) {
  try {
    const response = await axios.delete(
      `${zohoConfig.apiDomain}/crm/v2/actions/watch`,
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${zohoConfig.accessToken}`,
          'Content-Type': 'application/json'
        },
        data: {
          watch: [
            {
              channel_id: channelId
            }
          ]
        }
      }
    );
    
    if (response.data.watch && response.data.watch[0].status === 'success') {
      console.log(`✅ Deleted webhook: ${channelId}`);
      return true;
    } else {
      console.error(`❌ Failed to delete webhook: ${channelId}`);
      return false;
    }
  } catch (error) {
    console.error(`Error deleting webhook ${channelId}:`, error.response?.data || error.message);
    return false;
  }
}

/**
 * Create webhooks for all modules
 */
async function createWebhooksForAllModules() {
  let zohoConfig = loadZohoConfig();
  let webhookConfig = loadWebhookConfig();
  
  // Get webhook URL if not configured
  if (!webhookConfig.webhookUrl) {
    console.log('\nWebhook URL is required. This should be a publicly accessible URL that will receive notifications.');
    console.log('For testing, you can use services like ngrok, webhook.site, or requestbin.\n');
    webhookConfig.webhookUrl = await prompt('Enter your webhook URL: ');
    
    // Optional: Set webhook secret for verification
    const secret = await prompt('Enter webhook secret (optional, press Enter to skip): ');
    if (secret) {
      webhookConfig.webhookSecret = secret;
    }
    
    saveWebhookConfig(webhookConfig);
  }
  
  // Refresh token if needed
  zohoConfig = await refreshAccessToken(zohoConfig);
  
  // Get all modules
  console.log('\nFetching available modules...');
  const modules = await getAllModules(zohoConfig);
  console.log(`Found ${modules.length} modules.\n`);
  
  // Ask which modules to create webhooks for
  console.log('Available modules:');
  modules.forEach((module, index) => {
    console.log(`${index + 1}. ${module.api_name} (${module.plural_label})`);
  });
  
  const moduleSelection = await prompt('\nEnter module numbers to create webhooks for (comma-separated, or "all" for all modules): ');
  
  let selectedModules = [];
  if (moduleSelection.toLowerCase() === 'all') {
    selectedModules = modules;
  } else {
    const indices = moduleSelection.split(',').map(s => parseInt(s.trim()) - 1);
    selectedModules = indices.filter(i => i >= 0 && i < modules.length).map(i => modules[i]);
  }
  
  if (selectedModules.length === 0) {
    console.log('No modules selected.');
    return;
  }
  
  console.log(`\nCreating webhooks for ${selectedModules.length} modules...`);
  
  // Create webhooks
  const createdWebhooks = [];
  for (const module of selectedModules) {
    const webhook = await createWebhook(zohoConfig, webhookConfig, module);
    if (webhook) {
      createdWebhooks.push(webhook);
    }
  }
  
  // Update webhook config with created webhooks
  webhookConfig.createdWebhooks = [
    ...webhookConfig.createdWebhooks,
    ...createdWebhooks
  ];
  saveWebhookConfig(webhookConfig);
  
  console.log(`\n✅ Created ${createdWebhooks.length} webhooks successfully!`);
  
  if (createdWebhooks.length > 0) {
    console.log('\nWebhook Details:');
    createdWebhooks.forEach(wh => {
      console.log(`- ${wh.module}: Channel ${wh.channelId} (expires: ${new Date(wh.channelExpiry).toLocaleString()})`);
    });
  }
}

/**
 * List all existing webhooks
 */
async function listAllWebhooks() {
  let zohoConfig = loadZohoConfig();
  zohoConfig = await refreshAccessToken(zohoConfig);
  
  console.log('\nFetching existing webhooks...');
  const webhooks = await listWebhooks(zohoConfig);
  
  if (webhooks.length === 0) {
    console.log('No webhooks found.');
    return;
  }
  
  console.log(`\nFound ${webhooks.length} webhooks:\n`);
  webhooks.forEach((webhook, index) => {
    console.log(`${index + 1}. Channel: ${webhook.channel_id}`);
    console.log(`   Module: ${webhook.module}`);
    console.log(`   Events: ${webhook.events.join(', ')}`);
    console.log(`   Notify URL: ${webhook.notify_url}`);
    console.log(`   Expires: ${new Date(webhook.channel_expiry).toLocaleString()}`);
    console.log('');
  });
}

/**
 * Delete specific webhooks
 */
async function deleteWebhooks() {
  let zohoConfig = loadZohoConfig();
  zohoConfig = await refreshAccessToken(zohoConfig);
  
  // First, list existing webhooks
  const webhooks = await listWebhooks(zohoConfig);
  
  if (webhooks.length === 0) {
    console.log('No webhooks found to delete.');
    return;
  }
  
  console.log('\nExisting webhooks:');
  webhooks.forEach((webhook, index) => {
    console.log(`${index + 1}. ${webhook.module} - Channel: ${webhook.channel_id}`);
  });
  
  const selection = await prompt('\nEnter webhook numbers to delete (comma-separated): ');
  const indices = selection.split(',').map(s => parseInt(s.trim()) - 1);
  const selectedWebhooks = indices.filter(i => i >= 0 && i < webhooks.length).map(i => webhooks[i]);
  
  if (selectedWebhooks.length === 0) {
    console.log('No webhooks selected.');
    return;
  }
  
  console.log(`\nDeleting ${selectedWebhooks.length} webhooks...`);
  
  for (const webhook of selectedWebhooks) {
    await deleteWebhook(zohoConfig, webhook.channel_id);
  }
  
  // Update local config
  const webhookConfig = loadWebhookConfig();
  webhookConfig.createdWebhooks = webhookConfig.createdWebhooks.filter(
    wh => !selectedWebhooks.some(sw => sw.channel_id === wh.channelId)
  );
  saveWebhookConfig(webhookConfig);
}

/**
 * Delete all webhooks
 */
async function deleteAllWebhooks() {
  let zohoConfig = loadZohoConfig();
  zohoConfig = await refreshAccessToken(zohoConfig);
  
  const confirm = await prompt('Are you sure you want to delete ALL webhooks? (yes/no): ');
  if (confirm.toLowerCase() !== 'yes') {
    console.log('Cancelled.');
    return;
  }
  
  console.log('\nFetching all webhooks...');
  const webhooks = await listWebhooks(zohoConfig);
  
  if (webhooks.length === 0) {
    console.log('No webhooks found to delete.');
    return;
  }
  
  console.log(`Deleting ${webhooks.length} webhooks...`);
  
  for (const webhook of webhooks) {
    await deleteWebhook(zohoConfig, webhook.channel_id);
  }
  
  // Clear local config
  const webhookConfig = loadWebhookConfig();
  webhookConfig.createdWebhooks = [];
  saveWebhookConfig(webhookConfig);
  
  console.log('\n✅ All webhooks deleted successfully!');
}

/**
 * Main function
 */
async function main() {
  const args = process.argv.slice(2);
  const command = args[0] || 'help';
  
  try {
    switch (command) {
      case 'create':
        await createWebhooksForAllModules();
        break;
        
      case 'list':
        await listAllWebhooks();
        break;
        
      case 'delete':
        await deleteWebhooks();
        break;
        
      case 'delete-all':
        await deleteAllWebhooks();
        break;
        
      case 'help':
      default:
        console.log('\nZoho Webhook Setup Script\n');
        console.log('Usage:');
        console.log('  node zoho-webhook-setup.js create      - Create webhooks for modules');
        console.log('  node zoho-webhook-setup.js list        - List all existing webhooks');
        console.log('  node zoho-webhook-setup.js delete      - Delete specific webhooks');
        console.log('  node zoho-webhook-setup.js delete-all  - Delete all webhooks');
        console.log('  node zoho-webhook-setup.js help        - Show this help message');
        break;
    }
  } catch (error) {
    console.error('\nError:', error.message);
  } finally {
    rl.close();
  }
}

main();