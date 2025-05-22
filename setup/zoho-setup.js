/**
 * Zoho Setup Script
 * 
 * This script helps with:
 * 1. Generating an OAuth URL to obtain an authorization code
 * 2. Exchanging the authorization code for access and refresh tokens
 * 3. Setting up a webhook notification channel in Zoho
 * 4. Refreshing tokens when needed
 * 5. Saving all credentials to a local config file
 * 
 * Usage: 
 * - First run: node zoho-setup.js setup
 * - Refresh tokens: node zoho-setup.js refresh
 * - Setup webhook: node zoho-setup.js webhook
 */

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const readline = require('readline');

// Config file path
const CONFIG_PATH = path.join(__dirname, 'zoho-config.json');

// Default config template
const DEFAULT_CONFIG = {
  clientId: '',
  clientSecret: '',
  redirectUri: 'http://localhost:3000/oauth/callback',
  refreshToken: '',
  accessToken: '',
  tokenExpiry: 0,
  modules: ['Contacts'], // Add other modules as needed
  webhookUrl: '',
  channelId: 'zoho_airtable_sync_channel',
  apiDomain: 'https://www.zohoapis.com', // Change for different regions if needed
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
 * Initial setup to get client ID, client secret and generate OAuth URL
 */
async function initialSetup() {
  console.log('\n=== Zoho API Setup ===\n');
  console.log('This script will guide you through setting up Zoho API credentials.\n');
  
  let config = loadConfig();
  
  // Get client ID and secret
  if (!config.clientId) {
    console.log('1. Go to https://api-console.zoho.com/ and create a Server-based Application');
    console.log('2. Use the following as your Redirect URI: ' + config.redirectUri);
    console.log('3. After creating the application, you\'ll get a Client ID and Client Secret\n');
    
    config.clientId = await prompt('Enter your Client ID: ');
    config.clientSecret = await prompt('Enter your Client Secret: ');
    
    // Save immediately in case process is interrupted
    saveConfig(config);
  }
  
  // Get modules to sync
  const modulesInput = await prompt(`Enter Zoho modules to sync (comma-separated, default: ${config.modules.join(',')}): `);
  if (modulesInput.trim()) {
    config.modules = modulesInput.split(',').map(m => m.trim());
  }
  
  // Generate OAuth URL
  const scopes = [
    'ZohoCRM.modules.ALL',
    'ZohoCRM.settings.ALL',
    'ZohoCRM.notifications.ALL'
  ];
  
  const oauthUrl = `https://accounts.zoho.com/oauth/v2/auth?scope=${encodeURIComponent(scopes.join(','))}&client_id=${config.clientId}&response_type=code&access_type=offline&redirect_uri=${encodeURIComponent(config.redirectUri)}`;
  
  console.log('\n=== OAuth Authorization ===\n');
  console.log('1. Open the following URL in your browser:');
  console.log(oauthUrl);
  console.log('2. Log in to Zoho and authorize the application');
  console.log('3. You\'ll be redirected to a URL that contains a code parameter\n');
  
  // Open the URL in the default browser
  try {
    const open = (await import('open')).default;
    await open(oauthUrl);
  } catch (error) {
    console.log('Could not open browser automatically. Please copy and paste the URL above manually.');
  }
  
  const redirectUrl = await prompt('Enter the full redirect URL you received: ');
  
  // Extract code from URL
  const codeMatch = redirectUrl.match(/code=([^&]+)/);
  if (!codeMatch) {
    console.error('Could not find authorization code in the URL');
    return;
  }
  
  const authCode = codeMatch[1];
  console.log(`\nAuthorization code obtained: ${authCode}`);
  
  // Exchange auth code for tokens
  try {
    const tokenResponse = await axios.post('https://accounts.zoho.com/oauth/v2/token', null, {
      params: {
        grant_type: 'authorization_code',
        client_id: config.clientId,
        client_secret: config.clientSecret,
        code: authCode,
        redirect_uri: config.redirectUri
      }
    });
    
    config.refreshToken = tokenResponse.data.refresh_token;
    config.accessToken = tokenResponse.data.access_token;
    // Calculate expiry time (now + expires_in seconds)
    config.tokenExpiry = Date.now() + (tokenResponse.data.expires_in * 1000);
    
    console.log('\nAccess token and refresh token obtained successfully!');
    
    // Ask for webhook URL
    config.webhookUrl = await prompt('Enter your webhook URL (leave empty to skip webhook setup): ');
    
    // Save the updated config
    saveConfig(config);
    
    // If webhook URL was provided, set up the webhook
    if (config.webhookUrl) {
      await setupWebhook(config);
    }
    
  } catch (error) {
    console.error('Error exchanging auth code for tokens:', error.response?.data || error.message);
  }
}

/**
 * Refresh the access token using the refresh token
 * @param {Object} config - The configuration object
 * @returns {Object} - Updated configuration with new access token
 */
async function refreshAccessToken(config) {
  if (!config.refreshToken) {
    console.error('No refresh token found. Please run the initial setup first.');
    return config;
  }
  
  try {
    const response = await axios.post('https://accounts.zoho.com/oauth/v2/token', null, {
      params: {
        refresh_token: config.refreshToken,
        client_id: config.clientId,
        client_secret: config.clientSecret,
        grant_type: 'refresh_token'
      }
    });
    
    config.accessToken = response.data.access_token;
    config.tokenExpiry = Date.now() + (response.data.expires_in * 1000);
    
    console.log('Access token refreshed successfully!');
    saveConfig(config);
    
    return config;
  } catch (error) {
    console.error('Error refreshing access token:', error.response?.data || error.message);
    return config;
  }
}

/**
 * Setup webhook notification channel in Zoho
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
  
  // Make sure we have a valid access token
  if (Date.now() >= config.tokenExpiry) {
    config = await refreshAccessToken(config);
  }
  
  // Prepare the events list based on configured modules
  const events = [];
  for (const module of config.modules) {
    events.push(`${module}.create`, `${module}.edit`, `${module}.delete`);
  }
  
  // Calculate expiry time (7 days from now)
  const expiryDate = new Date();
  expiryDate.setDate(expiryDate.getDate() + 7);
  
  // Generate a unique channel ID based on timestamp
  const channelId = Date.now();
  
  try {
    const response = await axios.post(
      `${config.apiDomain}/crm/v2/actions/watch`,
      {
        watch: [
          {
            channel_id: channelId,
            events: events,
            notify_url: config.webhookUrl,
            token: "zoho_sync_webhook_token"
          }
        ]
      },
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    console.log('Webhook notification channel setup successful!');
    console.log('Channel details:', response.data.watch);
    
    // Update channel ID in case it changed
    if (response.data.watch && response.data.watch[0]) {
      config.channelId = response.data.watch[0].channel_id || channelId;
      saveConfig(config);
    } else {
      config.channelId = channelId;
      saveConfig(config);
    }
    
  } catch (error) {
    console.error('Error setting up webhook notification channel:', error.response?.data || error.message);
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
      
    case 'refresh':
      await refreshAccessToken(config);
      break;
      
    case 'webhook':
      await setupWebhook(config);
      break;
      
    case 'help':
    default:
      console.log('\nZoho Setup Script Usage:');
      console.log('  node zoho-setup.js setup    - Run initial setup');
      console.log('  node zoho-setup.js refresh  - Refresh access token');
      console.log('  node zoho-setup.js webhook  - Setup webhook notification channel');
      console.log('  node zoho-setup.js help     - Show this help message');
      break;
  }
  
  rl.close();
}

main();