/**
 * Zoho Setup Script
 * 
 * This script helps with:
 * 1. Generating an OAuth URL to obtain an authorization code
 * 2. Exchanging the authorization code for access and refresh tokens
 * 3. Refreshing tokens when needed
 * 4. Saving all credentials to a local config file
 * 
 * Usage: 
 * - First run: node zoho-setup.js setup
 * - Refresh tokens: node zoho-setup.js refresh
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
    // Error loading config
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
    // Error saving config
  }
}

/**
 * Initial setup to get client ID, client secret and generate OAuth URL
 */
async function initialSetup() {
  let config = loadConfig();
  
  // Get client ID and secret
  if (!config.clientId) {
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
  
  // Open the URL in the default browser
  try {
    const open = (await import('open')).default;
    await open(oauthUrl);
  } catch (error) {
    // Could not open browser
  }
  
  const redirectUrl = await prompt('Enter the full redirect URL you received: ');
  
  // Extract code from URL
  const codeMatch = redirectUrl.match(/code=([^&]+)/);
  if (!codeMatch) {
    return;
  }
  
  const authCode = codeMatch[1];
  
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
    
    // Save the updated config
    saveConfig(config);
    
    console.log('\n✅ Zoho setup complete!');
    
  } catch (error) {
    // Error exchanging tokens
  }
}

/**
 * Refresh the access token using the refresh token
 * @param {Object} config - The configuration object
 * @returns {Object} - Updated configuration with new access token
 */
async function refreshAccessToken(config) {
  if (!config.refreshToken) {
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
    
    saveConfig(config);
    
    return config;
  } catch (error) {
    return config;
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
      
    case 'help':
    default:
      break;
  }
  
  rl.close();
}

main();