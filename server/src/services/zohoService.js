const axios = require('axios');
const { loadZohoConfig, saveZohoConfig, filterIgnoredFields, FIELD_MAPPING } = require('../config/config');

// Refresh Zoho access token
async function refreshZohoToken(config) {
  try {
    console.log('🔄 Refreshing Zoho access token...');
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
    
    // Save the updated config
    const saved = saveZohoConfig(config);
    if (!saved) {
      console.error('❌ Failed to save updated Zoho config');
      return false;
    }
    
    console.log('✅ Zoho access token refreshed successfully!');
    return true;
  } catch (error) {
    console.error('❌ Error refreshing Zoho access token:', error.response?.data || error.message);
    return false;
  }
}

// Fetch lead details from Zoho with automatic token refresh
async function getLeadDetails(leadId, config = null) {
  if (!config) {
    config = loadZohoConfig();
    if (!config) {
      console.error('Failed to load Zoho config');
      return null;
    }
  }

  try {
    // Check if token needs refresh
    if (Date.now() >= config.tokenExpiry) {
      const refreshed = await refreshZohoToken(config);
      if (!refreshed) {
        console.error('Failed to refresh token');
        return null;
      }
    }
    
    const response = await axios.get(
      `${config.apiDomain}/crm/v2/Leads/${leadId}`,
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    return response.data;
  } catch (error) {
    // If token is invalid, try refreshing once
    if (error.response?.data?.code === 'INVALID_TOKEN') {
      console.log('🔄 Token invalid, attempting refresh...');
      const refreshed = await refreshZohoToken(config);
      if (refreshed) {
        // Retry the request with new token
        try {
          const response = await axios.get(
            `${config.apiDomain}/crm/v2/Leads/${leadId}`,
            {
              headers: {
                'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
                'Content-Type': 'application/json'
              }
            }
          );
          return response.data;
        } catch (retryError) {
          console.error('Error after token refresh:', retryError.response?.data || retryError.message);
          return null;
        }
      }
    }
    
    console.error('Error fetching lead details:', error.response?.data || error.message);
    return null;
  }
}

// Update Zoho lead with automatic token refresh
async function updateZohoLead(leadId, fieldUpdates, config = null) {
  if (!config) {
    config = loadZohoConfig();
    if (!config) {
      console.error('Failed to load Zoho config');
      return null;
    }
  }

  try {
    // Check if token needs refresh
    if (Date.now() >= config.tokenExpiry) {
      const refreshed = await refreshZohoToken(config);
      if (!refreshed) {
        console.error('Failed to refresh token');
        return null;
      }
    }
    
    const response = await axios.put(
      `${config.apiDomain}/crm/v2/Leads/${leadId}`,
      {
        data: [fieldUpdates]
      },
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    console.log('✅ Zoho lead updated successfully');
    return response.data;
  } catch (error) {
    // If token is invalid, try refreshing once
    if (error.response?.data?.code === 'INVALID_TOKEN') {
      console.log('🔄 Token invalid, attempting refresh...');
      const refreshed = await refreshZohoToken(config);
      if (refreshed) {
        // Retry the request with new token
        try {
          const response = await axios.put(
            `${config.apiDomain}/crm/v2/Leads/${leadId}`,
            {
              data: [fieldUpdates]
            },
            {
              headers: {
                'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
                'Content-Type': 'application/json'
              }
            }
          );
          console.log('✅ Zoho lead updated successfully');
          return response.data;
        } catch (retryError) {
          console.error('Error after token refresh:', retryError.response?.data || retryError.message);
          return null;
        }
      }
    }
    
    console.error('❌ Error updating Zoho lead:', error.response?.data || error.message);
    return null;
  }
}

// Show only changed fields with filtering
function getChangedFields(leadId, leadData, affectedFieldsArray) {
  console.log('\n=== Changed Fields Only ===');
  
  if (!affectedFieldsArray || affectedFieldsArray.length === 0) {
    console.log('No affected fields data available');
    return;
  }
  
  // Find the affected fields for this specific lead ID
  const leadAffectedFields = affectedFieldsArray.find(item => item[leadId]);
  
  if (!leadAffectedFields || !leadAffectedFields[leadId]) {
    console.log('No affected fields found for this lead');
    return;
  }
  
  const allChangedFields = leadAffectedFields[leadId];
  // Filter out ignored fields
  const changedFieldNames = filterIgnoredFields(allChangedFields, 'zoho');
  
  console.log(`All fields that changed: ${allChangedFields.join(', ')}`);
  console.log(`Syncable fields that changed: ${changedFieldNames.join(', ')}`);
  
  if (allChangedFields.length !== changedFieldNames.length) {
    const { shouldIgnoreField } = require('../config/config');
    const ignoredFields = allChangedFields.filter(field => shouldIgnoreField(field, 'zoho'));
    console.log(`Ignored fields: ${ignoredFields.join(', ')}`);
  }
  
  // Show the current values of changed fields (excluding ignored ones)
  console.log('\n=== Current Values of Syncable Changed Fields ===');
  changedFieldNames.forEach(fieldName => {
    const fieldValue = leadData[fieldName];
    console.log(`${fieldName}: ${fieldValue || 'null'}`);
  });
  
  return {
    changedFields: changedFieldNames,
    allChangedFields: allChangedFields,
    currentValues: changedFieldNames.reduce((acc, field) => {
      acc[field] = leadData[field];
      return acc;
    }, {})
  };
}

// Extract key lead information for logging
function logLeadDetails(lead) {
  console.log('\n=== Key Lead Information ===');
  console.log('Name:', lead.Full_Name || lead.First_Name + ' ' + lead.Last_Name);
  console.log('Email:', lead.Email);
  console.log('Phone:', lead.Phone);
  console.log('Company:', lead.Company);
  console.log('Lead Status:', lead.Lead_Status);
  console.log('Lead Source:', lead.Lead_Source);
  console.log('Modified Time:', lead.Modified_Time);
}

module.exports = {
  refreshZohoToken,
  getLeadDetails,
  updateZohoLead,
  getChangedFields,
  logLeadDetails
};