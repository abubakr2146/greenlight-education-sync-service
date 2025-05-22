const axios = require('axios');
const { loadZohoConfig, saveZohoConfig, filterIgnoredFields, FIELD_MAPPING } = require('../config/config');

// Refresh Zoho access token
async function refreshZohoToken(config) {
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
    
    // Save the updated config
    const saved = saveZohoConfig(config);
    if (!saved) {
      return false;
    }
    
    return true;
  } catch (error) {
    return false;
  }
}

// Fetch lead details from Zoho with automatic token refresh
async function getLeadDetails(leadId, config = null) {
  if (!config) {
    config = loadZohoConfig();
    if (!config) {
      return null;
    }
  }

  try {
    // Check if token needs refresh
    if (Date.now() >= config.tokenExpiry) {
      const refreshed = await refreshZohoToken(config);
      if (!refreshed) {
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
          return null;
        }
      }
    }
    
    return null;
  }
}

// Update Zoho lead with automatic token refresh
async function updateZohoLead(leadId, fieldUpdates, config = null) {
  if (!config) {
    config = loadZohoConfig();
    if (!config) {
      return null;
    }
  }

  try {
    // Check if token needs refresh
    if (Date.now() >= config.tokenExpiry) {
      const refreshed = await refreshZohoToken(config);
      if (!refreshed) {
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
    
    return response.data;
  } catch (error) {
    // If token is invalid, try refreshing once
    if (error.response?.data?.code === 'INVALID_TOKEN') {
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
          return response.data;
        } catch (retryError) {
          return null;
        }
      }
    }
    
    return null;
  }
}

// Show only changed fields with filtering
function getChangedFields(leadId, leadData, affectedFieldsArray) {
  if (!affectedFieldsArray || affectedFieldsArray.length === 0) {
    return;
  }
  
  // Find the affected fields for this specific lead ID
  const leadAffectedFields = affectedFieldsArray.find(item => item[leadId]);
  
  if (!leadAffectedFields || !leadAffectedFields[leadId]) {
    return;
  }
  
  const allChangedFields = leadAffectedFields[leadId];
  // Filter out ignored fields
  const changedFieldNames = filterIgnoredFields(allChangedFields, 'zoho');
  
  if (allChangedFields.length !== changedFieldNames.length) {
    const { shouldIgnoreField } = require('../config/config');
    const ignoredFields = allChangedFields.filter(field => shouldIgnoreField(field, 'zoho'));
  }
  
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
  // Lead details logged
}

module.exports = {
  refreshZohoToken,
  getLeadDetails,
  updateZohoLead,
  getChangedFields,
  logLeadDetails
};