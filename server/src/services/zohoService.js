const axios = require('axios');
const { loadZohoConfig, saveZohoConfig, filterIgnoredFields, FIELD_MAPPING } = require('../config/config');
const { getZohoModulePluralName } = require('./moduleConfigService');

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

// ========== MODULE-AWARE FUNCTIONS ==========

// Fetch record details from Zoho with automatic token refresh (module-aware)
async function getRecordDetails(recordId, module = 'Leads', config = null) {
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
    
    const modulePlural = await getZohoModulePluralName(module);
    const response = await axios.get(
      `${config.apiDomain}/crm/v2/${modulePlural}/${recordId}`,
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
          const modulePlural = await getZohoModulePluralName(module);
          const response = await axios.get(
            `${config.apiDomain}/crm/v2/${modulePlural}/${recordId}`,
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

// Update Zoho record with automatic token refresh (module-aware)
async function updateZohoRecord(recordId, fieldUpdates, module = 'Leads', config = null) {
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
    
    const modulePlural = await getZohoModulePluralName(module);
    const response = await axios.put(
      `${config.apiDomain}/crm/v2/${modulePlural}/${recordId}`,
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
          const modulePlural = await getZohoModulePluralName(module);
          const response = await axios.put(
            `${config.apiDomain}/crm/v2/${modulePlural}/${recordId}`,
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

// Create Zoho record (module-aware)
async function createZohoRecord(recordData, module = 'Leads', config = null) {
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
    
    const modulePlural = await getZohoModulePluralName(module);
    const response = await axios.post(
      `${config.apiDomain}/crm/v2/${modulePlural}`,
      {
        data: [recordData]
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
          const modulePlural = await getZohoModulePluralName(module);
          const response = await axios.post(
            `${config.apiDomain}/crm/v2/${modulePlural}`,
            {
              data: [recordData]
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

// Search Zoho records by criteria (module-aware)
async function searchZohoRecords(criteria, module = 'Leads', config = null) {
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
    
    const modulePlural = await getZohoModulePluralName(module);
    const response = await axios.get(
      `${config.apiDomain}/crm/v2/${modulePlural}/search`,
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
          'Content-Type': 'application/json'
        },
        params: criteria
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
          const modulePlural = await getZohoModulePluralName(module);
          const response = await axios.get(
            `${config.apiDomain}/crm/v2/${modulePlural}/search`,
            {
              headers: {
                'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
                'Content-Type': 'application/json'
              },
              params: criteria
            }
          );
          return response.data;
        } catch (retryError) {
          return null;
        }
      }
    }
    
    // Return null for other errors including NO_CONTENT
    return null;
  }
}

// Get records modified since a timestamp (module-aware)
async function getRecordsModifiedSince(sinceTimestamp, module = 'Leads', config = null) {
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
    
    const sinceDate = new Date(sinceTimestamp).toISOString();
    const modulePlural = await getZohoModulePluralName(module);
    
    const response = await axios.get(
      `${config.apiDomain}/crm/v2/${modulePlural}`,
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
          'Content-Type': 'application/json',
          'If-Modified-Since': sinceDate
        },
        params: {
          fields: 'id,Modified_Time,Created_Time',
          sort_by: 'Modified_Time',
          sort_order: 'desc'
        }
      }
    );
    
    return response.data;
  } catch (error) {
    if (error.response?.status === 304) {
      // No modifications since the given timestamp
      return { data: [] };
    }
    
    // If token is invalid, try refreshing once
    if (error.response?.data?.code === 'INVALID_TOKEN') {
      const refreshed = await refreshZohoToken(config);
      if (refreshed) {
        // Retry the request with new token
        try {
          const sinceDate = new Date(sinceTimestamp).toISOString();
          const modulePlural = await getZohoModulePluralName(module);
          
          const response = await axios.get(
            `${config.apiDomain}/crm/v2/${modulePlural}`,
            {
              headers: {
                'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
                'Content-Type': 'application/json',
                'If-Modified-Since': sinceDate
              },
              params: {
                fields: 'id,Modified_Time,Created_Time',
                sort_by: 'Modified_Time',
                sort_order: 'desc'
              }
            }
          );
          
          return response.data;
        } catch (retryError) {
          if (retryError.response?.status === 304) {
            return { data: [] };
          }
          return null;
        }
      }
    }
    
    return null;
  }
}

// Get multiple record details by IDs (module-aware)
async function getMultipleRecordDetails(recordIds, module = 'Leads', config = null) {
  if (!recordIds || recordIds.length === 0) {
    return { data: [] };
  }
  
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
    
    const modulePlural = await getZohoModulePluralName(module);
    const idsParam = recordIds.join(',');
    
    const response = await axios.get(
      `${config.apiDomain}/crm/v2/${modulePlural}`,
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
          'Content-Type': 'application/json'
        },
        params: {
          ids: idsParam
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
          const modulePlural = await getZohoModulePluralName(module);
          const idsParam = recordIds.join(',');
          
          const response = await axios.get(
            `${config.apiDomain}/crm/v2/${modulePlural}`,
            {
              headers: {
                'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
                'Content-Type': 'application/json'
              },
              params: {
                ids: idsParam
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

// Delete Zoho record (module-aware)
async function deleteZohoRecord(recordId, module = 'Leads', config = null) {
  if (!config) {
    config = loadZohoConfig();
    if (!config) {
      return false;
    }
  }

  try {
    // Check if token needs refresh
    if (Date.now() >= config.tokenExpiry) {
      const refreshed = await refreshZohoToken(config);
      if (!refreshed) {
        return false;
      }
    }
    
    const modulePlural = await getZohoModulePluralName(module);
    const response = await axios.delete(
      `${config.apiDomain}/crm/v2/${modulePlural}/${recordId}`,
      {
        headers: {
          'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    // Check if deletion was successful
    if (response.data && response.data.data) {
      const result = response.data.data[0];
      return result.code === 'SUCCESS';
    }
    
    return false;
  } catch (error) {
    // If token is invalid, try refreshing once
    if (error.response?.data?.code === 'INVALID_TOKEN') {
      const refreshed = await refreshZohoToken(config);
      if (refreshed) {
        // Retry the request with new token
        try {
          const modulePlural = await getZohoModulePluralName(module);
          const response = await axios.delete(
            `${config.apiDomain}/crm/v2/${modulePlural}/${recordId}`,
            {
              headers: {
                'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
                'Content-Type': 'application/json'
              }
            }
          );
          
          if (response.data && response.data.data) {
            const result = response.data.data[0];
            return result.code === 'SUCCESS';
          }
          
          return false;
        } catch (retryError) {
          return false;
        }
      }
    }
    
    return false;
  }
}

// ========== LEGACY FUNCTIONS FOR BACKWARD COMPATIBILITY ==========

// Legacy function for backward compatibility
async function getLeadDetails(leadId, config = null) {
  return getRecordDetails(leadId, 'Leads', config);
}

// Legacy function for backward compatibility
async function updateZohoLead(leadId, fieldUpdates, config = null) {
  return updateZohoRecord(leadId, fieldUpdates, 'Leads', config);
}

// Legacy function for backward compatibility
async function createZohoLead(leadData, config = null) {
  return createZohoRecord(leadData, 'Leads', config);
}

// Legacy function for backward compatibility
async function searchLeadsByPhone(phone, config = null) {
  const criteria = {
    criteria: `(Phone:equals:${phone})`
  };
  return searchZohoRecords(criteria, 'Leads', config);
}

// Legacy function for backward compatibility
async function searchLeadsByEmail(email, config = null) {
  const criteria = {
    criteria: `(Email:equals:${email})`
  };
  return searchZohoRecords(criteria, 'Leads', config);
}

// Legacy function for backward compatibility
async function searchLeadsByAirtableId(airtableId, config = null) {
  const criteria = {
    criteria: `(Airtable_Record_ID:equals:${airtableId})`
  };
  return searchZohoRecords(criteria, 'Leads', config);
}

// Legacy function for backward compatibility
async function getLeadsModifiedSince(sinceTimestamp, config = null) {
  return getRecordsModifiedSince(sinceTimestamp, 'Leads', config);
}

// Legacy function for backward compatibility
async function getMultipleLeadDetails(leadIds, config = null) {
  return getMultipleRecordDetails(leadIds, 'Leads', config);
}

// Legacy function for backward compatibility
async function deleteZohoLead(leadId, config = null) {
  return deleteZohoRecord(leadId, 'Leads', config);
}

// ========== BULK OPERATIONS ==========

// Bulk create Zoho records (max 100 per request)
// IMPORTANT: recordsData should be array of record objects with all required fields
async function createZohoRecordsBulk(recordsData, module = 'Leads', config = null) {
  if (!recordsData || recordsData.length === 0) return { success: [], errors: [] };
  
  if (!config) {
    config = loadZohoConfig();
    if (!config) {
      console.log(`❌ No Zoho config found`);
      return { success: [], errors: [] };
    }
  }
  
  // Check if token needs refresh
  if (Date.now() >= config.tokenExpiry) {
    const refreshed = await refreshZohoToken(config);
    if (!refreshed) {
      console.log(`❌ Failed to refresh Zoho token`);
      return { success: [], errors: [] };
    }
  }
  
  const results = [];
  const errors = [];
  const batchSize = 100; // Zoho API limit
  const modulePlural = await getZohoModulePluralName(module);
  
  for (let i = 0; i < recordsData.length; i += batchSize) {
    const batch = recordsData.slice(i, i + batchSize);
    
    try {
      const response = await axios.post(
        `${config.apiDomain}/crm/v2/${modulePlural}`,
        { data: batch },
        {
          headers: {
            'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
            'Content-Type': 'application/json'
          }
        }
      );
      
      if (response.data && response.data.data) {
        // Process each record result
        response.data.data.forEach((result, index) => {
          if (result.code === 'SUCCESS') {
            results.push({
              ...result,
              originalData: batch[index]
            });
          } else {
            errors.push({
              record: batch[index],
              error: result.message || result.code,
              details: result
            });
          }
        });
      }
    } catch (error) {
      console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, error.message);
      if (error.response) {
        console.error(`❌ Response:`, JSON.stringify(error.response.data, null, 2));
      }
      
      // If token is invalid, try refreshing once
      if (error.response?.data?.code === 'INVALID_TOKEN') {
        const refreshed = await refreshZohoToken(config);
        if (refreshed) {
          // Retry this batch
          try {
            const retryResponse = await axios.post(
              `${config.apiDomain}/crm/v2/${modulePlural}`,
              { data: batch },
              {
                headers: {
                  'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
                  'Content-Type': 'application/json'
                }
              }
            );
            
            if (retryResponse.data && retryResponse.data.data) {
              retryResponse.data.data.forEach((result, index) => {
                if (result.code === 'SUCCESS') {
                  results.push({
                    ...result,
                    originalData: batch[index]
                  });
                } else {
                  errors.push({
                    record: batch[index],
                    error: result.message || result.code,
                    details: result
                  });
                }
              });
            }
          } catch (retryError) {
            errors.push({
              batch: Math.floor(i/batchSize) + 1,
              error: retryError.message,
              records: batch
            });
          }
        } else {
          errors.push({
            batch: Math.floor(i/batchSize) + 1,
            error: 'Token refresh failed',
            records: batch
          });
        }
      } else {
        errors.push({
          batch: Math.floor(i/batchSize) + 1,
          error: error.message,
          records: batch
        });
      }
    }
  }
  
  return { success: results, errors };
}

// Bulk update Zoho records (max 100 per request)
// IMPORTANT: updates should be array of objects with 'id' field and other fields to update
async function updateZohoRecordsBulk(updates, module = 'Leads', config = null) {
  if (!updates || updates.length === 0) return { success: [], errors: [] };
  
  if (!config) {
    config = loadZohoConfig();
    if (!config) {
      console.log(`❌ No Zoho config found`);
      return { success: [], errors: [] };
    }
  }
  
  // Check if token needs refresh
  if (Date.now() >= config.tokenExpiry) {
    const refreshed = await refreshZohoToken(config);
    if (!refreshed) {
      console.log(`❌ Failed to refresh Zoho token`);
      return { success: [], errors: [] };
    }
  }
  
  const results = [];
  const errors = [];
  const batchSize = 100; // Zoho API limit
  const modulePlural = await getZohoModulePluralName(module);
  
  for (let i = 0; i < updates.length; i += batchSize) {
    const batch = updates.slice(i, i + batchSize);
    
    try {
      const response = await axios.put(
        `${config.apiDomain}/crm/v2/${modulePlural}`,
        { data: batch },
        {
          headers: {
            'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
            'Content-Type': 'application/json'
          }
        }
      );
      
      if (response.data && response.data.data) {
        // Process each record result
        response.data.data.forEach((result, index) => {
          if (result.code === 'SUCCESS') {
            results.push({
              ...result,
              originalData: batch[index]
            });
          } else {
            errors.push({
              record: batch[index],
              error: result.message || result.code,
              details: result
            });
          }
        });
      }
    } catch (error) {
      console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, error.message);
      if (error.response) {
        console.error(`❌ Response:`, JSON.stringify(error.response.data, null, 2));
      }
      
      // If token is invalid, try refreshing once
      if (error.response?.data?.code === 'INVALID_TOKEN') {
        const refreshed = await refreshZohoToken(config);
        if (refreshed) {
          // Retry this batch
          try {
            const retryResponse = await axios.put(
              `${config.apiDomain}/crm/v2/${modulePlural}`,
              { data: batch },
              {
                headers: {
                  'Authorization': `Zoho-oauthtoken ${config.accessToken}`,
                  'Content-Type': 'application/json'
                }
              }
            );
            
            if (retryResponse.data && retryResponse.data.data) {
              retryResponse.data.data.forEach((result, index) => {
                if (result.code === 'SUCCESS') {
                  results.push({
                    ...result,
                    originalData: batch[index]
                  });
                } else {
                  errors.push({
                    record: batch[index],
                    error: result.message || result.code,
                    details: result
                  });
                }
              });
            }
          } catch (retryError) {
            errors.push({
              batch: Math.floor(i/batchSize) + 1,
              error: retryError.message,
              records: batch
            });
          }
        } else {
          errors.push({
            batch: Math.floor(i/batchSize) + 1,
            error: 'Token refresh failed',
            records: batch
          });
        }
      } else {
        errors.push({
          batch: Math.floor(i/batchSize) + 1,
          error: error.message,
          records: batch
        });
      }
    }
  }
  
  return { success: results, errors };
}

module.exports = {
  // Core functions
  refreshZohoToken,
  getZohoModulePluralName, // Added for export
  
  // Module-aware functions
  getRecordDetails,
  updateZohoRecord,
  createZohoRecord,
  searchZohoRecords,
  getRecordsModifiedSince,
  getMultipleRecordDetails,
  deleteZohoRecord,
  
  // Bulk operations
  createZohoRecordsBulk,
  updateZohoRecordsBulk,
  
  // Legacy functions for backward compatibility
  getLeadDetails,
  updateZohoLead,
  createZohoLead,
  searchLeadsByPhone,
  searchLeadsByEmail,
  searchLeadsByAirtableId,
  getLeadsModifiedSince,
  getMultipleLeadDetails,
  deleteZohoLead
};
