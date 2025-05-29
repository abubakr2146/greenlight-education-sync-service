/**
 * Script to sync Zoho CRM modules and their fields into Airtable
 * Updates or inserts each module and its fields with full JSON
 *
 * Script Properties Required:
 * - airtableId
 * 
 * - clientIdCRM
 * - clientSecretCRM
 * - refreshTokenCRM
 */

function syncZohoModulesToAirtable() {
  const airtablePAT = PropertiesService.getScriptProperties().getProperty('airtableApiKey');
  const clientId = PropertiesService.getScriptProperties().getProperty('clientIdCRM');
  const clientSecret = PropertiesService.getScriptProperties().getProperty('clientSecretCRM');
  const refreshToken = PropertiesService.getScriptProperties().getProperty('refreshTokenCRM');

  const airtableBaseId = 'app4xfaklobVjLusr';
  const moduleTableId = 'tbl2HlEPyESvXUXHN';
  const fieldTableId = 'tbl0JfUjWhV4TvLz2';
  const moduleUrl = `https://api.airtable.com/v0/${airtableBaseId}/${moduleTableId}`;
  const fieldUrl = `https://api.airtable.com/v0/${airtableBaseId}/${fieldTableId}`;

  const headers = {
    'Authorization': 'Bearer ' + airtablePAT,
    'Content-Type': 'application/json'
  };

  const headersWithTypecast = {
    'Authorization': 'Bearer ' + airtablePAT,
    'Content-Type': 'application/json'
  };

  console.log('Fetching Zoho access token...');
  const accessToken = getZohoAccessToken(clientId, clientSecret, refreshToken);
  console.log('Fetching modules from Zoho CRM...');
  const modules = getZohoModules(accessToken);
  console.log(`Fetched ${modules.length} modules.`);

  const filterFormulas = modules.map(m => `{api_name}='${m.api_name}'`);
  const existingModuleMap = {};

  // Fetch existing modules in batches
  for (let i = 0; i < filterFormulas.length; i += 10) {
    const batch = filterFormulas.slice(i, i + 10);
    const formula = `OR(${batch.join(',')})`;
    const response = UrlFetchApp.fetch(`${moduleUrl}?filterByFormula=${encodeURIComponent(formula)}&pageSize=100`, { headers });
    const data = JSON.parse(response.getContentText());
    for (const record of data.records) {
      existingModuleMap[record.fields.api_name] = record.id;
    }
  }

  const moduleUpdates = [];
  const moduleCreates = [];
  const timestamp = new Date().toISOString();

  // First pass: Handle modules and collect field data
  const moduleFieldsMap = {}; // Store fields by module
  
  modules.forEach(module => {
    const apiName = module.api_name;
    const moduleJson = JSON.stringify(module);

    const moduleFields = {
      'api_name': apiName,
      'JSON': moduleJson,
      'Last Automation Time': timestamp,
      'Application': ['recQ0e0nQ4kI50kcc']
    };

    // Add to update or create array
    if (existingModuleMap[apiName]) {
      moduleUpdates.push({ id: existingModuleMap[apiName], fields: moduleFields });
    } else {
      moduleCreates.push({ fields: moduleFields });
    }
    
    // Get fields for this module
    const zohoFields = getZohoModuleFields(accessToken, apiName);
    if (zohoFields.length) {
      moduleFieldsMap[apiName] = zohoFields;
    }
  });

  // Process module updates and creates
  if (moduleUpdates.length > 0) {
    console.log(`Updating ${moduleUpdates.length} existing modules...`);
    for (let i = 0; i < moduleUpdates.length; i += 10) {
      const payload = JSON.stringify({ records: moduleUpdates.slice(i, i + 10) });
      UrlFetchApp.fetch(moduleUrl, { method: 'PATCH', headers, payload });
    }
  }

  if (moduleCreates.length > 0) {
    console.log(`Creating ${moduleCreates.length} new modules...`);
    for (let i = 0; i < moduleCreates.length; i += 10) {
      const payload = JSON.stringify({ records: moduleCreates.slice(i, i + 10) });
      const response = UrlFetchApp.fetch(moduleUrl, { method: 'POST', headers, payload });
      const data = JSON.parse(response.getContentText());
      data.records.forEach((rec, idx) => {
        const fields = moduleCreates[i + idx].fields;
        existingModuleMap[fields.api_name] = rec.id;
      });
    }
  }

  // Now process fields by module
  for (const [moduleApiName, fields] of Object.entries(moduleFieldsMap)) {
    const moduleId = existingModuleMap[moduleApiName];
    if (!moduleId) {
      console.warn(`Module ID not found for ${moduleApiName}, skipping fields`);
      continue;
    }
    
    // Create upsertion keys for all fields in this module
    const upsertionKeys = fields.map(field => `${moduleApiName}-${field.api_name}`);
    console.log(`Processing ${upsertionKeys.length} fields for module ${moduleApiName}`);
    
    // Batch lookup existing fields by Upsertion Check in efficient batches
    const existingFieldMap = {};
    
    for (let i = 0; i < upsertionKeys.length; i += 10) {
      const keyBatch = upsertionKeys.slice(i, i + 10);
      const formula = `OR(${keyBatch.map(key => `{Upsertion Check}='${key}'`).join(',')})`;      
      try {
        const response = UrlFetchApp.fetch(`${fieldUrl}?filterByFormula=${encodeURIComponent(formula)}`, { headers });
        const data = JSON.parse(response.getContentText());
        
        for (const record of data.records) {
          if (record.fields['Upsertion Check']) {
            existingFieldMap[record.fields['Upsertion Check']] = record.id;
          }
        }
      } catch (error) {
        console.warn(`Error fetching fields with upsertion keys: ${error.message}`);
        // Continue with next batch if one fails
      }
    }
    
    // Search for matching fields in tblLcwcFbjLyQXdl6 by UI name
    const matchingFieldsMap = searchForMatchingFields(fields, moduleApiName, headers);
    
    // Prepare updates and creates for this module's fields
    const fieldUpdates = [];
    const fieldCreates = [];
    
    fields.forEach(field => {
      const fieldApiName = field.api_name;
      const fieldJson = JSON.stringify(field);
      const upsertionKey = `${moduleApiName}-${fieldApiName}`;
      
      // Extract the UI name from the field data
      const uiName = field.display_label || field.field_label || fieldApiName;
      
      // Extract the Zoho field type
      const zohoFieldType = field.data_type || '';
      
      if (existingFieldMap[upsertionKey]) {
        // Field exists - update timestamp, JSON, UI Name, and Zoho Field Type
        fieldUpdates.push({
          id: existingFieldMap[upsertionKey],
          fields: {
            'Last Automation Time': timestamp,
            'JSON': fieldJson,
            'UI Name': uiName,
            'Zoho Field Type': zohoFieldType
          }
        });
      } else {
        // Check if we found a matching field in tblLcwcFbjLyQXdl6
        const matchingFieldId = matchingFieldsMap[uiName];
        
        if (matchingFieldId) {
          // Link to existing field instead of creating new
          fieldCreates.push({
            fields: {
              'api_name': fieldApiName, // Keep original API name
              'JSON': fieldJson,
              'Last Automation Time': timestamp,
              'Application': ['recQ0e0nQ4kI50kcc'],
              'Module': [moduleId],
              'Upsertion Check': upsertionKey,
              'UI Name': uiName,
              'Zoho Field Type': zohoFieldType,
              'Field': [matchingFieldId] // Link to the matching field
            }
          });
        } else {
          // Field doesn't exist - create new with UI name and field type
          fieldCreates.push({
            fields: {
              'api_name': fieldApiName, // Keep original API name
              'JSON': fieldJson,
              'Last Automation Time': timestamp,
              'Application': ['recQ0e0nQ4kI50kcc'],
              'Module': [moduleId],
              'Upsertion Check': upsertionKey,
              'UI Name': uiName,
              'Zoho Field Type': zohoFieldType
            }
          });
        }
      }
    });
    
    // Process updates for this module
    if (fieldUpdates.length > 0) {
      console.log(`Updating ${fieldUpdates.length} existing fields for module ${moduleApiName}...`);
      for (let i = 0; i < fieldUpdates.length; i += 10) {
        const payload = JSON.stringify({ 
          records: fieldUpdates.slice(i, i + 10),
          typecast: true
        });
        UrlFetchApp.fetch(fieldUrl, { method: 'PATCH', headers: headersWithTypecast, payload });
      }
    }
    
    // Process creates for this module
    if (fieldCreates.length > 0) {
      console.log(`Creating ${fieldCreates.length} new fields for module ${moduleApiName}...`);
      for (let i = 0; i < fieldCreates.length; i += 10) {
        const payload = JSON.stringify({ 
          records: fieldCreates.slice(i, i + 10),
          typecast: true
        });
        UrlFetchApp.fetch(fieldUrl, { method: 'POST', headers: headersWithTypecast, payload });
      }
    }
  }

  console.log('Module and field sync complete.');
}

function getZohoAccessToken(clientId, clientSecret, refreshToken) {
  const tokenUrl = `https://accounts.zoho.com/oauth/v2/token?refresh_token=${refreshToken}&client_id=${clientId}&client_secret=${clientSecret}&grant_type=refresh_token`;
  const response = UrlFetchApp.fetch(tokenUrl, { method: 'POST' });
  const json = JSON.parse(response.getContentText());
  if (!json.access_token) throw new Error('Zoho access token fetch failed');
  return json.access_token;
}

function getZohoModules(accessToken) {
  const url = 'https://www.zohoapis.com/crm/v2/settings/modules';
  const headers = {
    'Authorization': `Zoho-oauthtoken ${accessToken}`
  };
  const response = UrlFetchApp.fetch(url, { headers });
  const json = JSON.parse(response.getContentText());
  return json.modules || [];
}

function getZohoModuleFields(accessToken, moduleApiName) {
  const url = `https://www.zohoapis.com/crm/v2/settings/fields?module=${moduleApiName}`;
  const headers = {
    'Authorization': `Zoho-oauthtoken ${accessToken}`
  };
  try {
    const response = UrlFetchApp.fetch(url, { headers });
    const json = JSON.parse(response.getContentText());
    return json.fields || [];
  } catch (error) {
    console.warn(`Failed to fetch fields for module '${moduleApiName}': ${error.message}`);
    return [];
  }
}

function searchForMatchingFields(fields, moduleApiName, headers) {
  const lookupTableId = 'tblLcwcFbjLyQXdl6';
  const airtableBaseId = 'app4xfaklobVjLusr';
  const lookupUrl = `https://api.airtable.com/v0/${airtableBaseId}/${lookupTableId}`;
  
  const matchingFieldsMap = {};
  
  // Extract unique UI names from the fields
  const uiNames = [...new Set(fields.map(field => 
    field.display_label || field.field_label || field.api_name
  ))];
  
  console.log(`Searching for ${uiNames.length} field names in lookup table for module ${moduleApiName}`);
  
  // Search in batches to avoid URL length limits
  for (let i = 0; i < uiNames.length; i += 10) {
    const namesBatch = uiNames.slice(i, i + 10);
    
    // Create filter formula to search for fields with matching names and table
    const nameConditions = namesBatch.map(name => `{Name}='${name.replace(/'/g, "\\'")}'`);
    const tableCondition = `{Table}='${moduleApiName}'`;
    const formula = `AND(${tableCondition},OR(${nameConditions.join(',')}))`;
    
    try {
      const response = UrlFetchApp.fetch(`${lookupUrl}?filterByFormula=${encodeURIComponent(formula)}`, { headers });
      const data = JSON.parse(response.getContentText());
      
      // Map the found records by their Name field
      for (const record of data.records) {
        if (record.fields.Name && record.fields.Table === moduleApiName) {
          matchingFieldsMap[record.fields.Name] = record.id;
          console.log(`Found matching field: ${record.fields.Name} -> ${record.id}`);
        }
      }
    } catch (error) {
      console.warn(`Error searching for matching fields: ${error.message}`);
      // Continue with next batch if one fails
    }
  }
  
  return matchingFieldsMap;
}