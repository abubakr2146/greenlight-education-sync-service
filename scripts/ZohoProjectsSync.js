/**
 * Standalone version of fetchZohoTaskByIdFromAirtable for environments where functions are not selectable
 * Script Properties required:
 * - airtableApiKey
 * - clientIdProjects
 * - clientSecretProjects
 * - refreshTokenProjects
 */

function runStandaloneZohoTaskFetch() {
  const clientId = PropertiesService.getScriptProperties().getProperty('clientIdProjects');
  const clientSecret = PropertiesService.getScriptProperties().getProperty('clientSecretProjects');
  const refreshToken = PropertiesService.getScriptProperties().getProperty('refreshTokenProjects');

  const tokenUrl = `https://accounts.zoho.com/oauth/v2/token?refresh_token=${refreshToken}&client_id=${clientId}&client_secret=${clientSecret}&grant_type=refresh_token`;
  const tokenResponse = UrlFetchApp.fetch(tokenUrl, { method: 'post' });
  const tokenData = JSON.parse(tokenResponse.getContentText());
  if (!tokenData.access_token) throw new Error("Failed to get access token: " + JSON.stringify(tokenData));
  Logger.log("Successfully obtained Zoho Projects access token");
  const accessToken = tokenData.access_token;

  const airtablePAT = PropertiesService.getScriptProperties().getProperty('airtableApiKey');
  const airtableUrl = 'https://api.airtable.com/v0/app4xfaklobVjLusr/tblYqgemNWDIL2Zbv?view=viwVzBUrAoAFdiq2K&maxRecords=1';
  const airtableOptions = {
    method: 'get',
    headers: { Authorization: `Bearer ${airtablePAT}` }
  };
  const airtableResponse = UrlFetchApp.fetch(airtableUrl, airtableOptions);
  const airtableData = JSON.parse(airtableResponse.getContentText());
  const airtableRecord = airtableData.records[0];

  const fields = airtableRecord.fields;
  const taskId = fields['Zoho Projects ID'];
  const projectId = fields['Zoho Projects ID (from Project)'];
  const portalId = 'greenlighteducation791';

  const headers = {
    'Authorization': 'Zoho-oauthtoken ' + accessToken,
    'Content-Type': 'application/json'
  };

  const taskUrl = `https://projectsapi.zoho.com/restapi/portal/${portalId}/projects/${projectId}/tasks/${taskId}/`;
  try {
    const taskResp = UrlFetchApp.fetch(taskUrl, { headers });
    const taskData = JSON.parse(taskResp.getContentText());
    const task = taskData.tasks[0];

    Logger.log("=== Task Fields ===");
    const keys = Object.keys(task);
    Logger.log(`Found ${keys.length} fields in the task`);

    const now = new Date().toISOString();
    const createBatch = [];
    const updateBatch = [];

    keys.forEach(k => {
      const value = task[k];
      Logger.log(`Field: ${k}`);
      Logger.log(`Type: ${typeof value}`);
      Logger.log(`Value: ${JSON.stringify(value)}`);
      Logger.log("---");

      const fieldsPayload = {
        "Field Type": typeof value,
        "JSON": JSON.stringify({ name: k, value }),
        "Last Automation Time": now,
        "api_name": k,
        "Module": ["recyHbidcT8ptu2U5"],
        "Application": ["recXkmU64mdG4S4eM"]
      };

      const encodedApiName = encodeURIComponent(k);
      const filterFormula = `AND(api_name='${k}', FIND('recXkmU64mdG4S4eM', ARRAYJOIN(Application)))`;
      const filterUrl = `https://api.airtable.com/v0/app4xfaklobVjLusr/tbl0JfUjWhV4TvLz2?filterByFormula=${encodeURIComponent(filterFormula)}`;

      const filterResp = UrlFetchApp.fetch(filterUrl, {
        method: 'get',
        headers: { Authorization: `Bearer ${airtablePAT}` }
      });
      const filterData = JSON.parse(filterResp.getContentText());

      if (filterData.records.length > 0) {
        updateBatch.push({
          id: filterData.records[0].id,
          fields: fieldsPayload
        });
      } else {
        createBatch.push({ fields: fieldsPayload });
      }
    });

    if (createBatch.length > 0) {
      for (let i = 0; i < createBatch.length; i += 10) {
        const chunk = createBatch.slice(i, i + 10);
        const createResp = UrlFetchApp.fetch('https://api.airtable.com/v0/app4xfaklobVjLusr/tbl0JfUjWhV4TvLz2', {
          method: 'post',
          contentType: 'application/json',
          headers: { Authorization: `Bearer ${airtablePAT}` },
          payload: JSON.stringify({ records: chunk })
        });
        Logger.log(`Created batch chunk: ${createResp.getContentText()}`);
      }
    }

    if (updateBatch.length > 0) {
      for (let i = 0; i < updateBatch.length; i += 10) {
        const chunk = updateBatch.slice(i, i + 10);
        const updateResp = UrlFetchApp.fetch('https://api.airtable.com/v0/app4xfaklobVjLusr/tbl0JfUjWhV4TvLz2', {
          method: 'patch',
          contentType: 'application/json',
          headers: { Authorization: `Bearer ${airtablePAT}` },
          payload: JSON.stringify({ records: chunk })
        });
        Logger.log(`Updated batch chunk: ${updateResp.getContentText()}`);
      }
    }

  } catch (err) {
    Logger.log(`Error fetching task details: ${err}`);
  }

  // === Fetch Custom Fields Again ===
  try {
    const customFieldsUrl = `https://projectsapi.zoho.com/restapi/portal/${portalId}/projects/${projectId}/tasks/customfields/`;
    const customResp = UrlFetchApp.fetch(customFieldsUrl, { headers });
    const customData = JSON.parse(customResp.getContentText());

    if (customData && customData.customfields) {
      Logger.log("=== Custom Fields ===");
      Logger.log(`Found ${customData.customfields.length} custom fields`);

      customData.customfields.forEach(field => {
        Logger.log(`ID: ${field.id}`);
        Logger.log(`Field: ${field.field_name}`);
        Logger.log(`Label: ${field.label_name}`);
        Logger.log(`Type: ${field.custom_field_type}`);
        Logger.log("---");
      });
    } else {
      Logger.log("No custom fields found");
    }
  } catch (err) {
    Logger.log(`Error fetching custom fields: ${err}`);
  }
}
