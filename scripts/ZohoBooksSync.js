/**
 * Fetches and inserts sample record fields into Airtable for various Zoho Books entities
 * Script Properties required:
 * - clientIdBooks
 * - clientSecretBooks
 * - refreshTokenBooks
 * - airtableApiKey
 */

const ZOHO_ORGANIZATION_ID = '732022817';
const AIRTABLE_BASE_ID = 'app4xfaklobVjLusr';
const AIRTABLE_TABLE_ID = 'tbl0JfUjWhV4TvLz2';
const AIRTABLE_VIEW_ID = 'viw19wmgmu9FDbr5m';
const APPLICATION_ID = 'recUwrBtC1DZBT0CL';

const MODULE_IDS = {
  'Items': 'recLyOhvlDHb2Ktjc',
  'Customers': 'recOQrAsUgMKR6Mug',
  'Vendors': 'recB08BDONjq07eaA',
  'Expenses': 'rec4gKgznaPROR0pW',
  'Bills': 'rec0f4gTitGUnt4rF',
  'Invoices': 'recM6fYI3Qqes2bZA'
};

function getZohoBooksAccessToken() {
  const clientId = PropertiesService.getScriptProperties().getProperty('clientIdBooks');
  const clientSecret = PropertiesService.getScriptProperties().getProperty('clientSecretBooks');
  const refreshToken = PropertiesService.getScriptProperties().getProperty('refreshTokenBooks');

  const url = `https://accounts.zoho.com/oauth/v2/token?refresh_token=${refreshToken}&client_id=${clientId}&client_secret=${clientSecret}&grant_type=refresh_token`;

  const response = UrlFetchApp.fetch(url, { method: 'POST' });
  const data = JSON.parse(response.getContentText());

  if (!data.access_token) throw new Error("Failed to get access token: " + JSON.stringify(data));
  return data.access_token;
}

function fetchFieldsFromZohoBooks() {
  const accessToken = getZohoBooksAccessToken();
  const headers = {
    'Authorization': 'Zoho-oauthtoken ' + accessToken,
    'Content-Type': 'application/json'
  };

  const airtableToken = PropertiesService.getScriptProperties().getProperty('airtableApiKey');
  const airtableHeaders = {
    'Authorization': 'Bearer ' + airtableToken,
    'Content-Type': 'application/json'
  };

  const entities = [
    { name: 'Items', url: `https://www.zohoapis.com/books/v3/items?per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'items' },
    { name: 'Customers', url: `https://www.zohoapis.com/books/v3/contacts?type=customer&per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'contacts' },
    { name: 'Vendors', url: `https://www.zohoapis.com/books/v3/contacts?type=vendor&per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'contacts' },
    { name: 'Expenses', url: `https://www.zohoapis.com/books/v3/expenses?per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'expenses' },
    { name: 'Bills', url: `https://www.zohoapis.com/books/v3/bills?per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'bills' },
    { name: 'Invoices', url: `https://www.zohoapis.com/books/v3/invoices?per_page=1&organization_id=${ZOHO_ORGANIZATION_ID}`, key: 'invoices' }
  ];

  entities.forEach(entity => {
    try {
      const res = UrlFetchApp.fetch(entity.url, { headers });
      const data = JSON.parse(res.getContentText());

      if (data[entity.key] && data[entity.key].length > 0) {
        const record = data[entity.key][0];
        Logger.log(`Adding ${Object.keys(record).length} fields for ${entity.name}`);

        const timestamp = new Date().toISOString();
        const fieldPayloads = Object.keys(record).map(field => {
          const value = record[field];
          const isCustom = typeof field === 'string' && field.startsWith('cf_');
          let fieldMeta = null;
          let fieldId = null;

          if (isCustom && value && typeof value === 'object') {
            fieldMeta = value;
            fieldId = value.id || null;
          }

          return {
            fields: {
              "Application": [APPLICATION_ID],
              "api_name": field,
              "Module": MODULE_IDS[entity.name] ? [MODULE_IDS[entity.name]] : [],
              "field_id": fieldId || '',
              "JSON": JSON.stringify(fieldMeta || {}),
              "Last Automation Time": timestamp,
              "Upsertion Check": `${entity.name} - ${field}`
            }
          };
        });

        // For proper upserting, we need to fetch all records for this module
        // and then match on the Upsertion Check field in memory
        let existingRecords = [];
        try {
          // Instead of filtering by module ID (which might still get too many records),
          // fetch records using a FIND() operation on the entity name part of Upsertion Check
          const filterFormula = encodeURIComponent(`FIND("${entity.name} - ", {Upsertion Check})=1`);
          const existingUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}?filterByFormula=${filterFormula}&fields%5B%5D=Upsertion+Check`;
          
          Logger.log(`Fetching existing records for ${entity.name} with URL: ${existingUrl}`);
          const existingRes = UrlFetchApp.fetch(existingUrl, { headers: airtableHeaders });
          const existingData = JSON.parse(existingRes.getContentText());
          existingRecords = existingData.records || [];
          Logger.log(`Found ${existingRecords.length} existing records for ${entity.name}`);
        } catch (filterErr) {
          Logger.log(`Error fetching existing records: ${filterErr}`);
          // Continue with empty existing records
        }

        // Build a map of Upsertion Check values to Airtable record IDs
        const existingMap = {};
        existingRecords.forEach(rec => {
          if (rec.fields && rec.fields["Upsertion Check"]) {
            existingMap[rec.fields["Upsertion Check"]] = rec.id;
          }
        });

        const createRecords = [];
        const updateRecords = [];

        // Process each field and determine if it needs to be created or updated
        fieldPayloads.forEach(payload => {
          const key = payload.fields["Upsertion Check"];
          if (existingMap[key]) {
            // This record exists, add to update list
            updateRecords.push({ id: existingMap[key], fields: payload.fields });
            Logger.log(`Will update existing record for ${key}`);
          } else {
            // This is a new record, add to create list
            createRecords.push(payload);
            Logger.log(`Will create new record for ${key}`);
          }
        });

        // Batch create new records
        for (let i = 0; i < createRecords.length; i += 10) {
          const batch = createRecords.slice(i, i + 10);
          const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}`;
          const options = {
            method: 'post',
            contentType: 'application/json',
            headers: airtableHeaders,
            payload: JSON.stringify({ records: batch }),
            muteHttpExceptions: true
          };
          const airtableRes = UrlFetchApp.fetch(url, options);
          const responseCode = airtableRes.getResponseCode();
          Logger.log(`Created ${batch.length} records for ${entity.name}, response code: ${responseCode}`);
          if (responseCode !== 200) {
            Logger.log(`Error response: ${airtableRes.getContentText()}`);
          }
        }

        // Batch update existing records
        for (let i = 0; i < updateRecords.length; i += 10) {
          const batch = updateRecords.slice(i, i + 10);
          const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}`;
          const options = {
            method: 'patch',
            contentType: 'application/json',
            headers: airtableHeaders,
            payload: JSON.stringify({ records: batch }),
            muteHttpExceptions: true
          };
          const airtableRes = UrlFetchApp.fetch(url, options);
          const responseCode = airtableRes.getResponseCode();
          Logger.log(`Updated ${batch.length} records for ${entity.name}, response code: ${responseCode}`);
          if (responseCode !== 200) {
            Logger.log(`Error response: ${airtableRes.getContentText()}`);
          }
        }
      } else {
        Logger.log(`No ${entity.name} records found.`);
      }
    } catch (err) {
      Logger.log(`Error processing ${entity.name}: ${err}`);
    }
  });
}