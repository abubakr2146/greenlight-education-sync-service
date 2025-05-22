const { loadZohoConfig, loadAirtableConfig } = require('../config/config');
const { getLeadDetails, getChangedFields, logLeadDetails } = require('../services/zohoService');
const { processChangedTables, fetchWebhookPayloads } = require('../services/airtableService');
const { createAirtableRecordFromZohoLead, handleZohoLeadUpdate, handleAirtableRecordUpdate } = require('../services/syncService');

// Constants
const HTTP_STATUS = {
  OK: 200,
  INTERNAL_SERVER_ERROR: 500
};

const TIMING = {
  AIRTABLE_PAYLOAD_TIMEOUT_MS: 300000, // 5 minutes
  AIRTABLE_PAYLOAD_TIMEOUT_DISPLAY: '5 minutes',
  PAYLOAD_FETCH_DELAY_MS: 2000, // 2 seconds initial delay
  PAYLOAD_FETCH_RETRY_DELAY_MS: 1000, // 1 second between retries
  PAYLOAD_FETCH_MAX_RETRIES: 3
};

const RESPONSES = {
  SUCCESS: {
    ZOHO: {
      status: 'success',
      message: 'Notification received and processed'
    },
    AIRTABLE: {
      status: 'success',
      message: 'Airtable notification received and processed'
    },
    HEALTH: {
      status: 'Server is running'
    }
  },
  ERROR: {
    CONFIG_LOAD_FAILED: (service) => ({
      error: `Unable to load ${service} config`
    })
  }
};

// Helper function to send standardized responses
function sendResponse(res, statusCode, data) {
  res.status(statusCode).json(data);
}

function sendSuccess(res, responseType) {
  sendResponse(res, HTTP_STATUS.OK, RESPONSES.SUCCESS[responseType]);
}

function sendError(res, errorType, ...args) {
  const errorData = typeof RESPONSES.ERROR[errorType] === 'function' 
    ? RESPONSES.ERROR[errorType](...args)
    : RESPONSES.ERROR[errorType];
  sendResponse(res, HTTP_STATUS.INTERNAL_SERVER_ERROR, errorData);
}

// Validation helpers
function validateZohoWebhookPayload(body) {
  return body.ids && body.ids.length > 0;
}

function validateAirtableWebhookPayload(body) {
  return body.base && body.base.id && body.webhook && body.webhook.id;
}

// Zoho webhook processing functions
async function processZohoLeadIds(leadIds, operation, affectedFields, config) {
  for (const leadId of leadIds) {
    await processZohoLead(leadId, operation, affectedFields, config);
  }
}

async function processZohoLead(leadId, operation, affectedFields, config) {
  // Log affected fields for this specific lead
  logAffectedFields(leadId, affectedFields);
  
  try {
    const leadDetails = await getLeadDetails(leadId, config);
    if (!leadDetails) {
      return;
    }

    const lead = leadDetails.data[0];
    logLeadDetails(lead);
    
    await handleLeadOperation(operation, leadId, lead, affectedFields);
  } catch (error) {
    // Error handled
  }
}

function logAffectedFields(leadId, affectedFields) {
  if (affectedFields) {
    const leadAffectedFields = affectedFields.find(item => item[leadId]);
    if (leadAffectedFields && leadAffectedFields[leadId]) {
      // Fields logged
    }
  }
}

async function handleLeadOperation(operation, leadId, lead, affectedFields) {
  switch (operation) {
    case 'create':
      await createAirtableRecordFromZohoLead(leadId, lead);
      break;
    
    case 'update':
      const changedFieldsInfo = getChangedFields(leadId, lead, affectedFields);
      await handleZohoLeadUpdate(leadId, lead, changedFieldsInfo);
      break;
    
    default:
      // Unknown operation
  }
}

// Airtable webhook processing functions
async function processAirtableWebhookData(webhookBody, config) {
  // Check if we have direct change data in the webhook
  if (webhookBody.changedTablesById) {
    return await processDirectChangeData(webhookBody.changedTablesById, config);
  } else {
    return await processPayloadBasedChanges(webhookBody, config);
  }
}

async function processDirectChangeData(changedTablesById, config) {
  const changeInfo = await processChangedTables(changedTablesById, config);
  if (changeInfo) {
    await handleAirtableRecordUpdate(changeInfo.recordId, changeInfo.changedFields);
  }
}

async function processPayloadBasedChanges(webhookBody, config) {
  // Add initial delay to allow Airtable to process the change
  await delay(TIMING.PAYLOAD_FETCH_DELAY_MS);
  
  let targetPayload = null;
  let attempts = 0;
  
  while (attempts <= TIMING.PAYLOAD_FETCH_MAX_RETRIES && !targetPayload) {
    attempts++;
    
    // Fetch fewer payloads to get more recent ones faster
    const payloads = await fetchWebhookPayloads(config, webhookBody.webhook.id, 50);
    
    if (payloads.length === 0) {
      if (attempts <= TIMING.PAYLOAD_FETCH_MAX_RETRIES) {
        await delay(TIMING.PAYLOAD_FETCH_RETRY_DELAY_MS);
        continue;
      }
      return;
    }
    
    targetPayload = findBestMatchingPayload(payloads, webhookBody.timestamp);
    
    if (!targetPayload && attempts <= TIMING.PAYLOAD_FETCH_MAX_RETRIES) {
      await delay(TIMING.PAYLOAD_FETCH_RETRY_DELAY_MS);
    }
  }
  
  if (!targetPayload) {
    return;
  }
  
  await processSelectedPayload(targetPayload, webhookBody.timestamp, config);
}

// Helper function for delays
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function findBestMatchingPayload(payloads, webhookTimestamp) {
  const webhookTime = new Date(webhookTimestamp);
  
  if (payloads.length === 0) {
    return null;
  }
  
  // Strategy 1: Look for payloads that came AFTER the webhook (the change we just made)
  let candidatePayloads = payloads.filter(payload => {
    const payloadTime = new Date(payload.timestamp);
    const timeDiff = payloadTime.getTime() - webhookTime.getTime();
    // Accept payloads that are up to 30 seconds after the webhook
    return timeDiff >= 0 && timeDiff <= 30000;
  });
  
  if (candidatePayloads.length > 0) {
    return candidatePayloads[0]; // Most recent of the "after" payloads
  }
  
  // Strategy 2: Look for payloads within the original timeout window (either direction)
  candidatePayloads = payloads.filter(payload => {
    const payloadTime = new Date(payload.timestamp);
    const timeDiff = Math.abs(webhookTime.getTime() - payloadTime.getTime());
    return timeDiff <= TIMING.AIRTABLE_PAYLOAD_TIMEOUT_MS;
  });
  
  if (candidatePayloads.length > 0) {
    return candidatePayloads[0];
  }
  
  // Strategy 3: Use the most recent payload regardless of timing
  return payloads[0]; // Most recent (already sorted newest first)
}

async function processSelectedPayload(targetPayload, webhookTimestamp, config) {
  const payloadTime = new Date(targetPayload.timestamp);
  const webhookTime = new Date(webhookTimestamp);
  const timeDiff = webhookTime.getTime() - payloadTime.getTime();
  
  // Process the payload data
  if (targetPayload.changedTablesById) {
    const changeInfo = await processChangedTables(targetPayload.changedTablesById, config);
    if (changeInfo) {
      await handleAirtableRecordUpdate(changeInfo.recordId, changeInfo.changedFields);
    }
  }
}

// Main webhook handlers
async function handleZohoWebhook(req, res) {
  try {
    // Load and validate config
    const config = loadZohoConfig();
    if (!config) {
      return sendError(res, 'CONFIG_LOAD_FAILED', 'Zoho');
    }
    
    // Validate payload
    if (!validateZohoWebhookPayload(req.body)) {
      return sendSuccess(res, 'ZOHO');
    }
    
    // Process leads
    await processZohoLeadIds(
      req.body.ids,
      req.body.operation,
      req.body.affected_fields,
      config
    );
    
    // Send success response
    sendSuccess(res, 'ZOHO');
    
  } catch (error) {
    sendError(res, 'CONFIG_LOAD_FAILED', 'Zoho');
  }
}

async function handleAirtableWebhook(req, res) {
  try {
    // Load and validate config
    const config = loadAirtableConfig();
    if (!config) {
      return sendError(res, 'CONFIG_LOAD_FAILED', 'Airtable');
    }
    
    // Validate and process payload
    if (validateAirtableWebhookPayload(req.body)) {
      await processAirtableWebhookData(req.body, config);
    }
    
    // Send success response
    sendSuccess(res, 'AIRTABLE');
    
  } catch (error) {
    sendError(res, 'CONFIG_LOAD_FAILED', 'Airtable');
  }
}

// Health check handler
function handleHealthCheck(req, res) {
  sendResponse(res, HTTP_STATUS.OK, RESPONSES.SUCCESS.HEALTH);
}

module.exports = {
  // Main handlers
  handleZohoWebhook,
  handleAirtableWebhook,
  handleHealthCheck,
  
  // Utility functions (exported for testing)
  validateZohoWebhookPayload,
  validateAirtableWebhookPayload,
  findBestMatchingPayload,
  processZohoLead,
  processAirtableWebhookData,
  delay,
  
  // Constants
  HTTP_STATUS,
  TIMING,
  RESPONSES
};