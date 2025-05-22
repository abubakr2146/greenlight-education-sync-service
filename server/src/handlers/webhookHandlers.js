const { loadZohoConfig, loadAirtableConfig } = require('../config/config');
const { getLeadDetails, getChangedFields, logLeadDetails } = require('../services/zohoService');
const { processChangedTables, fetchWebhookPayloads } = require('../services/airtableService');
const { createAirtableRecordFromZohoLead, handleZohoLeadUpdate, handleAirtableRecordUpdate } = require('../services/syncService');

// Zoho webhook handler
async function handleZohoWebhook(req, res) {
  console.log('Received Zoho notification:', JSON.stringify(req.body, null, 2));
  
  const config = loadZohoConfig();
  if (!config) {
    return res.status(500).json({ error: 'Unable to load Zoho config' });
  }
  
  // Process each lead ID in the notification
  if (req.body.ids && req.body.ids.length > 0) {
    for (const leadId of req.body.ids) {
      console.log(`\n=== Processing Lead ID: ${leadId} ===`);
      console.log('Operation:', req.body.operation);
      
      // Log affected fields for this specific lead
      if (req.body.affected_fields) {
        const leadAffectedFields = req.body.affected_fields.find(item => item[leadId]);
        if (leadAffectedFields && leadAffectedFields[leadId]) {
          console.log('Affected Fields:', leadAffectedFields[leadId]);
        }
      }
      
      const leadDetails = await getLeadDetails(leadId, config);
      if (leadDetails) {
        console.log('Lead Details:', JSON.stringify(leadDetails, null, 2));
        
        // Extract specific fields for processing
        const lead = leadDetails.data[0];
        logLeadDetails(lead);
        
        // Handle different operations
        if (req.body.operation === 'create') {
          // New lead created - create corresponding Airtable record
          console.log(`🆕 New lead created in Zoho: ${leadId}`);
          await createAirtableRecordFromZohoLead(leadId, lead);
        } else if (req.body.operation === 'update') {
          // Lead updated - check for phone field changes
          const changedFieldsInfo = getChangedFields(leadId, lead, req.body.affected_fields);
          await handleZohoLeadUpdate(leadId, lead, changedFieldsInfo);
        }
      }
    }
  }
  
  // Acknowledge receipt
  res.status(200).json({ 
    status: 'success',
    message: 'Notification received and processed' 
  });
}

// Airtable webhook handler
async function handleAirtableWebhook(req, res) {
  console.log('Received Airtable notification:', JSON.stringify(req.body, null, 2));
  
  const config = loadAirtableConfig();
  if (!config) {
    return res.status(500).json({ error: 'Unable to load Airtable config' });
  }
  
  // Process the webhook payload
  if (req.body.base && req.body.base.id && req.body.webhook && req.body.webhook.id) {
    console.log(`\n=== Airtable Webhook Details ===`);
    console.log('Base ID:', req.body.base.id);
    console.log('Webhook ID:', req.body.webhook.id);
    console.log('Timestamp:', req.body.timestamp);
    
    // Check if we have change data in the webhook
    if (!req.body.changedTablesById) {
      console.log('\n⚠️  No changedTablesById in webhook payload - fetching payloads manually...');
      
      // Fetch the actual payloads from Airtable API
      const payloads = await fetchWebhookPayloads(config, req.body.webhook.id);
      
      if (payloads.length > 0) {
        console.log(`\n=== Found ${payloads.length} payload(s) ===`);
        
        // Process payloads with flexible timing approach
        const webhookTime = new Date(req.body.timestamp);
        console.log(`\n=== Webhook timestamp: ${req.body.timestamp} ===`);
        
        // First try: Look for payloads within 5 minutes (to handle Airtable delays)
        let candidatePayloads = payloads.filter(payload => {
          const payloadTime = new Date(payload.timestamp);
          const timeDiff = Math.abs(webhookTime.getTime() - payloadTime.getTime());
          return timeDiff <= 300000; // 5 minutes
        });
        
        if (candidatePayloads.length === 0) {
          // Second try: Use the most recent payload regardless of timing
          console.log(`⚠️  No payloads within 5 minutes, using most recent payload`);
          candidatePayloads = payloads.slice(0, 1); // Most recent (already sorted newest first)
        }
        
        if (candidatePayloads.length > 0) {
          console.log(`\n=== Processing ${candidatePayloads.length} candidate payload(s) ===`);
          
          // Use the most recent candidate payload
          const targetPayload = candidatePayloads[0]; // Most recent due to sort
          const payloadTime = new Date(targetPayload.timestamp);
          const timeDiff = webhookTime.getTime() - payloadTime.getTime();
          
          console.log('Selected payload timestamp:', targetPayload.timestamp);
          console.log('Webhook timestamp:', req.body.timestamp);
          console.log(`Time difference: ${timeDiff}ms (${Math.abs(timeDiff/1000)}s ${timeDiff < 0 ? 'after' : 'before'} webhook)`);
          
          // Process the payload data
          if (targetPayload.changedTablesById) {
            const changeInfo = await processChangedTables(targetPayload.changedTablesById, config);
            if (changeInfo) {
              await handleAirtableRecordUpdate(changeInfo.recordId, changeInfo.changedFields);
            }
          } else {
            console.log('⚠️  No changedTablesById in selected payload');
          }
        } else {
          console.log(`\n❌ No payloads available to process`);
        }
      } else {
        console.log('No payloads found');
      }
    } else {
      // We have direct change data in the webhook payload
      console.log('\n✅ Found changedTablesById in webhook payload');
      const changeInfo = await processChangedTables(req.body.changedTablesById, config);
      if (changeInfo) {
        await handleAirtableRecordUpdate(changeInfo.recordId, changeInfo.changedFields);
      }
    }
  }
  
  // Acknowledge receipt
  res.status(200).json({ 
    status: 'success',
    message: 'Airtable notification received and processed' 
  });
}

// Health check handler
function handleHealthCheck(req, res) {
  res.json({ status: 'Server is running' });
}

module.exports = {
  handleZohoWebhook,
  handleAirtableWebhook,
  handleHealthCheck
};