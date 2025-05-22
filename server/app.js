const express = require('express');
const cors = require('cors');

// Import handlers
const { 
  handleZohoWebhook, 
  handleAirtableWebhook, 
  handleHealthCheck 
} = require('./src/handlers/webhookHandlers');

const app = express();
const PORT = 5000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
app.post('/leads-notif', handleZohoWebhook);
app.post('/airtable-notif', handleAirtableWebhook);
app.get('/health', handleHealthCheck);

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  console.log(`Zoho webhook endpoint: http://localhost:${PORT}/leads-notif`);
  console.log(`Airtable webhook endpoint: http://localhost:${PORT}/airtable-notif`);
});

module.exports = app;