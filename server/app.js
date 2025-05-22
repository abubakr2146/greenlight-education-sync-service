const express = require('express');
const cors = require('cors');

// Import field mapping cache
const fieldMappingCache = require('./src/utils/fieldMappingCache');

const app = express();
const PORT = 5000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'Server is running' });
});

// Add cache status endpoint
app.get('/cache-status', (req, res) => {
  res.json(fieldMappingCache.getStatus());
});

async function startServer() {
  try {
    // Initialize field mapping cache before starting server
    await fieldMappingCache.initialize();
    
    app.listen(PORT, () => {
      // Server started
    });
  } catch (error) {
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', () => {
  fieldMappingCache.destroy();
  process.exit(0);
});

process.on('SIGTERM', () => {
  fieldMappingCache.destroy();
  process.exit(0);
});

startServer();

module.exports = app;