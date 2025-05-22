#!/usr/bin/env node

/**
 * Manual Sync Manager
 * 
 * Usage:
 * - Run single sync: node syncManager.js
 * - Check status: node syncManager.js status
 * - Set sync window: node syncManager.js --since "2024-01-15T10:00:00.000Z"
 * - Force full sync: node syncManager.js --full
 */

const { performSync, getSyncStatus, setLastSyncTimestamp } = require('./src/services/pollingService');
const fieldMappingCache = require('./src/utils/fieldMappingCache');

async function main() {
  const args = process.argv.slice(2);
  const command = args[0] || 'sync';
  
  console.log('🚀 Zoho-Airtable Sync Manager');
  console.log('================================');
  
  try {
    // Initialize field mapping cache
    console.log('🔧 Initializing field mapping cache...');
    await fieldMappingCache.initialize();
    
    // Wait a moment for cache to populate
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    switch (command) {
      case 'status':
        await showStatus();
        break;
        
      case 'sync':
      default:
        await runSync(args);
        break;
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  } finally {
    // Cleanup
    fieldMappingCache.destroy();
  }
}

async function showStatus() {
  const status = getSyncStatus();
  
  console.log('\n📊 Sync Status:');
  console.log(`├─ Last Sync: ${status.lastSyncTime}`);
  console.log(`├─ Field Mapping Ready: ${status.fieldMappingReady ? '✅' : '❌'}`);
  console.log(`└─ Mapped Fields Count: ${status.fieldMappingCount}`);
  
  if (!status.fieldMappingReady) {
    console.log('\n⚠️  Field mapping not ready. Please check your Airtable configuration.');
  }
}

async function runSync(args) {
  // Parse arguments
  const sinceArg = args.find(arg => arg.startsWith('--since'));
  const fullSync = args.includes('--full');
  
  if (sinceArg) {
    const sinceValue = sinceArg.split('=')[1] || args[args.indexOf(sinceArg) + 1];
    if (sinceValue) {
      const sinceTimestamp = new Date(sinceValue).getTime();
      if (!isNaN(sinceTimestamp)) {
        setLastSyncTimestamp(sinceTimestamp);
        console.log(`📅 Set sync window to: ${new Date(sinceTimestamp).toISOString()}`);
      } else {
        console.error('❌ Invalid date format. Use ISO format: 2024-01-15T10:00:00.000Z');
        return;
      }
    }
  }
  
  if (fullSync) {
    // Set to 30 days ago for full sync
    const thirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000);
    setLastSyncTimestamp(thirtyDaysAgo);
    console.log(`📅 Full sync mode: Looking back 30 days to ${new Date(thirtyDaysAgo).toISOString()}`);
  }
  
  // Check if field mapping is ready
  const status = getSyncStatus();
  if (!status.fieldMappingReady) {
    console.log('\n⚠️  Field mapping not ready, retrying in 3 seconds...');
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    if (!getSyncStatus().fieldMappingReady) {
      console.error('❌ Field mapping still not ready. Please check your Airtable "Zoho Fields" table configuration.');
      return;
    }
  }
  
  console.log(`\n🔄 Starting sync with ${status.fieldMappingCount} mapped fields...`);
  
  // Run the sync
  const startTime = Date.now();
  const result = await performSync();
  const duration = Date.now() - startTime;
  
  // Show results
  console.log('\n📈 Sync Results:');
  console.log(`├─ Duration: ${Math.round(duration / 1000)}s`);
  console.log(`├─ Successful: ${result.successful}`);
  console.log(`├─ Failed: ${result.failed}`);
  
  if (result.error) {
    console.log(`└─ Error: ${result.error}`);
  } else {
    console.log(`└─ Success Rate: ${result.successful + result.failed > 0 ? Math.round((result.successful / (result.successful + result.failed)) * 100) : 100}%`);
  }
  
  if (result.successful > 0) {
    console.log('\n✅ Sync completed successfully!');
  } else if (result.failed > 0) {
    console.log('\n⚠️  Sync completed with errors.');
  } else {
    console.log('\n📭 No changes to sync.');
  }
}

// Handle process termination gracefully
process.on('SIGINT', () => {
  console.log('\n\n🛑 Sync interrupted by user');
  fieldMappingCache.destroy();
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n\n🛑 Sync terminated');
  fieldMappingCache.destroy();
  process.exit(0);
});

// Run the main function
main();