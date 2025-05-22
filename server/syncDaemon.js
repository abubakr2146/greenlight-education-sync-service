/**
 * Sync Daemon - Scheduled Sync Service
 * 
 * Runs automatic sync at regular intervals
 * 
 * Usage:
 * - Start daemon: node syncDaemon.js
 * - Custom interval: node syncDaemon.js --interval (cron expression)
 * - With logging: node syncDaemon.js --verbose
 */

const cron = require('node-cron');
const { performSync, getSyncStatus } = require('./src/services/pollingService');
const fieldMappingCache = require('./src/utils/fieldMappingCache');

class SyncDaemon {
  constructor(options = {}) {
    this.cronExpression = options.interval || '* * * * *'; // Default: every minute
    this.verbose = options.verbose || false;
    this.isRunning = false;
    this.stats = {
      totalRuns: 0,
      successfulRuns: 0,
      failedRuns: 0,
      totalSynced: 0,
      totalFailed: 0,
      lastRun: null,
      lastError: null
    };
    this.task = null;
  }

  async start() {
    try {
      console.log('🚀 Starting Zoho-Airtable Sync Daemon');
      console.log('=====================================');
      console.log(`⏰ Schedule: ${this.cronExpression} (every minute)`);
      console.log(`📊 Verbose logging: ${this.verbose ? 'ON' : 'OFF'}`);
      
      // Initialize field mapping cache
      console.log('🔧 Initializing field mapping cache...');
      await fieldMappingCache.initialize();
      
      // Wait for cache to populate
      await this.waitForCache();
      
      // Validate cron expression
      if (!cron.validate(this.cronExpression)) {
        throw new Error(`Invalid cron expression: ${this.cronExpression}`);
      }
      
      // Start the scheduled task
      this.task = cron.schedule(this.cronExpression, async () => {
        await this.runSync();
      }, {
        scheduled: false // Don't start immediately
      });
      
      // Start the cron job
      this.task.start();
      this.isRunning = true;
      
      console.log('✅ Sync daemon started successfully');
      console.log('📝 Press Ctrl+C to stop\n');
      
      // Show initial status
      this.showStatus();
      
    } catch (error) {
      console.error('❌ Failed to start sync daemon:', error.message);
      process.exit(1);
    }
  }

  async stop() {
    if (this.task) {
      this.task.stop();
      this.task = null;
    }
    
    fieldMappingCache.destroy();
    this.isRunning = false;
    
    console.log('\n🛑 Sync daemon stopped');
    this.showFinalStats();
  }

  async runSync() {
    if (!this.isRunning) return;
    
    this.stats.totalRuns++;
    this.stats.lastRun = new Date();
    
    try {
      if (this.verbose) {
        console.log(`\n🔄 [${this.stats.lastRun.toISOString()}] Starting sync cycle #${this.stats.totalRuns}`);
      } else {
        process.stdout.write('.');
      }
      
      const result = await performSync();
      
      if (result.error) {
        this.stats.failedRuns++;
        this.stats.lastError = result.error;
        
        if (this.verbose) {
          console.log(`❌ Sync failed: ${result.error}`);
        } else {
          process.stdout.write('X');
        }
      } else {
        this.stats.successfulRuns++;
        this.stats.totalSynced += result.successful;
        this.stats.totalFailed += result.failed;
        
        if (this.verbose) {
          console.log(`✅ Sync completed: ${result.successful} successful, ${result.failed} failed`);
        } else if (result.successful > 0 || result.failed > 0) {
          process.stdout.write(`${result.successful > 0 ? '✓' : '!'}`);
        }
      }
      
    } catch (error) {
      this.stats.failedRuns++;
      this.stats.lastError = error.message;
      
      if (this.verbose) {
        console.log(`❌ Sync error: ${error.message}`);
      } else {
        process.stdout.write('X');
      }
    }
  }

  async waitForCache() {
    let attempts = 0;
    const maxAttempts = 10;
    
    while (attempts < maxAttempts && !fieldMappingCache.isReady()) {
      console.log(`⏳ Waiting for field mapping cache... (${attempts + 1}/${maxAttempts})`);
      await new Promise(resolve => setTimeout(resolve, 1000));
      attempts++;
    }
    
    if (!fieldMappingCache.isReady()) {
      throw new Error('Field mapping cache failed to initialize');
    }
    
    const status = fieldMappingCache.getStatus();
    console.log(`✅ Field mapping ready: ${status.mappingCount} fields mapped`);
  }

  showStatus() {
    const status = getSyncStatus();
    console.log('📊 Current Status:');
    console.log(`├─ Field Mappings: ${fieldMappingCache.getStatus().mappingCount}`);
    console.log(`├─ Last Sync Window: ${status.lastSyncTime}`);
    console.log(`└─ Ready: ${status.fieldMappingReady ? '✅' : '❌'}\n`);
  }

  showFinalStats() {
    console.log('\n📈 Final Statistics:');
    console.log(`├─ Total Runs: ${this.stats.totalRuns}`);
    console.log(`├─ Successful: ${this.stats.successfulRuns} (${this.stats.totalRuns > 0 ? Math.round((this.stats.successfulRuns / this.stats.totalRuns) * 100) : 0}%)`);
    console.log(`├─ Failed: ${this.stats.failedRuns}`);
    console.log(`├─ Records Synced: ${this.stats.totalSynced}`);
    console.log(`├─ Sync Failures: ${this.stats.totalFailed}`);
    
    if (this.stats.lastRun) {
      console.log(`├─ Last Run: ${this.stats.lastRun.toISOString()}`);
    }
    
    if (this.stats.lastError) {
      console.log(`└─ Last Error: ${this.stats.lastError}`);
    } else {
      console.log('└─ No Errors');
    }
  }

  // Show live stats on demand
  showLiveStats() {
    const uptime = this.stats.lastRun ? 
      Math.round((Date.now() - this.stats.lastRun.getTime()) / 1000) : 0;
    
    console.log('\n📊 Live Statistics:');
    console.log(`├─ Runs: ${this.stats.totalRuns} (${this.stats.successfulRuns}✅ ${this.stats.failedRuns}❌)`);
    console.log(`├─ Synced: ${this.stats.totalSynced} records`);
    console.log(`├─ Last Run: ${uptime}s ago`);
    console.log(`└─ Status: ${this.isRunning ? '🟢 Running' : '🔴 Stopped'}\n`);
  }
}

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {};
  
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    
    if (arg === '--verbose' || arg === '-v') {
      options.verbose = true;
    } else if (arg === '--interval' || arg === '-i') {
      options.interval = args[i + 1];
      i++; // Skip next arg as it's the interval value
    } else if (arg === '--help' || arg === '-h') {
      showHelp();
      process.exit(0);
    }
  }
  
  return options;
}

function showHelp() {
  console.log(`
🚀 Sync Daemon Usage:

Basic:
  node syncDaemon.js                    # Run with default settings (every minute)

Options:
  --interval, -i "*/2 * * * *"         # Custom cron interval (every 2 minutes)
  --verbose, -v                        # Enable detailed logging
  --help, -h                           # Show this help

Examples:
  node syncDaemon.js --verbose                    # Every minute with detailed logs
  node syncDaemon.js -i "*/5 * * * *"            # Every 5 minutes
  node syncDaemon.js -i "0 */2 * * *" --verbose  # Every 2 hours with logs

Cron Format: * * * * *
             │ │ │ │ │
             │ │ │ │ └─ Day of week (0-7)
             │ │ │ └─── Month (1-12)
             │ │ └───── Day of month (1-31)
             │ └─────── Hour (0-23)
             └───────── Minute (0-59)

Keyboard Shortcuts:
  Ctrl+C: Stop daemon
  Ctrl+Z: Show live stats (when verbose mode is off)
`);
}

// Main execution
async function main() {
  const options = parseArgs();
  const daemon = new SyncDaemon(options);
  
  // Handle graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\n\n🛑 Shutdown requested...');
    await daemon.stop();
    process.exit(0);
  });
  
  process.on('SIGTERM', async () => {
    console.log('\n\n🛑 Process termination requested...');
    await daemon.stop();
    process.exit(0);
  });
  
  // Handle stats display in non-verbose mode
  process.on('SIGTSTP', () => {
    if (!options.verbose) {
      daemon.showLiveStats();
    }
  });
  
  // Start the daemon
  await daemon.start();
}

// Run if this file is executed directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
  });
}

module.exports = SyncDaemon;