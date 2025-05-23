/**
 * Sync Daemon - Scheduled Sync Service
 *
 * Runs automatic sync at regular intervals for specified modules.
 *
 * Usage:
 * - Start daemon for Leads (default): node syncDaemon.js
 * - Start daemon for specific modules: node syncDaemon.js --modules Leads,Contacts
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
    this.modulesToSync = options.modules || ['Leads']; // Default to 'Leads' if not specified
    this.isRunning = false;
    this.stats = {
      totalRuns: 0,
      successfulRuns: 0,
      failedRuns: 0,
      totalRecordsAttempted: 0, // Sum of attempted from all modules in a run
      totalRecordsSynced: 0,   // Sum of successful from all modules in a run
      totalRecordsFailed: 0,   // Sum of failed from all modules in a run
      lastRunStartTime: null,
      lastRunEndTime: null,
      lastError: null,
      moduleStats: {} // Per-module stats: { moduleName: { runs: 0, synced: 0, failed: 0 } }
    };
    this.task = null;

    this.modulesToSync.forEach(moduleName => {
      this.stats.moduleStats[moduleName] = { runs: 0, synced: 0, failed: 0, errors: 0 };
    });
  }

  async start() {
    try {
      console.log('🚀 Starting Zoho-Airtable Sync Daemon');
      console.log('=====================================');
      console.log(`⏰ Schedule: ${this.cronExpression}`);
      console.log(`🔄 Modules to sync: ${this.modulesToSync.join(', ')}`);
      console.log(`📊 Verbose logging: ${this.verbose ? 'ON' : 'OFF'}`);

      console.log('🔧 Initializing field mapping cache for all specified modules...');
      for (const moduleName of this.modulesToSync) {
        console.log(`   Initializing cache for ${moduleName}...`);
        await fieldMappingCache.initialize(moduleName);
      }

      await this.waitForAllCaches();

      if (!cron.validate(this.cronExpression)) {
        throw new Error(`Invalid cron expression: ${this.cronExpression}`);
      }

      this.task = cron.schedule(this.cronExpression, async () => {
        if (this.isSyncing) {
          if (this.verbose) console.log(`[${new Date().toISOString()}] Sync cycle already in progress. Skipping new run.`);
          return;
        }
        await this.runSyncCycle();
      }, { scheduled: false });

      this.task.start();
      this.isRunning = true;
      this.isSyncing = false; // Flag to prevent overlapping runs

      console.log('✅ Sync daemon started successfully');
      console.log('📝 Press Ctrl+C to stop\n');
      this.showStatus();

    } catch (error) {
      console.error('❌ Failed to start sync daemon:', error.message);
      if (this.verbose && error.stack) console.error(error.stack);
      process.exit(1);
    }
  }

  async stop() {
    if (this.task) {
      this.task.stop();
      this.task = null;
    }
    this.isRunning = false;
    console.log('\n⏳ Waiting for any ongoing sync to complete before shutting down cache...');
    // Simple wait, ideally would have a more robust check for isSyncing completion
    await new Promise(resolve => setTimeout(resolve, 5000));


    for (const moduleName of this.modulesToSync) {
      fieldMappingCache.destroyModule(moduleName);
      if (this.verbose) console.log(`   Cache for ${moduleName} destroyed.`);
    }

    console.log('\n🛑 Sync daemon stopped');
    this.showFinalStats();
  }

  async runSyncCycle() {
    if (!this.isRunning) return;
    this.isSyncing = true;
    this.stats.totalRuns++;
    this.stats.lastRunStartTime = new Date();
    let cycleError = null;
    let cycleTotalAttempted = 0;
    let cycleTotalSynced = 0;
    let cycleTotalFailed = 0;

    if (this.verbose) {
      console.log(`\n🔄 [${this.stats.lastRunStartTime.toISOString()}] Starting sync cycle #${this.stats.totalRuns} for modules: ${this.modulesToSync.join(', ')}`);
    } else {
      process.stdout.write(`\nCycle #${this.stats.totalRuns} (${this.stats.lastRunStartTime.toLocaleTimeString()}): `);
    }

    for (const moduleName of this.modulesToSync) {
      try {
        if (this.verbose) console.log(`   [${moduleName}] Starting sync...`);
        // We assume performSync is module-aware or handles its own module logic.
        // If performSync needs the module name, it should be passed: await performSync(moduleName);
        // For now, sticking to the original call if pollingService is not being changed.
        const result = await performSync(moduleName); // Pass moduleName

        this.stats.moduleStats[moduleName].runs++;
        if (result.error) {
          this.stats.moduleStats[moduleName].errors = (this.stats.moduleStats[moduleName].errors || 0) + 1;
          cycleError = cycleError || result.error; // Keep first error for overall cycle
          if (this.verbose) console.log(`   [${moduleName}] ❌ Sync failed: ${result.error}`);
          else process.stdout.write(`[${moduleName}:X]`);
        } else {
          this.stats.moduleStats[moduleName].synced += result.successful;
          this.stats.moduleStats[moduleName].failed += result.failed;
          cycleTotalAttempted += (result.successful + result.failed);
          cycleTotalSynced += result.successful;
          cycleTotalFailed += result.failed;
          if (this.verbose) console.log(`   [${moduleName}] ✅ Sync completed: ${result.successful} successful, ${result.failed} failed.`);
          else process.stdout.write(`[${moduleName}:${result.successful}✓${result.failed}!]`);
        }
      } catch (error) {
        this.stats.moduleStats[moduleName].errors = (this.stats.moduleStats[moduleName].errors || 0) + 1;
        cycleError = cycleError || error.message;
        if (this.verbose) console.log(`   [${moduleName}] ❌ Sync error: ${error.message}`);
        else process.stdout.write(`[${moduleName}:E]`);
        if (this.verbose && error.stack) console.error(error.stack);
      }
    }

    this.stats.lastRunEndTime = new Date();
    this.stats.totalRecordsAttempted += cycleTotalAttempted;
    this.stats.totalRecordsSynced += cycleTotalSynced;
    this.stats.totalRecordsFailed += cycleTotalFailed;

    if (cycleError) {
      this.stats.failedRuns++;
      this.stats.lastError = cycleError;
    } else {
      this.stats.successfulRuns++;
      this.stats.lastError = null; // Clear last error on successful run
    }
    
    if (!this.verbose) console.log(` Done (${Math.round((this.stats.lastRunEndTime - this.stats.lastRunStartTime) / 1000)}s)`);
    else console.log(`\n🏁 [${this.stats.lastRunEndTime.toISOString()}] Sync cycle #${this.stats.totalRuns} finished. Duration: ${Math.round((this.stats.lastRunEndTime - this.stats.lastRunStartTime) / 1000)}s`);

    this.isSyncing = false;
  }

  async waitForAllCaches() {
    let allReady = false;
    const maxAttempts = 10;
    let currentAttempt = 0;

    while (currentAttempt < maxAttempts && !allReady) {
      currentAttempt++;
      allReady = true; // Assume ready until a module proves otherwise
      console.log(`⏳ Waiting for field mapping caches... (Attempt ${currentAttempt}/${maxAttempts})`);
      for (const moduleName of this.modulesToSync) {
        if (!fieldMappingCache.isReady(moduleName)) {
          allReady = false;
          console.log(`   [${moduleName}] cache not ready.`);
          // No need to break, check all modules in each attempt
        } else {
          if (this.verbose) console.log(`   [${moduleName}] cache ready.`);
        }
      }
      if (!allReady && currentAttempt < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2s between attempts
      }
    }

    if (!allReady) {
      const notReadyModules = this.modulesToSync.filter(m => !fieldMappingCache.isReady(m));
      throw new Error(`Field mapping cache failed to initialize for modules: ${notReadyModules.join(', ')}`);
    }

    console.log('✅ All field mapping caches are ready:');
    for (const moduleName of this.modulesToSync) {
      const status = fieldMappingCache.getStatus(moduleName);
      console.log(`   [${moduleName}]: ${status.mappingCount} fields mapped.`);
    }
  }

  showStatus() {
    // getSyncStatus might need to be module-aware or this needs to iterate
    console.log('📊 Current Daemon Status:');
    this.modulesToSync.forEach(moduleName => {
      const moduleCacheStatus = fieldMappingCache.getStatus(moduleName);
      console.log(`  Module [${moduleName}]:`);
      console.log(`  ├─ Field Mappings: ${moduleCacheStatus.mappingCount} (Initialized: ${moduleCacheStatus.initialized ? '✅' : '❌'})`);
      // Polling service status is harder to make module-specific without changing pollingService
    });
    const overallPollingStatus = getSyncStatus(); // This is likely not module-specific
    console.log(`  Overall Polling Service:`);
    console.log(`  ├─ Last Sync Window: ${overallPollingStatus.lastSyncTime || 'N/A'}`);
    console.log(`  └─ Field Mapping Ready (Legacy Check): ${overallPollingStatus.fieldMappingReady ? '✅' : '❌'}`);
    console.log(`Daemon Running: ${this.isRunning ? '🟢' : '🔴'}`);
  }

  showFinalStats() {
    console.log('\n📈 Final Statistics:');
    console.log(`├─ Modules Synced: ${this.modulesToSync.join(', ')}`);
    console.log(`├─ Total Sync Cycles: ${this.stats.totalRuns}`);
    console.log(`├─ Successful Cycles: ${this.stats.successfulRuns} (${this.stats.totalRuns > 0 ? Math.round((this.stats.successfulRuns / this.stats.totalRuns) * 100) : 0}%)`);
    console.log(`├─ Failed Cycles: ${this.stats.failedRuns}`);
    console.log(`├─ Total Records Attempted: ${this.stats.totalRecordsAttempted}`);
    console.log(`├─ Total Records Synced Successfully: ${this.stats.totalRecordsSynced}`);
    console.log(`├─ Total Records Failed to Sync: ${this.stats.totalRecordsFailed}`);
    if (this.stats.lastRunStartTime) console.log(`├─ Last Cycle Started: ${this.stats.lastRunStartTime.toISOString()}`);
    if (this.stats.lastError) console.log(`└─ Last Error: ${this.stats.lastError}`);
    else console.log('└─ No major errors in last cycle.');

    if (this.verbose && Object.keys(this.stats.moduleStats).length > 0) {
        console.log('\n📊 Per-Module Statistics:');
        for (const moduleName of this.modulesToSync) {
            const mStats = this.stats.moduleStats[moduleName];
            console.log(`  [${moduleName}]:`);
            console.log(`  ├─ Syncs Attempted: ${mStats.runs}`);
            console.log(`  ├─ Records Synced: ${mStats.synced}`);
            console.log(`  ├─ Records Failed: ${mStats.failed}`);
            console.log(`  └─ Sync Errors: ${mStats.errors}`);
        }
    }
  }

  showLiveStats() {
    const uptimeSeconds = this.stats.lastRunStartTime ? Math.round((Date.now() - this.stats.lastRunStartTime.getTime()) / 1000) : 0;
    console.log('\n📊 Live Statistics (Aggregated):');
    console.log(`├─ Modules: ${this.modulesToSync.join(', ')}`);
    console.log(`├─ Cycles: ${this.stats.totalRuns} (${this.stats.successfulRuns}✅ ${this.stats.failedRuns}❌)`);
    console.log(`├─ Records Synced (Total): ${this.stats.totalRecordsSynced}`);
    console.log(`├─ Records Failed (Total): ${this.stats.totalRecordsFailed}`);
    console.log(`├─ Last Cycle Started: ${uptimeSeconds}s ago`);
    console.log(`└─ Status: ${this.isRunning ? (this.isSyncing ? '🟡 Syncing...' : '🟢 Idle') : '🔴 Stopped'}\n`);
  }
}

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--verbose' || arg === '-v') {
      options.verbose = true;
    } else if (arg === '--interval' || arg === '-i') {
      if (args[i + 1]) {
        options.interval = args[i + 1];
        i++;
      } else { console.error("Error: --interval option requires a value."); process.exit(1); }
    } else if (arg === '--modules') {
      if (args[i + 1]) {
        options.modules = args[i + 1].split(',').map(m => m.trim()).filter(m => m);
        i++;
      } else { console.error("Error: --modules option requires a comma-separated list of module names."); process.exit(1); }
    } else if (arg === '--help' || arg === '-h') {
      showHelp();
      process.exit(0);
    }
  }
  if (!options.modules || options.modules.length === 0) {
    options.modules = ['Leads']; // Default if --modules is not provided or empty
  }
  return options;
}

function showHelp() {
  console.log(`
🚀 Sync Daemon Usage:
  node syncDaemon.js [options]

Options:
  --modules <M1,M2,...>  Comma-separated list of modules to sync (e.g., Leads,Contacts). Defaults to 'Leads'.
  --interval, -i "cron"  Custom cron interval (e.g., "*/2 * * * *"). Defaults to every minute.
  --verbose, -v          Enable detailed logging.
  --help, -h             Show this help message.

Examples:
  node syncDaemon.js                                  # Sync Leads, every minute
  node syncDaemon.js --modules Leads,Contacts         # Sync Leads and Contacts
  node syncDaemon.js --interval "*/5 * * * *" -v      # Sync default modules every 5 mins, verbose

Cron Format: <minute> <hour> <day_of_month> <month> <day_of_week>
Keyboard Shortcuts (while running):
  Ctrl+C: Stop daemon
  Ctrl+Z: Show live stats (SIGTSTP)
`);
}

async function main() {
  const options = parseArgs();
  const daemon = new SyncDaemon(options);

  const shutdown = async (signal) => {
    console.log(`\n\n🛑 ${signal} received. Shutting down...`);
    await daemon.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGTSTP', () => { // Typically Ctrl+Z
    daemon.showLiveStats();
  });

  await daemon.start();
}

if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error in daemon main:', error.message);
    if (error.stack) console.error(error.stack);
    process.exit(1);
  });
}

module.exports = SyncDaemon;
