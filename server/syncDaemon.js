#!/usr/bin/env node

/**
 * Sync Daemon - Bulk Sync Service
 *
 * Runs bulk sync at regular intervals for specified modules.
 * Uses the latest bulkSync.js implementation.
 *
 * Usage:
 * - Start daemon for Leads (default): node syncDaemon.js
 * - Start daemon for specific modules: node syncDaemon.js --modules Leads,Contacts
 * - Custom frequency: node syncDaemon.js --frequency "every-5-minutes"
 * - With logging: node syncDaemon.js --verbose
 */

const cron = require('node-cron');
const { spawn } = require('child_process');
const path = require('path');

class SyncDaemon {
  constructor(options = {}) {
    // Configuration variables
    this.modules = options.modules || ['Leads']; // Module variable
    this.frequency = options.frequency || '* * * * *'; // Frequency variable (default: every minute)
    this.verbose = options.verbose || false;
    this.dryRun = options.dryRun || false;
    this.noDelete = options.noDelete || false;
    
    // Runtime state
    this.isRunning = false;
    this.isSyncing = false;
    this.task = null;
    
    // Statistics
    this.stats = {
      totalRuns: 0,
      successfulRuns: 0,
      failedRuns: 0,
      totalRecordsProcessed: 0,
      totalRecordsSynced: 0,
      totalRecordsFailed: 0,
      lastRunStartTime: null,
      lastRunEndTime: null,
      lastError: null,
      moduleStats: {}
    };

    // Initialize module stats
    this.modules.forEach(moduleName => {
      this.stats.moduleStats[moduleName] = { 
        runs: 0, 
        processed: 0, 
        synced: 0, 
        failed: 0, 
        errors: 0 
      };
    });
  }

  async start() {
    try {
      console.log('🚀 Starting Bulk Sync Daemon');
      console.log('============================');
      console.log(`⏰ Frequency: ${this.frequency}`);
      console.log(`🔄 Modules: ${this.modules.join(', ')}`);
      console.log(`📊 Verbose logging: ${this.verbose ? 'ON' : 'OFF'}`);
      console.log(`🧪 Dry run mode: ${this.dryRun ? 'ON' : 'OFF'}`);
      console.log(`🗑️ Delete operations: ${this.noDelete ? 'OFF' : 'ON'}`);

      // Validate cron expression
      if (!cron.validate(this.frequency)) {
        throw new Error(`Invalid cron expression: ${this.frequency}`);
      }

      // Schedule the bulk sync task
      this.task = cron.schedule(this.frequency, async () => {
        if (this.isSyncing) {
          if (this.verbose) {
            console.log(`[${new Date().toISOString()}] Sync cycle already in progress. Skipping new run.`);
          }
          return;
        }
        await this.runSyncCycle();
      }, { scheduled: false });

      this.task.start();
      this.isRunning = true;
      this.isSyncing = false;

      console.log('✅ Sync daemon started successfully');
      console.log('📝 Press Ctrl+C to stop, Ctrl+Z for stats\n');
      
      await this.showStatus();

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
    
    console.log('\n⏳ Waiting for any ongoing sync to complete...');
    
    // Wait for current sync to finish
    while (this.isSyncing) {
      await new Promise(resolve => setTimeout(resolve, 1000));
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
    let cycleTotalProcessed = 0;
    let cycleTotalSynced = 0;
    let cycleTotalFailed = 0;

    if (this.verbose) {
      console.log(`\n🔄 [${this.stats.lastRunStartTime.toISOString()}] Starting sync cycle #${this.stats.totalRuns}`);
      console.log(`   Modules: ${this.modules.join(', ')}`);
    } else {
      process.stdout.write(`\nCycle #${this.stats.totalRuns} (${this.stats.lastRunStartTime.toLocaleTimeString()}): `);
    }

    // Run bulk sync for each module
    for (const moduleName of this.modules) {
      try {
        if (this.verbose) console.log(`   [${moduleName}] Starting bulk sync...`);
        
        const result = await this.runBulkSync(moduleName);
        
        this.stats.moduleStats[moduleName].runs++;
        
        if (result.success) {
          this.stats.moduleStats[moduleName].processed += result.processed;
          this.stats.moduleStats[moduleName].synced += result.synced;
          this.stats.moduleStats[moduleName].failed += result.failed;
          
          cycleTotalProcessed += result.processed;
          cycleTotalSynced += result.synced;
          cycleTotalFailed += result.failed;
          
          if (this.verbose) {
            console.log(`   [${moduleName}] ✅ Completed: ${result.processed} processed, ${result.synced} synced, ${result.failed} failed`);
          } else {
            process.stdout.write(`[${moduleName}:${result.synced}✓${result.failed}!]`);
          }
        } else {
          this.stats.moduleStats[moduleName].errors++;
          cycleError = cycleError || result.error;
          
          if (this.verbose) {
            console.log(`   [${moduleName}] ❌ Failed: ${result.error}`);
          } else {
            process.stdout.write(`[${moduleName}:X]`);
          }
        }
        
      } catch (error) {
        this.stats.moduleStats[moduleName].errors++;
        cycleError = cycleError || error.message;
        
        if (this.verbose) {
          console.log(`   [${moduleName}] ❌ Error: ${error.message}`);
          if (error.stack) console.error(error.stack);
        } else {
          process.stdout.write(`[${moduleName}:E]`);
        }
      }
    }

    this.stats.lastRunEndTime = new Date();
    this.stats.totalRecordsProcessed += cycleTotalProcessed;
    this.stats.totalRecordsSynced += cycleTotalSynced;
    this.stats.totalRecordsFailed += cycleTotalFailed;

    if (cycleError) {
      this.stats.failedRuns++;
      this.stats.lastError = cycleError;
    } else {
      this.stats.successfulRuns++;
      this.stats.lastError = null;
    }

    const duration = Math.round((this.stats.lastRunEndTime - this.stats.lastRunStartTime) / 1000);
    
    if (!this.verbose) {
      console.log(` Done (${duration}s)`);
    } else {
      console.log(`\n🏁 [${this.stats.lastRunEndTime.toISOString()}] Cycle #${this.stats.totalRuns} finished. Duration: ${duration}s`);
    }

    this.isSyncing = false;
  }

  async runBulkSync(moduleName) {
    return new Promise((resolve) => {
      const bulkSyncPath = path.join(__dirname, 'bulkSync.js');
      const args = ['--module', moduleName];
      
      if (this.dryRun) args.push('--dry-run');
      if (this.noDelete) args.push('--no-delete');
      if (this.verbose) args.push('--verbose');

      const child = spawn('node', [bulkSyncPath, ...args], {
        stdio: this.verbose ? 'inherit' : 'pipe'
      });

      let output = '';
      let errorOutput = '';

      if (!this.verbose) {
        child.stdout?.on('data', (data) => {
          output += data.toString();
        });

        child.stderr?.on('data', (data) => {
          errorOutput += data.toString();
        });
      }

      child.on('close', (code) => {
        if (code === 0) {
          // Parse output for statistics
          const stats = this.parseStatsFromOutput(output);
          resolve({
            success: true,
            processed: stats.processed || 0,
            synced: stats.synced || 0,
            failed: stats.failed || 0
          });
        } else {
          resolve({
            success: false,
            error: errorOutput || `Process exited with code ${code}`,
            processed: 0,
            synced: 0,
            failed: 0
          });
        }
      });

      child.on('error', (error) => {
        resolve({
          success: false,
          error: error.message,
          processed: 0,
          synced: 0,
          failed: 0
        });
      });
    });
  }

  parseStatsFromOutput(output) {
    const stats = { processed: 0, synced: 0, failed: 0 };
    
    try {
      // Look for summary patterns in bulkSync output
      const processedMatch = output.match(/(\d+)\s+total.*records/i);
      const syncedMatch = output.match(/(\d+)\s+synced/i);
      const failedMatch = output.match(/(\d+)\s+failed/i);
      
      if (processedMatch) stats.processed = parseInt(processedMatch[1]);
      if (syncedMatch) stats.synced = parseInt(syncedMatch[1]);
      if (failedMatch) stats.failed = parseInt(failedMatch[1]);
    } catch (error) {
      // Ignore parsing errors
    }
    
    return stats;
  }

  async showStatus() {
    console.log('📊 Current Daemon Status:');
    console.log(`  ├─ Running: ${this.isRunning ? '🟢' : '🔴'}`);
    console.log(`  ├─ Frequency: ${this.frequency}`);
    console.log(`  ├─ Modules: ${this.modules.join(', ')}`);
    console.log(`  ├─ Dry Run: ${this.dryRun ? 'ON' : 'OFF'}`);
    console.log(`  ├─ Total Runs: ${this.stats.totalRuns}`);
    console.log(`  └─ Last Error: ${this.stats.lastError || 'None'}`);
  }

  showFinalStats() {
    console.log('\n📈 Final Statistics:');
    console.log(`├─ Modules Synced: ${this.modules.join(', ')}`);
    console.log(`├─ Total Sync Cycles: ${this.stats.totalRuns}`);
    console.log(`├─ Successful Cycles: ${this.stats.successfulRuns} (${this.stats.totalRuns > 0 ? Math.round((this.stats.successfulRuns / this.stats.totalRuns) * 100) : 0}%)`);
    console.log(`├─ Failed Cycles: ${this.stats.failedRuns}`);
    console.log(`├─ Total Records Processed: ${this.stats.totalRecordsProcessed}`);
    console.log(`├─ Total Records Synced: ${this.stats.totalRecordsSynced}`);
    console.log(`├─ Total Records Failed: ${this.stats.totalRecordsFailed}`);
    
    if (this.stats.lastRunStartTime) {
      console.log(`├─ Last Cycle Started: ${this.stats.lastRunStartTime.toISOString()}`);
    }
    
    if (this.stats.lastError) {
      console.log(`└─ Last Error: ${this.stats.lastError}`);
    } else {
      console.log('└─ No major errors in last cycle.');
    }

    if (this.verbose && Object.keys(this.stats.moduleStats).length > 0) {
      console.log('\n📊 Per-Module Statistics:');
      for (const moduleName of this.modules) {
        const mStats = this.stats.moduleStats[moduleName];
        console.log(`  [${moduleName}]:`);
        console.log(`  ├─ Syncs Attempted: ${mStats.runs}`);
        console.log(`  ├─ Records Processed: ${mStats.processed}`);
        console.log(`  ├─ Records Synced: ${mStats.synced}`);
        console.log(`  ├─ Records Failed: ${mStats.failed}`);
        console.log(`  └─ Sync Errors: ${mStats.errors}`);
      }
    }
  }

  showLiveStats() {
    const uptimeSeconds = this.stats.lastRunStartTime ? Math.round((Date.now() - this.stats.lastRunStartTime.getTime()) / 1000) : 0;
    console.log('\n📊 Live Statistics:');
    console.log(`├─ Modules: ${this.modules.join(', ')}`);
    console.log(`├─ Frequency: ${this.frequency}`);
    console.log(`├─ Cycles: ${this.stats.totalRuns} (${this.stats.successfulRuns}✅ ${this.stats.failedRuns}❌)`);
    console.log(`├─ Records Synced (Total): ${this.stats.totalRecordsSynced}`);
    console.log(`├─ Records Failed (Total): ${this.stats.totalRecordsFailed}`);
    console.log(`├─ Last Cycle: ${uptimeSeconds}s ago`);
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
    } else if (arg === '--dry-run') {
      options.dryRun = true;
    } else if (arg === '--no-delete') {
      options.noDelete = true;
    } else if (arg === '--frequency' || arg === '-f') {
      if (args[i + 1]) {
        options.frequency = args[i + 1];
        i++;
      } else {
        console.error("Error: --frequency option requires a cron expression.");
        process.exit(1);
      }
    } else if (arg === '--modules' || arg === '-m') {
      if (args[i + 1]) {
        options.modules = args[i + 1].split(',').map(m => m.trim()).filter(m => m);
        i++;
      } else {
        console.error("Error: --modules option requires a comma-separated list of module names.");
        process.exit(1);
      }
    } else if (arg === '--help' || arg === '-h') {
      showHelp();
      process.exit(0);
    }
  }
  
  if (!options.modules || options.modules.length === 0) {
    options.modules = ['Leads']; // Default if --modules is not provided
  }
  
  return options;
}

function showHelp() {
  console.log(`
🚀 Bulk Sync Daemon Usage:
  node syncDaemon.js [options]

Options:
  --modules, -m <M1,M2,...>    Comma-separated list of modules to sync (e.g., Leads,Contacts). Defaults to 'Leads'.
  --frequency, -f "cron"       Cron expression for sync frequency (e.g., "*/5 * * * *"). Defaults to every minute.
  --verbose, -v                Enable detailed logging.
  --dry-run                    Preview mode - shows what would be synced without making changes.
  --no-delete                  Skip deletion checks and operations.
  --help, -h                   Show this help message.

Examples:
  node syncDaemon.js                                     # Sync Leads every minute
  node syncDaemon.js --modules Leads,Contacts            # Sync multiple modules every minute
  node syncDaemon.js --frequency "*/5 * * * *" -v        # Sync every 5 minutes with verbose logging
  node syncDaemon.js --modules Partners --dry-run        # Preview Partners sync every minute

Cron Format: <minute> <hour> <day_of_month> <month> <day_of_week>
  "*/1 * * * *"  = Every minute
  "*/5 * * * *"  = Every 5 minutes
  "0 * * * *"    = Every hour
  "0 */2 * * *"  = Every 2 hours
  "0 9-17 * * *" = Every hour from 9 AM to 5 PM

Keyboard Shortcuts (while running):
  Ctrl+C: Stop daemon
  Ctrl+Z: Show live stats
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
  process.on('SIGTSTP', () => { // Ctrl+Z
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