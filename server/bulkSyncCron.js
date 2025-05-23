#!/usr/bin/env node

/**
 * Bulk Sync Cron Job
 * 
 * Runs bulk sync for multiple modules on a schedule using cron expressions.
 * Unlike syncDaemon.js which does incremental polling, this performs full bulk syncs.
 * 
 * Usage:
 * - Default (Leads + Partners every hour): node bulkSyncCron.js
 * - Custom modules: node bulkSyncCron.js --modules Leads,Partners,Contacts
 * - Custom schedule: node bulkSyncCron.js --cron "0 star/2 star star star" (every 2 hours)
 * - Dry run: node bulkSyncCron.js --dry-run
 * - Verbose: node bulkSyncCron.js --verbose
 */

const cron = require('node-cron');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs').promises;

class BulkSyncCron {
  constructor(options = {}) {
    this.modules = options.modules || ['Leads', 'Partners'];
    this.cronExpression = options.cron || '0 * * * *'; // Default: every hour
    this.dryRun = options.dryRun || false;
    this.verbose = options.verbose || false;
    this.runNow = options.runNow || false;
    this.logDir = path.join(__dirname, 'logs', 'bulk-sync');
    this.task = null;
    this.isRunning = false;
    this.stats = {
      totalRuns: 0,
      successfulRuns: 0,
      failedRuns: 0,
      moduleStats: {},
      lastRunStart: null,
      lastRunEnd: null
    };

    // Initialize module stats
    this.modules.forEach(module => {
      this.stats.moduleStats[module] = {
        runs: 0,
        successful: 0,
        failed: 0,
        lastError: null
      };
    });
  }

  async ensureLogDirectory() {
    try {
      await fs.mkdir(this.logDir, { recursive: true });
    } catch (error) {
      console.error('Failed to create log directory:', error);
    }
  }

  async start() {
    await this.ensureLogDirectory();
    
    console.log('🚀 Starting Bulk Sync Cron Job');
    console.log('=====================================');
    console.log(`📋 Modules: ${this.modules.join(', ')}`);
    console.log(`⏰ Schedule: ${this.cronExpression}`);
    console.log(`🎯 Mode: ${this.dryRun ? 'DRY RUN' : 'LIVE SYNC'}`);
    console.log(`📊 Verbose: ${this.verbose ? 'ON' : 'OFF'}`);
    console.log(`📁 Logs: ${this.logDir}`);
    console.log('');

    // Validate cron expression
    if (!cron.validate(this.cronExpression)) {
      console.error('❌ Invalid cron expression:', this.cronExpression);
      process.exit(1);
    }

    this.isRunning = true;

    // Schedule the cron job
    this.task = cron.schedule(this.cronExpression, () => {
      this.runBulkSync();
    });

    console.log('✅ Cron job scheduled. Waiting for next execution...');
    console.log('   Press Ctrl+C to stop\n');

    // Run initial sync if requested
    if (this.runNow) {
      console.log('🔄 Running initial sync...');
      await this.runBulkSync();
    }
  }

  async runBulkSync() {
    if (!this.isRunning) return;

    this.stats.totalRuns++;
    this.stats.lastRunStart = new Date();
    const runId = this.stats.lastRunStart.toISOString().replace(/[:.]/g, '-');
    
    console.log(`\n🔄 [${this.stats.lastRunStart.toISOString()}] Starting bulk sync run #${this.stats.totalRuns}`);
    
    let allSuccess = true;

    for (const module of this.modules) {
      console.log(`\n📦 Syncing ${module}...`);
      const startTime = Date.now();
      
      try {
        const success = await this.runModuleSync(module, runId);
        const duration = Math.round((Date.now() - startTime) / 1000);
        
        this.stats.moduleStats[module].runs++;
        
        if (success) {
          this.stats.moduleStats[module].successful++;
          console.log(`✅ ${module} sync completed in ${duration}s`);
        } else {
          this.stats.moduleStats[module].failed++;
          allSuccess = false;
          console.log(`❌ ${module} sync failed after ${duration}s`);
        }
      } catch (error) {
        this.stats.moduleStats[module].failed++;
        this.stats.moduleStats[module].lastError = error.message;
        allSuccess = false;
        console.error(`❌ ${module} sync error:`, error.message);
      }
    }

    this.stats.lastRunEnd = new Date();
    const totalDuration = Math.round((this.stats.lastRunEnd - this.stats.lastRunStart) / 1000);

    if (allSuccess) {
      this.stats.successfulRuns++;
      console.log(`\n✅ Bulk sync run #${this.stats.totalRuns} completed successfully in ${totalDuration}s`);
    } else {
      this.stats.failedRuns++;
      console.log(`\n❌ Bulk sync run #${this.stats.totalRuns} completed with errors in ${totalDuration}s`);
    }

    this.showStats();
  }

  async runModuleSync(module, runId) {
    return new Promise((resolve, reject) => {
      const args = ['bulkSync.js', '--module', module];
      
      if (this.dryRun) args.push('--dry-run');
      if (this.verbose) args.push('--verbose');

      const logFile = path.join(this.logDir, `${runId}-${module}.log`);
      const logStream = require('fs').createWriteStream(logFile);

      const child = spawn('node', args, {
        cwd: __dirname,
        env: process.env
      });

      let output = '';

      child.stdout.on('data', (data) => {
        const text = data.toString();
        output += text;
        logStream.write(text);
        if (this.verbose) {
          process.stdout.write(`  [${module}] ${text}`);
        }
      });

      child.stderr.on('data', (data) => {
        const text = data.toString();
        output += text;
        logStream.write(text);
        if (this.verbose) {
          process.stderr.write(`  [${module}] ${text}`);
        }
      });

      child.on('close', (code) => {
        logStream.end();
        
        // Extract summary from output
        const summaryMatch = output.match(/✨ Success Rate[^:]+: (\d+)%/);
        if (summaryMatch && !this.verbose) {
          const successRate = parseInt(summaryMatch[1]);
          console.log(`  Success rate: ${successRate}%`);
        }

        if (code === 0) {
          resolve(true);
        } else {
          reject(new Error(`Process exited with code ${code}`));
        }
      });

      child.on('error', (error) => {
        logStream.end();
        reject(error);
      });
    });
  }

  showStats() {
    console.log('\n📊 Cron Job Statistics');
    console.log('====================');
    console.log(`Total Runs: ${this.stats.totalRuns} (${this.stats.successfulRuns} successful, ${this.stats.failedRuns} failed)`);
    console.log('\nModule Statistics:');
    
    for (const [module, stats] of Object.entries(this.stats.moduleStats)) {
      console.log(`\n${module}:`);
      console.log(`  Runs: ${stats.runs}`);
      console.log(`  Successful: ${stats.successful}`);
      console.log(`  Failed: ${stats.failed}`);
      if (stats.lastError) {
        console.log(`  Last Error: ${stats.lastError}`);
      }
    }
  }

  stop() {
    if (this.task) {
      this.task.stop();
      this.task = null;
    }
    this.isRunning = false;
    console.log('\n🛑 Bulk sync cron job stopped');
    this.showStats();
  }
}

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {};

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    
    switch (arg) {
      case '--modules':
        if (args[i + 1]) {
          options.modules = args[i + 1].split(',').map(m => m.trim()).filter(m => m);
          i++;
        } else {
          console.error('Error: --modules requires a comma-separated list');
          process.exit(1);
        }
        break;
        
      case '--cron':
        if (args[i + 1]) {
          options.cron = args[i + 1];
          i++;
        } else {
          console.error('Error: --cron requires a cron expression');
          process.exit(1);
        }
        break;
        
      case '--dry-run':
        options.dryRun = true;
        break;
        
      case '--verbose':
      case '-v':
        options.verbose = true;
        break;
        
      case '--run-now':
        options.runNow = true;
        break;
        
      case '--help':
      case '-h':
        showHelp();
        process.exit(0);
        break;
        
      default:
        console.error(`Unknown argument: ${arg}`);
        showHelp();
        process.exit(1);
    }
  }

  return options;
}

function showHelp() {
  console.log(`
Bulk Sync Cron Job

Usage: node bulkSyncCron.js [options]

Options:
  --modules <list>    Comma-separated list of modules to sync
                      Default: Leads,Partners
                      
  --cron <expression> Cron expression for scheduling
                      Default: "0 * * * *" (every hour)
                      Examples:
                        "*/15 * * * *"     Every 15 minutes
                        "0 */2 * * *"      Every 2 hours
                        "0 0 * * *"        Daily at midnight
                        "0 9-17 * * 1-5"   Every hour 9AM-5PM weekdays
                        
  --dry-run           Run in dry-run mode (no actual changes)
  
  --verbose, -v       Show detailed output
  
  --run-now           Run an initial sync immediately
  
  --help, -h          Show this help message

Examples:
  # Run default (Leads + Partners every hour)
  node bulkSyncCron.js
  
  # Run every 30 minutes for Leads only
  node bulkSyncCron.js --modules Leads --cron "*/30 * * * *"
  
  # Run daily at 2 AM for multiple modules
  node bulkSyncCron.js --modules Leads,Partners,Contacts --cron "0 2 * * *"
  
  # Test with dry run and immediate execution
  node bulkSyncCron.js --dry-run --run-now --verbose
`);
}

// Main execution
if (require.main === module) {
  const options = parseArgs();
  const cronJob = new BulkSyncCron(options);

  // Handle graceful shutdown
  process.on('SIGINT', () => {
    console.log('\n\n🛑 Received interrupt signal...');
    cronJob.stop();
    process.exit(0);
  });

  process.on('SIGTERM', () => {
    console.log('\n\n🛑 Received termination signal...');
    cronJob.stop();
    process.exit(0);
  });

  // Start the cron job
  cronJob.start().catch(error => {
    console.error('Failed to start cron job:', error);
    process.exit(1);
  });
}

module.exports = BulkSyncCron;