# Cron Job Setup for Bulk Sync

This guide explains how to set up automated bulk synchronization between Zoho CRM and Airtable for multiple modules using cron jobs.

## Overview

The system provides two types of scheduled synchronization:

1. **Incremental Sync (syncDaemon.js)** - Polls for recent changes every minute
2. **Bulk Sync (bulkSyncCron.js)** - Performs full synchronization on a schedule

## Quick Start

### Run Bulk Sync for Leads and Partners

```bash
# Default: Syncs Leads and Partners every hour
npm run sync:cron

# Test run with immediate execution
npm run sync:cron:test

# Daily sync at 2 AM
npm run sync:cron:daily

# Custom schedule (every 30 minutes)
node bulkSyncCron.js --modules Leads,Partners --cron "*/30 * * * *"
```

## Bulk Sync Cron (Recommended for Full Syncs)

### Basic Usage

```bash
# Run with default settings (Leads + Partners, hourly)
node bulkSyncCron.js

# Specify modules
node bulkSyncCron.js --modules Leads,Partners,Contacts

# Custom schedule
node bulkSyncCron.js --cron "0 */2 * * *"  # Every 2 hours

# Dry run mode (preview without making changes)
node bulkSyncCron.js --dry-run

# Verbose output
node bulkSyncCron.js --verbose

# Run immediately for testing
node bulkSyncCron.js --run-now
```

### Cron Expression Examples

| Expression | Description |
|------------|-------------|
| `0 * * * *` | Every hour at minute 0 |
| `*/15 * * * *` | Every 15 minutes |
| `0 */2 * * *` | Every 2 hours |
| `0 0 * * *` | Daily at midnight |
| `0 2 * * *` | Daily at 2 AM |
| `0 9-17 * * 1-5` | Every hour from 9 AM to 5 PM on weekdays |
| `0 0 * * 0` | Weekly on Sunday at midnight |

### Features

- **Multi-module support**: Sync multiple modules in sequence
- **Logging**: All sync operations are logged to `logs/bulk-sync/`
- **Error handling**: Continues with other modules if one fails
- **Statistics**: Shows success/failure rates for each module
- **Graceful shutdown**: Ctrl+C stops the cron job cleanly

## Incremental Sync Daemon (For Real-time Updates)

### Basic Usage

```bash
# Sync Leads only (default)
npm run sync:daemon

# Sync multiple modules
npm run sync:daemon:multi  # Syncs Leads, Contacts, Accounts

# Custom modules
node syncDaemon.js --modules Leads,Partners

# With verbose logging
node syncDaemon.js --modules Leads,Partners --verbose
```

### Features

- Runs every minute by default
- Only syncs records modified since last sync
- Lower resource usage than bulk sync
- Suitable for keeping systems in near real-time sync

## System Service Setup

### Linux/Ubuntu (systemd)

1. Create a service file:

```bash
sudo nano /etc/systemd/system/zoho-airtable-sync.service
```

2. Add the following content:

```ini
[Unit]
Description=Zoho-Airtable Bulk Sync Service
After=network.target

[Service]
Type=simple
User=your-username
WorkingDirectory=/path/to/greenlight-education-sync-service/server
ExecStart=/usr/bin/node /path/to/greenlight-education-sync-service/server/bulkSyncCron.js --modules Leads,Partners --cron "0 * * * *"
Restart=always
RestartSec=10
StandardOutput=append:/var/log/zoho-airtable-sync.log
StandardError=append:/var/log/zoho-airtable-sync-error.log

[Install]
WantedBy=multi-user.target
```

3. Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable zoho-airtable-sync.service
sudo systemctl start zoho-airtable-sync.service

# Check status
sudo systemctl status zoho-airtable-sync.service

# View logs
sudo journalctl -u zoho-airtable-sync.service -f
```

### Windows (Task Scheduler)

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (e.g., "Daily" or "When computer starts")
4. Set action: Start a program
   - Program: `node.exe`
   - Arguments: `C:\path\to\bulkSyncCron.js --modules Leads,Partners`
   - Start in: `C:\path\to\greenlight-education-sync-service\server`

### Using PM2 (Recommended for Production)

```bash
# Install PM2 globally
npm install -g pm2

# Start bulk sync cron
pm2 start bulkSyncCron.js --name "bulk-sync" -- --modules Leads,Partners

# Start incremental sync daemon
pm2 start syncDaemon.js --name "incremental-sync" -- --modules Leads,Partners

# Save PM2 configuration
pm2 save

# Setup PM2 to start on boot
pm2 startup

# View logs
pm2 logs bulk-sync
pm2 logs incremental-sync

# Monitor
pm2 monit
```

## Monitoring and Logs

### Log Locations

- Bulk sync logs: `server/logs/bulk-sync/`
- Log format: `YYYY-MM-DDTHH-mm-ss-Module.log`

### Viewing Logs

```bash
# View latest logs
tail -f logs/bulk-sync/*.log

# Search for errors
grep -i error logs/bulk-sync/*.log

# View specific module logs
ls -la logs/bulk-sync/*Partners*.log
```

## Best Practices

1. **Choose the Right Sync Method**:
   - Use bulk sync for daily/hourly full synchronization
   - Use incremental sync for near real-time updates
   - Can run both simultaneously for different modules

2. **Schedule Considerations**:
   - Run bulk sync during off-peak hours
   - Consider API rate limits when setting frequency
   - Stagger multiple module syncs if needed

3. **Monitoring**:
   - Set up alerts for sync failures
   - Monitor log file sizes
   - Check sync statistics regularly

4. **Testing**:
   - Always test with `--dry-run` first
   - Start with one module before adding more
   - Use `--run-now` to test schedules

## Troubleshooting

### Common Issues

1. **Module not found error**:
   - Ensure the module exists in Airtable's "Zoho Modules" table
   - Check module name spelling (case-sensitive)

2. **Permission errors**:
   - Ensure the user has write access to log directory
   - Check file permissions on scripts

3. **Sync not running**:
   - Verify cron expression with online validators
   - Check system time and timezone
   - Ensure node process has necessary permissions

### Debug Commands

```bash
# Test cron expression
node bulkSyncCron.js --cron "*/5 * * * *" --dry-run --verbose

# Check field mappings
node bulkSync.js --module Partners --dry-run --verbose

# Verify module configuration
node testModuleConfig.js Partners
```

## Examples

### Production Setup

```bash
# Hourly sync for critical modules
pm2 start bulkSyncCron.js --name "hourly-sync" -- --modules Leads,Partners --cron "0 * * * *"

# Daily full sync for all modules
pm2 start bulkSyncCron.js --name "daily-sync" -- --modules Leads,Partners,Contacts,Accounts --cron "0 2 * * *"

# Real-time sync for Leads
pm2 start syncDaemon.js --name "realtime-leads" -- --modules Leads
```

### Development Setup

```bash
# Test with verbose output and immediate run
node bulkSyncCron.js --modules Leads,Partners --verbose --run-now

# Dry run to preview changes
node bulkSyncCron.js --modules Partners --dry-run --run-now
```