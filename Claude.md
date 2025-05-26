# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a polling-based two-way sync service between Zoho CRM and Airtable, designed for syncing multiple modules including Leads, Partners, Contacts, and Accounts. The service uses a polling-based architecture to detect changes in both systems and synchronize data based on modification timestamps.

## Architecture

### Core Components

1. **Polling Service** (`server/src/services/pollingService.js`)
   - Polls both Zoho and Airtable for changes since last sync
   - Resolves conflicts based on modification timestamps (newer wins, Zoho wins ties)
   - Handles creation of new records in both directions

2. **Sync Service** (`server/src/services/syncService.js`)
   - Core sync logic for field-level synchronization
   - Handles field mapping between systems
   - Prevents sync loops using sync tracking

3. **Field Mapping System**
   - Dynamic field mapping from Airtable "Zoho Fields" table
   - Caches mappings for performance
   - Falls back to static config if dynamic mapping unavailable

4. **Service Layer**
   - `zohoService.js`: Zoho CRM API integration
   - `airtableService.js`: Airtable API integration
   - `syncExecutionService.js`: Executes full record syncs

## Common Development Commands

### Running the Sync Service

```bash
cd server

# Test connections to both systems
npm test

# Single manual sync
npm run sync

# Check sync status
npm run sync:status

# Full sync (30 days back)
npm run sync:full

# Custom time window
node syncManager.js --since "2024-01-15T10:00:00.000Z"

# Start automatic sync daemon (every minute)
npm run sync:daemon              # Leads only (default)
npm run sync:daemon:leads        # Leads only
npm run sync:daemon:leads:verbose # Leads with verbose logging
npm run sync:daemon:contacts     # Contacts only
npm run sync:daemon:multi        # Leads, Contacts, Accounts
npm run sync:daemon:multi:verbose # Multiple modules with verbose

# Custom modules for daemon
node syncDaemon.js --modules Leads,Partners
node syncDaemon.js --modules Leads,Partners --verbose

# Custom sync intervals
node syncDaemon.js --frequency "*/2 * * * *"  # Every 2 minutes
node syncDaemon.js --frequency "*/5 * * * *"  # Every 5 minutes
node syncDaemon.js --frequency "0 * * * *"    # Every hour
```

### Bulk Sync Operations

```bash
# Bulk sync for specific modules
npm run sync:bulk                      # Default to Leads
npm run sync:bulk:leads                # Leads module
npm run sync:bulk:leads:preview        # Dry run for Leads
npm run sync:bulk:leads:verbose        # Verbose logging for Leads
npm run sync:bulk:contacts             # Contacts module
npm run sync:bulk:contacts:preview     # Dry run for Contacts
npm run sync:bulk:partners             # Partners module
npm run sync:bulk:partners:preview     # Dry run for Partners

# Cron-based bulk sync
npm run sync:cron                      # Default hourly sync
npm run sync:cron:hourly               # Every hour for Leads & Partners
npm run sync:cron:daily                # Daily at 2 AM for Leads & Partners
npm run sync:cron:test                 # Test run with immediate execution

# Custom cron schedules
node bulkSyncCron.js --modules Leads,Partners --cron "*/30 * * * *"  # Every 30 minutes
node bulkSyncCron.js --modules Leads,Partners,Contacts --verbose
node bulkSyncCron.js --dry-run --run-now  # Test without making changes
```

### Field Mapping Management

```bash
# Export field mappings
npm run export:mappings                      # All modules
npm run export:mappings:preview              # Preview all modules
npm run export:mappings:leads               # Leads module only
npm run export:mappings:leads:preview       # Preview Leads mappings
```

### Duplicate Record Removal

```bash
# Remove duplicate records (keeps most recent based on modification time)
npm run duplicates:remove                           # Default to Leads
npm run duplicates:remove:leads                     # Leads module
npm run duplicates:remove:leads:preview             # Dry run for Leads (safe preview)
npm run duplicates:remove:leads:verbose             # Verbose dry run for Leads
npm run duplicates:remove:partners                  # Partners module
npm run duplicates:remove:partners:preview          # Dry run for Partners
npm run duplicates:remove:contacts                  # Contacts module
npm run duplicates:remove:contacts:preview          # Dry run for Contacts

# Custom options
node removeDuplicates.js --module Leads --dry-run --verbose
```

### Setup Commands

```bash
cd setup

# Initial Zoho setup
npm run setup-zoho

# Refresh Zoho tokens
npm run refresh-token
```

## Key Configuration Files

- **Zoho Config**: `setup/zoho-config.json` - Contains Zoho API credentials and tokens
- **Airtable Config**: `setup/airtable-config.json` - Contains Airtable API key and base info
- **Field Mapping**: Dynamically loaded from Airtable "Zoho Fields" table

## Important Development Notes

### Field Mapping & Ignore Logic
- Field mappings are loaded dynamically from Airtable's "Zoho Fields" table
- The system caches these mappings for performance using `fieldMappingCache`
- Field ignore logic is centralized in `src/config/config.js` via `shouldIgnoreField(fieldName, system)`
- Always ensure field IDs start with "fld" for Airtable fields
- The mapping format: `{ zohoFieldName: { airtable: 'fldXXXX', zoho: 'Zoho_Field_Name' } }`
- Zoho system fields starting with '$' are automatically ignored
- Check field mapping issues by verifying the "Zoho Fields" table configuration

### Sync Behavior
- The service tracks modification timestamps to determine sync direction
- Conflicts (changes within 1 minute) are resolved with Zoho winning
- The system prevents sync loops by tracking recent sync operations
- Deletions are handled with a 24-hour grace period for new records
- Sync flow: Polling → Compare Modified_Time → Resolve conflicts → Complete sync

### API Rate Limits
- Be mindful of both Zoho and Airtable API rate limits
- The polling service runs every minute by default
- Bulk operations should use the dedicated bulk sync commands
- Consider using staggered schedules for multiple modules

### Error Handling
- The service continues operation even if individual syncs fail
- Failed syncs are logged but don't stop the daemon
- Check logs for specific sync failures
- Log locations:
  - Bulk sync logs: `server/logs/bulk-sync/`
  - Log format: `YYYY-MM-DDTHH-mm-ss-Module.log`

### Daemon Output Indicators
- `.` = Sync completed, no changes
- `✓` = Sync completed with successful updates
- `!` = Sync completed with some failures
- `X` = Sync failed completely

## Testing & Debugging

- Use `npm run sync:daemon:verbose` for detailed logging during development
- The `--dry-run` flag on bulk operations shows what would be synced without making changes
- Check field mapping status with the `/cache-status` endpoint when server is running
- Press Ctrl+Z in non-verbose daemon mode to show live stats
- Debug commands:
  ```bash
  # Test cron expression
  node bulkSyncCron.js --cron "*/5 * * * *" --dry-run --verbose
  
  # Check field mappings
  node bulkSync.js --module Partners --dry-run --verbose
  
  # Verify module configuration
  node testModuleConfig.js Partners
  ```

## Production Deployment

### Using PM2 (Recommended)

```bash
# Install PM2 globally
npm install -g pm2

# Start bulk sync daemon (recommended)
pm2 start syncDaemon.js --name "bulk-sync-daemon" -- --modules Leads,Partners

# Start bulk sync daemon with custom frequency
pm2 start syncDaemon.js --name "bulk-sync-5min" -- --modules Leads,Partners --frequency "*/5 * * * *"

# Save PM2 configuration
pm2 save

# Setup PM2 to start on boot
pm2 startup

# View logs
pm2 logs bulk-sync-daemon
pm2 logs bulk-sync-5min

# Monitor
pm2 monit
```

### Production Setup Examples

```bash
# Hourly sync for critical modules
pm2 start syncDaemon.js --name "hourly-sync" -- --modules Leads,Partners --frequency "0 * * * *"

# Daily full sync for all modules
pm2 start syncDaemon.js --name "daily-sync" -- --modules Leads,Partners,Contacts,Accounts --frequency "0 2 * * *"

# Real-time sync for Leads (every minute)
pm2 start syncDaemon.js --name "realtime-leads" -- --modules Leads --frequency "* * * * *"
```

### Sync Strategy
- **Bulk Sync Daemon (syncDaemon.js)**: Uses bulkSync.js for complete record synchronization at configurable intervals
- **Incremental Sync (pollingService.js)**: For near real-time updates, polls every minute for recent changes (legacy approach)
- **Bulk Sync Scripts (bulkSync.js)**: Direct bulk synchronization for specific modules
- Can run multiple daemons simultaneously for different modules and frequencies

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

### Best Practices
1. Run bulk sync during off-peak hours
2. Start with one module before adding more
3. Always test with `--dry-run` first
4. Monitor log file sizes regularly
5. Set up alerts for sync failures

## Recent Updates & Known Issues

### Field Ignore Logic Centralization (Latest)
- **Issue**: Field ignore logic was scattered across multiple files with hardcoded checks
- **Fix**: Centralized all field ignore logic in `src/config/config.js` via `shouldIgnoreField(fieldName, system)`
  - Enhanced function to handle Zoho system fields starting with '$'
  - Updated all sync services to use centralized function
  - Removed duplicate implementations across `syncService.js`, `syncExecutionService.js`, and `bulkSync.js`
- **Impact**: Consistent field filtering behavior across entire codebase

### New Bulk Sync Daemon
- **Enhancement**: Created new `syncDaemon.js` that uses the latest `bulkSync.js` implementation
- **Features**: Configurable modules (`--modules`) and frequency (`--frequency`) variables
- **Benefits**: No deletion logic, uses centralized field ignore logic, full PM2 compatibility
- **Usage**: `node syncDaemon.js --modules Leads,Partners --frequency "*/5 * * * *"`

### Critical Performance Fix
- **Issue**: During bulk sync updates, the system was attempting to create new Airtable records instead of updating existing ones
- **Cause**: The sync service's `findOrCreateAirtableRecord` function was being called for every field update, and when it couldn't find records (due to lookup issues), it tried to create duplicates
- **Fix**: Modified bulk sync to pass known record IDs directly to sync functions, avoiding unnecessary lookups and record creation attempts
  - Updated `handleZohoRecordUpdate`, `syncFromZohoToAirtable`, and `syncFromAirtableToZoho` to accept optional `knownAirtableRecordId`/`knownZohoRecordId` parameters
  - Modified `bulkSync.js` to pass these IDs when calling sync functions
- **Impact**: Prevents rate limit errors and duplicate record creation during updates

### Performance Optimizations

1. **Parallel Processing** (batches of 10 records)
   - Modified `executeBulkSyncPlan` in `bulkSync.js` to use `Promise.all()` for concurrent operations
   - Processes records in chunks to avoid overwhelming API rate limits

2. **Field Mapping Caching**
   - Added `fieldIdToNameCache` Map in `airtableService.js` to cache field mappings
   - Prevents repeated API calls to fetch field mappings during bulk operations
   - Cache persists for the duration of the process

3. **Field Value Comparison**
   - Added `compareFieldValues()` and `normalizeValue()` methods in `bulkSync.js`
   - Skips syncs when field values haven't changed (only timestamps differ)
   - Reduces unnecessary API calls and sync operations

4. **Progress Tracking**
   - Real-time progress indicators during bulk sync operations
   - Shows current record being processed (e.g., "Processing record 5 of 100")

### Implementation Details

#### Bulk Sync for Specific Records
```bash
# Sync a single Zoho record
node bulkSync.js --module Partners --zoho-id 12345678901234567

# Sync a single Airtable record  
node bulkSync.js --module Partners --airtable-id recXXXXXXXXXXXXXX
```

#### Field Value Comparison Logic
The system now compares actual field values before syncing:
- Empty strings, null, undefined, and "null" are treated as equivalent
- Number comparisons handle string/number type differences
- Arrays and objects use JSON.stringify for comparison
- Only syncs when actual values differ, not just timestamps

### Known Issues
1. **Multiple API calls per record**: Currently makes one API request per field update instead of batching all fields together
   - Each field update is a separate PATCH request to Airtable
   - Could be optimized to update all fields in a single request
   
2. **Field mapping lookups**: "Zoho CRM ID mapping not found in dynamic cache" warnings for Partners module
   - Occurs when field mappings aren't properly configured in Airtable
   - Non-critical but indicates configuration issues
   
3. **Rate limits**: Can hit Airtable's 5 requests/second limit during large syncs
   - Mitigated by batch processing (10 records at a time)
   - Consider adding retry logic with exponential backoff

## Troubleshooting

### Common Issues

1. **Field Mapping Issues**
   - Check your "Zoho Fields" table in Airtable
   - Ensure field IDs start with "fld"
   - Verify field names match exactly
   - Ensure field types match between systems

2. **API Issues**
   - Run `npm test` to verify connections
   - Check token expiry in config files
   - Verify API permissions

3. **Sync Issues**
   - Check logs for specific error messages
   - Ensure required fields are populated
   - Verify modification timestamps are being updated

4. **Module Not Found Error**
   - Ensure the module exists in Airtable's "Zoho Modules" table
   - Check module name spelling (case-sensitive)

5. **Permission Errors**
   - Ensure the user has write access to log directory
   - Check file permissions on scripts

6. **Duplicate Record Creation**
   - If sync tries to create records when it should update, check that Zoho CRM ID field is properly mapped
   - Verify records have matching Zoho IDs in Airtable
   - Check for formatting differences in ID fields

## General Development Guidelines

* Always read entire files. Otherwise, you don't know what you don't know, and will end up making mistakes, duplicating code that already exists, or misunderstanding the architecture.  
* Commit early and often. When working on large tasks, your task could be broken down into multiple logical milestones. After a certain milestone is completed and confirmed to be ok by the user, you should commit it. If you do not, if something goes wrong in further steps, we would need to end up throwing away all the code, which is expensive and time consuming.  
* Your internal knowledgebase of libraries might not be up to date. When working with any external library, unless you are 100% sure that the library has a super stable interface, you will look up the latest syntax and usage via either Perplexity (first preference) or web search (less preferred, only use if Perplexity is not available)  
* Do not say things like: "x library isn't working so I will skip it". Generally, it isn't working because you are using the incorrect syntax or patterns. This applies doubly when the user has explicitly asked you to use a specific library, if the user wanted to use another library they wouldn't have asked you to use a specific one in the first place.  
* Always run linting after making major changes. Otherwise, you won't know if you've corrupted a file or made syntax errors, or are using the wrong methods, or using methods in the wrong way.   
* Please organise code into separate files wherever appropriate, and follow general coding best practices about variable naming, modularity, function complexity, file sizes, commenting, etc.  
* Code is read more often than it is written, make sure your code is always optimised for readability  
* Unless explicitly asked otherwise, the user never wants you to do a "dummy" implementation of any given task. Never do an implementation where you tell the user: "This is how it *would* look like". Just implement the thing.  
* Whenever you are starting a new task, it is of utmost importance that you have clarity about the task. You should ask the user follow up questions if you do not, rather than making incorrect assumptions.  
* Do not carry out large refactors unless explicitly instructed to do so.  
* When starting on a new task, you should first understand the current architecture, identify the files you will need to modify, and come up with a Plan. In the Plan, you will think through architectural aspects related to the changes you will be making, consider edge cases, and identify the best approach for the given task. Get your Plan approved by the user before writing a single line of code.   
* If you are running into repeated issues with a given task, figure out the root cause instead of throwing random things at the wall and seeing what sticks, or throwing in the towel by saying "I'll just use another library / do a dummy implementation".   
* You are an incredibly talented and experienced polyglot with decades of experience in diverse areas such as software architecture, system design, development, UI & UX, copywriting, and more.  
* When doing UI & UX work, make sure your designs are both aesthetically pleasing, easy to use, and follow UI / UX best practices. You pay attention to interaction patterns, micro-interactions, and are proactive about creating smooth, engaging user interfaces that delight users.   
* When you receive a task that is very large in scope or too vague, you will first try to break it down into smaller subtasks. If that feels difficult or still leaves you with too many open questions, push back to the user and ask them to consider breaking down the task for you, or guide them through that process. This is important because the larger the task, the more likely it is that things go wrong, wasting time and energy for everyone involved.