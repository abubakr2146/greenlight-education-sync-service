# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a two-way sync service between Zoho CRM and Airtable, specifically designed for syncing Leads. The service uses a polling-based architecture to detect changes in both systems and synchronize data based on modification timestamps.

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

# Start automatic sync daemon (every minute)
npm run sync:daemon

# Daemon with verbose logging
npm run sync:daemon:verbose

# Bulk sync operations
npm run sync:bulk
npm run sync:bulk:preview    # Dry run
npm run sync:bulk:verbose   # With detailed logs
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

### Field Mapping
- Field mappings are loaded dynamically from Airtable's "Zoho Fields" table
- The system caches these mappings for performance
- Always ensure field IDs start with "fld" for Airtable fields
- The mapping format: `{ zohoFieldName: { airtable: 'fldXXXX', zoho: 'Zoho_Field_Name' } }`

### Sync Behavior
- The service tracks modification timestamps to determine sync direction
- Conflicts (changes within 1 minute) are resolved with Zoho winning
- The system prevents sync loops by tracking recent sync operations
- Deletions are handled with a 24-hour grace period for new records

### API Rate Limits
- Be mindful of both Zoho and Airtable API rate limits
- The polling service runs every minute by default
- Bulk operations should use the dedicated bulk sync commands

### Error Handling
- The service continues operation even if individual syncs fail
- Failed syncs are logged but don't stop the daemon
- Check logs for specific sync failures

## Testing & Debugging

- Use `npm run sync:daemon:verbose` for detailed logging during development
- The `--dry-run` flag on bulk operations shows what would be synced without making changes
- Check field mapping status with the `/cache-status` endpoint when server is running

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