# 🔄 Zoho-Airtable Sync Service

A polling-based two-way sync service between Zoho CRM Leads and Airtable records.

## 🚀 Quick Start

### 1. Install Dependencies
```bash
cd server
npm install
```

### 2. Test Connections
```bash
npm test
```

### 3. Run Sync Options

#### 🔄 **Start Automatic Sync (Every Minute)**
```bash
# Basic mode (shows dots for activity)
npm run sync:daemon

# Verbose mode (detailed logs)
npm run sync:daemon:verbose
```

#### 🎯 **Manual Sync Commands**
```bash
# Single sync run
npm run sync

# Check sync status
npm run sync:status

# Full sync (30 days back)
npm run sync:full

# Custom time window
node syncManager.js --since "2024-01-15T10:00:00.000Z"
```

## 📊 Understanding the Output

### Daemon Mode (Basic)
- `.` = Sync completed, no changes
- `✓` = Sync completed with successful updates
- `!` = Sync completed with some failures
- `X` = Sync failed completely

### Daemon Mode (Verbose)
- Shows detailed logs for each sync cycle
- Displays records synced, conflicts resolved, etc.

## ⚙️ Configuration

The sync service uses your existing configuration:
- **Zoho Config**: `../setup/zoho-config.json`
- **Airtable Config**: `../setup/airtable-config.json`
- **Field Mapping**: Dynamic from Airtable "Zoho Fields" table

## 🔧 Advanced Usage

### Custom Sync Intervals
```bash
# Every 2 minutes
node syncDaemon.js --interval "*/2 * * * *"

# Every 5 minutes
node syncDaemon.js --interval "*/5 * * * *"

# Every hour
node syncDaemon.js --interval "0 * * * *"
```

### Monitoring
```bash
# In non-verbose mode, press Ctrl+Z to show live stats
# Ctrl+C to stop the daemon
```

## 🔄 How It Works

1. **Polling**: Checks both systems every minute for changes
2. **Comparison**: Compares `Modified_Time` vs `Last Modified Time`
3. **Conflict Resolution**: Newer record wins (Zoho wins ties)
4. **Complete Sync**: Syncs all mapped fields from source of truth
5. **Field Mapping**: Uses your existing Zoho Fields table configuration

## 📈 Sync Flow

```
┌─────────────┐    ┌─────────────┐
│    Zoho     │    │  Airtable   │
│   Leads     │◄──►│   Leads     │
└─────────────┘    └─────────────┘
       │                  │
       ▼                  ▼
┌─────────────────────────────────┐
│      Polling Service            │
│  • Fetch recent changes         │
│  • Compare Modified_Time        │
│  • Resolve conflicts            │
│  • Sync complete records        │
└─────────────────────────────────┘
```

## 🛟 Troubleshooting

### Field Mapping Issues
- Check your "Zoho Fields" table in Airtable
- Ensure field IDs start with "fld"
- Verify field names match exactly

### API Issues
- Run `npm test` to verify connections
- Check token expiry in config files
- Verify API permissions

### Sync Issues
- Check logs for specific error messages
- Verify field types match between systems
- Ensure required fields are populated

## 📝 Logs & Monitoring

The daemon provides real-time feedback:
- **Success Rate**: Tracks successful vs failed syncs
- **Record Counts**: Shows how many records were processed
- **Error Details**: Logs specific failures for debugging
- **Performance**: Displays sync duration and timing

Perfect for production environments with proper monitoring and alerting.