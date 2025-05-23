// Track recent syncs to prevent infinite loops
const recentSyncs = new Map();
const recentRecordSyncs = new Map(); // Track record-level syncs for polling
const SYNC_COOLDOWN_MS = 10000; // 10 seconds cooldown
const RECORD_SYNC_COOLDOWN_MS = 120000; // 2 minutes cooldown for record-level syncs

// Function to record a specific field value sync
function recordSyncedValue(source, recordId, fieldName, newValue) {
  const syncKey = `${source}:${recordId}:${fieldName}`;
  const now = Date.now();
  recentSyncs.set(syncKey, { timestamp: now, value: newValue });

  // Clean up old entries (older than cooldown period) - can be run less frequently if performance is an issue
  // For simplicity, running it here.
  for (const [key, data] of recentSyncs.entries()) {
    if ((now - data.timestamp) > SYNC_COOLDOWN_MS) {
      recentSyncs.delete(key);
    }
  }
}

// Function to check if we should skip sync to prevent loops
function shouldSkipSync(source, recordId, fieldName, newValue) {
  const syncKey = `${source}:${recordId}:${fieldName}`;
  const now = Date.now();
  
  if (recentSyncs.has(syncKey)) {
    const lastSync = recentSyncs.get(syncKey);
    if ((now - lastSync.timestamp) < SYNC_COOLDOWN_MS) {
      // Check if the value is the same as what we just synced
      if (JSON.stringify(lastSync.value) === JSON.stringify(newValue)) {
        return true; // Skip if same value within cooldown
      }
    }
  }
  
  // If not skipping, record this attempt (or successful sync)
  recordSyncedValue(source, recordId, fieldName, newValue);
  
  return false; // Don't skip
}

// Clear sync history for a specific key (useful for testing)
function clearSyncHistory(source, recordId, fieldName) {
  const syncKey = `${source}:${recordId}:${fieldName}`;
  recentSyncs.delete(syncKey);
}

// Get current sync status (useful for debugging)
function getSyncStatus() {
  const now = Date.now();
  const activeSyncs = [];
  
  for (const [key, data] of recentSyncs.entries()) {
    if ((now - data.timestamp) < SYNC_COOLDOWN_MS) {
      activeSyncs.push({
        key,
        timestamp: data.timestamp,
        value: data.value,
        age: now - data.timestamp
      });
    }
  }
  
  return activeSyncs;
}

// Function to check if we should skip record-level sync (for polling)
function shouldSkipRecordSync(source, recordId) {
  const syncKey = `${source}:${recordId}`;
  const now = Date.now();
  
  if (recentRecordSyncs.has(syncKey)) {
    const lastSync = recentRecordSyncs.get(syncKey);
    if ((now - lastSync.timestamp) < RECORD_SYNC_COOLDOWN_MS) {
      return true; // Skip this record sync
    }
  }
  
  return false; // Allow sync
}

// Record that we just synced a record (for polling)
function recordRecordSync(source, recordId) {
  const syncKey = `${source}:${recordId}`;
  const now = Date.now();
  
  recentRecordSyncs.set(syncKey, { timestamp: now });
  
  // Clean up old entries
  for (const [key, data] of recentRecordSyncs.entries()) {
    if ((now - data.timestamp) > RECORD_SYNC_COOLDOWN_MS) {
      recentRecordSyncs.delete(key);
    }
  }
}

// Clear record sync history for a specific record
function clearRecordSyncHistory(source, recordId) {
  const syncKey = `${source}:${recordId}`;
  recentRecordSyncs.delete(syncKey);
}

// Get current record sync status
function getRecordSyncStatus() {
  const now = Date.now();
  const activeRecordSyncs = [];
  
  for (const [key, data] of recentRecordSyncs.entries()) {
    if ((now - data.timestamp) < RECORD_SYNC_COOLDOWN_MS) {
      activeRecordSyncs.push({
        key,
        timestamp: data.timestamp,
        age: now - data.timestamp
      });
    }
  }
  
  return activeRecordSyncs;
}

module.exports = {
  shouldSkipSync,
  recordSyncedValue, // Added export
  clearSyncHistory,
  getSyncStatus,
  shouldSkipRecordSync,
  recordRecordSync,
  clearRecordSyncHistory,
  getRecordSyncStatus,
  SYNC_COOLDOWN_MS,
  RECORD_SYNC_COOLDOWN_MS
};
