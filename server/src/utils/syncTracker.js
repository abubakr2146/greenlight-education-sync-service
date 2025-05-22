// Track recent syncs to prevent infinite loops
const recentSyncs = new Map();
const SYNC_COOLDOWN_MS = 10000; // 10 seconds cooldown

// Function to check if we should skip sync to prevent loops
function shouldSkipSync(source, recordId, fieldName, newValue) {
  const syncKey = `${source}:${recordId}:${fieldName}`;
  const now = Date.now();
  
  if (recentSyncs.has(syncKey)) {
    const lastSync = recentSyncs.get(syncKey);
    if ((now - lastSync.timestamp) < SYNC_COOLDOWN_MS) {
      // Check if the value is the same as what we just synced
      if (JSON.stringify(lastSync.value) === JSON.stringify(newValue)) {
        console.log(`⏭️  Skipping sync - recently synced ${syncKey} with same value`);
        return true;
      }
    }
  }
  
  // Record this sync
  recentSyncs.set(syncKey, { timestamp: now, value: newValue });
  
  // Clean up old entries (older than cooldown period)
  for (const [key, data] of recentSyncs.entries()) {
    if ((now - data.timestamp) > SYNC_COOLDOWN_MS) {
      recentSyncs.delete(key);
    }
  }
  
  return false;
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

module.exports = {
  shouldSkipSync,
  clearSyncHistory,
  getSyncStatus,
  SYNC_COOLDOWN_MS
};