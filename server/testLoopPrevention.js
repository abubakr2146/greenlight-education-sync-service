#!/usr/bin/env node

/**
 * Test Loop Prevention
 * 
 * Quick test to verify loop prevention is working
 */

const { performSync, getSyncStatus } = require('./src/services/pollingService');
const fieldMappingCache = require('./src/utils/fieldMappingCache');

async function testLoopPrevention() {
  console.log('🧪 Testing Loop Prevention');
  console.log('==========================\n');
  
  try {
    // Initialize field mapping cache
    console.log('🔧 Initializing field mapping cache...');
    await fieldMappingCache.initialize();
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    console.log('✅ Field mapping cache ready\n');
    
    // Run first sync
    console.log('1️⃣  Running first sync...');
    const result1 = await performSync();
    console.log(`   Result: ${result1.successful} successful, ${result1.failed} failed\n`);
    
    // Check status
    const status = getSyncStatus();
    console.log(`📊 Status: ${status.activeRecordSyncs} records in cooldown\n`);
    
    // Run second sync immediately (should skip recently synced records)
    console.log('2️⃣  Running second sync (should skip recent syncs)...');
    const result2 = await performSync();
    console.log(`   Result: ${result2.successful} successful, ${result2.failed} failed\n`);
    
    // Check status again
    const status2 = getSyncStatus();
    console.log(`📊 Status: ${status2.activeRecordSyncs} records in cooldown`);
    console.log('📋 Recently synced records:');
    status2.recentlySyncedRecords.forEach(record => {
      const ageSeconds = Math.round(record.age / 1000);
      console.log(`   - ${record.key} (${ageSeconds}s ago)`);
    });
    
    console.log('\n🎉 Loop prevention test completed!');
    console.log('💡 Records synced in the first run should be skipped in the second run');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  } finally {
    fieldMappingCache.destroy();
  }
}

testLoopPrevention();