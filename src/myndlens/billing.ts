// src/myndlens/billing.ts
// Usage tracking buffer that flushes to billing API every 60 seconds
// Tracks {agentId, inputTokens, outputTokens, model} and batches for efficiency

interface UsageRecord {
  agentId: string;
  inputTokens: number;
  outputTokens: number;
  model: string;
  timestamp: string;
  tenantId?: string;
}

interface BillingPayload {
  usage_records: UsageRecord[];
  batch_id: string;
  submitted_at: string;
}

interface BillingResponse {
  status: string;
  batch_id: string;
  records_processed: number;
  errors?: string[];
}

// Configuration
const FLUSH_INTERVAL_MS = 60 * 1000; // 60 seconds
const MAX_BUFFER_SIZE = 1000; // Flush early if buffer gets too large
const OBEGEE_URL = process.env.OBEGEE_URL || 'https://api.obegee.co.uk';
const BILLING_ENDPOINT = '/api/billing/usage';

// In-memory buffer for usage records
let usageBuffer: UsageRecord[] = [];
let flushTimer: NodeJS.Timeout | null = null;
let isShuttingDown = false;

/**
 * Validates a usage record has required fields
 */
function isValidUsageRecord(record: Partial<UsageRecord>): record is UsageRecord {
  return !!(record.agentId && 
           typeof record.inputTokens === 'number' && 
           typeof record.outputTokens === 'number' && 
           record.model &&
           record.timestamp);
}

/**
 * Generates a unique batch ID for tracking
 */
function generateBatchId(): string {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substring(2, 8);
  return `batch_${timestamp}_${random}`;
}

/**
 * Sends usage records to billing API
 */
async function sendToBillingAPI(records: UsageRecord[]): Promise<boolean> {
  const timestamp = new Date().toISOString();
  const batchId = generateBatchId();
  
  if (records.length === 0) {
    console.log(`[${timestamp}] No usage records to flush`);
    return true;
  }
  
  const payload: BillingPayload = {
    usage_records: records,
    batch_id: batchId,
    submitted_at: timestamp
  };
  
  try {
    console.log(`[${timestamp}] Flushing ${records.length} usage records to billing API (batch: ${batchId})`);
    
    const response = await fetch(`${OBEGEE_URL}${BILLING_ENDPOINT}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'MyndLens-Billing/1.0'
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(30000) // 30 second timeout
    });
    
    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');
      console.error(`[${timestamp}] Billing API error ${response.status}: ${errorText}`);
      return false;
    }
    
    const result: BillingResponse = await response.json();
    console.log(`[${timestamp}] Billing flush successful: ${result.records_processed} records processed (batch: ${batchId})`);
    
    if (result.errors && result.errors.length > 0) {
      console.warn(`[${timestamp}] Billing API warnings: ${result.errors.join(', ')}`);
    }
    
    return true;
    
  } catch (error) {
    const errMsg = error instanceof Error ? error.message : 'Unknown error';
    console.error(`[${timestamp}] Failed to send usage records to billing API: ${errMsg}`);
    return false;
  }
}

/**
 * Flushes current buffer to billing API
 */
export async function flush(): Promise<boolean> {
  const timestamp = new Date().toISOString();
  
  if (usageBuffer.length === 0) {
    console.log(`[${timestamp}] Flush called but buffer is empty`);
    return true;
  }
  
  // Take snapshot of current buffer and clear it
  const recordsToFlush = [...usageBuffer];
  usageBuffer = [];
  
  console.log(`[${timestamp}] Manual flush triggered for ${recordsToFlush.length} records`);
  
  const success = await sendToBillingAPI(recordsToFlush);
  
  if (!success) {
    // On failure, add records back to buffer (at the front to preserve order)
    console.warn(`[${timestamp}] Flush failed, re-adding ${recordsToFlush.length} records to buffer`);
    usageBuffer = [...recordsToFlush, ...usageBuffer];
  }
  
  return success;
}

/**
 * Internal flush function called by timer
 */
async function timerFlush(): Promise<void> {
  const timestamp = new Date().toISOString();
  
  if (isShuttingDown) {
    console.log(`[${timestamp}] Skipping timer flush - shutdown in progress`);
    return;
  }
  
  try {
    await flush();
  } catch (error) {
    const errMsg = error instanceof Error ? error.message : 'Unknown error';
    console.error(`[${timestamp}] Timer flush error: ${errMsg}`);
  }
  
  // Schedule next flush if not shutting down
  if (!isShuttingDown) {
    scheduleNextFlush();
  }
}

/**
 * Schedules the next automatic flush
 */
function scheduleNextFlush(): void {
  if (flushTimer) {
    clearTimeout(flushTimer);
  }
  
  flushTimer = setTimeout(timerFlush, FLUSH_INTERVAL_MS);
}

/**
 * Initializes the billing buffer and starts automatic flushing
 */
export function initialize(): void {
  const timestamp = new Date().toISOString();
  
  if (flushTimer !== null) {
    console.log(`[${timestamp}] Billing system already initialized`);
    return;
  }
  
  console.log(`[${timestamp}] Initializing billing system with ${FLUSH_INTERVAL_MS}ms flush interval`);
  scheduleNextFlush();
}

/**
 * Gracefully shuts down the billing system
 * Flushes remaining records and clears timers
 */
export async function shutdown(): Promise<void> {
  const timestamp = new Date().toISOString();
  
  if (isShuttingDown) {
    console.log(`[${timestamp}] Billing shutdown already in progress`);
    return;
  }
  
  isShuttingDown = true;
  console.log(`[${timestamp}] Shutting down billing system...`);
  
  // Clear the timer
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }
  
  // Flush any remaining records
  if (usageBuffer.length > 0) {
    console.log(`[${timestamp}] Flushing ${usageBuffer.length} records before shutdown`);
    await flush();
  }
  
  console.log(`[${timestamp}] Billing system shutdown complete`);
}

/**
 * Tracks a usage record in the buffer
 */
export function trackUsage(
  agentId: string,
  inputTokens: number,
  outputTokens: number,
  model: string,
  tenantId?: string
): void {
  const timestamp = new Date().toISOString();
  
  const record: Partial<UsageRecord> = {
    agentId,
    inputTokens,
    outputTokens,
    model,
    timestamp,
    tenantId
  };
  
  if (!isValidUsageRecord(record)) {
    console.error(`[${timestamp}] Invalid usage record - missing required fields`);
    return;
  }
  
  usageBuffer.push(record);
  
  const bufferSize = usageBuffer.length;
  console.log(`[${timestamp}] Usage tracked: agent=${agentId}, tokens=${inputTokens}+${outputTokens}, buffer_size=${bufferSize}`);
  
  // Flush early if buffer is getting large
  if (bufferSize >= MAX_BUFFER_SIZE) {
    console.log(`[${timestamp}] Buffer size limit reached (${MAX_BUFFER_SIZE}), triggering immediate flush`);
    flush().catch((error) => {
      const errMsg = error instanceof Error ? error.message : 'Unknown error';
      console.error(`[${timestamp}] Immediate flush failed: ${errMsg}`);
    });
  }
}

/**
 * Returns current buffer size for monitoring
 */
export function getBufferSize(): number {
  return usageBuffer.length;
}

/**
 * Returns whether the billing system is currently initialized and running
 */
export function isInitialized(): boolean {
  return flushTimer !== null && !isShuttingDown;
}
