/**
 * MyndLens ↔ OpenClaw Channel Adapter
 * 
 * PURPOSE:
 * Receives Master Intent Objects (MIOs) from MyndLens BE
 * Validates signatures, authenticates, and dispatches to OpenClaw Gateway
 * 
 * DEPLOYMENT:
 * Runs inside tenant Docker container alongside OpenClaw Gateway
 * Pre-configured during container startup
 * 
 * SECURITY:
 * - ED25519 signature validation
 * - Dispatch token authentication
 * - Tenant isolation enforcement
 * - Audit trail for all operations
 */

const http = require('http');
const crypto = require('crypto');
const { URL } = require('url');

// Configuration from environment
const ADAPTER_PORT = parseInt(process.env.ADAPTER_PORT || '10000'); // Same as RUNTIME_PORT
const TENANT_ID = process.env.TENANT_ID;
const OPENCLAW_GATEWAY_PORT = parseInt(process.env.OPENCLAW_GATEWAY_PORT || '18789');
const OPENCLAW_GATEWAY_TOKEN = process.env.OPENCLAW_GATEWAY_TOKEN;
const MYNDLENS_DISPATCH_TOKEN = process.env.MYNDLENS_DISPATCH_TOKEN || 'myndlens_dispatch_secret_2026';

// MyndLens public key for signature verification (ED25519)
const MYNDLENS_PUBLIC_KEY_HEX = process.env.MYNDLENS_PUBLIC_KEY || '';

// Adapter state
const adapterState = {
  status: 'initializing',
  tenant_id: TENANT_ID,
  openclaw_gateway_url: `http://localhost:${OPENCLAW_GATEWAY_PORT}`,
  dispatches_processed: 0,
  last_dispatch: null
};

// Audit log storage (in-memory for now, should persist to volume)
const auditLog = [];

/**
 * Emit audit event
 */
function emitAudit(event, mio_id, details) {
  const entry = {
    timestamp: new Date().toISOString(),
    event,
    mio_id,
    tenant_id: TENANT_ID,
    details: details || {}
  };
  
  auditLog.push(entry);
  console.log(`[ADAPTER] AUDIT: ${event} - MIO ${mio_id}`);
  
  // Keep last 1000 entries
  if (auditLog.length > 1000) {
    auditLog.shift();
  }
}

/**
 * Verify ED25519 signature on MIO
 */
function verifyMIOSignature(mio, signature) {
  if (!MYNDLENS_PUBLIC_KEY_HEX) {
    console.warn('[ADAPTER] MyndLens public key not configured, skipping signature verification');
    return true; // For development - should be false in production
  }
  
  try {
    // Convert hex to buffer
    const publicKey = Buffer.from(MYNDLENS_PUBLIC_KEY_HEX, 'hex');
    const signatureBuffer = Buffer.from(signature, 'hex');
    
    // Canonical MIO representation for signature
    const mioCanonical = JSON.stringify(mio, Object.keys(mio).sort());
    const message = Buffer.from(mioCanonical, 'utf8');
    
    // Verify using crypto
    const isValid = crypto.verify(
      null, // ED25519 doesn't use hash
      message,
      { key: publicKey, type: 'ed25519' },
      signatureBuffer
    );
    
    return isValid;
  } catch (error) {
    console.error('[ADAPTER] Signature verification error:', error.message);
    return false;
  }
}

/**
 * Check MIO TTL (Time To Live)
 */
function checkMIOTTL(mio) {
  if (!mio.expires_at) {
    return true; // No TTL specified
  }
  
  const expiryTime = new Date(mio.expires_at);
  const now = new Date();
  
  return now < expiryTime;
}

/**
 * Idempotency check - prevent duplicate execution
 */
const processedMIOs = new Map(); // In-memory cache (should use Redis in production)

function checkIdempotency(idempotencyKey) {
  if (processedMIOs.has(idempotencyKey)) {
    return {
      isDuplicate: true,
      previousResult: processedMIOs.get(idempotencyKey)
    };
  }
  return { isDuplicate: false };
}

function markProcessed(idempotencyKey, result) {
  processedMIOs.set(idempotencyKey, {
    timestamp: new Date().toISOString(),
    result
  });
  
  // Clean old entries (keep last 10000)
  if (processedMIOs.size > 10000) {
    const firstKey = processedMIOs.keys().next().value;
    processedMIOs.delete(firstKey);
  }
}

/**
 * Map MyndLens action to OpenClaw endpoint
 */
function mapMIOToOpenClawAction(mio) {
  const actionClass = mio.action_class || mio.type;
  
  // Mapping table per spec
  const mappings = {
    'COMM_SEND': {
      method: 'POST',
      endpoint: '/message/send',
      transform: (mio) => ({
        to: mio.params?.recipient,
        message: mio.params?.message,
        channel: mio.params?.channel || 'whatsapp'
      })
    },
    'EMAIL_SEND': {
      method: 'POST',
      endpoint: '/email/send',
      transform: (mio) => ({
        to: mio.params?.to,
        subject: mio.params?.subject,
        body: mio.params?.body
      })
    },
    'WEB_SEARCH': {
      method: 'POST',
      endpoint: '/search',
      transform: (mio) => ({
        query: mio.params?.query,
        max_results: mio.params?.max_results || 5
      })
    },
    'AGENT_CHAT': {
      method: 'POST',
      endpoint: '/agent',
      transform: (mio) => ({
        message: mio.params?.message,
        session_id: mio.session_id
      })
    }
  };
  
  return mappings[actionClass] || null;
}

/**
 * Dispatch to OpenClaw Gateway
 */
async function dispatchToOpenClaw(mapping, payload) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: OPENCLAW_GATEWAY_PORT,
      path: mapping.endpoint,
      method: mapping.method,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${OPENCLAW_GATEWAY_TOKEN}`
      }
    };
    
    const req = http.request(options, (res) => {
      let data = '';
      
      res.on('data', (chunk) => {
        data += chunk;
      });
      
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try {
            const result = JSON.parse(data);
            resolve({ success: true, data: result });
          } catch (e) {
            resolve({ success: true, data: { raw: data } });
          }
        } else {
          reject(new Error(`OpenClaw returned ${res.statusCode}: ${data}`));
        }
      });
    });
    
    req.on('error', (error) => {
      reject(error);
    });
    
    req.write(JSON.stringify(payload));
    req.end();
    
    // Timeout after 30 seconds
    req.setTimeout(30000, () => {
      req.destroy();
      reject(new Error('OpenClaw request timeout'));
    });
  });
}

/**
 * Main dispatch handler
 */
async function handleDispatch(req, res) {
  let mio_id = null;
  
  try {
    // Parse request body
    let body = '';
    for await (const chunk of req) {
      body += chunk;
    }
    
    const request = JSON.parse(body);
    const { mio, signature, tenant_id, session_id } = request;
    mio_id = mio?.mio_id || 'unknown';
    
    // Audit: DISPATCH_REQUESTED
    emitAudit('DISPATCH_REQUESTED', mio_id, { action_class: mio?.action_class });
    
    // 1. Validate dispatch token
    const dispatchToken = req.headers['x-myndlens-dispatch-token'];
    if (!dispatchToken || dispatchToken !== MYNDLENS_DISPATCH_TOKEN) {
      emitAudit('DISPATCH_REJECTED', mio_id, { reason: 'INVALID_AUTH' });
      res.writeHead(401, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 'REJECTED', error: 'INVALID_AUTH' }));
    }
    
    // 2. Verify tenant_id matches this container
    if (tenant_id !== TENANT_ID) {
      emitAudit('DISPATCH_REJECTED', mio_id, { reason: 'TENANT_MISMATCH' });
      res.writeHead(403, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 'REJECTED', error: 'TENANT_MISMATCH' }));
    }
    
    // 3. Check idempotency
    const idempotencyKey = req.headers['idempotency-key'] || `${session_id}:${mio_id}`;
    const idempotencyCheck = checkIdempotency(idempotencyKey);
    
    if (idempotencyCheck.isDuplicate) {
      emitAudit('DISPATCH_DUPLICATE', mio_id, { idempotency_key: idempotencyKey });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({
        status: 'OK',
        mio_id,
        duplicate: true,
        previous_result: idempotencyCheck.previousResult
      }));
    }
    
    // 4. Verify signature
    if (signature && !verifyMIOSignature(mio, signature)) {
      emitAudit('DISPATCH_REJECTED', mio_id, { reason: 'MIO_INVALID_SIGNATURE' });
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 'REJECTED', error: 'MIO_INVALID_SIGNATURE' }));
    }
    
    emitAudit('DISPATCH_VALIDATED', mio_id, {});
    
    // 5. Check TTL
    if (!checkMIOTTL(mio)) {
      emitAudit('DISPATCH_REJECTED', mio_id, { reason: 'MIO_EXPIRED' });
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 'REJECTED', error: 'MIO_EXPIRED' }));
    }
    
    // 6. Map MIO to OpenClaw action
    const mapping = mapMIOToOpenClawAction(mio);
    if (!mapping) {
      emitAudit('DISPATCH_REJECTED', mio_id, { reason: 'UNMAPPED_ACTION', action: mio.action_class });
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 'REJECTED', error: 'UNMAPPED_ACTION' }));
    }
    
    emitAudit('DISPATCH_MAPPED', mio_id, { endpoint: mapping.endpoint });
    
    // 7. Transform payload
    const openclawPayload = mapping.transform(mio);
    
    // 8. Dispatch to OpenClaw
    emitAudit('DISPATCH_SENT', mio_id, { endpoint: mapping.endpoint });
    
    try {
      const result = await dispatchToOpenClaw(mapping, openclawPayload);
      
      emitAudit('DISPATCH_SUCCESS', mio_id, { openclaw_response: result.data });
      
      // Mark as processed
      markProcessed(idempotencyKey, result);
      
      adapterState.dispatches_processed++;
      adapterState.last_dispatch = new Date().toISOString();
      
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'OK',
        mio_id,
        openclaw_request_id: result.data?.request_id || result.data?.id,
        result: result.data
      }));
      
    } catch (error) {
      emitAudit('DISPATCH_FAIL', mio_id, { error: error.message });
      
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'FAILED',
        mio_id,
        error: 'OPENCLAW_ERROR',
        message: error.message
      }));
    }
    
  } catch (error) {
    console.error('[ADAPTER] Dispatch error:', error);
    emitAudit('DISPATCH_ERROR', mio_id, { error: error.message });
    
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'FAILED',
      error: 'INTERNAL_ERROR',
      message: error.message
    }));
  }
}

/**
 * Health check endpoint
 */
function handleHealth(req, res) {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'healthy',
    adapter_version: '1.0.0',
    tenant_id: TENANT_ID,
    openclaw_gateway: `http://localhost:${OPENCLAW_GATEWAY_PORT}`,
    dispatches_processed: adapterState.dispatches_processed,
    last_dispatch: adapterState.last_dispatch,
    uptime_seconds: process.uptime()
  }));
}

/**
 * Audit log endpoint
 */
function handleAuditLog(req, res) {
  const limit = 100;
  const recent = auditLog.slice(-limit);
  
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    total_entries: auditLog.length,
    entries: recent
  }));
}

/**
 * HTTP Server - handles MyndLens dispatch requests
 */
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${ADAPTER_PORT}`);
  
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-MYNDLENS-DISPATCH-TOKEN, Idempotency-Key');
  
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }
  
  // Route handling
  if (req.method === 'POST' && url.pathname === '/v1/dispatch') {
    return handleDispatch(req, res);
  }
  
  if (req.method === 'GET' && url.pathname === '/health') {
    return handleHealth(req, res);
  }
  
  if (req.method === 'GET' && url.pathname === '/audit') {
    return handleAuditLog(req, res);
  }
  
  // 404 Not Found
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not Found' }));
});

/**
 * Start the Channel Adapter server
 */
function startAdapter() {
  server.listen(ADAPTER_PORT, '127.0.0.1', () => {
    adapterState.status = 'running';
    console.log(`[ADAPTER] MyndLens Channel Adapter listening on http://localhost:${ADAPTER_PORT}`);
    console.log(`[ADAPTER] Tenant: ${TENANT_ID}`);
    console.log(`[ADAPTER] OpenClaw Gateway: http://localhost:${OPENCLAW_GATEWAY_PORT}`);
    console.log(`[ADAPTER] Endpoints:`);
    console.log(`[ADAPTER]   POST /v1/dispatch - Receive MIOs from MyndLens`);
    console.log(`[ADAPTER]   GET  /health - Adapter health check`);
    console.log(`[ADAPTER]   GET  /audit - Audit log`);
  });
}

// Export for integration with tenant_worker.js
module.exports = {
  startAdapter,
  adapterState,
  handleDispatch,
  verifyMIOSignature,
  mapMIOToOpenClawAction
};

// Start if run directly
if (require.main === module) {
  startAdapter();
}
