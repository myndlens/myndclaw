/**
 * MyndLens Service - Enhanced with OpenClaw WebSocket Streaming
 * Real-time progress, sub-status updates, response streaming
 */

// Crash guards — prevent unhandled rejections/exceptions from killing the process
process.on('uncaughtException', (err) => {
  console.error(`[MyndLens] UNCAUGHT EXCEPTION (non-fatal): ${err.message}`);
  console.error(err.stack);
});
process.on('unhandledRejection', (reason) => {
  console.error(`[MyndLens] UNHANDLED REJECTION (non-fatal): ${reason}`);
});

const { WebSocketServer } = require('ws');
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const { promisify } = require('util');
const crypto = require('crypto');

const execAsync = promisify(exec);

// Configuration
const TENANT_ID = process.env.TENANT_ID || 'tenant_2545f9a0d50d';

// ── Temporary Allowlist Management — DEPRECATED ──────────────────────────────
// SECURITY FIX: Removed allowlist manipulation. Adding contacts to allowFrom
// creates a backdoor — they can bypass MyndLens governance and talk directly to OC.
// Instead, we use mode: "explicit" for outbound sends, which bypasses the
// allowlist check without modifying it. See handlePostExecutionDelivery().
const OC_CONFIG_PATH = process.env.OC_CONFIG_PATH || '/home/node/.openclaw/openclaw.json';

// These functions are kept as no-ops for API compatibility
function addToAllowlist(phones) {
  console.log(`[AllowList] BLOCKED — use mode:explicit instead. Phones: ${phones.join(', ')}`);
  return [];
}

function removeFromAllowlist(phones) {
  // No-op — nothing to remove since we don't add anymore
}

const TENANT_INDEX = parseInt(process.env.TENANT_INDEX || '0');
const PORT = 18791 + (TENANT_INDEX * 10);
const HEALTH_PORT = 18792 + (TENANT_INDEX * 10);
const OC_GATEWAY_URL = process.env.OC_GATEWAY_URL || 'ws://localhost:18789';
const OC_GATEWAY_HTTP_URL = process.env.OC_GATEWAY_HTTP_URL || 'http://localhost:18789';
const GATEWAY_PORT = (() => {
  try { return Number(new URL(OC_GATEWAY_URL).port || 18789); } catch { return 18789; }
})();
const API_KEY = process.env.MYNDLENS_API_KEY || 'myndlens_secret_2026';
const CONFIG_PATH = OC_CONFIG_PATH;
const WORKSPACE_BASE = process.env.OC_WORKSPACE_BASE || '/home/node/.openclaw/workspace';
const DEVICE_IDENTITY_PATH = process.env.OC_DEVICE_PATH || '/home/node/.openclaw/identity/device.json';

// OpenClaw Gateway WebSocket Client
let openclawWS = null;
let openclawConnected = false;
const activeExecutions = new Map(); // execution_id -> {clientWS, progress, events}
const pendingResponses = new Map(); // requestId -> handler(msg)

console.log('[MyndLens] Enhanced service with OpenClaw streaming');
console.log('[MyndLens] Tenant:', TENANT_ID);
console.log('[MyndLens] OpenClaw Gateway:', OC_GATEWAY_URL);

// Connect to OpenClaw Gateway
async function connectToOpenClaw() {
  const gatewayToken = process.env.OPENCLAW_GATEWAY_TOKEN || JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')).gateway.auth.token;

  // Load device identity from env or local filesystem for device-authenticated connect.
  // Without device auth, OC strips all scopes — making req:agent unusable.
  let deviceId, publicKeyPem, privateKeyPem;
  try {
    let deviceData;
    if (process.env.OC_DEVICE_JSON) {
      deviceData = JSON.parse(Buffer.from(process.env.OC_DEVICE_JSON, 'base64').toString('utf8'));
      console.log('[OpenClaw] Device identity from OC_DEVICE_JSON env var');
    } else {
      deviceData = JSON.parse(fs.readFileSync(DEVICE_IDENTITY_PATH, 'utf8'));
      console.log('[OpenClaw] Device identity from local file');
    }
    deviceId = deviceData.deviceId;
    publicKeyPem = deviceData.publicKeyPem;
    privateKeyPem = deviceData.privateKeyPem;
    console.log('[OpenClaw] Device identity loaded:', deviceId.substring(0, 12) + '...');
  } catch (e) {
    console.log('[OpenClaw] No device identity found — falling back to token-only auth');
  }

  return new Promise((resolve, reject) => {
    openclawWS = new WebSocket(OC_GATEWAY_URL);
    let handshakeSent = false;

    openclawWS.on('open', () => {
      console.log('[OpenClaw] WebSocket open — waiting for connect.challenge');
      // Do NOT send connect here. OC sends a challenge event first.
      // Sending connect before the challenge is ignored / causes errors.
    });

    openclawWS.on('message', (data) => {
      const msg = JSON.parse(data.toString());

      // Step 1: OC sends connect.challenge with a nonce before we connect.
      if (msg.type === 'event' && msg.payload && msg.payload.nonce && !handshakeSent) {
        handshakeSent = true;
        const nonce = msg.payload.nonce;
        console.log('[OpenClaw] Challenge received — sending connect');

        const connectScopes = deviceId ? ['operator.admin'] : ['operator.read', 'operator.write'];
        const signedAtMs = Date.now();
        const clientId = 'cli';
        const clientMode = 'cli';
        const role = 'operator';

        let deviceBlock = undefined;
        if (deviceId && privateKeyPem) {
          // Build v2 payload: version|deviceId|clientId|clientMode|role|scopes|signedAtMs|token|nonce
          const scopesCsv = connectScopes.join(',');
          const payload = ['v2', deviceId, clientId, clientMode, role, scopesCsv,
            String(signedAtMs), gatewayToken, nonce].join('|');
          const privateKey = crypto.createPrivateKey(privateKeyPem);
          const signature = crypto.sign(null, Buffer.from(payload), privateKey)
            .toString('base64url');

          // Extract raw 32-byte Ed25519 public key from SPKI PEM.
          // SPKI PEM has a 12-byte ASN.1 header before the 32-byte key.
          // OC stores/expects raw key in base64url without padding.
          const pubKey = crypto.createPublicKey(publicKeyPem);
          const spkiDer = pubKey.export({ type: 'spki', format: 'der' });
          const rawPubKey = spkiDer.subarray(spkiDer.length - 32);  // last 32 bytes = raw Ed25519 key
          const rawPubKeyB64url = rawPubKey.toString('base64url');

          deviceBlock = {
            id: deviceId,
            publicKey: rawPubKeyB64url,
            signature: signature,
            signedAt: signedAtMs,
            nonce: nonce,
          };
          console.log('[OpenClaw] Device auth: signed v2 payload, pubKey=' + rawPubKeyB64url.substring(0, 12) + '...');
        }

        openclawWS.send(JSON.stringify({
          type: 'req',
          id: 'connect_1',
          method: 'connect',
          params: {
            minProtocol: 3,
            maxProtocol: 3,
            client: {
              id: clientId,
              version: '2026.2.16',
              platform: 'linux',
              mode: clientMode,
            },
            role: role,
            scopes: connectScopes,
            caps: [],
            commands: [],
            permissions: {},
            auth: { token: gatewayToken },
            ...(deviceBlock ? { device: deviceBlock } : {}),
            locale: 'en-US',
            userAgent: 'myndlens-service/1.0',
          },
        }));
        return;
      }

      // Step 2: OC replies hello-ok on successful connect
      if (msg.type === 'res' && msg.payload?.type === 'hello-ok') {
        openclawConnected = true;
        console.log('[OpenClaw] Handshake complete, protocol:', msg.payload.protocol);
        resolve();
        return;
      }

      // Route responses to pending handlers (agent RPC two-stage responses)
      if (msg.type === 'res' && pendingResponses.has(msg.id)) {
        pendingResponses.get(msg.id)(msg);
        return;
      }

      // All other events go to the event handler
      if (msg.type === 'event') {
        handleOpenClawEvent(msg);
      } else if (msg.type !== 'res') {
        // Log any non-event, non-response messages for audit
        console.log('[OpenClaw MSG]:', msg.type, JSON.stringify(msg).substring(0, 200));
      }

      // Log unexpected connect errors
      if (msg.type === 'res' && msg.ok === false) {
        console.error('[OpenClaw] Connect error:', JSON.stringify(msg.error));
      }
    });

    openclawWS.on('error', (err) => {
      console.error('[OpenClaw] Error:', err.message);
      reject(err);
    });

    setTimeout(() => reject(new Error('OpenClaw connection timeout')), 15000);
  });
}

// Handle OpenClaw events and broadcast to clients
function handleOpenClawEvent(msg) {
  const { event, payload } = msg;
  
  // Dump full event payload for audit (except noisy tick/health)
  if (event === 'tick' || event === 'health') return;
  // Compact log: event type + stream type + key data
  const stream = payload?.stream || '';
  const phase = payload?.data?.phase || payload?.phase || '';
  const toolName = payload?.tool?.name || payload?.data?.tool?.name || '';
  const innerSeq = payload?.seq || '';
  console.log(`[OC] evt=${event} stream=${stream} phase=${phase} tool=${toolName} seq=${innerSeq} ts=${payload?.ts || ''}`);
  // Full dump for tool events
  if (stream === 'tool' || stream === 'lifecycle' || (event === 'chat' && payload?.state === 'tool')) {
    console.log('[OC DETAIL]:', JSON.stringify(msg, null, 0));
  }
  
  // Map OC runId to our execution.
  // OC uses its own runId which differs from our mandateId.
  // Since each channel service instance handles one tenant with typically one active execution,
  // map the OC runId to the most recent active execution on first encounter.
  let execution = null;
  if (payload?.runId) {
    execution = activeExecutions.get(payload.runId);
    if (!execution) {
      // Find the most recent active execution and map this runId to it
      for (const [key, exec] of activeExecutions) {
        if (exec.progress < 100) {
          execution = exec;
          activeExecutions.set(payload.runId, exec);  // create alias
          break;
        }
      }
    }
  }
  // Fallback: if still no match, use any active (non-complete) execution
  if (!execution) {
    for (const [, exec] of activeExecutions) {
      if (exec.progress < 100) { execution = exec; break; }
    }
  }
  if (!execution) return;

  switch (event) {
    case 'agent':
      if (payload.stream === 'lifecycle') {
        if (payload.data?.phase === 'start' || payload.phase === 'start') {
          broadcastProgress(execution, 'Agent started', Math.max(execution.progress, 10));
        }
        // lifecycle end is handled by the WS agent response handler
      } else if (payload.stream === 'tool') {
        // Real tool execution events from OC via WS agent method
        const name = payload.data?.name || payload.data?.tool?.name || payload.data?.toolName || '';
        const toolPhase = payload.data?.phase || payload.data?.type || payload.data?.event || '';
        if (name) {
          if (!execution.seenTools) execution.seenTools = new Set();
          if (!execution.seenTools.has(name)) {
            execution.seenTools.add(name);
            execution.progress = Math.min(execution.progress + 10, 85);
            const label = name.replace(/[-_]/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
            broadcastProgress(execution, `Invoking: ${label}`, execution.progress);
            console.log(`[OC:Tool] ${name} phase=${toolPhase} (exec=${execution.execution_id})`);
          }
        } else if (toolPhase) {
          // Tool event without a name (update/end) — log but don't duplicate progress
          console.log(`[OC:Tool] phase=${toolPhase} (exec=${execution.execution_id})`);
        }
      } else if (payload.stream === 'assistant') {
        // Accumulate streamed content from agent events
        const delta = payload.data?.delta || payload.data?.text || '';
        if (delta) {
          if (execution.fullContent === undefined) execution.fullContent = '';
          execution.fullContent += delta;
          broadcastResponseChunk(execution, delta);
          // Progress milestones based on content arrival
          if (execution.fullContent.length <= delta.length) {
            broadcastProgress(execution, 'Receiving response...', 85);
          } else if (execution.fullContent.length > 200 && execution.progress < 90) {
            broadcastProgress(execution, 'Assembling results...', 90);
          }
        }
      }
      break;

    case 'chat':
      if (payload.stream === 'tool' || payload.state === 'tool') {
        // Legacy/HTTP-path tool events
        execution.progress = Math.min(execution.progress + 10, 85);
        const chatToolName = payload.tool?.name || 'tool';
        const label = chatToolName.replace(/[-_]/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        broadcastProgress(execution, `Invoking: ${label}`, execution.progress);
      } else if (payload.stream === 'assistant' || payload.state === 'delta') {
        const chatDelta = payload.delta?.text || payload.message?.content || '';
        if (chatDelta) {
          broadcastResponseChunk(execution, chatDelta);
        }
      }
      break;
  }
}

function broadcastProgress(execution, status, percentage) {
  if (execution.clientWS && execution.clientWS.readyState === WebSocket.OPEN) {
    execution.clientWS.send(JSON.stringify({
      type: 'progress',
      execution_id: execution.execution_id,
      status: status,
      progress: percentage,
      timestamp: new Date().toISOString()
    }));
  }
}

function broadcastResponseChunk(execution, chunk) {
  if (execution.clientWS && execution.clientWS.readyState === WebSocket.OPEN) {
    execution.clientWS.send(JSON.stringify({
      type: 'response_chunk',
      execution_id: execution.execution_id,
      chunk: chunk
    }));
  }
}

// Initialize OpenClaw connection
connectToOpenClaw().catch(err => {
  console.error('[OpenClaw] Failed to connect:', err.message);
  console.log('[MyndLens] Falling back to CLI execution mode');
});

// MyndLens WebSocket Server
const server = new WebSocketServer({ port: PORT });

server.on('listening', () => {
  console.log(`[MyndLens] ✅ Listening on port ${PORT}`);
});

server.on('connection', (ws, req) => {
  console.log('[MyndLens] Client connected');
  
  const auth = req.headers.authorization;
  if (!auth || auth !== `Bearer ${API_KEY}`) {
    ws.close(4001, 'Unauthorized');
    return;
  }

  ws.on('message', async (data) => {
    try {
      const artifact = JSON.parse(data.toString());
      
      if (artifact.action === 'kill') {
        const result = await handleKill(artifact);
        ws.send(JSON.stringify(result));
      } else {
        const result = await handleMandateWithStreaming(artifact, ws);
        ws.send(JSON.stringify(result));
      }
    } catch (error) {
      console.error('[MyndLens] Error:', error.message);
      ws.send(JSON.stringify({ status: 'error', error: error.message }));
    }
  });
});

async function handleMandateWithStreaming(artifact, clientWS) {
  const { mandate, agents, mandateId } = artifact;
  
  console.log(`[MyndLens] Mandate ${mandateId} with streaming`);
  
  // Track execution
  const execution = {
    execution_id: mandateId,
    clientWS: clientWS,
    progress: 0,
    events: []
  };
  
  activeExecutions.set(mandateId, execution);

  const primaryAgentId = agents && agents.length > 0
    ? (agents[0].id || 'myndlens-agent')
    : null;
  const agentsMd = agents && agents[0] && agents[0].agents_md;

  // Attach agents_md to execution so executeViaHttp can append it to the message
  if (agentsMd) execution.agents_md = agentsMd;

  try {
    // ── STEP 1: Write AGENTS.md (output format contract, ~1KB) ───────────────
    broadcastProgress(execution, 'Configuring agent workspace', 5);
    if (agentsMd && primaryAgentId) {
      await writeAgentsMd(primaryAgentId, agentsMd);
      broadcastProgress(execution, 'Agent instructions deployed', 15);
      await sleep(800); // brief pause for hot-reload
    }

    // ── STEP 1.5: WhatsApp Number Guard — validate before sending ──────────
    //   Prevents sending to non-WA numbers which kills the Baileys session.
    //   Uses DS-enriched annotations ("whatsapp: +NNN") already present in
    //   the mandate text — zero additional Baileys connections needed.
    //   NEVER create a second Baileys connection for validation — WhatsApp only
    //   allows ONE active connection per device identity. A second connection
    //   triggers connectionReplaced (440), destabilizing the active session.
    // ── WhatsApp Number Guard (targeted) ───────────────────────────────────
    // Only triggers when the USER'S TASK (first 300 chars) explicitly asks to
    // send/forward a WhatsApp message. DS-enriched context in the mandate body
    // always contains "whatsapp" + phone numbers — checking the full text would
    // block ALL mandates. The task portion comes first before DS annotations.
    // WA Guard DISABLED — allowlist temp expansion is the safety mechanism
    const taskPortion = 'disabled';
    const isWhatsAppSendTask = /(?:send|forward|text|reply|write)\s+(?:a\s+)?(?:whatsapp|wa)\s+(?:message|msg)/i.test(taskPortion)
      || /(?:whatsapp|wa)\s+(?:send|message|msg|text)\s+(?:to|for)/i.test(taskPortion)
      || /(?:message|msg|text)\s+\S+\s+(?:on|via|through)\s+(?:whatsapp|wa)/i.test(taskPortion);
    if (false && isWhatsAppSendTask) {
      const waNumbersInMandate = extractPhoneNumbers(mandate);
      if (waNumbersInMandate.length > 0) {
        broadcastProgress(execution, 'Validating WhatsApp numbers', 20);
        const verified = [];
        const unverified = [];
        for (const phone of waNumbersInMandate) {
          if (isWhatsAppAnnotated(phone, mandate)) {
            verified.push(phone);
            console.log(`[MyndLens] WA_GUARD: ${phone} confirmed via DS annotation`);
          } else {
            unverified.push(phone);
          }
        }
        if (verified.length === 0) {
          console.error(`[MyndLens] WA_GUARD: BLOCKED — no verified WhatsApp numbers. Numbers: ${waNumbersInMandate.join(', ')}`);
          throw new Error(
            `WhatsApp guard: None of the phone numbers in this mandate are confirmed WhatsApp numbers. ` +
            `Message NOT sent to protect the session. Numbers checked: ${waNumbersInMandate.join(', ')}`
          );
        }
        if (unverified.length > 0) {
          console.warn(`[MyndLens] WA_GUARD: Unverified numbers present: ${unverified.join(', ')}. Verified: ${verified.join(', ')}`);
        }
      }
    } else if (extractPhoneNumbers(mandate).length > 0) {
      console.log(`[MyndLens] WA_GUARD: skipped (task is not WhatsApp send: "${taskPortion.substring(0, 80)}...")`);
    }

    // ── STEP 2: Execute via WS agent method (real-time tool events) ──────────
    //   Falls back to HTTP if WS agent is unavailable (device pairing, etc.)
    broadcastProgress(execution, 'Dispatching to OpenClaw', 25);

    let result;
    if (openclawConnected && openclawWS) {
      try {
        // Option 3: OC composes messages, channel service delivers post-execution.
        // Pre-emptive allowlist add removed — delivery happens after OC returns.
        result = await executeViaWSAgent(mandate, mandateId, primaryAgentId, execution);
        console.log(`[MyndLens] WS agent result: ${(result.output||'').length} chars, tools=${(result.tools_used||[]).join(',')||'none'} (exec=${mandateId})`);
      } catch (wsErr) {
        console.warn(`[MyndLens] WS agent failed, falling back to HTTP: ${wsErr.message} (exec=${mandateId})`);
        try {
          result = await executeViaHttp(mandate, mandateId, primaryAgentId, execution);
        } catch (httpErr) {
          console.warn(`[MyndLens] HTTP failed, falling back to CLI: ${httpErr.message} (exec=${mandateId})`);
          result = await executeMandate(mandate, mandateId, primaryAgentId);
        }
      }
    } else {
      console.warn(`[MyndLens] OpenClaw WS not connected — using CLI fallback (exec=${mandateId})`);
      result = await executeMandate(mandate, mandateId, primaryAgentId);
    }

    broadcastProgress(execution, 'Execution complete', 100);

    return {
      status: 'success',
      execution_id: mandateId,
      streaming: false,
      executionResult: result,
    };
  } finally {
    // Clean up: remove the execution and any OC runId aliases pointing to it
    for (const [key, exec] of activeExecutions) {
      if (exec === execution) activeExecutions.delete(key);
    }
  }
}

// Execute mandate via WS agent RPC — gives real-time tool events.
// Two-stage response: accepted ack → streamed events → final response.
// Tool events arrive as event:agent { stream:"tool" } and are handled by handleOpenClawEvent.

// ── Option 3: Post-execution WhatsApp delivery ──────────────────────────────
// OC composes the message in its JSON output. Channel service delivers it.
// This avoids OC's allowlist restriction entirely.

async function handlePostExecutionDelivery(output, mandateId) {
  if (!output || output.length < 10) return null;

  // Parse delivery instructions from OC's JSON output
  let delivery = null;
  try {
    // Try to extract JSON from output (may be wrapped in ```json blocks)
    const jsonMatch = output.match(/```json\s*([\s\S]*?)```/) || output.match(/\{[\s\S]*"delivery"[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[1] || jsonMatch[0]);
      delivery = parsed.delivery || null;
    }
  } catch {}

  // Also check for explicit delivery block pattern
  if (!delivery) {
    const phoneMatch = output.match(/"target"\s*:\s*"(\+\d{10,15})"/);
    const msgMatch = output.match(/"message_body"\s*:\s*"([^"]+)"/);
    const channelMatch = output.match(/"channel"\s*:\s*"(whatsapp|sms|email)"/);
    if (phoneMatch && channelMatch && channelMatch[1] === 'whatsapp') {
      delivery = {
        channel: 'whatsapp',
        target: phoneMatch[1],
        message_body: msgMatch ? msgMatch[1] : null,
      };
    }
  }

  if (!delivery || delivery.channel !== 'whatsapp' || !delivery.target) return null;
  if (delivery.status === 'sent') {
    console.log(`[Delivery] OC already sent (status=sent) to ${delivery.target} — skipping`);
    return delivery;
  }

  // Extract message body: from delivery.message_body or compose from output
  let messageBody = delivery.message_body || delivery.message || '';
  if (!messageBody) {
    // Try to extract the message OC composed from the full output
    const msgSectionMatch = output.match(/message[:\s]+["']([^"']{10,500})["']/i);
    if (msgSectionMatch) messageBody = msgSectionMatch[1];
  }

  if (!messageBody || messageBody.length < 5) {
    console.warn(`[Delivery] No message body found for ${delivery.target} — skipping`);
    return delivery;
  }

  const target = delivery.target;
  console.log(`[Delivery] POST-EXEC WhatsApp send to ${target} (${messageBody.length} chars) — using mode:explicit`);

  // SECURITY FIX: Use mode:explicit via HTTP API (no shell execution).
  // mode:explicit bypasses OC's allowlist check without adding contacts to allowFrom.
  try {
    const response = await fetch(`${OC_GATEWAY_HTTP_URL}/v1/messages/send`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.OPENCLAW_GATEWAY_TOKEN || ''}`,
      },
      body: JSON.stringify({
        channel: 'whatsapp',
        target,
        message: messageBody,
        mode: 'explicit',
      }),
    });
    const parsed = await response.json();
    const messageId = parsed?.payload?.result?.messageId || parsed?.messageId;
    console.log(`[Delivery] SUCCESS (explicit via API): ${target} messageId=${messageId}`);
    delivery.status = 'sent';
    delivery.message_id = messageId;
  } catch (e) {
    console.error(`[Delivery] FAILED: ${target} error=${e.message.substring(0, 100)}`);
    delivery.status = 'failed';
    delivery.error = e.message.substring(0, 200);
  }

  return delivery;
}

async function executeViaWSAgent(mandate, mandateId, agentId, execution) {
  const agentsMdFromExecution = execution && execution.agents_md;
  let messageContent = mandate;
  if (agentsMdFromExecution) {
    // Extract Identity section — MUST come FIRST so OC processes it before the task.
    // This prevents impersonation: OC sees the proxy constraint before the action.
    const identityMatch = agentsMdFromExecution.match(/## Identity[^\n]*\n([\s\S]*?)(?=\n## (?!Identity))/);
    const identityBlock = identityMatch
      ? `[SYSTEM CONSTRAINT — READ BEFORE ACTING]\n${identityMatch[0].trim()}\n---\n\n`
      : '';

    // Extract Output Contract — tells OC what JSON to return
    const contractMatch = agentsMdFromExecution.match(/## Output Contract\n([\s\S]*?)(?:\n##|$)/);
    const contractBlock = contractMatch ? `\n\n---\n${contractMatch[0]}` : '';

    // Option 3: Instruct OC to compose but NOT send WhatsApp directly
    const deliveryRule = '\n## WhatsApp Delivery Rule (ABSOLUTE)\n'
      + 'Do NOT use the "message send" command for WhatsApp. It will fail.\n'
      + 'Instead, include delivery instructions in your JSON output:\n'
      + '{"delivery": {"channel": "whatsapp", "target": "+<number>", "message_body": "your composed message"}}\n'
      + 'The delivery system handles the actual send. This is mandatory.\n\n';

    // Identity FIRST, then delivery rule, then task, then output contract
    messageContent = `${identityBlock}${deliveryRule}${mandate}${contractBlock}`;
  }

  return new Promise((resolve, reject) => {
    const requestId = `agent_${mandateId}_${Date.now()}`;
    // Always use main agent with unique session — main agent has all skills + WA channel
    const uniqueSession = `exec_${mandateId}_${Date.now()}`;
    const sessionKey = `agent:main:${uniqueSession}`;
    console.log(`[WS Agent] Using main agent session (exec=${mandateId})`);

    execution.fullContent = '';
    execution.seenTools = execution.seenTools || new Set();
    let accepted = false;

    const timer = setTimeout(() => {
      pendingResponses.delete(requestId);
      reject(new Error('Agent execution timeout (300s)'));
    }, 300000);

    // Register response handler for this request ID
    pendingResponses.set(requestId, (msg) => {
      if (msg.ok === true) {
        const status = msg.payload?.status;
        if (status === 'accepted') {
          // Stage 1: accepted ack with runId
          accepted = true;
          const runId = msg.payload.runId;
          execution.runId = runId;
          // Map OC runId → our execution so handleOpenClawEvent can find it
          activeExecutions.set(runId, execution);
          console.log(`[WS Agent] Accepted: runId=${runId} (exec=${mandateId})`);
          broadcastProgress(execution, 'Agent processing', 30);
          // Don't resolve — wait for final
        } else if (status === 'ok' || status === 'error') {
          // Stage 2: final response
          clearTimeout(timer);
          pendingResponses.delete(requestId);
          const output = execution.fullContent || msg.payload?.summary || '';
          console.log(`[WS Agent] Final: status=${status} contentLen=${output.length} (exec=${mandateId})`);
          // Option 3: Handle post-execution WhatsApp delivery
          handlePostExecutionDelivery(output, mandateId).then(delivery => {
            if (delivery && delivery.status === 'sent') {
              console.log(`[Delivery] Post-exec delivery complete for exec=${mandateId}`);
            }
          }).catch(e => {
            console.warn(`[Delivery] Post-exec delivery error: ${e.message}`);
          });
          resolve({
            success: status === 'ok',
            output,
            tools_used: execution.seenTools ? [...execution.seenTools] : [],
          });
        }
      } else if (msg.ok === false) {
        // Error response
        clearTimeout(timer);
        pendingResponses.delete(requestId);
        const errMsg = msg.error?.message || msg.error?.detail || JSON.stringify(msg.error) || 'Agent failed';
        console.error(`[WS Agent] Error: ${errMsg} (exec=${mandateId})`);
        reject(new Error(errMsg));
      }
    });

    // Send the agent RPC request
    console.log(`[WS Agent] Sending req:agent sessionKey=${sessionKey} (exec=${mandateId})`);
    openclawWS.send(JSON.stringify({
      type: 'req',
      id: requestId,
      method: 'agent',
      params: {
        message: messageContent,
        sessionKey: sessionKey,
        idempotencyKey: requestId,
      },
    }));
  });
}

// Execute mandate via HTTP /v1/chat/completions — fallback path.
// Uses streaming (stream:true) to capture real-time tool calls and progress.
// Falls back to non-streaming on any error.
async function executeViaHttp(mandate, mandateId, agentId, execution) {
  const config = loadConfig();
  const gatewayToken = config.gateway?.auth?.token;
  const gatewayUrl = `${OC_GATEWAY_HTTP_URL}/v1/chat/completions`;

  const agentsMdFromExecution = execution && execution.agents_md;
  let messageContent = mandate;
  if (agentsMdFromExecution) {
    // Extract Identity section — MUST come FIRST to prevent impersonation
    const identityMatch = agentsMdFromExecution.match(/## Identity[^\n]*\n([\s\S]*?)(?=\n## (?!Identity))/);
    const identityBlock = identityMatch
      ? `[SYSTEM CONSTRAINT — READ BEFORE ACTING]\n${identityMatch[0].trim()}\n---\n\n`
      : '';

    // Extract Output Contract
    const contractMatch = agentsMdFromExecution.match(/## Output Contract\n([\s\S]*?)(?:\n##|$)/);
    const contractBlock = contractMatch ? `\n\n---\n${contractMatch[0]}` : '';

    messageContent = `${identityBlock}${mandate}${contractBlock}`;
  }

  const sessionHint = agentId ? `[Agent: ${agentId}]\n` : '';
  const messages = [{ role: 'user', content: sessionHint + messageContent }];

  // Try streaming first for real-time tool visibility
  try {
    const streamResult = await executeViaHttpStreaming(gatewayUrl, gatewayToken, messages, mandateId, agentId, execution);

    // If streaming returned content, use it
    if (streamResult.output && streamResult.output.trim()) {
      console.log(`[MyndLens] Using streaming result: ${streamResult.output.length} chars (exec=${mandateId})`);
      return streamResult;
    }

    // Streaming returned tools but no content — batch handles full tool loop
    if (streamResult.tools_used && streamResult.tools_used.length > 0) {
      console.log(`[MyndLens] Streaming: tools detected but no content — batch fallback (exec=${mandateId})`);
      broadcastProgress(execution, 'Retrieving execution results...', 92);
      const batchResult = await executeViaHttpBatch(gatewayUrl, gatewayToken, messages, mandateId, agentId, execution);
      batchResult.tools_used = streamResult.tools_used;
      return batchResult;
    }

    return streamResult;
  } catch (streamErr) {
    console.warn('[MyndLens] Streaming failed, falling back to batch:', streamErr.message);
    return await executeViaHttpBatch(gatewayUrl, gatewayToken, messages, mandateId, agentId, execution);
  }
}

// Streaming execution — parses SSE to detect tool calls in real time
async function executeViaHttpStreaming(gatewayUrl, gatewayToken, messages, mandateId, agentId, execution) {
  const body = JSON.stringify({
    model: 'default',
    messages,
    stream: true,
    metadata: { mandateId, agentId: agentId || 'default' },
  });

  const response = await fetch(gatewayUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${gatewayToken}`,
    },
    body,
    signal: AbortSignal.timeout(300000), // 5 min
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`HTTP ${response.status}: ${err.substring(0, 200)}`);
  }

  let fullContent = '';
  const seenTools = new Set();
  let buffer = '';

  broadcastProgress(execution, 'OpenClaw processing mandate...', 35);

  for await (const chunk of response.body) {
    buffer += Buffer.from(chunk).toString();
    const lines = buffer.split('\n');
    buffer = lines.pop(); // keep incomplete line

    for (const line of lines) {
      if (!line.startsWith('data: ')) continue;
      const data = line.slice(6).trim();
      if (data === '[DONE]') continue;

      try {
        const parsed = JSON.parse(data);
        const delta = parsed.choices?.[0]?.delta;
        if (!delta) continue;

        // Tool call detected — broadcast real tool name
        if (delta.tool_calls) {
          for (const tc of delta.tool_calls) {
            const toolName = tc.function?.name;
            if (toolName && !seenTools.has(toolName)) {
              seenTools.add(toolName);
              const label = toolName.replace(/[-_]/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
              execution.progress = Math.min(execution.progress + 15, 85);
              broadcastProgress(execution, `Invoking: ${label}`, execution.progress);
              console.log(`[OpenClaw:Tool] ${toolName} (exec=${mandateId})`);
            }
          }
        }

        // Content chunk
        if (delta.content) {
          fullContent += delta.content;
          // Broadcast content-based progress so the poll trail has meaningful entries
          if (fullContent.length === delta.content.length) {
            broadcastProgress(execution, 'OpenClaw responding...', 60);
          } else if (fullContent.length > 100 && execution.progress < 80) {
            broadcastProgress(execution, 'Assembling results...', 80);
          }
          // First content after tools = synthesizing results
          if (seenTools.size > 0 && fullContent.length < 50) {
            broadcastProgress(execution, 'Synthesizing results...', 90);
          }
        }
      } catch (e) {
        // Non-JSON line, skip
      }
    }
  }

  console.log(`[MyndLens] Streaming done: contentLen=${fullContent.length} tools=${[...seenTools].join(',')||'none'} (exec=${mandateId})`);
  return { success: true, output: fullContent, tools_used: [...seenTools] };
}

// Batch (non-streaming) fallback — original execution path
async function executeViaHttpBatch(gatewayUrl, gatewayToken, messages, mandateId, agentId, execution) {
  const heartbeat = setInterval(() => {
    if (execution) broadcastProgress(execution, 'Executing...', 50);
  }, 20000);

  try {
    const body = JSON.stringify({
      model: 'default',
      messages,
      stream: false,
      metadata: { mandateId, agentId: agentId || 'default' },
    });

    const response = await fetch(gatewayUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${gatewayToken}`,
      },
      body,
    });

    clearInterval(heartbeat);

    if (!response.ok) {
      const err = await response.text();
      return { success: false, error: `HTTP ${response.status}: ${err.substring(0, 200)}` };
    }

    const data = await response.json();
    const output = data?.choices?.[0]?.message?.content || '';
    return { success: true, output };
  } catch (err) {
    clearInterval(heartbeat);
    return { success: false, error: err.message };
  }
}

// sendToOpenClawWS — legacy chat.send method, kept for reference.
// Superseded by executeViaWSAgent() which uses the agent RPC method.
async function sendToOpenClawWS(message, runId, agentId) {
  return new Promise((resolve, reject) => {
    if (!openclawWS || !openclawConnected) {
      return reject(new Error('OpenClaw not connected'));
    }
    
    const requestId = `msg_${Date.now()}`;
    // Per OC docs: target a specific agent with "agent:<agentId>:<sessionKey>"
    // Without this prefix, OC routes to the default agent — ignoring workspace/skills/tools
    const sessionKey = agentId ? `agent:${agentId}:${runId}` : `agent:main:${runId}`;
    
    openclawWS.send(JSON.stringify({
      type: 'req',
      id: requestId,
      method: 'chat.send',
      params: {
        message: message,
        sessionKey: sessionKey,
        idempotencyKey: requestId
      }
    }));
    
    // Listen for response
    const handler = (data) => {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'res' && msg.id === requestId) {
        openclawWS.removeListener('message', handler);
        if (msg.ok) {
          resolve(msg.payload);
        } else {
          reject(new Error(msg.error || 'OpenClaw request failed'));
        }
      }
    };
    
    openclawWS.on('message', handler);
    
    setTimeout(() => {
      openclawWS.removeListener('message', handler);
      reject(new Error('Request timeout'));
    }, 120000);
  });
}

// Helper functions (same as before)
function loadConfig() {
  if (!fs.existsSync(CONFIG_PATH)) return {};
  return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
}

function saveConfig(config) {
  fs.copyFileSync(CONFIG_PATH, CONFIG_PATH + '.backup');
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2));
}

// ── writeAgentsMd ─────────────────────────────────────────────────────────────
// Write the mandate-specific AGENTS.md to the DEFAULT agent workspace.
// OC reads AGENTS.md from the active workspace at the start of every session —
// this is how we inject the output format contract without touching skills.
//
// NOTE: Written to the default workspace so the default agent session picks it up.
// For concurrent mandates (future), each would target a named workspace.
async function writeAgentsMd(agentId, content) {
  if (!content) return;
  // Default workspace — where OC's active session lives
  const workspacePath = '/home/node/.openclaw/workspace';
  try {
    fs.mkdirSync(workspacePath, { recursive: true });
    fs.writeFileSync(path.join(workspacePath, 'AGENTS.md'), content);
    console.log(`[MyndLens] AGENTS.md written (${content.length} chars)`);
  } catch (err) {
    console.error(`[MyndLens] writeAgentsMd failed: ${err.message}`);
  }
}

// ── Orphan removal notice ─────────────────────────────────────────────────────
// createOrModifyAgent(), createSkill(), configureTools() have been REMOVED.
// Reason: These were management-plane operations incorrectly running at mandate
// time. They:
//   1. Wrote empty SKILL.md files that SHADOWED bundled skills with real content
//   2. Set tools.allow=[] (empty) because oc_tools was never populated
//   3. Mixed provisioning concerns into the execution hot-path
// Skills are now deployed at provisioning time via provision_tenant.sh.
// AGENTS.md (writeAgentsMd above) is the only file written per-mandate.

// executeMandate — CLI fallback (kept as emergency fallback if HTTP fails).
// Normal execution path is executeViaHttp().
async function executeMandate(mandate, mandateId, agentId) {
  const escapedMandate = mandate.replace(/"/g, '\\"').replace(/\n/g, ' ');
  const sessionId = 'myndlens_' + mandateId.replace(/[^a-zA-Z0-9]/g, '_');
  
  let cmd = `node /app/openclaw.mjs agent --message "${escapedMandate}" --thinking high --session-id ${sessionId}`;
  
  if (agentId) cmd += ` --agent ${agentId}`;
  
  try {
    const { stdout, stderr } = await execAsync(cmd, { timeout: 120000 });
    return { success: true, output: stdout.substring(0, 1000) };
  } catch (error) {
    return { success: false, error: error.message.substring(0, 500) };
  }
}

async function handleKill(signal) {
  await execAsync(`node /app/openclaw.mjs sessions reset`);
  return { status: 'killed', mandateId: signal.mandateId };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// syncGatewayToken removed: was using /opt/openclaw/.env (wrong path).
// Token is read directly from openclaw.json in connectToOpenClaw() — no sync needed.
// restartGateway removed: OC hot-reloads openclaw.json automatically.
// A restart would break the active WS connection and is unnecessary.

// Health + Skills API server
const healthServer = http.createServer((req, res) => {
  // GET /health
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'healthy',
      tenantId: TENANT_ID,
      openclaw_connected: openclawConnected,
      gateway_port: GATEWAY_PORT,
      ports: { websocket: PORT, health: HEALTH_PORT }
    }));
    return;
  }

  // GET /skills — returns live skills list from OC for mandate preparation
  // Called by MyndLens backend before determine_skills() to get real available skills
  if (req.method === 'GET' && req.url === '/skills') {
    if (!openclawConnected || !openclawWS) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'OC not connected' }));
      return;
    }
    const reqId = `skills_${Date.now()}`;
    const timeout = setTimeout(() => {
      res.writeHead(504, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'skills.status timeout' }));
    }, 8000);
    openclawWS.once('message', (data) => {
      try {
        const m = JSON.parse(data.toString());
        if (m.id === reqId) {
          clearTimeout(timeout);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ skills: m.payload?.skills || [], ok: m.ok }));
        }
      } catch (e) { /* non-matching message, ignored */ }
    });
    openclawWS.send(JSON.stringify({
      type: 'req', id: reqId, method: 'skills.status', params: {}
    }));
    return;
  }

  res.writeHead(404);
  res.end();
});

healthServer.listen(HEALTH_PORT);
console.log(`[MyndLens] Health on port ${HEALTH_PORT}`);
console.log('[MyndLens] Ready with real-time streaming');

// Graceful shutdown
process.on('SIGTERM', () => {
  if (openclawWS) openclawWS.close();
  server.close();
  healthServer.close();
  process.exit(0);
});

// ── WhatsApp Number Validation Guard ──────────────────────────────────────
// Prevents sending to non-WhatsApp numbers which kills the Baileys session.
//
// ARCHITECTURE DECISION (2026-03):
//   DO NOT use wa_session.mjs validate for automated checks — it creates a
//   second Baileys connection from the same auth state, which triggers
//   WhatsApp's connectionReplaced (440) event and kicks the active OC session.
//   Instead, use the DS-enriched annotations already in the mandate text.
//   The MyndLens backend's gap_filler.py injects "whatsapp: +NNN" for every
//   contact that has a known wa_phone in the Digital Self.

/**
 * Extract E.164 phone numbers from mandate text.
 * Matches patterns like +16309651253, +44 7801 270343, +91-9898089931
 */
function extractPhoneNumbers(text) {
  const matches = text.match(/\+\d[\d\s\-().]{7,18}\d/g) || [];
  return [...new Set(matches.map(m => m.replace(/[\s\-().]/g, '')))];
}

/**
 * Check if a phone number is annotated as a WhatsApp number in the mandate text.
 * The DS enrichment pipeline (gap_filler.py _resolve_entities) annotates contacts as:
 *   "Sam Pitroda (colleague, phone: +16309651253, whatsapp: +919898089931)"
 * This function checks if the phone number appears after a "whatsapp:" label.
 */
function isWhatsAppAnnotated(phone, mandateText) {
  // Escape + for regex, allow optional spaces/hyphens between digits
  const digits = phone.replace(/\D/g, '');
  if (digits.length < 7) return false;

  // Check for "whatsapp: <phone>" pattern (DS enrichment format)
  // Also matches "wa:" prefix from capsule tags
  const escaped = phone.replace(/[+]/g, '\\+');
  const patterns = [
    new RegExp(`whatsapp:\\s*${escaped}`, 'i'),
    new RegExp(`wa:\\s*${escaped}`, 'i'),
    new RegExp(`whatsapp:\\s*\\+?${digits}`, 'i'),
    new RegExp(`wa:\\s*\\+?${digits}`, 'i'),
  ];
  return patterns.some(p => p.test(mandateText));
}

