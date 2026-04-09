/**
 * tenant_ds_service.mjs — Tenant Digital Self Processing Service.
 *
 * Runs INSIDE the tenant's OpenClaw Docker container.
 * Port: 10014 + tenant_index * 10 (mapped in docker-compose).
 * Internal port: 10014 (always — 10013 is reserved for OpenClaw CDP/browser).
 *
 * Pipeline:
 *   1. Receive raw data or pre-extracted contacts
 *   2. Stage 1: LLM Distillation (Gemini Flash) — raw text → structured entities
 *   3. Stage 2: ONNX Vectorization — entities → entities + 384-dim vectors
 *   4. Stage 3: Split into dual streams:
 *      - Stream A: Full entities + vectors (→ device PKG via MyndLens relay)
 *      - Stream B: Vectors + enriched metadata only (→ ChromaDB QS via MyndLens)
 *   5. POST both streams to MyndLens backend
 *
 * Privacy: Raw text NEVER leaves this container (except to LLM API).
 *          MyndLens receives ONLY vectors (Stream B) and encrypted entity blobs (Stream A).
 *
 * API:
 *   POST /ds/process          — Full pipeline (raw data → dual streams)
 *   POST /ds/process-contacts — Pre-distilled contacts (skip Stage 1)
 *   POST /ds/vectorize        — Vectorize only (no distillation)
 *   GET  /health              — Service health check
 *
 * Environment:
 *   MYNDLENS_API_URL  — MyndLens backend URL
 *   LLM_KEY           — Gemini API key (MyndLens standard or BYOK)
 *   TENANT_ID         — Tenant identifier
 *   DS_SERVICE_PORT   — Port to listen on (default: 10014)
 */

import http from 'http';
import { exec, spawn } from 'child_process';
import { promisify } from 'util';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'fs';
import { getPromptsForSource, buildUserPrompt } from './tenant_prompts.mjs';
import { callGemini } from './tenant_llm.mjs';
import { vectorize, buildEmbeddingText, dimension } from './tenant_vectorizer.mjs';
import { deriveKey, encrypt, isEncrypted } from './tenant_crypto.mjs';

const PORT = parseInt(process.env.DS_SERVICE_PORT || '10014', 10);
const TENANT_ID = process.env.TENANT_ID || 'unknown';
const MYNDLENS_API = process.env.MYNDLENS_API_URL || 'https://app.myndlens.com';
const LLM_KEY = process.env.LLM_KEY || '';
const MYNDLENS_DS_TOKEN = process.env.MYNDLENS_DS_TOKEN || '';
const PKG_ENCRYPTION_SECRET = process.env.PKG_ENCRYPTION_SECRET || '';
const execAsync = promisify(exec);

const MAX_CHUNK_CHARS = 60000;
const TENANT_WORKSPACE = existsSync('/home/node/.openclaw/workspace') ? '/home/node/.openclaw/workspace' : '/tmp';
const WA_BOOTSTRAP_AUTH_DIR = '/home/node/.openclaw/credentials/whatsapp/bootstrap';
const WA_BOOTSTRAP_STATE_FILE = `${TENANT_WORKSPACE}/wa_history_bootstrap_state_${TENANT_ID}.json`;
const WA_BOOTSTRAP_STORE_FILE = `${TENANT_WORKSPACE}/wa_ds_messages_${TENANT_ID}_bootstrap.json`;
const WA_BOOTSTRAP_DISTILL_FILE = `${TENANT_WORKSPACE}/wa_ds_distilled_${TENANT_ID}.json`;
const bootstrapJobs = new Map();

// Cache for Device public keys (per-user)
const _deviceKeyCache = new Map();

/**
 * Fetch the Device's X25519 public key from MyndLens CP.
 * Cached per-user for 1 hour to avoid repeated network calls.
 * Returns null if no key registered (device hasn't set up yet).
 */
async function fetchDevicePublicKey(userId) {
  const cached = _deviceKeyCache.get(userId);
  if (cached && (Date.now() - cached.ts) < 3600000) return cached.key;

  try {
    const res = await fetch(`${MYNDLENS_API}/api/ds-keys/public/${userId}`, {
      headers: { 'X-Internal-Key': MYNDLENS_DS_TOKEN },
    });
    if (!res.ok) {
      if (res.status === 404) return null; // No key registered yet
      console.warn(`[TenantDS:${TENANT_ID}] Device key fetch failed: HTTP ${res.status}`);
      return null;
    }
    const data = await res.json();
    const key = data.public_key;
    _deviceKeyCache.set(userId, { key, ts: Date.now() });
    return key;
  } catch (e) {
    console.warn(`[TenantDS:${TENANT_ID}] Device key fetch error: ${e.message}`);
    return null;
  }
}


// ── Request parsing ──────────────────────────────────────────────────────────

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 50_000_000) reject(new Error('Body too large')); });
    req.on('end', () => { try { resolve(body ? JSON.parse(body) : {}); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}

function send(res, status, body) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

function nowIso() {
  return new Date().toISOString();
}

function readBootstrapState() {
  try {
    if (!existsSync(WA_BOOTSTRAP_STATE_FILE)) return null;
    return JSON.parse(readFileSync(WA_BOOTSTRAP_STATE_FILE, 'utf8'));
  } catch {
    return null;
  }
}

function writeBootstrapState(patch = {}) {
  const current = readBootstrapState() || {
    tenant_id: TENANT_ID,
    status: 'idle',
    started_at: nowIso(),
  };
  const next = {
    ...current,
    ...patch,
    tenant_id: TENANT_ID,
    auth_dir: WA_BOOTSTRAP_AUTH_DIR,
    raw_store_file: WA_BOOTSTRAP_STORE_FILE,
    distill_file: WA_BOOTSTRAP_DISTILL_FILE,
    updated_at: nowIso(),
  };
  mkdirSync(TENANT_WORKSPACE, { recursive: true });
  writeFileSync(WA_BOOTSTRAP_STATE_FILE, JSON.stringify(next, null, 2));
  return next;
}

async function runBootstrapDistillation(userId = '') {
  writeBootstrapState({ status: 'distilling', phase: 'distilling', user_id: userId || '' });
  const cmd = `node /app/wa_ds_extractor.mjs ${TENANT_ID} ${MYNDLENS_API}`;
  const result = await execAsync(cmd, {
    timeout: 30 * 60 * 1000,
    env: {
      ...process.env,
      HOME: '/home/node',
      USER_ID: userId || '',
      WA_DS_PROCESS_STORE_ONLY: '1',
      WA_DS_STORE_FILE: WA_BOOTSTRAP_STORE_FILE,
      WA_DS_DISTILL_FILE: WA_BOOTSTRAP_DISTILL_FILE,
    },
  });

  let contactCount = 0;
  for (const line of String(result.stdout || '').split('\n')) {
    try {
      const parsed = JSON.parse(line);
      if (parsed.phase === 'done' && typeof parsed.contact_count === 'number') {
        contactCount = parsed.contact_count;
      }
    } catch {}
  }

  writeBootstrapState({
    status: 'done',
    phase: 'done',
    contact_count: contactCount,
    completed_at: nowIso(),
  });
  return contactCount;
}

function spawnBootstrapJob({ method, phoneNumber, userId }) {
  const existing = bootstrapJobs.get(TENANT_ID);
  if (existing && !existing.killed) {
    throw new Error('bootstrap_already_running');
  }

  const args = ['/app/wa_history_bootstrap.mjs', TENANT_ID, method];
  if (method === 'phone' && phoneNumber) args.push(phoneNumber);

  try { rmSync(WA_BOOTSTRAP_AUTH_DIR, { recursive: true, force: true }); } catch {}
  try { rmSync(WA_BOOTSTRAP_STORE_FILE, { force: true }); } catch {}
  try { rmSync(WA_BOOTSTRAP_DISTILL_FILE, { force: true }); } catch {}
  try { rmSync(WA_BOOTSTRAP_STATE_FILE, { force: true }); } catch {}
  mkdirSync(WA_BOOTSTRAP_AUTH_DIR, { recursive: true });

  writeBootstrapState({
    job_id: `bootstrap_${Date.now().toString(36)}`,
    status: 'starting',
    phase: 'starting',
    method,
    phone_number: phoneNumber || null,
    user_id: userId || '',
    started_at: nowIso(),
    completed_at: null,
    contact_count: 0,
    total_messages: 0,
    pairing_code: null,
    qr_raw: null,
    error: null,
  });

  const proc = spawn('node', args, {
    env: {
      ...process.env,
      HOME: '/home/node',
      WA_BOOTSTRAP_AUTH_DIR,
      WA_BOOTSTRAP_STORE_FILE,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  bootstrapJobs.set(TENANT_ID, proc);

  let stdoutBuffer = '';
  proc.stdout.on('data', (chunk) => {
    stdoutBuffer += chunk.toString();
    const lines = stdoutBuffer.split('\n');
    stdoutBuffer = lines.pop();
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const evt = JSON.parse(line);
        const patch = { phase: evt.phase || null };
        if (evt.pairing_code) {
          patch.pairing_code = evt.pairing_code;
          patch.status = 'awaiting_code';
        }
        if (evt.qr_raw) {
          patch.qr_raw = evt.qr_raw;
          patch.status = 'awaiting_scan';
        }
        if (evt.phase === 'connected' || evt.phase === 'history_batch' || evt.phase === 'messages_upsert') {
          patch.status = 'capturing_history';
        }
        if (typeof evt.total_messages === 'number') patch.total_messages = evt.total_messages;
        if (typeof evt.contacts === 'number') patch.contacts_seen = evt.contacts;
        if (evt.phase === 'done') {
          patch.status = 'captured';
          patch.raw_store_file = evt.store_file || WA_BOOTSTRAP_STORE_FILE;
          patch.contacts_seen = evt.contacts || 0;
          patch.total_messages = evt.total_messages || 0;
        }
        if (evt.phase === 'failed' || evt.error) {
          patch.status = 'failed';
          patch.error = evt.error || evt.phase;
        }
        writeBootstrapState(patch);
      } catch {}
    }
  });

  proc.stderr.on('data', (chunk) => {
    const text = chunk.toString().trim();
    if (!text) return;
    const current = readBootstrapState() || {};
    writeBootstrapState({
      status: current.status === 'failed' ? 'failed' : current.status,
      last_log: text.substring(0, 300),
    });
  });

  proc.on('close', async (code) => {
    bootstrapJobs.delete(TENANT_ID);
    const current = readBootstrapState() || {};
    if (code !== 0 || current.status === 'failed') {
      writeBootstrapState({
        status: 'failed',
        phase: 'failed',
        error: current.error || `bootstrap exited with code ${code}`,
      });
      return;
    }

    if (current.status === 'captured') {
      try {
        await runBootstrapDistillation(userId || current.user_id || '');
      } catch (e) {
        writeBootstrapState({ status: 'failed', phase: 'failed', error: e.message });
      }
    }
  });

  return readBootstrapState();
}

// ── Stage 1: LLM Distillation ────────────────────────────────────────────────

async function distill(rawData, source, userName, userId) {
  if (!rawData || !rawData.trim()) return [];
  if (!LLM_KEY) {
    console.error(`[TenantDS:${TENANT_ID}] No LLM key — cannot distill`);
    return [];
  }

  const todayDate = new Date().toISOString().split('T')[0];
  const { system, user: userTemplate } = getPromptsForSource(source);
  const userContent = buildUserPrompt(userTemplate, { userName, todayDate, rawData });

  console.log(`[TenantDS:${TENANT_ID}] DISTILL source=${source} chars=${rawData.length}`);
  const start = Date.now();

  try {
    const responseText = await callGemini(system, userContent, {
      apiKey: LLM_KEY,
      maxOutputTokens: 65536,
    });

    console.log(`[TenantDS:${TENANT_ID}] LLM_DONE response_chars=${responseText.length} time=${Date.now() - start}ms`);
    return parseEntities(responseText, source);
  } catch (e) {
    console.error(`[TenantDS:${TENANT_ID}] LLM_ERROR: ${e.message}`);
    return [];
  }
}

function parseEntities(text, source) {
  let clean = (text || '').trim();
  if (clean.startsWith('```json')) clean = clean.slice(7);
  if (clean.startsWith('```')) clean = clean.slice(3);
  if (clean.endsWith('```')) clean = clean.slice(0, -3);
  clean = clean.trim();

  try {
    const parsed = JSON.parse(clean);
    if (Array.isArray(parsed)) {
      console.log(`[TenantDS:${TENANT_ID}] PARSE_OK source=${source} entities=${parsed.length}`);
      return parsed;
    }
    if (typeof parsed === 'object') {
      console.log(`[TenantDS:${TENANT_ID}] PARSE_OK source=${source} single_entity=1`);
      return [parsed];
    }
    console.warn(`[TenantDS:${TENANT_ID}] PARSE_UNEXPECTED source=${source} type=${typeof parsed}`);
    return [];
  } catch (primaryErr) {
    // Try to find JSON array in response
    const startIdx = clean.indexOf('[');
    const endIdx = clean.lastIndexOf(']');
    if (startIdx >= 0 && endIdx > startIdx) {
      try {
        const parsed = JSON.parse(clean.substring(startIdx, endIdx + 1));
        if (Array.isArray(parsed)) {
          console.log(`[TenantDS:${TENANT_ID}] PARSE_RECOVERED source=${source} entities=${parsed.length} (extracted from response)`);
          return parsed;
        }
      } catch { /* fall through */ }
    }
    console.error(`[TenantDS:${TENANT_ID}] PARSE_FAILED source=${source} error=${primaryErr.message} response_preview="${clean.substring(0, 120)}"`);
    return [];
  }
}

// ── Stage 1b: Validate entities ──────────────────────────────────────────────

function validateEntity(entity, source, index) {
  if (typeof entity !== 'object' || !entity) {
    console.warn(`[TenantDS:${TENANT_ID}] VALIDATE_SKIP source=${source} idx=${index} reason=not_object`);
    return null;
  }

  const identity = entity.identity || {};
  // identity.display_name is set by the tenant DS service itself.
  // identity.name is set by wa_ds_extractor.mjs (from Baileys contact data).
  // Fall through all available name fields before giving up.
  let displayName = identity.display_name || identity.name || identity.phone || entity.label || entity.name || '';
  if (!displayName) {
    console.warn(`[TenantDS:${TENANT_ID}] VALIDATE_SKIP source=${source} idx=${index} reason=no_display_name`);
    return null;
  }

  // Ensure canonical_id
  if (!entity.canonical_id) {
    const slug = displayName.toLowerCase().replace(/\s+/g, '_').replace(/@/g, '_at_').substring(0, 30);
    entity.canonical_id = `${source}_${slug}`;
  }

  // Defaults
  entity.node_type = entity.node_type || 'Person';
  entity.identity = entity.identity || {};
  entity.identity.display_name = entity.identity.display_name || displayName;
  entity.identity.full_name = entity.identity.full_name || displayName;
  entity.identity.nicknames = entity.identity.nicknames || [];
  entity.identity.relationship = entity.identity.relationship || 'unknown';
  entity.identity.confidential = entity.identity.confidential || false;

  entity.contact_methods = entity.contact_methods || {};
  entity.contact_methods.phones = entity.contact_methods.phones || [];
  entity.contact_methods.emails = entity.contact_methods.emails || [];

  const mi = entity.merged_intelligence = entity.merged_intelligence || {};
  mi.active_threads = mi.active_threads || [];
  mi.pending_actions = mi.pending_actions || { user_owes: [], they_owe: [] };
  mi.emotional_context = mi.emotional_context || { current_mood: 'unknown', recent_events: [] };
  mi.pattern = mi.pattern || { frequency: 'unknown', style: 'unknown', peak_hours: 'varied' };
  mi.aspirations_mentioned = mi.aspirations_mentioned || [];
  mi.inner_circle_rank = mi.inner_circle_rank || 99;
  mi.message_count_90d = mi.message_count_90d || 0;
  mi.sentiment = mi.sentiment || 'neutral';
  mi.upcoming_events = mi.upcoming_events || [];
  mi.financial_mentions = mi.financial_mentions || [];
  mi.travel_mentions = mi.travel_mentions || [];

  // Auto-generate merge keys
  const mk = entity.merge_keys = entity.merge_keys || {};
  if (!mk.name_variants) {
    const nameLower = displayName.toLowerCase();
    const variants = new Set([nameLower, ...nameLower.split(/\s+/)]);
    for (const nick of (entity.identity.nicknames || [])) {
      if (nick) variants.add(nick.toLowerCase());
    }
    mk.name_variants = [...variants];
  }
  if (!mk.phone_normalized) {
    mk.phone_normalized = (entity.contact_methods.phones || [])
      .map(p => p.number).filter(Boolean);
  }
  if (!mk.email_normalized) {
    mk.email_normalized = (entity.contact_methods.emails || [])
      .map(e => (e.address || '').toLowerCase()).filter(Boolean);
  }

  return entity;
}

// ── Normalize extractor contacts (pre-distilled) ─────────────────────────────

function normalizeExtractorContact(contact, source) {
  // If contact is already in canonical format (has canonical_id + identity.display_name),
  // validate and pass through. Otherwise fall back to legacy normalization.
  const identity = contact.identity || {};
  const mi = contact.merged_intelligence || {};

  // Canonical format: has display_name and canonical_id
  if (identity.display_name && contact.canonical_id) {
    // Validate: display_name must not be empty
    const displayName = identity.display_name.trim();
    if (!displayName) {
      console.warn(`[TenantDS:${TENANT_ID}] NORMALIZE_SKIP source=${source} reason=empty_display_name`);
      return null;
    }
    // Pass through with source tag and defaults
    contact.source = source;
    contact.node_type = contact.node_type || 'Person';
    identity.display_name = displayName;
    identity.full_name = identity.full_name || displayName;
    identity.nicknames = identity.nicknames || [];
    identity.relationship = identity.relationship || 'unknown';
    identity.confidential = !!identity.confidential;
    contact.contact_methods = contact.contact_methods || { phones: [], emails: [], telegram: null, linkedin_url: null };
    contact.merged_intelligence = {
      active_threads: mi.active_threads || [],
      pending_actions: mi.pending_actions || { user_owes: [], they_owe: [] },
      emotional_context: mi.emotional_context || { current_mood: 'unknown', recent_events: [] },
      pattern: mi.pattern || {},
      aspirations_mentioned: mi.aspirations_mentioned || [],
      inner_circle_rank: mi.inner_circle_rank || 99,
      message_count_90d: mi.message_count_90d || 0,
      last_interaction: mi.last_interaction || '',
      sentiment: mi.sentiment || 'neutral',
      upcoming_events: mi.upcoming_events || [],
      financial_mentions: mi.financial_mentions || [],
      travel_mentions: mi.travel_mentions || [],
      mentioned_people: mi.mentioned_people || [],
    };
    contact.merge_keys = contact.merge_keys || {};
    return contact;
  }

  // Legacy format fallback (non-canonical contacts from older extractors)
  let name = identity.display_name || identity.name || '';
  if (!name || name.toLowerCase() === 'unknown') name = identity.phone || 'unknown';

  const slug = name.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '').substring(0, 30);
  const canonicalId = `${source}_${slug}`;
  if (!slug) {
    console.warn(`[TenantDS:${TENANT_ID}] NORMALIZE_SKIP source=${source} reason=empty_slug name="${name}"`);
    return null;
  }

  const phone = identity.phone || '';
  const email = identity.email || '';
  const phones = [];
  const emails = [];
  if (phone) phones.push({ number: phone, type: source.startsWith('whatsapp') ? 'whatsapp' : 'mobile', source, primary: true });
  if (email) emails.push({ address: email, source, primary: true });

  return {
    canonical_id: canonicalId,
    node_type: contact.node_type || 'Person',
    source,
    identity: {
      display_name: name,
      full_name: name,
      relationship: identity.relationship || 'unknown',
      nicknames: identity.nicknames || [],
      confidential: identity.confidential || false,
    },
    contact_methods: { phones, emails, telegram: null, linkedin_url: null },
    merged_intelligence: {
      pattern: mi.pattern || contact.pattern || {},
      sentiment: mi.sentiment || contact.sentiment || 'neutral',
      active_threads: mi.active_threads || contact.active_threads || [],
      pending_actions: mi.pending_actions || contact.pending_actions || { user_owes: [], they_owe: [] },
      aspirations_mentioned: mi.aspirations_mentioned || contact.aspirations_mentioned || [],
      upcoming_events: mi.upcoming_events || contact.upcoming_events || [],
      emotional_context: mi.emotional_context || contact.emotional_context || { current_mood: 'unknown', recent_events: [] },
      financial_mentions: mi.financial_mentions || contact.financial_mentions || [],
      travel_mentions: mi.travel_mentions || contact.travel_mentions || [],
      mentioned_people: mi.mentioned_people || contact.mentioned_people || [],
      inner_circle_rank: mi.inner_circle_rank || contact.inner_circle_rank || 99,
      message_count_90d: mi.message_count_90d || contact.message_count_90d || 0,
      last_interaction: mi.last_interaction || contact.last_interaction || '',
    },
    interaction_signals: contact.interaction_signals || {},
    merge_keys: contact.merge_keys || {},
  };
}

// ── Chunking ─────────────────────────────────────────────────────────────────

function chunkData(text, maxChars) {
  if (text.length <= maxChars) return [text];
  const chunks = [];
  let remaining = text;
  while (remaining) {
    if (remaining.length <= maxChars) { chunks.push(remaining); break; }
    let splitAt = remaining.lastIndexOf('\n', maxChars);
    if (splitAt <= 0) splitAt = maxChars;
    chunks.push(remaining.substring(0, splitAt));
    remaining = remaining.substring(splitAt).replace(/^\n+/, '');
  }
  return chunks;
}

// ── Deduplication ────────────────────────────────────────────────────────────

function deduplicateEntities(entities) {
  const seen = new Map();
  for (const e of entities) {
    const cid = e.canonical_id || '';
    if (cid) seen.set(cid, e);
    else seen.set(Symbol(), e);
  }
  const deduped = [...seen.values()];
  const removed = entities.length - deduped.length;
  if (removed > 0) {
    console.log(`[TenantDS:${TENANT_ID}] DEDUP removed=${removed} before=${entities.length} after=${deduped.length}`);
  }
  return deduped;
}

// ── Build merge key metadata strings ─────────────────────────────────────────

function buildMergeKeyStrings(entity) {
  const phones = new Set();
  const emails = new Set();
  const names = new Set();

  const cm = entity.contact_methods || {};
  for (const p of (cm.phones || [])) {
    const num = (p.number || '').replace(/[^\d+]/g, '');
    if (num.length >= 7) phones.add(num);
  }
  for (const e of (cm.emails || [])) {
    const addr = (e.address || '').trim().toLowerCase();
    if (addr) emails.add(addr);
  }

  const mk = entity.merge_keys || {};
  for (const p of (mk.phone_normalized || [])) { if (p) phones.add(p); }
  for (const e of (mk.email_normalized || [])) { if (e) emails.add(e.toLowerCase()); }
  for (const n of (mk.name_variants || [])) { if (n) names.add(n.toLowerCase()); }

  const id = entity.identity || {};
  if (id.display_name) names.add(id.display_name.toLowerCase());
  for (const nick of (id.nicknames || [])) { if (nick) names.add(nick.toLowerCase()); }

  return {
    phones: [...phones].sort().join(','),
    emails: [...emails].sort().join(','),
    names: [...names].sort().join(','),
  };
}

// ── Stream B: Build QS record (vectors + metadata only for ChromaDB) ─────────

function buildStreamBRecord(entity, userId, source) {
  const mergeKeys = buildMergeKeyStrings(entity);
  const mi = entity.merged_intelligence || {};
  return {
    canonical_id: entity.canonical_id || '',
    user_id: userId,
    vector: entity.vector || [],
    source,
    display_name: (entity.identity || {}).display_name || '',
    node_type: entity.node_type || 'Person',
    inner_circle_rank: mi.inner_circle_rank || 99,
    relationship: (entity.identity || {}).relationship || 'unknown',
    updated_at: new Date().toISOString(),
    phones: mergeKeys.phones,
    emails: mergeKeys.emails,
    names: mergeKeys.names,
    qs_record: 'true',
  };
}

// ── Full Pipeline ────────────────────────────────────────────────────────────

async function processFull(rawData, source, userId, userName) {
  const pipelineStart = Date.now();
  const rawChars = rawData.length;

  console.log(`[TenantDS:${TENANT_ID}] PIPELINE_START source=${source} user=${userId} chars=${rawChars}`);

  // Stage 1: Chunk + Distill
  const chunks = chunkData(rawData, MAX_CHUNK_CHARS);
  let allEntities = [];
  const stage1Start = Date.now();

  for (let i = 0; i < chunks.length; i++) {
    console.log(`[TenantDS:${TENANT_ID}] STAGE1_CHUNK ${i + 1}/${chunks.length} chars=${chunks[i].length}`);
    const entities = await distill(chunks[i], source, userName, userId);
    allEntities.push(...entities);
  }

  // Validate
  allEntities = allEntities
    .map((e, i) => validateEntity(e, source, i))
    .filter(Boolean);

  const stage1Time = Date.now() - stage1Start;
  console.log(`[TenantDS:${TENANT_ID}] STAGE1_DONE entities=${allEntities.length} time=${stage1Time}ms`);

  if (allEntities.length === 0) {
    return buildResult(source, [], [], rawChars, chunks.length, stage1Time, 0, Date.now() - pipelineStart);
  }

  // Dedup
  allEntities = deduplicateEntities(allEntities);

  // Stage 2: Vectorize
  const stage2Start = Date.now();
  const vectorized = await vectorize(allEntities);
  const stage2Time = Date.now() - stage2Start;

  const vectorizedCount = vectorized.filter(e => e.vector).length;
  console.log(`[TenantDS:${TENANT_ID}] STAGE2_DONE vectorized=${vectorizedCount}/${vectorized.length} time=${stage2Time}ms`);

  // Stage 3: Build dual streams
  const streamA = vectorized; // Full entities → device PKG
  const streamB = vectorized
    .filter(e => e.vector && e.vector.length > 0)
    .map(e => buildStreamBRecord(e, userId, source));

  const totalTime = Date.now() - pipelineStart;
  console.log(`[TenantDS:${TENANT_ID}] PIPELINE_DONE source=${source} stream_a=${streamA.length} stream_b=${streamB.length} total=${totalTime}ms`);

  return buildResult(source, streamA, streamB, rawChars, chunks.length, stage1Time, stage2Time, totalTime);
}

// ── Contacts Pipeline (pre-distilled, skip Stage 1) ──────────────────────────

async function processContacts(contacts, source, userId, userName) {
  const pipelineStart = Date.now();
  console.log(`[TenantDS:${TENANT_ID}] CONTACTS_MODE source=${source} contacts=${contacts.length} (skip distillation)`);

  let allEntities = contacts
    .map(c => normalizeExtractorContact(c, source))
    .filter(Boolean);

  if (allEntities.length === 0) {
    console.warn(`[TenantDS:${TENANT_ID}] CONTACTS_EMPTY source=${source} all ${contacts.length} contacts rejected during normalization`);
    return buildResult(source, [], [], 0, 0, 0, 0, 0);
  }

  allEntities = deduplicateEntities(allEntities);

  // Stage 2: Vectorize
  const stage2Start = Date.now();
  const vectorized = await vectorize(allEntities);
  const stage2Time = Date.now() - stage2Start;

  const vectorizedCount = vectorized.filter(e => e.vector).length;

  // Stage 3: Dual streams
  const streamA = vectorized;
  const streamB = vectorized
    .filter(e => e.vector && e.vector.length > 0)
    .map(e => buildStreamBRecord(e, userId, source));

  const totalTime = Date.now() - pipelineStart;
  console.log(`[TenantDS:${TENANT_ID}] CONTACTS_DONE source=${source} vectorized=${vectorizedCount} stream_a=${streamA.length} stream_b=${streamB.length} time=${totalTime}ms`);

  return buildResult(source, streamA, streamB, 0, contacts.length, 0, stage2Time, totalTime);
}

// ── Result builder ───────────────────────────────────────────────────────────

function buildResult(source, streamA, streamB, rawChars, chunks, stage1Ms, stage2Ms, totalMs) {
  return {
    pkg_diff: {
      source,
      nodes: streamA,
      edges: [],
      processed_at: new Date().toISOString(),
    },
    backend_qs_records: streamB,
    stats: {
      raw_chars: rawChars,
      chunks,
      entities_distilled: streamA.length,
      entities_vectorized: streamA.filter(e => e.vector).length,
      processing_time_ms: totalMs,
      stage1_time_ms: stage1Ms,
      stage2_time_ms: stage2Ms,
    },
  };
}

// ── Forward to MyndLens ──────────────────────────────────────────────────────

async function forwardToMyndLens(result, userId, tenantId) {
  const { pkg_diff, backend_qs_records, stats } = result;

  console.log(`[TenantDS:${TENANT_ID}] FORWARD_START user=${userId} stream_a=${pkg_diff.nodes.length} stream_b=${backend_qs_records.length}`);

  // Stream A → MyndLens /receive-pkg-diff (relay to device)
  // Phase 8: ECIES encryption with Device public key (MyndLens is cryptographically blind)
  if (pkg_diff.nodes.length > 0) {
    try {
      let payload;
      
      // Try v2 encryption first (Device Key — truly blind)
      const deviceKey = await fetchDevicePublicKey(userId);
      if (deviceKey) {
        const { encryptForDevice } = await import('./tenant_crypto_v2.mjs');
        payload = encryptForDevice(pkg_diff, deviceKey, userId);
        console.log(`[TenantDS:${TENANT_ID}] Stream A ENCRYPTED_V2 (Device Key) user=${userId} nodes=${pkg_diff.nodes.length}`);
      } else if (PKG_ENCRYPTION_SECRET) {
        // Fallback to v1 (shared secret) for devices that haven't registered keys yet
        const key = deriveKey(PKG_ENCRYPTION_SECRET, userId, tenantId);
        payload = encrypt(pkg_diff, key);
        console.log(`[TenantDS:${TENANT_ID}] Stream A ENCRYPTED_V1 (shared secret) user=${userId} nodes=${pkg_diff.nodes.length}`);
      } else {
        payload = pkg_diff;
        console.warn(`[TenantDS:${TENANT_ID}] Stream A UNENCRYPTED (no key available)`);
      }

      const res = await fetch(`${MYNDLENS_API}/api/digital-self/receive-pkg-diff`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Internal-Key': MYNDLENS_DS_TOKEN,
          'X-Tenant-ID': tenantId,
        },
        body: JSON.stringify({
          user_id: userId,
          tenant_id: tenantId,
          pkg_diff: payload,
        }),
      });
      const data = await res.json().catch(() => ({}));
      console.log(`[TenantDS:${TENANT_ID}] Stream A → MyndLens: ${res.status} delivered=${data.delivered}`);
    } catch (e) {
      console.error(`[TenantDS:${TENANT_ID}] Stream A forward error: ${e.message}`);
    }
  }

  // Stream B → MyndLens /receive-vectors (store in ChromaDB)
  if (backend_qs_records.length > 0) {
    try {
      const res = await fetch(`${MYNDLENS_API}/api/digital-self/receive-vectors`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Internal-Key': MYNDLENS_DS_TOKEN,
          'X-Tenant-ID': tenantId,
        },
        body: JSON.stringify({
          user_id: userId,
          tenant_id: tenantId,
          qs_records: backend_qs_records,
        }),
      });
      const data = await res.json().catch(() => ({}));
      console.log(`[TenantDS:${TENANT_ID}] Stream B → MyndLens: ${res.status} stored=${data.stored}`);
    } catch (e) {
      console.error(`[TenantDS:${TENANT_ID}] Stream B forward error: ${e.message}`);
    }
  }
}

// ── HTTP Server ──────────────────────────────────────────────────────────────


// ── Source Extraction Adapters ────────────────────────────────────────────────
// Tenant-side data fetching. ObeGee passes OAuth tokens; tenant calls APIs directly.
// Raw data NEVER leaves the tenant — fetched, distilled, and vectorized locally.

const GMAIL_API = 'https://gmail.googleapis.com/gmail/v1/users/me';
const DRIVE_API = 'https://www.googleapis.com/drive/v3';

async function _fetchGmail(accessToken, params = {}) {
  const days = params.days || 7;
  const maxThreads = params.max_threads || 50;
  const sinceDate = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];
  const query = `after:${sinceDate}`;

  const listUrl = `${GMAIL_API}/messages?q=${encodeURIComponent(query)}&labelIds=SENT&maxResults=${maxThreads}`;
  const listResp = await fetch(listUrl, { headers: { Authorization: `Bearer ${accessToken}` } });
  if (!listResp.ok) throw new Error(`Gmail list failed: ${listResp.status}`);
  const listData = await listResp.json();
  const messageIds = (listData.messages || []).map(m => m.id);

  if (messageIds.length === 0) return '';

  const lines = [];
  for (const msgId of messageIds.slice(0, maxThreads)) {
    try {
      const msgResp = await fetch(`${GMAIL_API}/messages/${msgId}?format=full`, {
        headers: { Authorization: `Bearer ${accessToken}` },
      });
      if (!msgResp.ok) continue;
      const msg = await msgResp.json();

      const headers = msg.payload?.headers || [];
      const getHeader = (name) => (headers.find(h => h.name.toLowerCase() === name.toLowerCase()) || {}).value || '';
      const from = getHeader('From');
      const to = getHeader('To');
      const subject = getHeader('Subject');
      const date = getHeader('Date');

      let body = '';
      const extractText = (part) => {
        if (part.mimeType === 'text/plain' && part.body?.data) {
          body += Buffer.from(part.body.data, 'base64url').toString('utf8');
        }
        if (part.parts) part.parts.forEach(extractText);
      };
      extractText(msg.payload || {});

      lines.push(`From: ${from}\nTo: ${to}\nDate: ${date}\nSubject: ${subject}\n${body.substring(0, 2000)}\n---`);
      await new Promise(r => setTimeout(r, 100));
    } catch (_) {}
  }
  return lines.join('\n');
}

async function _fetchGDrive(accessToken, params = {}) {
  const days = params.days || 1;
  const maxFiles = params.max_files || 30;
  const since = new Date(Date.now() - days * 86400000).toISOString();

  const query = `modifiedTime > '${since}' and mimeType != 'application/vnd.google-apps.folder'`;
  const listUrl = `${DRIVE_API}/files?q=${encodeURIComponent(query)}&fields=files(id,name,mimeType,modifiedTime)&pageSize=${maxFiles}`;
  const listResp = await fetch(listUrl, { headers: { Authorization: `Bearer ${accessToken}` } });
  if (!listResp.ok) throw new Error(`GDrive list failed: ${listResp.status}`);
  const files = (await listResp.json()).files || [];

  if (files.length === 0) return '';

  const lines = [];
  for (const file of files) {
    try {
      let content = '';
      if (file.mimeType.startsWith('application/vnd.google-apps.')) {
        const exportUrl = `${DRIVE_API}/files/${file.id}/export?mimeType=${encodeURIComponent('text/plain')}`;
        const resp = await fetch(exportUrl, { headers: { Authorization: `Bearer ${accessToken}` } });
        if (resp.ok) content = await resp.text();
      } else {
        const resp = await fetch(`${DRIVE_API}/files/${file.id}?alt=media`, { headers: { Authorization: `Bearer ${accessToken}` } });
        if (resp.ok) { const text = await resp.text(); if (text.length < 100000) content = text; }
      }
      if (content && content.length > 50) {
        lines.push(`File: ${file.name}\nModified: ${file.modifiedTime}\n${content.substring(0, 5000)}\n---`);
      }
      await new Promise(r => setTimeout(r, 200));
    } catch (_) {}
  }
  return lines.join('\n');
}

async function _fetchLinkedIn(accessToken, params = {}) {
  if (params.csv_data) return params.csv_data;
  return '';
}

async function _fetchIMAP(accessToken, params = {}) {
  // IMAP: raw data is passed directly in params (IMAP credentials stay on ObeGee)
  // The tenant doesn't connect to IMAP servers — ObeGee handles the connection
  // and passes the extracted text for distillation only
  if (params.raw_data) return params.raw_data;
  return '';
}




// ── Phase 8: Delta Sync from OC Session Data ─────────────────────────────────
// Reads OC's conversation JSONL files for recent WhatsApp messages.
// Groups by contact, extracts user messages, sends through delta distillation.
// Triggered by daily cron or manual POST /management/wa/delta-sync

async function handleDeltaSync(req, res) {
  const body = await parseBody(req).catch(() => ({}));
  const sinceHours = body.since_hours || 24;
  const sinceMs = Date.now() - (sinceHours * 3600 * 1000);
  const userId = body.user_id || process.env.USER_ID || '';

  console.log(`[TenantDS:${TENANT_ID}] DELTA_SYNC start since=${sinceHours}h user=${userId}`);

  try {
    const fs = await import('fs');
    const path = await import('path');

    // Find OC session JSONL files
    const agentsDir = '/home/node/.openclaw/agents';
    const sessionFiles = [];

    function findJsonl(dir) {
      try {
        for (const entry of fs.default.readdirSync(dir, { withFileTypes: true })) {
          const full = path.default.join(dir, entry.name);
          if (entry.isDirectory()) findJsonl(full);
          else if (entry.name.endsWith('.jsonl')) {
            const stat = fs.default.statSync(full);
            if (stat.mtimeMs > sinceMs) sessionFiles.push(full);
          }
        }
      } catch {}
    }
    findJsonl(agentsDir);

    console.log(`[TenantDS:${TENANT_ID}] DELTA_SYNC found ${sessionFiles.length} recent session files`);

    // Parse messages from JSONL files, group by WhatsApp contact
    const contactMessages = new Map(); // phone → { name, messages[] }

    for (const file of sessionFiles) {
      try {
        const lines = fs.default.readFileSync(file, 'utf8').split('\n').filter(Boolean);
        for (const line of lines) {
          try {
            const entry = JSON.parse(line);
            if (entry.type !== 'message') continue;
            const msg = entry.message || {};
            const ts = new Date(entry.timestamp || 0).getTime();
            if (ts < sinceMs) continue;

            // Extract WhatsApp user messages (role: user from WhatsApp channel)
            if (msg.role === 'user' && typeof msg.content === 'string') {
              // Find which contact this session belongs to
              // OC session keys contain channel info: "agent:main:main" with deliveryContext
              const text = msg.content;
              if (text.length < 3) continue;
              
              // Use session file path to determine agent/context
              const sessionKey = path.default.basename(file, '.jsonl');
              if (!contactMessages.has(sessionKey)) {
                contactMessages.set(sessionKey, { name: sessionKey, messages: [] });
              }
              contactMessages.get(sessionKey).messages.push({
                text: text.substring(0, 500),
                timestamp: ts,
                fromMe: false,
              });
            }

            // Also capture assistant responses for context
            if (msg.role === 'assistant' && Array.isArray(msg.content)) {
              for (const block of msg.content) {
                if (block.type === 'text' && block.text) {
                  const sessionKey = path.default.basename(file, '.jsonl');
                  if (!contactMessages.has(sessionKey)) {
                    contactMessages.set(sessionKey, { name: sessionKey, messages: [] });
                  }
                  contactMessages.get(sessionKey).messages.push({
                    text: block.text.substring(0, 500),
                    timestamp: new Date(entry.timestamp || 0).getTime(),
                    fromMe: true,
                  });
                }
              }
            }
          } catch {}
        }
      } catch (e) {
        console.warn(`[TenantDS:${TENANT_ID}] DELTA_SYNC error reading ${file}: ${e.message}`);
      }
    }

    const totalMessages = Array.from(contactMessages.values()).reduce((s, c) => s + c.messages.length, 0);
    console.log(`[TenantDS:${TENANT_ID}] DELTA_SYNC parsed ${totalMessages} messages from ${contactMessages.size} sessions`);

    if (totalMessages === 0) {
      send(res, 200, { status: 'no_new_messages', since_hours: sinceHours });
      return;
    }

    // Build delta text for distillation
    const deltaLines = [`DELTA UPDATE — Recent conversations (last ${sinceHours}h):\n`];
    for (const [sessionKey, data] of contactMessages) {
      deltaLines.push(`Session: ${data.name}`);
      const sorted = data.messages.sort((a, b) => a.timestamp - b.timestamp);
      for (const msg of sorted.slice(-30)) {
        const speaker = msg.fromMe ? 'Agent' : 'User';
        deltaLines.push(`  ${speaker}: ${msg.text.substring(0, 200)}`);
      }
      deltaLines.push('');
    }

    // Process through DS pipeline as delta
    const deltaText = deltaLines.join('\n');
    const result = await processFull(deltaText, 'whatsapp_delta', userId, 'User');

    // Forward to MyndLens (Stream A → device, Stream B → ChromaDB)
    await forwardToMyndLens(result, userId, TENANT_ID);

    const streamACount = result.pkg_diff?.nodes?.length || 0;
    const streamBCount = result.backend_qs_records?.length || 0;
    console.log(`[TenantDS:${TENANT_ID}] DELTA_SYNC complete: entities=${result.stats?.entities_distilled || 0} stream_a=${streamACount} stream_b=${streamBCount}`);
    send(res, 200, {
      status: 'processed',
      sessions: contactMessages.size,
      messages: totalMessages,
      entities: result.stats?.entities_distilled || 0,
      stream_a: streamACount,
      stream_b: streamBCount,
    });
  } catch (e) {
    console.error(`[TenantDS:${TENANT_ID}] DELTA_SYNC error: ${e.message}`);
    send(res, 500, { error: e.message });
  }
}


// OC readiness cache — updated by port check (lightweight, no CLI spawn)
const _ocReadyCache = { ready: false, checkedAt: 0 };
const OC_HEARTBEAT_PORT = Number(process.env.OC_HEARTBEAT_PORT || '18789');

// ── Lightweight readiness check: TCP port probe instead of CLI spawn ──────────
// The CLI `openclaw channels status` spawns a full Node.js process (~200MB).
// On a 2GB container with OC + DS + ONNX, this causes OOM.
// Instead, probe TCP port from env (default 18789 inside tenant container) — zero memory overhead.
setInterval(async () => {
  if (_ocReadyCache.ready) return; // already detected
  const net = await import('net');
  const sock = net.default.connect(OC_HEARTBEAT_PORT, () => {
    sock.destroy();
    if (!_ocReadyCache.ready) {
      _ocReadyCache.ready = true;
      _ocReadyCache.checkedAt = Date.now();
      console.log(`[TenantDS:${TENANT_ID}] HEARTBEAT: OC gateway port ${OC_HEARTBEAT_PORT} is OPEN — marking ready`);
      // Push initial status to MyndLens
      fetch(`${MYNDLENS_API}/api/digital-self/wa-status-sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Internal-Key': MYNDLENS_DS_TOKEN },
        body: JSON.stringify({ tenant_id: TENANT_ID, wa_status: 'connected' }),
      }).catch(() => {});
    }
  });
  sock.on('error', () => sock.destroy());
  sock.setTimeout(3000, () => sock.destroy());
}, 15000);

const server = http.createServer(async (req, res) => {
  // Phase 8: Delta sync endpoint
  if (req.method === 'POST' && (req.url === '/management/wa/delta-sync' || req.url === '/ds/delta-sync')) {
    return handleDeltaSync(req, res);
  }

  // Health check — includes OC readiness state (updated by heartbeat loop)
  if (req.method === 'GET' && req.url === '/health') {
    return send(res, 200, {
      status: 'healthy',
      service: 'tenant-ds-service',
      tenant_id: TENANT_ID,
      embedding_dim: dimension(),
      has_llm_key: !!LLM_KEY,
      myndlens_api: MYNDLENS_API,
      oc_ready: _ocReadyCache.ready,
    });
  }

  // POST /ds/process — DEPRECATED: raw data should not cross the network
  // Blocked for external callers. All data sourcing must happen at the tenant.
  if (req.method === 'POST' && req.url === '/ds/process') {
    console.warn(`[TenantDS:${TENANT_ID}] BLOCKED /ds/process — raw data must not be sent from Control Plane. Use /management/source-extract instead.`);
    return send(res, 410, { error: 'DEPRECATED: /ds/process is disabled. Use /management/source-extract with OAuth token.' });
  }

  // POST /ds/process-contacts — Pre-distilled contacts (skip Stage 1)
  if (req.method === 'POST' && req.url === '/ds/process-contacts') {
    try {
      const body = await parseBody(req);
      const { contacts, source, user_id, user_name } = body;

      console.log(`[TenantDS:${TENANT_ID}] REQ /ds/process-contacts source=${source || 'whatsapp'} user=${user_id || '?'} contacts=${(contacts || []).length}`);

      if (!contacts || !contacts.length) return send(res, 400, { error: 'contacts required' });

      const result = await processContacts(contacts, source || 'whatsapp', user_id || '', user_name || 'User');

      // Forward to MyndLens
      await forwardToMyndLens(result, user_id, TENANT_ID);

      send(res, 200, {
        status: 'processed',
        stats: result.stats,
        stream_a_count: result.pkg_diff.nodes.length,
        stream_b_count: result.backend_qs_records.length,
      });
    } catch (e) {
      console.error(`[TenantDS:${TENANT_ID}] /ds/process-contacts error:`, e.message);
      send(res, 500, { error: e.message });
    }
    return;
  }

  // POST /ds/vectorize — Vectorize only (no distillation, no forwarding)
  if (req.method === 'POST' && req.url === '/ds/vectorize') {
    try {
      const body = await parseBody(req);
      const { entities } = body;

      console.log(`[TenantDS:${TENANT_ID}] REQ /ds/vectorize entities=${(entities || []).length}`);

      if (!entities || !entities.length) return send(res, 400, { error: 'entities required' });

      const vectorized = await vectorize(entities);
      const count = vectorized.filter(e => e.vector).length;

      send(res, 200, {
        status: 'vectorized',
        entities: vectorized,
        vectorized_count: count,
        embedding_dim: dimension(),
      });
    } catch (e) {
      console.error(`[TenantDS:${TENANT_ID}] /ds/vectorize error:`, e.message);
      send(res, 500, { error: e.message });
    }
    return;
  }

  // ── Phase 8: Delta Processing Endpoint ─────────────────────────────────────

  // POST /ds/process-delta — Process incremental messages (delta distillation)
  if (req.method === 'POST' && req.url === '/ds/process-delta') {
    try {
      const body = await parseBody(req);
      const { raw_text, source, tenant_id, is_delta } = body;

      console.log(`[TenantDS:${TENANT_ID}] REQ /ds/process-delta source=${source} delta=${is_delta}`);

      if (!raw_text) return send(res, 400, { error: 'raw_text required' });

      // Use delta distillation prompt
      const { DELTA_DISTILLATION_SYSTEM, buildDeltaPrompt } = await import('./tenant_prompts.mjs');

      const userPrompt = buildDeltaPrompt({
        userName: 'User',
        todayDate: new Date().toISOString().split('T')[0],
        existingProfiles: '', // TODO: fetch existing profiles from QS for richer deltas
        rawData: raw_text,
      });

      // LLM distillation with delta prompt
      const llmResult = await callGemini(DELTA_DISTILLATION_SYSTEM, userPrompt, { apiKey: LLM_KEY });

      let entities = [];
      try {
        const parsed = JSON.parse(llmResult.replace(/```json\n?/g, '').replace(/```\n?/g, ''));
        entities = parsed.entities || [];
      } catch (e) {
        console.warn(`[TenantDS:${TENANT_ID}] Delta LLM output not valid JSON — attempting extraction`);
        // Try to extract JSON from the response
        const match = llmResult.match(/\{[\s\S]*\}/);
        if (match) {
          try {
            const parsed = JSON.parse(match[0]);
            entities = parsed.entities || [];
          } catch {}
        }
      }

      if (entities.length === 0) {
        return send(res, 200, { status: 'no_delta', pkg_diff: { nodes: [], edges: [] }, backend_qs_records: [] });
      }

      // Mark entities as delta updates
      for (const entity of entities) {
        entity.delta = true;
        entity.source = source || 'whatsapp_delta';
      }

      // Vectorize
      const vectorized = await vectorize(entities);

      // Build dual streams
      const pkg_nodes = vectorized.map(e => ({
        ...e,
        delta: true,
      }));

      const qs_records = vectorized.filter(e => e.vector).map(e => ({
        canonical_id: e.canonical_id || `delta_${Date.now()}`,
        user_id: body.user_id || '',
        vector: e.vector,
        source: source || 'whatsapp_delta',
        display_name: e.identity?.display_name || '',
        node_type: e.identity?.entity_type || 'Person',
        relationship: e.identity?.relationship || '',
        inner_circle_rank: e.merged_intelligence?.inner_circle_rank || 99,
        phones: (e.merge_keys?.phone_normalized || []).join(','),
        emails: (e.merge_keys?.email_normalized || []).join(','),
        names: (e.merge_keys?.name_variants || []).join(','),
        qs_record: 'true',
        delta: 'true',
      }));

      const result = {
        pkg_diff: { source: source || 'whatsapp_delta', nodes: pkg_nodes, edges: [], delta: true },
        backend_qs_records: qs_records,
        stats: { entities_in: entities.length, vectorized: qs_records.length, delta: true },
      };

      // Forward to MyndLens (encrypted if configured)
      if (body.user_id && body.tenant_id) {
        await forwardToMyndLens(result, body.user_id, body.tenant_id);
      }

      console.log(`[TenantDS:${TENANT_ID}] Delta processed: entities=${entities.length} qs_records=${qs_records.length}`);
      send(res, 200, result);
    } catch (e) {
      console.error(`[TenantDS:${TENANT_ID}] /ds/process-delta error:`, e.message);
      send(res, 500, { error: e.message });
    }
    return;
  }

  // POST /ds/contact-event — Handle real-time contact events (Phase 8g)
  if (req.method === 'POST' && req.url === '/ds/contact-event') {
    try {
      const body = await parseBody(req);
      const { event_type, jid, name, tenant_id } = body;

      console.log(`[TenantDS:${TENANT_ID}] REQ /ds/contact-event type=${event_type} name=${name}`);

      if (event_type === 'new_contact') {
        // Create minimal entity for new contact
        const entity = {
          canonical_id: `wa_${jid}`,
          identity: { display_name: name, entity_type: 'Person', relationship: 'unknown' },
          merged_intelligence: { inner_circle_rank: 90, topics: [] },
          merge_keys: { phone_normalized: [jid], name_variants: [name.toLowerCase()] },
          source: 'whatsapp_realtime',
        };

        const vectorized = await vectorize([entity]);
        if (vectorized[0]?.vector) {
          const qs_record = {
            canonical_id: entity.canonical_id,
            user_id: body.user_id || '',
            vector: vectorized[0].vector,
            source: 'whatsapp_realtime',
            display_name: name,
            node_type: 'Person',
            relationship: 'unknown',
            inner_circle_rank: 90,
            phones: jid,
            names: name.toLowerCase(),
            qs_record: 'true',
          };

          // Forward QS record to MyndLens
          try {
            await fetch(`${MYNDLENS_API}/api/digital-self/receive-vectors`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'X-Internal-Key': MYNDLENS_DS_TOKEN },
              body: JSON.stringify({ user_id: body.user_id, tenant_id, qs_records: [qs_record] }),
            });
          } catch (e) {
            console.warn(`[TenantDS:${TENANT_ID}] Forward new contact failed: ${e.message}`);
          }
        }

        send(res, 200, { status: 'new_contact_created', name });
      } else if (event_type === 'update_contact') {
        // Name change — update canonical
        send(res, 200, { status: 'contact_updated', name, note: 'Merge engine will reconcile on next sync' });
      } else {
        send(res, 200, { status: 'ignored', event_type });
      }
    } catch (e) {
      console.error(`[TenantDS:${TENANT_ID}] /ds/contact-event error:`, e.message);
      send(res, 500, { error: e.message });
    }
    return;
  }

  // POST /ds/converse — Real-time conversational AI response (Phase 10)
  // Routes user text through Gemini with DS context for natural conversation.
  // Called from ws_server.py _handle_converse_turn when active_mode=CONVERSE.
  if (req.method === 'POST' && req.url === '/ds/converse') {
    try {
      const body = await parseBody(req);
      const { text, user_id, ds_context, conversation_history, user_name } = body;

      console.log(`[TenantDS:${TENANT_ID}] REQ /ds/converse text="${(text || '').substring(0, 60)}"`);

      if (!text) return send(res, 400, { error: 'text required' });

      const { CONVERSE_SYSTEM_PROMPT, buildConverseTurnPrompt } = await import('./tenant_prompts.mjs');

      const userPrompt = buildConverseTurnPrompt({
        userName: user_name || 'User',
        text,
        dsContext: ds_context || '',
        conversationHistory: conversation_history || '',
      });

      const llmKey = body.user_llm_key || LLM_KEY;
      const llmResult = await callGemini(CONVERSE_SYSTEM_PROMPT, userPrompt, {
        apiKey: llmKey,
        maxOutputTokens: 512,   // Short responses for TTS
        temperature: 0.7,       // Conversational warmth
        responseMimeType: 'text/plain',  // Natural language, not JSON
      });

      // Clean any markdown or JSON wrapping from the response
      let response = llmResult.trim();
      // Strip JSON wrapper if present (Gemini may respond in JSON due to config)
      try {
        const parsed = JSON.parse(response);
        response = parsed.response || parsed.text || parsed.message || response;
      } catch {
        // Not JSON — use as-is (this is the normal case for conversational responses)
      }

      console.log(`[TenantDS:${TENANT_ID}] Converse response: "${response.substring(0, 80)}"`);
      send(res, 200, { response, status: 'ok' });
    } catch (e) {
      console.error(`[TenantDS:${TENANT_ID}] /ds/converse error:`, e.message);
      send(res, 500, { error: e.message });
    }
    return;
  }

  // POST /ds/converse-distill — Distill conversation insights (Phase 8j)
  if (req.method === 'POST' && req.url === '/ds/converse-distill') {
    try {
      const body = await parseBody(req);
      const { transcript, user_id, tenant_id, session_duration } = body;

      console.log(`[TenantDS:${TENANT_ID}] REQ /ds/converse-distill duration=${session_duration}`);

      if (!transcript) return send(res, 400, { error: 'transcript required' });

      const { CONVERSE_DISTILLATION_SYSTEM, buildConversePrompt } = await import('./tenant_prompts.mjs');

      const userPrompt = buildConversePrompt({
        userName: 'User',
        todayDate: new Date().toISOString().split('T')[0],
        sessionDuration: session_duration || 'unknown',
        transcript,
      });

      // Use user's LLM key if provided, else fallback (SRC-13)
      const llmKey = body.user_llm_key || LLM_KEY;
      const llmResult = await callGemini(CONVERSE_DISTILLATION_SYSTEM, userPrompt, { apiKey: llmKey });

      let insights = [];
      try {
        const parsed = JSON.parse(llmResult.replace(/```json\n?/g, '').replace(/```\n?/g, ''));
        insights = parsed.insights || [];
      } catch {
        const match = llmResult.match(/\{[\s\S]*\}/);
        if (match) {
          try {
            insights = JSON.parse(match[0]).insights || [];
          } catch {}
        }
      }

      console.log(`[TenantDS:${TENANT_ID}] Converse distilled: ${insights.length} insights`);
      send(res, 200, { status: 'distilled', insights, count: insights.length });
    } catch (e) {
      console.error(`[TenantDS:${TENANT_ID}] /ds/converse-distill error:`, e.message);
      send(res, 500, { error: e.message });
    }
    return;
  }


  // ── Management API (/management/*) ──────────────────────────────────────────
  // Auth: X-Internal-Key must match MYNDLENS_DS_TOKEN.
  // Used by ObeGee backend to manage WA pairing and config for K8s tenants.

  if (req.url.startsWith('/management/')) {
    const authKey = req.headers['x-internal-key'];
    if (authKey !== MYNDLENS_DS_TOKEN) {
      return send(res, 403, { error: 'forbidden' });
    }

    // GET /management/wa/status — check if WA is linked
    if (req.method === 'GET' && req.url === '/management/wa/status') {
      const { exec } = await import('child_process');
      const { promisify } = await import('util');
      const execAsync = promisify(exec);
      const credsPath = '/home/node/.openclaw/credentials/whatsapp/default/creds.json';
      const { existsSync } = await import('fs');
      const hasCredentials = existsSync(credsPath);

      let ocStatus = 'unknown';
      let linkedPhone = null;
      let dmPolicy = null;

      // Retry CLI call up to 2 times (OC may be starting up)
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const { stdout } = await execAsync(
            'node /usr/local/lib/node_modules/openclaw/openclaw.mjs channels status',
            { timeout: 20000 }
          );
          const lower = stdout.toLowerCase();
          ocStatus = (lower.includes('linked') && lower.includes('connected'))
            ? 'connected'
            : lower.includes('linked') ? 'linked_not_connected'
            : 'disconnected';
          const phoneMatch = stdout.match(/allow:\+?(\d+)/);
          if (phoneMatch) linkedPhone = '+' + phoneMatch[1];
          const dmMatch = stdout.match(/dm:(\w+)/);
          if (dmMatch) dmPolicy = dmMatch[1];
          break; // success
        } catch (e) {
          if (attempt === 0) {
            console.log(`[TenantDS:${TENANT_ID}] WA status CLI attempt 1 failed (OC may be starting), retrying in 5s...`);
            await new Promise(r => setTimeout(r, 5000));
          } else {
            ocStatus = 'starting';
            console.log(`[TenantDS:${TENANT_ID}] WA status CLI failed after 2 attempts — OC still starting`);
          }
        }
      }

      // Sync status to ObeGee MongoDB (fix stale dashboard entries)
      if (ocStatus === 'connected' || ocStatus === 'disconnected') {
        try {
          const syncPayload = { tenant_id: TENANT_ID, wa_status: ocStatus, linked_phone: linkedPhone, dm_policy: dmPolicy };
          fetch(`${MYNDLENS_API}/api/digital-self/wa-status-sync`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Internal-Key': MYNDLENS_DS_TOKEN },
            body: JSON.stringify(syncPayload),
          }).catch(() => {}); // fire-and-forget
        } catch (_) {}
      }

      return send(res, 200, {
        has_credentials: hasCredentials,
        oc_status: ocStatus,
        dm_policy: dmPolicy,
        linked_phone: linkedPhone,
        creds_path: credsPath,
      });
    }

    if (req.method === 'GET' && req.url === '/management/wa/bootstrap/status') {
      const state = readBootstrapState();
      if (!state) return send(res, 404, { status: 'not_started' });
      return send(res, 200, state);
    }

    if (req.method === 'GET' && req.url === '/management/wa/bootstrap/result') {
      const state = readBootstrapState();
      if (!state || state.status !== 'done') {
        return send(res, 404, { status: 'not_ready' });
      }
      try {
        const contacts = JSON.parse(readFileSync(WA_BOOTSTRAP_DISTILL_FILE, 'utf8'));
        return send(res, 200, {
          status: 'done',
          contact_count: Array.isArray(contacts) ? contacts.length : 0,
          contacts: Array.isArray(contacts) ? contacts : [],
          extracted_at: state.completed_at || state.updated_at,
          source: 'whatsapp_bootstrap',
        });
      } catch (e) {
        return send(res, 500, { error: e.message });
      }
    }

    if (req.method === 'POST' && req.url === '/management/wa/bootstrap/start') {
      try {
        const body = await parseBody(req);
        const method = (body.method || (body.phone_number ? 'phone' : 'qr')).toLowerCase();
        const phoneNumber = (body.phone_number || '').trim();
        const userId = (body.user_id || '').trim();

        if (!['phone', 'qr'].includes(method)) {
          return send(res, 400, { error: 'method must be phone or qr' });
        }
        if (method === 'phone' && !phoneNumber) {
          return send(res, 400, { error: 'phone_number required for phone mode' });
        }

        const state = spawnBootstrapJob({ method, phoneNumber, userId });
        return send(res, 200, {
          status: state.status,
          job_id: state.job_id,
          method,
          message: 'WhatsApp bootstrap started inside tenant',
        });
      } catch (e) {
        const statusCode = String(e.message || '').includes('bootstrap_already_running') ? 409 : 500;
        return send(res, statusCode, { error: e.message });
      }
    }

    // POST /management/wa/pair — trigger OC WhatsApp pairing (phone number method)
    // Body: { phone_number: "+919898089931" }
    // Returns: SSE stream of events: { type: "pairing_code"|"connected"|"error", ... }
    if (req.method === 'POST' && req.url === '/management/wa/pair') {
      const body = await parseBody(req);
      const { phone_number } = body;
      if (!phone_number) return send(res, 400, { error: 'phone_number required' });

      // Set up SSE headers for streaming
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      });

      const sendSSE = (data) => {
        res.write(`data: ${JSON.stringify(data)}\n\n`);
      };

      const { spawn } = await import('child_process');

      // Use OC's built-in channels login — this uses OC's own Baileys session (no conflict)
      const phone = phone_number.replace(/[^\d]/g, '');
      sendSSE({ type: 'starting', message: `Starting WA pairing for +${phone}` });

      const proc = spawn('node', [
        '/usr/local/lib/node_modules/openclaw/openclaw.mjs',
        'channels', 'login', '--channel', 'whatsapp', '--verbose',
      ], { env: { ...process.env, HOME: '/home/node' } });

      let buffer = '';
      let pairingCodeSent = false;

      proc.stdout.on('data', (chunk) => {
        buffer += chunk.toString();
        // Detect pairing code in OC output (format: XXXX-XXXX)
        const codeMatch = buffer.match(/\b([A-Z0-9]{4}-[A-Z0-9]{4})\b/);
        if (codeMatch && !pairingCodeSent) {
          pairingCodeSent = true;
          sendSSE({ type: 'pairing_code', pairing_code: codeMatch[1] });
        }
        // Detect connected
        if (/linked|connected|already linked/i.test(buffer)) {
          sendSSE({ type: 'connected', message: 'WhatsApp linked successfully' });
        }
      });

      proc.stderr.on('data', (chunk) => {
        const txt = chunk.toString();
        if (/error|fail/i.test(txt)) {
          sendSSE({ type: 'log', message: txt.trim().substring(0, 200) });
        }
      });

      proc.on('close', (code) => {
        if (code !== 0 && !pairingCodeSent) {
          sendSSE({ type: 'error', message: `Process exited with code ${code}` });
        }
        res.end();
      });

      // Timeout after 5 minutes
      setTimeout(() => {
        if (!res.writableEnded) {
          sendSSE({ type: 'timeout', message: 'Pairing timed out' });
          proc.kill();
          res.end();
        }
      }, 300000);

      return;
    }

    // POST /management/config — push openclaw.json update from ObeGee
    // Body: any top-level openclaw.json keys (channels, models, agents, gateway, etc.)
    // Deep-merges each top-level key, preserving existing values unless overridden.
    if (req.method === 'POST' && req.url === '/management/config') {
      const body = await parseBody(req);
      const { readFileSync, writeFileSync } = await import('fs');
      const configPath = '/home/node/.openclaw/openclaw.json';
      try {
        const cfg = JSON.parse(readFileSync(configPath, 'utf8'));

        // Deep merge helper for one level deep
        function mergeKey(target, src) {
          if (src && typeof src === 'object' && !Array.isArray(src)) {
            return { ...target, ...src };
          }
          return src;
        }

        // Merge all provided top-level keys
        for (const key of Object.keys(body)) {
          cfg[key] = mergeKey(cfg[key] || {}, body[key]);
        }

        // Special cleanup for channels.whatsapp — remove invalid OC keys
        if (body.channels?.whatsapp) {
          delete cfg.channels.whatsapp.enabled;
          delete cfg.channels.whatsapp.pairedAt;
        }

        // OC hot-reloads openclaw.json automatically
        writeFileSync(configPath, JSON.stringify(cfg, null, 2));
        console.log(`[TenantDS:${TENANT_ID}] Config updated via /management/config keys=${Object.keys(body).join(',')}`);
        return send(res, 200, { ok: true, updated: Object.keys(body) });
      } catch (e) {
        return send(res, 500, { error: e.message });
      }
    }

    // POST /management/llm-config — configure Kimi K2.5 LLM provider
    // Body: { moonshot_api_key: "sk-..." }
    // Writes BOTH:
    //   1. agents/main/agent/models.json — per-agent (takes precedence), ACTUAL key value required
    //   2. openclaw.json agents.defaults.model.primary → "moonshot/kimi-k2.5"
    // Root cause: if models.json has literal "MOONSHOT_API_KEY" string (not real key), OC falls back to anthropic
    if (req.method === 'POST' && req.url === '/management/llm-config') {
      const body = await parseBody(req);
      const { readFileSync, writeFileSync, mkdirSync, rmSync } = await import('fs');
      const { moonshot_api_key } = body;
      if (!moonshot_api_key) return send(res, 400, { error: 'moonshot_api_key required' });

      const agentDir = '/home/node/.openclaw/agents/main/agent';
      const modelsPath = `${agentDir}/models.json`;
      const configPath = '/home/node/.openclaw/openclaw.json';

      // Remove stale auth-profiles.json
      try { rmSync(`${agentDir}/auth-profiles.json`); } catch (_) {}

      try {
        mkdirSync(agentDir, { recursive: true });

        // 1. Write agent-level models.json with ACTUAL key value (not literal env var name)
        const modelsJson = {
          providers: {
            moonshot: {
              apiKey: moonshot_api_key,
              baseUrl: 'https://api.moonshot.cn/v1',
              models: [
                { id: 'kimi-k2.5',      name: 'kimi-k2.5'      },
                { id: 'kimi-k1.5',      name: 'kimi-k1.5'      },
                { id: 'moonshot-v1-8k', name: 'moonshot-v1-8k' },
              ],
            },
          },
        };
        writeFileSync(modelsPath, JSON.stringify(modelsJson, null, 2));

        // 2. Set openclaw.json agent default to kimi-k2.5 (must match models.json ID)
        const cfg = JSON.parse(readFileSync(configPath, 'utf8'));
        cfg.agents = cfg.agents || {};
        cfg.agents.defaults = cfg.agents.defaults || {};
        cfg.agents.defaults.model = { primary: 'moonshot/kimi-k2.5' };
        writeFileSync(configPath, JSON.stringify(cfg, null, 2));

        console.log(`[TenantDS:${TENANT_ID}] Kimi K2.5 configured via /management/llm-config`);
        return send(res, 200, { ok: true, provider: 'moonshot', model: 'kimi-k2.5' });
      } catch (e) {
        return send(res, 500, { error: e.message });
      }
    }

    // POST /management/wa/backup-session — backup WA credentials to ObeGee MongoDB
    // Reads creds from PVC, base64-encodes, POSTs to ObeGee for durable storage.
    // Replaces the broken save_session.py (INC-011).
    if (req.method === 'POST' && req.url === '/management/wa/backup-session') {
      try {
        const { readFileSync, readdirSync, existsSync } = await import('fs');
        const { join } = await import('path');

        const credsDir = join('/home/node/.openclaw', 'credentials', 'whatsapp', 'default');
        if (!existsSync(credsDir)) {
          return send(res, 404, { error: 'No WA credentials directory found', path: credsDir });
        }

        // Read all credential files
        const files = readdirSync(credsDir);
        if (files.length === 0) {
          return send(res, 404, { error: 'No WA credential files found' });
        }

        const credBundle = {};
        for (const file of files) {
          const filePath = join(credsDir, file);
          const data = readFileSync(filePath);
          credBundle[file] = data.toString('base64');
        }

        // POST to ObeGee internal API for MongoDB storage
        const apiUrl = process.env.OBEGEE_API_URL;
        if (!apiUrl) throw new Error('OBEGEE_API_URL not set');
        const dsToken = process.env.MYNDLENS_DS_TOKEN || '';

        const payload = JSON.stringify({
          tenant_id: TENANT_ID,
          creds_bundle: credBundle,
          file_count: files.length,
          backed_up_at: new Date().toISOString(),
          creds_path: credsDir,
        });

        // Use native http(s) to avoid dependency
        const https = await import('https');
        const http = await import('http');
        const url = new URL(`${apiUrl}/api/internal/wa-session-backup`);
        const client = url.protocol === 'https:' ? https : http;

        await new Promise((resolve, reject) => {
          const req2 = client.request(url, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'X-Internal-API-Key': process.env.OBEGEE_INTERNAL_KEY || dsToken,
              'Content-Length': Buffer.byteLength(payload),
            },
            timeout: 15000,
          }, (resp) => {
            let body = '';
            resp.on('data', c => body += c);
            resp.on('end', () => {
              if (resp.statusCode < 300) {
                resolve(body);
              } else {
                reject(new Error(`ObeGee returned ${resp.statusCode}: ${body}`));
              }
            });
          });
          req2.on('error', reject);
          req2.on('timeout', () => { req2.destroy(); reject(new Error('timeout')); });
          req2.write(payload);
          req2.end();
        });

        console.log(`[TenantDS:${TENANT_ID}] WA session backed up: ${files.length} files`);
        return send(res, 200, {
          ok: true,
          files_backed_up: files.length,
          tenant_id: TENANT_ID,
          backed_up_at: new Date().toISOString(),
        });
      } catch (e) {
        console.error(`[TenantDS:${TENANT_ID}] WA backup failed: ${e.message}`);
        return send(res, 500, { error: `Backup failed: ${e.message}` });
      }
    }


    // POST /management/wa/extract — trigger WhatsApp DS extraction inside the pod
    // K8s equivalent of the VPS pairing service docker exec path.
    // Pauses OC (dmPolicy=disabled), runs wa_ds_extractor.mjs, restores OC after.
    // Non-blocking: returns job_id immediately, extraction runs in background IIFE.
    if (req.method === 'POST' && req.url === '/management/wa/extract') {
      const body = await parseBody(req);
      const userId = body.user_id || '';  // ObeGee user_id — required for DS stream routing
      if (!userId) {
        console.warn(`[TenantDS:${TENANT_ID}] /management/wa/extract: user_id not provided — DS streams will use empty user_id`);
      }
      const jobId = `k8s_${Date.now().toString(36)}`;
      console.log(`[TenantDS:${TENANT_ID}] WA extraction triggered — job ${jobId} user=${userId || 'MISSING'}`);

      (async () => {
        const { exec } = await import('child_process');
        const { promisify } = await import('util');
        const { readFileSync, writeFileSync } = await import('fs');
        const execAsync = promisify(exec);
        const configPath = '/home/node/.openclaw/openclaw.json';

        // Step 1: Set dmPolicy=disabled — OC hot-reloads and disconnects Baileys.
        //
        // Pre-March 3 VPS pairing service used EXACTLY this approach (confirmed).
        // dmPolicy=disabled tells OC "don't route any messages" → OC disconnects Baileys.
        // After the 6s hot-reload wait, the extractor connects to an idle WA session.
        //
        // IMPORTANT: Do NOT remove channels.whatsapp entirely.
        // If removed, OC falls back to dmPolicy:'pairing' (the OC default) and sends
        // pairing codes to EVERY incoming WhatsApp message during extraction.
        // (This was a "CRITICAL BUG FIXED" in the VPS pairing service code.)
        let savedWaConfig = null;
        try {
          const cfg = JSON.parse(readFileSync(configPath, 'utf8'));
          savedWaConfig = cfg.channels?.whatsapp
            ? JSON.parse(JSON.stringify(cfg.channels.whatsapp))
            : null;
          // Mirror VPS pauseOCWhatsApp() exactly: set disabled, keep the block
          cfg.channels = cfg.channels || {};
          cfg.channels.whatsapp = Object.assign({}, cfg.channels.whatsapp || {}, {
            dmPolicy: 'disabled',
            groupPolicy: 'disabled',
          });
          writeFileSync(configPath, JSON.stringify(cfg, null, 2));
          console.log(`[TenantDS:${TENANT_ID}] OC WA set to dmPolicy=disabled — Baileys will disconnect on hot-reload`);
        } catch (e) { console.warn(`[TenantDS:${TENANT_ID}] pauseOC: ${e.message}`); }

        // Wait for OC hot-reload to process config change and disconnect Baileys socket.
        // OC 2026.3.7 after 440: non-retryable (stops reconnecting) ← KEY behaviour.
        // Extractor connects after OC releases → should be exclusive.
        // 15s gives OC enough time to hot-reload + fully release the Baileys socket.
        await new Promise(r => setTimeout(r, 15000));

        // Step 2: Run wa_ds_extractor.mjs — self-contained, POSTs via localhost:10014
        // Retry extractor if it did not reach a connected state (typically 440/session conflict race).
        let extractionOk = false;
        let stdout = '';
        let lastError = '';

        const isSessionRace = (logText) => {
          const t = (logText || '').toLowerCase();
          return (
            t.includes('440') ||
            t.includes('stream errored out') ||
            t.includes('conflict') ||
            t.includes('connection closed') ||
            t.includes('connection lost')
          );
        };

        const extractorCmd = `node /app/wa_ds_extractor.mjs ${TENANT_ID} ${MYNDLENS_API}`;
        const maxAttempts = 3;

        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
          let attemptStdout = '';
          let attemptStderr = '';
          try {
            const result = await execAsync(extractorCmd, {
              timeout: 30 * 60 * 1000,
              env: { ...process.env, HOME: '/home/node', USER_ID: userId },
            });

            attemptStdout = result.stdout || '';
            attemptStderr = result.stderr || '';
            const merged = `${attemptStdout}\n${attemptStderr}`;
            const connected = attemptStdout.includes('"phase":"connected"');
            const done = attemptStdout.includes('"phase":"done"');

            if (connected && done) {
              extractionOk = true;
              stdout = attemptStdout;
              if (attemptStderr) {
                console.warn(`[TenantDS:${TENANT_ID}] Extractor stderr attempt=${attempt}: ${attemptStderr.substring(0, 200)}`);
              }
              console.log(`[TenantDS:${TENANT_ID}] Extraction complete job=${jobId} attempt=${attempt}`);
              break;
            }

            const retryable = !connected || isSessionRace(merged);
            lastError = `Extractor did not reach connected+done state (attempt=${attempt})`;
            console.warn(`[TenantDS:${TENANT_ID}] ${lastError}`);

            if (!retryable || attempt === maxAttempts) {
              stdout = attemptStdout;
              break;
            }

            await new Promise(r => setTimeout(r, 12000));
          } catch (e) {
            attemptStdout = e.stdout || '';
            attemptStderr = e.stderr || '';
            const merged = `${e.message || ''}\n${attemptStdout}\n${attemptStderr}`;
            const retryable = isSessionRace(merged);
            lastError = (e.message || 'Extractor failed').substring(0, 220);

            console.error(`[TenantDS:${TENANT_ID}] Extraction failed job=${jobId} attempt=${attempt}: ${lastError}`);

            if (!retryable || attempt === maxAttempts) {
              stdout = attemptStdout;
              break;
            }

            await new Promise(r => setTimeout(r, 12000));
          }
        }

        // Step 3: Restore OC WA config — mirror VPS resumeOCWhatsApp()
        // Restores savedWaConfig + sets allowlist so OC reconnects and routes correctly.
        try {
          const cfg3 = JSON.parse(readFileSync(configPath, 'utf8'));
          let phone = '';
          try {
            const c = JSON.parse(readFileSync('/home/node/.openclaw/credentials/whatsapp/default/creds.json', 'utf8'));
            phone = '+' + ((c.me?.id || '').split('@')[0] || '').split(':')[0];
          } catch (_) {}
          cfg3.channels = cfg3.channels || {};
          cfg3.channels.whatsapp = Object.assign({}, savedWaConfig || {}, {
            dmPolicy: 'allowlist',
            allowFrom: phone ? [phone] : (savedWaConfig?.allowFrom || []),
            groupPolicy: 'disabled',
          });
          // Remove invalid keys
          delete cfg3.channels.whatsapp.enabled;
          delete cfg3.channels.whatsapp.pairedAt;
          writeFileSync(configPath, JSON.stringify(cfg3, null, 2));
          console.log(`[TenantDS:${TENANT_ID}] OC WA restored (dmPolicy=allowlist phone=${phone})`);
        } catch (e) { console.warn(`[TenantDS:${TENANT_ID}] resumeOC: ${e.message}`); }

        // Step 4: Notify ObeGee — mark done/failed in MongoDB
        if (extractionOk) {
          try {
            let contactCount = 0;
            for (const line of stdout.split('\n')) {
              try { const d = JSON.parse(line); if (d.phase === 'done' && typeof d.contact_count === 'number') { contactCount = d.contact_count; break; } } catch (_) {}
            }
            const resp = await fetch(`https://obegee.co.uk/api/internal/tenant-ds-sync-complete/${TENANT_ID}`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'X-Internal-API-Key': 'obegee_internal_key_prod' },
              body: JSON.stringify({ contact_count: contactCount }),
            });
            console.log(`[TenantDS:${TENANT_ID}] ObeGee DS sync updated: done contacts=${contactCount} HTTP=${resp.status}`);
          } catch (e) { console.warn(`[TenantDS:${TENANT_ID}] ObeGee notify: ${e.message}`); }
        } else {
          try {
            const resp = await fetch(`https://obegee.co.uk/api/internal/tenant-ds-sync-failed/${TENANT_ID}`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'X-Internal-API-Key': 'obegee_internal_key_prod' },
              body: JSON.stringify({ error: lastError || 'Extractor failed before completion' }),
            });
            console.warn(`[TenantDS:${TENANT_ID}] ObeGee DS sync marked failed HTTP=${resp.status}`);
          } catch (e) {
            console.warn(`[TenantDS:${TENANT_ID}] ObeGee failure notify: ${e.message}`);
          }
        }
      })();

      return send(res, 200, { job_id: jobId, status: 'started', message: 'Extraction running inside pod' });
    }

    // POST /management/source-extract — Tenant-side data sourcing
    // ObeGee passes OAuth access_token; tenant fetches data directly from APIs.
    // No raw text crosses the network. Tenant sources + distills + streams.
    if (req.method === 'POST' && req.url === '/management/source-extract') {
      const body = await parseBody(req);
      const { source, access_token, user_id, user_name, params } = body;

      if (!source || !access_token) {
        return send(res, 400, { error: 'source and access_token required' });
      }

      console.log(`[TenantDS:${TENANT_ID}] SOURCE_EXTRACT start source=${source} user=${user_id || '?'}`);

      try {
        let rawData = '';

        if (source === 'gmail') {
          rawData = await _fetchGmail(access_token, params || {});
        } else if (source === 'gdrive') {
          rawData = await _fetchGDrive(access_token, params || {});
        } else if (source === 'linkedin') {
          rawData = await _fetchLinkedIn(access_token, params || {});
        } else if (source === 'imap' || source === 'email') {
          rawData = await _fetchIMAP(access_token, params || {});
        } else {
          return send(res, 400, { error: `Unknown source: ${source}` });
        }

        if (!rawData || rawData.length < 50) {
          console.log(`[TenantDS:${TENANT_ID}] SOURCE_EXTRACT no data for source=${source}`);
          return send(res, 200, { status: 'no_data', source, chars: rawData.length });
        }

        console.log(`[TenantDS:${TENANT_ID}] SOURCE_EXTRACT fetched source=${source} chars=${rawData.length}`);

        // Distill + vectorize + stream (existing pipeline)
        const result = await processFull(rawData, source, user_id || '', user_name || 'User');
        await forwardToMyndLens(result, user_id || '', TENANT_ID);

        const streamA = result.pkg_diff?.nodes?.length || 0;
        const streamB = result.backend_qs_records?.length || 0;
        console.log(`[TenantDS:${TENANT_ID}] SOURCE_EXTRACT done source=${source} entities=${result.stats?.entities_distilled || 0} a=${streamA} b=${streamB}`);
        return send(res, 200, { status: 'processed', source, stats: result.stats, stream_a: streamA, stream_b: streamB });
      } catch (e) {
        console.error(`[TenantDS:${TENANT_ID}] SOURCE_EXTRACT error source=${source}: ${e.message}`);
        return send(res, 500, { error: e.message });
      }
    }

    return send(res, 404, { error: 'Unknown management endpoint' });
  }

  send(res, 404, { error: 'Not found' });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[TenantDS] Service started on port ${PORT} for tenant ${TENANT_ID}`);
  console.log(`[TenantDS] MyndLens API: ${MYNDLENS_API}`);
  console.log(`[TenantDS] LLM key: ${LLM_KEY ? 'present' : 'MISSING — DS distillation will fail'}`);
  console.log(`[TenantDS] DS token: ${MYNDLENS_DS_TOKEN ? 'present' : 'MISSING — /management/* will return 403'}`);
  console.log(`[TenantDS] Encryption secret: ${PKG_ENCRYPTION_SECRET ? 'present' : 'MISSING — Stream A encryption will use empty key'}`);
});
