/**
 * wa_ds_extractor.mjs v2 — WhatsApp Digital Self Intelligence Extractor
 *
 * Connects via Baileys, syncs 90 days of message history, captures ALL message
 * types (text, voice notes, documents, locations, contacts, reactions), and
 * distills per-contact intelligence via Gemini Flash.
 *
 * Data Model per contact:
 *   identity: name, phone, relationship, nicknames, confidential flag
 *   active_threads: topic, type, status, decided/undecided, actions, tension
 *   pending_actions: user_owes[], they_owe[]
 *   pattern: frequency, style, peak_hours, voice/doc/location ratios
 *   emotional_context: mood, recent_events
 *   aspirations_mentioned: []
 *
 * Thread types: PLANNING, TASK, CONFLICT, CONCERN, CELEBRATION, SUPPORT,
 *   INFORMATION, MEETING, TRAVEL, DELEGATION, REQUEST, ASPIRATION
 *
 * Usage: node /app/wa_ds_extractor.mjs <tenant_id> <myndlens_api_url> <llm_key>
 */
import makeWASocket_ from '@whiskeysockets/baileys';
import {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  Browsers,
  makeCacheableSignalKeyStore,
} from '@whiskeysockets/baileys';
import { existsSync, readFileSync, writeFileSync, rmSync } from 'fs';

const makeWASocket = makeWASocket_.default ?? makeWASocket_;

const TENANT_ID = process.argv[2];
const MYNDLENS_API = process.argv[3] || 'https://app.myndlens.com';
const FORCE_FULL = process.argv.includes('--force-full') || process.env.FORCE_FULL_EXTRACTION === '1';
// LLM_KEY is set in the container environment at provision time (KIMI_API_KEY for Starter,
// user BYOK key for Pro). Never passed via CLI — container env is the source of truth.
const LLM_KEY = process.env.LLM_KEY || '';

if (!TENANT_ID) { emit({ error: 'tenant_id required' }); process.exit(1); }

function emit(obj) { process.stdout.write(JSON.stringify(obj) + '\n'); }

// H4: Session lock — prevent concurrent Baileys connections
const LOCK_FILE = `/tmp/wa_ds_lock_${TENANT_ID}`;

function acquireLock() {
  if (existsSync(LOCK_FILE)) {
    try {
      const lockData = JSON.parse(readFileSync(LOCK_FILE, 'utf8'));
      const lockAge = Date.now() - (lockData.ts || 0);
      if (lockAge < 300000) { // 5 min max lock
        emit({ error: `WA extraction already running (pid=${lockData.pid}, age=${Math.round(lockAge/1000)}s). Skipping.` });
        process.exit(0);
      }
      // Stale lock — remove
      emit({ phase: 'stale_lock_removed', age_seconds: Math.round(lockAge/1000) });
    } catch { /* corrupt lock file */ }
  }
  writeFileSync(LOCK_FILE, JSON.stringify({ pid: process.pid, ts: Date.now() }));
}

function releaseLock() {
  try { rmSync(LOCK_FILE, { force: true }); } catch {}
}

// Acquire lock before anything
acquireLock();
process.on('exit', releaseLock);
process.on('SIGTERM', () => { releaseLock(); process.exit(0); });
process.on('SIGINT', () => { releaseLock(); process.exit(0); });

const NINETY_DAYS_MS = 90 * 24 * 60 * 60 * 1000;
const NINETY_DAYS_AGO = Date.now() - NINETY_DAYS_MS;
const AUTH_DIR = process.env.WA_AUTH_DIR || '/home/node/.openclaw/credentials/whatsapp/default';
const PROCESS_STORE_ONLY = process.env.WA_DS_PROCESS_STORE_ONLY === '1';

// Phase 3e: Use /data for persistence if it exists, fallback to /tmp
const DATA_DIR = process.env.WA_DS_DATA_DIR
  || (existsSync('/home/node/.openclaw/workspace') ? '/home/node/.openclaw/workspace' : '')
  || (existsSync('/home/node/.openclaw') ? '/home/node/.openclaw' : '')
  || (existsSync('/data') ? '/data' : '/tmp');
const STORE_FILE = process.env.WA_DS_STORE_FILE || `${DATA_DIR}/wa_ds_messages_${TENANT_ID}.json`;
const DISTILL_FILE = process.env.WA_DS_DISTILL_FILE || `${DATA_DIR}/wa_ds_distilled_${TENANT_ID}.json`;
const MIN_CONTACT_MESSAGES = Number(process.env.WA_DS_MIN_CONTACT_MESSAGES || (FORCE_FULL ? 1 : 3));
const INLINE_RESULTS = process.env.WA_DS_INLINE_RESULTS === '1';
const HISTORY_CUTOFF_TS = FORCE_FULL ? 0 : NINETY_DAYS_AGO;
const SEED_CHAT_JIDS = (() => {
  try {
    const raw = process.env.WA_SEED_CHAT_JIDS || '[]';
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
})();

function trimText(value, maxLen = 180) {
  if (typeof value !== 'string') return '';
  return value.trim().slice(0, maxLen);
}

function compactStringList(values, maxItems = 6, maxLen = 140) {
  if (!Array.isArray(values)) return [];
  return values
    .filter(Boolean)
    .map(v => trimText(String(v), maxLen))
    .filter(Boolean)
    .slice(0, maxItems);
}

function compactActions(values, maxItems = 6) {
  if (!Array.isArray(values)) return [];
  return values
    .map(v => {
      if (typeof v === 'string') return trimText(v, 180);
      if (!v || typeof v !== 'object') return '';
      return trimText(v.action || v.text || JSON.stringify(v), 180);
    })
    .filter(Boolean)
    .slice(0, maxItems);
}

function compactIntelProfile(profile) {
  const p = profile && typeof profile === 'object' ? profile : {};
  const identity = p.identity || {};
  const mi = p.merged_intelligence || {};
  const pending = mi.pending_actions || p.pending_actions || {};
  const emotional = mi.emotional_context || p.emotional_context || {};
  const threads = mi.active_threads || p.active_threads || [];

  const compactThreads = Array.isArray(threads)
    ? threads.slice(0, 4).map(t => ({
        topic: trimText((t || {}).topic || '', 120),
        type: trimText((t || {}).type || 'Discussion', 24),
        status: trimText((t || {}).status || 'active', 24),
        tension_level: trimText((t || {}).tension_level || 'none', 12),
        decided: compactStringList((t || {}).decided, 4, 120),
        undecided: compactStringList((t || {}).undecided, 4, 120),
        user_action: trimText((t || {}).user_action || '', 180),
        their_action: trimText((t || {}).their_action || '', 180),
        deadline: (t || {}).deadline || null,
      })).filter(t => t.topic)
    : [];

  const compactPending = {
    user_owes: compactActions(pending.user_owes, 6),
    they_owe: compactActions(pending.they_owe, 6),
  };

  return {
    canonical_id: p.canonical_id || '',
    node_type: p.node_type || 'Person',
    identity: {
      display_name: trimText(identity.display_name || identity.name || '', 80),
      full_name: trimText(identity.full_name || identity.display_name || identity.name || '', 120),
      nicknames: compactStringList(identity.nicknames, 6, 40),
      relationship: trimText(identity.relationship || 'unknown', 64),
      confidential: !!identity.confidential,
    },
    contact_methods: p.contact_methods || { phones: [], emails: [], telegram: null, linkedin_url: null },
    merged_intelligence: {
      active_threads: compactThreads,
      pending_actions: compactPending,
      emotional_context: {
        current_mood: trimText(emotional.current_mood || 'unknown', 80),
        recent_events: compactStringList(emotional.recent_events, 5, 120),
      },
      pattern: mi.pattern || p.pattern || {},
      aspirations_mentioned: compactStringList(mi.aspirations_mentioned || p.aspirations_mentioned, 5, 120),
      inner_circle_rank: mi.inner_circle_rank || p.inner_circle_rank || 99,
      message_count_90d: mi.message_count_90d || p.message_count_90d || 0,
      last_interaction: mi.last_interaction || p.last_interaction || '',
      sentiment: mi.sentiment || p.sentiment || 'neutral',
      upcoming_events: Array.isArray(mi.upcoming_events || p.upcoming_events)
        ? (mi.upcoming_events || p.upcoming_events).slice(0, 5).map(e => ({
            event: trimText((e || {}).event || '', 120),
            date: (e || {}).date || null,
            location: trimText((e || {}).location || '', 60) || null,
            contacts: compactStringList((e || {}).contacts, 4, 60),
          })).filter(e => e.event)
        : [],
      financial_mentions: Array.isArray(mi.financial_mentions || p.financial_mentions)
        ? (mi.financial_mentions || p.financial_mentions).slice(0, 6).map(f => ({
            type: trimText((f || {}).type || '', 24),
            amount: (f || {}).amount || null,
            currency: trimText((f || {}).currency || '', 8) || null,
            due_date: (f || {}).due_date || null,
            counterparty: trimText((f || {}).counterparty || '', 80),
          })).filter(f => f.type || f.counterparty)
        : [],
      travel_mentions: Array.isArray(mi.travel_mentions || p.travel_mentions)
        ? (mi.travel_mentions || p.travel_mentions).slice(0, 4).map(t => ({
            type: trimText((t || {}).type || '', 24),
            reference: trimText((t || {}).reference || '', 40) || null,
            date: (t || {}).date || null,
            origin: trimText((t || {}).origin || '', 60) || null,
            destination: trimText((t || {}).destination || '', 60) || null,
          })).filter(t => t.type || t.origin || t.destination)
        : [],
      mentioned_people: Array.isArray(mi.mentioned_people || p.mentioned_people)
        ? (mi.mentioned_people || p.mentioned_people).slice(0, 8).map(mp => ({
            name: trimText((mp || {}).name || '', 60),
            context: trimText((mp || {}).context || '', 120),
            phone_if_known: trimText((mp || {}).phone_if_known || '', 32) || null,
          })).filter(mp => mp.name)
        : [],
    },
    merge_keys: p.merge_keys || {},
    interaction_signals: p.interaction_signals || {},
  };
}

function shouldIncludeContact(contact) {
  if (!contact) return false;
  if ((contact.count || 0) >= MIN_CONTACT_MESSAGES) return true;
  const signal =
    (contact.voice_notes || 0) +
    (contact.documents || 0) +
    (contact.types?.image || 0) +
    (contact.types?.video || 0) +
    (contact.locations || 0);
  return signal > 0;
}

function clearExtractionCaches() {
  const candidates = [
    `/data/wa_ds_messages_${TENANT_ID}.json`,
    `/data/wa_ds_distilled_${TENANT_ID}.json`,
    `/tmp/wa_ds_messages_${TENANT_ID}.json`,
    `/tmp/wa_ds_distilled_${TENANT_ID}.json`,
    `/data/wa_ds_messages_${TENANT_ID.replace(/_/g, '-')}.json`,
    `/tmp/wa_ds_messages_${TENANT_ID.replace(/_/g, '-')}.json`,
  ];

  for (const file of candidates) {
    try { rmSync(file, { force: true }); } catch (_) {}
  }
}

function normalizeToJid(value) {
  if (!value || typeof value !== 'string') return '';
  const trimmed = value.trim();
  if (!trimmed) return '';
  if (trimmed.includes('@')) return trimmed;
  const digits = trimmed.replace(/\D/g, '');
  if (!digits) return '';
  return `${digits}@s.whatsapp.net`;
}

// ── Message extraction helpers ────────────────────────────────────────────────

function extractTimestamp(msg) {
  const raw = msg.messageTimestamp;
  if (!raw) return 0;
  // Protobuf Long object has { low, high, unsigned }
  const val = typeof raw === 'object' ? (raw.low || 0) : Number(raw);
  // Baileys timestamps are in SECONDS. If > year 2100 in seconds, it's already ms.
  return val > 4102444800 ? val : val * 1000;
}

function extractPhoneFromJid(jid) {
  if (!jid) return '';
  const digits = jid.replace(/@.*/, '');
  return digits.startsWith('+') ? digits : '+' + digits;
}

function extractMessageData(msg) {
  const content = msg.message;
  if (!content) return null;

  const fromMe = msg.key?.fromMe || false;
  const ts = extractTimestamp(msg);

  // Text messages
  if (content.conversation) {
    return { type: 'text', text: content.conversation, fromMe, timestamp: ts };
  }
  if (content.extendedTextMessage?.text) {
    return { type: 'text', text: content.extendedTextMessage.text, fromMe, timestamp: ts };
  }

  // Voice notes — critical relationship signal
  if (content.audioMessage) {
    const duration = content.audioMessage.seconds || 0;
    const isPtt = content.audioMessage.ptt || false;
    return {
      type: isPtt ? 'voice_note' : 'audio',
      text: `[Voice note ${duration}s]`,
      duration,
      fromMe, timestamp: ts,
    };
  }

  // Documents — work relationship signal
  if (content.documentMessage) {
    const fileName = content.documentMessage.fileName || 'document';
    const caption = content.documentMessage.caption || '';
    return {
      type: 'document',
      text: caption || `[Shared file: ${fileName}]`,
      fileName,
      fromMe, timestamp: ts,
    };
  }

  // Images with captions
  if (content.imageMessage) {
    const caption = content.imageMessage.caption || '';
    return {
      type: 'image',
      text: caption || '[Photo]',
      fromMe, timestamp: ts,
    };
  }

  // Videos with captions
  if (content.videoMessage) {
    const caption = content.videoMessage.caption || '';
    return {
      type: 'video',
      text: caption || '[Video]',
      fromMe, timestamp: ts,
    };
  }

  // Shared contacts — social graph signal
  if (content.contactMessage) {
    const name = content.contactMessage.displayName || 'contact';
    return {
      type: 'shared_contact',
      text: `[Shared contact: ${name}]`,
      fromMe, timestamp: ts,
    };
  }
  if (content.contactsArrayMessage?.contacts) {
    const names = content.contactsArrayMessage.contacts.map(c => c.displayName || '?').join(', ');
    return {
      type: 'shared_contacts',
      text: `[Shared contacts: ${names}]`,
      fromMe, timestamp: ts,
    };
  }

  // Location — physical meetup signal
  if (content.locationMessage) {
    const name = content.locationMessage.name || '';
    return {
      type: 'location',
      text: name ? `[Location: ${name}]` : '[Shared location]',
      fromMe, timestamp: ts,
    };
  }
  if (content.liveLocationMessage) {
    return { type: 'live_location', text: '[Live location sharing]', fromMe, timestamp: ts };
  }

  // Reactions — engagement signal
  if (content.reactionMessage) {
    return {
      type: 'reaction',
      text: content.reactionMessage.text || '👍',
      fromMe, timestamp: ts,
    };
  }

  // Stickers — casual relationship signal
  if (content.stickerMessage) {
    return { type: 'sticker', text: '[Sticker]', fromMe, timestamp: ts };
  }

  // Poll
  if (content.pollCreationMessage) {
    return {
      type: 'poll',
      text: `[Poll: ${content.pollCreationMessage.name || 'question'}]`,
      fromMe, timestamp: ts,
    };
  }

  // Protocol messages (delete, edit) — skip
  if (content.protocolMessage) return null;

  // Catch-all for unknown types
  return { type: 'other', text: '[Message]', fromMe, timestamp: ts };
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function run() {
  emit({ phase: 'init', tenant_id: TENANT_ID });

  if (FORCE_FULL && !PROCESS_STORE_ONLY) {
    clearExtractionCaches();
    emit({ phase: 'force_full_cache_cleared', tenant_id: TENANT_ID });
  }

  // Accumulated data
  const contactMessages = new Map(); // jid → messages[]
  const contactNames = new Map();    // jid → display name
  const contactNotify = new Map();   // jid → WhatsApp pushName (profile name)
  const seedChatCandidates = new Set(SEED_CHAT_JIDS.map(normalizeToJid).filter(Boolean));
  let historyDone = false;
  let closeFinalizeTimer = null;

  // Load previously synced messages if available
  try {
    if (!FORCE_FULL && existsSync(STORE_FILE)) {
      const stored = JSON.parse(readFileSync(STORE_FILE, 'utf8'));
      for (const [jid, msgs] of Object.entries(stored.messages || {})) {
        const filtered = Array.isArray(msgs)
          ? msgs.filter(m => Number(m?.timestamp || 0) >= HISTORY_CUTOFF_TS)
          : [];
        if (filtered.length > 0) contactMessages.set(jid, filtered);
      }
      for (const [jid, name] of Object.entries(stored.names || {})) {
        contactNames.set(jid, name);
      }
      for (const [jid, notify] of Object.entries(stored.notify || {})) {
        if (notify) contactNotify.set(jid, notify);
      }
      if (contactMessages.size > 0) {
        emit({ phase: 'loaded_from_store', contacts: contactMessages.size,
               messages: Array.from(contactMessages.values()).reduce((s, m) => s + m.length, 0) });
      }

      // Keep store JIDs as fetch seeds (even if we force full and clear old message payloads).
      for (const jid of Object.keys(stored.messages || {})) {
        const normalized = normalizeToJid(jid);
        if (normalized) seedChatCandidates.add(normalized);
      }
    }
  } catch { /* no store yet */ }

  if (seedChatCandidates.size > 0) {
    emit({ phase: 'seed_candidates', count: seedChatCandidates.size });
  }

  if (PROCESS_STORE_ONLY) {
    emit({ phase: 'process_store_only', store_file: STORE_FILE, distill_file: DISTILL_FILE });
    await processAndExit();
    return;
  }

  if (!existsSync(AUTH_DIR + '/creds.json')) {
    emit({ error: 'No pairing session found at ' + AUTH_DIR });
    process.exit(1);
  }

  // Connect to WhatsApp
  let waVersion;
  try {
    const { version } = await fetchLatestBaileysVersion();
    waVersion = version;
  } catch {
    waVersion = [2, 3000, 1033846690];
  }
  emit({ phase: 'connecting', version: waVersion });

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const noop = () => {};
  const silentLogger = { level: 'silent', trace: noop, debug: noop, info: noop, warn: noop, error: noop, fatal: noop, child: () => silentLogger };

  const sock = makeWASocket({
    version: waVersion,
    logger: silentLogger,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, silentLogger),
    },
    browser: Browsers.ubuntu('Chrome'),
    printQRInTerminal: false,
    syncFullHistory: true,
    shouldSyncHistoryMessage: () => true,
    generateHighQualityLinkPreview: false,
  });

  sock.ev.on('creds.update', saveCreds);

  // ── Collect from ALL event sources ──────────────────────────────────────

  function storeMessage(jid, msgData) {
    if (!jid || jid === 'status@broadcast') return;
    if (jid.endsWith('@g.us')) return; // skip groups for now
    if (!msgData || msgData.timestamp < HISTORY_CUTOFF_TS) return;
    if (!contactMessages.has(jid)) contactMessages.set(jid, []);
    contactMessages.get(jid).push(msgData);
  }

  function storeName(id, name) {
    if (id && name) contactNames.set(id, name);
  }

  function storeNotify(id, notify) {
    if (id && notify) contactNotify.set(id, notify);
  }

  // History sync (first connection only)
  sock.ev.on('messaging-history.set', ({ contacts, messages, chats }) => {
    emit({ phase: 'history_batch', contacts: contacts?.length || 0, messages: messages?.length || 0, chats: chats?.length || 0 });

    if (contacts) {
      for (const c of contacts) {
        storeName(c.id, c.name || c.notify);
        if (c.notify) storeNotify(c.id, c.notify);
      }
    }
    if (messages) {
      for (const msg of messages) {
        const jid = msg.key?.remoteJid;
        const msgData = extractMessageData(msg);
        if (msgData) storeMessage(jid, msgData);
      }
    }
  });

  // Real-time messages
  sock.ev.on('messages.upsert', ({ messages: msgs }) => {
    for (const msg of msgs) {
      const jid = msg.key?.remoteJid;
      const msgData = extractMessageData(msg);
      if (msgData) storeMessage(jid, msgData);
    }
  });

  // Contact names from various events
  sock.ev.on('contacts.upsert', (contacts) => {
    for (const c of contacts) {
      storeName(c.id, c.name || c.notify);
      if (c.notify) storeNotify(c.id, c.notify);
    }
    emit({ phase: 'contacts_upsert', count: contactNames.size });
  });
  sock.ev.on('contacts.set', ({ contacts }) => {
    if (contacts) for (const c of contacts) {
      storeName(c.id, c.name || c.notify);
      if (c.notify) storeNotify(c.id, c.notify);
    }
  });

  // knownChats: capture chat JIDs + last-message cursor for fetchMessageHistory fallback
  const knownChats = new Map(); // jid → { key, ts } or null
  sock.ev.on('chats.upsert', (chats) => {
    for (const c of chats) {
      storeName(c.id, c.name);
      if (c.id && !c.id.endsWith('@g.us') && c.id !== 'status@broadcast') {
        // Grab the most-recent message key if available — used as cursor in fallback
        const lastMsg = c.messages?.[0]?.message ? c.messages[0] : null;
        knownChats.set(c.id, lastMsg ? { key: lastMsg.key, ts: Number(lastMsg.messageTimestamp || 0) * 1000 } : null);
      }
    }
    emit({ phase: 'chats_upsert', count: knownChats.size });
  });

  sock.ev.on('chats.set', ({ chats }) => {
    if (!Array.isArray(chats)) return;
    for (const c of chats) {
      storeName(c.id, c.name);
      if (c.id && !c.id.endsWith('@g.us') && c.id !== 'status@broadcast') {
        const lastMsg = c.messages?.[0]?.message ? c.messages[0] : null;
        knownChats.set(c.id, lastMsg ? { key: lastMsg.key, ts: Number(lastMsg.messageTimestamp || 0) * 1000 } : null);
      }
    }
    emit({ phase: 'chats_set', count: knownChats.size });
  });

  // ── Connection handling ─────────────────────────────────────────────────

  sock.ev.on('connection.update', async (update) => {
    const { connection } = update;
    emit({ phase: 'connection', connection });

    if (connection === 'open') {
      if (closeFinalizeTimer) {
        clearTimeout(closeFinalizeTimer);
        closeFinalizeTimer = null;
      }
      emit({ phase: 'connected' });

      // Phase 1: wait for auto-history-sync (messaging-history.set)
      // Fresh sessions: WhatsApp sends 90-day history automatically (30s window)
      // Established sessions: no auto-sync — need fetchMessageHistory fallback
      const initialWait = FORCE_FULL
        ? (contactMessages.size > 0 ? 45000 : 120000)
        : (contactMessages.size > 0 ? 10000 : 30000);

      setTimeout(async () => {
        if (historyDone) return;

        // Phase 2: if no messages from auto-sync, try on-demand fetchMessageHistory
        // This works on established sessions — sends historySyncOnDemandRequest per chat
        if (contactMessages.size === 0) {
          const fetchCandidates = new Map(knownChats);
          if (fetchCandidates.size === 0) {
            for (const jid of contactNames.keys()) {
              if (!jid || jid.endsWith('@g.us') || jid === 'status@broadcast') continue;
              fetchCandidates.set(jid, null);
            }
          }

          if (fetchCandidates.size === 0 && seedChatCandidates.size > 0) {
            for (const jid of seedChatCandidates) {
              if (!jid || jid.endsWith('@g.us') || jid === 'status@broadcast') continue;
              fetchCandidates.set(jid, null);
            }
          }

          emit({
            phase: 'fallback_fetch',
            chats_known: knownChats.size,
            chats_from_contacts: Math.max(fetchCandidates.size - knownChats.size, 0),
            chats_from_seed: seedChatCandidates.size,
            total_candidates: fetchCandidates.size,
          });

          if (fetchCandidates.size > 0) {
          let triggered = 0;
          for (const [jid, chatInfo] of fetchCandidates) {
            try {
              // Use the last-known message key as cursor (gets messages before it)
              // If no key known, use a synthetic cursor with current timestamp
              const key = chatInfo?.key || { remoteJid: jid, fromMe: false, id: '' };
              const ts  = chatInfo?.ts  || Date.now();
              await sock.fetchMessageHistory(200, key, ts);
              triggered++;
              await new Promise(r => setTimeout(r, 400)); // 400ms between requests
            } catch { /* ignore per-chat errors */ }
          }
          emit({ phase: 'fallback_triggered', count: triggered });

          // Wait for messaging-history.set events to arrive from WhatsApp
          const fallbackWaitMs = FORCE_FULL ? 300000 : 90000;
          await new Promise(r => setTimeout(r, fallbackWaitMs));
          }
        }

        historyDone = true;
        // Save to store
        if (contactMessages.size > 0) {
          try {
            const store = {
              messages: Object.fromEntries(contactMessages),
              names: Object.fromEntries(contactNames),
              notify: Object.fromEntries(contactNotify),
              synced_at: new Date().toISOString(),
            };
            writeFileSync(STORE_FILE, JSON.stringify(store));
            emit({ phase: 'saved', contacts: contactMessages.size });
          } catch (e) { emit({ phase: 'save_error', error: e.message }); }
        } else {
          emit({ phase: 'save_skipped_empty' });
        }
        await processAndExit();
      }, initialWait);
    }

    if (connection === 'close') {
      const statusCode = update?.lastDisconnect?.error?.output?.statusCode || null;
      const reason = update?.lastDisconnect?.error?.message || 'connection_closed';
      emit({ phase: 'connection_closed', status_code: statusCode, reason: String(reason).slice(0, 220) });

      if (!historyDone && !closeFinalizeTimer) {
        // Grace period: Baileys may transiently close/reopen during history fetch.
        closeFinalizeTimer = setTimeout(async () => {
          if (!historyDone) {
            historyDone = true;
            await processAndExit();
          }
        }, 30000);
      }
    }
  });

  // ── Processing ──────────────────────────────────────────────────────────

  async function processAndExit() {
    const totalMsgs = Array.from(contactMessages.values()).reduce((s, m) => s + m.length, 0);
    emit({ phase: 'processing', total_contacts: contactMessages.size, total_messages: totalMsgs });

    if (contactMessages.size === 0) {
      emit({
        phase: 'no_history',
        error: 'NO_HISTORY_RECEIVED',
        message: FORCE_FULL
          ? 'No messages received in full-history mode (auto-sync + fallback fetch)'
          : 'No messages received in 90-day window (auto-sync + fallback fetch)',
      });
      process.exit(2);
    }

    // Build contact profiles with interaction stats
    const sortedContacts = Array.from(contactMessages.entries())
      .map(([jid, msgs]) => {
        const sorted = msgs.sort((a, b) => a.timestamp - b.timestamp);
        const types = {};
        for (const m of sorted) { types[m.type] = (types[m.type] || 0) + 1; }
        return {
          jid,
          phone: extractPhoneFromJid(jid),
          name: contactNames.get(jid) || jid.replace(/@.*/, ''),
          pushName: contactNotify.get(jid) || '',
          messages: sorted,
          count: sorted.length,
          types,
          sent: sorted.filter(m => m.fromMe).length,
          received: sorted.filter(m => !m.fromMe).length,
          voice_notes: types.voice_note || 0,
          documents: types.document || 0,
          locations: (types.location || 0) + (types.live_location || 0),
          reactions: types.reaction || 0,
          last_ts: sorted[sorted.length - 1]?.timestamp || 0,
        };
      })
      .sort((a, b) => b.count - a.count);

    const filteredContacts = sortedContacts.filter(shouldIncludeContact);
    emit({
      phase: 'contacts_filtered',
      before: sortedContacts.length,
      after: filteredContacts.length,
      dropped: sortedContacts.length - filteredContacts.length,
      min_messages: MIN_CONTACT_MESSAGES,
    });

    if (filteredContacts.length === 0) {
      emit({ phase: 'no_contacts_after_filter', error: 'NO_CONTACTS_AFTER_FILTER' });
      process.exit(3);
    }

    emit({ phase: 'distilling', contacts: filteredContacts.length });

    // Build cross-contact mention index: for each contact name, find messages
    // from OTHER contacts that mention them. This completes the picture.
    function getCrossContactContext(contactName) {
      const mentions = [];
      const nameLower = contactName.toLowerCase();
      // Skip generic/numeric names
      if (/^\d+$/.test(contactName) || contactName.length < 3) return mentions;

      for (const other of sortedContacts) {
        if (other.name === contactName) continue; // skip self
        for (const m of other.messages) {
          if (m.text && m.text.toLowerCase().includes(nameLower)) {
            mentions.push({
              otherContact: other.name,
              text: `${m.fromMe ? 'User' : other.name}: ${m.text.substring(0, 120)}`,
            });
          }
        }
      }
      return mentions.slice(0, 10); // max 10 cross-references
    }

    // Load previously distilled results (for resume after interruption)
    let previousResults = [];
    const distilledSourceKeys = new Set();
    try {
      if (!FORCE_FULL && existsSync(DISTILL_FILE)) {
        previousResults = JSON.parse(readFileSync(DISTILL_FILE, 'utf8'));
        for (const r of previousResults) {
          const sourceKey = r.source_jid || r.identity?.phone || '';
          if (sourceKey) distilledSourceKeys.add(sourceKey);
        }
        emit({ phase: 'resume', already_distilled: distilledSourceKeys.size });
      }
    } catch { /* fresh start */ }

    const results = [...previousResults];
    // FIX-8: Build nickname index from already-distilled contacts.
    // As contacts are distilled, their nicknames and mentioned_people become available
    // for resolving references in later contacts' conversations.
    const nicknameIndex = new Map(); // nickname → canonical name
    for (const r of previousResults) {
      const cname = r.identity?.display_name || r.identity?.name || '';
      for (const nick of (r.identity?.nicknames || [])) {
        if (nick && nick.toLowerCase() !== cname.toLowerCase()) {
          nicknameIndex.set(nick.toLowerCase(), cname);
        }
      }
      // Also index mentioned_people across contacts
      const mpeople = r.merged_intelligence?.mentioned_people || r.mentioned_people || [];
      for (const mp of mpeople) {
        if (mp.name) nicknameIndex.set(mp.name.toLowerCase(), `mentioned by ${cname}`);
      }
    }

    for (let i = 0; i < filteredContacts.length; i++) {
      const c = filteredContacts[i];
      // Skip already-distilled contacts (resume support)
      if (distilledSourceKeys.has(c.jid) || distilledSourceKeys.has(c.phone)) continue;

      emit({ phase: 'distilling_contact', index: i + 1, total: filteredContacts.length, name: c.name, msgs: c.count });

      // Get cross-contact mentions for richer context
      const crossContext = getCrossContactContext(c.name);
      if (crossContext.length > 0) {
        emit({ phase: 'cross_context', contact: c.name, mentions: crossContext.length });
      }

      try {
        const intel = await distillContact(c, i + 1, crossContext);
        if (intel) {
          const compact = compactIntelProfile(intel);
          compact.source_jid = c.jid;
          results.push(compact);
          // FIX-8: Update nickname index with newly distilled contact
          const cname = compact.identity?.display_name || compact.identity?.name || c.name;
          for (const nick of (compact.identity?.nicknames || [])) {
            if (nick && nick.toLowerCase() !== cname.toLowerCase()) {
              nicknameIndex.set(nick.toLowerCase(), cname);
            }
          }
          const mpeople = compact.merged_intelligence?.mentioned_people || compact.mentioned_people || [];
          for (const mp of mpeople) {
            if (mp.name) nicknameIndex.set(mp.name.toLowerCase(), `mentioned by ${cname}`);
          }
        }
      } catch (e) {
        emit({ phase: 'distill_error', contact: c.name, error: e.message });
        const fallback = compactIntelProfile(buildBasicProfile(c, i + 1));
        fallback.source_jid = c.jid;
        results.push(fallback);
      }
      // Save progress every 5 contacts
      if (results.length % 5 === 0) {
        try { writeFileSync(DISTILL_FILE, JSON.stringify(results)); } catch {}
      }
      await new Promise(r => setTimeout(r, 200));
    }

    // Save final results to file (always succeeds — used as the source of truth)
    try { writeFileSync(DISTILL_FILE, JSON.stringify(results)); } catch {}

    // ── Phase 5: Route through local Tenant DS Service ──────────────────────
    // Instead of emitting results and relying on the pairing service to forward
    // to MyndLens, we POST directly to the local DS service for ONNX vectorization
    // and dual-stream forwarding (Stream A → device, Stream B → ChromaDB).
    const dsServiceUrl = `http://localhost:${process.env.DS_SERVICE_PORT || '10014'}`;
    let dsRouted = false;

    if (results.length > 0) {
      emit({ phase: 'ds_service_routing', contacts: results.length, url: dsServiceUrl });

      try {
        const healthRes = await fetch(`${dsServiceUrl}/health`);
        if (healthRes.ok) {
          const userId = process.env.USER_ID || '';
          const res = await fetch(`${dsServiceUrl}/ds/process-contacts`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contacts: results,
              source: 'whatsapp',
              user_id: userId,
              user_name: 'User',
            }),
          });
          if (res.ok) {
            const data = await res.json();
            dsRouted = true;
            emit({
              phase: 'ds_service_done',
              status: data.status,
              stream_a: data.stream_a_count,
              stream_b: data.stream_b_count,
              stats: data.stats,
            });
          } else {
            emit({ phase: 'ds_service_error', status: res.status, error: await res.text().catch(() => '') });
          }
        } else {
          emit({ phase: 'ds_service_unavailable', reason: 'health check failed' });
        }
      } catch (e) {
        emit({ phase: 'ds_service_unavailable', reason: e.message });
      }
    }

    // Emit compact done signal — contacts are in the file, NOT in stdout
    // (full contacts JSON can exceed Docker exec pipe buffer limits)
    emit({
      phase: 'done',
      contact_count: results.length,
      results_file: DISTILL_FILE,
      ds_routed: dsRouted,
      contacts: INLINE_RESULTS ? results : undefined,
    });
    process.exit(0);
  }

  function buildBasicProfile(c, rank) {
    // Use pushName if name looks like a phone number
    const isPhone = /^\+?\d[\d\s-]{5,}$/.test(c.name.trim());
    const displayName = (isPhone && c.pushName && !/^\+?\d[\d\s-]{5,}$/.test(c.pushName.trim()))
      ? c.pushName : c.name;
    const slug = displayName.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '').substring(0, 30);
    return {
      canonical_id: `whatsapp_${slug}`,
      node_type: 'Person',
      identity: {
        display_name: displayName,
        full_name: displayName,
        nicknames: [],
        relationship: 'unknown',
        confidential: false,
      },
      contact_methods: {
        phones: [{ number: c.phone, type: 'whatsapp', source: 'whatsapp', primary: true }],
        emails: [],
        telegram: null,
        linkedin_url: null,
      },
      merged_intelligence: {
        active_threads: [],
        pending_actions: { user_owes: [], they_owe: [] },
        emotional_context: { current_mood: 'unknown', recent_events: [] },
        pattern: {
          frequency: c.count > 100 ? 'daily' : c.count > 30 ? 'weekly' : 'occasional',
          style: c.voice_notes > 5 ? 'voice_heavy' : 'text-based',
          peak_hours: 'varied',
        },
        aspirations_mentioned: [],
        inner_circle_rank: rank,
        message_count_90d: c.count,
        last_interaction: new Date(c.last_ts).toISOString(),
        sentiment: 'unknown',
        upcoming_events: [],
        financial_mentions: [],
        travel_mentions: [],
        mentioned_people: [],
      },
      merge_keys: {
        phone_normalized: [c.phone],
        email_normalized: [],
        name_variants: [displayName.toLowerCase()],
      },
      interaction_signals: c.types,
    };
  }

  async function distillContact(c, rank, crossContactContext) {
    if (!LLM_KEY) return buildBasicProfile(c, rank);

    // Build conversation text — include message type annotations
    const recent = c.messages.slice(-200);
    const convoLines = recent.map(m => {
      const speaker = m.fromMe ? 'Me' : c.name;
      if (m.type === 'text') return `${speaker}: ${m.text}`;
      if (m.type === 'voice_note') return `${speaker}: [Voice note ${m.duration || '?'}s]`;
      if (m.type === 'document') return `${speaker}: [Shared file: ${m.fileName || 'doc'}] ${m.text || ''}`;
      if (m.type === 'image') return `${speaker}: [Photo] ${m.text || ''}`;
      if (m.type === 'video') return `${speaker}: [Video] ${m.text || ''}`;
      if (m.type === 'location' || m.type === 'live_location') return `${speaker}: ${m.text}`;
      if (m.type === 'shared_contact' || m.type === 'shared_contacts') return `${speaker}: ${m.text}`;
      if (m.type === 'reaction') return `${speaker}: reacted ${m.text}`;
      if (m.type === 'sticker') return `${speaker}: [Sticker]`;
      return `${speaker}: ${m.text || '[message]'}`;
    });

    // Build cross-contact context section
    let crossSection = '';
    if (crossContactContext && crossContactContext.length > 0) {
      crossSection = '\nCROSS-CONTACT CONTEXT (mentions of people/topics from OTHER conversations):\n';
      for (const ctx of crossContactContext) {
        crossSection += `  [From chat with ${ctx.otherContact}]: ${ctx.text}\n`;
      }
    }

    // Determine best available name for the contact
    const isPhoneNumber = /^\+?\d[\d\s-]{5,}$/.test(c.name.trim());
    let nameContext = '';
    if (isPhoneNumber) {
      nameContext = `\nNAME RESOLUTION (CRITICAL):
The saved contact name "${c.name}" is a phone number, NOT a real name.`;
      if (c.pushName && !/^\+?\d[\d\s-]{5,}$/.test(c.pushName.trim())) {
        nameContext += `\nThe WhatsApp profile name (pushName) is: "${c.pushName}" — use this as the display_name.`;
      } else {
        nameContext += `\nNo WhatsApp profile name available. Determine the person's REAL name from:
  1. How the user addresses them in "Me:" messages (look for names, not phone numbers)
  2. How they sign off their messages
  3. How other contacts refer to them (see CROSS-CONTACT CONTEXT)
If you cannot determine a real name, use "${c.pushName || c.name}" as display_name.`;
      }
    }

    const todayDate = new Date().toISOString().split('T')[0];

    // System prompt: canonical entity schema + cultural intelligence
    const systemPrompt = `You are a Digital Self Distillation Engine. You process WhatsApp conversation data
and produce a single structured canonical entity.

OUTPUT SCHEMA — you MUST output a single JSON object matching this EXACT structure:
{
  "canonical_id": "whatsapp_<name_slug>",
  "node_type": "Person",
  "identity": {
    "display_name": "Full Name as commonly known",
    "full_name": "Full formal name if known",
    "nicknames": ["Nick1", "Nick2"],
    "relationship": "friend|colleague|family|manager|acquaintance|service_provider|unknown",
    "confidential": false
  },
  "contact_methods": {
    "phones": [{"number": "+E164 format", "type": "whatsapp", "source": "whatsapp", "primary": true}],
    "emails": [],
    "telegram": null,
    "linkedin_url": null
  },
  "merged_intelligence": {
    "active_threads": [
      {
        "topic": "Brief topic label (NOT message text)",
        "type": "Discussion|Planning|Negotiation|Support|Social",
        "status": "active|stalled|resolved",
        "tension_level": "none|low|medium|high",
        "decided": ["decisions made"],
        "undecided": ["open questions"],
        "user_action": "what user needs to do",
        "their_action": "what they need to do",
        "deadline": "YYYY-MM-DD or null"
      }
    ],
    "pending_actions": {
      "user_owes": [{"action": "description", "deadline": "YYYY-MM-DD or null", "context": "brief context"}],
      "they_owe": [{"action": "description", "deadline": "YYYY-MM-DD or null", "context": "brief context"}]
    },
    "emotional_context": {
      "current_mood": "happy|concerned|stressed|neutral|excited|frustrated|unknown",
      "recent_events": ["brief event labels, NOT message content"]
    },
    "pattern": {
      "frequency": "daily|weekly|monthly|sporadic|rare",
      "style": "short_bursts|long_conversations|voice_heavy|media_sharing|formal|unknown",
      "peak_hours": "morning|afternoon|evening|night|varied"
    },
    "aspirations_mentioned": ["goals, plans, wishes expressed"],
    "inner_circle_rank": 99,
    "message_count_90d": 0,
    "last_interaction": "YYYY-MM-DDTHH:MM:SSZ",
    "sentiment": "positive|negative|neutral|mixed",
    "upcoming_events": [
      {"event": "description", "date": "YYYY-MM-DD or null", "location": "place or null", "contacts": ["names involved"]}
    ],
    "financial_mentions": [
      {"type": "owed|lent|split|payment|bill", "amount": "numeric or null", "currency": "INR|USD|etc", "due_date": "YYYY-MM-DD or null", "counterparty": "name"}
    ],
    "travel_mentions": [
      {"type": "flight|hotel|train|trip", "reference": "PNR or booking ref or null", "date": "YYYY-MM-DD or null", "origin": "place or null", "destination": "place or null"}
    ],
    "mentioned_people": [
      {"name": "person mentioned", "context": "relationship to this contact or user", "phone_if_known": "number or null"}
    ]
  },
  "merge_keys": {
    "phone_normalized": ["+E164 numbers"],
    "email_normalized": [],
    "name_variants": ["lowercase name variations for fuzzy matching"]
  }
}

NICKNAME DETECTION (CRITICAL — read carefully):
- Scan every "Me:" message for how the USER addresses this contact.
- If the user consistently uses a word/term DIFFERENT from the contact name, that word IS a nickname. Add it to "nicknames".
- Nicknames can be endearments, cultural terms, pet names, or shortened forms. Extract ALL of them.

CULTURAL CONTEXT (IMPORTANT — Multi-cultural Address Terms):
SOUTH ASIAN (Telugu / Tamil / Hindi / Kannada / Malayalam):
- "Anna" = elder brother (Telugu/Tamil). If they call the user "Anna", they are a younger sibling.
- "Akka" = elder sister (Telugu/Tamil). "Didi" = elder sister (Hindi).
- "Bhaiya/Bhai" = brother (Hindi). "Thamma" = grandmother (Telugu).
- "Nanna/Nana" = father (Telugu). "Amma" = mother (Telugu, also used casually).
- "Papa" — In Telugu families, commonly an affectionate nickname for a younger female relative (sister, daughter, niece). It does NOT mean "father" in this context.
- "Babu" = affectionate term for son, younger male, or pet name.
- "Chinni/Chinna" = little one, endearment for younger relatives.
- "Ra" suffix (e.g., "Ravi ra") = casual address marker in Telugu, not a name.
- "Machi/Machan" = close friend or brother-in-law (Tamil).
- "Chetta/Chechi" = elder brother/sister (Malayalam).
ARABIC / MIDDLE EASTERN: "Habibi/Habibti" = dear/darling. "Abu/Umm + name" = father/mother of. "Ya" prefix = vocative, not a name.
EAST ASIAN: "-san/-kun/-chan" (Japanese) = honorifics. "Ge/Jie" (Chinese) = elder sibling. "Oppa/Unnie" (Korean) = elder sibling.
LATIN / HISPANIC: "Mijo/Mija" = my son/daughter. "Compadre/Comadre" = close family friend.

GENERAL RULES:
- When the user uses ANY of these terms, add them to "nicknames".
- Determine relationship from: (1) how they address each other, (2) conversation topics and tone, (3) cross-contact context.
- If she calls user "Anna" and user calls her "Papa" → she is the user's younger sister.

TRAVEL STATUS RULES:
- A trip has TWO legs: outbound (going) and return (coming back).
- Only mark travel as "resolved" when the person has RETURNED to their usual location.
- When information is insufficient, mark status as "unclear" rather than guessing.

CRITICAL RULES:
- inner_circle_rank: 1 = closest (daily contact, family), 99 = unknown/distant
- Output ONLY the JSON object. No markdown, no explanation, no preamble.
- Extract commitments with deadlines. If someone says "by Friday", calculate the actual date.
- Financial amounts must be numeric where possible.
- Travel references (PNR, booking IDs) must be captured verbatim.
- Do NOT include raw message text anywhere. All fields must be distilled summaries.
- The "display_name" MUST be a human-readable name, NEVER a phone number.
- merge_keys.name_variants MUST include: lowercase display_name, all nicknames lowercase, first name lowercase.`;

    // User prompt: conversation data
    const userPrompt = `Process this WhatsApp conversation for the user. Today's date is ${todayDate}.
${nameContext}
Contact saved name: "${c.name}"
WhatsApp pushName: "${c.pushName || 'not available'}"
Phone: ${c.phone}

STATS: ${c.count} messages (${c.sent} sent, ${c.received} received), ${c.voice_notes} voice notes, ${c.documents} documents shared, ${c.locations} locations shared
Inner circle rank: ${rank}
Last interaction: ${new Date(c.last_ts).toISOString().split('T')[0]}
${crossSection}
CONVERSATION (most recent ${recent.length} messages):
${convoLines.join('\n').substring(0, 8000)}

Output the canonical entity JSON object:`;

    try {
      const { callGemini } = await import('/app/tenant_llm.mjs');
      const response = await callGemini(systemPrompt, userPrompt, {
        apiKey: LLM_KEY,
        maxOutputTokens: 65536,
      });
      const parsed = parseJSON(response);
      if (parsed) {
        // Ensure phone is always from Baileys (source of truth)
        if (!parsed.contact_methods) parsed.contact_methods = {};
        parsed.contact_methods.phones = [{ number: c.phone, type: 'whatsapp', source: 'whatsapp', primary: true }];
        if (!parsed.merge_keys) parsed.merge_keys = {};
        if (!parsed.merge_keys.phone_normalized || parsed.merge_keys.phone_normalized.length === 0) {
          parsed.merge_keys.phone_normalized = [c.phone];
        }
        parsed.interaction_signals = c.types;
        return parsed;
      }
    } catch (e) {
      emit({ phase: 'llm_error', contact: c.name, error: e.message });
    }

    return buildBasicProfile(c, rank);
  }

  async function callLLM(prompt) {
    const { callSimple } = await import('/app/tenant_llm.mjs');
    return await callSimple(prompt, LLM_KEY);
  }

  function parseJSON(text) {
    try {
      let clean = text.trim();
      if (clean.startsWith('```json')) clean = clean.slice(7);
      if (clean.startsWith('```')) clean = clean.slice(3);
      if (clean.endsWith('```')) clean = clean.slice(0, -3);
      return JSON.parse(clean.trim());
    } catch { return null; }
  }

  // Hard timeout: 15 minutes (50 contacts × ~10s per LLM call = ~8 min)
  setTimeout(() => { emit({ error: 'timeout_900s' }); processAndExit(); }, 900000);
}

run().catch(e => { emit({ error: e.message }); process.exit(1); });
