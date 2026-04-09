import makeWASocket_ from '@whiskeysockets/baileys';
import {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  Browsers,
} from '@whiskeysockets/baileys';
import { existsSync, mkdirSync, writeFileSync } from 'fs';

const makeWASocket = makeWASocket_.default ?? makeWASocket_;

const TENANT_ID = process.argv[2];
const MODE = (process.argv[3] || 'qr').toLowerCase();
const PHONE = process.argv[4] || '';
const AUTH_DIR = process.env.WA_BOOTSTRAP_AUTH_DIR || '/home/node/.openclaw/credentials/whatsapp/bootstrap';
const DATA_DIR = process.env.WA_BOOTSTRAP_DATA_DIR
  || (existsSync('/home/node/.openclaw/workspace') ? '/home/node/.openclaw/workspace' : '')
  || (existsSync('/data') ? '/data' : '/tmp');
const STORE_FILE = process.env.WA_BOOTSTRAP_STORE_FILE || `${DATA_DIR}/wa_ds_messages_${TENANT_ID}_bootstrap.json`;
const HARD_TIMEOUT_MS = Number(process.env.WA_BOOTSTRAP_TIMEOUT_MS || 420000);
const MIN_OPEN_MS = Number(process.env.WA_BOOTSTRAP_MIN_OPEN_MS || 45000);
const QUIET_WINDOW_MS = Number(process.env.WA_BOOTSTRAP_QUIET_MS || 20000);
const MAX_OPEN_MS = Number(process.env.WA_BOOTSTRAP_MAX_OPEN_MS || 180000);

if (!TENANT_ID) {
  process.stdout.write(JSON.stringify({ error: 'tenant_id required' }) + '\n');
  process.exit(1);
}

if (!['phone', 'qr'].includes(MODE)) {
  process.stdout.write(JSON.stringify({ error: 'mode must be phone or qr' }) + '\n');
  process.exit(1);
}

if (MODE === 'phone' && !PHONE) {
  process.stdout.write(JSON.stringify({ error: 'phone_number required for phone mode' }) + '\n');
  process.exit(1);
}

mkdirSync(AUTH_DIR, { recursive: true });

const emit = (obj) => process.stdout.write(JSON.stringify(obj) + '\n');
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const contactMessages = new Map();
const contactNames = new Map();
const contactNotify = new Map();

let firstOpenAt = 0;
let lastDataAt = 0;
let finalizing = false;
let monitorTimer = null;
let hardTimeout = null;
let pairingCodeRequested = false;
let qrSeen = false;
let socketGeneration = 0;
let activeSocket = null;

function totalMessageCount() {
  return Array.from(contactMessages.values()).reduce((sum, msgs) => sum + msgs.length, 0);
}

function extractMessageData(msg) {
  const content = msg.message || {};
  const jid = msg.key?.remoteJid || '';
  if (!jid || jid === 'status@broadcast' || jid.endsWith('@g.us')) return null;
  const fromMe = !!msg.key?.fromMe;
  const ts = Number(msg.messageTimestamp || 0) * 1000 || Date.now();

  if (content.conversation) return { type: 'text', text: content.conversation, fromMe, timestamp: ts };
  if (content.extendedTextMessage?.text) return { type: 'text', text: content.extendedTextMessage.text, fromMe, timestamp: ts };
  if (content.imageMessage) return { type: 'image', text: content.imageMessage.caption || '[Image]', fromMe, timestamp: ts };
  if (content.videoMessage) return { type: 'video', text: content.videoMessage.caption || '[Video]', fromMe, timestamp: ts };
  if (content.documentMessage) return { type: 'document', text: content.documentMessage.fileName || '[Document]', fromMe, timestamp: ts };
  if (content.audioMessage) {
    return { type: content.audioMessage.ptt ? 'voice_note' : 'audio', text: content.audioMessage.ptt ? '[Voice note]' : '[Audio]', fromMe, timestamp: ts };
  }
  if (content.locationMessage) return { type: 'location', text: '[Location]', fromMe, timestamp: ts };
  if (content.liveLocationMessage) return { type: 'live_location', text: '[Live location]', fromMe, timestamp: ts };
  if (content.contactMessage) return { type: 'contact', text: '[Contact]', fromMe, timestamp: ts };
  if (content.reactionMessage) return { type: 'reaction', text: content.reactionMessage.text || '👍', fromMe, timestamp: ts };
  if (content.stickerMessage) return { type: 'sticker', text: '[Sticker]', fromMe, timestamp: ts };
  if (content.protocolMessage) return null;
  return { type: 'other', text: '[Message]', fromMe, timestamp: ts };
}

function storeMessage(jid, msgData) {
  if (!jid || !msgData) return;
  if (!contactMessages.has(jid)) contactMessages.set(jid, []);
  contactMessages.get(jid).push(msgData);
  lastDataAt = Date.now();
}

function storeName(jid, name) {
  if (jid && name) contactNames.set(jid, name);
}

function storeNotify(jid, notify) {
  if (jid && notify) contactNotify.set(jid, notify);
}

async function finalize(reason) {
  if (finalizing) return;
  finalizing = true;
  if (monitorTimer) clearInterval(monitorTimer);
  if (hardTimeout) clearTimeout(hardTimeout);

  const totalMessages = totalMessageCount();
  if (totalMessages === 0) {
    emit({ phase: 'failed', error: 'NO_HISTORY_RECEIVED', reason });
    try { activeSocket?.end(undefined); } catch {}
    setTimeout(() => process.exit(2), 500);
    return;
  }

  const store = {
    messages: Object.fromEntries(contactMessages),
    names: Object.fromEntries(contactNames),
    notify: Object.fromEntries(contactNotify),
    synced_at: new Date().toISOString(),
    bootstrap_mode: MODE,
    bootstrap_reason: reason,
  };
  writeFileSync(STORE_FILE, JSON.stringify(store));
  emit({
    phase: 'done',
    reason,
    contacts: contactMessages.size,
    total_messages: totalMessages,
    store_file: STORE_FILE,
    auth_dir: AUTH_DIR,
  });
  try { activeSocket?.end(undefined); } catch {}
  setTimeout(() => process.exit(0), 1500);
}

function armFinalizeMonitor() {
  if (monitorTimer) return;
  monitorTimer = setInterval(() => {
    if (!firstOpenAt || finalizing) return;
    const now = Date.now();
    const hasMessages = totalMessageCount() > 0;
    if (!hasMessages) return;
    if (now - firstOpenAt < MIN_OPEN_MS) return;
    if (now - lastDataAt >= QUIET_WINDOW_MS) {
      finalize('history_idle');
      return;
    }
    if (now - firstOpenAt >= MAX_OPEN_MS) {
      finalize('max_open_reached');
    }
  }, 5000);
}

async function startSocket() {
  const generation = ++socketGeneration;
  let waVersion;
  try {
    const { version } = await fetchLatestBaileysVersion();
    waVersion = version;
  } catch {
    waVersion = [2, 3000, 1033846690];
  }

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
  activeSocket = sock;
  sock.ev.on('creds.update', saveCreds);

  emit({ phase: 'connecting', generation, mode: MODE, auth_dir: AUTH_DIR, store_file: STORE_FILE });

  sock.ev.on('messaging-history.set', ({ contacts, messages, chats }) => {
    if (contacts) {
      for (const c of contacts) {
        storeName(c.id, c.name || c.notify);
        if (c.notify) storeNotify(c.id, c.notify);
      }
    }
    if (messages) {
      for (const msg of messages) {
        const msgData = extractMessageData(msg);
        if (msgData) storeMessage(msg.key?.remoteJid, msgData);
      }
    }
    emit({ phase: 'history_batch', contacts: contacts?.length || 0, messages: messages?.length || 0, chats: chats?.length || 0, total_messages: totalMessageCount() });
    armFinalizeMonitor();
  });

  sock.ev.on('messages.upsert', ({ messages }) => {
    for (const msg of messages || []) {
      const msgData = extractMessageData(msg);
      if (msgData) storeMessage(msg.key?.remoteJid, msgData);
    }
    if ((messages || []).length > 0) {
      emit({ phase: 'messages_upsert', count: (messages || []).length, total_messages: totalMessageCount() });
      armFinalizeMonitor();
    }
  });

  sock.ev.on('contacts.upsert', (contacts) => {
    for (const c of contacts || []) {
      storeName(c.id, c.name || c.notify);
      if (c.notify) storeNotify(c.id, c.notify);
    }
  });

  sock.ev.on('contacts.set', ({ contacts }) => {
    for (const c of contacts || []) {
      storeName(c.id, c.name || c.notify);
      if (c.notify) storeNotify(c.id, c.notify);
    }
  });

  sock.ev.on('connection.update', async (update) => {
    const { connection, qr, lastDisconnect } = update;
    if (generation !== socketGeneration || finalizing) return;

    if (qr && MODE === 'qr') {
      qrSeen = true;
      emit({ phase: 'awaiting_scan', qr_raw: qr });
    }

    if (connection) {
      emit({ phase: 'connection', connection, status_code: lastDisconnect?.error?.output?.statusCode || null });
    }

    if (connection === 'open') {
      if (!firstOpenAt) firstOpenAt = Date.now();
      lastDataAt = Date.now();
      emit({ phase: 'connected', total_messages: totalMessageCount() });
      armFinalizeMonitor();
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      if (statusCode === DisconnectReason.loggedOut) {
        emit({ phase: 'failed', error: 'logged_out' });
        setTimeout(() => process.exit(1), 500);
        return;
      }
      if (!finalizing && generation === socketGeneration) {
        emit({ phase: 'reconnecting', status_code: statusCode || null });
        setTimeout(() => {
          if (!finalizing && generation === socketGeneration) startSocket().catch((e) => {
            emit({ phase: 'failed', error: e.message });
            process.exit(1);
          });
        }, 1500);
      }
    }
  });

  if (!state.creds.registered && MODE === 'phone' && !pairingCodeRequested) {
    pairingCodeRequested = true;
    const cleanPhone = PHONE.replace(/\D/g, '');
    await delay(3000);
    try {
      const code = await sock.requestPairingCode(cleanPhone);
      const formatted = code.includes('-') ? code : code.replace(/^(.{4})(.{4})$/, '$1-$2');
      emit({ phase: 'awaiting_code', pairing_code: formatted });
    } catch (e) {
      emit({ phase: 'failed', error: e.message || 'pairing_code_request_failed' });
      process.exit(1);
    }
  }
}

emit({ phase: 'init', tenant_id: TENANT_ID, mode: MODE, auth_dir: AUTH_DIR, store_file: STORE_FILE });
hardTimeout = setTimeout(() => {
  if (finalizing) return;
  if (totalMessageCount() > 0) {
    finalize('hard_timeout');
    return;
  }
  emit({ phase: 'failed', error: qrSeen ? 'timeout_no_history' : 'timeout_before_pairing' });
  try { activeSocket?.end(undefined); } catch {}
  setTimeout(() => process.exit(1), 500);
}, HARD_TIMEOUT_MS);

startSocket().catch((e) => {
  emit({ phase: 'failed', error: e.message });
  process.exit(1);
});