/**
 * wa_pair.mjs v5 — Minimal working pairing
 *
 * Key insight: After requestPairingCode, Baileys socket CLOSES and must
 * RECONNECT for the handshake to complete. The reconnected socket,
 * using the saved creds (which now contain the pairing registration),
 * will reach 'open' once the user enters the code.
 *
 * Previous versions kept the socket alive but it was stuck in 'connecting'
 * forever because Baileys expects a close→reconnect cycle after pairing.
 */
import makeWASocket_ from '@whiskeysockets/baileys';
import {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
} from '@whiskeysockets/baileys';
import { mkdirSync } from 'fs';

const makeWASocket = makeWASocket_.default ?? makeWASocket_;
const phone = process.argv[2];
const AUTH_DIR = '/home/node/.openclaw/credentials/whatsapp/default';

if (!phone) { emit({ error: 'phone_required' }); process.exit(1); }
mkdirSync(AUTH_DIR, { recursive: true });

function emit(obj) { process.stdout.write(JSON.stringify(obj) + '\n'); }

async function connectToWhatsApp() {
  let waVersion;
  try {
    const { version } = await fetchLatestBaileysVersion();
    waVersion = version;
    emit({ info: 'version', version: waVersion });
  } catch {
    waVersion = [2, 3000, 1033846690];
  }

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);

  const sock = makeWASocket({
    version: waVersion,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, { level: 'silent', child: () => ({ level: 'silent', info(){}, error(){}, warn(){}, debug(){}, trace(){}, fatal(){} }) }),
    },
    printQRInTerminal: false,
    browser: ['Ubuntu', 'Chrome', '22.04.4'],
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    emit({ event: 'connection', connection, statusCode: lastDisconnect?.error?.output?.statusCode });

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

      if (shouldReconnect) {
        emit({ info: 'reconnecting', statusCode });
        // Reconnect — this is the EXPECTED flow after pairing code is issued.
        // On reconnect, if the user has entered the code, creds will be valid
        // and connection will reach 'open'.
        connectToWhatsApp();
      } else {
        emit({ error: 'logged_out', statusCode });
        process.exit(1);
      }
    }

    if (connection === 'open') {
      emit({ connected: true, info: 'WhatsApp linked successfully' });
      // Graceful shutdown: close socket and wait for pending creds.update writes
      // to flush to disk. process.exit(0) without this causes partial key file
      // writes → AES-GCM decryption errors on next load.
      try { sock.end(undefined); } catch {}
      setTimeout(() => process.exit(0), 3000);
    }
  });

  // Request pairing code if not already registered
  if (!sock.authState.creds.registered) {
    const cleanNumber = phone.replace(/\D/g, '');
    // Small delay to let the socket stabilize
    await new Promise(r => setTimeout(r, 3000));
    try {
      const code = await sock.requestPairingCode(cleanNumber);
      const fmt = code.includes('-') ? code : code.replace(/^(.{4})(.{4})$/, '$1-$2');
      emit({ pairing_code: fmt });
    } catch (e) {
      emit({ error: 'code_request_failed: ' + e.message });
      process.exit(1);
    }
  } else {
    emit({ info: 'already_registered', me: sock.authState.creds.me });
  }
}

connectToWhatsApp();

// Hard timeout
setTimeout(() => { emit({ error: 'timeout_180s' }); process.exit(1); }, 180000);
