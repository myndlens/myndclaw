/**
 * wa_session.mjs — Direct Baileys session manager
 * Runs inside the OpenClaw container.
 *
 * Modes:
 *   node wa_session.mjs qr        <auth_dir>               → emits {"qr":"<string>"}
 *   node wa_session.mjs phone     <auth_dir> <e164>         → emits {"pairing_code":"XXXX-XXXX"}
 *   node wa_session.mjs status    <auth_dir>                → emits {"connected":bool}
 *   node wa_session.mjs validate  <auth_dir> <e164>         → emits {"exists":bool,"jid":"..."}
 *
 * Output: newline-delimited JSON to stdout.
 */

import makeWASocket_ from '@whiskeysockets/baileys';
import { useMultiFileAuthState } from '@whiskeysockets/baileys';
import { DisconnectReason } from '@whiskeysockets/baileys';
import { mkdirSync } from 'fs';

const makeWASocket = makeWASocket_.default ?? makeWASocket_;
const [,, mode, authDir, phoneArg] = process.argv;

if (!authDir) { emit({ error: 'auth_dir required' }); process.exit(1); }

mkdirSync(authDir, { recursive: true });

function emit(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

async function run() {
  const { state, saveCreds } = await useMultiFileAuthState(authDir);

  const sock = makeWASocket({
    auth:                          state,
    printQRInTerminal:             false,
    syncFullHistory:               false,
    generateHighQualityLinkPreview: false,
    browser:                       ['Ubuntu', 'Chrome', '22.04.4'],
  });

  sock.ev.on('creds.update', saveCreds);

  let done = false;

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    // QR mode — emit the raw QR string
    if (qr && (mode === 'qr') && !done) {
      done = true;
      emit({ qr });
    }

    // Phone number pairing mode — request code once connected to WA servers
    if (connection === 'connecting' && mode === 'phone' && phoneArg && !done) {
      try {
        await new Promise(r => setTimeout(r, 2000));
        const code = await sock.requestPairingCode(phoneArg.replace(/\D/g, ''));
        done = true;
        emit({ pairing_code: code });
      } catch (e) {
        emit({ error: 'pairing_code_failed: ' + e.message });
        process.exit(1);
      }
    }

    // Validate mode — once connection is open, check if number is on WhatsApp
    if (connection === 'open' && mode === 'validate' && phoneArg && !done) {
      done = true;
      try {
        const cleaned = phoneArg.replace(/\D/g, '');
        const jid = cleaned + '@s.whatsapp.net';
        const [result] = await sock.onWhatsApp(jid);
        if (result && result.exists) {
          emit({ exists: true, jid: result.jid, number: cleaned });
        } else {
          emit({ exists: false, number: cleaned, reason: 'not_on_whatsapp' });
        }
      } catch (e) {
        emit({ error: 'validate_failed: ' + e.message });
      }
      // Graceful disconnect — do NOT call sock.logout() as it destroys credentials
      sock.end(undefined);
      process.exit(0);
    }

    if (connection === 'open' && mode !== 'validate') {
      emit({ connected: true });
      process.exit(0);
    }

    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode ?? 0;
      if (code === DisconnectReason.loggedOut) {
        emit({ error: 'logged_out' });
        process.exit(1);
      }
    }
  });

  // Status mode: just check if creds exist and connection opens
  if (mode === 'status') {
    setTimeout(() => {
      emit({ connected: false, reason: 'timeout' });
      process.exit(0);
    }, 8000);
    return;
  }

  // Timeout
  const timeouts = { phone: 20000, qr: 12000, validate: 15000, status: 8000 };
  setTimeout(() => {
    if (!done) {
      emit({ error: 'timeout_waiting_for_' + mode });
      process.exit(1);
    }
  }, timeouts[mode] || 15000);
}

run().catch(e => { emit({ error: e.message }); process.exit(1); });
