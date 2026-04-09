#!/bin/bash
# entrypoint.sh — Unified tenant container entry point.
# Starts BOTH the OpenClaw gateway AND the DS service.
#
# Config handling:
#   - First boot (empty PVC): writes a minimal valid gateway config
#   - Subsequent boots / migration: preserves existing config, only cleans known-bad keys
#
# Environment:
#   DS_SERVICE_PORT  — DS service port (default: 10014)
#   TENANT_ID        — Tenant identifier

set +e

DS_PORT="${DS_SERVICE_PORT:-10014}"
OC_CONFIG_DIR="${HOME}/.openclaw"
OC_CONFIG_FILE="${OC_CONFIG_DIR}/openclaw.json"
export NODE_PATH="/app/node_modules:/usr/local/lib/node_modules/openclaw/node_modules:${NODE_PATH:-}"

# ── Config initialization ─────────────────────────────────────────────────────
mkdir -p "${OC_CONFIG_DIR}/credentials" "${OC_CONFIG_DIR}/sessions" "${OC_CONFIG_DIR}/channels" \
         "${OC_CONFIG_DIR}/agents/main/sessions" 2>/dev/null || true

if [ -f "${OC_CONFIG_FILE}" ] && [ -s "${OC_CONFIG_FILE}" ]; then
  echo "[Entrypoint] Existing config found at ${OC_CONFIG_FILE} — preserving."
  # Clean known-bad keys that break older OC versions (idempotent sed)
  sed -i '/"bots":\s*{}/d' "${OC_CONFIG_FILE}" 2>/dev/null || true
else
  echo "[Entrypoint] First boot — writing minimal gateway config."
  cat > "${OC_CONFIG_FILE}" << 'OCCONFIG'
{
  "agents": {
    "defaults": {}
  },
  "gateway": {
    "mode": "local",
    "bind": "lan",
    "auth": {
      "mode": "token"
    },
    "controlUi": {
      "allowedOrigins": ["*"]
    }
  },
  "channels": {
    "whatsapp": {
      "dmPolicy": "disabled",
      "accounts": {}
    }
  }
}
OCCONFIG
fi

chown -R 1000:1000 "${OC_CONFIG_DIR}" 2>/dev/null || true

# ── Configure LLM provider for OC agent model + gateway trustedProxies ─────────
# LLM_KEY is the tenant's provisioned key (Gemini API key for standard offering,
# or user's BYOK key). Run on every boot so key changes take effect after restart.
# OC uses the OpenAI-compatible endpoint format for Gemini.
AGENT_DIR="${OC_CONFIG_DIR}/agents/main/agent"
mkdir -p "${AGENT_DIR}" 2>/dev/null || true
rm -f "${AGENT_DIR}/auth-profiles.json" 2>/dev/null || true   # stale OAuth file — causes warnings

if [ -n "${LLM_KEY}" ] && [ -f "${OC_CONFIG_FILE}" ]; then
  node -e "
    const fs = require('fs');
    const key = process.env.LLM_KEY;
    const agentDir = '${AGENT_DIR}';
    const ocConfig = '${OC_CONFIG_FILE}';

    // 1. Write models.json at agent level
    const modelsJson = {
      providers: {
        moonshot: {
          apiKey: key,
          baseUrl: 'https://generativelanguage.googleapis.com/v1beta/openai',
          api: 'openai-completions',
          models: [
            { id: 'gemini-2.5-flash', name: 'gemini-2.5-flash' }
          ]
        }
      }
    };
    fs.writeFileSync(agentDir + '/models.json', JSON.stringify(modelsJson, null, 2));

    // 2. Set openclaw.json: gateway config + model default + GLOBAL models.providers
    const cfg = JSON.parse(fs.readFileSync(ocConfig, 'utf8'));

    // Gateway: trustedProxies
    cfg.gateway = cfg.gateway || {};
    cfg.gateway.trustedProxies = ['127.0.0.1/32', '::1/128'];

    // Gateway: auth token from env (Issue 7 fix)
    cfg.gateway.auth = cfg.gateway.auth || {};
    cfg.gateway.auth.mode = 'token';
    cfg.gateway.auth.token = process.env.OPENCLAW_GATEWAY_TOKEN || cfg.gateway.auth.token;

    // Agent model default (preserve existing if set, otherwise use gemini)
    cfg.agents = cfg.agents || {};
    cfg.agents.defaults = cfg.agents.defaults || {};
    if (!cfg.agents.defaults.model || !cfg.agents.defaults.model.primary) {
      cfg.agents.defaults.model = { primary: 'moonshot/gemini-2.5-flash' };
    }

    // GLOBAL models.providers (Issue 11 fix — OC resolves providers from here)
    cfg.models = cfg.models || {};
    cfg.models.providers = cfg.models.providers || {};
    if (!cfg.models.providers.moonshot) {
      cfg.models.providers.moonshot = {
        apiKey: key,
        baseUrl: 'https://generativelanguage.googleapis.com/v1beta/openai',
        api: 'openai-completions',
        models: [
          { id: 'gemini-2.5-flash', name: 'gemini-2.5-flash' }
        ]
      };
    }

    fs.writeFileSync(ocConfig, JSON.stringify(cfg, null, 2));
    console.log('[Entrypoint] LLM + gateway token + global models configured');
  " 2>/dev/null || true
else
  echo "[Entrypoint] WARNING: LLM_KEY not set — OC agents will fail with 'no API key' error"
fi

# ── CRITICAL SAFETY GUARD: never allow dmPolicy="pairing" ─────────────────────
# OC default is dmPolicy: "pairing" — this auto-replies to EVERY incoming WA
# message with a pairing code, spamming all contacts. (Incident: 2026-03-06, 2026-03-09)
# On every boot: if dmPolicy is "pairing" or absent, force to "disabled".
# "disabled" = silently block DMs. Correct policy (allowlist) is pushed by ObeGee.
if node -e "
  try {
    const fs = require('fs');
    const cfg = JSON.parse(fs.readFileSync('${OC_CONFIG_FILE}', 'utf8'));
    const dm = cfg?.channels?.whatsapp?.dmPolicy;
    if (!dm || dm === 'pairing') { process.exit(1); }
  } catch(e) { process.exit(1); }
" 2>/dev/null; then
  echo "[Entrypoint] dmPolicy check: OK (dmPolicy is safe)"
else
  echo "[Entrypoint] SAFETY: dmPolicy is 'pairing' or missing — forcing to 'disabled' to prevent contact spam."
  node -e "
    const fs = require('fs');
    try {
      const cfg = JSON.parse(fs.readFileSync('${OC_CONFIG_FILE}', 'utf8'));
      cfg.channels = cfg.channels || {};
      cfg.channels.whatsapp = cfg.channels.whatsapp || {};
      if (!cfg.channels.whatsapp.dmPolicy || cfg.channels.whatsapp.dmPolicy === 'pairing') {
        cfg.channels.whatsapp.dmPolicy = 'disabled';
        delete cfg.channels.whatsapp.enabled;
        delete cfg.channels.whatsapp.pairedAt;
      }
      fs.writeFileSync('${OC_CONFIG_FILE}', JSON.stringify(cfg, null, 2));
      console.log('[Entrypoint] dmPolicy set to disabled');
    } catch(e) { console.error('[Entrypoint] Safety guard failed:', e.message); }
  " 2>/dev/null || true
fi

# ── Ensure Baileys symlink for wa_ds_extractor.mjs ───────────────────────────
mkdir -p /app/node_modules/@whiskeysockets 2>/dev/null || true
ln -sf /usr/local/lib/node_modules/openclaw/node_modules/@whiskeysockets/baileys \
       /app/node_modules/@whiskeysockets/baileys 2>/dev/null || true

# ── Start DS service with auto-restart loop + exponential backoff ─────────────
(
  sleep 5
  RESTART_COUNT=0
  while true; do
    RESTART_COUNT=$((RESTART_COUNT + 1))
    echo "[Entrypoint] Starting DS service (run #${RESTART_COUNT}) on port ${DS_PORT} for tenant ${TENANT_ID:-unknown}..."
    node --max-old-space-size=512 /app/tenant_ds_service.mjs
    EXIT_CODE=$?
    BACKOFF=$((5 * RESTART_COUNT))
    if [ "$BACKOFF" -gt 60 ]; then BACKOFF=60; fi
    echo "[Entrypoint] DS service exited (code=${EXIT_CODE}). Restart #${RESTART_COUNT} in ${BACKOFF}s..."
    sleep "$BACKOFF"
  done
) &
DS_PID=$!

# ── Start embedded MyndLens channel service with auto-restart loop ────────────
(
  sleep 25
  RESTART_COUNT=0
  while true; do
    RESTART_COUNT=$((RESTART_COUNT + 1))
    echo "[Entrypoint] Starting embedded channel service (run #${RESTART_COUNT}) for tenant ${TENANT_ID:-unknown}..."
    TENANT_INDEX=${TENANT_INDEX:-2} \
    OC_GATEWAY_URL="ws://localhost:18789" \
    OC_GATEWAY_HTTP_URL="http://localhost:18789" \
    OC_CONFIG_PATH="${OC_CONFIG_FILE}" \
    OC_WORKSPACE_BASE="${OC_CONFIG_DIR}/workspace" \
    OPENCLAW_GATEWAY_TOKEN="${OPENCLAW_GATEWAY_TOKEN}" \
    MYNDLENS_API_KEY="${MYNDLENS_API_KEY:-myndlens_secret_2026}" \
    node /app/myndlens_channel_service.js
    EXIT_CODE=$?
    BACKOFF=$((5 * RESTART_COUNT))
    if [ "$BACKOFF" -gt 60 ]; then BACKOFF=60; fi
    echo "[Entrypoint] Channel service exited (code=${EXIT_CODE}). Restart #${RESTART_COUNT} in ${BACKOFF}s..."
    sleep "$BACKOFF"
  done
) &
CHANNEL_PID=$!

# ── Graceful shutdown handler ─────────────────────────────────────────────────
cleanup() {
  echo "[Entrypoint] Received shutdown signal. Stopping background services (DS PID=$DS_PID, CHANNEL PID=$CHANNEL_PID)..."
  kill "$DS_PID" 2>/dev/null || true
  kill "$CHANNEL_PID" 2>/dev/null || true
  wait "$DS_PID" 2>/dev/null || true
  wait "$CHANNEL_PID" 2>/dev/null || true
  echo "[Entrypoint] Shutdown complete."
}
trap cleanup SIGTERM SIGINT

# ── Start OpenClaw gateway in foreground ──────────────────────────────────────
echo "[Entrypoint] Starting OpenClaw gateway..."

# Auto-approve pending device requests after gateway starts (Issue 8 fix)
(
  sleep 90  # Wait for gateway to be fully ready
  echo "[Entrypoint] Checking for pending device approvals..."
  PENDING=$(node /app/openclaw.mjs devices list --json 2>/dev/null | node -e "
    let d=''; process.stdin.on('data',c=>d+=c); process.stdin.on('end',()=>{
      try { const r=JSON.parse(d); const p=r.pending||[];
        if(p.length>0){console.log(p[0].requestId);} } catch(e){}
    });" 2>/dev/null)
  if [ -n "$PENDING" ]; then
    echo "[Entrypoint] Auto-approving device: $PENDING"
    node /app/openclaw.mjs devices approve "$PENDING" 2>/dev/null || true
  else
    echo "[Entrypoint] No pending device approvals."
  fi
) &

exec node /app/openclaw.mjs gateway --bind lan
