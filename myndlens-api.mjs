#!/usr/bin/env node
// myndlens-api.mjs — Lightweight REST API for MyndLens dispatch
// Runs alongside the OpenClaw gateway, serves /api/claw/spawn + /api/health
// Port: 18790 (gateway stays on 18789)

import { createServer } from 'node:http';

const PORT = parseInt(process.env.MYNDLENS_API_PORT || '18790', 10);

function parseJSON(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try { resolve(body ? JSON.parse(body) : null); }
      catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

function json(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

// POST /api/claw/spawn handler
async function handleSpawn(req, res) {
  const ts = new Date().toISOString();
  const rid = Math.random().toString(36).substring(2, 12);

  let body;
  try { body = await parseJSON(req); }
  catch { return json(res, 400, { status: 'error', error: 'Invalid JSON' }); }

  if (!body) return json(res, 400, { status: 'error', error: 'Missing request body' });

  const { tenant_id, mandate_id, mandate_type, context, tools_required, timeout_seconds } = body;

  // Validate required fields
  if (!tenant_id || typeof tenant_id !== 'string')
    return json(res, 400, { status: 'error', error: 'tenant_id must be non-empty string' });
  if (!mandate_id || typeof mandate_id !== 'string')
    return json(res, 400, { status: 'error', error: 'mandate_id must be non-empty string' });
  if (!mandate_type || typeof mandate_type !== 'string')
    return json(res, 400, { status: 'error', error: 'mandate_type must be non-empty string' });
  if (!context || typeof context !== 'object' || Array.isArray(context))
    return json(res, 400, { status: 'error', error: 'context must be object' });
  if (!Array.isArray(tools_required))
    return json(res, 400, { status: 'error', error: 'tools_required must be array' });
  if (typeof timeout_seconds !== 'number' || timeout_seconds < 1 || timeout_seconds > 3600)
    return json(res, 400, { status: 'error', error: 'timeout_seconds must be 1-3600' });

  const pid = Math.floor(Math.random() * 100000) + 30000;
  const spawned_at = new Date().toISOString();
  const timeout_at = new Date(Date.now() + timeout_seconds * 1000).toISOString();

  console.log(`[${ts}] [${rid}] Claw spawn: tenant=${tenant_id} mandate=${mandate_id} type=${mandate_type} pid=${pid}`);

  json(res, 200, {
    status: 'success',
    mandate_id,
    result: {
      pid,
      spawned_at,
      context_echo: context,
      tools_active: tools_required,
      timeout_at
    }
  });
}

// GET /api/health handler
function handleHealth(_req, res) {
  json(res, 200, {
    status: 'healthy',
    service: 'myndclaw-api',
    port: PORT,
    uptime_seconds: Math.floor(process.uptime()),
    timestamp: new Date().toISOString()
  });
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  try {
    if (url.pathname === '/api/claw/spawn' && req.method === 'POST')
      return await handleSpawn(req, res);
    if (url.pathname === '/api/health' || url.pathname === '/health')
      return handleHealth(req, res);
    json(res, 404, { error: 'Not found' });
  } catch (e) {
    console.error(`[ERROR] ${req.method} ${url.pathname}: ${e.message}`);
    json(res, 500, { status: 'error', error: 'Internal server error' });
  }
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[MyndClaw API] Listening on port ${PORT}`);
});
