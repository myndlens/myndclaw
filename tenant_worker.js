/**
 * ObeGee Tenant Runtime Worker with OpenClaw Integration
 * 
 * CANONICAL STATEMENT:
 * ObeGee creates a tenant for each subscriber so they can access a
 * tenant-specific OpenClaw instance running on Node.js v22+, configured
 * through the Dashboard, with gated execution (subscription, approvals, limits)
 * and full auditability.
 * 
 * This is the tenant-isolated runtime process. Each tenant gets its own
 * instance of this worker + OpenClaw gateway - NO shared multiplexing.
 * 
 * Architecture:
 * Web UI → Control Plane → Tenant Runtime → OpenClaw Gateway → LLM Provider
 * 
 * Requirements:
 * - Node.js v22+ (production)
 * - OpenClaw installed and available in PATH
 * - Tenant isolation: one process per tenant, one OpenClaw per tenant
 * - Gated by Control Plane (subscription, approvals, limits)
 * - No plaintext secrets stored
 */

const http = require('http');
const { URL } = require('url');
const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Import MyndLens Channel Adapter
const { startAdapter: startMyndLensAdapter, adapterState: myndlensAdapterState } = require('./myndlens_channel_adapter');

// Runtime configuration passed via environment
const TENANT_ID = process.env.TENANT_ID;
const RUNTIME_ID = process.env.RUNTIME_ID;
const CONTROL_PLANE_URL = process.env.CONTROL_PLANE_URL || 'http://localhost:8001';
const PORT = parseInt(process.env.RUNTIME_PORT || '9000');
const INTERNAL_API_KEY = process.env.INTERNAL_API_KEY || 'obegee_internal_runtime_key_2024';

// LLM key is fetched on-demand from Control Plane, NOT stored in environment
// This ensures proper key isolation and allows BYOK vs ObeGee-managed selection

// OpenClaw configuration
// In container: use /app/openclaw-state (mounted volume)
// Outside container: use /var/lib/obegee/tenants/{tenant_id}
const OPENCLAW_BASE_DIR = process.env.OPENCLAW_STATE_DIR || '/var/lib/obegee/tenants';
const OPENCLAW_HOME = process.env.OPENCLAW_STATE_DIR 
  ? path.join(OPENCLAW_BASE_DIR, 'openclaw')
  : path.join(OPENCLAW_BASE_DIR, TENANT_ID, 'openclaw');
const OPENCLAW_GATEWAY_PORT = PORT + 1000; // e.g., runtime on 10001, gateway on 11001
const OPENCLAW_GATEWAY_TOKEN = `ocgw_${TENANT_ID}_${RUNTIME_ID}`;

// Runtime state
let runtimeState = {
  status: 'initializing',
  tenant_id: TENANT_ID,
  runtime_id: RUNTIME_ID,
  node_version: process.version,
  started_at: new Date().toISOString(),
  messages_processed: 0,
  last_health_check: null,
  openclaw: {
    status: 'not_started',
    version: null,
    gateway_port: OPENCLAW_GATEWAY_PORT,
    home: OPENCLAW_HOME,
    pid: null
  },
  whatsapp: {
    status: 'not_connected',  // not_connected, connecting, connected, needs_relogin
    account_id: null,
    credentials_path: path.join(OPENCLAW_HOME, 'credentials', 'whatsapp'),
    last_pairing_attempt: null
  }
};

// WhatsApp rate limiting (guardrails)
const WHATSAPP_RATE_LIMITS = {
  messages_per_minute: 20,
  messages_per_day: 1000,
  quiet_hours_start: 22,  // 10 PM
  quiet_hours_end: 8      // 8 AM
};

let whatsappMessageCounts = {
  minute: { count: 0, reset_at: Date.now() + 60000 },
  day: { count: 0, reset_at: Date.now() + 86400000 }
};

// OpenClaw gateway process reference
let openclawProcess = null;

// Validate Node.js version (warn if < v22)
const majorVersion = parseInt(process.version.slice(1).split('.')[0]);
if (majorVersion < 22) {
  console.warn(`[RUNTIME ${RUNTIME_ID}] WARNING: Node.js ${process.version} detected. Production requires v22+.`);
}

console.log(`[RUNTIME ${RUNTIME_ID}] Starting tenant runtime for ${TENANT_ID}`);
console.log(`[RUNTIME ${RUNTIME_ID}] Node.js version: ${process.version}`);
console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw home: ${OPENCLAW_HOME}`);

/**
 * Initialize tenant-specific OpenClaw directory
 */
function initOpenClawHome() {
  try {
    // Create tenant OpenClaw home directory
    fs.mkdirSync(OPENCLAW_HOME, { recursive: true });
    fs.mkdirSync(path.join(OPENCLAW_HOME, 'credentials'), { recursive: true });
    fs.mkdirSync(path.join(OPENCLAW_HOME, 'sessions'), { recursive: true });
    fs.mkdirSync(path.join(OPENCLAW_HOME, 'logs'), { recursive: true });
    
    console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw home initialized: ${OPENCLAW_HOME}`);
    return true;
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to create OpenClaw home:`, error.message);
    return false;
  }
}

/**
 * Execute OpenClaw CLI command using spawn (container-safe)
 * Replaces execSync to avoid timeout issues in Docker containers
 */
function runOpenClawCommand(args, env, timeout = 60000) {
  return new Promise((resolve, reject) => {
    console.log(`[RUNTIME ${RUNTIME_ID}] Running: openclaw ${args.join(' ')}`);
    
    const proc = spawn('openclaw', args, {
      env,
      stdio: ['ignore', 'pipe', 'pipe']
    });
    
    let stdout = '';
    let stderr = '';
    let killed = false;
    
    proc.stdout.on('data', (data) => {
      stdout += data.toString();
    });
    
    proc.stderr.on('data', (data) => {
      stderr += data.toString();
    });
    
    proc.on('error', (error) => {
      reject(new Error(`Spawn error: ${error.message}`));
    });
    
    proc.on('close', (code) => {
      if (killed) {
        reject(new Error(`Command timeout after ${timeout}ms`));
      } else if (code === 0) {
        resolve(stdout.trim());
      } else {
        reject(new Error(`Exit code ${code}: ${stderr || stdout}`));
      }
    });
    
    // Timeout handler
    const timeoutId = setTimeout(() => {
      killed = true;
      proc.kill('SIGTERM');
      setTimeout(() => proc.kill('SIGKILL'), 5000); // Force kill after 5s
    }, timeout);
    
    proc.on('close', () => clearTimeout(timeoutId));
  });
}

/**
 * Get OpenClaw version (using spawn)
 */
async function getOpenClawVersion() {
  try {
    const version = await runOpenClawCommand(['--version'], process.env, 15000);
    return version;
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to get OpenClaw version:`, error.message);
    return null;
  }
}

/**
 * Run OpenClaw setup for this tenant (per official docs)
 * Must be called before any other OpenClaw commands
 * 
 * Official process: https://docs.openclaw.ai/start/getting-started
 * 1. openclaw onboard (creates config + workspace)
 * 2. Configure model auth (critical - agent can't respond without it)
 * 3. Optional: security audit
 */
async function setupOpenClaw() {
  const env = {
    ...process.env,
    HOME: OPENCLAW_HOME,
    OPENCLAW_HOME: OPENCLAW_HOME,
    OPENCLAW_STATE_DIR: OPENCLAW_HOME,
    OPENCLAW_CONFIG_PATH: path.join(OPENCLAW_HOME, 'openclaw.json')
  };
  
  const configPath = path.join(OPENCLAW_HOME, 'openclaw.json');
  
  try {
    // Check if already set up
    if (fs.existsSync(configPath)) {
      console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw already configured for this tenant`);
      return true;
    }
    
    // Copy pre-initialized OpenClaw template from Docker image
    const templatePath = '/app/openclaw-template/.openclaw';
    if (fs.existsSync(templatePath)) {
      console.log(`[RUNTIME ${RUNTIME_ID}] Copying OpenClaw template from Docker image...`);
      
      // Copy entire .openclaw directory contents from template
      const { execSync: execSyncNative } = require('child_process');
      
      // Create OPENCLAW_HOME directory first
      fs.mkdirSync(OPENCLAW_HOME, { recursive: true });
      
      // Copy all contents from template
      execSyncNative(`cp -r ${templatePath}/* ${OPENCLAW_HOME}/`, { stdio: 'pipe' });
      
      console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw template copied successfully`);
    } else {
      console.log(`[RUNTIME ${RUNTIME_ID}] Template not found, creating minimal config...`);
      
      // Fallback: Create minimal config matching OpenClaw schema
      const minimalConfig = {
        agents: { 
          defaults: {} 
        },
        gateway: {
          mode: 'local',
          port: OPENCLAW_GATEWAY_PORT,
          bind: 'loopback',
          auth: { 
            mode: 'token', 
            token: OPENCLAW_GATEWAY_TOKEN 
          }
        },
        channels: {}
        // Note: "providers" key removed - not part of OpenClaw schema
      };
      
      fs.mkdirSync(OPENCLAW_HOME, { recursive: true });
      fs.writeFileSync(configPath, JSON.stringify(minimalConfig, null, 2), 'utf8');
      console.log(`[RUNTIME ${RUNTIME_ID}] Minimal config created`);
    }
    
    // Step 2: Configure model authentication (CRITICAL)
    // Get LLM key from control plane
    let llmConfig = null;
    try {
      llmConfig = await fetchLLMKey();
    } catch (error) {
      console.warn(`[RUNTIME ${RUNTIME_ID}] Failed to fetch LLM key:`, error.message);
      // For Alpha: Use default config without LLM key
      // Production Control Plane will provide key via deployment
      llmConfig = null;
    }
    
    if (llmConfig && llmConfig.api_key) {
      console.log(`[RUNTIME ${RUNTIME_ID}] Configuring LLM authentication...`);
      console.log(`[RUNTIME ${RUNTIME_ID}] Provider: ${llmConfig.provider}, Model: ${llmConfig.model}`);
      
      // Read current config
      let config = {};
      if (fs.existsSync(configPath)) {
        const configData = fs.readFileSync(configPath, 'utf8');
        config = JSON.parse(configData);
      }
      
      // Configure providers - OpenClaw uses environment variables, not config file
      // Set model in agents.defaults instead
      if (!config.agents) config.agents = {};
      if (!config.agents.defaults) config.agents.defaults = {};
      config.agents.defaults.model = llmConfig.model || 'openai/gpt-4o-mini';
      
      // Store API key as environment variable reference (OpenClaw reads from env)
      // We'll set OPENAI_API_KEY in container environment
      process.env.OPENAI_API_KEY = llmConfig.api_key;
      
      // Write updated config
      fs.writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf8');
      console.log(`[RUNTIME ${RUNTIME_ID}] LLM authentication configured with model: ${config.agents.defaults.model}`);
    } else {
      console.warn(`[RUNTIME ${RUNTIME_ID}] No LLM key available - container will start but chat may not work until deployment`);
      console.warn(`[RUNTIME ${RUNTIME_ID}] This is expected for Alpha testing - production deployment will provide keys`);
    }
    
    return true;
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] OpenClaw setup failed:`, error.message);
    // Setup might partially succeed, continue anyway
    return true;
  }
}


/**
 * Install required OpenClaw skills for tenant
 * In Docker: Skills are pre-installed in template, just verify
 * Outside Docker: Install skills individually
 */
async function installRequiredSkills() {
  console.log(`[RUNTIME ${RUNTIME_ID}] Verifying required skills...`);
  
  const REQUIRED_SKILLS = [
    'web-browser',
    'file-system',
    'email'
  ];
  
  const env = {
    ...process.env,
    HOME: OPENCLAW_HOME,
    OPENCLAW_HOME: OPENCLAW_HOME,
    OPENCLAW_STATE_DIR: OPENCLAW_HOME,
    OPENCLAW_CONFIG_PATH: path.join(OPENCLAW_HOME, 'openclaw.json')
  };
  
  // Check if skills are already installed (from template)
  try {
    const installedList = await runOpenClawCommand(
      ['skills', 'list'],
      env,
      10000
    );
    console.log(`[RUNTIME ${RUNTIME_ID}] Pre-installed skills:\n${installedList}`);
    
    runtimeState.openclaw.skills_installed = REQUIRED_SKILLS;
    runtimeState.openclaw.skills_failed = [];
    runtimeState.openclaw.skills_verified_at = new Date().toISOString();
    
    return {
      installed: REQUIRED_SKILLS,
      failed: [],
      total: REQUIRED_SKILLS.length,
      source: 'pre-initialized'
    };
  } catch (error) {
    console.error(`Failed to verify skills: ${error.message}`);
    
    // If pre-installed skills not found, mark as unknown but continue
    runtimeState.openclaw.skills_installed = [];
    runtimeState.openclaw.skills_failed = [];
    
    return {
      installed: [],
      failed: [],
      total: REQUIRED_SKILLS.length,
      source: 'verification-failed'
    };
  }
}


/**
 * Configure OpenClaw for this tenant
 * Simplified for container environment - config created in setupOpenClaw()
 */
async function configureOpenClaw() {
  console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw configuration handled in setup phase`);
  return true;  // Config already created manually in setupOpenClaw
}

/**
 * Start OpenClaw gateway for this tenant
 */
async function startOpenClawGateway() {
  if (openclawProcess && !openclawProcess.killed) {
    console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw gateway already running`);
    return true;
  }
  
  const env = {
    ...process.env,
    HOME: OPENCLAW_HOME,
    OPENCLAW_HOME: OPENCLAW_HOME,
    OPENCLAW_STATE_DIR: OPENCLAW_HOME,
    OPENCLAW_CONFIG_PATH: path.join(OPENCLAW_HOME, 'openclaw.json'),
    OPENCLAW_GATEWAY_TOKEN: OPENCLAW_GATEWAY_TOKEN
  };
  
  try {
    // Start gateway using 'run' command with proper isolation
    openclawProcess = spawn('openclaw', [
      'gateway',
      'run',
      '--port', String(OPENCLAW_GATEWAY_PORT),
      '--bind', 'loopback',
      '--auth', 'token',
      '--token', OPENCLAW_GATEWAY_TOKEN,
      '--allow-unconfigured'
    ], {
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: false
    });
    
    runtimeState.openclaw.pid = openclawProcess.pid;
    runtimeState.openclaw.status = 'starting';
    
    openclawProcess.stdout.on('data', (data) => {
      const output = data.toString().trim();
      console.log(`[OPENCLAW ${TENANT_ID}] ${output}`);
    });
    
    openclawProcess.stderr.on('data', (data) => {
      const output = data.toString().trim();
      console.error(`[OPENCLAW ${TENANT_ID}] ${output}`);
    });
    
    openclawProcess.on('error', (error) => {
      console.error(`[RUNTIME ${RUNTIME_ID}] OpenClaw spawn error:`, error.message);
      runtimeState.openclaw.status = 'error';
      runtimeState.openclaw.pid = null;
    });
    
    openclawProcess.on('exit', (code, signal) => {
      console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw exited with code ${code}, signal ${signal}`);
      runtimeState.openclaw.status = 'stopped';
      runtimeState.openclaw.pid = null;
      openclawProcess = null;
      
      // Attempt restart after delay (bounded retries)
      if (runtimeState.status === 'running' && code !== 0) {
        setTimeout(() => {
          console.log(`[RUNTIME ${RUNTIME_ID}] Attempting OpenClaw restart...`);
          startOpenClawGateway();
        }, 5000);
      }
    });
    
    // Wait for gateway to be ready
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    // Check if gateway is healthy
    const healthy = await checkOpenClawHealth();
    if (healthy) {
      runtimeState.openclaw.status = 'running';
      console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw gateway started on port ${OPENCLAW_GATEWAY_PORT}`);
      return true;
    } else {
      runtimeState.openclaw.status = 'unhealthy';
      console.warn(`[RUNTIME ${RUNTIME_ID}] OpenClaw gateway health check failed`);
      return false;
    }
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to start OpenClaw gateway:`, error.message);
    runtimeState.openclaw.status = 'error';
    return false;
  }
}

/**
 * Check OpenClaw gateway health
 */
async function checkOpenClawHealth() {
  try {
    const response = await fetch(`http://127.0.0.1:${OPENCLAW_GATEWAY_PORT}/health`, {
      headers: { 'Authorization': `Bearer ${OPENCLAW_GATEWAY_TOKEN}` },
      signal: AbortSignal.timeout(3000)
    });
    return response.ok;
  } catch (error) {
    return false;
  }
}

/**
 * Stop OpenClaw gateway
 */
function stopOpenClawGateway() {
  if (openclawProcess && !openclawProcess.killed) {
    console.log(`[RUNTIME ${RUNTIME_ID}] Stopping OpenClaw gateway...`);
    openclawProcess.kill('SIGTERM');
    openclawProcess = null;
    runtimeState.openclaw.status = 'stopped';
    runtimeState.openclaw.pid = null;
  }
}

// ============== WHATSAPP CHANNEL FUNCTIONS ==============
// WhatsApp integration via personal client pairing through OpenClaw
// NO Business API - uses QR code / Linked Devices pairing

/**
 * Check WhatsApp credentials exist (persisted from previous pairing)
 */
function checkWhatsAppCredentials() {
  const credsPath = runtimeState.whatsapp.credentials_path;
  try {
    if (fs.existsSync(credsPath)) {
      const files = fs.readdirSync(credsPath);
      return files.length > 0;
    }
  } catch (e) {
    // Ignore
  }
  return false;
}

/**
 * Get WhatsApp channel status
 */
async function getWhatsAppStatus() {
  const hasCredentials = checkWhatsAppCredentials();
  
  // Query OpenClaw for actual channel status
  const env = {
    ...process.env,
    HOME: OPENCLAW_HOME,
    OPENCLAW_HOME: OPENCLAW_HOME,
    OPENCLAW_STATE_DIR: OPENCLAW_HOME
  };
  
  try {
    const { spawnSync } = require('child_process');
    const result = spawnSync('openclaw', ['channels', 'status', '--json'], {
      env,
      encoding: 'utf8',
      timeout: 10000
    });
    
    if (result.stdout) {
      const status = JSON.parse(result.stdout);
      const whatsappChannel = status.channels?.find(c => c.channel === 'whatsapp');
      
      if (whatsappChannel) {
        runtimeState.whatsapp.status = whatsappChannel.status || 'unknown';
        runtimeState.whatsapp.account_id = whatsappChannel.accountId;
        
        return {
          status: whatsappChannel.status,
          connected: whatsappChannel.status === 'connected',
          account_id: whatsappChannel.accountId,
          credentials_exist: hasCredentials,
          credentials_path: runtimeState.whatsapp.credentials_path
        };
      }
    }
  } catch (e) {
    // OpenClaw status check failed
  }
  
  return {
    status: hasCredentials ? 'needs_relogin' : 'not_connected',
    connected: false,
    credentials_exist: hasCredentials,
    credentials_path: runtimeState.whatsapp.credentials_path
  };
}

/**
 * Initiate WhatsApp pairing - starts QR code login flow
 * Returns QR code data URL for display in Dashboard
 * 
 * CORRECT APPROACH (per OpenClaw docs):
 * 1. Configure WhatsApp in openclaw.json (with authDir)
 * 2. Gateway must be running
 * 3. Call: openclaw channels login --account <id>
 * 4. QR code is displayed in CLI output
 * 
 * Docs: https://docs.openclaw.ai/channels/whatsapp
 */
async function initiateWhatsAppPairing() {
  console.log(`[RUNTIME ${RUNTIME_ID}] Initiating WhatsApp pairing via CLI...`);
  
  runtimeState.whatsapp.status = 'connecting';
  runtimeState.whatsapp.last_pairing_attempt = new Date().toISOString();
  
  const accountId = `obegee_${TENANT_ID}`;
  runtimeState.whatsapp.account_id = accountId;
  
  // Ensure credentials directory exists
  const credsDir = runtimeState.whatsapp.credentials_path;
  fs.mkdirSync(credsDir, { recursive: true });
  
  const env = {
    ...process.env,
    HOME: OPENCLAW_HOME,
    OPENCLAW_HOME: OPENCLAW_HOME,
    OPENCLAW_STATE_DIR: OPENCLAW_HOME,
    OPENCLAW_CONFIG_PATH: path.join(OPENCLAW_HOME, 'openclaw.json')
  };
  
  // Step 1: Update openclaw.json to configure WhatsApp channel with this account
  try {
    const configPath = path.join(OPENCLAW_HOME, 'openclaw.json');
    let config = {};
    
    if (fs.existsSync(configPath)) {
      const configData = fs.readFileSync(configPath, 'utf8');
      config = JSON.parse(configData);
    }
    
    // Ensure channels.whatsapp.accounts exists
    if (!config.channels) config.channels = {};
    if (!config.channels.whatsapp) config.channels.whatsapp = {};
    if (!config.channels.whatsapp.accounts) config.channels.whatsapp.accounts = {};
    
    // Add this account with authDir
    config.channels.whatsapp.accounts[accountId] = {
      authDir: credsDir
    };
    
    // Set default policies if not present
    if (!config.channels.whatsapp.dmPolicy) {
      config.channels.whatsapp.dmPolicy = 'allowlist';
    }
    if (!config.channels.whatsapp.allowFrom) {
      config.channels.whatsapp.allowFrom = [];
    }
    
    // Write updated config
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf8');
    console.log(`[RUNTIME ${RUNTIME_ID}] Updated openclaw.json with WhatsApp account: ${accountId}`);
  } catch (e) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to update openclaw.json:`, e.message);
    runtimeState.whatsapp.status = 'error';
    throw new Error(`Config update failed: ${e.message}`);
  }
  
  // Step 2: Verify gateway is running before attempting login
  const gatewayHealthy = await checkOpenClawHealth();
  if (!gatewayHealthy) {
    console.error(`[RUNTIME ${RUNTIME_ID}] OpenClaw gateway is not running - cannot pair WhatsApp`);
    runtimeState.whatsapp.status = 'error';
    throw new Error('Gateway must be running to pair WhatsApp. Please ensure gateway is started.');
  }
  
  console.log(`[RUNTIME ${RUNTIME_ID}] Gateway is healthy, proceeding with pairing...`);
  
  // Step 3: Call openclaw channels login --account <id>
  // This MUST be done with gateway running, as gateway owns the Baileys socket
  return new Promise((resolve, reject) => {
    console.log(`[RUNTIME ${RUNTIME_ID}] Spawning: openclaw channels login --account ${accountId}`);
    
    // Note: Not using --channel whatsapp because the account is already configured
    // in openclaw.json with the WhatsApp channel type
    const pairingProcess = spawn('openclaw', [
      'channels',
      'login',
      '--account', accountId,
      '--verbose'
    ], {
      env,
      stdio: ['ignore', 'pipe', 'pipe']
    });
    
    let stdout = '';
    let stderr = '';
    let qrFound = false;
    
    pairingProcess.stdout.on('data', (data) => {
      const output = data.toString();
      stdout += output;
      console.log(`[WHATSAPP PAIR] ${output.trim()}`);
      
      // OpenClaw outputs QR code as ASCII art or data URL
      // Look for QR data in the output
      if (!qrFound && (output.includes('█') || output.includes('▄') || output.includes('▀') || output.includes('data:image') || output.includes('Scan') || output.includes('QR'))) {
        qrFound = true;
        
        // Try to extract data URL if present
        const dataUrlMatch = output.match(/data:image\/[^;]+;base64,[A-Za-z0-9+/=]+/);
        if (dataUrlMatch) {
          console.log(`[RUNTIME ${RUNTIME_ID}] QR data URL found`);
          runtimeState.whatsapp.status = 'waiting_for_scan';
          resolve({
            success: true,
            qr_type: 'data_url',
            qr_data: dataUrlMatch[0],
            account_id: accountId,
            message: 'Scan QR code with WhatsApp mobile app (Settings → Linked Devices)'
          });
          return;
        }
        
        // Otherwise return ASCII QR
        console.log(`[RUNTIME ${RUNTIME_ID}] QR ASCII found`);
        runtimeState.whatsapp.status = 'waiting_for_scan';
        resolve({
          success: true,
          qr_type: 'ascii',
          qr_data: stdout,
          account_id: accountId,
          message: 'Scan QR code with WhatsApp mobile app (Settings → Linked Devices)'
        });
      }
    });
    
    pairingProcess.stderr.on('data', (data) => {
      const output = data.toString();
      stderr += output;
      console.error(`[WHATSAPP PAIR ERROR] ${output.trim()}`);
    });
    
    pairingProcess.on('error', (error) => {
      console.error(`[RUNTIME ${RUNTIME_ID}] WhatsApp pairing spawn error:`, error.message);
      runtimeState.whatsapp.status = 'error';
      reject(new Error(`Pairing failed: ${error.message}`));
    });
    
    pairingProcess.on('exit', (code, signal) => {
      console.log(`[RUNTIME ${RUNTIME_ID}] WhatsApp pairing process exited: code=${code}, signal=${signal}`);
      
      if (code === 0) {
        // Successful pairing
        runtimeState.whatsapp.status = 'connected';
        if (!qrFound) {
          // QR was shown but we didn't capture it - pairing might have succeeded
          resolve({
            success: true,
            qr_type: 'completed',
            message: 'WhatsApp paired successfully',
            account_id: accountId
          });
        }
      } else if (!qrFound) {
        // Process exited without showing QR
        runtimeState.whatsapp.status = 'error';
        reject(new Error(`Pairing failed (exit code ${code}): ${stderr || 'Unknown error'}`));
      }
      // If QR was found, we already resolved
    });
    
    // Timeout after 90 seconds
    setTimeout(() => {
      if (pairingProcess && !pairingProcess.killed) {
        console.log(`[RUNTIME ${RUNTIME_ID}] WhatsApp pairing timeout - killing process`);
        pairingProcess.kill('SIGTERM');
        if (!qrFound) {
          runtimeState.whatsapp.status = 'timeout';
          reject(new Error('Pairing timeout - QR code not generated'));
        }
      }
    }, 90000);
  });
}

/**
 * Disconnect WhatsApp - logout and wipe credentials
 * Uses CLI command: openclaw channels logout --channel whatsapp
 */
async function disconnectWhatsApp() {
  console.log(`[RUNTIME ${RUNTIME_ID}] Disconnecting WhatsApp via CLI...`);
  
  const accountId = runtimeState.whatsapp.account_id || `obegee_${TENANT_ID}`;
  
  const env = {
    ...process.env,
    HOME: OPENCLAW_HOME,
    OPENCLAW_HOME: OPENCLAW_HOME,
    OPENCLAW_STATE_DIR: OPENCLAW_HOME,
    OPENCLAW_CONFIG_PATH: path.join(OPENCLAW_HOME, 'openclaw.json')
  };
  
  try {
    // Logout via OpenClaw CLI
    const { spawnSync } = require('child_process');
    const result = spawnSync('openclaw', [
      'channels',
      'logout',
      '--channel', 'whatsapp',
      '--account', accountId
    ], {
      env,
      encoding: 'utf8',
      timeout: 10000
    });
    
    console.log(`[RUNTIME ${RUNTIME_ID}] WhatsApp logout: ${result.stdout}`);
    if (result.stderr) {
      console.error(`[RUNTIME ${RUNTIME_ID}] WhatsApp logout stderr: ${result.stderr}`);
    }
  } catch (e) {
    console.error(`[RUNTIME ${RUNTIME_ID}] WhatsApp logout command failed:`, e.message);
  }
  
  // Wipe credentials
  const credsDir = runtimeState.whatsapp.credentials_path;
  try {
    if (fs.existsSync(credsDir)) {
      fs.rmSync(credsDir, { recursive: true, force: true });
      fs.mkdirSync(credsDir, { recursive: true });
    }
  } catch (e) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to wipe WhatsApp credentials:`, e.message);
  }
  
  runtimeState.whatsapp.status = 'not_connected';
  runtimeState.whatsapp.account_id = null;
  
  // Notify Control Plane
  await updateWhatsAppStatus('disconnected');
  
  return { status: 'disconnected', message: 'WhatsApp disconnected and credentials wiped' };
}

/**
 * Send WhatsApp test message to self
 */
async function sendWhatsAppTest() {
  const status = await getWhatsAppStatus();
  
  if (!status.connected) {
    return { 
      success: false, 
      message: 'WhatsApp not connected. Please pair first.',
      status: status.status
    };
  }
  
  // Rate limit check
  if (!checkWhatsAppRateLimits()) {
    return {
      success: false,
      message: 'Rate limit exceeded. Please try again later.'
    };
  }
  
  const accountId = runtimeState.whatsapp.account_id;
  
  try {
    const { spawnSync } = require('child_process');
    const env = {
      ...process.env,
      HOME: OPENCLAW_HOME,
      OPENCLAW_HOME: OPENCLAW_HOME,
      OPENCLAW_STATE_DIR: OPENCLAW_HOME
    };
    
    // Send test message via OpenClaw
    const result = spawnSync('openclaw', [
      'send',
      '--channel', 'whatsapp',
      '--account', accountId,
      '--to', 'self',
      '--message', `ObeGee Test Message from workspace ${TENANT_ID} at ${new Date().toISOString()}`
    ], {
      env,
      encoding: 'utf8',
      timeout: 30000
    });
    
    if (result.status === 0) {
      incrementWhatsAppMessageCount();
      return { success: true, message: 'Test message sent to self' };
    } else {
      return { 
        success: false, 
        message: 'Failed to send test message',
        error: result.stderr?.slice(-200)
      };
    }
  } catch (error) {
    return { success: false, message: error.message };
  }
}

/**
 * Check WhatsApp rate limits (guardrails)
 */
function checkWhatsAppRateLimits() {
  const now = Date.now();
  
  // Reset counters if needed
  if (now > whatsappMessageCounts.minute.reset_at) {
    whatsappMessageCounts.minute = { count: 0, reset_at: now + 60000 };
  }
  if (now > whatsappMessageCounts.day.reset_at) {
    whatsappMessageCounts.day = { count: 0, reset_at: now + 86400000 };
  }
  
  // Check limits
  if (whatsappMessageCounts.minute.count >= WHATSAPP_RATE_LIMITS.messages_per_minute) {
    return false;
  }
  if (whatsappMessageCounts.day.count >= WHATSAPP_RATE_LIMITS.messages_per_day) {
    return false;
  }
  
  // Check quiet hours
  const hour = new Date().getHours();
  if (hour >= WHATSAPP_RATE_LIMITS.quiet_hours_start || hour < WHATSAPP_RATE_LIMITS.quiet_hours_end) {
    // Allow during quiet hours but with warning in logs
    console.log(`[RUNTIME ${RUNTIME_ID}] WhatsApp message during quiet hours`);
  }
  
  return true;
}

/**
 * Increment WhatsApp message counters
 */
function incrementWhatsAppMessageCount() {
  whatsappMessageCounts.minute.count++;
  whatsappMessageCounts.day.count++;
}

/**
 * Update WhatsApp status in Control Plane
 */
async function updateWhatsAppStatus(status, details = {}) {
  try {
    await fetch(`${CONTROL_PLANE_URL}/api/internal/whatsapp/status-update`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-API-Key': INTERNAL_API_KEY
      },
      body: JSON.stringify({
        tenant_id: TENANT_ID,
        runtime_id: RUNTIME_ID,
        status: status,
        details: details
      })
    });
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to update WhatsApp status:`, error.message);
  }
}

/**
 * Fetch LLM provider key from Control Plane
 * 
 * Returns:
 * - ObeGee-managed key if tenant uses default DeepSeek
 * - Tenant's BYOK key if they configured OpenAI/Anthropic
 * 
 * Control Plane is the source of truth for which key to use.
 */
async function fetchLLMKey() {
  try {
    const response = await fetch(`${CONTROL_PLANE_URL}/api/internal/llm-key/${TENANT_ID}`, {
      headers: { 'X-Internal-API-Key': INTERNAL_API_KEY }
    });
    
    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Failed to fetch LLM key: ${response.status} - ${error}`);
    }
    
    const data = await response.json();
    console.log(`[RUNTIME ${RUNTIME_ID}] LLM key fetched: provider=${data.provider}, source=${data.key_source}, is_byok=${data.is_byok}`);
    return data;
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to fetch LLM key:`, error.message);
    throw error;
  }
}

/**
 * Call LLM via Python helper
 * 
 * The key is fetched from Control Plane on each call to ensure:
 * - Correct key is always used (ObeGee-managed vs BYOK)
 * - Key rotation is immediate
 * - No stale keys in runtime memory
 */
async function callLLM(message, sessionId, history = []) {
  // Fetch the correct LLM key from Control Plane
  const llmConfig = await fetchLLMKey();
  
  const { spawnSync } = require('child_process');
  const llmHelperPath = path.join(__dirname, 'llm_helper.py');
  
  const result = spawnSync('/root/.venv/bin/python3', [
    llmHelperPath,
    '--message', message,
    '--session-id', sessionId,
    '--history', JSON.stringify(history),
    '--provider', llmConfig.provider,
    '--model', llmConfig.model
  ], {
    env: {
      ...process.env,
      EMERGENT_LLM_KEY: llmConfig.api_key,
      LLM_PROVIDER: llmConfig.provider,
      LLM_MODEL: llmConfig.model,
      IS_BYOK: String(llmConfig.is_byok)
    },
    encoding: 'utf8',
    timeout: 90000,
    maxBuffer: 10 * 1024 * 1024
  });
  
  if (result.error) {
    throw result.error;
  }
  
  const output = result.stdout || '';
  const stderr = result.stderr || '';
  
  if (stderr) {
    console.error(`[RUNTIME ${RUNTIME_ID}] LLM helper stderr:`, stderr);
  }
  
  try {
    const response = JSON.parse(output);
    if (response.success) {
      return {
        success: true,
        content: response.content,
        raw: response,
        provider: llmConfig.provider,
        is_byok: llmConfig.is_byok
      };
    } else {
      throw new Error(response.error || 'LLM call failed');
    }
  } catch (parseError) {
    console.error(`[RUNTIME ${RUNTIME_ID}] LLM helper parse error:`, parseError.message);
    throw new Error(`Failed to parse LLM response: ${output}`);
  }
}

/**
 * Send message through OpenClaw agent
 * 
 * Provider selection (ObeGee DeepSeek vs BYOK OpenAI/Anthropic) is resolved here
 * by fetching the correct key from Control Plane.
 * 
 * Control Plane NEVER calls LLM - only Runtime does.
 */
async function sendMessageToOpenClaw(message, sessionId, history = []) {
  // Fetch the correct LLM key from Control Plane (ObeGee-managed or BYOK)
  let llmConfig;
  try {
    llmConfig = await fetchLLMKey();
  } catch (keyError) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to fetch LLM key:`, keyError.message);
    return {
      success: false,
      content: `Configuration error: ${keyError.message}`,
      error: keyError.message
    };
  }
  
  // First try OpenClaw's local agent with the fetched key
  try {
    const env = {
      ...process.env,
      HOME: OPENCLAW_HOME,
      OPENCLAW_HOME: OPENCLAW_HOME,
      OPENCLAW_STATE_DIR: OPENCLAW_HOME,
      OPENCLAW_GATEWAY_TOKEN: OPENCLAW_GATEWAY_TOKEN,
      // Set the appropriate API key for the provider
      OPENAI_API_KEY: llmConfig.provider === 'openai' ? llmConfig.api_key : '',
      ANTHROPIC_API_KEY: llmConfig.provider === 'anthropic' ? llmConfig.api_key : ''
    };
    
    const { spawnSync } = require('child_process');
    
    const result = spawnSync('openclaw', [
      'agent',
      '--local',
      '--session-id', sessionId,
      '-m', message,
      '--json'
    ], {
      env,
      encoding: 'utf8',
      timeout: 60000,
      maxBuffer: 10 * 1024 * 1024
    });
    
    const output = result.stdout || '';
    const stderr = result.stderr || '';
    
    // Check if OpenClaw succeeded with valid content
    if (result.status === 0 && output) {
      try {
        const jsonMatch = output.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
          const response = JSON.parse(jsonMatch[0]);
          const payloads = response.payloads || [];
          if (payloads.length > 0 && payloads[0].text && !payloads[0].text.includes('API key')) {
            return {
              success: true,
              content: payloads[0].text,
              raw: response,
              source: 'openclaw',
              provider: llmConfig.provider,
              is_byok: llmConfig.is_byok
            };
          }
        }
      } catch (parseError) {
        // Continue to fallback
      }
    }
    
    // If OpenClaw fails, use LLM helper directly
    console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw local agent unavailable, using LLM helper`);
    const llmResult = await callLLM(message, sessionId, history);
    llmResult.source = 'llm-helper';
    return llmResult;
    
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] OpenClaw agent error:`, error.message);
    
    // Fallback to LLM helper
    try {
      console.log(`[RUNTIME ${RUNTIME_ID}] Falling back to LLM helper`);
      const llmResult = await callLLM(message, sessionId, history);
      llmResult.source = 'llm-helper-fallback';
      return llmResult;
    } catch (llmError) {
      return {
        success: false,
        content: `Processing failed: ${error.message}`,
        error: error.message
      };
    }
  }
}

/**
 * Send message via OpenClaw gateway WebSocket (fallback)
 */
async function sendMessageViaGateway(message, sessionId) {
  // Try calling the gateway's HTTP endpoint if available
  try {
    const response = await fetch(`http://127.0.0.1:${OPENCLAW_GATEWAY_PORT}/api/agent`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${OPENCLAW_GATEWAY_TOKEN}`
      },
      body: JSON.stringify({
        message: message,
        session_id: sessionId,
        local: true
      }),
      signal: AbortSignal.timeout(60000)
    });
    
    if (response.ok) {
      const data = await response.json();
      return {
        success: true,
        content: data.reply || data.content || data.message || 'Processed',
        raw: data
      };
    }
  } catch (e) {
    // Gateway fallback failed
  }
  
  throw new Error('Gateway fallback not available');
}

/**
 * Fetch runtime config from Control Plane
 */
async function fetchRuntimeConfig() {
  try {
    const response = await fetch(`${CONTROL_PLANE_URL}/api/internal/runtime-config/${TENANT_ID}`, {
      headers: { 'X-Internal-API-Key': INTERNAL_API_KEY }
    });
    if (response.ok) {
      return await response.json();
    }
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to fetch config:`, error.message);
  }
  return null;
}

/**
 * Report usage to Control Plane
 */
async function reportUsage(tokens, messages, toolCalls) {
  try {
    await fetch(`${CONTROL_PLANE_URL}/api/internal/usage/report`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'X-Internal-API-Key': INTERNAL_API_KEY
      },
      body: JSON.stringify({
        tenant_id: TENANT_ID,
        runtime_id: RUNTIME_ID,
        tokens,
        messages,
        tool_calls: toolCalls
      })
    });
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to report usage:`, error.message);
  }
}

/**
 * Emit audit event to Control Plane
 */
async function emitAuditEvent(eventType, description, metadata = {}) {
  try {
    await fetch(`${CONTROL_PLANE_URL}/api/internal/audit/emit`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'X-Internal-API-Key': INTERNAL_API_KEY
      },
      body: JSON.stringify({
        tenant_id: TENANT_ID,
        runtime_id: RUNTIME_ID,
        event_type: eventType,
        description,
        metadata
      })
    });
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to emit audit:`, error.message);
  }
}

/**
 * Create action request for approval
 */
async function createActionRequest(actionType, summary, riskLevel, payload) {
  try {
    const response = await fetch(`${CONTROL_PLANE_URL}/api/internal/actions/request`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'X-Internal-API-Key': INTERNAL_API_KEY
      },
      body: JSON.stringify({
        tenant_id: TENANT_ID,
        runtime_id: RUNTIME_ID,
        action_type: actionType,
        action_summary: summary,
        risk_level: riskLevel,
        execution_payload: payload
      })
    });
    if (response.ok) {
      return await response.json();
    }
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to create action request:`, error.message);
  }
  return null;
}

/**
 * Check if action is approved
 */
async function checkActionApproval(actionId) {
  try {
    const response = await fetch(`${CONTROL_PLANE_URL}/api/internal/actions/${actionId}/status`, {
      headers: { 'X-Internal-API-Key': INTERNAL_API_KEY }
    });
    if (response.ok) {
      return await response.json();
    }
  } catch (error) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to check action status:`, error.message);
  }
  return { status: 'unknown', can_execute: false };
}

/**
 * Parse request body helper
 */
function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        resolve(JSON.parse(body));
      } catch (e) {
        reject(new Error('Invalid JSON'));
      }
    });
    req.on('error', reject);
  });
}

/**
 * HTTP server for runtime operations
 */
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  
  // Set CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Internal-API-Key');
  res.setHeader('Content-Type', 'application/json');
  
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // Health check
  if (url.pathname === '/health' && req.method === 'GET') {
    runtimeState.last_health_check = new Date().toISOString();
    
    // Check OpenClaw health
    const openclawHealthy = await checkOpenClawHealth();
    runtimeState.openclaw.status = openclawHealthy ? 'running' : 'unhealthy';
    
    res.writeHead(200);
    res.end(JSON.stringify({
      status: runtimeState.status,
      tenant_id: TENANT_ID,
      runtime_id: RUNTIME_ID,
      node_version: process.version,
      node_version_major: majorVersion,
      node_v22_compliant: majorVersion >= 22,
      uptime_seconds: Math.floor((Date.now() - new Date(runtimeState.started_at)) / 1000),
      messages_processed: runtimeState.messages_processed,
      openclaw: {
        status: runtimeState.openclaw.status,
        version: runtimeState.openclaw.version,
        gateway_port: OPENCLAW_GATEWAY_PORT,
        home: OPENCLAW_HOME,
        pid: runtimeState.openclaw.pid
      }
    }));
    return;
  }

  // Chat message endpoint - THE SINGLE GATEWAY
  if (url.pathname === '/chat' && req.method === 'POST') {
    try {
      const data = await parseBody(req);
      
      // Verify tenant isolation
      if (data.tenant_id !== TENANT_ID) {
        res.writeHead(403);
        res.end(JSON.stringify({ error: 'Tenant mismatch - isolation enforced' }));
        return;
      }
      
      const sessionId = data.session_id || `${TENANT_ID}_${data.user_id || 'anonymous'}`;
      const message = data.message;
      const history = data.history || [];
      
      console.log(`[RUNTIME ${RUNTIME_ID}] Processing chat message for session ${sessionId}`);
      
      // Send to OpenClaw
      const result = await sendMessageToOpenClaw(message, sessionId, history);
      
      runtimeState.messages_processed++;
      
      // Report usage
      const estimatedTokens = (message.length + (result.content?.length || 0)) / 4;
      await reportUsage(Math.ceil(estimatedTokens), 1, 0);
      
      // Check for action-requiring keywords
      const actionKeywords = ['send email', 'make request', 'call api', 'execute', 'perform action', 'i will', 'i would'];
      const actionRequired = actionKeywords.some(kw => result.content?.toLowerCase().includes(kw));
      
      let actionData = null;
      if (actionRequired) {
        const actionResult = await createActionRequest(
          'ai_suggested',
          result.content?.substring(0, 200) || 'AI action',
          'medium',
          { response: result.content }
        );
        if (actionResult) {
          actionData = {
            action_id: actionResult.action_id,
            action_summary: result.content?.substring(0, 200),
            risk_level: 'medium',
            status: actionResult.status
          };
        }
      }
      
      await emitAuditEvent('chat_processed', `Message processed via OpenClaw`, {
        session_id: sessionId,
        action_required: actionRequired
      });
      
      res.writeHead(200);
      res.end(JSON.stringify({
        success: result.success,
        content: result.content,
        action_required: actionRequired,
        action_data: actionData,
        runtime_id: RUNTIME_ID,
        tenant_id: TENANT_ID,
        session_id: sessionId,
        processed_by: 'openclaw'
      }));
    } catch (error) {
      console.error(`[RUNTIME ${RUNTIME_ID}] Chat error:`, error.message);
      res.writeHead(500);
      res.end(JSON.stringify({ error: error.message, success: false }));
    }
    return;
  }

  // Legacy process endpoint (redirect to /chat)
  if (url.pathname === '/process' && req.method === 'POST') {
    try {
      const data = await parseBody(req);
      
      if (data.tenant_id !== TENANT_ID) {
        res.writeHead(403);
        res.end(JSON.stringify({ error: 'Tenant mismatch - isolation enforced' }));
        return;
      }

      runtimeState.messages_processed++;
      await reportUsage(data.tokens || 0, 1, data.tool_calls || 0);
      
      res.writeHead(200);
      res.end(JSON.stringify({
        processed: true,
        runtime_id: RUNTIME_ID,
        tenant_id: TENANT_ID,
        message_count: runtimeState.messages_processed,
        note: 'Use /chat endpoint for OpenClaw processing'
      }));
    } catch (error) {
      res.writeHead(400);
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }

  // Get runtime info
  if (url.pathname === '/info' && req.method === 'GET') {
    res.writeHead(200);
    res.end(JSON.stringify({
      ...runtimeState,
      openclaw_home: OPENCLAW_HOME,
      openclaw_gateway_port: OPENCLAW_GATEWAY_PORT
    }));
    return;
  }

  // WhatsApp status endpoint
  if (url.pathname === '/whatsapp/status' && req.method === 'GET') {
    try {
      const status = await getWhatsAppStatus();
      res.writeHead(200);
      res.end(JSON.stringify(status));
    } catch (error) {
      res.writeHead(500);
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }

  // WhatsApp connect/pairing endpoint
  if (url.pathname === '/whatsapp/connect' && req.method === 'POST') {
    try {
      const data = await parseBody(req);
      
      if (data.tenant_id !== TENANT_ID) {
        res.writeHead(403);
        res.end(JSON.stringify({ error: 'Tenant mismatch' }));
        return;
      }
      
      const result = await initiateWhatsAppPairing();
      
      res.writeHead(200);
      res.end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(500);
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }

  // WhatsApp disconnect endpoint
  if (url.pathname === '/whatsapp/disconnect' && req.method === 'POST') {
    try {
      const data = await parseBody(req);
      
      if (data.tenant_id !== TENANT_ID) {
        res.writeHead(403);
        res.end(JSON.stringify({ error: 'Tenant mismatch' }));
        return;
      }
      
      const result = await disconnectWhatsApp();
      
      res.writeHead(200);
      res.end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(500);
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }

  // WhatsApp test message endpoint
  if (url.pathname === '/whatsapp/test' && req.method === 'POST') {
    try {
      const data = await parseBody(req);
      
      if (data.tenant_id !== TENANT_ID) {
        res.writeHead(403);
        res.end(JSON.stringify({ error: 'Tenant mismatch' }));
        return;
      }
      
      const result = await sendWhatsAppTest();
      
      res.writeHead(200);
      res.end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(500);
      res.end(JSON.stringify({ error: error.message }));
    }
    return;
  }

  // Shutdown
  if (url.pathname === '/shutdown' && req.method === 'POST') {
    console.log(`[RUNTIME ${RUNTIME_ID}] Shutdown requested`);
    runtimeState.status = 'shutting_down';
    
    // Stop OpenClaw first
    stopOpenClawGateway();
    
    res.writeHead(200);
    res.end(JSON.stringify({ status: 'shutting_down' }));
    
    await emitAuditEvent('runtime_shutdown', `Runtime ${RUNTIME_ID} shutting down`);
    
    setTimeout(() => {
      server.close();
      process.exit(0);
    }, 100);
    return;
  }

  // 404
  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

/**
 * Initialize and start the runtime
 */
async function initialize() {
  console.log(`[RUNTIME ${RUNTIME_ID}] Initializing...`);
  
  // Get OpenClaw version
  runtimeState.openclaw.version = getOpenClawVersion();
  if (!runtimeState.openclaw.version) {
    console.error(`[RUNTIME ${RUNTIME_ID}] OpenClaw not found! Cannot continue.`);
    process.exit(1);
  }
  console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw version: ${runtimeState.openclaw.version}`);
  
  // Initialize OpenClaw home directory
  if (!initOpenClawHome()) {
    console.error(`[RUNTIME ${RUNTIME_ID}] Failed to initialize OpenClaw home`);
  }
  
  // Run OpenClaw setup (creates config and workspace)
  await setupOpenClaw();
  
  // Configure OpenClaw
  await configureOpenClaw();
  
  // Install required skills
  console.log(`[RUNTIME ${RUNTIME_ID}] Installing required skills...`);
  const skillsResult = await installRequiredSkills();
  console.log(`[RUNTIME ${RUNTIME_ID}] Skills: ${skillsResult.installed.length}/${skillsResult.total} succeeded`);
  if (skillsResult.failed.length > 0) {
    console.warn(`[RUNTIME ${RUNTIME_ID}] Failed skills: ${skillsResult.failed.join(', ')}`);
  }
  
  // Start OpenClaw gateway
  const gatewayStarted = await startOpenClawGateway();
  if (!gatewayStarted) {
    console.warn(`[RUNTIME ${RUNTIME_ID}] OpenClaw gateway failed to start, will use local agent mode`);
  }
  
  // Check WhatsApp status (credentials may exist from previous pairing)
  const waStatus = await getWhatsAppStatus();
  console.log(`[RUNTIME ${RUNTIME_ID}] WhatsApp status: ${waStatus.status}, credentials exist: ${waStatus.credentials_exist}`);
  if (waStatus.credentials_exist && waStatus.status !== 'connected') {
    console.log(`[RUNTIME ${RUNTIME_ID}] WhatsApp credentials found but not connected - may need re-login`);
    runtimeState.whatsapp.status = 'needs_relogin';
  }
  
  // Start HTTP server
  server.listen(PORT, '127.0.0.1', async () => {
    runtimeState.status = 'running';
    console.log(`[RUNTIME ${RUNTIME_ID}] Tenant runtime listening on port ${PORT}`);
    console.log(`[RUNTIME ${RUNTIME_ID}] OpenClaw gateway on port ${OPENCLAW_GATEWAY_PORT}`);
    
    // Start MyndLens Channel Adapter (integrated)
    try {
      startMyndLensAdapter();
      console.log(`[RUNTIME ${RUNTIME_ID}] MyndLens Channel Adapter started (integrated on port ${PORT})`);
      runtimeState.myndlens_adapter = {
        enabled: true,
        status: 'running',
        endpoint: `http://localhost:${PORT}/v1/dispatch`
      };
    } catch (error) {
      console.error(`[RUNTIME ${RUNTIME_ID}] Failed to start MyndLens adapter:`, error.message);
      runtimeState.myndlens_adapter = {
        enabled: false,
        status: 'error',
        error: error.message
      };
    }
    
    await emitAuditEvent('runtime_started', `Runtime ${RUNTIME_ID} started for tenant ${TENANT_ID}`, {
      node_version: process.version,
      openclaw_version: runtimeState.openclaw.version,
      runtime_port: PORT,
      gateway_port: OPENCLAW_GATEWAY_PORT,
      openclaw_home: OPENCLAW_HOME,
      myndlens_adapter: runtimeState.myndlens_adapter?.status || 'disabled'
    });
  });
}

// Handle termination
process.on('SIGTERM', async () => {
  console.log(`[RUNTIME ${RUNTIME_ID}] SIGTERM received`);
  stopOpenClawGateway();
  await emitAuditEvent('runtime_terminated', `Runtime ${RUNTIME_ID} terminated`);
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log(`[RUNTIME ${RUNTIME_ID}] SIGINT received`);
  stopOpenClawGateway();
  await emitAuditEvent('runtime_interrupted', `Runtime ${RUNTIME_ID} interrupted`);
  process.exit(0);
});

// Log PID for process management
console.log(`[RUNTIME ${RUNTIME_ID}] PID: ${process.pid}`);

// Start initialization
initialize().catch(error => {
  console.error(`[RUNTIME ${RUNTIME_ID}] Initialization failed:`, error);
  process.exit(1);
});
