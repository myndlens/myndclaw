/**
 * tenant_llm.mjs — LLM Gateway for Tenant DS Service.
 *
 * Provider: Google Gemini (gemini-2.5-flash).
 *
 * Default (Starter Plan): MyndLens-provided Gemini API key.
 * Pro Plan: User's BYOK key (set via ObeGee dashboard).
 *
 * The LLM_KEY environment variable is set at provision time:
 *   - Starter: MyndLens Gemini API key
 *   - Pro BYOK: user's own Gemini key
 *
 * NOTE: Emergent LLM keys are NEVER used here.
 */

const GEMINI_API_BASE = 'https://generativelanguage.googleapis.com/v1beta';
const DEFAULT_MODEL   = 'gemini-2.5-flash';

/**
 * Full LLM call with system prompt (used by tenant_ds_service.mjs and wa_ds_extractor.mjs).
 *
 * @param {string} systemPrompt
 * @param {string} userContent
 * @param {Object} options
 * @param {string} options.apiKey        - Gemini API key
 * @param {string} [options.model]       - Model override
 * @param {number} [options.maxOutputTokens]
 * @param {number} [options.temperature]
 * @returns {Promise<string>} Response text (JSON string for distillation)
 */
export async function callGemini(systemPrompt, userContent, options = {}) {
  const {
    apiKey,
    model           = DEFAULT_MODEL,
    maxOutputTokens = 65536,
    temperature     = 0.3,
  } = options;

  if (!apiKey) {
    throw new Error('LLM_KEY_MISSING: No API key provided for LLM distillation');
  }

  console.log(`[TenantLLM] CALL model=${model} sys_chars=${systemPrompt.length} user_chars=${userContent.length}`);
  const start = Date.now();

  const body = {
    system_instruction: {
      parts: [{ text: systemPrompt }],
    },
    contents: [
      {
        role: 'user',
        parts: [{ text: userContent }],
      },
    ],
    generationConfig: {
      temperature,
      maxOutputTokens,
      responseMimeType: 'application/json',
    },
  };

  const url = `${GEMINI_API_BASE}/models/${model}:generateContent?key=${apiKey}`;

  let response;
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch (fetchErr) {
    console.error(`[TenantLLM] Network error ${model} after ${Date.now() - start}ms: ${fetchErr.message}`);
    throw fetchErr;
  }

  if (!response.ok) {
    const errText = await response.text().catch(() => '');
    console.error(`[TenantLLM] API_ERROR model=${model} status=${response.status} after ${Date.now() - start}ms: ${errText.substring(0, 200)}`);
    throw new Error(`LLM_API_ERROR: Gemini ${response.status} — ${errText.substring(0, 200)}`);
  }

  const data = await response.json();
  const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';

  if (!text) {
    console.error(`[TenantLLM] EMPTY_RESPONSE model=${model} after ${Date.now() - start}ms`);
    throw new Error('LLM_EMPTY_RESPONSE: Gemini returned empty response');
  }

  console.log(`[TenantLLM] OK model=${model} chars=${text.length} time=${Date.now() - start}ms`);
  return text;
}

/**
 * Simple LLM call without system prompt (used by wa_ds_extractor.mjs).
 */
export async function callSimple(prompt, apiKey) {
  if (!apiKey) throw new Error('LLM_KEY_MISSING');

  console.log(`[TenantLLM] SIMPLE_CALL model=${DEFAULT_MODEL} prompt_chars=${prompt.length}`);
  const start = Date.now();

  const body = {
    contents: [
      {
        role: 'user',
        parts: [{ text: prompt }],
      },
    ],
    generationConfig: {
      temperature: 0.3,
      maxOutputTokens: 65536,
      responseMimeType: 'application/json',
    },
  };

  const url = `${GEMINI_API_BASE}/models/${DEFAULT_MODEL}:generateContent?key=${apiKey}`;

  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errText = await response.text().catch(() => '');
    console.error(`[TenantLLM] SIMPLE_FAIL status=${response.status} after ${Date.now() - start}ms: ${errText.substring(0, 200)}`);
    throw new Error(`Gemini ${response.status} — ${errText.substring(0, 200)}`);
  }

  const data = await response.json();
  const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
  console.log(`[TenantLLM] SIMPLE_OK chars=${text.length} time=${Date.now() - start}ms`);
  return text;
}
