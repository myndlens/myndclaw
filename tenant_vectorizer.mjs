/**
 * tenant_vectorizer.mjs — ONNX Vectorization for Tenant DS Service.
 *
 * Generates 384-dim semantic embeddings using BAAI/bge-small-en-v1.5 via
 * @xenova/transformers (ONNX Runtime under the hood).
 *
 * The embedding text is constructed from entity fields:
 *   display_name + relationship + frequency + active thread topics +
 *   sentiment + pending action summaries + aspirations
 *
 * This produces a semantically rich vector without storing readable text.
 *
 * ONNX model + deps are lazy-loaded on first vectorization request.
 * This allows the tenant container to start fast without the ~33MB model.
 */

let _extractor = null;
let _depsInstalled = false;

const MODEL_NAME = 'Xenova/bge-small-en-v1.5';
const EMBEDDING_DIM = 384;

/**
 * Ensure ONNX dependencies are available.
 * Dependencies must be installed at build time (Dockerfile) or pre-provisioned.
 * If missing, vectorization gracefully degrades with a clear error.
 */
async function ensureDeps() {
  if (_depsInstalled) return;
  try {
    await import('@xenova/transformers');
    _depsInstalled = true;
  } catch (e) {
    throw new Error(
      '[TenantVectorizer] @xenova/transformers not installed. ' +
      'Run the ONNX-enabled Dockerfile build or install manually: ' +
      'npm install @xenova/transformers@^2.17.0 onnxruntime-node@^1.19.0'
    );
  }
}

/**
 * Initialize the embedding pipeline (lazy, cached after first call).
 */
async function getExtractor() {
  if (_extractor) return _extractor;

  await ensureDeps();
  const { pipeline, env } = await import('@xenova/transformers');

  // Disable remote model downloads in production — model must be pre-cached
  env.allowRemoteModels = true; // Allow on first run for model download
  env.cacheDir = '/app/.cache/transformers';

  console.log(`[TenantVectorizer] Loading model: ${MODEL_NAME}`);
  const start = Date.now();
  _extractor = await pipeline('feature-extraction', MODEL_NAME);
  console.log(`[TenantVectorizer] Model ready in ${Date.now() - start}ms`);

  return _extractor;
}

/**
 * Generate embeddings for a list of texts.
 *
 * @param {string[]} texts - Array of texts to embed
 * @returns {Promise<number[][]>} Array of 384-dim float vectors
 */
export async function embed(texts) {
  if (!texts || texts.length === 0) return [];

  const extractor = await getExtractor();
  const results = [];

  // Process in batches of 32 to manage memory
  const BATCH_SIZE = 32;
  for (let i = 0; i < texts.length; i += BATCH_SIZE) {
    const batch = texts.slice(i, i + BATCH_SIZE);
    if (texts.length > BATCH_SIZE) {
      console.log(`[TenantVectorizer] Embedding batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(texts.length / BATCH_SIZE)} (${batch.length} texts)`);
    }
    const output = await extractor(batch, { pooling: 'mean', normalize: true });

    // Convert tensor to nested array
    for (let j = 0; j < batch.length; j++) {
      const vec = [];
      for (let k = 0; k < EMBEDDING_DIM; k++) {
        vec.push(output.data[j * EMBEDDING_DIM + k]);
      }
      results.push(vec);
    }
  }

  return results;
}

/**
 * Generate embedding for a single text.
 */
export async function embedOne(text) {
  const results = await embed([text]);
  return results[0] || [];
}

/**
 * Return embedding dimension (384 for bge-small-en-v1.5).
 */
export function dimension() {
  return EMBEDDING_DIM;
}

/**
 * Build semantically rich embedding text from a canonical entity.
 *
 * Mirrors the Python _build_embedding_text() in vectorizer.py.
 */
export function buildEmbeddingText(entity) {
  const parts = [];

  // WHO — identity
  const identity = entity.identity || {};
  if (identity.display_name) parts.push(identity.display_name);
  if (identity.relationship && identity.relationship !== 'unknown') parts.push(identity.relationship);
  if (identity.nicknames?.length) parts.push(identity.nicknames.slice(0, 5).join(' '));
  if (identity.phone) parts.push(identity.phone);

  // HOW — pattern
  const mi = entity.merged_intelligence || {};
  const pattern = mi.pattern || {};
  if (pattern.frequency && pattern.frequency !== 'unknown') parts.push(pattern.frequency);
  if (mi.sentiment && mi.sentiment !== 'neutral') parts.push(mi.sentiment);
  if (pattern.style && pattern.style !== 'unknown') parts.push(pattern.style);

  // WHAT — active threads
  for (const thread of (mi.active_threads || []).slice(0, 10)) {
    const topic = thread.topic || '';
    const status = thread.status || '';
    if (topic) parts.push(`${topic} ${status}`.trim());
  }

  // WHAT — pending actions
  const pending = mi.pending_actions || {};
  for (const action of (pending.user_owes || []).slice(0, 2)) {
    const act = typeof action === 'object' ? (action.action || '') : String(action);
    if (act) parts.push(act);
  }
  for (const action of (pending.they_owe || []).slice(0, 2)) {
    const act = typeof action === 'object' ? (action.action || '') : String(action);
    if (act) parts.push(act);
  }

  // WHAT — aspirations
  for (const asp of (mi.aspirations_mentioned || []).slice(0, 2)) {
    if (asp) parts.push(asp);
  }

  // WHAT — upcoming events
  for (const event of (mi.upcoming_events || []).slice(0, 2)) {
    const evt = typeof event === 'object' ? (event.event || '') : String(event);
    if (evt) parts.push(evt);
  }

  // WHAT — emotional context
  const emo = mi.emotional_context || {};
  if (emo.current_mood && emo.current_mood !== 'unknown') parts.push(emo.current_mood);

  // WHAT — financial mentions
  for (const fin of (mi.financial_mentions || []).slice(0, 2)) {
    if (typeof fin === 'object') {
      const t = fin.type || '';
      const cp = fin.counterparty || '';
      if (t) parts.push(`${t} ${cp}`.trim());
    }
  }

  // WHAT — travel mentions
  for (const trv of (mi.travel_mentions || []).slice(0, 2)) {
    if (typeof trv === 'object') {
      const t = [trv.type, trv.destination, trv.origin].filter(Boolean).join(' ');
      if (t) parts.push(t);
    }
  }

  // WHO — mentioned people
  for (const mp of (mi.mentioned_people || []).slice(0, 3)) {
    if (typeof mp === 'object' && mp.name) {
      parts.push(`${mp.name} ${mp.context || ''}`.trim());
    }
  }

  // Source context from canonical_id
  if (!entity.source_records) {
    const cid = entity.canonical_id || '';
    if (cid.includes('_')) parts.push(cid.split('_')[0]);
  }

  return parts.join(' ').trim();
}

/**
 * Vectorize a list of canonical entities (add 'vector' field to each).
 *
 * @param {Object[]} entities - List of canonical entity objects
 * @returns {Promise<Object[]>} Same entities with 'vector' field added
 */
export async function vectorize(entities) {
  if (!entities || entities.length === 0) return [];

  const texts = [];
  const validIndices = [];

  for (let i = 0; i < entities.length; i++) {
    const text = buildEmbeddingText(entities[i]);
    if (text) {
      texts.push(text);
      validIndices.push(i);
    } else {
      console.warn(`[TenantVectorizer] EMPTY_TEXT entity=${entities[i].canonical_id || `idx_${i}`} — skipped (no embeddable fields)`);
    }
  }

  if (texts.length === 0) return entities;

  console.log(`[TenantVectorizer] Embedding ${texts.length} entities, avg_chars=${Math.round(texts.reduce((s, t) => s + t.length, 0) / texts.length)}`);

  const vectors = await embed(texts);

  for (let vi = 0; vi < validIndices.length; vi++) {
    entities[validIndices[vi]].vector = vectors[vi];
  }

  const vectorizedCount = entities.filter(e => e.vector).length;
  console.log(`[TenantVectorizer] Done: ${vectorizedCount}/${entities.length} vectorized, dim=${EMBEDDING_DIM}`);

  return entities;
}
