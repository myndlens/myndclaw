/**
 * tenant_crypto.mjs — AES-256-GCM Encryption for PKG Diffs (Phase 7).
 *
 * Key derivation: HKDF-SHA256
 *   ikm  = PKG_ENCRYPTION_SECRET + user_id + "pkg_diff_encryption"
 *   salt = tenant_id (UTF-8)
 *   info = "myndlens_pkg_diff_v1"
 *   key  = 32 bytes (AES-256)
 *
 * Both tenant and device derive the same key independently.
 * MyndLens backend relays the opaque blob — it never has the derived key.
 *
 * Encryption: AES-256-GCM (authenticated encryption)
 *   - 12-byte random IV per message
 *   - 16-byte auth tag
 *   - Ciphertext = AES-256-GCM(key, iv, plaintext)
 *
 * Wire format (JSON):
 *   {
 *     "encrypted": true,
 *     "v": 1,
 *     "iv": "<base64>",
 *     "tag": "<base64>",
 *     "ct": "<base64>"     // ciphertext
 *   }
 */

import { createCipheriv, createDecipheriv, randomBytes, hkdfSync } from 'crypto';

const INFO = 'myndlens_pkg_diff_v1';
const KEY_LENGTH = 32;  // AES-256

/**
 * Derive AES-256 key using HKDF-SHA256.
 *
 * @param {string} secret  - PKG_ENCRYPTION_SECRET (= JWT_SECRET from provisioning)
 * @param {string} userId  - User identifier
 * @param {string} tenantId - Tenant identifier (used as salt)
 * @returns {Buffer} 32-byte AES key
 */
export function deriveKey(secret, userId, tenantId) {
  if (!secret || !userId || !tenantId) {
    throw new Error('CRYPTO_KEY_DERIVE: secret, userId, and tenantId are all required');
  }

  const ikm = Buffer.from(secret + userId + 'pkg_diff_encryption', 'utf-8');
  const salt = Buffer.from(tenantId, 'utf-8');
  const info = Buffer.from(INFO, 'utf-8');

  return Buffer.from(hkdfSync('sha256', ikm, salt, info, KEY_LENGTH));
}

/**
 * Encrypt a PKG diff payload using AES-256-GCM.
 *
 * @param {object|string} plaintext - PKG diff (object will be JSON-stringified)
 * @param {Buffer} key - 32-byte AES key from deriveKey()
 * @returns {object} Encrypted envelope: { encrypted: true, v: 1, iv, tag, ct }
 */
export function encrypt(plaintext, key) {
  const text = typeof plaintext === 'string' ? plaintext : JSON.stringify(plaintext);
  const iv = randomBytes(12);  // 96-bit IV for GCM
  const cipher = createCipheriv('aes-256-gcm', key, iv);

  const encrypted = Buffer.concat([
    cipher.update(text, 'utf-8'),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();

  return {
    encrypted: true,
    v: 1,
    iv: iv.toString('base64'),
    tag: tag.toString('base64'),
    ct: encrypted.toString('base64'),
  };
}

/**
 * Decrypt an encrypted PKG diff envelope.
 *
 * @param {object} envelope - { encrypted: true, v: 1, iv, tag, ct }
 * @param {Buffer} key - 32-byte AES key from deriveKey()
 * @returns {object} Decrypted PKG diff (parsed JSON)
 */
export function decrypt(envelope, key) {
  if (!envelope || !envelope.encrypted) {
    throw new Error('CRYPTO_DECRYPT: not an encrypted envelope');
  }
  if (envelope.v !== 1) {
    throw new Error(`CRYPTO_DECRYPT: unsupported version ${envelope.v}`);
  }

  const iv = Buffer.from(envelope.iv, 'base64');
  const tag = Buffer.from(envelope.tag, 'base64');
  const ct = Buffer.from(envelope.ct, 'base64');

  const decipher = createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(tag);

  const decrypted = Buffer.concat([
    decipher.update(ct),
    decipher.final(),
  ]);

  return JSON.parse(decrypted.toString('utf-8'));
}

/**
 * Check if a payload is an encrypted envelope.
 */
export function isEncrypted(payload) {
  return !!(payload && payload.encrypted === true && payload.v === 1);
}
