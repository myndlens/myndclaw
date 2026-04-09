// src/myndlens/auth-bridge.ts
// JWT validation using jsonwebtoken + jwks-rsa
// JWKS: https://obegee.co.uk/.well-known/jwks.json, kid: 9f7861e78fced351, RS256

import jwt, { JwtHeader, SigningKeyCallback, VerifyErrors, JwtPayload as JwtBasePayload } from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';
import type { Request, Response, NextFunction } from 'express';

// Configuration from environment variables
const JWKS_URI = process.env.JWKS_URI || 'https://obegee.co.uk/.well-known/jwks.json';
const EXPECTED_KID = process.env.JWT_KID || '9f7861e78fced351';
const ALGO = 'RS256';

// Initialize JWKS client with caching and rate limiting
const client = jwksClient({
  jwksUri: JWKS_URI,
  cache: true,
  cacheMaxEntries: parseInt(process.env.JWKS_CACHE_MAX_ENTRIES || '5', 10),
  cacheMaxAge: parseInt(process.env.JWKS_CACHE_MAX_AGE || '600000', 10), // 10 minutes
  rateLimit: true,
  jwksRequestsPerMinute: parseInt(process.env.JWKS_REQUESTS_PER_MINUTE || '5', 10),
  timeout: parseInt(process.env.JWKS_TIMEOUT || '30000', 10), // 30 seconds
});

interface CustomJwtPayload {
  tenant_id?: string;
  sub?: string;
  iss?: string;
  aud?: string;
  exp?: number;
  iat?: number;
  [key: string]: unknown;
}

interface AuthenticatedRequest extends Request {
  user?: CustomJwtPayload;
}

/**
 * Signing key callback for JWT verification
 * Enforces specific kid and retrieves public key from JWKS
 */
function getKey(header: JwtHeader, callback: SigningKeyCallback): void {
  const timestamp = new Date().toISOString();
  
  // Strict kid enforcement
  if (header.kid !== EXPECTED_KID) {
    const err = new Error(`Invalid key ID: expected ${EXPECTED_KID}, got ${header.kid}`);
    console.error(`[${timestamp}] JWT key validation failed: ${err.message}`);
    callback(err, undefined);
    return;
  }

  // Retrieve signing key from JWKS
  client.getSigningKey(EXPECTED_KID, (err, key) => {
    if (err) {
      console.error(`[${timestamp}] JWKS key retrieval failed: ${err.message}`);
      callback(err, undefined);
      return;
    }
    
    if (!key) {
      const keyErr = new Error('No signing key found');
      console.error(`[${timestamp}] No signing key returned from JWKS`);
      callback(keyErr, undefined);
      return;
    }
    
    try {
      const signingKey = key.getPublicKey();
      console.log(`[${timestamp}] Successfully retrieved signing key for kid=${EXPECTED_KID}`);
      callback(null, signingKey);
    } catch (keyError) {
      console.error(`[${timestamp}] Failed to extract public key: ${keyError}`);
      callback(keyError as Error, undefined);
    }
  });
}

/**
 * Extracts Bearer token from Authorization header
 */
function extractBearerToken(authHeader: string | string[] | undefined): string | null {
  if (!authHeader) {
    return null;
  }
  
  const headerValue = Array.isArray(authHeader) ? authHeader[0] : authHeader;
  const match = headerValue.match(/^Bearer\s+(.+)$/i);
  return match ? match[1] : null;
}

/**
 * Verifies JWT token using JWKS and returns decoded payload
 */
export async function verifyToken(token: string): Promise<CustomJwtPayload> {
  const timestamp = new Date().toISOString();
  const tokenId = token.substring(0, 10) + '...';
  
  return new Promise((resolve, reject) => {
    console.log(`[${timestamp}] Verifying JWT token: ${tokenId}`);
    
    jwt.verify(token, getKey, {
      algorithms: [ALGO],
      clockTolerance: 60, // Allow 60 seconds clock skew
    }, (err: VerifyErrors | null, decoded: string | JwtBasePayload | undefined) => {
      if (err) {
        console.error(`[${timestamp}] JWT verification failed for ${tokenId}: ${err.message}`);
        reject(new Error(`Token verification failed: ${err.message}`));
        return;
      }
      
      if (!decoded || typeof decoded !== 'object') {
        console.error(`[${timestamp}] Invalid JWT payload for ${tokenId}`);
        reject(new Error('Invalid token payload'));
        return;
      }
      
      const payload = decoded as CustomJwtPayload;
      
      // Additional payload validation
      if (!payload.sub) {
        console.error(`[${timestamp}] Missing 'sub' claim in JWT ${tokenId}`);
        reject(new Error('Token missing required subject claim'));
        return;
      }
      
      console.log(`[${timestamp}] JWT verified successfully for ${tokenId}, sub=${payload.sub}`);
      resolve(payload);
    });
  });
}

/**
 * Express middleware for JWT authentication
 * Validates Authorization header and attaches user to request
 */
export function authMiddleware(
  req: AuthenticatedRequest,
  res: Response,
  next: NextFunction
): void {
  const timestamp = new Date().toISOString();
  const requestId = Math.random().toString(36).substring(2, 15);
  
  console.log(`[${timestamp}] [${requestId}] Auth middleware processing ${req.method} ${req.url}`);
  
  // Extract token from Authorization header
  const authHeader = req.headers.authorization || req.headers.Authorization;
  const token = extractBearerToken(authHeader);
  
  if (!token) {
    console.error(`[${timestamp}] [${requestId}] Missing or invalid Authorization header`);
    res.status(401).json({
      status: 'error',
      error: 'Authorization header with Bearer token is required'
    });
    return;
  }
  
  verifyToken(token)
    .then((payload) => {
      req.user = payload;
      console.log(`[${timestamp}] [${requestId}] Authentication successful for sub=${payload.sub}`);
      next();
    })
    .catch((error) => {
      console.error(`[${timestamp}] [${requestId}] Authentication failed: ${error.message}`);
      res.status(401).json({
        status: 'error',
        error: `Authentication failed: ${error.message}`
      });
    });
}

/**
 * Optional JWT authentication middleware
 * If token is present, validates it and attaches user
 * If no token, proceeds without authentication (req.user remains undefined)
 */
export function optionalAuthMiddleware(
  req: AuthenticatedRequest,
  res: Response,
  next: NextFunction
): void {
  const timestamp = new Date().toISOString();
  const requestId = Math.random().toString(36).substring(2, 15);
  
  console.log(`[${timestamp}] [${requestId}] Optional auth middleware processing ${req.method} ${req.url}`);
  
  // Extract token from Authorization header
  const authHeader = req.headers.authorization || req.headers.Authorization;
  const token = extractBearerToken(authHeader);
  
  // If no token, proceed without authentication
  if (!token) {
    console.log(`[${timestamp}] [${requestId}] No token provided, proceeding without authentication`);
    next();
    return;
  }
  
  // If token is present, validate it
  verifyToken(token)
    .then((payload) => {
      req.user = payload;
      console.log(`[${timestamp}] [${requestId}] Optional authentication successful for sub=${payload.sub}`);
      next();
    })
    .catch((error) => {
      console.error(`[${timestamp}] [${requestId}] Optional authentication failed: ${error.message}`);
      // For optional auth, we still proceed even if token is invalid
      // The endpoint can decide if authentication is required based on req.user
      console.log(`[${timestamp}] [${requestId}] Proceeding without user context`);
      next();
    });
}
