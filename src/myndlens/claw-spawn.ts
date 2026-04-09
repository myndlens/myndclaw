// src/myndlens/claw-spawn.ts
// Express-style HTTP handler for POST /api/claw/spawn
// Validates input, logs activity, simulates spawn, returns {status, mandate_id, result}

import type { Request, Response, NextFunction } from 'express';

type UnknownRecord = Record<string, unknown>;

interface ClawSpawnBody {
  tenant_id: string;
  mandate_id: string;
  mandate_type: string;
  context: UnknownRecord;
  tools_required: string[];
  timeout_seconds: number;
}

interface SpawnResult {
  pid: number;
  spawned_at: string;
  context_echo: UnknownRecord;
  tools_active: string[];
  timeout_at: string;
}

interface SuccessResponse {
  status: 'success';
  mandate_id: string;
  result: SpawnResult;
}

interface ErrorResponse {
  status: 'error';
  error: string;
  mandate_id?: string;
}

/**
 * Validates that a value is a non-empty string
 */
function isValidString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

/**
 * Validates that timeout is within acceptable range
 */
function isValidTimeout(value: unknown): value is number {
  return typeof value === 'number' && 
         Number.isInteger(value) && 
         value >= 1 && 
         value <= 3600;
}

/**
 * Validates tools_required array contains only strings
 */
function isValidToolsArray(value: unknown): value is string[] {
  return Array.isArray(value) && 
         value.every(tool => typeof tool === 'string' && tool.trim().length > 0);
}

/**
 * Main handler for POST /api/claw/spawn
 * Validates input parameters and simulates Claw process spawning
 */
export const handler = async (req: Request, res: Response, _next?: NextFunction): Promise<void> => {
  const timestamp = new Date().toISOString();
  const requestId = Math.random().toString(36).substring(2, 15);
  
  try {
    console.log(`[${timestamp}] [${requestId}] Claw spawn request received`);
    
    // Validate request body exists
    const body = req.body as ClawSpawnBody | null;
    if (!body) {
      console.error(`[${timestamp}] [${requestId}] Missing request body`);
      const errorResponse: ErrorResponse = {
        status: 'error',
        error: 'Missing request body'
      };
      res.status(400).json(errorResponse);
      return;
    }

    const { tenant_id, mandate_id, mandate_type, context, tools_required, timeout_seconds } = body;

    // Validate tenant_id
    if (!isValidString(tenant_id)) {
      console.error(`[${timestamp}] [${requestId}] Invalid tenant_id: ${JSON.stringify(tenant_id)}`);
      const errorResponse: ErrorResponse = {
        status: 'error',
        error: 'tenant_id must be non-empty string'
      };
      res.status(400).json(errorResponse);
      return;
    }

    // Validate mandate_id
    if (!isValidString(mandate_id)) {
      console.error(`[${timestamp}] [${requestId}] Invalid mandate_id: ${JSON.stringify(mandate_id)}`);
      const errorResponse: ErrorResponse = {
        status: 'error',
        error: 'mandate_id must be non-empty string',
        mandate_id
      };
      res.status(400).json(errorResponse);
      return;
    }

    // Validate mandate_type
    if (!isValidString(mandate_type)) {
      console.error(`[${timestamp}] [${requestId}] Invalid mandate_type: ${JSON.stringify(mandate_type)}`);
      const errorResponse: ErrorResponse = {
        status: 'error',
        error: 'mandate_type must be non-empty string',
        mandate_id
      };
      res.status(400).json(errorResponse);
      return;
    }

    // Validate context (must be object)
    if (!context || typeof context !== 'object' || Array.isArray(context)) {
      console.error(`[${timestamp}] [${requestId}] Invalid context: ${JSON.stringify(context)}`);
      const errorResponse: ErrorResponse = {
        status: 'error',
        error: 'context must be object',
        mandate_id
      };
      res.status(400).json(errorResponse);
      return;
    }

    // Validate tools_required
    if (!isValidToolsArray(tools_required)) {
      console.error(`[${timestamp}] [${requestId}] Invalid tools_required: ${JSON.stringify(tools_required)}`);
      const errorResponse: ErrorResponse = {
        status: 'error',
        error: 'tools_required must be array of non-empty strings',
        mandate_id
      };
      res.status(400).json(errorResponse);
      return;
    }

    // Validate timeout_seconds
    if (!isValidTimeout(timeout_seconds)) {
      console.error(`[${timestamp}] [${requestId}] Invalid timeout_seconds: ${timeout_seconds}`);
      const errorResponse: ErrorResponse = {
        status: 'error',
        error: 'timeout_seconds must be integer between 1 and 3600',
        mandate_id
      };
      res.status(400).json(errorResponse);
      return;
    }

    // Simulate Claw process spawning
    const spawnedAt = new Date().toISOString();
    const timeoutAt = new Date(Date.now() + timeout_seconds * 1000).toISOString();
    const pid = Math.floor(Math.random() * 100000) + 30000;

    const result: SpawnResult = {
      pid,
      spawned_at: spawnedAt,
      context_echo: context,
      tools_active: tools_required,
      timeout_at: timeoutAt
    };

    console.log(`[${timestamp}] [${requestId}] Claw spawned successfully: pid=${pid}, mandate=${mandate_id}`);

    const successResponse: SuccessResponse = {
      status: 'success',
      mandate_id,
      result
    };

    res.status(200).json(successResponse);

  } catch (error) {
    const errMsg = error instanceof Error ? error.message : 'Unknown error';
    console.error(`[${timestamp}] [${requestId}] Unexpected error during claw spawn: ${errMsg}`);
    
    const errorResponse: ErrorResponse = {
      status: 'error',
      error: 'Internal server error during claw spawn'
    };
    
    res.status(500).json(errorResponse);
  }
};
