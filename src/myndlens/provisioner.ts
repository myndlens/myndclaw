// src/myndlens/provisioner.ts
// User lifecycle management - provision/deprovision user directories and SOUL.md files
// Uses fs/promises for async file operations

import { promises as fs } from 'fs';
import { join } from 'path';

// Configuration
const BASE_USER_DIR = process.env.MYNDLENS_USER_DIR || './users';
const SOUL_FILENAME = 'SOUL.md';

interface ProvisionResult {
  success: boolean;
  tenantId: string;
  userDir: string;
  soulPath: string;
  message: string;
}

interface DeprovisionResult {
  success: boolean;
  tenantId: string;
  userDir: string;
  message: string;
  filesRemoved: number;
}

/**
 * Validates tenant ID format
 */
function isValidTenantId(tenantId: string): boolean {
  // Must be non-empty string with safe filesystem characters
  return /^[a-zA-Z0-9_-]+$/.test(tenantId) && tenantId.length >= 3 && tenantId.length <= 50;
}

/**
 * Validates phone number format (supports international formats)
 */
function isValidPhoneNumber(phoneNumber: string): boolean {
  // Allow international format with country code, spaces, dashes, parentheses
  // Must have at least 7 digits after stripping non-numeric characters
  const digitsOnly = phoneNumber.replace(/[\s\-\(\)\+]/g, '');
  return /^\+?[0-9]{7,15}$/.test(digitsOnly);
}

/**
 * Validates user name
 */
function isValidUserName(userName: string): boolean {
  return userName.trim().length >= 2 && userName.trim().length <= 100;
}

/**
 * Generates SOUL.md content for a new user
 */
function generateSoulContent(tenantId: string, phoneNumber: string, userName: string): string {
  const timestamp = new Date().toISOString();
  const formattedDate = new Date().toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  });
  
  return `# SOUL Profile

**User:** ${userName}  
**Tenant ID:** ${tenantId}  
**Phone:** ${phoneNumber}  
**Created:** ${formattedDate}  
**System Timestamp:** ${timestamp}

---

## About Me

*This is your personal SOUL file - a space for your thoughts, preferences, and context that helps MyndLens understand you better.*

### My Preferences
- Communication style: 
- Preferred response length: 
- Topics of interest: 
- Time zone: 

### My Context
- Role/profession: 
- Current projects: 
- Goals: 

### My History
- Key conversations: 
- Important decisions: 
- Learning journey: 

---

*Last updated: ${formattedDate}*
*This file is private to your tenant and encrypted at rest.*
`;
}

/**
 * Recursively counts files in a directory
 */
async function countFilesRecursive(dirPath: string): Promise<number> {
  let fileCount = 0;
  
  try {
    const entries = await fs.readdir(dirPath, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = join(dirPath, entry.name);
      
      if (entry.isDirectory()) {
        fileCount += await countFilesRecursive(fullPath);
      } else if (entry.isFile()) {
        fileCount++;
      }
    }
  } catch (error) {
    // Directory might not exist or be accessible
    console.warn(`Could not count files in ${dirPath}: ${error}`);
  }
  
  return fileCount;
}

/**
 * Recursively removes directory and all contents
 */
async function removeDirectoryRecursive(dirPath: string): Promise<number> {
  let filesRemoved = 0;
  
  try {
    const entries = await fs.readdir(dirPath, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = join(dirPath, entry.name);
      
      if (entry.isDirectory()) {
        filesRemoved += await removeDirectoryRecursive(fullPath);
      } else {
        await fs.unlink(fullPath);
        filesRemoved++;
      }
    }
    
    await fs.rmdir(dirPath);
  } catch (error) {
    const errMsg = error instanceof Error ? error.message : 'Unknown error';
    console.error(`Error removing directory ${dirPath}: ${errMsg}`);
    throw error;
  }
  
  return filesRemoved;
}

/**
 * Provisions a new user by creating directory structure and SOUL.md file
 */
export async function provisionUser(
  tenantId: string,
  phoneNumber: string,
  userName: string
): Promise<ProvisionResult> {
  const timestamp = new Date().toISOString();
  const requestId = Math.random().toString(36).substring(2, 15);
  
  console.log(`[${timestamp}] [${requestId}] Provisioning user: tenant=${tenantId}, name=${userName}`);
  
  try {
    // Validate inputs
    if (!isValidTenantId(tenantId)) {
      const error = 'Invalid tenant ID - must be 3-50 alphanumeric characters, underscores, or dashes';
      console.error(`[${timestamp}] [${requestId}] ${error}`);
      return {
        success: false,
        tenantId,
        userDir: '',
        soulPath: '',
        message: error
      };
    }
    
    if (!isValidPhoneNumber(phoneNumber)) {
      const error = 'Invalid phone number format';
      console.error(`[${timestamp}] [${requestId}] ${error}`);
      return {
        success: false,
        tenantId,
        userDir: '',
        soulPath: '',
        message: error
      };
    }
    
    if (!isValidUserName(userName)) {
      const error = 'Invalid user name - must be 2-100 characters';
      console.error(`[${timestamp}] [${requestId}] ${error}`);
      return {
        success: false,
        tenantId,
        userDir: '',
        soulPath: '',
        message: error
      };
    }
    
    // Build directory paths
    const userDir = join(BASE_USER_DIR, tenantId);
    const soulPath = join(userDir, SOUL_FILENAME);
    
    // Check if user directory already exists
    try {
      await fs.access(userDir);
      const existingError = `User directory already exists for tenant ${tenantId}`;
      console.error(`[${timestamp}] [${requestId}] ${existingError}`);
      return {
        success: false,
        tenantId,
        userDir,
        soulPath,
        message: existingError
      };
    } catch {
      // Directory does not exist - this is expected
    }
    
    // Create user directory
    await fs.mkdir(userDir, { recursive: true });
    console.log(`[${timestamp}] [${requestId}] Created user directory: ${userDir}`);
    
    // Generate and write SOUL.md file
    const soulContent = generateSoulContent(tenantId, phoneNumber, userName);
    await fs.writeFile(soulPath, soulContent, 'utf8');
    console.log(`[${timestamp}] [${requestId}] Created SOUL.md: ${soulPath}`);
    
    const successMessage = `User provisioned successfully`;
    console.log(`[${timestamp}] [${requestId}] ${successMessage}`);
    
    return {
      success: true,
      tenantId,
      userDir,
      soulPath,
      message: successMessage
    };
    
  } catch (error) {
    const errMsg = error instanceof Error ? error.message : 'Unknown error';
    console.error(`[${timestamp}] [${requestId}] Provisioning failed: ${errMsg}`);
    
    return {
      success: false,
      tenantId,
      userDir: '',
      soulPath: '',
      message: `Provisioning failed: ${errMsg}`
    };
  }
}

/**
 * Deprovisions a user by removing their directory and all files
 */
export async function deprovisionUser(tenantId: string): Promise<DeprovisionResult> {
  const timestamp = new Date().toISOString();
  const requestId = Math.random().toString(36).substring(2, 15);
  
  console.log(`[${timestamp}] [${requestId}] Deprovisioning user: tenant=${tenantId}`);
  
  try {
    // Validate tenant ID
    if (!isValidTenantId(tenantId)) {
      const error = 'Invalid tenant ID format';
      console.error(`[${timestamp}] [${requestId}] ${error}`);
      return {
        success: false,
        tenantId,
        userDir: '',
        message: error,
        filesRemoved: 0
      };
    }
    
    const userDir = join(BASE_USER_DIR, tenantId);
    
    // Check if user directory exists
    try {
      await fs.access(userDir);
    } catch {
      const notFoundError = `User directory not found for tenant ${tenantId}`;
      console.error(`[${timestamp}] [${requestId}] ${notFoundError}`);
      return {
        success: false,
        tenantId,
        userDir,
        message: notFoundError,
        filesRemoved: 0
      };
    }
    
    // Count files before removal
    const filesToRemove = await countFilesRecursive(userDir);
    
    // Remove directory recursively
    const filesRemoved = await removeDirectoryRecursive(userDir);
    
    const successMessage = `User deprovisioned successfully, ${filesRemoved} files removed`;
    console.log(`[${timestamp}] [${requestId}] ${successMessage}`);
    
    return {
      success: true,
      tenantId,
      userDir,
      message: successMessage,
      filesRemoved
    };
    
  } catch (error) {
    const errMsg = error instanceof Error ? error.message : 'Unknown error';
    console.error(`[${timestamp}] [${requestId}] Deprovisioning failed: ${errMsg}`);
    
    return {
      success: false,
      tenantId,
      userDir: '',
      message: `Deprovisioning failed: ${errMsg}`,
      filesRemoved: 0
    };
  }
}

/**
 * Checks if a user exists (directory exists)
 */
export async function userExists(tenantId: string): Promise<boolean> {
  try {
    const userDir = join(BASE_USER_DIR, tenantId);
    await fs.access(userDir);
    return true;
  } catch {
    return false;
  }
}

/**
 * Gets information about a user's directory and files
 */
export async function getUserInfo(tenantId: string): Promise<{ exists: boolean; fileCount: number; soulExists: boolean }> {
  const userDir = join(BASE_USER_DIR, tenantId);
  const soulPath = join(userDir, SOUL_FILENAME);
  
  const exists = await userExists(tenantId);
  
  if (!exists) {
    return { exists: false, fileCount: 0, soulExists: false };
  }
  
  const fileCount = await countFilesRecursive(userDir);
  
  let soulExists = false;
  try {
    await fs.access(soulPath);
    soulExists = true;
  } catch {
    soulExists = false;
  }
  
  return { exists: true, fileCount, soulExists };
}
