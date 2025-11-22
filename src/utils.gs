/**
 * Shared Utility Functions
 *
 * This file contains common utilities used across the CAPWATCH automation:
 * - File parsing and caching
 * - Retry logic for API calls
 * - Data validation and sanitization
 * - Structured logging
 */

// Cache for parsed CSV files to improve performance
const _fileCache = {};

/**
 * Parses a CAPWATCH CSV file from Google Drive with caching and a robust fallback.
 *
 * Attempts to use the fast Utilities.parseCsv method first. If that fails with a
 * server error, it falls back to a more resilient manual line-by-line parser.
 *
 * @param {string} fileName - Name of file without extension (e.g., 'Member' not 'Member.txt')
 * @returns {Array<Array<string>>} Parsed CSV data with header row excluded
 * @throws {Error} If fileName parameter is invalid
 */
function parseFile(fileName) {
  if (!fileName || typeof fileName !== 'string') {
    throw new Error('Invalid fileName parameter');
  }

  if (_fileCache[fileName]) {
    return _fileCache[fileName];
  }

  const folder = DriveApp.getFolderById(CONFIG.CAPWATCH_DATA_FOLDER_ID);
  const files = folder.getFilesByName(fileName + '.txt');

  if (files.hasNext()) {
    const fileContent = files.next().getBlob().getDataAsString();

    if (!fileContent) {
      Logger.warn('Empty file encountered', { fileName: fileName });
      return [];
    }

    try {
      // Primary method: Fast, but can be brittle.
      _fileCache[fileName] = Utilities.parseCsv(fileContent).slice(1);
      return _fileCache[fileName];
    } catch (e) {
      Logger.warn('Utilities.parseCsv failed, falling back to manual parser.', {
        fileName: fileName,
        errorMessage: e.message
      });

      // Fallback method: Slower, but more resilient to formatting errors.
      try {
        const lines = fileContent.split(/[\r\n]+/);
        const data = lines.slice(1).map(line => {
          // This is a simple split, assuming no commas within quoted fields.
          // For CAPWATCH data, this is usually a safe assumption.
          if (line) return line.split(',');
          return null;
        }).filter(row => row); // Filter out any empty lines

        _fileCache[fileName] = data;
        return data;
      } catch (manualError) {
        Logger.error('Manual CSV parsing also failed.', {
          fileName: fileName,
          errorMessage: manualError.message
        });
        return [];
      }
    }
  } else {
    Logger.warn('File not found', {
      fileName: fileName,
      expectedPath: fileName + '.txt',
      folderId: CONFIG.CAPWATCH_DATA_FOLDER_ID
    });
    return [];
  }
}

/**
 * Clears all cached parsed file data
 * Should be called at the start of major operations to ensure fresh data
 * @returns {void}
 */
function clearCache() {
  const cacheSize = Object.keys(_fileCache).length;
  Object.keys(_fileCache).forEach(key => delete _fileCache[key]);
  Logger.info('File cache cleared', { filesCleared: cacheSize });
}

/**
 * Executes a function with smart rate limiting and exponential backoff
 * - Adds a fixed inter-call delay (token bucket pacing)
 * - Retries on transient errors (403 quotaExceeded, 429, 500, 503)
 * - Stops immediately on permanent errors (400, 404, 409)
 */
function executeWithRetry(fn, maxRetries = CONFIG.API_RETRY_ATTEMPTS || 5) {
  const apiDelay = CONFIG.API_DELAY_MS || 3000;          // steady pacing between calls
  const baseDelay = CONFIG.API_BACKOFF_BASE_MS || 10000;  // first backoff wait
  const backoffFactor = 2.0;                             // double each retry delay

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      Utilities.sleep(apiDelay); // token bucket throttle
      return fn();
    } catch (e) {
      const code = e.details?.code || 0;
      const transient = [403, 429, 500, 503].includes(code);

      if (!transient || attempt === maxRetries) {
        Logger.error('Max retries exceeded', {
          attempts: attempt,
          errorMessage: e.message,
          errorCode: code
        });
        throw e;
      }

      const delay = baseDelay * Math.pow(backoffFactor, attempt - 1);
      Logger.warn('Retrying after transient/quota error', {
        attempt,
        waitTime: `${delay}ms`,
        errorMessage: e.message,
        errorCode: code
      });
      Utilities.sleep(delay);
    }
  }
}

/**
 * Validates a member object has required fields and proper formatting
 *
 * @param {Object} member - Member object to validate
 * @param {string} member.capsn - CAP Serial Number
 * @param {string} member.firstName - First name
 * @param {string} member.lastName - Last name
 * @param {string} member.email - Email address (optional)
 * @param {string} member.orgPath - Organization path
 * @returns {Object} Validation result with isValid boolean and errors array
 * @returns {boolean} returns.isValid - True if member data is valid
 * @returns {string[]} returns.errors - Array of error messages if invalid
 */
function validateMember(member) {
  const errors = [];

  if (!member.capsn || !/^\d+$/.test(member.capsn)) {
    errors.push('Invalid or missing CAPID (must be numeric)');
  }

  if (!member.firstName || !member.lastName) {
    errors.push('Missing name');
  }

  if (member.email && !isValidEmail(member.email)) {
    errors.push('Invalid email format');
  }

  if (!member.orgPath) {
    errors.push('Missing organization path');
  }

  return {
    isValid: errors.length === 0,
    errors: errors
  };
}

/**
 * Validates email address format using regex
 *
 * @param {string} email - Email address to validate
 * @returns {boolean} True if email format is valid
 */
function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

/**
 * Sanitizes and validates an email address
 *
 * Performs the following:
 * - Trims whitespace
 * - Converts to lowercase
 * - Validates format
 *
 * @param {string} email - Email address to sanitize
 * @returns {string|null} Sanitized email address or null if invalid
 */
function sanitizeEmail(email) {
  if (!email || typeof email !== 'string') {
    return null;
  }

  email = email.trim().toLowerCase();

  // Validate format
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    return null;
  }

  return email;
}

/**
 * Calculates the group ID for a squadron
 *
 * @param {string} orgid - Organization ID
 * @param {Object} squadrons - Squadrons lookup object
 * @returns {string} Group ID or empty string if not applicable
 */
function calculateGroup(orgid, squadrons) {
  if (!squadrons[orgid]) {
    Logger.error('calculateGroup: missing org', { orgid: orgid });
    return ''; // prevents crash and logs the missing orgid
  }

  const squadron = squadrons[orgid];
  return squadron.scope === 'UNIT'
    ? squadron.nextLevel
    : (squadron.scope === 'GROUP' ? orgid : '');
}

/**
 * Converts a string to Title Case (first letter upper, rest lower for each word)
 * @param {string} str
 * @returns {string}
 */
function toTitleCase(str) {
  if (!str || typeof str !== 'string') return '';
  return str.replace(/\w\S*/g, txt =>
    txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
  );
}

/**
 * Structured Logging Utility
 *
 * Provides consistent logging with:
 * - Timestamp on every log entry
 * - Structured JSON output
 * - Multiple log levels (info, warn, error)
 * - Internal log storage for summary reporting
 *
 * Usage:
 *   Logger.info('Operation completed', { count: 5 });
 *   Logger.warn('Potential issue', { value: null });
 *   Logger.error('Operation failed', errorObject);
 */
const Logger = {
  _logs: [],

  /**
   * Logs informational message
   * @param {string} message - Log message
   * @param {Object} data - Additional data to log
   * @returns {void}
   */
  info: function(message, data) {
    const log = {
      level: 'INFO',
      timestamp: new Date().toISOString(),
      message: message,
      data: data || {}
    };
    console.log(JSON.stringify(log));
    this._logs.push(log);
  },

  /**
   * Logs error message with error details
   * Handles both Error objects and custom data objects
   *
   * @param {string} message - Error message
   * @param {Error|Object} errorOrData - Error object or custom data
   * @returns {void}
   */
  error: function(message, errorOrData) {
    let errorInfo;

    // Check if it's an Error object (has message property and possibly stack)
    if (errorOrData && (errorOrData.message || errorOrData.stack)) {
      // It's an Error object - extract error details
      errorInfo = {
        message: errorOrData.message,
        code: errorOrData.details?.code,
        stack: errorOrData.stack
      };
    } else {
      // It's a plain data object - use it directly
      errorInfo = errorOrData || {};
    }

    const log = {
      level: 'ERROR',
      timestamp: new Date().toISOString(),
      message: message,
      error: errorInfo
    };
    console.error(JSON.stringify(log));
    this._logs.push(log);
  },

  /**
   * Logs warning message
   * @param {string} message - Warning message
   * @param {Object} data - Additional data to log
   * @returns {void}
   */
  warn: function(message, data) {
    const log = {
      level: 'WARN',
      timestamp: new Date().toISOString(),
      message: message,
      data: data || {}
    };
    console.warn(JSON.stringify(log));
    this._logs.push(log);
  },

  /**
   * Gets summary of logged messages
   * @returns {Object} Summary with total, error count, and warning count
   */
  getSummary: function() {
    return {
      total: this._logs.length,
      errors: this._logs.filter(l => l.level === 'ERROR').length,
      warnings: this._logs.filter(l => l.level === 'WARN').length,
      info: this._logs.filter(l => l.level === 'INFO').length
    };
  },

  /**
   * Gets all logged messages
   * @returns {Array<Object>} Array of all log entries
   */
  getAllLogs: function() {
    return this._logs;
  },

  /**
   * Clears all stored logs
   * @returns {void}
   */
  clearLogs: function() {
    this._logs = [];
  }
};

/**
 * Quick test to confirm the current CAPWATCH data folder ID in use.
 * Run manually via Run → confirmConfigFolderId
 */
function confirmConfigFolderId() {
  console.log('CONFIG.CAPWATCH_DATA_FOLDER_ID:', CONFIG.CAPWATCH_DATA_FOLDER_ID);
}

function testDriveHealth() {
  const res = UrlFetchApp.fetch('https://www.googleapis.com/drive/v3/about?fields=user,storageQuota', {
    headers: {Authorization: 'Bearer ' + ScriptApp.getOAuthToken()},
    muteHttpExceptions: true
  });
  console.log('Drive service check:', res.getResponseCode(), res.getContentText());
}

function testDriveV3Direct() {
  const folderId = CONFIG.CAPWATCH_DATA_FOLDER_ID;
  const token = ScriptApp.getOAuthToken();
  const res = UrlFetchApp.fetch(`https://www.googleapis.com/drive/v3/files/${folderId}?supportsAllDrives=true&fields=id,name,kind`, {
    method: 'get',
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  });
  console.log(res.getResponseCode(), res.getContentText());
}
