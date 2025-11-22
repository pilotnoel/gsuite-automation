/**
 * CAPWATCH Data Download Automation
 *
 * This script automates the download of CAPWATCH data from eServices API
 * and updates files in a specified Google Drive folder.
 *
 * Authors: Jeremy Ginnard, jginnard@a2cap.org
 *          Luke Bunge, luke.bunge@miwg.cap.gov
 *
 * Setup Instructions:
 * Step 1: Temporarily set username and password variables, then run setAuthorization()
 * Step 2: The authorization token is stored securely in User Properties
 * Step 3: Verify CONFIG.CAPWATCH_DATA_FOLDER_ID points to correct folder with write permission
 * Step 4: Set up a time-driven trigger for getCapwatch():
 *         - Navigate to Current Project's Triggers
 *         - Add Trigger
 *         - Choose: getCapwatch, Head, Time-driven, Day timer, Select time (recommend overnight)
 *
 * Security Note:
 * Credentials are stored using PropertiesService.getUserProperties(), which is:
 * - Accessible only by the user who set them
 * - Not shared with other users or scripts
 * - Persists across script executions
 */

/**
 * Validates that CAPWATCH credentials are properly configured
 * @returns {string} The Base64-encoded authorization token
 * @throws {Error} If CAPWATCH_AUTHORIZATION property is not set
 */
function checkCredentials() {
  const userProperties = PropertiesService.getUserProperties();
  const auth = userProperties.getProperty('CAPWATCH_AUTHORIZATION');

  if (!auth) {
    throw new Error('CAPWATCH_AUTHORIZATION not set. Run setAuthorization() first.');
  }

  return auth;
}

/**
 * Downloads CAPWATCH data from eServices API and updates Google Drive files
 *
 * This function:
 * 1. Validates credentials are configured
 * 2. Fetches CAPWATCH data as a ZIP file from the API
 * 3. Extracts the ZIP contents
 * 4. Updates existing files or creates new ones in the configured folder
 *
 * @returns {void}
 * @throws {Error} If credentials are invalid or API call fails
 */
function getCapwatch() {
  console.log("🚀 Running GetCapwatch build 1.3");

  const AUTHORIZATION = checkCredentials();

  // ────────────────────────────────────────────────
  // Determine if this is a Region-level run
  // (If CONFIG.REGION is not empty, set unitOnly = 1)
  // ────────────────────────────────────────────────
  const isRegion = CONFIG.REGION && CONFIG.REGION !== "";
  const unitOnlyParam = isRegion ? "1" : "0";

  const url = 'https://www.capnhq.gov/CAP.CapWatchAPI.Web/api/cw?ORGID=' +
              CONFIG.CAPWATCH_ORGID + '&unitOnly=' + unitOnlyParam;

  try {
    // Log what’s being fetched, including Region setting
    console.info(`📡 Fetching CAPWATCH URL: ${url} (Region: ${CONFIG.REGION || "none"}, unitOnly=${unitOnlyParam})`);

    // ────────────────────────────────────────────────
    // Delete any older CAPWATCH ZIPs in the same folder
    // Keep only the newest one (by name or modified date)
    // ────────────────────────────────────────────────
    const cleanupFolder = DriveApp.getFolderById(CONFIG.CAPWATCH_DATA_FOLDER_ID);
    const filesIter = cleanupFolder.getFiles();
    let zipCount = 0;

    while (filesIter.hasNext()) {
      const file = filesIter.next();
      const name = file.getName();
      if (name.toLowerCase().endsWith('.zip')) {
        zipCount++;
        if (file.getLastUpdated() < new Date(Date.now() - 1000 * 60 * 5)) { // older than 5 min
          console.log(`🗑️ Removing old CAPWATCH ZIP: ${name}`);
          file.setTrashed(true);
        }
      }
    }

    console.log(`🧹 Cleaned up ${zipCount - 1} old CAPWATCH ZIP(s), keeping the newest.`);

    // Make the API call using headers that match Python's working behavior
    const response = executeWithRetry(() =>
      UrlFetchApp.fetch(url, {
        headers: {
          "Authorization": "Basic " + AUTHORIZATION,
          "Accept": "*/*",
          "User-Agent": "python-requests/2.31.0"
        },
        muteHttpExceptions: false
      })
    );
    const files = Utilities.unzip(response.getBlob());

    const zipSizeMB = response.getBlob().getBytes().length / (1024 * 1024);
    console.log(`📦 CAPWATCH ZIP size: ${zipSizeMB.toFixed(1)} MB`);

    const folder = DriveApp.getFolderById(CONFIG.CAPWATCH_DATA_FOLDER_ID);

    let updated = 0;
    let created = 0;

    files.forEach(function(blob) {
      const name = blob.getName();
      const sizeMB = blob.getBytes().length / (1024 * 1024);

      // Skip oversized CAPWATCH text files (>50 MB)
      if (sizeMB > 50) {
        console.warn(`⚠️ Skipping ${name} (${sizeMB.toFixed(1)} MB) — exceeds Apps Script limit`);
        return;
      }

      const existingFiles = folder.getFilesByName(name);
      if (existingFiles.hasNext()) {
        const file = existingFiles.next();
        file.setContent(blob.getDataAsString());
        Logger.info('CAPWATCH file updated', { fileName: name });
        updated++;
      } else {
        folder.createFile(blob);
        Logger.info('CAPWATCH file created', { fileName: name });
        created++;
      }
    });

    Logger.info('CAPWATCH data download completed', {
      updated: updated,
      created: created,
      totalFiles: files.length,
      orgid: CONFIG.CAPWATCH_ORGID
    });
  } catch (e) {
    Logger.error('Failed to download CAPWATCH data', {
      errorMessage: e.message,
      errorCode: e.details?.code,
      orgid: CONFIG.CAPWATCH_ORGID,
      url: url
    });
    throw e;
  }
}

/**
 * Encodes eServices credentials and stores them securely
 *
 * IMPORTANT: This function should only be run once during initial setup.
 * After running, immediately clear the username and password variables
 * from this code for security.
 *
 * The encoded authorization token is stored in User Properties and will
 * persist for future executions without needing to store credentials in code.
 *
 * @returns {void}
 */
function setAuthorization(){
  let username = ''; // Set your eServices username here temporarily
  let password = ''; // Set your eServices password here temporarily

  if (!username || !password) {
    Logger.error('Username and password must be set', {
      note: 'Edit this function to add credentials, run it once, then clear them'
    });
    throw new Error('Username and password are required. Edit setAuthorization() to add them.');
  }

  let authorization = Utilities.base64Encode(username + ':' + password);
  let userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('CAPWATCH_AUTHORIZATION', authorization);

  Logger.info('Authorization token saved', {
    note: 'Remember to clear username and password from code now'
  });

  // Security reminder
  console.log('✅ Authorization saved successfully!');
  console.log('⚠️  IMPORTANT: Clear the username and password from this function now for security.');
}

/**
 * Test function to verify CAPWATCH download works
 * @returns {void}
 */
function testGetCapwatch() {
  try {
    getCapwatch();
    Logger.info('Test completed successfully');
  } catch (e) {
    Logger.error('Test failed', e);
  }
}