/**
 * Member Synchronization Module
 *
 * Manages synchronization between CAPWATCH data and Google Workspace:
 * - Retrieves and parses CAPWATCH member data
 * - Creates/updates Google Workspace user accounts
 * - Manages email aliases
 * - Suspends expired members
 * - Reactivates renewed members (including archived)
 * - Tracks changes for efficient updates
 */

/**
 * Gets all squadrons for the configured wing from CAPWATCH data
 * Includes both regular squadrons and special units (e.g., AEM)
 *
 * @returns {Object} Squadron data indexed by orgid with properties:
 *   - orgid: Organization ID
 *   - name: Squadron name
 *   - charter: Charter number (e.g., "NER-MI-100")
 *   - unit: Unit number
 *   - nextLevel: Parent organization ID
 *   - scope: Organization scope (UNIT, GROUP, WING)
 *   - wing: Wing abbreviation
 *   - orgPath: Google Workspace organizational unit path
 */
function getSquadrons() {
  let squadrons = {};
  let squadronData = parseFile('Organization');

  for (let i = 0; i < squadronData.length; i++) {
    if (squadronData[i][2] === CONFIG.WING) {
      squadrons[squadronData[i][0]] = {
        orgid: squadronData[i][0],
        name: squadronData[i][5],
        charter: Utilities.formatString("%s-%s-%03d", squadronData[i][1], squadronData[i][2], squadronData[i][3]),
        unit: squadronData[i][3],
        nextLevel: squadronData[i][4],
        scope: squadronData[i][9],
        wing: squadronData[i][2],
        orgPath: ''
      }
    }
  }

  // Create artificial AEM Unit using MIWG as template
  squadrons[CONFIG.SPECIAL_ORGS.AEM_UNIT] = Object.assign(
    {},
    squadrons[CONFIG.CAPWATCH_ORGID],
    {
      orgid: CONFIG.SPECIAL_ORGS.AEM_UNIT,
      name: "Aerospace Education Members"
    }
  );

  // Add organizational unit paths from OrgPaths file
  let orgPaths = parseFile('OrgPaths');
  for (let i = 0; i < orgPaths.length; i++) {
    if (squadrons[orgPaths[i][0]]) {
      squadrons[orgPaths[i][0]].orgPath = orgPaths[i][1];
    }
  }

  return squadrons;
}

/**
 * Retrieves and processes member data from CAPWATCH files
 *
 * This is the main data retrieval function that:
 * 1. Parses member data from CAPWATCH files
 * 2. Filters by member type and status
 * 3. Validates member data
 * 4. Adds contact information
 * 5. Optionally adds duty positions
 *
 * @param {string[]} types - Member types to include (default: all active types)
 * @param {boolean} includeDutyPositions - Whether to parse duty positions (default: true)
 * @returns {Object} Members object indexed by CAPID
 */

/**
 * Loads manual members from the 'ManualMembers' sheet and returns an object indexed by CAPID.
 * Each object is formatted to match the structure of CAPWATCH members.
 *
 * @param {Object} squadrons - Squadron lookup object
 * @returns {Object} Manual members indexed by CAPID
 */
function loadManualMembers(squadrons) {
  const sheet = SpreadsheetApp.openById(CONFIG.AUTOMATION_SPREADSHEET_ID).getSheetByName('Manual Members');
  if (!sheet) return {};

  const rows = sheet.getDataRange().getValues();
  const header = rows[0];
  const members = {};

  for (let i = 1; i < rows.length; i++) {
    const row = {};
    for (let j = 0; j < header.length; j++) {
      row[header[j]] = rows[i][j];
    }

    const orgid = row['OrgID'];
    const squadron = squadrons[orgid];
    if (!squadron) continue;

    const dutyId = (row['DutyID'] || '').toString().trim();

    members[row['CAPID']] = {
      capsn: String(row['CAPID']),
      lastName: row['LastName'],
      firstName: row['FirstName'],
      orgid: orgid,
      group: calculateGroup(orgid, squadrons),
      charter: squadron.charter,
      orgName: squadron.name,
      rank: row['Rank'] || '',
      type: row['Type'] || 'SENIOR',
      status: row['Status'] || 'ACTIVE',
      middleName: row['MiddleName'] || '',
      suffix: row['Suffix'] || '',
      modified: new Date().toISOString(),
      orgPath: squadron.orgPath,
      email: row['Email'] || null,

      dutyPositions: dutyId
        ? [{ id: dutyId, assistant: row['Assistant'] == '1' }]
        : [],

      dutyPositionIds: dutyId ? [dutyId] : [],

      dutyPositionIdsAndLevel: dutyId ? [dutyId + '_4'] : []
    };
  }

  return members;
}
function getMembers(types = CONFIG.MEMBER_TYPES.ACTIVE, includeDutyPositions = true) {
  const start = new Date();
  const members = {};
  const squadrons = getSquadrons();

  Logger.info('Starting member data retrieval', { types: types });

  // Build member objects from Member.txt
  const memberData = parseFile('Member');
  let processedCount = 0;

  for (let i = 0; i < memberData.length; i++) {
    if (shouldProcessMember(memberData[i], types)) {
      const member = createMemberObject(memberData[i], squadrons);

      // Validate before adding
      const validation = validateMember(member);
      if (validation.isValid) {
        members[memberData[i][0]] = member;
        processedCount++;
      } else {
        Logger.warn('Invalid member data', {
          capsn: memberData[i][0],
          errors: validation.errors
        });
      }
    }
  }

  Logger.info('Members parsed', {
    count: processedCount,
    duration: new Date() - start + 'ms'
  });

  // Add contact information from MbrContact.txt
  const contactStart = new Date();
  addContactInfo(members, parseFile('MbrContact'));
  Logger.info('Contact info added', {
    duration: new Date() - contactStart + 'ms'
  });

  // Add duty positions if requested
  if (includeDutyPositions) {
    const dutyStart = new Date();
    addDutyPositions(members, parseFile('DutyPosition'), squadrons);
    addCadetDutyPositions(members, parseFile('CadetDutyPositions'), squadrons);
    Logger.info('Duty positions added', {
      duration: new Date() - dutyStart + 'ms'
    });
  }

  // Merge in manual members from the ManualMembers sheet
  const manualMembers = loadManualMembers(squadrons);
  Object.assign(members, manualMembers);
  Logger.info('Manual members merged', { count: Object.keys(manualMembers).length });

  Logger.info('Member retrieval completed', {
    totalMembers: Object.keys(members).length,
    totalDuration: new Date() - start + 'ms'
  });

  return members;
}

/**
 * Determines if a member should be processed based on status and type
 *
 * @param {Array} memberRow - Raw member data row from CSV
 * @param {string[]} types - Valid member types to include
 * @returns {boolean} True if member should be processed
 */
function shouldProcessMember(memberRow, types) {
  return memberRow[24] === 'ACTIVE' &&
         memberRow[13] != 0 &&
         memberRow[13] != 999 &&
         types.indexOf(memberRow[21]) > -1;
}

/**
 * Creates a structured member object from raw CAPWATCH data
 *
 * @param {Array} memberRow - Raw member data row from CSV
 * @param {Object} squadrons - Squadron lookup object
 * @returns {Object} Formatted member object with all required fields
 */
function createMemberObject(memberRow, squadrons) {
  return {
    capsn: memberRow[0],
    lastName: memberRow[2],
    firstName: memberRow[3],
    middleName: memberRow[4] || '',
    suffix: memberRow[5] || '',
    orgid: memberRow[11],
    group: calculateGroup(memberRow[11], squadrons),
    charter: squadrons[memberRow[11]].charter,
    orgName: squadrons[memberRow[11]].name,
    rank: memberRow[14],
    type: memberRow[21],
    status: memberRow[24],
    modified: memberRow[19],
    orgPath: squadrons[memberRow[11]].orgPath,
    email: null,
    dutyPositions: [],
    dutyPositionIds: [],
    dutyPositionIdsAndLevel: []
  };
}

function addContactInfo(members, contactData) {
  for (let i = 0; i < contactData.length; i++) {
    const capid = contactData[i][0];
    const type = contactData[i][1]?.toUpperCase() || '';
    const priority = contactData[i][2]?.toUpperCase() || '';
    const contact = contactData[i][3]?.trim() || '';
    const doNotContact = contactData[i][6]?.toUpperCase() === 'TRUE';

    if (!members[capid] || priority !== 'PRIMARY' || doNotContact) continue;

    if (type === 'EMAIL') {
      const email = sanitizeEmail(contact);
      if (email) members[capid].email = email;
    }

    if (type.includes('CELL') || type === 'PHONE') {
      const digits = contact.replace(/\D/g, '');
      if (digits.length >= 10) {
        members[capid].phone = `+1${digits.slice(-10)}`;
      }
    }
  }

  Logger.info('Contact info added (email + phone)', {
    totalMembers: Object.keys(members).length
  });
}

/**
 * Adds senior member duty positions to member objects
 *
 * @param {Object} members - Members object indexed by CAPID
 * @param {Array} dutyPositionData - Parsed duty position data
 * @param {Object} squadrons - Squadron lookup object
 * @returns {void}
 */

function addDutyPositions(members, dutyPositionData, squadrons) {
  for (let i = 0; i < dutyPositionData.length; i++) {
    if (members[dutyPositionData[i][0]]) {
      let dutyPositionID = dutyPositionData[i][1].trim();
      members[dutyPositionData[i][0]].dutyPositions.push({
        value: Utilities.formatString("%s (%s) (%s)",
          dutyPositionID,
          (dutyPositionData[i][4] == '1' ? 'A' : 'P'),
          squadrons[dutyPositionData[i][7]].charter),
        id: dutyPositionID,
        level: dutyPositionData[i][3],
        assistant: dutyPositionData[i][4] == '1'
      });
      members[dutyPositionData[i][0]].dutyPositionIds.push(dutyPositionID);
      members[dutyPositionData[i][0]].dutyPositionIdsAndLevel.push(
        dutyPositionID + '_' + dutyPositionData[i][3]
      );
    }
  }
}

/**
 * Assigns manager email for each member based on their unit commander from Commanders.txt
 * @param {Object} members - Members object indexed by CAPID
 */
function assignManagerEmails(members) {
  const commandersData = parseFile('Commanders');
  const commanders = {};

  // Build commander map: ORGID → commander email (ORGID = col 1, CAPID = col 5)
  for (let i = 0; i < commandersData.length; i++) {
    const orgid = commandersData[i][0];
    const commanderCAPID = commandersData[i][4];
    if (members[commanderCAPID]) {
      const commander = members[commanderCAPID];
      const email = `${commander.firstName.toLowerCase()}.${commander.lastName.toLowerCase()}@${CONFIG.DOMAIN}`;
      commanders[orgid] = email;
    }
  }

  // Assign managerEmail for each member in same org
  for (const capid in members) {
    const m = members[capid];
    m.managerEmail = commanders[m.orgid] || '';
  }

  Logger.info('Manager emails assigned', { count: Object.keys(commanders).length });
}

/**
 * Adds cadet duty positions to member objects
 *
 * @param {Object} members - Members object indexed by CAPID
 * @param {Array} cadetDutyPositionData - Parsed cadet duty position data
 * @param {Object} squadrons - Squadron lookup object
 * @returns {void}
 */
function addCadetDutyPositions(members, cadetDutyPositionData, squadrons) {
  for (let i = 0; i < cadetDutyPositionData.length; i++) {
    if (members[cadetDutyPositionData[i][0]]) {
      members[cadetDutyPositionData[i][0]].dutyPositions.push({
        value: Utilities.formatString("%s (%s) (%s)",
          cadetDutyPositionData[i][1],
          (cadetDutyPositionData[i][4] == '1' ? 'A' : 'P'),
          squadrons[cadetDutyPositionData[i][7]].charter)
      });
      members[cadetDutyPositionData[i][0]].dutyPositionIds.push(
        cadetDutyPositionData[i][1]
      );
    }
  }
}

/**
 * Retrieves Aerospace Education Members only
 * Convenience function that calls getMembers with AEM filter
 *
 * @returns {Object} AEM members object indexed by CAPID
 */
function getAEMembers() {
  return getMembers(CONFIG.MEMBER_TYPES.AEM_ONLY, false);
}

/**
 * Retrieves previously saved member data from Drive
 * Used to detect changes and avoid unnecessary API calls
 *
 * @returns {Object} Previously saved member data or empty object
 */
function getCurrentMemberData() {
  let folder = DriveApp.getFolderById(CONFIG.CAPWATCH_DATA_FOLDER_ID);
  let files = folder.getFilesByName('CurrentMembers.txt');

  if (files.hasNext()) {
    let content = files.next().getBlob().getDataAsString();
    if (content) {
      try {
        return JSON.parse(content);
      } catch (e) {
        Logger.error('Failed to parse CurrentMembers.txt', { errorMessage: e.message });
        return {};
      }
    }
  }

  Logger.warn('CurrentMembers.txt not found or empty');
  return {};
}

/**
 * Saves current member data to Drive for change detection
 *
 * @param {Object} currentMembers - Current member data to save
 * @returns {void}
 */
function saveCurrentMemberData(currentMembers) {
  let folder = DriveApp.getFolderById(CONFIG.CAPWATCH_DATA_FOLDER_ID);
  let files = folder.getFilesByName('CurrentMembers.txt');

  if (files.hasNext()) {
    let file = files.next();
    let content = JSON.stringify(currentMembers);
    file.setContent(content);
    Logger.info('Current member data saved', {
      memberCount: Object.keys(currentMembers).length,
      fileName: 'CurrentMembers.txt'
    });
  } else {
    Logger.warn('CurrentMembers.txt not found - cannot save', {
      folderId: CONFIG.CAPWATCH_DATA_FOLDER_ID
    });
  }
}

/**
 * Checks if a member's data has changed since last update
 * Compares rank, charter, duty positions, status, and email
 *
 * @param {Object} newMember - New member data
 * @param {Object} previousMember - Previously saved member data
 * @returns {boolean} True if member data has changed or is new
 */
function memberUpdated(newMember, previousMember) {
  return (!newMember || !previousMember) ||
         (newMember.rank !== previousMember.rank ||
          newMember.charter !== previousMember.charter ||
          newMember.dutyPositions.join('') !== previousMember.dutyPositions.join('') ||
          newMember.status !== previousMember.status ||
          newMember.email !== previousMember.email);
}

/**
 * Updates or creates a Google Workspace user for a CAP member
 *
 * Process:
 * 1. Attempts to update existing user
 * 2. If not found, creates new user
 * 3. Adds email alias for new users
 * 4. Suspends users in excluded organizations
 *
 * @param {Object} member - Member object containing CAP data
 * @returns {void}
 */
function addOrUpdateUser(member) {
  const baseEmail = `${member.firstName}.${member.lastName}`.toLowerCase().replace(/\s+/g, '');
  let primaryEmail = baseEmail + CONFIG.EMAIL_DOMAIN;
  let user;

  let updates = {
    employeeId: String(member.capsn),
    externalIds: [{ type: 'organization', value: String(member.capsn) }],
    organizations: [{
      title: member.dutyPositions && member.dutyPositions.length > 0
        ? member.dutyPositions.filter(d => !d.assistant).map(d => d.id).join(', ')
        : 'Member',
      // Squadron / Unit Name (Organization.name)
      department: toTitleCase(member.orgName || member.charter || ''),
      // Charter (PCR-HI-077 etc.)
      costCenter: member.charter || '',
      description: member.type || '',
      type: 'work',
      primary: true
    }],
    orgUnitPath: member.orgPath,
    recoveryEmail: member.email,
    phones: member.phone ? [{ type: 'mobile', value: member.phone }] : [],
    emails: member.email ? [{ type: 'other', address: member.email }] : [],
    suspended: CONFIG.EXCLUDED_ORG_IDS.includes(String(member.orgid)),
    name: {
      givenName: member.firstName,
      familyName: member.lastName,
      middleName: member.middleName || '',
      fullName: [
        member.firstName,
        member.middleName || '',
        member.lastName,
        member.suffix || ''
      ].filter(Boolean).join(' '),
      displayName: [
        member.lastName + (member.suffix ? ' ' + member.suffix : ''),
        ', ',
        member.firstName,
        member.middleName ? ' ' + member.middleName : '',
        member.rank ? ' ' + member.rank : ''
      ].join('').trim()
    },
    relations: [{
      type: 'manager',
      value: member.managerEmail || ''
    }],
    customSchemas: {
      CAP: {
        Rank: member.rank || ''
      }
    }
  };

  // Try updating existing user
  try {
    user = executeWithRetry(() =>
      AdminDirectory.Users.update(updates, primaryEmail)
    );
    Logger.info('User updated', {
      email: primaryEmail,
      capsn: member.capsn
    });
    // Build Gmail Send-As display name
    const sendAsDisplayName = [
      member.lastName + (member.suffix ? ' ' + member.suffix : ''),
      ', ',
      member.firstName,
      member.middleName ? ' ' + member.middleName : '',
      member.rank ? ' ' + member.rank : ''
    ].join('').trim();

    // Update user's Send-As display name
    try {
      updateGmailSendAsDisplayName(primaryEmail, sendAsDisplayName);
    } catch (err) {
      Logger.error('Send-As update failed', {
        email: primaryEmail,
        errorMessage: err.message
      });
    }
  } catch (e) {
    if (String(e.message).includes("Resource Not Found")) {
      // Possible archived user — attempt to fetch
      let archivedCheck = null;
      try {
        archivedCheck = AdminDirectory.Users.get(primaryEmail, { projection: "full" });
      } catch (ignored) {}

      if (archivedCheck && archivedCheck.archived) {
        try {
          AdminDirectory.Users.update({ archived: false, suspended: false }, primaryEmail);
          user = AdminDirectory.Users.update(updates, primaryEmail);
          Logger.info("Archived user restored and updated", {
            email: primaryEmail,
            capsn: member.capsn
          });
          return;
        } catch (err2) {
          Logger.error("Failed to unarchive/update archived user", {
            email: primaryEmail,
            capsn: member.capsn,
            errorMessage: err2.message
          });
        }
      }

      // treat as non-existent user
      user = null;
    } else {
      Logger.error('Unable to update user', {
        email: primaryEmail,
        capsn: member.capsn,
        name: member.firstName + ' ' + member.lastName,
        orgid: member.orgid,
        charter: member.charter,
        orgPath: member.orgPath,
        errorMessage: e.message,
        errorCode: e.details?.code
      });
    }
  }

  // Create new user if update failed
  if (!user) {
    user = {
      employeeId: String(member.capsn),
      externalIds: [{ type: 'organization', value: String(member.capsn) }],
      organizations: [{
        title: member.dutyPositions && member.dutyPositions.length > 0
          ? member.dutyPositions.filter(d => !d.assistant).map(d => d.id).join(', ')
          : 'Member',
        // Squadron / Unit Name (Organization.name)
        department: member.orgName || member.charter || '',
        // Charter (PCR-HI-077 etc.)
        costCenter: member.charter || '',
        description: member.type || '',
        type: 'work',
        primary: true
      }],
      primaryEmail: primaryEmail,
      name: {
        givenName: member.firstName,
        familyName: member.lastName,
        middleName: member.middleName || '',
        fullName: [
          member.firstName,
          member.middleName || '',
          member.lastName,
          member.suffix || ''
        ].filter(Boolean).join(' '),
        displayName: [
          member.lastName + (member.suffix ? ' ' + member.suffix : ''),
          ', ',
          member.firstName,
          member.middleName ? ' ' + member.middleName : '',
          member.rank ? ' ' + member.rank : ''
        ].join('').trim()
      },
      suspended: CONFIG.EXCLUDED_ORG_IDS.includes(String(member.orgid)),
      changePasswordAtNextLogin: true,
      password: Math.random().toString(36),
      orgUnitPath: member.orgPath,
      recoveryEmail: member.email,
      phones: member.phone ? [{ type: 'mobile', value: member.phone }] : [],
      emails: member.email ? [{ type: 'other', address: member.email }] : [],
      relations: [{
        type: 'manager',
        value: member.managerEmail || ''
      }],
      customSchemas: {
        CAP: {
          Rank: member.rank || ''
        }
      }
    };

    try {
      let newUser = executeWithRetry(() =>
        AdminDirectory.Users.insert(user)
      );
      Logger.info('New user created', {
        email: primaryEmail,
        capsn: member.capsn,
        name: member.firstName + ' ' + member.lastName
      });

      if (CONFIG.UPDATE_SIGNATURE_ON_NEW_USERS_ONLY) {
        try {
          const signatureHTML = generateEmailSignature(member);
          updateSignatureForAllAliases(primaryEmail, signatureHTML);
        } catch (err) {
          Logger.error('Signature update failed for new user', {
            email: primaryEmail,
            error: err.message
          });
        }
      }

      // Build Gmail Send-As display name
      const sendAsDisplayName = [
        member.lastName + (member.suffix ? ' ' + member.suffix : ''),
        ', ',
        member.firstName,
        member.middleName ? ' ' + member.middleName : '',
        member.rank ? ' ' + member.rank : ''
      ].join('').trim();

      // Update user's Send-As display name
      try {
        updateGmailSendAsDisplayName(primaryEmail, sendAsDisplayName);
      } catch (err) {
        Logger.error('Send-As update failed', {
          email: primaryEmail,
          errorMessage: err.message
        });
      }
      // Alias insertion for CAPID removed.
    } catch (e) {
      Logger.error('Failed to create new user', {
        email: primaryEmail,
        capsn: member.capsn,
        name: member.firstName + ' ' + member.lastName,
        orgid: member.orgid,
        charter: member.charter,
        orgPath: member.orgPath,
        errorMessage: e.message,
        errorCode: e.details?.code
      });
    }
  }
}

/**
 * Gets all active members from CAPWATCH data
 * Returns simplified object with just CAPID and join date
 *
 * @returns {Object} Active members indexed by CAPID with join date values
 */
function getActiveMembers() {
  let activeMembers = {};
  let memberData = parseFile('Member');

  for (let i = 0; i < memberData.length; i++) {
    if (memberData[i][24] === 'ACTIVE') {
      activeMembers[memberData[i][0]] = memberData[i][16];
    }
  }

  Logger.info('Active members retrieved', {
    count: Object.keys(activeMembers).length
  });
  return activeMembers;
}

/**
 * Suspends a Google Workspace user account
 *
 * @param {string} email - User's email address
 * @returns {boolean} True if suspension successful, false otherwise
 */
function suspendMember(email) {
  try {
    executeWithRetry(() =>
      AdminDirectory.Users.update({suspended: true}, email)
    );
    Logger.info('Member suspended', { email: email });
    return true;
  } catch (e) {
    Logger.error('Error suspending member', {
      email: email,
      errorMessage: e.message,
      errorCode: e.details?.code
    });
    return false;
  }
}

/**
 * Retrieves all active (non-suspended) users from Google Workspace
 * Filters to non-admin users in /MI-001 organizational unit
 *
 * @returns {Array<Object>} Array of user objects with email, capid, and lastUpdated
 */
function getActiveUsers() {
  let activeUsers = [];
  let nextPageToken = '';

  do {
    let page = AdminDirectory.Users.list({
      domain: CONFIG.DOMAIN,
      maxResults: 500,
      query: 'isSuspended=false isAdmin=false orgUnitPath=/MI-001',
      fields: 'users(primaryEmail,externalIds),nextPageToken',
      pageToken: nextPageToken
    });

    nextPageToken = page.nextPageToken;

    if (page.users) {
      for (let i = 0; i < page.users.length; i++) {
        const ids = page.users[i].externalIds || [];
        const capidExt = ids.find(id => id.type === 'organization');

        if (capidExt) {
          activeUsers.push({
            email: page.users[i].primaryEmail,
            capid: capidExt.value,
            lastUpdated: null // no schema anymore
          });
        }
      }
    }
  } while (nextPageToken);

  Logger.info('Active users retrieved from Workspace', {
    count: activeUsers.length
  });
  return activeUsers;
}

/**
 * Main function to update all member accounts in Google Workspace
 *
 * Process:
 * 1. Retrieves current CAPWATCH member data
 * 2. Compares with previously saved data
 * 3. Updates only changed members
 * 4. Saves current data for future comparison
 * 5. Logs progress every 100 members
 *
 * @returns {void}
 */
function updateAllMembers() {
  clearCache(); // Clear cache for fresh data
  const start = new Date();

  Logger.info('Starting member update process');

  let members = getMembers();
  let currentMembers = getCurrentMemberData();
  assignManagerEmails(members);
  const totalMembers = Object.keys(members).length;

  let processed = 0;

  // Build reduced set of members whose data changed
  let toProcess = {};
  let skipped = 0;

  for (const capsn in members) {
    if (memberUpdated(members[capsn], currentMembers[capsn])) {
      toProcess[capsn] = members[capsn];
    } else {
      skipped++;
    }
  }

  Logger.info("Batch update starting", {
    changedMembers: Object.keys(toProcess).length,
    skipped: skipped,
    total: totalMembers
  });

  // 🔥 MAIN CHANGE — batch processing instead of inline updates
  batchUpdateMembers(toProcess);

  // 👉 Alias updates — from sheet tab Aliases
  //addAliasesFromSheet();

  saveCurrentMemberData(members);

  // Reactivate any members who renewed
  Logger.info('Checking for renewed members to reactivate');
  const reactivationStart = new Date();
  let totalReactivated = 0;

  try {
    // Get inactive users before calling reactivateRenewedMembers
    const activeMembers = getActiveMembers();
    const inactiveUsers = getInactiveUsers();
    let reactivated = 0;
    let unarchived = 0;

    for (let i = 0; i < inactiveUsers.length; i++) {
      const user = inactiveUsers[i];

      if (user.capid && (user.capid in activeMembers)) {
        const wasArchived = user.archived;
        const success = reactivateMember(user.email, wasArchived);

        if (success) {
          if (wasArchived) {
            unarchived++;
          } else {
            reactivated++;
          }
        }
      }
    }

    totalReactivated = reactivated + unarchived;

    Logger.info('Renewed member reactivation completed', {
      duration: new Date() - reactivationStart + 'ms',
      reactivated: reactivated,
      unarchived: unarchived,
      total: totalReactivated
    });
  } catch (err) {
    Logger.error('Reactivation check failed', {
      errorMessage: err.message
    });
  }

  Logger.info('Member update completed', {
    duration: new Date() - start + 'ms',
    totalProcessed: processed,
    updated: Object.keys(toProcess).length,
    skipped: skipped,
    reactivated: totalReactivated
  });
}

/**
 * Suspends Google Workspace accounts for members who are no longer active in CAPWATCH
 *
 * Process:
 * 1. Gets active members from CAPWATCH
 * 2. Gets active users from Google Workspace
 * 3. Identifies users not in CAPWATCH
 * 4. Suspends after grace period expires
 *
 * @returns {void}
 */
function suspendExpiredMembers() {
  const start = new Date();
  Logger.info('Starting expired member suspension process');

  let activeMembers = getActiveMembers();
  let users = getActiveUsers();
  let suspended = 0;
  let pending = 0;
  const suspensionTime = new Date().getTime() - (CONFIG.SUSPENSION_GRACE_DAYS * 86400000);

  for(let i = 0; i < users.length; i++) {
    if (users[i].capid && !(users[i].capid in activeMembers)) {
      if (!users[i].lastUpdated || suspensionTime > new Date(users[i].lastUpdated).getTime()) {
        let success = suspendMember(users[i].email);
        if (success) {
          suspended++;
        }
      } else {
        Logger.info('Member expired - pending suspension', {
          email: users[i].email,
          capid: users[i].capid,
          lastUpdated: users[i].lastUpdated,
          graceDaysRemaining: Math.ceil((new Date(users[i].lastUpdated).getTime() + (CONFIG.SUSPENSION_GRACE_DAYS * 86400000) - new Date().getTime()) / 86400000)
        });
        pending++;
      }
    }
  }

  Logger.info('Expired member suspension completed', {
    duration: new Date() - start + 'ms',
    suspended: suspended,
    pending: pending,
    graceDays: CONFIG.SUSPENSION_GRACE_DAYS
  });
}

/**
 * Reactivates Google Workspace accounts for members who renewed after being suspended or archived
 *
 * Process:
 * 1. Gets active members from CAPWATCH
 * 2. Gets suspended/archived users from Google Workspace
 * 3. Identifies users who are now active in CAPWATCH
 * 4. Unsuspends and/or unarchives them
 *
 * This handles both:
 * - Members who renewed within 1 year (suspended only)
 * - Members who renewed after 1+ year (archived)
 *
 * @returns {void}
 */
function reactivateRenewedMembers() {
  const start = new Date();
  Logger.info('Starting renewed member reactivation process');

  const activeMembers = getActiveMembers();
  const inactiveUsers = getInactiveUsers();
  let reactivated = 0;
  let unarchived = 0;
  let failed = 0;

  for (let i = 0; i < inactiveUsers.length; i++) {
    const user = inactiveUsers[i];

    // Check if user is now active in CAPWATCH
    if (user.capid && (user.capid in activeMembers)) {
      const wasArchived = user.archived;
      const success = reactivateMember(user.email, wasArchived);

      if (success) {
        if (wasArchived) {
          unarchived++;
          Logger.info('Archived member reactivated', {
            email: user.email,
            capid: user.capid,
            wasArchived: true
          });
        } else {
          reactivated++;
          Logger.info('Suspended member reactivated', {
            email: user.email,
            capid: user.capid
          });
        }
      } else {
        failed++;
      }
    }
  }

  Logger.info('Renewed member reactivation completed', {
    duration: new Date() - start + 'ms',
    reactivated: reactivated,
    unarchived: unarchived,
    failed: failed,
    total: reactivated + unarchived
  });
}

/**
 * Reactivates a Google Workspace user account
 * Handles both suspended and archived users
 *
 * @param {string} email - User's email address
 * @param {boolean} wasArchived - Whether the user was archived (vs just suspended)
 * @returns {boolean} True if reactivation successful, false otherwise
 */
function reactivateMember(email, wasArchived = false) {
  try {
    const updateObject = {
      suspended: false,
      archived: false
    };

    executeWithRetry(() =>
      AdminDirectory.Users.update(updateObject, email)
    );

    const status = wasArchived ? 'Member unarchived and unsuspended' : 'Member unsuspended';
    Logger.info(status, { email: email });
    return true;
  } catch (e) {
    Logger.error('Error reactivating member', {
      email: email,
      wasArchived: wasArchived,
      errorMessage: e.message,
      errorCode: e.details?.code
    });
    return false;
  }
}

/**
 * Retrieves all inactive (suspended or archived) users from Google Workspace
 * Filters to non-admin users with CAPID
 *
 * @returns {Array<Object>} Array of user objects with email, capid, archived status
 */
function getInactiveUsers() {
  let inactiveUsers = [];
  let nextPageToken = '';

  do {
    let page = AdminDirectory.Users.list({
      domain: CONFIG.DOMAIN,
      maxResults: 500,
      query: 'isSuspended=true isAdmin=false',
      fields: 'users(primaryEmail,suspended,archived,externalIds),nextPageToken',
      pageToken: nextPageToken
    });

    nextPageToken = page.nextPageToken;

    if (page.users) {
      for (let i = 0; i < page.users.length; i++) {
        const ids = page.users[i].externalIds || [];
        const capidExt = ids.find(id => id.type === 'organization');

        if (capidExt) {
          inactiveUsers.push({
            email: page.users[i].primaryEmail,
            capid: capidExt.value,
            archived: page.users[i].archived || false
          });
        }
      }
    }
  } while (nextPageToken);

  Logger.info('Inactive users retrieved from Workspace', {
    count: inactiveUsers.length
  });
  return inactiveUsers;
}

/**
 * Adds an email alias to a user account with retry logic for conflicts
 *
 * Tries firstname.lastname first, then firstname.lastname1, firstname.lastname2, etc.
 * up to 5 attempts if alias already exists
 *
 * @param {Object} user - User object with name properties
 * @param {Object} user.name - Name object
 * @param {string} user.name.givenName - First name
 * @param {string} user.name.familyName - Last name
 * @param {string} user.primaryEmail - User's primary email
 * @returns {Object|null} Alias object if successful, null if failed
 */

function addAlias(user) {
  const maxRetry = 5;
  let aliasEmail;
  let alias;

  // Try setting default alias first
  try {
    aliasEmail = user.name.givenName.replace(/\s/g, '') + '.' +
                 user.name.familyName.replace(/\s/g, '') + CONFIG.EMAIL_DOMAIN;
    alias = AdminDirectory.Users.Aliases.insert({alias: aliasEmail}, user.primaryEmail);
    if (alias) {
      Logger.info('Alias added', {
        user: user.primaryEmail,
        alias: aliasEmail
      });
      return alias;
    }
  } catch(err) {
    if (err.details?.code !== 409) {
      Logger.error('Failed to add alias', {
        user: user.primaryEmail,
        attemptedAlias: aliasEmail,
        errorMessage: err.message,
        errorCode: err.details?.code
      });
      return null;
    }
    // 409 = Conflict, try with number suffix
  }

  // Make 5 attempts with incrementing numbers
  for (let index = 1; index <= maxRetry; index++) {
    try {
      aliasEmail = user.name.givenName.replace(/\s/g, '') + '.' +
                   user.name.familyName.replace(/\s/g, '') + index + CONFIG.EMAIL_DOMAIN;
      alias = AdminDirectory.Users.Aliases.insert({alias: aliasEmail}, user.primaryEmail);
      if (alias) {
        Logger.info('Alias added with suffix', {
          user: user.primaryEmail,
          alias: aliasEmail,
          attempt: index
        });
        return alias;
      }
    } catch (err) {
      if (err.details?.code !== 409) {
        Logger.error('Failed to add alias with suffix', {
          user: user.primaryEmail,
          attemptedAlias: aliasEmail,
          attempt: index,
          errorMessage: err.message,
          errorCode: err.details?.code
        });
        return null;
      }
    }
  }

  Logger.error('All alias attempts failed', {
    user: user.primaryEmail,
    attempts: maxRetry + 1
  });
  return null;
}

/**
 * Generates an OAuth2 token for a service account to impersonate a user.
 * This is required for APIs like Gmail settings that have strict delegation rules.
 * @param {string} userToImpersonate The email address of the user to impersonate.
 * @param {string} scope The OAuth2 scope(s) required for the API call.
 * @returns {string} The access token.
 */
function getImpersonatedToken_(userToImpersonate, scope) {
  // TODO: Fill in these values from your GCP Service Account JSON key file.
  const SERVICE_ACCOUNT_EMAIL = "capwatch@pcr-capwatch.iam.gserviceaccount.com";
  const PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDBXg5DVixU//de\nxT8l42j+BWsYW2G6PmkJp8iewdiWU+KtARsKzXYXmksvAyHIbEiyweENCr0E2WVI\nHoXDGDFp0Dhsjn1jnQ3jdky11P3D9wxvMxM+lQ59MqjoIOD/58SCGoJOFF7lt5YK\nh/AeoyHWOzv4R8uim9I1WLooxaP543WF67z9yKKc4gSGyuWTBMIP5/6UG97Ow7eC\n6pHNLKqT3RCCfH1RK/tY3k5Z6F6ASeYNlfVla1ZyW+RHpQztFTzrAOkueeIq/N21\nTDVBJUCjTA/sHQBQeGAhViA1qIgvmbhU32k9jgPhYU4Iy88ho444IcBXD0aWoccY\nr5WPYS1jAgMBAAECggEAKZF3HnmhaRpniqt7ckErWmQ+zAsk/J0bBnTXt20zisl4\nsrlIn29gwh0sqWwScJv6mtb78spKrQaw86qAFdsXEEivQIL3KJlkGXBeeD5T2TM8\nLJF9wxfW+AoSbmhXBhxETbW2KmPNrLNlIVlswKFQDlZIg4ynlYrKyyYKSuaF5Btm\nVewmypRHapUJVLWViSKBb7/X4jc/sK6caJyn294EKIP70b4ihzP47RT/5vY2khFP\n5Opw10wjEjlDtOF3fwLAQPJ+8LhpbnWIy707ugKvjZc0IGAY6s8dCT9ZikyrGPtj\nEPpY5dXv8kYmYmFTOiq1j8hLUYn8n8ftcNjQ9j0c+QKBgQDtVnBUxetn2lsRIKcU\nGbJRb/ZWI2s/GdZNMKLPY+hvW4Lh6D+Ha/EJsLmk/Yhqq97Uq9xFyEWmwMLnt8Tk\nCqDTIVThT4nlrJbwKOOYNuP0xJGdT9KCdcGWxw30gIMpfGBuPf6Ayv75SWo6ucfY\noihnlz73J++k1yE0Zc7gwWBJGwKBgQDQkoEb3oRPgSGRb3s03U5ulcNiLCWnqGJQ\n2dVrfZMg8crTfV/jma9YFbuiZJz6ie3RInLHVzVAhJdl3AR1TXCxRwKNpAVqgvEF\naTFpOLE38yN697j+CIEFlRyzafOCz9osWJz4Lhy24G3BZNgNE01UNc7E03uALCwM\nHIJihq55WQKBgQCA0HNzb2CPI1Jd/2zvWesQjEYVBnBE9U784jLbgQw8tFxbJGSm\nqY1Phx2bUQfjbZkpsIWDUmmLUf/3KCSy6JnVPbgF+deMUpoxit/MU65xwOaPjS1i\nJWuG3E7Ur5OAxsLH0tn5KTQuNQx1BzRSfeCUKODB4GkO/LxG5iLcldgelQKBgE8p\na8tSF1G9pyn18ANOg7hBK1kVfG034ajiJLiZfsAgRWUjzsMpz31VMlQeb94/f33C\n32F9Xf7Q1E2axi5naABA/V0ZBd05OZVeKZzQIaMkqzC+2P3B6IZf4/bMndnmXd46\n+8jOZ6OZZs7iIYZE7zKpAYN+6P7qxQULxQj0KUBxAoGBAN7VMmQxAub1yGa5/TDv\n/+8SOi/xxYBJO0YDUQxzpzq7v86dv1X3RebYeAsJD6uAnUpKlICz1vBIw9aNaz00\nvEhAhvE1YR3FkEhyF+QVllZCXsn6yp27G6e1M9HmvXAwFquOcus2AvWPQBFMqqu3\n32zH63Uoe2LI70o2rgunqnns\n-----END PRIVATE KEY-----\n";

  const now = Math.floor(Date.now() / 1000);

    const claimSet = {
      iss: SERVICE_ACCOUNT_EMAIL,
      sub: userToImpersonate,
      aud: 'https://oauth2.googleapis.com/token',
      exp: now + 3600,
      iat: now,
      scope: scope
    };

    const header = { alg: 'RS256', typ: 'JWT' };
    const toSign =
      Utilities.base64EncodeWebSafe(JSON.stringify(header)) + '.' +
      Utilities.base64EncodeWebSafe(JSON.stringify(claimSet));

    const signature = Utilities.computeRsaSha256Signature(toSign, PRIVATE_KEY);
    const jwt = toSign + '.' + Utilities.base64EncodeWebSafe(signature);

    const response = UrlFetchApp.fetch('https://oauth2.googleapis.com/token', {
      method: 'post',
      contentType: 'application/x-www-form-urlencoded',
      payload: {
        grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        assertion: jwt
      }==
    });

    const token = JSON.parse(response.getContentText());
    return token.access_token;
  }

/**
 * Updates Gmail "Send As" display name for a user via impersonation.
 *
 * @param {string} primaryEmail - User's primary email
 * @param {string} displayName - Gmail Send-As display name
 */
function updateGmailSendAsDisplayName(primaryEmail, displayName) {
  const scope =
    'https://www.googleapis.com/auth/gmail.settings.basic ' +
    'https://www.googleapis.com/auth/gmail.settings.sharing';
  let accessToken;

  // Get impersonation token
  try {
    accessToken = getImpersonatedToken_(primaryEmail, scope);
  } catch (e) {
    Logger.error('Impersonation token failed', {
      user: primaryEmail,
      errorMessage: e.message
    });
    return;
  }

  // ---- NEW: Add alias from Aliases sheet if applicable ----
  try {
    const sheet = SpreadsheetApp
      .openById(CONFIG.AUTOMATION_SPREADSHEET_ID)
      .getSheetByName('Aliases');

    if (sheet) {
      const rows = sheet.getDataRange().getValues();
      const header = rows.shift(); // remove header row

      // Find matching row for this primaryEmail
      const row = rows.find(r => String(r[0]).trim().toLowerCase() === primaryEmail.toLowerCase());

      if (row) {
        const aliasEmail = String(row[1] || '').trim();
        const aliasDisplay = String(row[2] || displayName).trim();
        const aliasSignature = row[3] || '';

        if (aliasEmail) {
          const aliasBody = {
            sendAsEmail: aliasEmail,
            displayName: aliasDisplay,
            signature: aliasSignature,
            treatAsAlias: true
          };

          const aliasUrl = "https://gmail.googleapis.com/gmail/v1/users/me/settings/sendAs";

          const aliasResp = UrlFetchApp.fetch(aliasUrl, {
            method: 'post',
            contentType: 'application/json',
            headers: { Authorization: 'Bearer ' + accessToken },
            payload: JSON.stringify(aliasBody),
            muteHttpExceptions: true
          });

          const aCode = aliasResp.getResponseCode();
          if (aCode >= 200 && aCode < 300) {
            Logger.info('Alias added inside updateGmailSendAsDisplayName', {
              primary: primaryEmail,
              alias: aliasEmail
            });
          } else {
            Logger.warn('Alias add failed inside updateGmailSendAsDisplayName', {
              primary: primaryEmail,
              alias: aliasEmail,
              code: aCode,
              response: aliasResp.getContentText()
            });
          }
        }
      }
    }
  } catch (aliasErr) {
    Logger.error('Alias-add attempt inside updateGmailSendAsDisplayName failed', {
      primary: primaryEmail,
      error: aliasErr.message
    });
  }
  // ---- END NEW ----

  // Send-As API call

  try {
    const apiUrl =
      `https://gmail.googleapis.com/gmail/v1/users/me/settings/sendAs/${encodeURIComponent(primaryEmail)}`;

    const sendAs = {
      sendAsEmail: primaryEmail,
      displayName: displayName,
      treatAsAlias: true
    };

    const payload = JSON.stringify(sendAs);

    const response = UrlFetchApp.fetch(apiUrl, {
      method: 'patch',
      contentType: 'application/json',
      headers: { Authorization: 'Bearer ' + accessToken },
      payload: payload,
      muteHttpExceptions: true
    });

    const code = response.getResponseCode();

    if (code >= 200 && code < 300) {
      Logger.info('Updated Send As display name', {
        email: primaryEmail,
        displayName: displayName
      });
    } else {
      Logger.warn('Failed to update Send As display name', {
        email: primaryEmail,
        code: code,
        response: response.getContentText()
      });
    }
  } catch (err) {
    Logger.error('Error updating Gmail Send As', {
      email: primaryEmail,
      errorMessage: err.message
    });
  }
}

/**
 * Pushes signatures for all members to all aliases.
 * This is the production version replacing pushSignatureToAllAliases454201().
 */
/**
 * Pushes signatures for all Workspace users to all aliases.
 * Reads actual Workspace users, matches via CAPID, generates signature,
 * and updates Gmail signatures for each alias.
 */
function pushAllSignatures() {
  Logger.info('Starting pushAllSignatures');

  // 1. Fetch CAPWATCH members
  const members = getMembers();
  const memberIndex = {};

  // Build quick lookup: CAPID → member object
  for (const capid in members) {
    memberIndex[String(capid)] = members[capid];
  }

  // 2. Fetch all Workspace users
  let pageToken = null;
  const workspaceUsers = [];

  do {
    const page = AdminDirectory.Users.list({
      customer: "my_customer",
      maxResults: 200,
      projection: "full",
      pageToken: pageToken
    });

    if (page.users) {
      workspaceUsers.push(...page.users);
    }

    pageToken = page.nextPageToken;

  } while (pageToken);

  // 3. Loop through Workspace users
  workspaceUsers.forEach(user => {
      const capid = user.externalIds?.[0]?.value;
      const member = memberIndex[capid];

    if (!member) {
      Logger.warn('Workspace user has no matching CAPWATCH record', {
        email: user.primaryEmail
      });
      return;
    }


    // Generate signature
    const signature = generateEmailSignature(member);

    // Push signature to all aliases
    updateSignatureForAllAliases(user.primaryEmail, signature);

    Utilities.sleep(200);
  });

  Logger.info('pushAllSignatures completed for all users');
}

// ==========================================================================
// SHARED SIGNATURE HELPERS
// ==========================================================================

function toTitleCase(str) {
  return (str || '')
    .toLowerCase()
    .replace(/\b\w+/g, t => t[0].toUpperCase() + t.substring(1));
}

function formatPhone(phone) {
  if (!phone) return '';
  const digits = phone.replace(/\D/g, '').slice(-10);
  return digits ? digits.replace(/(\d{3})(\d{3})(\d{4})/, '$1.$2.$3') : '';
}

function getPublicRank(rank) {
  const MAP = {
    "SSgt": "Staff Sgt.",
    "TSgt": "Tech. Sgt.",
    "MSgt": "Master Sgt.",
    "SMSgt": "Senior Master Sgt.",
    "CMSgt": "Chief Master Sgt.",
    "FO": "Flight Officer",
    "TFO": "Tech. Flight Officer",
    "SFO": "Senior Flight Officer",
    "2d Lt": "2nd Lt.",
    "1st Lt": "1st Lt.",
    "Capt": "Capt.",
    "Maj": "Maj.",
    "Lt Col": "Lt. Col.",
    "Col": "Col.",
    "Brig Gen": "Brig. Gen.",
    "Maj Gen": "Maj. Gen."
  };
  return MAP[rank] || rank || '';
}

function getWingCode(member) {
  const prefix = (member.charter || '').split('-')[0]; // PCR, HIWG, CAWG, etc.
  return prefix.toLowerCase();
}

function getDutyBlock(member) {
  if (!member.dutyPositions || member.dutyPositions.length === 0) {
    return 'Member';
  }
  return member.dutyPositions
    .filter(dp => !dp.assistant)
    .map(dp => `${toTitleCase(member.orgName)} ${dp.id}`)
    .join('<br>');
}

/**
 * Generates an HTML email signature for a member using the standard CAP template.
 * This produces a Gmail-ready HTML block that can be pasted into the user's signature settings
 * or pushed programmatically.
 *
 * @param {Object} member - A fully constructed member object from getMembers()
 * @returns {string} HTML signature block
 */
function generateEmailSignature(member) {
  const rank = getPublicRank(member.rank);
  const first = member.firstName || '';
  const last = member.lastName || '';
  const duty = getDutyBlock(member);
  const wingCode = getWingCode(member);
  const phoneDigits = member.phone ? member.phone.replace(/\D/g, '').slice(-10) : '';
  const phoneFormatted = formatPhone(member.phone);

  return `
<!DOCTYPE html>
<html>
<body>
<br />

<h1 style="font-size: 12px; line-height: 12px;
           font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif;
           color: #001871; font-weight: bold; margin: 0 0 5px;">
  ${rank} ${first} ${last}
</h1>

<h2 style="font-size: 12px; line-height: 12px;
           font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif;
           color: #000000; font-weight: normal; margin: 0 0 5px;">
  ${duty}
</h2>

<h2 style="font-size: 12px; line-height: 12px;
           font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif;
           color: #000000; margin: 0 0 20px;">
  Civil Air Patrol, U.S. Air Force Auxiliary
</h2>

<p style="font-size: 12px; line-height: 12px;
          font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif;
          color: #000000; font-weight: normal; margin: 0 0 5px;">
  (M) <a href="tel:+1${phoneDigits}" style="color: #000000; text-decoration: none;">${phoneFormatted}</a>
</p>

<p style="font-size: 12px; line-height: 12px;
          font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif;
          color: #001871; font-weight: normal; margin: 0 0 5px;">
  <a href="https://www.GoCivilAirPatrol.com"
     style="color: #000000; text-decoration: underline;">
     GoCivilAirPatrol.com
  </a>
</p>

<p style="font-size: 12px; line-height: 12px;
        font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif;
        color: #001871; font-weight: normal; margin: 0 0 5px;">
 <a href="https://${wingCode}.cap.gov"
   style="color: #000000; text-decoration: underline;">
   ${wingCode}.cap.gov
 </a>
</p>

<a href="https://www.GoCivilAirPatrol.com">
  <img
    style="margin: 15px 0 20px -5px;"
    src="https://cdn-assets-cloud.frontify.com/s3/frontify-cloud-files-us/eyJwYXRoIjoiZnJvbnRpZnlcL2ZpbGVcLzFNOTF6UzJGRXByV0pNUjQ1cnkxLnBuZyJ9:frontify:kAS-l74of5IyRDi0IxD9lG5yNH6i-Zg30PGjGUjU8y0?width=200">
</a>

<p style="font-size: 12px; line-height: 14px;
          font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif;
          color:#000000; font-weight: normal; font-style: italic;
          white-space: normal; margin: 0 0 5px;">
  Volunteers serving America&apos;s communities, saving lives, and shaping futures.
</p>

</body>
</html>
  `;
}

/**
 * Processes members in batches to manage API rate limits
 *
 * @param {Object} members - Members object to process
 * @param {number} batchSize - Number of members per batch (default: 50)
 * @returns {void}
 */
function batchUpdateMembers(members, batchSize = CONFIG.BATCH_SIZE) {
  const memberArray = Object.values(members);
  const totalBatches = Math.ceil(memberArray.length / batchSize);

  Logger.info('Starting batch member update', {
    totalMembers: memberArray.length,
    batchSize: batchSize,
    totalBatches: totalBatches
  });

  for (let i = 0; i < memberArray.length; i += batchSize) {
    const batch = memberArray.slice(i, i + batchSize);
    const batchNumber = Math.floor(i / batchSize) + 1;

    Logger.info('Processing batch', {
      batch: batchNumber,
      totalBatches: totalBatches,
      batchSize: batch.length
    });

    // Process batch
    batch.forEach(member => {
      addOrUpdateUser(member);
    });

    // Add delay between batches to avoid rate limits
    if (i + batchSize < memberArray.length) {
      Utilities.sleep(1000); // 1 second delay
    }
  }

  Logger.info('Batch update completed', {
    totalMembers: memberArray.length,
    batches: totalBatches
  });
}

/**
 * Finds squadrons that are missing organizational unit paths
 * Useful for identifying configuration issues
 *
 * @returns {Array<Object>} Array of squadrons missing orgPath
 */
function findMissingOrgPaths() {
  const squadrons = getSquadrons();
  const missing = [];

  for (const orgid in squadrons) {
    if (!squadrons[orgid].orgPath || squadrons[orgid].orgPath === '') {
      missing.push({
        orgid: orgid,
        name: squadrons[orgid].name,
        charter: squadrons[orgid].charter,
        scope: squadrons[orgid].scope
      });
    }
  }

  Logger.info('Missing orgPath check completed', {
    totalSquadrons: Object.keys(squadrons).length,
    missingOrgPaths: missing.length
  });

  if (missing.length > 0) {
    Logger.warn('Squadrons missing orgPaths', {
      count: missing.length,
      squadrons: missing
    });
  }

  return missing;
}

/**
 * Restores Google Workspace users that were manually deleted but still appear in CAPWATCH.
 *
 * This function:
 * 1. Loads all CAPWATCH members
 * 2. Computes expected primary email (firstname.lastname@wingcap.org)
 * 3. Checks if the Workspace user exists
 * 4. Recreates the user if missing
 */
function restoreDeletedUsers() {
  Logger.info("Starting restore-deleted-users process");

  const members = getMembers(); // All active CAPWATCH members
  const wingDomain = CONFIG.EMAIL_DOMAIN; // e.g., "@hiwgcap.org"

  let restored = 0;
  let skipped = 0;
  let errors = 0;

  for (const capid in members) {
    const m = members[capid];

    // Build expected primary email
    const baseEmail = `${m.firstName}.${m.lastName}`
      .toLowerCase()
      .replace(/\s+/g, '');

    const primaryEmail = baseEmail + wingDomain;

    // 1. Does Workspace already have this user?
    let exists = true;
    try {
      const u = AdminDirectory.Users.get(primaryEmail, { projection: "full" });
      if (u.archived) {
        try {
          AdminDirectory.Users.update({ archived: false, suspended: false }, primaryEmail);
          Logger.info("Unarchived user during restore", { email: primaryEmail });
        } catch (errU) {
          Logger.error("Failed to unarchive during restore", {
            email: primaryEmail,
            error: errU.message
          });
          errors++;
          continue;
        }
      }
    } catch (e) {
      if (String(e.message).includes("Resource Not Found")) {
        exists = false;
      } else {
        Logger.error("Error checking user existence", {
          email: primaryEmail,
          error: e.message
        });
        errors++;
        continue;
      }
    }

    if (exists) {
      skipped++;
      continue;
    }

    // 2. User missing → restore
    try {
      addOrUpdateUser(m);
      restored++;
      Logger.info("Restored missing user", {
        capid: capid,
        email: primaryEmail
      });
    } catch (e) {
      Logger.error("Failed to restore missing user", {
        capid: capid,
        email: primaryEmail,
        error: e.message
      });
      errors++;
    }
  }

  Logger.info("Restore-deleted-users completed", {
    restored: restored,
    skippedExisting: skipped,
    errors: errors,
    totalMembers: Object.keys(members).length
  });
}

// ============================================================================
// TEST FUNCTIONS
// ============================================================================

/**
 * Test function for addOrUpdateUser with a specific member
 * @returns {void}
 */
function testaddOrUpdateUser() {
  Logger.info('Starting test - addOrUpdateUser');
  let members = getMembers();
  if (members[454201]) {
    addOrUpdateUser(members[454201]);
    Logger.info('Test completed');
  } else {
    Logger.error('Test member not found', { capsn: 454201 });
  }
}

/**
 * Test function to retrieve and display a specific member
 * @returns {void}
 */
function testGetMember() {
  Logger.info('Starting test - getMember');
  let members = getMembers();
  let member = members[105576];
  if (member) {
    Logger.info('Test member data', { member: member });
  } else {
    Logger.error('Test member not found', { capsn: 105576 });
  }
}

/**
 * Test function to retrieve and display squadron data
 * @returns {void}
 */
function testGetSquadrons() {
  Logger.info('Starting test - getSquadrons');
  let squadrons = getSquadrons();
  if (squadrons[2503]) {
    Logger.info('Test squadron data', { squadron: squadrons[2503] });
  } else {
    Logger.error('Test squadron not found', { orgid: 2503 });
  }
}

/**
 * Test function to save empty member data
 * @returns {void}
 */
function testSaveCurrentMembersData() {
  Logger.info('Starting test - saveCurrentMemberData');
  saveCurrentMemberData({});
  Logger.info('Test completed');
}

function updateSignatureForAllAliases(primaryEmail, signatureHTML) {
  const scope =
    'https://www.googleapis.com/auth/gmail.settings.sharing ' +
    'https://www.googleapis.com/auth/gmail.settings.basic';
  let accessToken;

  try {
    accessToken = getImpersonatedToken_(primaryEmail, scope);
  } catch (e) {
    Logger.error('Failed to get impersonation token', { user: primaryEmail, error: e.message });
    return;
  }

  try {
    const sendAsListUrl = 'https://gmail.googleapis.com/gmail/v1/users/me/settings/sendAs';
    const listResponse = UrlFetchApp.fetch(sendAsListUrl, {
      method: 'get',
      headers: { 'Authorization': 'Bearer ' + accessToken },
      muteHttpExceptions: true
    });

    const listCode = listResponse.getResponseCode();
    if (listCode < 200 || listCode >= 300) {
      Logger.error('Failed to list send-as identities', {
        user: primaryEmail,
        code: listCode,
        response: listResponse.getContentText()
      });
      return;
    }

    const identities = JSON.parse(listResponse.getContentText()).sendAs || [];

    identities.forEach(identity => {
      const aliasEmail = identity.sendAsEmail;

      // 👉 Only update signatures for @pcrcap.org
      //if (!aliasEmail.toLowerCase().endsWith("@pcrcap.org")) {
      //  Logger.info("Skipping non-org alias", { alias: aliasEmail });
      //  return;
      //}

      const apiUrl =
        `https://gmail.googleapis.com/gmail/v1/users/${encodeURIComponent(primaryEmail)}/settings/sendAs/${encodeURIComponent(aliasEmail)}`;
      const payload = JSON.stringify({ signature: signatureHTML });

      const resp = UrlFetchApp.fetch(apiUrl, {
        method: 'patch',
        contentType: 'application/json',
        headers: { 'Authorization': 'Bearer ' + accessToken },
        payload: payload,
        muteHttpExceptions: true
      });

      const code = resp.getResponseCode();
      if (code >= 200 && code < 300) {
        Logger.info('Updated signature for alias', { primary: primaryEmail, alias: aliasEmail });
      } else {
        Logger.error('Failed to update signature for alias', {
          alias: aliasEmail,
          code: code,
          response: resp.getContentText()
        });
      }
    });

  } catch (e) {
    Logger.error('Error updating alias signatures', { user: primaryEmail, error: e.message });
  }
}

/**
 * Adds Gmail aliases and properly updates both alias and primary Send-As identities.
 * Uses service account impersonation with correct Gmail API rules.
 */
function addAliasesFromSheet() {
  Logger.info('Starting alias creation from sheet using direct impersonation.');

  const sheet = SpreadsheetApp
    .openById(CONFIG.AUTOMATION_SPREADSHEET_ID)
    .getSheetByName('Aliases');

  if (!sheet) {
    Logger.error('Aliases sheet not found');
    return;
  }

  const data = sheet.getDataRange().getValues();
  data.shift(); // remove header row

  let totalProcessed = 0;
  let totalAdded = 0;
  let totalFailed = 0;
  let totalSkipped = 0;

  const scope =
    'https://www.googleapis.com/auth/gmail.settings.basic ' +
    'https://www.googleapis.com/auth/gmail.settings.sharing';

  for (let i = 0; i < data.length; i++) {
    const primaryEmail = (data[i][0] || '').trim();
    const aliasEmail = (data[i][1] || '').trim();
    const displayName = (data[i][2] || '').trim();
    const signature = data[i][3] || '';

    if (!primaryEmail || !aliasEmail) continue;
    totalProcessed++;

    // --- Check if user is an admin ---
    try {
      const user = AdminDirectory.Users.get(primaryEmail, { fields: "isAdmin" });
      if (user.isAdmin) {
        Logger.info("Skipping admin user (aliases must be set manually)", {
          user: primaryEmail
        });
        totalSkipped++;
        continue;
      }
    } catch (e) {
      Logger.error("Admin check failed", {
        user: primaryEmail,
        error: e.message
      });
      totalFailed++;
      continue;
    }

    // --- Get impersonated token ---
    let accessToken = "";
    try {
      accessToken = getImpersonatedToken_(primaryEmail, scope);
    } catch (e) {
      Logger.error("Could not impersonate user", {
        user: primaryEmail,
        error: e.message
      });
      totalFailed++;
      continue;
    }

    //
    // ---------------------------------------------------------
    //  STEP 1 — CREATE / UPDATE THE ALIAS (POST)
    // ---------------------------------------------------------
    //
    try {
      const sendAsAliasBody = {
        sendAsEmail: aliasEmail,
        displayName: displayName,
        signature: signature,
        treatAsAlias: true
      };

      const aliasUrl =
        "https://gmail.googleapis.com/gmail/v1/users/me/settings/sendAs";

      const aliasResp = UrlFetchApp.fetch(aliasUrl, {
        method: "post",
        contentType: "application/json",
        headers: { Authorization: "Bearer " + accessToken },
        payload: JSON.stringify(sendAsAliasBody),
        muteHttpExceptions: true
      });

      const code = aliasResp.getResponseCode();

      if (code >= 200 && code < 300) {
        Logger.info("Alias added successfully", {
          primary: primaryEmail,
          alias: aliasEmail
        });
        totalAdded++;
      } else if (code === 409) {
        Logger.info("Alias already exists, skipping", {
          primary: primaryEmail,
          alias: aliasEmail
        });
        totalSkipped++;
      } else {
        Logger.error("Alias creation failed", {
          primary: primaryEmail,
          alias: aliasEmail,
          code: code,
          response: aliasResp.getContentText()
        });
        totalFailed++;
      }

    } catch (e) {
      Logger.error("Unhandled error during alias POST", {
        user: primaryEmail,
        alias: aliasEmail,
        error: e.message
      });
      totalFailed++;
      continue;
    }

    //
    // ---------------------------------------------------------
    //  STEP 2 — FIX PRIMARY SEND-AS NAME (PATCH)
    // ---------------------------------------------------------
    //
    try {
      /**
       * Gmail rules for PRIMARY send-as:
       *   URL MUST be:     /users/{userId}/settings/sendAs/{sendAsEmail}
       *   DO NOT send:     sendAsEmail, treatAsAlias
       *   Allowed fields:  displayName, signature
       */

      const primaryPatchUrl =
        `https://gmail.googleapis.com/gmail/v1/users/${encodeURIComponent(primaryEmail)}/settings/sendAs/${encodeURIComponent(primaryEmail)}`;

      const primaryPatchBody = {
        displayName: displayName,
        signature: signature
      };

      const primaryResp = UrlFetchApp.fetch(primaryPatchUrl, {
        method: "patch",
        contentType: "application/json",
        headers: { Authorization: "Bearer " + accessToken },
        payload: JSON.stringify(primaryPatchBody),
        muteHttpExceptions: true
      });

      const pcode = primaryResp.getResponseCode();

      if (pcode >= 200 && pcode < 300) {
        Logger.info("Primary Send-As display name updated", {
          primary: primaryEmail,
          displayName: displayName
        });
      } else {
        Logger.warn("Primary Send-As update failed", {
          primary: primaryEmail,
          code: pcode,
          response: primaryResp.getContentText()
        });
        totalFailed++;
      }

    } catch (e) {
      Logger.error("Primary Send-As update threw exception", {
        user: primaryEmail,
        error: e.message
      });
      totalFailed++;
    }
  }

  Logger.info("Alias creation completed", {
    processed: totalProcessed,
    added: totalAdded,
    failed: totalFailed,
    skipped: totalSkipped
  });
}

function testUpdateSendAs() {
  const email = "william.adam@pcrcap.org";
  const display = "Adam, William Lt Col";

  updateGmailSendAsDisplayName(email, display);
}
