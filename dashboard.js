'use strict';

// ================================================================
// CONFIGURATION
// ================================================================
const API_BASE = 'https://gymnasiumleiden.zportal.nl/api/v3';
let API_TOKEN = localStorage.getItem('zermelo_token') || '';
const MAX_CONCURRENT = 6;

// Pauzes zitten tussen even→oneven uur: 2→3, 4→5, 6→7
const PAUZE_TRANSITIONS = new Set(['2-3', '4-5', '6-7']);

// Vakken die altijd worden uitgesloten (geen echte lessen)
const EXCLUDED_SUBJECTS = new Set(['vergadering']);

// Waarneemuur: telt als tussenuur voor docenten (ingeboekt opvanguur)
const WAARNEEMUUR_SUBJECTS = new Set(['wna']);

// Vakken waar blokuren normaal/gewenst zijn
const BLOKUUR_OK_SUBJECTS = new Set(['lo', 'bv', 'ckv', 'pws', 'pro']);

const DAYS_SHORT = ['zo', 'ma', 'di', 'wo', 'do', 'vr', 'za'];
const DAYS_LABELS = ['Ma', 'Di', 'Wo', 'Do', 'Vr'];

// SGL website kleurenpalet
const LOC_COLORS = {
    // Base kleuren (docenten / overall)
    sgl:      { line: 'rgba(65, 80, 158, 0.95)',  bg: 'rgba(65, 80, 158, 0.12)',  hex: '#41509E', text: '#41509E' },
    athena:   { line: 'rgba(252, 195, 0, 0.90)',   bg: 'rgba(252, 195, 0, 0.12)',   hex: '#FCC300', text: '#B38A00' },
    socrates: { line: 'rgba(183, 14, 12, 0.90)',   bg: 'rgba(183, 14, 12, 0.12)',   hex: '#B70E0C', text: '#B70E0C' },
    // Lichte tint (onderbouw)
    sglLight:      { line: 'rgba(123, 135, 191, 0.90)', hex: '#7B87BF', text: '#5E6CB0' },
    athenaLight:   { line: 'rgba(253, 218, 99, 0.90)',  hex: '#FDDA63', text: '#C99B00' },
    socratesLight: { line: 'rgba(224, 82, 80, 0.90)',   hex: '#E05250', text: '#D04040' },
    // Donkere tint (bovenbouw)
    sglDark:      { line: 'rgba(45, 56, 112, 0.95)',  hex: '#2D3870', text: '#2D3870' },
    athenaDark:   { line: 'rgba(179, 138, 0, 0.95)',  hex: '#B38A00', text: '#8A6B00' },
    socratesDark: { line: 'rgba(122, 9, 7, 0.95)',    hex: '#7A0907', text: '#7A0907' },
};

// Helper: locatie → kleur voor charts en kaarten
function locColor(loc, bouw) {
    const suffix = bouw === 'onderbouw' ? 'Light' : bouw === 'bovenbouw' ? 'Dark' : '';
    const key = loc === 'alle' ? 'sgl' : loc.toLowerCase();
    return LOC_COLORS[key + suffix] || LOC_COLORS[key] || LOC_COLORS.sgl;
}

const COLORS = {
    green: 'rgba(39, 174, 96, 0.8)',
    greenBg: 'rgba(39, 174, 96, 0.15)',
    orange: 'rgba(230, 126, 34, 0.8)',
    orangeBg: 'rgba(230, 126, 34, 0.15)',
    red: 'rgba(231, 76, 60, 0.8)',
    redBg: 'rgba(231, 76, 60, 0.15)',
};

// ================================================================
// DOCENT METRICS — 9 gewogen metrics voor docentenscore
// ================================================================
const DOCENT_METRICS = {
    M1: { label: 'Vast lokaal hele week', weight: 5 },
    M2: { label: 'Vast lokaal per dag', weight: 2 },
    M3: { label: 'Lokaalwissel in pauze', weight: -2 },
    M4: { label: 'Lokaalwissel buiten pauze', weight: -5 },
    M5: { label: 'Geen tussenuren', weight: 3 },
    M6: { label: 'Veel tussenuren (>2)', weight: -3 },
    M7: { label: 'Late uren (8e/9e)', weight: -2 },
    M8: { label: 'Pendel mét reistijd', weight: -2 },
    M9: { label: 'Pendel zónder reistijd', weight: -10 },
};

// ================================================================
// LEERLING METRICS — 8 gewogen metrics voor leerlingenscore
// ================================================================
const LEERLING_METRICS = {
    L1: { label: 'Weinig tussenuren (<2/week)', weight: 5 },
    L2: { label: 'Veel tussenuren (≥5/week)', weight: -4 },
    L3: { label: 'Aaneengesloten tussenuren (≥2)', weight: -5 },
    L4: { label: 'Eerste uur vrij (>2x/week)', weight: 1 },
    L5: { label: 'Late uren (8e/9e)', weight: -2 },
    L6: { label: 'Mentorles niet aan rand', weight: -2 },
    L7: { label: 'Blokuur onwenselijk vak', weight: -2 },
    L8: { label: 'Klas >29 leerlingen', weight: -3 },
};

// ================================================================
// STATE
// ================================================================
let state = {
    activeTab: 'docenten',
    activeFilter: 'alle',
    currentWeekCode: null,
    // Reference data
    branches: {},          // branchId -> {id, name}
    locationToBranch: {},  // locationName -> branchName (Athena/Socrates)
    mentorGroups: [],      // [{name, branchName}]
    allTeacherCodes: [],   // ['aal', 'abc', ...]
    // CumLaude bovenbouw data
    cumLaudeStudents: null, // {leerlingnummer -> {klas, leerjaar, lesgroepen: [...]}} or null
    // Weekly schedule data
    groupAppointments: {}, // groupName -> [appointment] (mentor groups + cluster groups)
    teacherAppointments: {},// teacherCode -> [appointment]
    // Computed metrics
    docentMetrics: null,
    leerlingMetrics: null,
    // Accumulated weekly data for averaging
    weeklyTeachers: {},    // weekCode -> teachers array (for computing averages)
    docentMetricsAvg: null, // averaged across all non-vacation weeks
    // Leerling scores per locatie per bouw
    leerlingScoresByLocation: null,
    // UI state
    leerlingSubFilter: 'alle',  // 'alle', 'onderbouw', 'bovenbouw'
    // Charts
    charts: {},
    sortState: {},
};

// ================================================================
// API LAYER — ALLEEN GET! NOOIT POST/PUT/DELETE!
// ================================================================

/**
 * GET request naar Zermelo API.
 * VEILIGHEID: Alleen GET, nooit iets anders.
 */
async function apiGet(endpoint, params = {}) {
    const url = new URL(`${API_BASE}/${endpoint}`);
    url.searchParams.set('access_token', API_TOKEN);
    for (const [k, v] of Object.entries(params)) {
        url.searchParams.set(k, String(v));
    }

    // CRITICAL: method MUST be GET
    const res = await fetch(url.toString(), { method: 'GET' });
    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`API ${res.status} ${endpoint}: ${text.slice(0, 200)}`);
    }
    const json = await res.json();
    return json.response?.data || [];
}

/**
 * Parallel fetch met concurrency limiet.
 */
async function parallelFetch(taskFns, onProgress) {
    const results = new Array(taskFns.length);
    let completed = 0;
    let nextIdx = 0;

    async function worker() {
        while (nextIdx < taskFns.length) {
            const i = nextIdx++;
            try {
                results[i] = await taskFns[i]();
            } catch (e) {
                console.warn(`Task ${i} failed:`, e.message);
                results[i] = null;
            }
            completed++;
            onProgress?.(completed, taskFns.length);
        }
    }

    const n = Math.min(MAX_CONCURRENT, taskFns.length);
    await Promise.all(Array.from({ length: n }, () => worker()));
    return results;
}

// ================================================================
// PROGRESS UI
// ================================================================
function showProgress(show) {
    document.getElementById('progress-container').classList.toggle('hidden', !show);
}

function updateProgress(done, total, label) {
    const pct = total > 0 ? Math.round((done / total) * 100) : 0;
    document.getElementById('progress-fill').style.width = pct + '%';
    document.getElementById('progress-text').textContent =
        label ? `${label} (${done}/${total})` : `${done}/${total} geladen...`;
}

// ================================================================
// CUMLAUDE EXCEL PARSING
// ================================================================

/**
 * Parse CumLaude Excel: extract leerlingnummer -> {klas, leerjaar, lesgroepen}
 * Expects headers in row 5 (0-indexed), data from row 6+.
 * Key columns: Klas, Leerjaar, Leerlingnummer, Lesgroep
 */
function parseCumLaudeExcel(workbook) {
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });

    // Find header row (first row containing 'Leerlingnummer')
    let headerIdx = -1;
    for (let i = 0; i < Math.min(rows.length, 10); i++) {
        if (rows[i] && rows[i].some(c => String(c) === 'Leerlingnummer')) {
            headerIdx = i;
            break;
        }
    }
    if (headerIdx < 0) throw new Error('Header rij met "Leerlingnummer" niet gevonden');

    const headers = rows[headerIdx].map(h => String(h || ''));
    const colKlas = headers.indexOf('Klas');
    const colLeerjaar = headers.indexOf('Leerjaar');
    const colLnr = headers.indexOf('Leerlingnummer');
    const colLesgroep = headers.indexOf('Lesgroep');

    if (colLnr < 0 || colLesgroep < 0) {
        throw new Error('Kolommen "Leerlingnummer" en/of "Lesgroep" niet gevonden');
    }

    const students = {};
    for (let i = headerIdx + 1; i < rows.length; i++) {
        const row = rows[i];
        if (!row) continue;
        const lnr = row[colLnr];
        const lesgroep = row[colLesgroep];
        if (!lnr || !lesgroep) continue;

        const key = String(Math.round(Number(lnr)));
        if (!students[key]) {
            students[key] = {
                klas: String(row[colKlas] || ''),
                leerjaar: Math.round(Number(row[colLeerjaar] || 0)),
                lesgroepen: new Set(),
            };
        }
        students[key].lesgroepen.add(String(lesgroep));
    }

    // Convert Sets to Arrays
    for (const s of Object.values(students)) {
        s.lesgroepen = [...s.lesgroepen];
    }

    return students;
}

function initCumLaudeUpload() {
    const input = document.getElementById('cumlaude-upload');
    const statusEl = document.getElementById('cumlaude-status');
    const label = document.getElementById('cumlaude-label');

    input.addEventListener('change', async (e) => {
        const file = e.target.files[0];
        if (!file) return;

        statusEl.textContent = 'Verwerken...';
        try {
            const data = await file.arrayBuffer();
            const wb = XLSX.read(data, { type: 'array' });
            state.cumLaudeStudents = parseCumLaudeExcel(wb);

            const count = Object.keys(state.cumLaudeStudents).length;
            statusEl.textContent = `✓ ${count} leerlingen`;
            label.classList.add('loaded');
            console.log(`CumLaude: ${count} bovenbouw leerlingen geladen`);

            // Auto-trigger: load all weeks after Excel upload
            await triggerLoadAllWeeks();
        } catch (err) {
            statusEl.textContent = 'Fout: ' + err.message;
            console.error('CumLaude parse error:', err);
        }
    });

    // Auto-load cumlaude.xlsx als die in de dashboard-map staat
    if (!state.cumLaudeStudents) {
        fetch('cumlaude.xlsx')
            .then(r => { if (!r.ok) throw new Error('niet gevonden'); return r.arrayBuffer(); })
            .then(data => {
                const wb = XLSX.read(data, { type: 'array' });
                state.cumLaudeStudents = parseCumLaudeExcel(wb);
                const count = Object.keys(state.cumLaudeStudents).length;
                statusEl.textContent = `✓ ${count} leerlingen`;
                label.classList.add('loaded');
                console.log(`CumLaude auto-loaded: ${count} bovenbouw leerlingen`);
            })
            .catch(() => {}); // Stille fout als bestand niet bestaat
    }
}

// ================================================================
// REFERENCE DATA LOADING
// ================================================================
async function loadReferenceData() {
    showProgress(true);
    updateProgress(0, 6, 'Referentiedata laden');

    const [branchesOfSchools, locations, groups, timeslots, departments] = await Promise.all([
        apiGet('branchesofschools'),
        apiGet('locationofbranches'),
        apiGet('groupindepartments'),
        apiGet('timeslots'),
        apiGet('departmentsofbranches'),
    ]);
    updateProgress(4, 6, 'Referentiedata laden');

    // Load employees separately (needs specific URL construction due to Zermelo quirk)
    const usersUrl = `${API_BASE}/users?isEmployee=true&schoolInSchoolYear=352&access_token=${API_TOKEN}`;
    const usersRes = await fetch(usersUrl, { method: 'GET' });
    const usersJson = await usersRes.json();
    const users = usersJson.response?.data || [];
    updateProgress(5, 6, 'Referentiedata laden');

    // Branch mapping: numeric branchOfSchool ID -> display name
    // branchesofschools gives us {id, branch, name, schoolInSchoolYear}
    const BRANCH_DISPLAY = { ath: 'Athena', soc: 'Socrates' };
    const branchIdToName = {};
    for (const b of branchesOfSchools) {
        const displayName = BRANCH_DISPLAY[b.branch] || BRANCH_DISPLAY[b.name] || b.branch || b.name;
        branchIdToName[b.id] = displayName;
    }
    state.branchIdToName = branchIdToName;
    console.log('Branch ID mapping:', Object.keys(branchIdToName).length, 'entries');

    // Locations: map location name to branch name (Athena/Socrates)
    state.locationToBranch = {};
    for (const loc of locations) {
        const branchName = branchIdToName[loc.branchOfSchool] || 'Onbekend';
        const name = loc.name || String(loc.id);
        state.locationToBranch[name] = branchName;
    }
    console.log('Locations mapped:', Object.keys(state.locationToBranch).length);

    // Department mapping: departmentId -> branch display name
    // departmentsofbranches has {id, branchOfSchool, branchOfSchoolCode}
    const deptIdToBranch = {};
    for (const d of departments) {
        const displayName = BRANCH_DISPLAY[d.branchOfSchoolCode] || branchIdToName[d.branchOfSchool] || 'Onbekend';
        deptIdToBranch[d.id] = displayName;
    }
    console.log('Department mapping:', Object.keys(deptIdToBranch).length, 'entries');

    // Groups: filter mentor groups, deduplicate by name
    // Groups have departmentOfBranch (not branchOfSchool directly)
    const groupMap = new Map();
    for (const g of groups) {
        if (g.isMentorGroup) {
            const name = g.name || g.code || String(g.id);
            if (!groupMap.has(name)) {
                const branchName = deptIdToBranch[g.departmentOfBranch] || 'Onbekend';
                groupMap.set(name, { name, extId: g.extId || g.name, branchName });
            }
        }
    }
    state.mentorGroups = [...groupMap.values()];
    state.mentorGroups.sort((a, b) => a.name.localeCompare(b.name, 'nl', { numeric: true }));
    console.log('Mentor groups:', state.mentorGroups.length);

    // Teachers: all employee codes
    state.allTeacherCodes = users
        .filter(u => u.isEmployee && !u.archived)
        .map(u => u.code);
    console.log('Teacher codes:', state.allTeacherCodes.length);

    updateProgress(6, 6, 'Referentiedata geladen');
    showProgress(false);
}

// ================================================================
// WEEK DATA LOADING
// ================================================================

/**
 * Parse liveschedule response into flat appointment list.
 * Response format: data[0].appointments[] (flat array, no intervals nesting)
 */
function parseLiveschedule(data) {
    const appointments = [];
    if (!data || !Array.isArray(data)) return appointments;

    for (const weekObj of data) {
        // liveschedule returns appointments directly (flat array)
        const appts = weekObj.appointments || [];
        for (const appt of appts) {
            const startTs = appt.start;
            const date = new Date(startTs * 1000);
            const dayOfWeek = date.getDay(); // 0=zo, 1=ma, ..., 5=vr

            const slot = parseInt(appt.startTimeSlotName) || 0;
            // Skip weekends and appointments without slot (exams have empty startTimeSlotName)
            if (dayOfWeek === 0 || dayOfWeek === 6) continue;

            appointments.push({
                slot, // 0 for exams/activities without timeslot
                day: dayOfWeek, // 1=ma, 2=di, ..., 5=vr
                subjects: appt.subjects || [],
                groups: appt.groups || [],
                locations: appt.locations || [],
                teachers: appt.teachers || [],
                cancelled: !!appt.cancelled,
                expectedStudentCount: appt.expectedStudentCount || 0,
                type: appt.appointmentType || 'lesson',
                startTs,
                dateStr: date.toISOString().slice(0, 10),
            });
        }
    }
    return appointments;
}

async function loadAllWeeks() {
    const select = document.getElementById('week-select');
    const originalWeek = select.value;
    const now = new Date();
    const currentWeekCode = `${getISOWeekYear(now)}${String(getISOWeek(now)).padStart(2, '0')}`;

    // Only load weeks up to current week
    const allWeeks = [...select.options]
        .map(o => o.value)
        .filter(w => w <= currentWeekCode);

    // Reset accumulated data
    state.weeklyTeachers = {};

    showProgress(true);
    for (let i = 0; i < allWeeks.length; i++) {
        updateProgress(i, allWeeks.length, `Week ${parseInt(allWeeks[i].slice(4))} (${i + 1}/${allWeeks.length})`);
        try {
            await loadWeekData(allWeeks[i], { silent: true });
        } catch (e) {
            console.warn(`Failed to load week ${allWeeks[i]}:`, e);
        }
    }

    // Compute week-averaged docent metrics (excluding vacation weeks)
    computeAveragedDocentMetrics();

    // Reload original week with rendering (uses averaged data)
    updateProgress(allWeeks.length, allWeeks.length, 'Klaar! Dashboard herladen...');
    await loadWeekData(originalWeek);
}

/**
 * Compute averaged docent metrics across all loaded non-vacation weeks.
 * Vacation weeks auto-detected: < 30% of median lesson count.
 */
function computeAveragedDocentMetrics() {
    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const weekEntries = Object.values(stored).filter(e => e.totalLessons > 0);
    if (weekEntries.length === 0) return;

    // Detect vacation weeks: < 30% of median lessons
    const lessonCounts = weekEntries.map(e => e.totalLessons).sort((a, b) => a - b);
    const median = lessonCounts[Math.floor(lessonCounts.length / 2)];
    const threshold = median * 0.3;
    const vacationWeeks = new Set(weekEntries.filter(e => e.totalLessons < threshold).map(e => e.week));
    console.log(`Vacation weeks detected (< ${Math.round(threshold)} lessons): ${[...vacationWeeks].map(w => 'W' + parseInt(w.slice(4))).join(', ')}`);

    // Mark vacation weeks in localStorage
    for (const [weekCode, entry] of Object.entries(stored)) {
        entry.isVacation = vacationWeeks.has(weekCode);
    }
    localStorage.setItem('rooster_scores', JSON.stringify(stored));

    // Get non-vacation weekly teacher data
    const validWeeks = Object.entries(state.weeklyTeachers)
        .filter(([wk]) => !vacationWeeks.has(wk));

    if (validWeeks.length === 0) return;

    // Aggregate per teacher across weeks
    const teacherWeekData = {}; // code -> [{week data}]
    for (const [, teachers] of validWeeks) {
        for (const t of teachers) {
            if (!teacherWeekData[t.code]) teacherWeekData[t.code] = [];
            teacherWeekData[t.code].push(t);
        }
    }

    // Compute averages
    const avgTeachers = [];
    for (const [code, weeks] of Object.entries(teacherWeekData)) {
        const n = weeks.length;
        const avg = {
            code,
            tussenuren: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, totaal: 0 },
            lokaalRating: Math.round(weeks.reduce((s, w) => s + w.lokaalRating, 0) / n),
            lokalen: new Set(),
            wisselingenBuitenPauze: +(weeks.reduce((s, w) => s + w.wisselingenBuitenPauze, 0) / n).toFixed(1),
            pendelt: weeks.some(w => w.pendelt),
            pendelBewegingen: deduplicatePendelBewegingen(weeks.flatMap(w => w.pendelBewegingen || [])),
            totalPendelBewegingen: +(weeks.reduce((s, w) => s + (w.totalPendelBewegingen || 0), 0) / n).toFixed(1),
            pendelZonderReistijd: +(weeks.reduce((s, w) => s + (w.pendelZonderReistijd || 0), 0) / n).toFixed(1),
            lateUren: +(weeks.reduce((s, w) => s + w.lateUren, 0) / n).toFixed(1),
            urenPerDag: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 },
            maxAaneengesloten: Math.max(...weeks.map(w => w.maxAaneengesloten)),
            fairnessScore: +(weeks.reduce((s, w) => s + w.fairnessScore, 0) / n).toFixed(1),
            locations: new Set(), // All locations this teacher taught at across weeks
        };

        for (const day of [1, 2, 3, 4, 5]) {
            avg.tussenuren[day] = +(weeks.reduce((s, w) => s + w.tussenuren[day], 0) / n).toFixed(1);
            avg.urenPerDag[day] = +(weeks.reduce((s, w) => s + w.urenPerDag[day], 0) / n).toFixed(1);
        }
        avg.tussenuren.totaal = +(weeks.reduce((s, w) => s + w.tussenuren.totaal, 0) / n).toFixed(1);

        // Average lokalen count (not union — union inflates the number)
        const avgLokalenCount = Math.round(weeks.reduce((s, w) => s + w.lokalen.size, 0) / n);
        avg.lokalen = { size: avgLokalenCount }; // set-like for table rendering

        // Merge all locations across weeks (for location filtering)
        for (const w of weeks) {
            if (w.locations) {
                for (const loc of w.locations) avg.locations.add(loc);
            }
        }

        avgTeachers.push(avg);
    }

    state.docentMetricsAvg = {
        teachers: avgTeachers,
        totalTeachers: avgTeachers.length,
        avgTussenuren: avgTeachers.length > 0
            ? (avgTeachers.reduce((s, t) => s + parseFloat(t.tussenuren.totaal), 0) / avgTeachers.length).toFixed(1) : 0,
        pendelaars: avgTeachers.filter(t => t.pendelt),
        avg8e9e: avgTeachers.length > 0
            ? (avgTeachers.reduce((s, t) => s + parseFloat(t.lateUren), 0) / avgTeachers.length).toFixed(1) : 0,
        weekCount: validWeeks.length,
    };

    console.log(`Averaged docent metrics: ${avgTeachers.length} teachers across ${validWeeks.length} non-vacation weeks`);
}

async function loadWeekData(weekCode, { silent = false } = {}) {
    if (!silent) showProgress(true);
    state.currentWeekCode = weekCode;
    state.groupAppointments = {};
    state.teacherAppointments = {};

    // Load liveschedule per teacher (all employees)
    const teacherCodes = state.allTeacherCodes;
    const teacherTasks = teacherCodes.map(code => () =>
        apiGet('liveschedule', { week: weekCode, teacher: code })
    );

    if (!silent) updateProgress(0, teacherTasks.length, 'Docenten laden');
    const teacherResults = await parallelFetch(teacherTasks, (done, total) => {
        if (!silent) updateProgress(done, total, `Docenten laden`);
    });

    // Parse teacher results
    let teachersWithData = 0;
    for (let i = 0; i < teacherCodes.length; i++) {
        const code = teacherCodes[i];
        const data = teacherResults[i];
        if (!data) continue;
        const appts = parseLiveschedule(data);
        if (appts.length > 0) {
            state.teacherAppointments[code] = appts;
            teachersWithData++;
        }
    }
    console.log(`Teachers with schedule data: ${teachersWithData}/${teacherCodes.length}`);

    // Build group appointments from teacher data
    // Store ALL groups (mentor + cluster) so bovenbouw per-student schedules can be built
    const mentorGroupNames = new Set(state.mentorGroups.map(g => g.name));
    for (const [, appts] of Object.entries(state.teacherAppointments)) {
        for (const a of appts) {
            for (const groupName of a.groups) {
                if (!state.groupAppointments[groupName]) {
                    state.groupAppointments[groupName] = [];
                }
                state.groupAppointments[groupName].push(a);
            }
        }
    }

    // Deduplicate group appointments (same lesson can appear from multiple teachers)
    for (const [groupName, appts] of Object.entries(state.groupAppointments)) {
        const seen = new Set();
        state.groupAppointments[groupName] = appts.filter(a => {
            const key = `${a.day}-${a.slot}-${a.subjects.join(',')}-${a.teachers.join(',')}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });
    }

    console.log('Groups with schedule data:', Object.keys(state.groupAppointments).length);
    if (!silent) showProgress(false);

    // Compute all metrics
    computeAllMetrics();
    if (!silent) renderDashboard();
}

// ================================================================
// METRIC CALCULATIONS — DOCENTEN
// ================================================================

function computeDocentMetrics() {
    const metrics = {
        teachers: [],
        totalTeachers: 0,
        avgTussenuren: 0,
        pendelaars: [],
        avg8e9e: 0,
    };

    for (const [code, appts] of Object.entries(state.teacherAppointments)) {
        // Filter: only regular lessons with a timeslot, not cancelled, no excluded subjects
        const activeLessons = appts.filter(a =>
            !a.cancelled && a.slot > 0 && a.type === 'lesson' &&
            !a.subjects.some(s => EXCLUDED_SUBJECTS.has(s.toLowerCase()))
        );
        if (activeLessons.length === 0) continue;

        const teacher = {
            code,
            tussenuren: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, totaal: 0 },
            lokaalRating: 1,
            lokalen: new Set(),
            wisselingenBuitenPauze: 0,
            pendelt: false,
            pendelBewegingen: [],        // [{day, from, to, metReistijd}]
            totalPendelBewegingen: 0,
            pendelZonderReistijd: 0,
            lateUren: 0,           // 8e + 9e uren
            urenPerDag: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 },
            maxAaneengesloten: 0,
            fairnessScore: 0,
        };

        // Group lessons by day
        const byDay = {};
        for (const a of activeLessons) {
            if (!byDay[a.day]) byDay[a.day] = [];
            byDay[a.day].push(a);
        }

        // Per dag: tussenuren, lokalen, late uren
        const lokaalPerDag = {};
        let totalWisselingenBuitenPauze = 0;
        let hasMultipleRooms = false;
        let roomChangesAllInPauze = true;

        for (const day of [1, 2, 3, 4, 5]) {
            const dayLessons = byDay[day] || [];
            if (dayLessons.length === 0) continue;

            // Sort by slot
            dayLessons.sort((a, b) => a.slot - b.slot);
            const slots = dayLessons.map(a => a.slot);
            const uniqueSlots = [...new Set(slots)].sort((a, b) => a - b);

            // wna (waarneemuur) slots tellen als tussenuren, niet als echte lessen
            const wnaSlots = new Set();
            for (const a of dayLessons) {
                if (a.subjects.some(s => WAARNEEMUUR_SUBJECTS.has(s.toLowerCase()))) {
                    wnaSlots.add(a.slot);
                }
            }

            // Echte lesuren (excl. wna)
            const realSlots = uniqueSlots.filter(s => !wnaSlots.has(s));
            teacher.urenPerDag[day] = realSlots.length;

            // Pendel-detectie: bouw blokken per branch, detecteer bewegingen
            // Excludeer WNA en lessen zonder bekende branch
            const realForPendel = dayLessons.filter(a => !wnaSlots.has(a.slot));
            const withBranch = realForPendel
                .sort((a, b) => a.slot - b.slot)
                .map(a => ({
                    slot: a.slot,
                    branch: state.locationToBranch[a.locations[0]] || null,
                }))
                .filter(a => a.branch != null); // Alleen lessen met bekende branch

            // Bouw blokken: opeenvolgende lessen aan dezelfde branch
            const blocks = [];
            if (withBranch.length > 0) {
                let blk = { branch: withBranch[0].branch, slots: [withBranch[0].slot] };
                for (let i = 1; i < withBranch.length; i++) {
                    if (withBranch[i].branch === blk.branch) {
                        blk.slots.push(withBranch[i].slot);
                    } else {
                        blocks.push(blk);
                        blk = { branch: withBranch[i].branch, slots: [withBranch[i].slot] };
                    }
                }
                blocks.push(blk);
            }

            // Detecteer pendelbewegingen (branch-overgangen)
            let pendelGapSlots = new Set();
            for (let bi = 1; bi < blocks.length; bi++) {
                if (blocks[bi].branch === blocks[bi - 1].branch) continue;
                const lastSlot = blocks[bi - 1].slots[blocks[bi - 1].slots.length - 1];
                const firstSlot = blocks[bi].slots[0];
                const gap = firstSlot - lastSlot;
                const metReistijd = gap >= 2; // minimaal 1 vrij uur voor reistijd

                teacher.pendelt = true;
                teacher.pendelBewegingen.push({
                    day,
                    from: blocks[bi - 1].branch,
                    to: blocks[bi].branch,
                    metReistijd,
                });
                teacher.totalPendelBewegingen++;
                if (!metReistijd) teacher.pendelZonderReistijd++;

                // Verzamel vrije slots als reistijd-gap (voor tussenuur-exclusie)
                if (metReistijd) {
                    for (let s = lastSlot + 1; s < firstSlot; s++) {
                        pendelGapSlots.add(s);
                    }
                }
            }

            // Tussenuren: gaten + wna-uren, maar NIET pendel-reistijd gaten
            if (uniqueSlots.length >= 2) {
                const first = uniqueSlots[0];
                const last = uniqueSlots[uniqueSlots.length - 1];
                for (let s = first + 1; s < last; s++) {
                    if (pendelGapSlots.has(s)) continue; // pendel-reistijd, niet meetellen
                    if (!uniqueSlots.includes(s) || wnaSlots.has(s)) {
                        teacher.tussenuren[day]++;
                    }
                }
            }
            teacher.tussenuren.totaal += teacher.tussenuren[day];

            // Late uren (8e en 9e)
            for (const s of uniqueSlots) {
                if (s >= 8) teacher.lateUren++;
            }

            // Max aaneengesloten lesuren
            let streak = 1;
            for (let i = 1; i < uniqueSlots.length; i++) {
                if (uniqueSlots[i] === uniqueSlots[i - 1] + 1) {
                    streak++;
                } else {
                    teacher.maxAaneengesloten = Math.max(teacher.maxAaneengesloten, streak);
                    streak = 1;
                }
            }
            teacher.maxAaneengesloten = Math.max(teacher.maxAaneengesloten, streak);

            // Lokalen per dag (excl. wna-uren — andere kamer voor opvang telt niet mee)
            const dayRooms = new Set();
            for (const a of dayLessons) {
                if (wnaSlots.has(a.slot)) continue;
                for (const loc of a.locations) {
                    dayRooms.add(loc);
                    teacher.lokalen.add(loc);
                }
            }
            lokaalPerDag[day] = dayRooms;

            // Check room changes within day (excl. wna)
            const realLessons = dayLessons.filter(a => !wnaSlots.has(a.slot));
            if (dayRooms.size > 1) {
                hasMultipleRooms = true;
                for (let i = 1; i < realLessons.length; i++) {
                    const prevLoc = realLessons[i - 1].locations[0] || '';
                    const currLoc = realLessons[i].locations[0] || '';
                    if (prevLoc && currLoc && prevLoc !== currLoc) {
                        const prevSlot = realLessons[i - 1].slot;
                        const currSlot = realLessons[i].slot;
                        if (currSlot - prevSlot === 1) {
                            const transition = `${prevSlot}-${currSlot}`;
                            if (!PAUZE_TRANSITIONS.has(transition)) {
                                totalWisselingenBuitenPauze++;
                                roomChangesAllInPauze = false;
                            }
                        }
                    }
                }
            }
        }

        // Lokaalconstantie rating
        teacher.wisselingenBuitenPauze = totalWisselingenBuitenPauze;
        if (teacher.lokalen.size === 1) {
            teacher.lokaalRating = 1; // Vast lokaal
        } else {
            // Check if same room per day
            const allSamePerDay = Object.values(lokaalPerDag).every(rooms => rooms.size <= 1);
            if (allSamePerDay) {
                teacher.lokaalRating = 2; // Vast per dag
            } else if (roomChangesAllInPauze) {
                teacher.lokaalRating = 3; // Wissel in pauze
            } else {
                teacher.lokaalRating = 4; // Wissel buiten pauze
            }
        }

        // Fairness score (hoger = slechter)
        const days = Object.values(teacher.urenPerDag).filter(v => v > 0);
        const mean = days.length ? days.reduce((s, v) => s + v, 0) / days.length : 0;
        const variance = days.length ? days.reduce((s, v) => s + (v - mean) ** 2, 0) / days.length : 0;
        const spreidingStdev = Math.sqrt(variance);

        teacher.fairnessScore =
            teacher.tussenuren.totaal * 3 +
            teacher.wisselingenBuitenPauze * 5 +
            teacher.lateUren * 2 +
            spreidingStdev * 4;

        metrics.teachers.push(teacher);
    }

    metrics.totalTeachers = metrics.teachers.length;
    metrics.avgTussenuren = metrics.totalTeachers > 0
        ? (metrics.teachers.reduce((s, t) => s + t.tussenuren.totaal, 0) / metrics.totalTeachers).toFixed(1)
        : 0;
    metrics.pendelaars = metrics.teachers.filter(t => t.pendelt);
    metrics.totalPendelBewegingen = metrics.teachers.reduce((s, t) => s + t.totalPendelBewegingen, 0);
    metrics.pendelZonderReistijd = metrics.teachers.reduce((s, t) => s + t.pendelZonderReistijd, 0);
    metrics.avg8e9e = metrics.totalTeachers > 0
        ? (metrics.teachers.reduce((s, t) => s + t.lateUren, 0) / metrics.totalTeachers).toFixed(1)
        : 0;

    return metrics;
}

// checkReistijd en findPendelGapSlots verwijderd — vervangen door
// blok-gebaseerde pendel-bewegingsdetectie in computeDocentMetrics

// ================================================================
// METRIC CALCULATIONS — LEERLINGEN
// ================================================================

function getYearFromGroupName(name) {
    const match = String(name).match(/^(\d)/);
    return match ? parseInt(match[1]) : 0;
}

function computeLeerlingMetrics() {
    const metrics = {
        klassen: [],            // onderbouw (1-3) klas metrics
        bovenbouw: null,        // {klassen, vakgroepen} when CumLaude data present
        totalKlassen: 0,
        avgTussenuren: 0,
        mentorRand: 0,
        avg8e9e: 0,
    };

    let totalMentorLessen = 0;
    let mentorAanRand = 0;

    // === ONDERBOUW (jaar 1-3): analyse per mentorgroep ===
    for (const group of state.mentorGroups) {
        const year = getYearFromGroupName(group.name);
        if (year >= 4) continue; // bovenbouw apart

        const appts = state.groupAppointments[group.name];
        if (!appts || appts.length === 0) continue;

        // Filter: only regular lessons with timeslot, not cancelled, no excluded subjects
        const activeLessons = appts.filter(a =>
            !a.cancelled && a.slot > 0 && a.type === 'lesson' &&
            !a.subjects.some(s => EXCLUDED_SUBJECTS.has(s.toLowerCase()))
        );
        if (activeLessons.length === 0) continue;

        const klas = {
            name: group.name,
            branchName: group.branchName,
            tussenuren: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, totaal: 0 },
            patternScore: 0,
            lateUren8: 0,
            lateUren9: 0,
            mentorLessen: 0,
            mentorAanRand: 0,
            avgSize: 0,
            laatsteUur: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 },
            vakSpreiding: [],
            blokuren: [],       // [{vak, day, slots, isOk}]
            maxConsecGaps: 0,   // max opeenvolgende tussenuren op één dag
            eersteUurVrijDagen: 0, // dagen waar eerste slot > 1
        };

        // Group by day
        const byDay = {};
        for (const a of activeLessons) {
            if (!byDay[a.day]) byDay[a.day] = [];
            byDay[a.day].push(a);
        }

        // Expected student count (average of non-zero values)
        const sizes = activeLessons
            .map(a => a.expectedStudentCount)
            .filter(s => s > 0);
        klas.avgSize = sizes.length > 0
            ? Math.round(sizes.reduce((s, v) => s + v, 0) / sizes.length)
            : 0;

        // Per day analysis
        for (const day of [1, 2, 3, 4, 5]) {
            const dayLessons = byDay[day] || [];
            if (dayLessons.length === 0) continue;

            dayLessons.sort((a, b) => a.slot - b.slot);
            const slots = [...new Set(dayLessons.map(a => a.slot))].sort((a, b) => a - b);

            // Tussenuren
            if (slots.length >= 2) {
                const first = slots[0];
                const last = slots[slots.length - 1];
                for (let s = first + 1; s < last; s++) {
                    if (!slots.includes(s)) {
                        klas.tussenuren[day]++;
                    }
                }
            }
            klas.tussenuren.totaal += klas.tussenuren[day];
            klas.patternScore += klas.tussenuren[day] ** 2; // swiss cheese penalty

            // Max consecutive gaps (for L3 metric)
            if (slots.length >= 2) {
                let consec = 0;
                for (let s = slots[0] + 1; s < slots[slots.length - 1]; s++) {
                    if (!slots.includes(s)) {
                        consec++;
                        klas.maxConsecGaps = Math.max(klas.maxConsecGaps, consec);
                    } else {
                        consec = 0;
                    }
                }
            }

            // Eerste uur vrij (for L4 metric)
            if (slots.length > 0 && slots[0] > 1) {
                klas.eersteUurVrijDagen++;
            }

            // Laatste uur
            klas.laatsteUur[day] = slots[slots.length - 1];

            // Late uren
            for (const s of slots) {
                if (s === 8) klas.lateUren8++;
                if (s >= 9) klas.lateUren9++;
            }

            // Mentoruren
            for (const a of dayLessons) {
                const isMentor = a.subjects.some(s =>
                    s.toLowerCase() === 'men' || s.toLowerCase() === 'mentor'
                );
                if (isMentor) {
                    klas.mentorLessen++;
                    totalMentorLessen++;
                    // Aan de rand = uur 1 OF laatste uur van die dag
                    const isRand = a.slot === slots[0] || a.slot === slots[slots.length - 1];
                    if (isRand) {
                        klas.mentorAanRand++;
                        mentorAanRand++;
                    }
                }
            }
        }

        // Vakspreiding
        const vakPerDag = {}; // subjectCode -> Set of days
        for (const a of activeLessons) {
            for (const subj of a.subjects) {
                if (!vakPerDag[subj]) vakPerDag[subj] = new Set();
                vakPerDag[subj].add(a.day);
            }
        }
        const vakCount = {}; // subjectCode -> total lessons
        for (const a of activeLessons) {
            for (const subj of a.subjects) {
                vakCount[subj] = (vakCount[subj] || 0) + 1;
            }
        }
        for (const [vak, dagen] of Object.entries(vakPerDag)) {
            const lessen = vakCount[vak];
            if (lessen >= 2) { // Only check subjects with 2+ lessons/week
                const idealDagen = Math.min(lessen, 5);
                const spreidingScore = dagen.size / idealDagen;
                klas.vakSpreiding.push({
                    vak,
                    lessen,
                    dagen: dagen.size,
                    score: spreidingScore,
                });
            }
        }
        // Sort: worst spreading first
        klas.vakSpreiding.sort((a, b) => a.score - b.score);

        // Blokuren: detect consecutive same-subject slots per day
        for (const day of [1, 2, 3, 4, 5]) {
            const dayLessons = byDay[day] || [];
            if (dayLessons.length < 2) continue;

            dayLessons.sort((a, b) => a.slot - b.slot);
            for (let i = 1; i < dayLessons.length; i++) {
                const prev = dayLessons[i - 1];
                const curr = dayLessons[i];
                // Consecutive slots with same subject
                if (curr.slot === prev.slot + 1) {
                    const sharedSubjects = prev.subjects.filter(s =>
                        curr.subjects.includes(s) && !EXCLUDED_SUBJECTS.has(s.toLowerCase())
                    );
                    for (const vak of sharedSubjects) {
                        const isOk = BLOKUUR_OK_SUBJECTS.has(vak.toLowerCase());
                        klas.blokuren.push({
                            vak,
                            day,
                            slots: `${prev.slot}-${curr.slot}`,
                            isOk,
                        });
                    }
                }
            }
        }

        metrics.klassen.push(klas);
    }

    metrics.totalKlassen = metrics.klassen.length;
    const schoolDays = 5;
    metrics.avgTussenuren = metrics.totalKlassen > 0
        ? (metrics.klassen.reduce((s, k) => s + k.tussenuren.totaal, 0) / (metrics.totalKlassen * schoolDays)).toFixed(2)
        : 0;
    metrics.mentorRand = totalMentorLessen > 0
        ? Math.round((mentorAanRand / totalMentorLessen) * 100)
        : 0;
    metrics.avg8e9e = metrics.totalKlassen > 0
        ? ((metrics.klassen.reduce((s, k) => s + k.lateUren8 + k.lateUren9, 0)) / metrics.totalKlassen).toFixed(1)
        : 0;

    // === BOVENBOUW (jaar 4-6): per-leerling analyse via CumLaude ===
    if (state.cumLaudeStudents) {
        metrics.bovenbouw = computeBovenbouwMetrics();
    }

    return metrics;
}

function computeBovenbouwMetrics() {
    const result = {
        klassen: [],        // per-klas aggregated student metrics
        vakgroepBlokuren: [], // blokuren from vakgroepen
        totalStudents: 0,
        avgTussenuren: 0,
        avgLateUren: 0,
    };

    // Case-insensitive lookup for group appointments
    const groupApptLower = {};
    for (const [name, appts] of Object.entries(state.groupAppointments)) {
        groupApptLower[name.toLowerCase()] = appts;
    }

    // Group CumLaude students by klas (mentor group)
    const studentsByKlas = {};
    for (const [lnr, student] of Object.entries(state.cumLaudeStudents)) {
        if (!studentsByKlas[student.klas]) studentsByKlas[student.klas] = [];
        studentsByKlas[student.klas].push({ lnr, ...student });
    }

    let allStudentTussenuren = 0;
    let allStudentLate = 0;
    let totalStudentCount = 0;
    let totalMaxTussen3Plus = 0;  // for B1 indicator
    let totalWithUur9 = 0;        // for B3 indicator

    for (const [klasName, students] of Object.entries(studentsByKlas)) {
        const perStudent = [];

        for (const student of students) {
            // Collect all appointments for this student
            const apptMap = new Map();

            // 1. Mentor group appointments
            const mentorAppts = groupApptLower[klasName.toLowerCase()] || [];
            for (const a of mentorAppts) {
                const key = `${a.day}-${a.slot}-${a.subjects.join(',')}-${a.teachers.join(',')}`;
                if (!apptMap.has(key)) apptMap.set(key, a);
            }

            // 2. Cluster group appointments (skip stamklas format like "4g-entl")
            for (const lesgroep of student.lesgroepen) {
                if (lesgroep.includes('-')) continue;
                const appts = groupApptLower[lesgroep.toLowerCase()];
                if (!appts) continue;
                for (const a of appts) {
                    const key = `${a.day}-${a.slot}-${a.subjects.join(',')}-${a.teachers.join(',')}`;
                    if (!apptMap.has(key)) apptMap.set(key, a);
                }
            }

            const activeLessons = [...apptMap.values()].filter(a =>
                !a.cancelled && a.slot > 0 && a.type === 'lesson' &&
                !a.subjects.some(s => EXCLUDED_SUBJECTS.has(s.toLowerCase()))
            );

            // Per day analysis
            const byDay = {};
            for (const a of activeLessons) {
                if (!byDay[a.day]) byDay[a.day] = [];
                byDay[a.day].push(a);
            }

            let tussenuren = 0;
            let lateUren8 = 0;
            let lateUren9 = 0;
            let maxConsecGaps = 0;
            let eersteUurVrijDagen = 0;
            let mentorNietRand = false;
            let heeftOnwenselijkBlok = false;
            const tussenPerDag = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
            const laatsteUur = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };

            for (const day of [1, 2, 3, 4, 5]) {
                const dayLessons = byDay[day] || [];
                if (dayLessons.length === 0) continue;
                const slots = [...new Set(dayLessons.map(a => a.slot))].sort((a, b) => a - b);

                laatsteUur[day] = slots[slots.length - 1];

                if (slots.length >= 2) {
                    for (let s = slots[0] + 1; s < slots[slots.length - 1]; s++) {
                        if (!slots.includes(s)) {
                            tussenPerDag[day]++;
                            tussenuren++;
                        }
                    }
                }

                for (const s of slots) {
                    if (s === 8) lateUren8++;
                    if (s >= 9) lateUren9++;
                }

                // Max consecutive gaps (for L3)
                if (slots.length >= 2) {
                    let consec = 0;
                    for (let s = slots[0] + 1; s < slots[slots.length - 1]; s++) {
                        if (!slots.includes(s)) {
                            consec++;
                            maxConsecGaps = Math.max(maxConsecGaps, consec);
                        } else {
                            consec = 0;
                        }
                    }
                }

                // Eerste uur vrij (for L4)
                if (slots[0] > 1) eersteUurVrijDagen++;

                // Mentor niet aan rand (for L6)
                for (const a of dayLessons) {
                    if (a.subjects.some(s => s.toLowerCase() === 'men' || s.toLowerCase() === 'mentor')) {
                        if (a.slot !== slots[0] && a.slot !== slots[slots.length - 1]) {
                            mentorNietRand = true;
                        }
                    }
                }

                // Onwenselijk blokuur (for L7)
                const sortedDayLessons = [...dayLessons].sort((a, b) => a.slot - b.slot);
                for (let i = 1; i < sortedDayLessons.length; i++) {
                    const prev = sortedDayLessons[i - 1];
                    const curr = sortedDayLessons[i];
                    if (curr.slot === prev.slot + 1) {
                        const shared = prev.subjects.filter(s =>
                            curr.subjects.includes(s) && !EXCLUDED_SUBJECTS.has(s.toLowerCase())
                        );
                        if (shared.some(v => !BLOKUUR_OK_SUBJECTS.has(v.toLowerCase()))) {
                            heeftOnwenselijkBlok = true;
                        }
                    }
                }
            }

            perStudent.push({
                totalLessons: activeLessons.length,
                tussenuren,
                tussenPerDag,
                lateUren8,
                lateUren9,
                laatsteUur,
                maxConsecGaps,
                eersteUurVrijDagen,
                mentorNietRand,
                heeftOnwenselijkBlok,
            });
        }

        if (perStudent.length === 0) continue;

        const mentorGroup = state.mentorGroups.find(g => g.name === klasName);
        const avgTussen = perStudent.reduce((s, m) => s + m.tussenuren, 0) / perStudent.length;
        const avgLate = perStudent.reduce((s, m) => s + m.lateUren8 + m.lateUren9, 0) / perStudent.length;

        allStudentTussenuren += perStudent.reduce((s, m) => s + m.tussenuren, 0);
        allStudentLate += perStudent.reduce((s, m) => s + m.lateUren8 + m.lateUren9, 0);
        totalStudentCount += perStudent.length;
        // Indicator stats (B1: max tussenuren on any day ≥3, B3: has 9th hour)
        for (const ps of perStudent) {
            if (Math.max(...Object.values(ps.tussenPerDag)) >= 3) totalMaxTussen3Plus++;
            if (ps.lateUren9 > 0) totalWithUur9++;
        }

        // Compute avg tussenuren per dag
        const avgTussenPerDag = {};
        for (const d of [1, 2, 3, 4, 5]) {
            avgTussenPerDag[d] = +(perStudent.reduce((s, m) => s + m.tussenPerDag[d], 0) / perStudent.length).toFixed(1);
        }

        // Compute avg laatste uur per dag
        const avgLaatsteUur = {};
        for (const d of [1, 2, 3, 4, 5]) {
            const withLessons = perStudent.filter(m => m.laatsteUur[d] > 0);
            avgLaatsteUur[d] = withLessons.length > 0
                ? Math.round(withLessons.reduce((s, m) => s + m.laatsteUur[d], 0) / withLessons.length)
                : 0;
        }

        result.klassen.push({
            name: klasName,
            branchName: mentorGroup?.branchName || 'Onbekend',
            leerjaar: getYearFromGroupName(klasName),
            studentCount: perStudent.length,
            avgLessons: +(perStudent.reduce((s, m) => s + m.totalLessons, 0) / perStudent.length).toFixed(1),
            avgTussenuren: +avgTussen.toFixed(1),
            maxTussenuren: Math.max(...perStudent.map(m => m.tussenuren)),
            avgTussenPerDag: avgTussenPerDag,
            avgLateUren: +avgLate.toFixed(1),
            avgLaatsteUur: avgLaatsteUur,
            studentsWithTussenuren: perStudent.filter(m => m.tussenuren > 0).length,
            studentsWithMaxTussen3Plus: perStudent.filter(ps => Math.max(...Object.values(ps.tussenPerDag)) >= 3).length,
            studentsWithUur9: perStudent.filter(ps => ps.lateUren9 > 0).length,
            // Leerling metrics aggregaties
            studentsWithFewTussenuren: perStudent.filter(ps => ps.tussenuren < 2).length,
            studentsWithManyTussenuren: perStudent.filter(ps => ps.tussenuren >= 5).length,
            studentsWithConsecGaps2Plus: perStudent.filter(ps => ps.maxConsecGaps >= 2).length,
            studentsWithEersteVrij2Plus: perStudent.filter(ps => ps.eersteUurVrijDagen > 2).length,
            studentsWithLateUren: perStudent.filter(ps => ps.lateUren8 + ps.lateUren9 > 0).length,
            studentsWithMentorNietRand: perStudent.filter(ps => ps.mentorNietRand).length,
            studentsWithOnwenselijkBlok: perStudent.filter(ps => ps.heeftOnwenselijkBlok).length,
        });
    }

    result.klassen.sort((a, b) => a.name.localeCompare(b.name, 'nl', { numeric: true }));
    result.totalStudents = totalStudentCount;
    result.avgTussenuren = totalStudentCount > 0 ? +(allStudentTussenuren / totalStudentCount).toFixed(2) : 0;
    result.avgLateUren = totalStudentCount > 0 ? +(allStudentLate / totalStudentCount).toFixed(1) : 0;
    result.studentsWithMaxTussen3Plus = totalMaxTussen3Plus;
    result.studentsWithUur9 = totalWithUur9;

    // Bovenbouw vakgroep blokuren analysis
    const bovenbouwGroupPattern = /^[456]s?\./i;
    for (const [groupName, appts] of Object.entries(state.groupAppointments)) {
        if (!bovenbouwGroupPattern.test(groupName)) continue;

        const activeLessons = appts.filter(a =>
            !a.cancelled && a.slot > 0 && a.type === 'lesson' &&
            !a.subjects.some(s => EXCLUDED_SUBJECTS.has(s.toLowerCase()))
        );
        if (activeLessons.length === 0) continue;

        const byDay = {};
        for (const a of activeLessons) {
            if (!byDay[a.day]) byDay[a.day] = [];
            byDay[a.day].push(a);
        }

        for (const day of [1, 2, 3, 4, 5]) {
            const dayLessons = byDay[day] || [];
            if (dayLessons.length < 2) continue;
            dayLessons.sort((a, b) => a.slot - b.slot);
            for (let i = 1; i < dayLessons.length; i++) {
                const prev = dayLessons[i - 1];
                const curr = dayLessons[i];
                if (curr.slot === prev.slot + 1) {
                    const sharedSubjects = prev.subjects.filter(s =>
                        curr.subjects.includes(s) && !EXCLUDED_SUBJECTS.has(s.toLowerCase())
                    );
                    for (const vak of sharedSubjects) {
                        result.vakgroepBlokuren.push({
                            groep: groupName,
                            vak,
                            day,
                            slots: `${prev.slot}-${curr.slot}`,
                            isOk: BLOKUUR_OK_SUBJECTS.has(vak.toLowerCase()),
                        });
                    }
                }
            }
        }
    }

    console.log(`Bovenbouw: ${result.klassen.length} klassen, ${result.totalStudents} leerlingen, ${result.vakgroepBlokuren.length} blokuren`);
    return result;
}

// ================================================================
// METRIC CALCULATIONS — ACHTERAF
// ================================================================

function computeAchterafMetrics() {
    const metrics = {
        totalLessen: 0,
        uitgevallen: 0,
        uitvalPct: 0,
        vervangen: 0, // we can't easily determine this from current data
        perVak: {},   // subject -> {total, cancelled}
        perDagUur: {},// day-slot -> {total, cancelled}
        perDocent: {},// teacher -> {total, cancelled}
    };

    // Gather all lesson appointments from all sources
    const allAppts = [];

    // From teacher schedules (most complete)
    // Filter: only lessons with timeslots (exclude meetings, exams, activities, vergadering)
    for (const [code, appts] of Object.entries(state.teacherAppointments)) {
        for (const a of appts) {
            if (a.slot > 0 && a.type === 'lesson' &&
                !a.subjects.some(s => EXCLUDED_SUBJECTS.has(s.toLowerCase()))) {
                allAppts.push({ ...a, teacherCode: code });
            }
        }
    }

    // Deduplicate by using a key (day + slot + teacher + subject)
    const seen = new Set();
    const uniqueAppts = [];
    for (const a of allAppts) {
        const key = `${a.day}-${a.slot}-${a.teachers.join(',')}-${a.subjects.join(',')}`;
        if (!seen.has(key)) {
            seen.add(key);
            uniqueAppts.push(a);
        }
    }

    metrics.totalLessen = uniqueAppts.length;
    metrics.uitgevallen = uniqueAppts.filter(a => a.cancelled).length;
    metrics.uitvalPct = metrics.totalLessen > 0
        ? ((metrics.uitgevallen / metrics.totalLessen) * 100).toFixed(1)
        : 0;

    // Per vak
    for (const a of uniqueAppts) {
        for (const subj of a.subjects) {
            if (!metrics.perVak[subj]) metrics.perVak[subj] = { total: 0, cancelled: 0 };
            metrics.perVak[subj].total++;
            if (a.cancelled) metrics.perVak[subj].cancelled++;
        }
    }

    // Per dag/uur
    for (const a of uniqueAppts) {
        const key = `${a.day}-${a.slot}`;
        if (!metrics.perDagUur[key]) metrics.perDagUur[key] = { total: 0, cancelled: 0 };
        metrics.perDagUur[key].total++;
        if (a.cancelled) metrics.perDagUur[key].cancelled++;
    }

    // Per docent
    for (const a of uniqueAppts) {
        for (const t of a.teachers) {
            if (!metrics.perDocent[t]) metrics.perDocent[t] = { total: 0, cancelled: 0 };
            metrics.perDocent[t].total++;
            if (a.cancelled) metrics.perDocent[t].cancelled++;
        }
    }

    return metrics;
}

// ================================================================
// LEERLING SCORE BEREKENING
// ================================================================

function computeLeerlingScore(locationFilter, bouw) {
    const lm = state.leerlingMetrics;
    if (!lm) return { metrics: {}, score: 0 };

    if (bouw === 'onderbouw') {
        let klassen = lm.klassen;
        if (locationFilter !== 'alle') klassen = klassen.filter(k => k.branchName === locationFilter);
        const n = klassen.reduce((s, k) => s + k.avgSize, 0);
        if (n === 0) return { metrics: {}, score: 0 };

        const m = {};
        m.L1 = +(klassen.filter(k => k.tussenuren.totaal < 2).reduce((s, k) => s + k.avgSize, 0) / n * 100).toFixed(1);
        m.L2 = +(klassen.filter(k => k.tussenuren.totaal >= 5).reduce((s, k) => s + k.avgSize, 0) / n * 100).toFixed(1);
        m.L3 = +(klassen.filter(k => k.maxConsecGaps >= 2).reduce((s, k) => s + k.avgSize, 0) / n * 100).toFixed(1);
        m.L4 = +(klassen.filter(k => k.eersteUurVrijDagen > 2).reduce((s, k) => s + k.avgSize, 0) / n * 100).toFixed(1);
        m.L5 = +(klassen.filter(k => k.lateUren8 + k.lateUren9 > 0).reduce((s, k) => s + k.avgSize, 0) / n * 100).toFixed(1);
        m.L6 = +(klassen.filter(k => k.mentorLessen > k.mentorAanRand).reduce((s, k) => s + k.avgSize, 0) / n * 100).toFixed(1);
        m.L7 = +(klassen.filter(k => k.blokuren.some(b => !b.isOk)).reduce((s, k) => s + k.avgSize, 0) / n * 100).toFixed(1);
        m.L8 = +(klassen.filter(k => k.avgSize > 29).reduce((s, k) => s + k.avgSize, 0) / n * 100).toFixed(1);

        let score = 0;
        for (const [key, val] of Object.entries(m)) score += val * LEERLING_METRICS[key].weight;
        return { metrics: m, score: +score.toFixed(1) };
    }

    if (bouw === 'bovenbouw') {
        if (!lm.bovenbouw) return { metrics: {}, score: 0 };
        let klassen = lm.bovenbouw.klassen;
        if (locationFilter !== 'alle') klassen = klassen.filter(k => k.branchName === locationFilter);
        const n = klassen.reduce((s, k) => s + k.studentCount, 0);
        if (n === 0) return { metrics: {}, score: 0 };

        const m = {};
        m.L1 = +(klassen.reduce((s, k) => s + (k.studentsWithFewTussenuren || 0), 0) / n * 100).toFixed(1);
        m.L2 = +(klassen.reduce((s, k) => s + (k.studentsWithManyTussenuren || 0), 0) / n * 100).toFixed(1);
        m.L3 = +(klassen.reduce((s, k) => s + (k.studentsWithConsecGaps2Plus || 0), 0) / n * 100).toFixed(1);
        m.L4 = +(klassen.reduce((s, k) => s + (k.studentsWithEersteVrij2Plus || 0), 0) / n * 100).toFixed(1);
        m.L5 = +(klassen.reduce((s, k) => s + (k.studentsWithLateUren || 0), 0) / n * 100).toFixed(1);
        m.L6 = +(klassen.reduce((s, k) => s + (k.studentsWithMentorNietRand || 0), 0) / n * 100).toFixed(1);
        m.L7 = +(klassen.reduce((s, k) => s + (k.studentsWithOnwenselijkBlok || 0), 0) / n * 100).toFixed(1);
        m.L8 = +(klassen.filter(k => k.studentCount > 29).reduce((s, k) => s + k.studentCount, 0) / n * 100).toFixed(1);

        let score = 0;
        for (const [key, val] of Object.entries(m)) score += val * LEERLING_METRICS[key].weight;
        return { metrics: m, score: +score.toFixed(1) };
    }

    return { metrics: {}, score: 0 };
}

function computeIndicators() {
    const dm = state.docentMetrics;
    const lm = state.leerlingMetrics;
    if (!dm || !lm) return;

    // Docenten: 9 gewogen metrics per locatie
    state.docentScoresByLocation = {};
    for (const loc of ['alle', 'Athena', 'Socrates']) {
        state.docentScoresByLocation[loc] = computeDocentScore(loc);
    }

    // Leerlingen: 8 gewogen metrics per locatie per bouw
    state.leerlingScoresByLocation = {};
    for (const loc of ['alle', 'Athena', 'Socrates']) {
        state.leerlingScoresByLocation[loc] = {
            onderbouw: computeLeerlingScore(loc, 'onderbouw'),
            bovenbouw: computeLeerlingScore(loc, 'bovenbouw'),
        };
    }

    saveWeekScore();
    console.log('Scores:', {
        docentSGL: state.docentScoresByLocation.alle.score,
        obSGL: state.leerlingScoresByLocation.alle.onderbouw.score,
        bvSGL: state.leerlingScoresByLocation.alle.bovenbouw.score,
    });
}

function computeDocentScore(locationFilter) {
    let teachers = state.docentMetrics.teachers;
    if (locationFilter !== 'alle') {
        teachers = teachers.filter(t => {
            const appts = state.teacherAppointments[t.code] || [];
            return appts.some(a => a.locations.some(loc => state.locationToBranch[loc] === locationFilter));
        });
    }
    const n = teachers.length;
    if (n === 0) return { metrics: {}, score: 0 };

    const metrics = {};
    metrics.M1 = +(teachers.filter(t => t.lokaalRating === 1).length / n * 100).toFixed(1);
    metrics.M2 = +(teachers.filter(t => t.lokaalRating === 2).length / n * 100).toFixed(1);
    metrics.M3 = +(teachers.filter(t => t.lokaalRating === 3).length / n * 100).toFixed(1);
    metrics.M4 = +(teachers.filter(t => t.lokaalRating === 4).length / n * 100).toFixed(1);
    metrics.M5 = +(teachers.filter(t => t.tussenuren.totaal === 0).length / n * 100).toFixed(1);
    metrics.M6 = +(teachers.filter(t => t.tussenuren.totaal > 2).length / n * 100).toFixed(1);
    metrics.M7 = +(teachers.filter(t => t.lateUren > 0).length / n * 100).toFixed(1);

    // M8: docenten die pendelen en ALLE bewegingen mét reistijd
    const pendelMet = teachers.filter(t =>
        t.pendelt && (parseFloat(t.pendelZonderReistijd) || 0) === 0
    ).length;
    metrics.M8 = +(pendelMet / n * 100).toFixed(1);

    // M9: docenten met ≥1 beweging zónder reistijd
    const pendelZonder = teachers.filter(t =>
        t.pendelt && (parseFloat(t.pendelZonderReistijd) || 0) > 0
    ).length;
    metrics.M9 = +(pendelZonder / n * 100).toFixed(1);

    // Gewogen score
    let score = 0;
    for (const [key, val] of Object.entries(metrics)) {
        score += val * DOCENT_METRICS[key].weight;
    }

    return { metrics, score: +score.toFixed(1) };
}

function saveWeekScore() {
    const weekCode = state.currentWeekCode || document.getElementById('week-select')?.value;
    if (!weekCode || !state.docentScoresByLocation) return;

    // Count total non-cancelled lessons this week (for vacation detection)
    let totalLessons = 0;
    for (const [, appts] of Object.entries(state.teacherAppointments)) {
        for (const a of appts) {
            if (!a.cancelled && a.slot > 0 && a.type === 'lesson') totalLessons++;
        }
    }

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entry = { week: weekCode, totalLessons };
    for (const loc of ['alle', 'Athena', 'Socrates']) {
        const ds = state.docentScoresByLocation[loc];
        const ls = state.leerlingScoresByLocation?.[loc];
        entry[loc] = {
            docentScore: ds.score,
            docentMetrics: { ...ds.metrics },
            onderbouwScore: ls?.onderbouw?.score ?? null,
            onderbouwMetrics: ls?.onderbouw?.metrics ? { ...ls.onderbouw.metrics } : {},
            bovenbouwScore: ls?.bovenbouw?.score ?? null,
            bovenbouwMetrics: ls?.bovenbouw?.metrics ? { ...ls.bovenbouw.metrics } : {},
        };
    }
    stored[weekCode] = entry;
    localStorage.setItem('rooster_scores', JSON.stringify(stored));

    // Accumulate teacher data for week-averaging
    if (state.docentMetrics && totalLessons > 0) {
        state.weeklyTeachers[weekCode] = state.docentMetrics.teachers.map(t => {
            // Collect teacher's locations this week
            const appts = state.teacherAppointments[t.code] || [];
            const locations = new Set();
            for (const a of appts) {
                for (const loc of a.locations) locations.add(loc);
            }
            return {
                code: t.code,
                tussenuren: { ...t.tussenuren },
                lokaalRating: t.lokaalRating,
                lokalen: new Set(t.lokalen),
                wisselingenBuitenPauze: t.wisselingenBuitenPauze,
                pendelt: t.pendelt,
                pendelBewegingen: [...t.pendelBewegingen],
                totalPendelBewegingen: t.totalPendelBewegingen,
                pendelZonderReistijd: t.pendelZonderReistijd,
                lateUren: t.lateUren,
                urenPerDag: { ...t.urenPerDag },
                maxAaneengesloten: t.maxAaneengesloten,
                fairnessScore: t.fairnessScore,
                locations: [...locations],
            };
        });
    }
}

// ================================================================
// COMPUTE ALL METRICS
// ================================================================

function computeAllMetrics() {
    state.docentMetrics = computeDocentMetrics();
    state.leerlingMetrics = computeLeerlingMetrics();
    computeIndicators();
    console.log('Metrics computed:', {
        docenten: state.docentMetrics.totalTeachers,
        klassen: state.leerlingMetrics.totalKlassen,
        docentScore: state.docentScoresByLocation?.alle?.score,
    });
}

// ================================================================
// RENDERING — MAIN
// ================================================================

function renderDashboard() {
    document.getElementById('placeholder').classList.add('hidden');

    // Show active tab
    document.querySelectorAll('.tab-content').forEach(el => el.classList.add('hidden'));
    const activeEl = document.getElementById(`tab-${state.activeTab}`);
    if (activeEl) activeEl.classList.remove('hidden');

    destroyCharts();

    // Locatiefilter permanent verbergen (alle 3 locaties worden tegelijk getoond)
    const locFilter = document.getElementById('location-filter');
    if (locFilter) locFilter.style.display = 'none';

    // Score-kaarten + trendgrafiek (altijd zichtbaar)
    if (state.docentScoresByLocation) {
        renderDocentScoreCards();
        renderTrendChart();
    }

    switch (state.activeTab) {
        case 'docenten': renderDocenten(); break;
        case 'leerlingen': renderLeerlingen(); break;
    }
}

function destroyCharts() {
    for (const c of Object.values(state.charts)) {
        c.destroy?.();
    }
    state.charts = {};
}

// ================================================================
// RENDERING — DOCENTEN TAB
// ================================================================

function renderDocenten() {
    if (!state.docentScoresByLocation) return;

    renderDocentScoreTrend();
    renderDocentMetricTable();
    renderDocentMetricTrend();
}

function renderDocentScoreCards() {
    const container = document.getElementById('doc-score-cards');
    if (!container) return;
    container.classList.remove('hidden');

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entries = filterVacationWeeks(Object.values(stored));
    const useAvg = entries.length >= 2;

    let html = '';
    for (const loc of ['alle', 'Athena', 'Socrates']) {
        const label = loc === 'alle' ? 'SGL' : loc;
        let score;
        if (useAvg) {
            const vals = entries.map(e => e[loc]?.docentScore).filter(v => v != null);
            score = vals.length > 0 ? +(vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(1) : null;
        } else {
            score = state.docentScoresByLocation[loc]?.score ?? null;
        }
        const displayScore = score != null ? (score >= 0 ? '+' + score : score) : '-';
        const c = locColor(loc);
        html += `<div class="doc-score-card">
            <div class="doc-score-label">${label}</div>
            <div class="doc-score-number" style="color:${c.text}">${displayScore}</div>
            <div class="doc-score-sub">${useAvg ? `gem. ${entries.length} weken` : 'huidige week'}</div>
        </div>`;
    }
    container.innerHTML = html;
}

function renderDocentScoreTrend() {
    const section = document.getElementById('doc-score-trend-section');
    if (!section) return;

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entries = filterVacationWeeks(Object.values(stored));

    if (entries.length < 2) {
        section.classList.add('hidden');
        return;
    }
    section.classList.remove('hidden');

    const labels = entries.map(e => `W${parseInt(e.week.slice(4))}`);
    const currentWeek = state.currentWeekCode;

    const mkLine = (label, loc, color, width) => ({
        label,
        data: entries.map(e => e[loc]?.docentScore ?? null),
        borderColor: color,
        backgroundColor: color.replace('0.9', '0.1'),
        borderWidth: width,
        tension: 0.3,
        pointRadius: entries.map(e => e.week === currentWeek ? 7 : 2),
        spanGaps: false,
    });

    const datasets = [
        mkLine('SGL', 'alle', LOC_COLORS.sgl.line, 3),
        mkLine('Athena', 'Athena', LOC_COLORS.athena.line, 2),
        mkLine('Socrates', 'Socrates', LOC_COLORS.socrates.line, 2),
    ];

    if (state.charts.docScoreTrend) state.charts.docScoreTrend.destroy();
    state.charts.docScoreTrend = new Chart(
        document.getElementById('chart-doc-score-trend'), {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        title: { display: true, text: 'Gewogen score' },
                    },
                },
                plugins: {
                    legend: { display: true, position: 'top' },
                    tooltip: {
                        callbacks: {
                            title: (items) => {
                                const idx = items[0]?.dataIndex;
                                if (idx == null) return '';
                                const e = entries[idx];
                                return `Week ${parseInt(e.week.slice(4))} (${e.week.slice(0, 4)})`;
                            },
                        },
                    },
                },
            },
        }
    );
}

function renderDocentMetricTable() {
    const tbody = document.querySelector('#doc-metric-table tbody');
    const header = document.getElementById('doc-metric-table-header');
    if (!tbody) return;

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entries = filterVacationWeeks(Object.values(stored));
    const useAvg = entries.length >= 2;

    if (header) {
        header.textContent = useAvg
            ? `Metric-overzicht (gemiddeld over ${entries.length} weken)`
            : 'Metric-overzicht';
    }

    const metricColor = (key, val) => {
        const w = DOCENT_METRICS[key].weight;
        if (w > 0) {
            // Positief: hoog = goed
            if (val >= 70) return 'metric-good';
            if (val >= 40) return 'metric-ok';
            return 'metric-bad';
        } else {
            // Negatief: laag = goed
            if (val <= 2) return 'metric-good';
            if (val <= 10) return 'metric-ok';
            return 'metric-bad';
        }
    };

    let rows = '';
    for (const [key, def] of Object.entries(DOCENT_METRICS)) {
        const weightDisplay = def.weight > 0 ? `+${def.weight}` : def.weight;
        const cells = ['alle', 'Athena', 'Socrates'].map(loc => {
            let val;
            if (useAvg) {
                const vals = entries.map(e => e[loc]?.docentMetrics?.[key]).filter(v => v != null);
                val = vals.length > 0 ? +(vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(1) : null;
            } else {
                val = state.docentScoresByLocation[loc]?.metrics?.[key] ?? null;
            }
            if (val == null) return '<td class="num">-</td>';
            return `<td class="num ${metricColor(key, val)}">${val}%</td>`;
        }).join('');

        rows += `<tr>
            <td>${def.label}</td>
            <td class="num"><strong>${weightDisplay}</strong></td>
            ${cells}
        </tr>`;
    }

    // Totaalrij
    const totalCells = ['alle', 'Athena', 'Socrates'].map(loc => {
        let score;
        if (useAvg) {
            const vals = entries.map(e => e[loc]?.docentScore).filter(v => v != null);
            score = vals.length > 0 ? +(vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(1) : null;
        } else {
            score = state.docentScoresByLocation[loc]?.score ?? null;
        }
        if (score == null) return '<td class="num">-</td>';
        const display = score >= 0 ? '+' + score : score;
        const c = locColor(loc);
        return `<td class="num" style="color:${c.text}"><strong>${display}</strong></td>`;
    }).join('');
    rows += `<tr class="total-row">
        <td><strong>Totaalscore</strong></td>
        <td class="num"></td>
        ${totalCells}
    </tr>`;

    tbody.innerHTML = rows;
}

function renderDocentMetricTrend() {
    const section = document.getElementById('doc-metric-trend-section');
    if (!section) return;

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entries = filterVacationWeeks(Object.values(stored));

    if (entries.length < 2) {
        section.classList.add('hidden');
        return;
    }
    section.classList.remove('hidden');

    const labels = entries.map(e => `W${parseInt(e.week.slice(4))}`);

    const metricDefs = [
        { key: 'M1', color: 'rgba(39, 174, 96, 0.9)', dash: [] },
        { key: 'M2', color: 'rgba(20, 143, 119, 0.9)', dash: [] },
        { key: 'M5', color: 'rgba(41, 128, 185, 0.9)', dash: [] },
        { key: 'M3', color: 'rgba(243, 156, 18, 0.85)', dash: [5, 3] },
        { key: 'M4', color: 'rgba(231, 76, 60, 0.85)', dash: [5, 3] },
        { key: 'M6', color: 'rgba(230, 126, 34, 0.85)', dash: [5, 3] },
        { key: 'M7', color: 'rgba(155, 89, 182, 0.85)', dash: [5, 3] },
        { key: 'M8', color: 'rgba(127, 140, 141, 0.85)', dash: [5, 3] },
        { key: 'M9', color: 'rgba(192, 57, 43, 0.9)', dash: [5, 3] },
    ];

    const datasets = metricDefs.map(d => ({
        label: DOCENT_METRICS[d.key].label,
        data: entries.map(e => e.alle?.docentMetrics?.[d.key] ?? null),
        borderColor: d.color,
        borderWidth: 2,
        borderDash: d.dash,
        tension: 0.3,
        pointRadius: 2,
        spanGaps: false,
    }));

    if (state.charts.docMetricTrend) state.charts.docMetricTrend.destroy();
    state.charts.docMetricTrend = new Chart(
        document.getElementById('chart-doc-metric-trend'), {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        min: 0,
                        suggestedMax: 100,
                        title: { display: true, text: '%' },
                    },
                },
                plugins: {
                    legend: { display: true, position: 'top' },
                },
            },
        }
    );
}

// ================================================================
// RENDERING — LEERLINGEN TAB
// ================================================================

function renderLeerlingen() {
    if (!state.leerlingScoresByLocation) return;

    const sub = state.leerlingSubFilter;
    const showOnderbouw = sub === 'alle' || sub === 'onderbouw';
    const showBovenbouw = sub === 'alle' || sub === 'bovenbouw';

    const onderbouwEl = document.getElementById('ll-onderbouw-section');
    const bovenbouwEl = document.getElementById('ll-bovenbouw-section');
    if (onderbouwEl) onderbouwEl.classList.toggle('hidden', !showOnderbouw);
    if (bovenbouwEl) bovenbouwEl.classList.toggle('hidden', !showBovenbouw);

    if (showOnderbouw) {
        renderLeerlingScoreCards('ll-ob-score-cards', 'onderbouw');
        renderLeerlingScoreTrend('ll-ob-score-trend-section', 'chart-ll-ob-score-trend', 'onderbouw', 'llObScoreTrend');
        renderLeerlingMetricTable('ll-ob-metric-table', 'll-ob-metric-table-header', 'onderbouw');
        renderLeerlingMetricTrend('ll-ob-metric-trend-section', 'chart-ll-ob-metric-trend', 'onderbouw', 'llObMetricTrend');
    }

    if (showBovenbouw) {
        const hasBV = state.leerlingMetrics?.bovenbouw != null;
        const cardsEl = document.getElementById('ll-bv-score-cards');
        if (!hasBV) {
            if (cardsEl) cardsEl.innerHTML =
                '<div class="doc-score-card" style="grid-column:1/-1;text-align:center">' +
                '<div class="doc-score-sub">Upload een CumLaude Excel (bovenbouw) voor per-leerling analyse</div></div>';
            // Hide trend/table/metric sections when no bovenbouw data
            document.getElementById('ll-bv-score-trend-section')?.classList.add('hidden');
            document.getElementById('ll-bv-metric-trend-section')?.classList.add('hidden');
            const bvTableSection = document.getElementById('ll-bv-metric-table')?.closest('.chart-section');
            if (bvTableSection) bvTableSection.style.display = 'none';
        } else {
            const bvTableSection = document.getElementById('ll-bv-metric-table')?.closest('.chart-section');
            if (bvTableSection) bvTableSection.style.display = '';
            renderLeerlingScoreCards('ll-bv-score-cards', 'bovenbouw');
            renderLeerlingScoreTrend('ll-bv-score-trend-section', 'chart-ll-bv-score-trend', 'bovenbouw', 'llBvScoreTrend');
            renderLeerlingMetricTable('ll-bv-metric-table', 'll-bv-metric-table-header', 'bovenbouw');
            renderLeerlingMetricTrend('ll-bv-metric-trend-section', 'chart-ll-bv-metric-trend', 'bovenbouw', 'llBvMetricTrend');
        }
    }
}

function renderLeerlingScoreCards(containerId, bouw) {
    const container = document.getElementById(containerId);
    if (!container) return;

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entries = filterVacationWeeks(Object.values(stored));
    const useAvg = entries.length >= 2;
    const scoreKey = bouw === 'onderbouw' ? 'onderbouwScore' : 'bovenbouwScore';

    let html = '';
    for (const loc of ['alle', 'Athena', 'Socrates']) {
        const label = loc === 'alle' ? 'SGL' : loc;
        let score;
        if (useAvg) {
            const vals = entries.map(e => e[loc]?.[scoreKey]).filter(v => v != null);
            score = vals.length > 0 ? +(vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(1) : null;
        } else {
            score = state.leerlingScoresByLocation?.[loc]?.[bouw]?.score ?? null;
        }
        const displayScore = score != null ? (score >= 0 ? '+' + score : score) : '-';
        const c = locColor(loc, bouw);
        html += `<div class="doc-score-card">
            <div class="doc-score-label">${label}</div>
            <div class="doc-score-number" style="color:${c.text}">${displayScore}</div>
            <div class="doc-score-sub">${useAvg ? `gem. ${entries.length} weken` : 'huidige week'}</div>
        </div>`;
    }
    container.innerHTML = html;
}

function renderLeerlingScoreTrend(sectionId, canvasId, bouw, chartKey) {
    const section = document.getElementById(sectionId);
    if (!section) return;

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entries = filterVacationWeeks(Object.values(stored));
    const scoreKey = bouw === 'onderbouw' ? 'onderbouwScore' : 'bovenbouwScore';

    if (entries.length < 2) { section.classList.add('hidden'); return; }
    section.classList.remove('hidden');

    const labels = entries.map(e => `W${parseInt(e.week.slice(4))}`);
    const currentWeek = state.currentWeekCode;

    const mkLine = (label, loc, color, width) => ({
        label,
        data: entries.map(e => e[loc]?.[scoreKey] ?? null),
        borderColor: color,
        backgroundColor: color.replace('0.9', '0.1'),
        borderWidth: width,
        tension: 0.3,
        pointRadius: entries.map(e => e.week === currentWeek ? 7 : 2),
        spanGaps: false,
    });

    if (state.charts[chartKey]) state.charts[chartKey].destroy();
    state.charts[chartKey] = new Chart(
        document.getElementById(canvasId), {
            type: 'line',
            data: {
                labels,
                datasets: [
                    mkLine('SGL', 'alle', locColor('alle', bouw).line, 3),
                    mkLine('Athena', 'Athena', locColor('Athena', bouw).line, 2),
                    mkLine('Socrates', 'Socrates', locColor('Socrates', bouw).line, 2),
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: { y: { title: { display: true, text: 'Gewogen score' } } },
                plugins: {
                    legend: { display: true, position: 'top' },
                    tooltip: {
                        callbacks: {
                            title: (items) => {
                                const idx = items[0]?.dataIndex;
                                if (idx == null) return '';
                                const e = entries[idx];
                                return `Week ${parseInt(e.week.slice(4))} (${e.week.slice(0, 4)})`;
                            },
                        },
                    },
                },
            },
        }
    );
}

function renderLeerlingMetricTable(tableId, headerId, bouw) {
    const tbody = document.querySelector(`#${tableId} tbody`);
    const header = document.getElementById(headerId);
    if (!tbody) return;

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entries = filterVacationWeeks(Object.values(stored));
    const useAvg = entries.length >= 2;
    const metricsKey = bouw === 'onderbouw' ? 'onderbouwMetrics' : 'bovenbouwMetrics';

    const bouwLabel = bouw === 'onderbouw' ? 'Onderbouw' : 'Bovenbouw';
    if (header) {
        header.textContent = useAvg
            ? `Metric-overzicht ${bouwLabel} (gemiddeld over ${entries.length} weken)`
            : `Metric-overzicht ${bouwLabel}`;
    }

    const metricColor = (key, val) => {
        const w = LEERLING_METRICS[key].weight;
        if (w > 0) {
            if (val >= 70) return 'metric-good';
            if (val >= 40) return 'metric-ok';
            return 'metric-bad';
        } else {
            if (val <= 2) return 'metric-good';
            if (val <= 10) return 'metric-ok';
            return 'metric-bad';
        }
    };

    let rows = '';
    for (const [key, def] of Object.entries(LEERLING_METRICS)) {
        const weightDisplay = def.weight > 0 ? `+${def.weight}` : def.weight;
        const cells = ['alle', 'Athena', 'Socrates'].map(loc => {
            let val;
            if (useAvg) {
                const vals = entries.map(e => e[loc]?.[metricsKey]?.[key]).filter(v => v != null);
                val = vals.length > 0 ? +(vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(1) : null;
            } else {
                val = state.leerlingScoresByLocation?.[loc]?.[bouw]?.metrics?.[key] ?? null;
            }
            if (val == null) return '<td class="num">-</td>';
            return `<td class="num ${metricColor(key, val)}">${val}%</td>`;
        }).join('');

        rows += `<tr>
            <td>${def.label}</td>
            <td class="num"><strong>${weightDisplay}</strong></td>
            ${cells}
        </tr>`;
    }

    // Totaalrij
    const scoreKey = bouw === 'onderbouw' ? 'onderbouwScore' : 'bovenbouwScore';
    const totalCells = ['alle', 'Athena', 'Socrates'].map(loc => {
        let score;
        if (useAvg) {
            const vals = entries.map(e => e[loc]?.[scoreKey]).filter(v => v != null);
            score = vals.length > 0 ? +(vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(1) : null;
        } else {
            score = state.leerlingScoresByLocation?.[loc]?.[bouw]?.score ?? null;
        }
        if (score == null) return '<td class="num">-</td>';
        const display = score >= 0 ? '+' + score : score;
        const c = locColor(loc, bouw);
        return `<td class="num" style="color:${c.text}"><strong>${display}</strong></td>`;
    }).join('');
    rows += `<tr class="total-row">
        <td><strong>Totaalscore</strong></td>
        <td class="num"></td>
        ${totalCells}
    </tr>`;

    tbody.innerHTML = rows;
}

function renderLeerlingMetricTrend(sectionId, canvasId, bouw, chartKey) {
    const section = document.getElementById(sectionId);
    if (!section) return;

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entries = filterVacationWeeks(Object.values(stored));
    const metricsKey = bouw === 'onderbouw' ? 'onderbouwMetrics' : 'bovenbouwMetrics';

    if (entries.length < 2) { section.classList.add('hidden'); return; }
    section.classList.remove('hidden');

    const labels = entries.map(e => `W${parseInt(e.week.slice(4))}`);

    const metricDefs = [
        { key: 'L1', color: 'rgba(39, 174, 96, 0.9)', dash: [] },
        { key: 'L4', color: 'rgba(20, 143, 119, 0.9)', dash: [] },
        { key: 'L2', color: 'rgba(231, 76, 60, 0.85)', dash: [5, 3] },
        { key: 'L3', color: 'rgba(192, 57, 43, 0.9)', dash: [5, 3] },
        { key: 'L5', color: 'rgba(155, 89, 182, 0.85)', dash: [5, 3] },
        { key: 'L6', color: 'rgba(243, 156, 18, 0.85)', dash: [5, 3] },
        { key: 'L7', color: 'rgba(230, 126, 34, 0.85)', dash: [5, 3] },
        { key: 'L8', color: 'rgba(127, 140, 141, 0.85)', dash: [5, 3] },
    ];

    const datasets = metricDefs.map(d => ({
        label: LEERLING_METRICS[d.key].label,
        data: entries.map(e => e.alle?.[metricsKey]?.[d.key] ?? null),
        borderColor: d.color,
        borderWidth: 2,
        borderDash: d.dash,
        tension: 0.3,
        pointRadius: 2,
        spanGaps: false,
    }));

    if (state.charts[chartKey]) state.charts[chartKey].destroy();
    state.charts[chartKey] = new Chart(
        document.getElementById(canvasId), {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: { min: 0, suggestedMax: 100, title: { display: true, text: '%' } },
                },
                plugins: { legend: { display: true, position: 'top' } },
            },
        }
    );
}

// ================================================================
// RENDERING — ACHTERAF TAB
// ================================================================

function renderAchteraf() {
    const m = state.achterafMetrics;
    if (!m) return;

    // KPIs
    setText('kpi-ach-uitval', m.uitvalPct + '%');
    setText('kpi-ach-vervangen', '-'); // Can't determine from current data

    // Management summary
    renderAchterafSummary(m);

    // Uitval per vak
    renderAchterafVak(m);

    // Uitval heatmap
    renderAchterafHeatmap(m);

    // Uitval per docent
    renderAchterafDocent(m);
}

function renderAchterafSummary(m) {
    const el = document.getElementById('ach-summary');
    let html = '<h3>Samenvatting Achteraf</h3>';

    const pct = parseFloat(m.uitvalPct);
    const cls = pct < 3 ? 'good' : pct < 8 ? 'warn' : 'bad';
    html += `<p>Van ${m.totalLessen} geplande lessen zijn er <span class="${cls}">${m.uitgevallen} uitgevallen (${m.uitvalPct}%)</span>. `;

    // Find worst subject
    const vakEntries = Object.entries(m.perVak)
        .filter(([, v]) => v.total >= 3)
        .map(([vak, v]) => ({ vak, pct: (v.cancelled / v.total * 100).toFixed(1), ...v }))
        .sort((a, b) => parseFloat(b.pct) - parseFloat(a.pct));

    if (vakEntries.length > 0 && parseFloat(vakEntries[0].pct) > 0) {
        html += `Meeste uitval bij: <strong>${vakEntries[0].vak}</strong> (${vakEntries[0].pct}%). `;
    }
    html += '</p>';
    el.innerHTML = html;
}

function renderAchterafVak(m) {
    const entries = Object.entries(m.perVak)
        .filter(([, v]) => v.total >= 2)
        .map(([vak, v]) => ({ vak, pct: v.cancelled / v.total * 100, ...v }))
        .sort((a, b) => b.pct - a.pct)
        .slice(0, 20);

    state.charts.achVak = new Chart(
        document.getElementById('chart-ach-vak'), {
            type: 'bar',
            data: {
                labels: entries.map(e => e.vak),
                datasets: [{
                    label: 'Uitval %',
                    data: entries.map(e => parseFloat(e.pct.toFixed(1))),
                    backgroundColor: entries.map(e =>
                        e.pct < 3 ? COLORS.green : e.pct < 8 ? COLORS.orange : COLORS.red
                    ),
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, title: { display: true, text: 'Uitval %' } },
                },
            },
        }
    );
}

function renderAchterafHeatmap(m) {
    const container = document.getElementById('ach-heatmap');
    const maxSlot = 9;

    // Find max percentage for color scaling
    let maxPct = 0;
    for (const [, v] of Object.entries(m.perDagUur)) {
        if (v.total >= 1) {
            const pct = v.cancelled / v.total;
            if (pct > maxPct) maxPct = pct;
        }
    }

    let html = '<table class="heatmap-table"><thead><tr><th></th>';
    for (let s = 1; s <= maxSlot; s++) html += `<th>Uur ${s}</th>`;
    html += '</tr></thead><tbody>';

    for (let d = 1; d <= 5; d++) {
        html += `<tr><td>${DAYS_LABELS[d - 1]}</td>`;
        for (let s = 1; s <= maxSlot; s++) {
            const key = `${d}-${s}`;
            const v = m.perDagUur[key];
            if (!v || v.total === 0) {
                html += '<td style="background:#f0f3f5">-</td>';
                continue;
            }
            const pct = v.cancelled / v.total;
            const pctStr = (pct * 100).toFixed(0) + '%';
            const bg = uitvalHeatColor(pct);
            const textColor = pct > 0.1 ? 'white' : '#333';
            html += `<td style="background:${bg};color:${textColor}" title="${v.cancelled}/${v.total}">${pctStr}</td>`;
        }
        html += '</tr>';
    }
    html += '</tbody></table>';
    container.innerHTML = html;
}

function renderAchterafDocent(m) {
    const tbody = document.querySelector('#ach-docent-table tbody');
    const entries = Object.entries(m.perDocent)
        .map(([code, v]) => ({ code, pct: v.total > 0 ? (v.cancelled / v.total * 100).toFixed(1) : 0, ...v }))
        .sort((a, b) => parseFloat(b.pct) - parseFloat(a.pct));

    tbody.innerHTML = entries.map(e => {
        const cls = parseFloat(e.pct) < 3 ? 'good' : parseFloat(e.pct) < 8 ? 'warn' : 'bad';
        return `<tr>
            <td>${e.code}</td>
            <td class="num">${e.total}</td>
            <td class="num">${e.cancelled}</td>
            <td class="num"><span class="badge ${cls}">${e.pct}%</span></td>
        </tr>`;
    }).join('');
}

function renderTrendChart() {
    const section = document.getElementById('trend-section');
    if (!section) return;

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entries = filterVacationWeeks(Object.values(stored));

    if (entries.length < 1) {
        section.classList.add('hidden');
        return;
    }
    section.classList.remove('hidden');

    const labels = entries.map(e => `W${parseInt(e.week.slice(4))}`);
    const currentWeek = state.currentWeekCode;

    const mkLine = (label, getter, color, width, dashed) => ({
        label,
        data: entries.map(getter),
        borderColor: color,
        backgroundColor: color.replace('0.9', '0.1'),
        borderWidth: width,
        borderDash: dashed ? [5, 3] : [],
        tension: 0.3,
        pointRadius: entries.map(e => e.week === currentWeek ? (width > 2 ? 7 : 5) : 2),
        spanGaps: false,
    });

    const datasets = [
        mkLine('SGL Docenten', e => e.alle?.docentScore ?? null, LOC_COLORS.sgl.line, 3, false),
        mkLine('Athena Docenten', e => e.Athena?.docentScore ?? null, LOC_COLORS.athena.line, 2, false),
        mkLine('Socrates Docenten', e => e.Socrates?.docentScore ?? null, LOC_COLORS.socrates.line, 2, false),
    ];

    if (state.charts.trend) state.charts.trend.destroy();
    state.charts.trend = new Chart(
        document.getElementById('chart-trend'), {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        title: { display: true, text: 'Gewogen score' },
                    },
                },
                plugins: {
                    legend: { display: true, position: 'top' },
                    tooltip: {
                        callbacks: {
                            title: (items) => {
                                const idx = items[0]?.dataIndex;
                                if (idx == null) return '';
                                const e = entries[idx];
                                return `Week ${parseInt(e.week.slice(4))} (${e.week.slice(0, 4)})`;
                            },
                        },
                    },
                },
            },
        }
    );
}

// ================================================================
// HELPER FUNCTIONS
// ================================================================

/**
 * Deduplicate pendel movements by day+from+to, keeping worst metReistijd.
 */
function deduplicatePendelBewegingen(bewegingen) {
    const map = new Map();
    for (const mv of bewegingen) {
        const key = `${mv.day}-${mv.from}-${mv.to}`;
        const existing = map.get(key);
        if (!existing) {
            map.set(key, { ...mv });
        } else {
            if (!mv.metReistijd) existing.metReistijd = false;
        }
    }
    return [...map.values()];
}

/**
 * Filter vacation weeks from stored score entries.
 * Detects on-the-fly: weeks with < 30% of median lesson count.
 */
function filterVacationWeeks(entries) {
    const withLessons = entries.filter(e => e.totalLessons > 0);
    if (withLessons.length < 3) {
        // Not enough data for median — just exclude entries without totalLessons
        return entries.filter(e => e.totalLessons > 0).sort((a, b) => a.week.localeCompare(b.week));
    }

    const counts = withLessons.map(e => e.totalLessons).sort((a, b) => a - b);
    const median = counts[Math.floor(counts.length / 2)];
    const threshold = median * 0.3;

    // Exclude: weeks without lesson count (legacy data) AND weeks below threshold
    return entries
        .filter(e => e.totalLessons >= threshold)
        .sort((a, b) => a.week.localeCompare(b.week));
}

function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
}

function heatColor(intensity) {
    if (intensity === 0) return '#f0f3f5';
    // Blue gradient: light to dark
    const r = Math.round(234 - intensity * 193);
    const g = Math.round(242 - intensity * 162);
    const b = Math.round(248 - intensity * 63);
    return `rgb(${r}, ${g}, ${b})`;
}

function uitvalHeatColor(pct) {
    if (pct <= 0) return '#f0f3f5';
    if (pct < 0.03) return 'rgba(39, 174, 96, 0.4)';
    if (pct < 0.08) return 'rgba(241, 196, 15, 0.6)';
    if (pct < 0.15) return 'rgba(230, 126, 34, 0.7)';
    return 'rgba(231, 76, 60, 0.8)';
}

// ================================================================
// TABLE SORTING
// ================================================================

function initSortableTable(tableId) {
    const table = document.getElementById(tableId);
    if (!table) return;

    table.querySelectorAll('thead th[data-sort]').forEach(th => {
        th.addEventListener('click', () => {
            const key = th.dataset.sort;
            const currentDir = state.sortState[tableId]?.key === key && state.sortState[tableId]?.dir === 'asc'
                ? 'desc' : 'asc';
            state.sortState[tableId] = { key, dir: currentDir };

            // Update sort indicators
            table.querySelectorAll('thead th').forEach(h => {
                h.classList.remove('sort-asc', 'sort-desc');
            });
            th.classList.add(currentDir === 'asc' ? 'sort-asc' : 'sort-desc');

            // Sort tbody rows
            const tbody = table.querySelector('tbody');
            const rows = [...tbody.querySelectorAll('tr')];
            const colIndex = [...th.parentElement.children].indexOf(th);

            rows.sort((a, b) => {
                const aText = a.children[colIndex]?.textContent.trim() || '';
                const bText = b.children[colIndex]?.textContent.trim() || '';
                const aNum = parseFloat(aText.replace(/[^0-9.-]/g, ''));
                const bNum = parseFloat(bText.replace(/[^0-9.-]/g, ''));

                if (!isNaN(aNum) && !isNaN(bNum)) {
                    return currentDir === 'asc' ? aNum - bNum : bNum - aNum;
                }
                return currentDir === 'asc'
                    ? aText.localeCompare(bText, 'nl', { numeric: true })
                    : bText.localeCompare(aText, 'nl', { numeric: true });
            });

            tbody.innerHTML = '';
            for (const row of rows) tbody.appendChild(row);
        });
    });
}

// ================================================================
// WEEK SELECTOR
// ================================================================

function populateWeekSelector() {
    const select = document.getElementById('week-select');

    // School year 2025-2026: week 36 (2025) to week 30 (2026)
    const weeks = [];

    // 2025: week 36 to 52
    for (let w = 36; w <= 52; w++) {
        weeks.push({ code: `2025${String(w).padStart(2, '0')}`, label: `Week ${w} (2025)` });
    }
    // 2026: week 1 to 30
    for (let w = 1; w <= 30; w++) {
        weeks.push({ code: `2026${String(w).padStart(2, '0')}`, label: `Week ${w} (2026)` });
    }

    select.innerHTML = weeks.map(w =>
        `<option value="${w.code}">${w.label}</option>`
    ).join('');

    // Default to current week
    const now = new Date();
    const currentWeek = getISOWeek(now);
    const currentYear = getISOWeekYear(now);
    const currentCode = `${currentYear}${String(currentWeek).padStart(2, '0')}`;

    // Select current week or closest
    const option = select.querySelector(`option[value="${currentCode}"]`);
    if (option) {
        option.selected = true;
    } else {
        // Select last available
        select.lastElementChild.selected = true;
    }
}

function getISOWeek(date) {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    d.setDate(d.getDate() + 3 - (d.getDay() + 6) % 7);
    const week1 = new Date(d.getFullYear(), 0, 4);
    return 1 + Math.round(((d - week1) / 86400000 - 3 + (week1.getDay() + 6) % 7) / 7);
}

function getISOWeekYear(date) {
    const d = new Date(date);
    d.setDate(d.getDate() + 3 - (d.getDay() + 6) % 7);
    return d.getFullYear();
}

// ================================================================
// TAB & FILTER MANAGEMENT
// ================================================================

function initTabs() {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.activeTab = btn.dataset.tab;
            if (state.docentMetrics) renderDashboard();
        });
    });
}

function initFilters() {
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.activeFilter = btn.dataset.filter;
            if (state.docentMetrics) renderDashboard();
        });
    });
}

function initLeerlingSubFilter() {
    document.querySelectorAll('.ll-sub-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.ll-sub-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.leerlingSubFilter = btn.dataset.sub;
            if (state.docentMetrics) renderDashboard();
        });
    });
}

function initLoadButton() {
    document.getElementById('btn-load-all').addEventListener('click', async () => {
        await triggerLoadAllWeeks();
    });

    // Week selector change: load that specific week for viewing
    document.getElementById('week-select').addEventListener('change', async () => {
        const weekCode = document.getElementById('week-select').value;
        if (!weekCode || !state.docentMetrics) return;
        try {
            await loadWeekData(weekCode);
        } catch (e) {
            console.error('Week switch failed:', e);
        }
    });
}

async function triggerLoadAllWeeks() {
    const btn = document.getElementById('btn-load-all');
    btn.disabled = true;
    btn.textContent = 'Bezig...';

    try {
        await loadAllWeeks();
    } catch (e) {
        console.error('Load all failed:', e);
        alert('Fout bij laden: ' + e.message);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Alle weken laden';
        showProgress(false);
    }
}

// ================================================================
// INITIALIZATION
// ================================================================

function showTokenInput() {
    document.getElementById('token-section').classList.remove('hidden');
    document.getElementById('filter-section').classList.add('hidden');
    document.getElementById('tab-nav').classList.add('hidden');
    document.getElementById('placeholder').classList.add('hidden');
    document.getElementById('btn-change-token').classList.add('hidden');
}

function hideTokenInput() {
    document.getElementById('token-section').classList.add('hidden');
    document.getElementById('filter-section').classList.remove('hidden');
    document.getElementById('tab-nav').classList.remove('hidden');
    document.getElementById('placeholder').classList.remove('hidden');
    document.getElementById('btn-change-token').classList.remove('hidden');
}

function initTokenHandlers() {
    document.getElementById('btn-token').addEventListener('click', async () => {
        const input = document.getElementById('token-input');
        const token = input.value.trim();
        if (!token) return;
        API_TOKEN = token;
        localStorage.setItem('zermelo_token', token);
        hideTokenInput();
        await loadAndInit();
    });

    document.getElementById('token-input').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') document.getElementById('btn-token').click();
    });

    document.getElementById('btn-change-token').addEventListener('click', () => {
        document.getElementById('token-input').value = '';
        showTokenInput();
    });
}

async function loadAndInit() {
    try {
        await loadReferenceData();
        console.log('Reference data loaded. Ready to analyze weeks.');
    } catch (e) {
        console.error('Failed to load reference data:', e);
        if (e.message.includes('401') || e.message.includes('403')) {
            localStorage.removeItem('zermelo_token');
            API_TOKEN = '';
            document.getElementById('placeholder').innerHTML =
                `<p style="color:var(--accent-red)">Token ongeldig of verlopen. Voer een nieuwe token in.</p>`;
            showTokenInput();
        } else {
            document.getElementById('placeholder').innerHTML =
                `<p style="color:var(--accent-red)">Fout bij laden referentiedata: ${e.message}</p>`;
        }
    }
}

async function init() {
    populateWeekSelector();
    initTabs();
    initFilters();
    initLeerlingSubFilter();
    initLoadButton();
    initTokenHandlers();
    initCumLaudeUpload();

    // Init sortable tables (metric tables are static, no sorting needed)

    // Check for stored token
    if (!API_TOKEN) {
        showTokenInput();
        return;
    }

    // Load reference data
    await loadAndInit();
}

// Expose for debugging
window.dashboardState = state;

// Start
document.addEventListener('DOMContentLoaded', init);
