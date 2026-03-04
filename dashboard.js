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

const COLORS = {
    primary: 'rgba(26, 82, 118, 0.8)',
    primaryBg: 'rgba(26, 82, 118, 0.15)',
    athena: 'rgba(41, 128, 185, 0.8)',
    athenaBg: 'rgba(41, 128, 185, 0.15)',
    socrates: 'rgba(230, 126, 34, 0.8)',
    socratesBg: 'rgba(230, 126, 34, 0.15)',
    green: 'rgba(39, 174, 96, 0.8)',
    greenBg: 'rgba(39, 174, 96, 0.15)',
    orange: 'rgba(230, 126, 34, 0.8)',
    orangeBg: 'rgba(230, 126, 34, 0.15)',
    red: 'rgba(231, 76, 60, 0.8)',
    redBg: 'rgba(231, 76, 60, 0.15)',
};

// ================================================================
// KEY INDICATORS — Streefwaarden met stoplicht-drempels
// ================================================================
const KEY_INDICATORS = {
    D1: { label: 'Lokaalwissel buiten pauze', unit: '%', green: 0, orange: 5, higherIsBetter: false, category: 'docenten' },
    D2: { label: 'Tussenuren >2/week', unit: '%', green: 5, orange: 15, higherIsBetter: false, category: 'docenten' },
    D3: { label: '5+ uur aaneengesloten', unit: '%', green: 0, orange: 5, higherIsBetter: false, category: 'docenten' },
    D4: { label: 'Pendel zonder reistijd', unit: '%', green: 0, orange: 25, higherIsBetter: false, category: 'docenten' },
    D5: { label: 'Late uren >1/week', unit: '%', green: 10, orange: 25, higherIsBetter: false, category: 'docenten' },
    D6: { label: 'Fairness spreiding', unit: '', green: 5, orange: 15, higherIsBetter: false, category: 'docenten' },
    O1: { label: 'Klassen met tussenuren', unit: '%', green: 5, orange: 20, higherIsBetter: false, category: 'onderbouw' },
    O2: { label: 'Mentoruur aan rand', unit: '%', green: 90, orange: 70, higherIsBetter: true, category: 'onderbouw' },
    O3: { label: 'Onwenselijke blokuren', unit: '%', green: 0, orange: 10, higherIsBetter: false, category: 'onderbouw' },
    O4: { label: 'Klassen met 8e/9e uur', unit: '%', green: 10, orange: 30, higherIsBetter: false, category: 'onderbouw' },
    B1: { label: 'Tussenuren ≥3 op dag', unit: '%', green: 0, orange: 10, higherIsBetter: false, category: 'bovenbouw' },
    B2: { label: 'Onwenselijke blokuren', unit: '%', green: 0, orange: 10, higherIsBetter: false, category: 'bovenbouw' },
    B3: { label: 'Leerlingen met 9e uur', unit: '%', green: 5, orange: 15, higherIsBetter: false, category: 'bovenbouw' },
    S1: { label: 'Uitval', unit: '%', green: 3, orange: 8, higherIsBetter: false, category: 'school' },
};

// ================================================================
// STATE
// ================================================================
let state = {
    activeTab: 'docenten',
    activeFilter: 'alle',
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
    achterafMetrics: null,
    // Key indicators
    indicators: null,       // { D1: {value, score, status}, ... }
    subtotals: null,        // { docenten: N, onderbouw: N, ... }
    overallScore: null,     // 0-10
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

            // Re-render if data already loaded
            if (state.docentMetrics) {
                computeAllMetrics();
                renderDashboard();
            }
        } catch (err) {
            statusEl.textContent = 'Fout: ' + err.message;
            console.error('CumLaude parse error:', err);
        }
    });
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

    showProgress(true);
    for (let i = 0; i < allWeeks.length; i++) {
        updateProgress(i, allWeeks.length, `Week ${parseInt(allWeeks[i].slice(4))} (${i + 1}/${allWeeks.length})`);
        try {
            await loadWeekData(allWeeks[i], { silent: true });
        } catch (e) {
            console.warn(`Failed to load week ${allWeeks[i]}:`, e);
        }
    }

    // Reload original week with rendering
    updateProgress(allWeeks.length, allWeeks.length, 'Klaar! Dashboard herladen...');
    await loadWeekData(originalWeek);
}

async function loadWeekData(weekCode, { silent = false } = {}) {
    if (!silent) showProgress(true);
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
            pendelDagen: [],
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

            // Tussenuren: gaten + wna-uren tussen eerste en laatste lesuur
            if (uniqueSlots.length >= 2) {
                const first = uniqueSlots[0];
                const last = uniqueSlots[uniqueSlots.length - 1];
                for (let s = first + 1; s < last; s++) {
                    // Tel als tussenuur als slot leeg is OF als het een wna-uur is
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

            // Lokalen per dag
            const dayRooms = new Set();
            for (const a of dayLessons) {
                for (const loc of a.locations) {
                    dayRooms.add(loc);
                    teacher.lokalen.add(loc);
                }
            }
            lokaalPerDag[day] = dayRooms;

            // Check room changes within day
            if (dayRooms.size > 1) {
                hasMultipleRooms = true;
                // Check each consecutive pair of lessons
                for (let i = 1; i < dayLessons.length; i++) {
                    const prevLoc = dayLessons[i - 1].locations[0] || '';
                    const currLoc = dayLessons[i].locations[0] || '';
                    if (prevLoc && currLoc && prevLoc !== currLoc) {
                        const prevSlot = dayLessons[i - 1].slot;
                        const currSlot = dayLessons[i].slot;
                        // Only count as problematic if slots are truly consecutive (no gap)
                        // If there's a free period between, the teacher has time to move
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

            // Pendel-check: teaches at both locations on same day
            const dayBranches = new Set();
            for (const a of dayLessons) {
                for (const loc of a.locations) {
                    const branch = state.locationToBranch[loc];
                    if (branch) dayBranches.add(branch);
                }
            }
            if (dayBranches.size > 1) {
                teacher.pendelt = true;
                // Check if enough travel time
                const branches = [...dayBranches];
                // Find last lesson at branch A and first lesson at branch B
                // For simplicity, flag as pendel day
                teacher.pendelDagen.push({
                    day,
                    locaties: [...dayBranches].join(' + '),
                    reistijdOk: checkReistijd(dayLessons, state.locationToBranch),
                });
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
    metrics.avg8e9e = metrics.totalTeachers > 0
        ? (metrics.teachers.reduce((s, t) => s + t.lateUren, 0) / metrics.totalTeachers).toFixed(1)
        : 0;

    return metrics;
}

/**
 * Check of er voldoende reistijd is bij pendelen.
 * Er moet minimaal 2 opeenvolgende vrije slots zijn (1 les + 1 pauze)
 * tussen de laatste les op locatie A en de eerste les op locatie B.
 */
function checkReistijd(dayLessons, locationToBranch) {
    // Group lessons by branch
    const byBranch = {};
    for (const a of dayLessons) {
        const branch = locationToBranch[a.locations[0]] || 'Onbekend';
        if (!byBranch[branch]) byBranch[branch] = [];
        byBranch[branch].push(a.slot);
    }

    const branchNames = Object.keys(byBranch);
    if (branchNames.length < 2) return true;

    // For each pair of branches, check gap
    for (let i = 0; i < branchNames.length; i++) {
        for (let j = i + 1; j < branchNames.length; j++) {
            const slotsA = byBranch[branchNames[i]].sort((a, b) => a - b);
            const slotsB = byBranch[branchNames[j]].sort((a, b) => a - b);
            // Check if A finishes before B starts (or vice versa)
            const lastA = slotsA[slotsA.length - 1];
            const firstB = slotsB[0];
            const lastB = slotsB[slotsB.length - 1];
            const firstA = slotsA[0];

            // Gap must be >= 2 (1 lesuur + 1 pauze)
            const gap1 = firstB - lastA;
            const gap2 = firstA - lastB;
            const gap = Math.max(gap1, gap2);
            if (gap < 2) return false;
        }
    }
    return true;
}

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
            }

            perStudent.push({
                totalLessons: activeLessons.length,
                tussenuren,
                tussenPerDag,
                lateUren8,
                lateUren9,
                laatsteUur,
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
// KEY INDICATOR SCORING
// ================================================================

function indicatorScore(value, greenMax, orangeMax, higherIsBetter) {
    if (higherIsBetter) {
        value = 100 - value;
        const g = 100 - greenMax;
        const o = 100 - orangeMax;
        greenMax = g;
        orangeMax = o;
    }
    if (value <= 0) return 10;
    if (greenMax > 0 && value <= greenMax) {
        return 10 - (value / greenMax) * 3; // 10 → 7
    }
    if (value <= orangeMax) {
        const range = orangeMax - greenMax;
        if (range <= 0) return 7;
        return 7 - ((value - greenMax) / range) * 3; // 7 → 4
    }
    // Red zone: 4 → 0
    const redRange = Math.max(orangeMax - greenMax, 1);
    const t = Math.min((value - orangeMax) / redRange, 1);
    return Math.max(4 - t * 4, 0);
}

function indicatorStatus(value, greenMax, orangeMax, higherIsBetter) {
    if (higherIsBetter) {
        if (value >= greenMax) return 'green';
        if (value >= orangeMax) return 'orange';
        return 'red';
    }
    if (value <= greenMax) return 'green';
    if (value <= orangeMax) return 'orange';
    return 'red';
}

function computeIndicator(key, value) {
    const def = KEY_INDICATORS[key];
    const v = +value.toFixed(1);
    return {
        value: v,
        score: +indicatorScore(v, def.green, def.orange, def.higherIsBetter).toFixed(1),
        status: indicatorStatus(v, def.green, def.orange, def.higherIsBetter),
    };
}

function computeIndicators() {
    const dm = state.docentMetrics;
    const lm = state.leerlingMetrics;
    const am = state.achterafMetrics;
    if (!dm || !lm || !am) return;

    // Bereken voor alle drie locatiefilters
    state.indicatorsByLocation = {};
    for (const loc of ['alle', 'Athena', 'Socrates']) {
        state.indicatorsByLocation[loc] = computeIndicatorsForFilter(loc);
    }

    // Backward compat: state.indicators/subtotals/overallScore = 'alle'
    const alle = state.indicatorsByLocation.alle;
    state.indicators = alle.indicators;
    state.subtotals = alle.subtotals;
    state.overallScore = alle.overall;

    saveWeekScore();
    console.log('Indicators per locatie:', {
        SGL: state.indicatorsByLocation.alle.overall,
        Athena: state.indicatorsByLocation.Athena.overall,
        Socrates: state.indicatorsByLocation.Socrates.overall,
    });
}

function computeIndicatorsForFilter(locationFilter) {
    const dm = state.docentMetrics;
    const lm = state.leerlingMetrics;

    // Filter teachers by location
    let teachers = dm.teachers;
    if (locationFilter !== 'alle') {
        teachers = teachers.filter(t => {
            const appts = state.teacherAppointments[t.code] || [];
            return appts.some(a => a.locations.some(loc => state.locationToBranch[loc] === locationFilter));
        });
    }

    // Filter onderbouw klassen
    let klassen = lm.klassen;
    if (locationFilter !== 'alle') {
        klassen = klassen.filter(k => k.branchName === locationFilter);
    }

    // Filter bovenbouw klassen
    let bvKlassen = lm.bovenbouw?.klassen;
    if (bvKlassen && locationFilter !== 'alle') {
        bvKlassen = bvKlassen.filter(k => k.branchName === locationFilter);
    }

    const indicators = {};
    const n = teachers.length;

    // === DOCENTEN ===
    indicators.D1 = computeIndicator('D1', n > 0 ? teachers.filter(t => t.lokaalRating === 4).length / n * 100 : 0);
    indicators.D2 = computeIndicator('D2', n > 0 ? teachers.filter(t => t.tussenuren.totaal > 2).length / n * 100 : 0);
    indicators.D3 = computeIndicator('D3', n > 0 ? teachers.filter(t => t.maxAaneengesloten >= 5).length / n * 100 : 0);

    const pend = teachers.filter(t => t.pendelt);
    indicators.D4 = computeIndicator('D4', pend.length > 0
        ? pend.filter(t => t.pendelDagen.some(d => !d.reistijdOk)).length / pend.length * 100 : 0);

    indicators.D5 = computeIndicator('D5', n > 0 ? teachers.filter(t => t.lateUren > 1).length / n * 100 : 0);

    const fScores = teachers.map(t => t.fairnessScore);
    const fMean = fScores.length > 0 ? fScores.reduce((s, v) => s + v, 0) / fScores.length : 0;
    const fVar = fScores.length > 0 ? fScores.reduce((s, v) => s + (v - fMean) ** 2, 0) / fScores.length : 0;
    indicators.D6 = computeIndicator('D6', Math.sqrt(fVar));

    // === ONDERBOUW ===
    const nk = klassen.length;
    indicators.O1 = computeIndicator('O1', nk > 0 ? klassen.filter(k => k.tussenuren.totaal > 0).length / nk * 100 : 0);

    const totalMentor = klassen.reduce((s, k) => s + k.mentorLessen, 0);
    const randMentor = klassen.reduce((s, k) => s + k.mentorAanRand, 0);
    indicators.O2 = computeIndicator('O2', totalMentor > 0 ? Math.round((randMentor / totalMentor) * 100) : 100);

    const obBlok = klassen.flatMap(k => k.blokuren);
    indicators.O3 = computeIndicator('O3', obBlok.length > 0 ? obBlok.filter(b => !b.isOk).length / obBlok.length * 100 : 0);
    indicators.O4 = computeIndicator('O4', nk > 0 ? klassen.filter(k => k.lateUren8 + k.lateUren9 > 0).length / nk * 100 : 0);

    // === BOVENBOUW ===
    if (bvKlassen && bvKlassen.length > 0) {
        const totalSt = bvKlassen.reduce((s, k) => s + k.studentCount, 0);
        const mt3 = bvKlassen.reduce((s, k) => s + (k.studentsWithMaxTussen3Plus || 0), 0);
        const u9 = bvKlassen.reduce((s, k) => s + (k.studentsWithUur9 || 0), 0);
        indicators.B1 = computeIndicator('B1', totalSt > 0 ? mt3 / totalSt * 100 : 0);
        const bvBlok = lm.bovenbouw.vakgroepBlokuren;
        indicators.B2 = computeIndicator('B2', bvBlok.length > 0 ? bvBlok.filter(b => !b.isOk).length / bvBlok.length * 100 : 0);
        indicators.B3 = computeIndicator('B3', totalSt > 0 ? u9 / totalSt * 100 : 0);
    }

    // === SCHOOL-BREED: uitval per locatie ===
    indicators.S1 = computeIndicator('S1', computeUitvalForFilter(locationFilter));

    // Subtotalen
    const categories = {};
    for (const [key, ind] of Object.entries(indicators)) {
        const cat = KEY_INDICATORS[key].category;
        if (!categories[cat]) categories[cat] = [];
        categories[cat].push(ind.score);
    }
    const subtotals = {};
    for (const [cat, scores] of Object.entries(categories)) {
        subtotals[cat] = +(scores.reduce((s, v) => s + v, 0) / scores.length).toFixed(1);
    }
    const allScores = Object.values(indicators).map(i => i.score);
    const overall = allScores.length > 0 ? +(allScores.reduce((s, v) => s + v, 0) / allScores.length).toFixed(1) : 0;

    return { indicators, subtotals, overall };
}

function computeUitvalForFilter(locationFilter) {
    const seen = new Set();
    let total = 0, cancelled = 0;
    for (const [, appts] of Object.entries(state.teacherAppointments)) {
        for (const a of appts) {
            if (a.slot <= 0 || a.type !== 'lesson') continue;
            if (a.subjects.some(s => EXCLUDED_SUBJECTS.has(s.toLowerCase()))) continue;
            if (locationFilter !== 'alle' && !a.locations.some(loc => state.locationToBranch[loc] === locationFilter)) continue;
            const key = `${a.day}-${a.slot}-${a.teachers.join(',')}-${a.subjects.join(',')}`;
            if (seen.has(key)) continue;
            seen.add(key);
            total++;
            if (a.cancelled) cancelled++;
        }
    }
    return total > 0 ? +(cancelled / total * 100).toFixed(1) : 0;
}

function saveWeekScore() {
    const weekCode = document.getElementById('week-select')?.value;
    if (!weekCode || !state.indicatorsByLocation) return;

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entry = { week: weekCode };
    for (const loc of ['alle', 'Athena', 'Socrates']) {
        const d = state.indicatorsByLocation[loc];
        entry[loc] = {
            overall: d.overall,
            docenten: d.subtotals.docenten ?? null,
            leerlingen: +(((d.subtotals.onderbouw || 0) + (d.subtotals.bovenbouw || d.subtotals.onderbouw || 0)) / (d.subtotals.bovenbouw ? 2 : 1)).toFixed(1),
            onderbouw: d.subtotals.onderbouw ?? null,
            bovenbouw: d.subtotals.bovenbouw ?? null,
        };
    }
    stored[weekCode] = entry;
    localStorage.setItem('rooster_scores', JSON.stringify(stored));
}

// ================================================================
// COMPUTE ALL METRICS
// ================================================================

function computeAllMetrics() {
    state.docentMetrics = computeDocentMetrics();
    state.leerlingMetrics = computeLeerlingMetrics();
    state.achterafMetrics = computeAchterafMetrics();
    computeIndicators();
    console.log('Metrics computed:', {
        docenten: state.docentMetrics.totalTeachers,
        klassen: state.leerlingMetrics.totalKlassen,
        lessen: state.achterafMetrics.totalLessen,
        overall: state.overallScore,
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

    // Overall rapportcijfer + trendgrafiek (altijd zichtbaar)
    if (state.indicators) {
        renderOverallScore();
        renderTrendChart();
    }

    switch (state.activeTab) {
        case 'docenten': renderDocenten(); break;
        case 'leerlingen': renderLeerlingen(); break;
        case 'achteraf': renderAchteraf(); break;
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
    const m = state.docentMetrics;
    if (!m) return;

    // Apply location filter
    const teachers = filterByLocation(m.teachers, 'teacher');

    // Indicator cards
    renderIndicatorCards('doc-indicators', ['D1', 'D2', 'D3', 'D4', 'D5', 'D6']);

    // KPIs
    setText('kpi-doc-total', teachers.length);
    const avgT = teachers.length > 0
        ? (teachers.reduce((s, t) => s + t.tussenuren.totaal, 0) / teachers.length).toFixed(1) : 0;
    setText('kpi-doc-tussenuren', avgT);
    setText('kpi-doc-pendelaars', teachers.filter(t => t.pendelt).length);
    const avg8 = teachers.length > 0
        ? (teachers.reduce((s, t) => s + t.lateUren, 0) / teachers.length).toFixed(1) : 0;
    setText('kpi-doc-late', avg8);

    // Management summary
    renderDocentSummary(teachers);

    // Tussenuren table
    renderDocentTussenuren(teachers);

    // Lokaalconstantie
    renderDocentLokaal(teachers);

    // Pendelen
    renderDocentPendel(teachers);

    // Spreiding & belasting
    renderDocentSpreiding(teachers);

    // Fairness
    renderDocentFairness(teachers);
}

function filterByLocation(teachers, type) {
    if (state.activeFilter === 'alle') return teachers;

    if (type === 'teacher') {
        return teachers.filter(t => {
            // Check if any of teacher's locations are at selected branch
            const appts = state.teacherAppointments[t.code] || [];
            return appts.some(a =>
                a.locations.some(loc =>
                    state.locationToBranch[loc] === state.activeFilter
                )
            );
        });
    }
    return teachers;
}

function renderDocentSummary(teachers) {
    const el = document.getElementById('doc-summary');
    if (teachers.length === 0) {
        el.innerHTML = '<h3>Samenvatting</h3><p>Geen data beschikbaar.</p>';
        return;
    }

    const worstTussenuren = [...teachers].sort((a, b) => b.tussenuren.totaal - a.tussenuren.totaal).slice(0, 3);
    const rating4Count = teachers.filter(t => t.lokaalRating === 4).length;
    const pendelCount = teachers.filter(t => t.pendelt).length;

    let html = '<h3>Samenvatting Docenten</h3>';
    html += `<p>${teachers.length} docenten geanalyseerd. `;

    const avgT = (teachers.reduce((s, t) => s + t.tussenuren.totaal, 0) / teachers.length).toFixed(1);
    if (parseFloat(avgT) <= 1) {
        html += `Gemiddeld <span class="good">${avgT}</span> tussenuren per week. `;
    } else if (parseFloat(avgT) <= 3) {
        html += `Gemiddeld <span class="warn">${avgT}</span> tussenuren per week. `;
    } else {
        html += `Gemiddeld <span class="bad">${avgT}</span> tussenuren per week. `;
    }

    if (worstTussenuren[0]?.tussenuren.totaal > 0) {
        html += `Meeste tussenuren: ${worstTussenuren.map(t =>
            `<strong>${t.code}</strong> (${t.tussenuren.totaal})`
        ).join(', ')}. `;
    }

    if (rating4Count > 0) {
        html += `<span class="bad">${rating4Count}</span> docent(en) wisselen van lokaal buiten pauzes. `;
    }
    if (pendelCount > 0) {
        html += `${pendelCount} docent(en) pendelen tussen locaties.`;
    }
    html += '</p>';
    el.innerHTML = html;
}

function renderDocentTussenuren(teachers) {
    const tbody = document.querySelector('#doc-tussenuren-table tbody');
    const sorted = [...teachers].sort((a, b) => b.tussenuren.totaal - a.tussenuren.totaal);

    tbody.innerHTML = sorted.map(t => {
        const cells = [1, 2, 3, 4, 5].map(d => {
            const v = t.tussenuren[d];
            const cls = v === 0 ? 'good' : v === 1 ? 'warn' : 'bad';
            return `<td class="num"><span class="badge ${cls}">${v}</span></td>`;
        }).join('');
        const totCls = t.tussenuren.totaal === 0 ? 'good' : t.tussenuren.totaal <= 2 ? 'warn' : 'bad';
        return `<tr>
            <td>${t.code}</td>
            ${cells}
            <td class="num"><strong><span class="badge ${totCls}">${t.tussenuren.totaal}</span></strong></td>
        </tr>`;
    }).join('');

    // Bar chart: top 10
    const top10 = sorted.filter(t => t.tussenuren.totaal > 0).slice(0, 10);
    state.charts.docTussenuren = new Chart(
        document.getElementById('chart-doc-tussenuren'), {
            type: 'bar',
            data: {
                labels: top10.map(t => t.code),
                datasets: [{
                    label: 'Tussenuren',
                    data: top10.map(t => t.tussenuren.totaal),
                    backgroundColor: COLORS.red,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, title: { display: true, text: 'Tussenuren' } },
                },
            },
        }
    );
}

function renderDocentLokaal(teachers) {
    const ratingLabels = {
        1: 'Vast lokaal',
        2: 'Vast per dag',
        3: 'Wissel in pauze',
        4: 'Wissel buiten pauze',
    };

    const tbody = document.querySelector('#doc-lokaal-table tbody');
    const sorted = [...teachers].sort((a, b) => b.lokaalRating - a.lokaalRating || b.wisselingenBuitenPauze - a.wisselingenBuitenPauze);

    tbody.innerHTML = sorted.map(t => `<tr>
        <td>${t.code}</td>
        <td><span class="badge rating-${t.lokaalRating}">${ratingLabels[t.lokaalRating]}</span></td>
        <td class="num">${t.lokalen.size}</td>
        <td class="num">${t.wisselingenBuitenPauze > 0
            ? `<span class="badge bad">${t.wisselingenBuitenPauze}</span>`
            : '<span class="badge good">0</span>'}</td>
    </tr>`).join('');

    // Donut chart: rating distribution
    const counts = [0, 0, 0, 0];
    for (const t of teachers) counts[t.lokaalRating - 1]++;

    state.charts.docLokaal = new Chart(
        document.getElementById('chart-doc-lokaal'), {
            type: 'doughnut',
            data: {
                labels: Object.values(ratingLabels),
                datasets: [{
                    data: counts,
                    backgroundColor: [COLORS.green, 'rgba(20, 143, 119, 0.7)', COLORS.orange, COLORS.red],
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'bottom' },
                },
            },
        }
    );
}

function renderDocentPendel(teachers) {
    const pendelaars = teachers.filter(t => t.pendelt);
    const tbody = document.querySelector('#doc-pendel-table tbody');

    if (pendelaars.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text-light)">Geen pendelaars gevonden</td></tr>';
        return;
    }

    const rows = [];
    for (const t of pendelaars) {
        for (const pd of t.pendelDagen) {
            rows.push(`<tr>
                <td>${t.code}</td>
                <td>${DAYS_LABELS[pd.day - 1]}</td>
                <td>${pd.locaties}</td>
                <td class="${pd.reistijdOk ? 'status-ok' : 'status-problem'}">
                    ${pd.reistijdOk ? 'Ja' : 'Nee'}
                </td>
            </tr>`);
        }
    }
    tbody.innerHTML = rows.join('');
}

function renderDocentSpreiding(teachers) {
    // Heatmap: teachers x days, cell = number of lessons
    const container = document.getElementById('doc-heatmap');
    const sorted = [...teachers].sort((a, b) => a.code.localeCompare(b.code));

    let maxVal = 0;
    for (const t of sorted) {
        for (const d of [1, 2, 3, 4, 5]) {
            if (t.urenPerDag[d] > maxVal) maxVal = t.urenPerDag[d];
        }
    }

    let html = '<table class="heatmap-table"><thead><tr><th>Docent</th>';
    for (const d of DAYS_LABELS) html += `<th>${d}</th>`;
    html += '</tr></thead><tbody>';

    for (const t of sorted) {
        html += `<tr><td>${t.code}</td>`;
        for (const d of [1, 2, 3, 4, 5]) {
            const val = t.urenPerDag[d];
            const intensity = maxVal ? val / maxVal : 0;
            const bg = heatColor(intensity);
            const textColor = intensity > 0.6 ? 'white' : '#333';
            html += `<td style="background:${bg};color:${textColor}">${val}</td>`;
        }
        html += '</tr>';
    }
    html += '</tbody></table>';
    container.innerHTML = html;

    // Bar chart: 8e/9e uren per docent
    const withLate = teachers.filter(t => t.lateUren > 0)
        .sort((a, b) => b.lateUren - a.lateUren)
        .slice(0, 15);

    state.charts.docLate = new Chart(
        document.getElementById('chart-doc-late'), {
            type: 'bar',
            data: {
                labels: withLate.map(t => t.code),
                datasets: [{
                    label: '8e/9e uren',
                    data: withLate.map(t => t.lateUren),
                    backgroundColor: COLORS.orange,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, title: { display: true, text: 'Aantal' } },
                },
            },
        }
    );
}

function renderDocentFairness(teachers) {
    if (teachers.length === 0) return;

    const scores = teachers.map(t => t.fairnessScore);
    const mean = scores.reduce((s, v) => s + v, 0) / scores.length;
    const stdev = Math.sqrt(scores.reduce((s, v) => s + (v - mean) ** 2, 0) / scores.length);

    // Histogram
    const min = Math.min(...scores);
    const max = Math.max(...scores);
    const binCount = Math.min(10, Math.ceil(Math.sqrt(scores.length)));
    const binWidth = (max - min) / binCount || 1;
    const bins = Array(binCount).fill(0);
    const binLabels = [];

    for (let i = 0; i < binCount; i++) {
        const lo = min + i * binWidth;
        const hi = lo + binWidth;
        binLabels.push(`${lo.toFixed(0)}-${hi.toFixed(0)}`);
        for (const s of scores) {
            if (s >= lo && (s < hi || (i === binCount - 1 && s <= hi))) bins[i]++;
        }
    }

    state.charts.docFairness = new Chart(
        document.getElementById('chart-doc-fairness'), {
            type: 'bar',
            data: {
                labels: binLabels,
                datasets: [{
                    label: 'Docenten',
                    data: bins,
                    backgroundColor: COLORS.primaryBg,
                    borderColor: COLORS.primary,
                    borderWidth: 1,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: { title: { display: true, text: 'Kwaliteitsscore (lager = beter)' } },
                    y: { beginAtZero: true, title: { display: true, text: 'Aantal docenten' } },
                },
            },
        }
    );

    // Stats
    const statsEl = document.getElementById('doc-fairness-stats');
    const fairnessLevel = stdev < 5 ? 'good' : stdev < 15 ? 'warn' : 'bad';
    const fairnessText = stdev < 5 ? 'Eerlijk' : stdev < 15 ? 'Redelijk' : 'Ongelijk';

    statsEl.innerHTML = `
        <div class="fairness-stat">
            <div class="value">${mean.toFixed(1)}</div>
            <div class="label">Gemiddelde score</div>
        </div>
        <div class="fairness-stat">
            <div class="value" style="color: var(--${fairnessLevel === 'good' ? 'success' : fairnessLevel === 'warn' ? 'accent' : 'accent-red'})">${stdev.toFixed(1)}</div>
            <div class="label">Standaarddeviatie</div>
        </div>
        <div class="fairness-stat">
            <div class="value">${fairnessText}</div>
            <div class="label">Verdeling</div>
        </div>
    `;
}

// ================================================================
// RENDERING — LEERLINGEN TAB
// ================================================================

function renderLeerlingen() {
    const m = state.leerlingMetrics;
    if (!m) return;

    const sub = state.leerlingSubFilter;
    const showOnderbouw = sub === 'alle' || sub === 'onderbouw';
    const showBovenbouw = sub === 'alle' || sub === 'bovenbouw';

    // Toggle section visibility
    const onderbouwEl = document.getElementById('ll-onderbouw-section');
    const bovenbouwEl = document.getElementById('ll-bovenbouw-section');
    if (onderbouwEl) onderbouwEl.classList.toggle('hidden', !showOnderbouw);
    if (bovenbouwEl) bovenbouwEl.classList.toggle('hidden', !showBovenbouw);

    if (showOnderbouw) {
        // Indicator cards onderbouw
        renderIndicatorCards('ll-indicators', ['O1', 'O2', 'O3', 'O4']);

        const klassen = filterKlassenByLocation(m.klassen);

        // KPIs (onderbouw)
        setText('kpi-ll-klassen', klassen.length);
        const avgT = klassen.length > 0
            ? (klassen.reduce((s, k) => s + k.tussenuren.totaal, 0) / (klassen.length * 5)).toFixed(2) : 0;
        setText('kpi-ll-tussenuren', avgT);

        const totalMentor = klassen.reduce((s, k) => s + k.mentorLessen, 0);
        const randMentor = klassen.reduce((s, k) => s + k.mentorAanRand, 0);
        setText('kpi-ll-mentor', totalMentor > 0 ? Math.round((randMentor / totalMentor) * 100) + '%' : '-');

        const avg8 = klassen.length > 0
            ? (klassen.reduce((s, k) => s + k.lateUren8 + k.lateUren9, 0) / klassen.length).toFixed(1) : 0;
        setText('kpi-ll-late', avg8);

        renderLeerlingenSummary(klassen);
        renderLeerlingenTussenuren(klassen);
        renderLeerlingenMentor(klassen);
        renderLeerlingenKlasgrootte(klassen);
        renderLeerlingenDaglengte(klassen);
        renderLeerlingenBlokuren(klassen);
        renderLeerlingenSpreiding(klassen);
    }

    if (showBovenbouw) {
        renderBovenbouw();
    }
}

function filterKlassenByLocation(klassen) {
    if (state.activeFilter === 'alle') return klassen;
    return klassen.filter(k => k.branchName === state.activeFilter);
}

function renderLeerlingenSummary(klassen) {
    const el = document.getElementById('ll-summary');
    if (klassen.length === 0) {
        el.innerHTML = '<h3>Samenvatting</h3><p>Geen data beschikbaar.</p>';
        return;
    }

    const worstSwissCheese = [...klassen].sort((a, b) => b.patternScore - a.patternScore).slice(0, 3);
    const totalMentor = klassen.reduce((s, k) => s + k.mentorLessen, 0);
    const randPct = totalMentor > 0
        ? Math.round((klassen.reduce((s, k) => s + k.mentorAanRand, 0) / totalMentor) * 100)
        : 0;

    let html = '<h3>Samenvatting Leerlingen</h3>';
    html += `<p>${klassen.length} klassen geanalyseerd. `;

    if (worstSwissCheese[0]?.patternScore > 0) {
        html += `Slechtste tussenuren-patroon: ${worstSwissCheese.map(k =>
            `<strong>${k.name}</strong> (score ${k.patternScore})`
        ).join(', ')}. `;
    }

    if (randPct >= 80) {
        html += `<span class="good">${randPct}%</span> mentoruren aan de rand van de dag. `;
    } else if (randPct >= 50) {
        html += `<span class="warn">${randPct}%</span> mentoruren aan de rand van de dag. `;
    } else if (totalMentor > 0) {
        html += `<span class="bad">${randPct}%</span> mentoruren aan de rand van de dag. `;
    }

    html += '</p>';
    el.innerHTML = html;
}

function renderLeerlingenTussenuren(klassen) {
    const tbody = document.querySelector('#ll-tussenuren-table tbody');
    const sorted = [...klassen].sort((a, b) => b.patternScore - a.patternScore);

    tbody.innerHTML = sorted.map(k => {
        const cells = [1, 2, 3, 4, 5].map(d => {
            const v = k.tussenuren[d];
            const cls = v === 0 ? 'good' : v === 1 ? 'warn' : 'bad';
            return `<td class="num"><span class="badge ${cls}">${v}</span></td>`;
        }).join('');
        const scoreCls = k.patternScore === 0 ? 'good' : k.patternScore <= 3 ? 'warn' : 'bad';
        return `<tr>
            <td>${k.name}</td>
            ${cells}
            <td class="num">${k.tussenuren.totaal}</td>
            <td class="num"><span class="badge ${scoreCls}">${k.patternScore}</span></td>
        </tr>`;
    }).join('');

    // Bar chart: top 10 worst pattern scores
    const top10 = sorted.filter(k => k.patternScore > 0).slice(0, 10);
    state.charts.llTussenuren = new Chart(
        document.getElementById('chart-ll-tussenuren'), {
            type: 'bar',
            data: {
                labels: top10.map(k => k.name),
                datasets: [{
                    label: 'Patroon-score',
                    data: top10.map(k => k.patternScore),
                    backgroundColor: COLORS.red,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, title: { display: true, text: 'Swiss cheese score' } },
                },
            },
        }
    );
}

function renderLeerlingenMentor(klassen) {
    const withMentor = klassen.filter(k => k.mentorLessen > 0)
        .sort((a, b) => a.name.localeCompare(b.name, 'nl', { numeric: true }));

    if (withMentor.length === 0) {
        document.getElementById('chart-ll-mentor').parentElement
            .insertAdjacentHTML('afterbegin', '<p style="color:var(--text-light)">Geen mentoruren gevonden</p>');
        return;
    }

    const randPcts = withMentor.map(k =>
        k.mentorLessen > 0 ? Math.round((k.mentorAanRand / k.mentorLessen) * 100) : 0
    );
    const middenPcts = randPcts.map(p => 100 - p);

    state.charts.llMentor = new Chart(
        document.getElementById('chart-ll-mentor'), {
            type: 'bar',
            data: {
                labels: withMentor.map(k => k.name),
                datasets: [
                    {
                        label: 'Aan de rand',
                        data: randPcts,
                        backgroundColor: COLORS.green,
                    },
                    {
                        label: 'Midden',
                        data: middenPcts,
                        backgroundColor: COLORS.red,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'top' } },
                scales: {
                    x: { stacked: true },
                    y: { stacked: true, max: 100, title: { display: true, text: '%' } },
                },
            },
        }
    );
}

function renderLeerlingenKlasgrootte(klassen) {
    const withSize = klassen.filter(k => k.avgSize > 0)
        .sort((a, b) => a.name.localeCompare(b.name, 'nl', { numeric: true }));

    if (withSize.length === 0) return;

    const bgColors = withSize.map(k => {
        if (k.avgSize < 20) return 'rgba(41, 128, 185, 0.7)';   // blauw (klein)
        if (k.avgSize <= 28) return COLORS.green;                 // groen (ideaal)
        if (k.avgSize <= 32) return COLORS.orange;                // oranje (groot)
        return COLORS.red;                                         // rood (te groot)
    });

    state.charts.llKlasgrootte = new Chart(
        document.getElementById('chart-ll-klasgrootte'), {
            type: 'bar',
            data: {
                labels: withSize.map(k => k.name),
                datasets: [{
                    label: 'Gemiddelde klasgrootte',
                    data: withSize.map(k => k.avgSize),
                    backgroundColor: bgColors,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, title: { display: true, text: 'Leerlingen' } },
                },
            },
        }
    );
}

function renderLeerlingenDaglengte(klassen) {
    // Heatmap: klas x dag, cel = laatste lesuur
    const container = document.getElementById('ll-daglengte-heatmap');
    const sorted = [...klassen].sort((a, b) => a.name.localeCompare(b.name, 'nl', { numeric: true }));

    let html = '<table class="heatmap-table"><thead><tr><th>Klas</th>';
    for (const d of DAYS_LABELS) html += `<th>${d}</th>`;
    html += '</tr></thead><tbody>';

    for (const k of sorted) {
        html += `<tr><td>${k.name}</td>`;
        for (const d of [1, 2, 3, 4, 5]) {
            const val = k.laatsteUur[d];
            let bg = '#f0f3f5';
            let textColor = '#333';
            if (val >= 9) { bg = 'rgba(231, 76, 60, 0.7)'; textColor = 'white'; }
            else if (val >= 8) { bg = 'rgba(230, 126, 34, 0.6)'; textColor = 'white'; }
            else if (val >= 7) { bg = 'rgba(241, 196, 15, 0.5)'; }
            else if (val > 0) { bg = 'rgba(39, 174, 96, 0.3)'; }
            html += `<td style="background:${bg};color:${textColor}">${val || '-'}</td>`;
        }
        html += '</tr>';
    }
    html += '</tbody></table>';
    container.innerHTML = html;

    // 8e/9e uren table
    const tbody = document.querySelector('#ll-late-table tbody');
    const sortedLate = [...klassen]
        .filter(k => k.lateUren8 > 0 || k.lateUren9 > 0)
        .sort((a, b) => (b.lateUren8 + b.lateUren9) - (a.lateUren8 + a.lateUren9));

    tbody.innerHTML = sortedLate.map(k => `<tr>
        <td>${k.name}</td>
        <td class="num">${k.lateUren8 > 0 ? `<span class="badge warn">${k.lateUren8}</span>` : '0'}</td>
        <td class="num">${k.lateUren9 > 0 ? `<span class="badge bad">${k.lateUren9}</span>` : '0'}</td>
    </tr>`).join('');
}

function renderLeerlingenBlokuren(klassen) {
    const tbody = document.querySelector('#ll-blokuren-table tbody');

    // Collect all blokuren entries
    const rows = [];
    for (const k of klassen) {
        for (const b of k.blokuren) {
            rows.push({ klas: k.name, ...b });
        }
    }

    // Sort: problematic ones first (isOk = false), then alphabetically
    rows.sort((a, b) => {
        if (a.isOk !== b.isOk) return a.isOk ? 1 : -1;
        return a.klas.localeCompare(b.klas, 'nl', { numeric: true }) || a.vak.localeCompare(b.vak);
    });

    if (rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text-light)">Geen blokuren gevonden</td></tr>';
        return;
    }

    tbody.innerHTML = rows.slice(0, 80).map(r => {
        const statusCls = r.isOk ? 'good' : 'warn';
        const statusText = r.isOk ? 'Normaal' : 'Onwenselijk';
        return `<tr>
            <td>${r.klas}</td>
            <td>${r.vak}</td>
            <td>${DAYS_LABELS[r.day - 1]}</td>
            <td class="num">${r.slots}</td>
            <td class="num"><span class="badge ${statusCls}">${statusText}</span></td>
        </tr>`;
    }).join('');

    // Chart: count blokuren per subject, split OK vs problematic
    const vakCounts = {};
    for (const r of rows) {
        if (!vakCounts[r.vak]) vakCounts[r.vak] = { ok: 0, problematic: 0 };
        if (r.isOk) vakCounts[r.vak].ok++;
        else vakCounts[r.vak].problematic++;
    }

    const sortedVakken = Object.entries(vakCounts)
        .sort((a, b) => (b[1].ok + b[1].problematic) - (a[1].ok + a[1].problematic))
        .slice(0, 15);

    state.charts.llBlokuren = new Chart(
        document.getElementById('chart-ll-blokuren'), {
            type: 'bar',
            data: {
                labels: sortedVakken.map(([vak]) => vak),
                datasets: [
                    {
                        label: 'Normaal (LO/BV etc)',
                        data: sortedVakken.map(([, c]) => c.ok),
                        backgroundColor: COLORS.green,
                    },
                    {
                        label: 'Onwenselijk',
                        data: sortedVakken.map(([, c]) => c.problematic),
                        backgroundColor: COLORS.orange,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'top' } },
                scales: {
                    x: { stacked: true },
                    y: { stacked: true, beginAtZero: true, title: { display: true, text: 'Aantal blokuren' } },
                },
            },
        }
    );
}

function renderLeerlingenSpreiding(klassen) {
    const tbody = document.querySelector('#ll-spreiding-table tbody');

    // Collect all bad spreiding entries (score < 0.7)
    const rows = [];
    for (const k of klassen) {
        for (const vs of k.vakSpreiding) {
            if (vs.score < 0.7) { // Less than 70% of ideal spread
                rows.push({ klas: k.name, ...vs });
            }
        }
    }
    rows.sort((a, b) => a.score - b.score);

    if (rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text-light)">Geen problemen met lesspreiding gevonden</td></tr>';
        return;
    }

    tbody.innerHTML = rows.slice(0, 50).map(r => {
        const scoreCls = r.score < 0.4 ? 'bad' : 'warn';
        const pct = Math.round(r.score * 100);
        return `<tr>
            <td>${r.klas}</td>
            <td>${r.vak}</td>
            <td class="num">${r.lessen}</td>
            <td class="num">${r.dagen}</td>
            <td class="num"><span class="badge ${scoreCls}">${pct}%</span></td>
        </tr>`;
    }).join('');
}

// ================================================================
// RENDERING — BOVENBOUW
// ================================================================

function renderBovenbouw() {
    const m = state.leerlingMetrics;
    const bv = m?.bovenbouw;
    const summaryEl = document.getElementById('bv-summary');
    const kpiEl = document.getElementById('bv-kpi-section');
    const dataEl = document.getElementById('bv-data-section');

    if (!bv) {
        // No CumLaude data uploaded
        if (summaryEl) summaryEl.innerHTML =
            '<h3>Bovenbouw (4-6)</h3>' +
            '<p>Upload een CumLaude Excel-bestand (met alle bovenbouw lesgroepen) om per-leerling roosteranalyse te zien. ' +
            'Zonder CumLaude data zijn bovenbouw-analyses niet beschikbaar omdat leerlingen individuele vakkenpakketten hebben.</p>';
        if (kpiEl) kpiEl.classList.add('hidden');
        if (dataEl) dataEl.classList.add('hidden');
        return;
    }

    if (kpiEl) kpiEl.classList.remove('hidden');
    if (dataEl) dataEl.classList.remove('hidden');

    // Indicator cards bovenbouw
    renderIndicatorCards('bv-indicators', ['B1', 'B2', 'B3']);

    const klassen = filterKlassenByLocation(bv.klassen);

    // KPIs
    setText('kpi-bv-leerlingen', bv.totalStudents);
    setText('kpi-bv-klassen', klassen.length);
    setText('kpi-bv-tussenuren', bv.avgTussenuren);
    setText('kpi-bv-late', bv.avgLateUren);

    // Summary
    renderBovenbouwSummary(klassen, bv);

    // Tussenuren table
    renderBovenbouwTussenuren(klassen);

    // Daglengte heatmap
    renderBovenbouwDaglengte(klassen);

    // Blokuren
    renderBovenbouwBlokuren(bv.vakgroepBlokuren);
}

function renderBovenbouwSummary(klassen, bv) {
    const el = document.getElementById('bv-summary');
    if (!el) return;

    let html = '<h3>Samenvatting Bovenbouw (4-6)</h3>';
    html += `<p>${bv.totalStudents} leerlingen in ${klassen.length} klassen geanalyseerd (per-leerling via CumLaude). `;

    if (bv.avgTussenuren <= 1) {
        html += `Gemiddeld <span class="good">${bv.avgTussenuren}</span> tussenuren per leerling per week. `;
    } else if (bv.avgTussenuren <= 3) {
        html += `Gemiddeld <span class="warn">${bv.avgTussenuren}</span> tussenuren per leerling per week. `;
    } else {
        html += `Gemiddeld <span class="bad">${bv.avgTussenuren}</span> tussenuren per leerling per week. `;
    }

    // Worst klassen
    const worst = [...klassen].sort((a, b) => b.avgTussenuren - a.avgTussenuren).slice(0, 3)
        .filter(k => k.avgTussenuren > 0);
    if (worst.length > 0) {
        html += `Meeste tussenuren: ${worst.map(k =>
            `<strong>${k.name}</strong> (gem. ${k.avgTussenuren})`
        ).join(', ')}. `;
    }

    const problemBlokuren = bv.vakgroepBlokuren.filter(b => !b.isOk).length;
    if (problemBlokuren > 0) {
        html += `<span class="warn">${problemBlokuren}</span> onwenselijke blokuren in vakgroepen. `;
    }

    html += '</p>';
    el.innerHTML = html;
}

function renderBovenbouwTussenuren(klassen) {
    const tbody = document.querySelector('#bv-tussenuren-table tbody');
    if (!tbody) return;

    const sorted = [...klassen].sort((a, b) => b.avgTussenuren - a.avgTussenuren);

    tbody.innerHTML = sorted.map(k => {
        const cells = [1, 2, 3, 4, 5].map(d => {
            const v = k.avgTussenPerDag[d];
            const cls = v === 0 ? 'good' : v <= 0.3 ? 'warn' : 'bad';
            return `<td class="num"><span class="badge ${cls}">${v}</span></td>`;
        }).join('');
        const avgCls = k.avgTussenuren === 0 ? 'good' : k.avgTussenuren <= 2 ? 'warn' : 'bad';
        return `<tr>
            <td>${k.name}</td>
            <td class="num">${k.studentCount}</td>
            ${cells}
            <td class="num"><span class="badge ${avgCls}">${k.avgTussenuren}</span></td>
            <td class="num">${k.maxTussenuren}</td>
        </tr>`;
    }).join('');

    // Bar chart: avg tussenuren per klas
    const chartData = sorted.filter(k => k.avgTussenuren > 0).slice(0, 15);
    if (state.charts.bvTussenuren) state.charts.bvTussenuren.destroy();
    state.charts.bvTussenuren = new Chart(
        document.getElementById('chart-bv-tussenuren'), {
            type: 'bar',
            data: {
                labels: chartData.map(k => k.name),
                datasets: [{
                    label: 'Gem. tussenuren',
                    data: chartData.map(k => k.avgTussenuren),
                    backgroundColor: COLORS.red,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, title: { display: true, text: 'Gem. tussenuren per leerling' } },
                },
            },
        }
    );
}

function renderBovenbouwDaglengte(klassen) {
    const container = document.getElementById('bv-daglengte-heatmap');
    if (!container) return;

    const sorted = [...klassen].sort((a, b) => a.name.localeCompare(b.name, 'nl', { numeric: true }));

    let html = '<table class="heatmap-table"><thead><tr><th>Klas</th>';
    for (const d of DAYS_LABELS) html += `<th>${d}</th>`;
    html += '</tr></thead><tbody>';

    for (const k of sorted) {
        html += `<tr><td>${k.name}</td>`;
        for (const d of [1, 2, 3, 4, 5]) {
            const val = k.avgLaatsteUur[d];
            let bg = '#f0f3f5';
            let textColor = '#333';
            if (val >= 9) { bg = 'rgba(231, 76, 60, 0.7)'; textColor = 'white'; }
            else if (val >= 8) { bg = 'rgba(230, 126, 34, 0.6)'; textColor = 'white'; }
            else if (val >= 7) { bg = 'rgba(241, 196, 15, 0.5)'; }
            else if (val > 0) { bg = 'rgba(39, 174, 96, 0.3)'; }
            html += `<td style="background:${bg};color:${textColor}">${val || '-'}</td>`;
        }
        html += '</tr>';
    }
    html += '</tbody></table>';
    container.innerHTML = html;
}

function renderBovenbouwBlokuren(blokuren) {
    const tbody = document.querySelector('#bv-blokuren-table tbody');
    if (!tbody) return;

    // Sort: problematic first
    const sorted = [...blokuren].sort((a, b) => {
        if (a.isOk !== b.isOk) return a.isOk ? 1 : -1;
        return a.groep.localeCompare(b.groep, 'nl', { numeric: true });
    });

    if (sorted.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text-light)">Geen blokuren gevonden</td></tr>';
        return;
    }

    tbody.innerHTML = sorted.slice(0, 80).map(r => {
        const statusCls = r.isOk ? 'good' : 'warn';
        const statusText = r.isOk ? 'Normaal' : 'Onwenselijk';
        return `<tr>
            <td>${r.groep}</td>
            <td>${r.vak}</td>
            <td>${DAYS_LABELS[r.day - 1]}</td>
            <td class="num">${r.slots}</td>
            <td class="num"><span class="badge ${statusCls}">${statusText}</span></td>
        </tr>`;
    }).join('');
}

// ================================================================
// RENDERING — ACHTERAF TAB
// ================================================================

function renderAchteraf() {
    const m = state.achterafMetrics;
    if (!m) return;

    // Indicator card
    renderIndicatorCards('ach-indicators', ['S1']);

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

// ================================================================
// RENDERING — INDICATORS & RAPPORTCIJFER
// ================================================================

function renderOverallScore() {
    const section = document.getElementById('overall-score-section');
    if (!section || !state.indicatorsByLocation) return;
    section.classList.remove('hidden');

    // Render score per locatie
    for (const [loc, elId] of [['alle', 'score-sgl'], ['Athena', 'score-athena'], ['Socrates', 'score-socrates']]) {
        const d = state.indicatorsByLocation[loc];
        if (!d) continue;
        const el = document.getElementById(elId);
        if (!el) continue;
        const cls = d.overall >= 7 ? 'score-green' : d.overall >= 4 ? 'score-orange' : 'score-red';
        el.className = 'score-number ' + cls;
        el.textContent = d.overall.toFixed(1);

        // Subtotalen docenten/leerlingen
        const docEl = document.getElementById(elId + '-doc');
        const llEl = document.getElementById(elId + '-ll');
        if (docEl) docEl.textContent = d.subtotals.docenten?.toFixed(1) ?? '-';
        if (llEl) {
            const llScore = d.subtotals.bovenbouw
                ? +((d.subtotals.onderbouw + d.subtotals.bovenbouw) / 2).toFixed(1)
                : d.subtotals.onderbouw ?? null;
            llEl.textContent = llScore?.toFixed(1) ?? '-';
        }
    }

    // Subtitle met indicator telling
    const inds = Object.values(state.indicators);
    const g = inds.filter(i => i.status === 'green').length;
    const o = inds.filter(i => i.status === 'orange').length;
    const r = inds.filter(i => i.status === 'red').length;
    const subtitleEl = document.getElementById('overall-score-subtitle');
    if (subtitleEl) subtitleEl.innerHTML = `${inds.length} indicatoren &mdash; <span class="dot-green"></span>${g} groen &middot; <span class="dot-orange"></span>${o} oranje &middot; <span class="dot-red"></span>${r} rood`;
}

function renderIndicatorCards(containerId, indicatorKeys) {
    const container = document.getElementById(containerId);
    if (!container || !state.indicatorsByLocation) return;

    const locData = state.indicatorsByLocation[state.activeFilter] || state.indicatorsByLocation.alle;
    let html = '';
    for (const key of indicatorKeys) {
        const ind = locData.indicators[key];
        if (!ind) continue;
        const def = KEY_INDICATORS[key];
        const displayVal = def.unit === '%' ? ind.value + '%' : ind.value;

        html += `<div class="indicator-card ${ind.status}">
            <div class="indicator-dot ${ind.status}"></div>
            <div class="indicator-value">${displayVal}</div>
            <div class="indicator-label">${def.label}</div>
        </div>`;
    }
    container.innerHTML = html;
}

function renderTrendChart() {
    const section = document.getElementById('trend-section');
    if (!section) return;

    const stored = JSON.parse(localStorage.getItem('rooster_scores') || '{}');
    const entries = Object.values(stored).sort((a, b) => a.week.localeCompare(b.week));

    if (entries.length < 1) {
        section.classList.add('hidden');
        return;
    }
    section.classList.remove('hidden');

    const labels = entries.map(e => `W${parseInt(e.week.slice(4))}`);
    const currentWeek = document.getElementById('week-select')?.value;

    // Per-locatie lijnen: SGL, Athena, Socrates (elk overall score)
    const datasets = [
        {
            label: 'SGL',
            data: entries.map(e => e.alle?.overall ?? e.overall ?? null),
            borderColor: 'rgba(26, 82, 118, 0.9)',
            backgroundColor: 'rgba(26, 82, 118, 0.1)',
            borderWidth: 3,
            tension: 0.3,
            pointRadius: entries.map(e => e.week === currentWeek ? 7 : 3),
        },
        {
            label: 'Athena',
            data: entries.map(e => e.Athena?.overall ?? null),
            borderColor: 'rgba(41, 128, 185, 0.9)',
            backgroundColor: 'rgba(41, 128, 185, 0.1)',
            borderWidth: 2,
            tension: 0.3,
            pointRadius: entries.map(e => e.week === currentWeek ? 6 : 3),
            spanGaps: false,
        },
        {
            label: 'Socrates',
            data: entries.map(e => e.Socrates?.overall ?? null),
            borderColor: 'rgba(230, 126, 34, 0.9)',
            backgroundColor: 'rgba(230, 126, 34, 0.1)',
            borderWidth: 2,
            tension: 0.3,
            pointRadius: entries.map(e => e.week === currentWeek ? 6 : 3),
            spanGaps: false,
        },
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
                        min: 0, max: 10,
                        title: { display: true, text: 'Rapportcijfer' },
                        ticks: { stepSize: 1 },
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
    document.getElementById('btn-load').addEventListener('click', async () => {
        const weekCode = document.getElementById('week-select').value;
        if (!weekCode) return;

        const btn = document.getElementById('btn-load');
        btn.disabled = true;
        btn.textContent = 'Bezig...';

        try {
            await loadWeekData(weekCode);
        } catch (e) {
            console.error('Load failed:', e);
            alert('Fout bij laden: ' + e.message);
        } finally {
            btn.disabled = false;
            btn.textContent = 'Laden';
            showProgress(false);
        }
    });

    document.getElementById('btn-load-all').addEventListener('click', async () => {
        const btn = document.getElementById('btn-load-all');
        const loadBtn = document.getElementById('btn-load');
        btn.disabled = true;
        loadBtn.disabled = true;
        btn.textContent = 'Bezig...';

        try {
            await loadAllWeeks();
        } catch (e) {
            console.error('Load all failed:', e);
            alert('Fout bij laden: ' + e.message);
        } finally {
            btn.disabled = false;
            loadBtn.disabled = false;
            btn.textContent = 'Alle weken';
            showProgress(false);
        }
    });
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

    // Init sortable tables
    const tableIds = [
        'doc-tussenuren-table', 'doc-lokaal-table', 'doc-pendel-table',
        'll-tussenuren-table', 'll-late-table', 'll-blokuren-table', 'll-spreiding-table',
        'bv-tussenuren-table', 'bv-blokuren-table',
        'ach-docent-table',
    ];
    for (const id of tableIds) initSortableTable(id);

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
