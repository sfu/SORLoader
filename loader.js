const xml2js = require('xml2js')
const pg = require('pg')
const fs = require('fs')
const crypto = require('crypto')
const hash = crypto.createHash
const db = require('./db')
const { type } = require('os')
const knexfile = require('./knexfile')

// For debugging purposes
var inspect = require('eyes').inspector({maxLength: false})

var importFile = process.argv[2]

var students
var depts
var courses
var uuids
var employees = new Map
var instructors = new Map
var employees_in_db
var instructors_in_db
var students_in_db
var uuids_in_db
var sfuids_present = []
var updates = {
                updated: 0,
                reactivated: 0,
                inserted: 0,
                removed: 0,
                groupsadded: 0
            }

// Load the Grouper_Loader_Groups config from json file
// The file must contain an array of JSON objects, each specified as such:
// { view: "grouper_academic_plans_v",
//   loader: "plans",
//   hasSemester: false
// } 
// The 'hasTerm' property is optional. If present and true, it indicates that a 'semester' should be 
// included in the WHERE clause to limit the results to the current semester
var rawdata = fs.readFileSync('grouper-loader.json');
var grouperLoaders = JSON.parse(rawdata);
console.log('Loaded '+ grouperLoaders.length + ' GrouperLoader definitions')

// Suck in the import file
var stripPrefix = xml2js.processors.stripPrefix
var parser = new xml2js.Parser( { 
                explicitRoot: false, 
                trim: true, 
                explicitArray: false,
                // Force tags to lowercase
                normalizeTags: true, 
                attrNameProcessors: [stripPrefix], 
                tagNameProcessors: [stripPrefix] 
            })

fs.readFile(importFile, 'utf8', async function(err, data) {
    if (data.startsWith('uuid')) {
        // Process UUID import
        uuids_in_db = await loadUuids();
        uuids = data.toString().split('\n')
        await processUuidImport();
        await db.queue.onIdle()    
        console.log('New Users added:         ' + updates.inserted)

        // at the end of the UUID import, also process the GrouperLoader groups.
        groups_in_db = await loadGrouperLoaderGroups();
        await processGrouperLoaderGroups();
        console.log('New LoaderGroups added:  ' + updates.groupsadded)
    }
    else if (data.startsWith('DEPT')) {
        // process DEPT import file
    }
    else {
        // process XML file import
        console.log("Loading XML extract from " + importFile)
        parser.parseString(data.replace(/&/g,'&amp;'), async function (err, extract) {
            if (err !== null) {
                console.log("Error parsing input. Can't continue. " + err);
            }
            else {
                var timestamp = extract.$.Timestamp
                // TODO: Compare timestamp against last processed one in DB and abort if older/same
                console.log("Extract time stamp: " + timestamp)
                if (extract.hasOwnProperty('student')) {
                    students = extract.student
                    students_in_db = await loadFromDb('SIMS')
                    await processStudentImport()
                }
                else if (extract.hasOwnProperty('department')) {
                    depts = extract.department
                    employees_in_db = await loadFromDb('HAP')
                    await processEmployeeImport()
                }
                else if (extract.hasOwnProperty('course')) {
                    courses = extract.course
                    //inspect(extract.course)
                    instructors_in_db = await (loadFromDb('SIMSINSTRUCT'))
                    await processInstructorImport()
                }
                else {
                    console.log("Extracted data unrecognized")
                }
            }
            await db.queue.onIdle()
            // There has to be a better way, but the last DB action counter doesn't get updated until after we get here, so one of these counters may be off by one
            console.log('Done')
            console.log('Users with updates:      ' + updates.updated)
            console.log('Users reactivated:       ' + updates.reactivated)
            console.log('New Users added:         ' + updates.inserted)
            console.log('Users removed from feed: ' + updates.removed)
        });
        
    }
});

// Clean up some data fields in the student object.
// This primarily just converts certain single-element values that could be arrays 
// into arrays to ensure consistency
//  
function normalizeStudent(student) {
    if ( typeof student.reginfo === 'undefined' ) { return }
    if ( typeof student.reginfo.affiliation !== 'undefined' && !Array.isArray(student.reginfo.affiliation)) {
        student.reginfo.affiliation = [student.reginfo.affiliation]
    }
    if (typeof student.reginfo.program !== 'undefined' && !Array.isArray(student.reginfo.program)) {
        student.reginfo.program = [student.reginfo.program]
    }
    if (typeof student.reginfo.course !== 'undefined' && !Array.isArray(student.reginfo.course)) {
        student.reginfo.course = [student.reginfo.course]
    }
}

// Generate an MD5 hash of the JSONified student object and store it in the object
function genhash(person) {
    person.hash = hash('md5').update(JSON.stringify(person)).digest('base64')
}

/* After the import file has been loaded, process it
 * For each student:
 * - Normalize the JS object produced
 * - Generate a hash based on the JSON version of the object
 * - Save the list of all SFUIDs seen
 * - Update or add changed/new students in DB
 * - Mark, as inactive, students who are no longer in the SoR feed
 */
async function processStudentImport() {
    students.forEach((student) => {
        normalizeStudent(student)
        genhash(student)
        sfuids_present.push(student.sfuid)
    })
    console.log("Active students loaded from XML: " + sfuids_present.length)
    
    students.forEach(async (student) => {
        let isUpdate = students_in_db.has(student.sfuid)
        if (!isUpdate || students_in_db.get(student.sfuid) !== student.hash) {
            await updateDbForPerson(student, 'SIMS', isUpdate)
        }
    })
    if (students_in_db.size > 0) {
        for (const sfuid of students_in_db.keys()) {
            //console.log("present: " + sfuids_present.includes(sfuid))
            if (!sfuids_present.includes(sfuid)) {
                // Student is no longer in SoR feed. Set to inactive
                try {
                    var rows = await db.updateSorObject({sfuid: sfuid, source: 'SIMS'},{status:'inactive'})        
                    if (rows.length) {
                            updates.removed++
                            console.log(sfuid + " removed from REG feed. Setting to inactive")
                    }
                    else { console.log("what the..?")}
                } catch(err) {
                    console.log("Error updating status for student: " + sfuid)
                    console.log(err)
                }
            }
        }
    }
}

// Process the Employee import data. The import comes to us as an array of departments,
// each with an array of employees. We need an array of employees each with an array of
// jobs/departments. Do the conversion and then compare the resulting employee 
// objects against the DB

async function processEmployeeImport() {
    let persons;
    depts.forEach((dept) => {
        let empls = dept.employees
        let role = 'employee'
        if (empls.hasOwnProperty('applicant')) {
            persons = empls.applicant
            role = 'applicant'
        }
        else if (empls.hasOwnProperty('emp')) {
            persons = empls.emp
        }
        else {
            console.log("Unrecognized department employee type")
            inspect(empls)
            return
        }
        if (!Array.isArray(persons)) {
            persons = [persons]
        }
        persons.forEach((person) => {
            //console.log("Processing " + person.sfuid)
            // According to Amaint, these are the documented Status flags an employee could have in a job:
            //  A - Active?
            //  L - ? treat as active
            //  P - ? treat as active
            //  W - ? treat as active
            //  U - ? treat as active
            //  Q - Retired
            //  R - Retired
            //  T - Terminated?
            //
            // We will default to status == inactive but set to active if any job isn't in the 'T' state
            person.role = role
            person.status = 'inactive'
            if (typeof person.job !== 'undefined') {
                if (!Array.isArray(person.job)) {
                    person.job = [person.job]
                }
                person.job.forEach((job) => {
                    if ( typeof dept.deptcode !== 'undefined') {
                        job.deptcode = dept.deptcode
                    }
                    if ( typeof dept.deptname !== 'undefined') {
                        job.deptname = dept.deptname
                    }
                    if (job.status !== 'T') {
                        person.status = 'active'
                    }
                })
            }
            if (!employees.has(person.sfuid)) {
                employees.set(person.sfuid,person)
            }
            else {
                //console.log("Adding job to " + person.sfuid)
                let newperson = employees.get(person.sfuid)
                newperson.job.push(...person.job)
                if (newperson.status !== 'active') {
                    newperson.status = person.status
                }
                employees.set(person.sfuid,newperson)
                //inspect(newperson)
            }
        })
    })
    employees.forEach(async (person) => {
        genhash(person)
        let isUpdate = employees_in_db.has(person.sfuid)
        if (!isUpdate || employees_in_db.get(person.sfuid) !== person.hash) {
            await updateDbForPerson(person, 'HAP', isUpdate)
        }
    })

    if (employees_in_db.size > 0) {
        for (const sfuid of employees_in_db.keys()) {
            //console.log("present: " + sfuids_present.includes(sfuid))
            if (!employees.has(sfuid)) {
                // Employee is no longer in SoR feed. Set to inactive
                try {
                    var rows = await db.updateSorObject({sfuid: sfuid, source: 'HAP'},{status:'inactive'})        
                    if (rows.length) {
                            updates.removed++
                            console.log(sfuid + " removed from HAP feed. Setting to inactive")
                    }
                    else { console.log("what the..?")}
                } catch(err) {
                    console.log("Error updating status for employee: " + sfuid)
                    console.log(err)
                }
            }
        }
    }
}

// Process the Instructors import data. The import comes to us as an array of courses,
// each with an array of sections with an array of instructors. 
// We need an array of instructors each with an array of sections. 
// Do the conversion and then compare the resulting instructor objects against the DB
async function processInstructorImport() {
    courses.forEach((course) => {
        if ( typeof course.classsections !== 'undefined' 
          && typeof course.classsections.associated !== 'undefined') {
            let assocs = course.classsections.associated
            if (!Array.isArray(assocs)) {
                assocs = [assocs]
            }
            assocs.forEach((assoc) => {
                let sections = assoc.component
                if (typeof sections === 'undefined') {
                    // placeholder course - no defined sections or instructors yet
                    return
                }
                if (!Array.isArray(sections)) {
                    sections = [sections]
                }
                sections.forEach((section) => {
                    if (typeof section.instructor !== 'undefined') {
                        let instructs = section.instructor
                        if (!Array.isArray(instructs)) {
                            instructs = [instructs]
                        }
                        instructs.forEach((instruct) => {
                            // Sanity check on the SFUID field
                            if (instruct.id.length < 5 || instruct.id < 9999) {
                                return
                            }
                            let instructor = {}
                            instructor.sfuid = instruct.id
                            instructor.sections = 
                                [
                                    {
                                        term:     course.term,
                                        name:     course.crsename,
                                        num:      course.crsenum,
                                        title:    course.crsetitle,
                                        code:     section.$.code,
                                        section:  section.sect,
                                        type:     section.classtype,
                                        status:   section.classstat,
                                        rolecode: instruct.rolecode
                                    }
                                ]
                            if (instructors.has(instructor.sfuid)) {
                                let newinstructor = instructors.get(instructor.sfuid)
                                newinstructor.sections.push(...instructor.sections)
                                instructors.set(instructor.sfuid,newinstructor)
                            }
                            else {
                                instructors.set(instructor.sfuid,instructor)
                            }
                        })
                    }
                })
            })
        } 
    })
    instructors.forEach(async (person) => {
        genhash(person)
        let isUpdate = instructors_in_db.has(person.sfuid)
        if (!isUpdate || instructors_in_db.get(person.sfuid) !== person.hash) {
            await updateDbForPerson(person, 'SIMSINSTRUCT', isUpdate)
        }
    })

    if (instructors_in_db.size > 0) {
        for (const sfuid of instructors_in_db.keys()) {
            //console.log("present: " + sfuids_present.includes(sfuid))
            if (!instructors.has(sfuid)) {
                // Employee is no longer in SoR feed. Set to inactive
                try {
                    var rows = await db.updateSorObject({sfuid: sfuid, source: 'SIMSINSTRUCT'},{status:'inactive'})        
                    if (rows.length) {
                            updates.removed++
                            console.log(sfuid + " removed from Instructor feed. Setting to inactive")
                    }
                    else { console.log("what the..?")}
                } catch(err) {
                    console.log("Error updating status for employee: " + sfuid)
                    console.log(err)
                }
            }
        }
    }
}

async function processUuidImport() {
    uuids.filter(v => ! v.includes('external_idNo')).forEach(async (line) => {
        let fields = line.split(/\s+/,2)
        try {
            if (typeof fields === 'undefined' || fields[0] == null || fields[0].length == 0 || fields[1] == null || fields[1].length == 0 ) {
                console.log("Skipping null entry: " + line);
            }
            // Check if a record already exists
            else if (! uuids_in_db.includes(fields[0])) {
                // Nope. Add one
                await db.addUuid({uuid: fields[0], sfuid: fields[1]});
                updates.inserted++;
            }
        } catch(err) {
            console.log("Error processing UUID entry for " + line);
            console.log(err);
        }
    });
}

// For each object in the Grouper_loader.json file, do a DB query to get the
// current list of groups in that loader's view. Add any missing groups to the grouper_loader_groups table
// { view: "grouper_academic_plans_v",
//   loader: "plans",
//   hasSemester: false
// } 
async function processGrouperLoaderGroups() {
    for (const job of grouperLoaders)  {
        viewgroups = new Array
        var tmpgroups
        if (job.hasSemester) {
            tmpgroups = await db.getGrouperView(job.view,{semester: currentTermCode()})
            viewgroups.push(...tmpgroups)
            tmpgroups = await db.getGrouperView(job.view,{semester: nextTermCode()})
            viewgroups.push(...tmpgroups)       
        } else {
            tmpgroups = await db.getGrouperView(job.view)
            viewgroups.push(...tmpgroups) 
        }
        viewgroups.forEach((vgroup) => {
            if (!groups_in_db.has(job.loader)) {
                groups_in_db.set(job.loader, new Array)
            }
            if (!groups_in_db.get(job.loader).includes(vgroup)) {
                await db.addGrouperLoaderGroup({group: vgroup, loader: job.loader})
                updates.groupsadded++
            }
        })
    }

}

// Load the contents of the grouper_loader table into a hash of arrays (one array per loader view)
async function loadGrouperLoaderGroups() {
    let groups_in_db = new Map
    try {
        var rows = await db.getGrouperLoaderGroups();
        if (rows != null) {
            rows.forEach((row) => {
                if (!groups_in_db.has(row.loader)) {
                    var groupArray = new Array
                    groups_in_db.set(row.loader,groupArray)
                }
                groups_in_db.get(row.loader).push(row.group)
            })
        }
    } catch(err) {
        console.log("Error loading LoaderGroups from DB")
        console.log(err)
        throw new Error("Something went badly wrong!");
    }
    return groups_in_db
}

async function loadFromDb(source) {  
    var users_in_db = new Map
    try {
        var rows = await db.getSorObjects(['sfuid','hash'],{status:'active',source: source})
        if (rows != null) {
            rows.forEach((row) => {
                users_in_db.set(row.sfuid,row.hash)
            })
            console.log("Active users loaded from DB for " + source + ": " + users_in_db.size)
        }
    } catch(err) {
        console.log("Error loading users from DB. Can't continue!")
        console.log(err)
        throw new Error("Something went badly wrong!");
    }
    return users_in_db;
}

async function loadUuids() {
    let users_in_db
    try {
        var rows = await db.getUuid();
        if (rows != null) {
            users_in_db = Array.from(rows, row => row.uuid)
        }
    } catch(err) {
        console.log("Error loading UUIDs from DB")
        console.log(err)
        throw new Error("Something went badly wrong!");
    }
    return users_in_db
}

async function updateDbForPerson(person,source,isUpdate) {
    var rows
    var update_type = "updated"
    try {
        if (isUpdate) {
            // Update an existing active person's record
            rows = await db.updateSorObject(
                            {sfuid: person.sfuid, source: source},
                            {
                                hash: person.hash,
                                lastname: person.lastname,
                                firstnames: person.firstnames,
                                userdata: JSON.stringify(person)
                            })
            console.log("Updated '" + source + "' record for: " + person.sfuid)
        }
        else {
            // Check whether the person exists in the DB at all yet
            rows = await db.getSorObjects('id',{sfuid: person.sfuid, source: source})  
            if ( rows.length > 0) {
                update_type = "reactivated"
                // Person is in the DB but inactive. Update
                    rows = await db.updateSorObject({sfuid: person.sfuid, source: source},{
                                    status: 'active',
                                    hash: person.hash,
                                    lastname: person.lastname,
                                    firstnames: person.firstnames,
                                    userdata: JSON.stringify(person)
                                })
                // TODO: If there are any other actions to kick off when a person re-appears, do it here
                console.log("Reactivated '" + source + "' record for: " + person.sfuid)
            }
            else {
                // Person not in DB. Insert
                update_type = "inserted"
                rows = await db.addSorObject({
                        sfuid: person.sfuid,
                        status: 'active',
                        hash: person.hash,
                        lastname: person.lastname,
                        firstnames: person.firstnames,
                        source: source,
                        userdata: JSON.stringify(person)
                    })
                console.log("Added '" + source + "' record for: " + person.sfuid)
            }
        }
        if (rows.length > 0) {
            updates[update_type]++
            //console.log(update_type + " = " + updates[update_type])
            if ((updates[update_type] % 1000) == 0) {
                console.log(update_type + " " + updates[update_type] + " users")
            }
        }
    } catch(err) {
        console.log("Error processing user: " + person.sfuid)
        console.log(err.message)   
    }
}

var currentTermCode = function() {
    var date = new Date();
    var month = date.getMonth();
    var centuryDigit = '1'; // I'll be long-dead before this is an issue
    var yearDigits = date.getFullYear().toString().substr(-2);
    var termDigit = month < 4 ? '1' : month >= 8 ? '7' : '4'

    return centuryDigit + yearDigits + termDigit;
};

var nextTermCode = function() {
    var curterm = currentTermCode();
    var offset = curterm.charAt(3) === '7' ? 4 : 3;
    var nextTerm = parseInt(curterm)+offset;
    return nextTerm.toString();
};
