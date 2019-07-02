const xml2js = require('xml2js')
const pg = require('pg')
const fs = require('fs')
const crypto = require('crypto')
const hash = crypto.createHash
const environment = process.env.NODE_ENV || 'development'
const dbconfig = require('./knexfile')[environment];    // require environment's settings from knexfile
const knex = require('knex')(dbconfig);

// To allow us to limit the concurrency of async tasks, so we don't overwhelm the DB
const {default: PQueue} = require('p-queue');
const queue = new PQueue({concurrency: 5});


// For debugging purposes
var inspect = require('eyes').inspector({maxLength: false})

var importFile = process.argv[2]

var students
var sfuids_present = []
var students_in_db = new Map
var students_loaded = false
var updated_students = 0
var reactivated_students = 0
var inserted_students = 0
var removed_students = 0

var async_processed=0
queue.on('active', () => {
    if (++async_processed % 1000 === 0 || queue.size === 0) {
        console.log(`Working on item ${async_processed}.  Size: ${queue.size}  Pending: ${queue.pending}`);
    }
});

// Load existing active students from the DB
// try {
//     await loadFromDb()
// } catch(err) {
//     console.log(err)
//     throw new Error("Can't continue")
// }

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

fs.readFile(importFile, async function(err, data) {
    // Load existing active students from the DB
    await loadFromDb()
    console.log("Loading XML extract from " + importFile)
    parser.parseString(data, async function (err, extract) {
        if (err !== null) {
            console.log("Error parsing input. Can't continue. " + err);
        }
        else {
            var timestamp = extract.$.Timestamp
            // TODO: Compare timestamp against last processed one in DB and abort if older/same
            console.log("Extract time stamp: " + timestamp)
            students = extract.student
            await processImport()
        }
        await queue.onIdle()
            console.log('Done')
            console.log('Students with updates:      ' + updated_students)
            console.log('Students reactivated:       ' + reactivated_students)
            console.log('New students added:         ' + inserted_students)
            console.log('Students removed from feed: ' + removed_students)
//        console.log(process._getActiveHandles())
//        console.log(process._getActiveRequests())
    });
});

// Clean up some data fields in the student object.
// This primarily just converts certain single-element values that could be arrays 
// into arrays to ensure consistency
//  
function normalize(student) {
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
function genhash(student) {
    student.hash = hash('md5').update(JSON.stringify(student)).digest('base64')
}

/* After the import file has been loaded, process it
 * For each student:
 * - Normalize the JS object produced
 * - Generate a hash based on the JSON version of the object
 * - Save the list of all SFUIDs seen
 * - Update or add changed/new students in DB
 * - Mark, as inactive, students who are no longer in the SoR feed
 */
async function processImport() {
    students.forEach((student) => {
        normalize(student)
        genhash(student)
        sfuids_present.push(student.sfuid)
    })
    console.log("Active students loaded from XML: " + sfuids_present.length)
    
    students.forEach(async (student) => {
        if (!students_in_db.has(student.sfuid) || students_in_db.get(student.sfuid) !== student.hash) {
            await queue.add(() => { updateDbForStudent(student) })
        }
    })
    if (students_in_db.size > 0) {
        for (const sfuid of students_in_db.keys()) {
            //console.log("present: " + sfuids_present.includes(sfuid))
            if (!sfuids_present.includes(sfuid)) {
                // Student is no longer in SoR feed. Set to inactive
                try {
                    var rows = await queue.add(async () => { 
                        return knex('sorstudents')
                        .returning('id')
                        .where({sfuid: sfuid})
                        .update({status:'inactive'})
                    })
                    if (rows.length) {
                            removed_students++
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

async function loadFromDb() {  
    try {
        var rows = await knex('sorstudents')
            .select('sfuid','hash')
            .where({status: 'active'})
        if (rows != null) {
            rows.forEach((row) => {
                students_in_db.set(row.sfuid,row.hash)
            })
            students_loaded = true
            console.log("Active students loaded from DB: " + students_in_db.size)
        }
    } catch(err) {
        console.log("Error loading students from DB. Can't continue!")
        console.log(err)
        throw new Error("Something went badly wrong!");
        process.exit(1)
    }
}

async function updateDbForStudent(student) {
    if (students_in_db.has(student.sfuid)) {
        try {
            // Update an existing active student's record
            rows = await queue.add(async () => {
                return knex('sorstudents')
                .returning('id')
                .where({sfuid: student.sfuid})
                .update({
                    hash: student.hash,
                    lastname: student.lastname,
                    firstnames: student.firstnames,
                    userdata: JSON.stringify(student)
                })
            })
            if (rows.length) {
                updated_students++
                if ((updated_students % 1000) == 0 ) {
                    console.log("Update got here: " + updated_students)
                }
            }
        } catch(err) {
            console.log("Error updating student: " + student.sfuid)
            console.log(err.message)
        }
    }
    else {
        // Check whether the student exists in the DB at all yet
        try {
            var rows = await queue.add(async () => { 
                return knex('sorstudents')
                .where({sfuid: student.sfuid})
            })  
            if ( rows.length > 0) {
                // Student is in the DB but inactive. Update
                try {
                    rows = await queue.add(async () => {
                        return knex('sorstudents')
                        .where({sfuid: student.sfuid})
                        .update({
                            status: 'active',
                            hash: student.hash,
                            lastname: student.lastname,
                            firstnames: student.firstnames,
                            userdata: JSON.stringify(student)
                        })
                    })
                    if (rows.length) {
                        reactivated_students++
                        if ((reactivated_students % 1000) == 0 ) {
                            console.log("Update from inactive got here: " + updated_students)
                        }
                    }
                } catch(err) {
                    console.log("Error updating student: " + student.sfuid)
                    console.log(err.message)
                }
                // TODO: If there are any other actions to kick off when a student re-appears, do it here
            }
            else {
                // Student not in DB. Insert
                try {
                    rows = await queue.add(async () => {
                        return knex('sorstudents')
                        .returning('id')
                        .insert({
                            sfuid: student.sfuid,
                            status: 'active',
                            hash: student.hash,
                            lastname: student.lastname,
                            firstnames: student.firstnames,
                            source: 'SIMS',
                            userdata: JSON.stringify(student)
                        })
                    })
                    if (rows.length > 0) { 
                        inserted_students++
                        if ((inserted_students % 1000) == 0) {
                            console.log("then got here: " + inserted_students)
                        }
                    }
                    
                } catch(err) {
                    console.log("Error inserting student: " + student.sfuid)
                    console.log(err.message)
                }
            }
        } catch(err) {
            console.log("Error searching for student: " + student.sfuid)
            console.log(err.message)   
        }
    } 
}
