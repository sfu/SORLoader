const xml2js = require('xml2js')
const pg = require('pg')
const fs = require('fs')
const crypto = require('crypto')
const hash = crypto.createHash
const db = require('./db')

// For debugging purposes
var inspect = require('eyes').inspector({maxLength: false})

var importFile = process.argv[2]

var students
var sfuids_present = []
var students_in_db = new Map
var students_loaded = false
var updates = {
                updated: 0,
                reactivated: 0,
                inserted: 0,
                removed: 0
            }

var async_processed=0
db.queue.on('active', () => {
    if (++async_processed % 1000 === 0 || db.queue.size === 0) {
        console.log(`Working on item ${async_processed}.  Size: ${db.queue.size}  Pending: ${db.queue.pending}`);
    }
});

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
    console.log("Updates: " + inspect(updates))
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
        await db.queue.onIdle()
            console.log('Done')
            console.log('Students with updates:      ' + updates.updated)
            console.log('Students reactivated:       ' + updates.reactivated)
            console.log('New students added:         ' + updates.inserted)
            console.log('Students removed from feed: ' + updates.removed)
            console.log(inspect(updates))
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
            await updateDbForStudent(student)
        }
    })
    if (students_in_db.size > 0) {
        for (const sfuid of students_in_db.keys()) {
            //console.log("present: " + sfuids_present.includes(sfuid))
            if (!sfuids_present.includes(sfuid)) {
                // Student is no longer in SoR feed. Set to inactive
                try {
                    var rows = await db.updateSorObject({sfuid: sfuid},{status:'inactive'})        
                    if (rows.length) {
                            updates.removed++
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
        var rows = await db.getSorObjects(['sfuid','hash'],{status:'active'})
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
    var rows
    var update_type = "updated"
    try {
        if (students_in_db.has(student.sfuid)) {
            // Update an existing active student's record
            rows = await db.updateSorObject(
                            {sfuid: student.sfuid},
                            {
                                hash: student.hash,
                                lastname: student.lastname,
                                firstnames: student.firstnames,
                                userdata: JSON.stringify(student)
                            })
        }
        else {
            // Check whether the student exists in the DB at all yet
            rows = await db.getSorObjects('id',{sfuid: student.sfuid})  
            if ( rows.length > 0) {
                update_type = "reactivated"
                // Student is in the DB but inactive. Update
                    rows = await db.updateSorObject({sfuid: student.sfuid},{
                                    status: 'active',
                                    hash: student.hash,
                                    lastname: student.lastname,
                                    firstnames: student.firstnames,
                                    userdata: JSON.stringify(student)
                                })
                // TODO: If there are any other actions to kick off when a student re-appears, do it here
            }
            else {
                // Student not in DB. Insert
                update_type = "inserted"
                rows = await db.addSorObject({
                        sfuid: student.sfuid,
                        status: 'active',
                        hash: student.hash,
                        lastname: student.lastname,
                        firstnames: student.firstnames,
                        source: 'SIMS',
                        userdata: JSON.stringify(student)
                    })
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
        console.log("Error processing user: " + student.sfuid)
        console.log(err.message)   
    }
}
