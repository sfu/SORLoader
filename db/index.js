const environment = process.env.NODE_ENV || 'development'
const dbconfig = require('../knexfile')[environment];    // require environment's settings from knexfile
const knex = require('knex')(dbconfig);
// To allow us to limit the concurrency of async tasks, so we don't overwhelm the DB
const {default: PQueue} = require('p-queue');
const queue = new PQueue({concurrency: 5});

const tablename = 'sorpeople'
const changelogtable = 'changelog'
const uuidtable = 'sorpeople_uuid'
const grouperloadertable = 'grouper_loader_groups'

async function updateSorObject(where, update) {
    return queue.add(async () => {
        let id    
        try {
            await knex.transaction(async (txn) => {
                const olddata = await txn(tablename)
                    .select('userdata')
                    .where(where).first()
                await txn(changelogtable)
                    .insert({
                        sfuid: where.sfuid,
                        source: where.source,
                        olduserdata: olddata.userdata,
                        newuserdata: update.userdata
                    })
                id = await txn(tablename).returning('id').where(where).update(update)
            })
        } catch (error) {
            console.error(error);
            throw error;
        }
        return id;
    })
}

async function getSorObjects(select,where) {
    return queue.add(async () => { 
        return knex(tablename).select(select).where(where)
    })
}

async function addSorObject(user) {
    return queue.add(async () => {
        let id    
        try {
            await knex.transaction(async (txn) => {
                await txn(changelogtable)
                    .insert({
                        sfuid: user.sfuid,
                        source: user.source,
                        olduserdata: '{}',
                        newuserdata: user.userdata
                    })
                id = await txn(tablename).returning('id').insert(user)
            })
        } catch (error) {
            console.error(error);
            throw error;
        }
        return id;
    })
}

async function addChangeLog(record) {
    return queue.add(async () => {
        return knex(changelogtable).returning('id').insert(record)
    })
}

async function getUuid(where) {
    return queue.add(async () => {
        if (typeof where === 'undefined') {
            return knex(uuidtable).select('uuid')
        }
        return knex(uuidtable).select('uuid').where(where)
    })
}

async function addUuid(record) {
    return queue.add(async () => {
        return knex(uuidtable).returning('uuid').insert(record)
    })
}

async function getGrouperLoaderGroups(where) {
    return queue.add(async () => {
        if (typeof where === 'undefined') {
            return knex(grouperloadertable).select(['group','loader'])
        }
        return knex(grouperloadertable).select(['group','loader']).where(where)
    })
}

async function addGrouperLoaderGroup(record) {
    return queue.add(async () => {
        return knex(grouperloadertable).returning('group').insert(record)
    })
}
async function getGrouperView(view,where) {
    return queue.add(async () => {
        if (typeof where === 'undefined') {
            return knex(view).distinct().returning('group_name')
        }
        return knex(view).distinct().returning('group_name').distinct().where(where)
    })
}



module.exports = {
    queue,
    updateSorObject,
    getSorObjects,
    addSorObject,
    addChangeLog,
    getUuid,
    addUuid,
    getGrouperLoaderGroups,
    addGrouperLoaderGroup,
    getGrouperView
}
