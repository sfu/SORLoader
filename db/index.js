const environment = process.env.NODE_ENV || 'development'
const dbconfig = require('../knexfile')[environment];    // require environment's settings from knexfile
const knex = require('knex')(dbconfig);
// To allow us to limit the concurrency of async tasks, so we don't overwhelm the DB
const {default: PQueue} = require('p-queue');
const queue = new PQueue({concurrency: 5});

const tablename = 'sorpeople'
const changelogtable = 'changelog'

async function updateSorObject(where, update) {
    return queue.add(async () => {
        let id    
        try {
            await knex.transaction(async (txn) => {
                const olddata = await txn(tablename)
                    .select('userdata')
                    .where(where)
                await txn(changelogtable)
                    .insert({
                        sfuid: where.sfuid,
                        source: where.source,
                        olduserdate: olddata,
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
                        olduserdate: '',
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

module.exports = {
    queue,
    updateSorObject,
    getSorObjects,
    addSorObject,
    addChangeLog
}
