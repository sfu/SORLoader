const environment = process.env.NODE_ENV || 'development'
const dbconfig = require('../knexfile')[environment];    // require environment's settings from knexfile
const knex = require('knex')(dbconfig);
// To allow us to limit the concurrency of async tasks, so we don't overwhelm the DB
const {default: PQueue} = require('p-queue');
const queue = new PQueue({concurrency: 5});

const tablename = 'sorstudents'

async function updateSorObject(where, update) {
    return await queue.add(async () => { 
        return knex(tablename).returning('id').where(where).update(update)
    })
}

async function getSorObjects(select,where) {
    return await queue.add(async () => { 
        return knex(tablename).select(select).where(where)
    })
}

async function addSorObject(user) {
    return await queue.add(async () => {
        return knex(tablename).returning('id').insert(user)
    })
}

module.exports = {
    queue,
    updateSorObject,
    getSorObjects,
    addSorObject
}