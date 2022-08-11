const table = 'grouper_loader_groups'

exports.up = (knex) => {
return knex.schema.createTable(table, (t) => {
    t.comment('GrouperLoader Groups')
    t.increments().primary()
    t.text('group_name').notNull().comment(`Group Name`)
    t.text('loader').notNull().comment('Associated Loader job')
    t.dateTime('created_at').notNull().defaultTo(knex.raw('now()'))
    t.index('group')
})
}

exports.down = (knex) => {
return knex.schema.dropTable(table)
}
  