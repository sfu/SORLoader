const table = 'changelog'

exports.up = (knex) => {
  return knex.schema.createTable(table, (t) => {
    t.comment('Change Log')
    t.increments().primary()
    t.text('sfuid').notNull().comment(`SoR SFUID`)
    t.string('source').notNull().comment('SoR Source name')
    t.dateTime('created_at').notNull().defaultTo(knex.raw('now()'))
    t.jsonb('olduserdata')
    t.jsonb('newuserdata')
    t.index('sfuid')
    t.index('created_at')
  })
}

exports.down = (knex) => {
  return knex.schema.dropTable(table)
}
