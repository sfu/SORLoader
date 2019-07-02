const table = 'sorstudents'

exports.up = (knex) => {
  return knex.schema.createTable(table, (t) => {
    t.comment('SoR records for students')
    t.increments().primary()
    t.text('sfuid').notNull().unique().comment(`SoR SFUID`)
    t.enu('status',['active','inactive','deleted']).comment(`Student''s status in SoR`)
    t.uuid('uuid').unique().comment('An opaque, unique identifier for the user')
    t.text('lastname').notNull().comment(`The user''s last name, as defined in SoR`)
    t.text('firstnames').notNull().comment(`The user''s legal given names, as defined in SoR`)
    t.string('hash').notNull().comment('hash of JSONified student object')
    t.string('source').notNull().comment('SoR Source name')
    t.dateTime('created_at').notNull().defaultTo(knex.raw('now()'))
    t.dateTime('updated_at').notNull().defaultTo(knex.raw('now()'))
    t.jsonb('userdata')
    t.index('sfuid')
    t.index('status')
    t.index('uuid')
    t.index('source')
  }).raw(`CREATE TRIGGER update_${table}_updated_at BEFORE UPDATE ON ${table} FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();`)
}

exports.down = (knex) => {
  return knex.schema.dropTable(table)
}
