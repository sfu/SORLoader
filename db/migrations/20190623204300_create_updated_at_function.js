const sql =
`CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';
`

exports.up = (knex) => {
  return knex.schema.raw(sql)
}

exports.down = (knex) => {
  return knex.schema.raw('DROP FUNCTION update_updated_at_column()')
}
