# frozen_string_literal: true

require 'pg'
require 'sequel'

def connect_db(config)
  Sequel.connect(
    adapter: :postgres,
    user: config['db']['user'],
    password: config['db']['password'],
    host: config['db']['host'],
    port: config['db']['port'],
    database: config['db']['dbname'],
    max_connections: config['db']['max_connections'] || 10
  )
end
