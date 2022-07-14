# frozen_string_literal: true

require 'pg'
require 'sequel'
require './lib/conf/conf'

DB = Sequel.connect(adapter: :postgres, user: CONFIG['db']['user'], password: CONFIG['db']['password'],
                    host: CONFIG['db']['host'], port: CONFIG['db']['port'], database: CONFIG['db']['dbname'],
                    max_connections: CONFIG['db']['max_connections'] || 10)

