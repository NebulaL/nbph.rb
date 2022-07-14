require './lib/db/connect_db.rb'

DB.create_table :nbph do
    primary_key :aid
    Integer :tid
    Time :create
end