# frozen_string_literal: true

require './lib/db/connect_db'

def create_table_nbph(config)
  db = connect_db(config)
  db.create_table :nbph do
    Bignum :aid
    Integer :tid
    Time :create
    Interger :c_seq
  end
end
