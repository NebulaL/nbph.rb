require "kemal"
require "yaml"
require "db"
require "pg"

# read config
Config = YAML.parse File.read "bcd-config.yml"

# connect db
Db = DB.open("postgres://#{Config["db"]["user"].as_s}:@#{Config["db"]["host"].as_s}:#{Config["db"]["port"].as_i}/#{Config["db"]["db"].as_s}")

get "/api/nbph/pn" do |env|
  aid = env.params.query["aid"]
  tid = env.params.query["tid"] || 30
  ps = env.params.query["ps"] || 50
  create = Db.query_one("SELECT video.create FROM video WHERE aid = $1;", aid, as: Time)
  count = Db.query_one("SELECT count(aid) FROM video WHERE video.tid = $1 AND video.create >= $2;", tid, create, as: Int64)
  (count / ps.to_f).ceil
end

Kemal.run
