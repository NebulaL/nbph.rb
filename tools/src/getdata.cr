require "http/client"
require "json"
require "yaml"
# require "db"
# require "pg"
require "dexter"

# require "ansi-escapes"

def get_archive_rank_by_partion(tid, pn, ps)
  HTTP::Client.get("http://api.bilibili.com/archive_rank/getarchiverankbypartion?jsonp=jsonp&tid=#{tid}&pn=#{pn}&ps=#{ps}").body
end

dir = "dat/active"
Dir.mkdir_p(dir)

# setup log
logfile = File.open("#{dir}/log.json", "w")
backend = Log::IOBackend.new(io: logfile)
backend.formatter = Dexter::JSONLogFormatter.proc
Log.dexter.configure(:info, backend)

# read config
# Config = YAML.parse File.read "bcd-config.yml"

# connect db
# Db = DB.open("postgres://#{Config["db"]["user"].as_s}:@#{Config["db"]["host"].as_s}:#{Config["db"]["port"].as_i}/#{Config["db"]["db"].as_s}")

# dbcnt = Db.query_one("SELECT count(aid) FROM videos",as: Int64)

# get page_count
response = get_archive_rank_by_partion(30, 1, 1)
rres = JSON.parse(response)
page_count = rres["data"]["page"]["count"].as_i64
Log.dexter.info { {page_count: page_count, info: "page_count"} }

target = (page_count/50).to_i
tasks_per_task = ARGV[0].to_i
target += (tasks_per_task - (target % tasks_per_task))
tasks = (target / tasks_per_task).to_i
Log.dexter.info { {fibernum: tasks, info: "dl_fibers"} }

info = {page_count: page_count, target: target, tasks_per_task: tasks_per_task, tasks: tasks, time: Time.local.to_s("%Y-%m-%d %H:%M:%S %:z")}

channel = Channel(Int64).new

tasks.times do |x|
  spawn name: "dl#{x}" do
    Log.dexter.info { {fiberid: x.to_i64, info: "spawn_dl_fiber"} }
    tasks_per_task.times do |i|
      pn = i + (x * tasks_per_task) + 1
      Log.dexter.info { {fiberid: x.to_i64, pn: pn.to_i64, info: "dl_page_start"} }
      res = get_archive_rank_by_partion(30, pn, 50)
      unless res[8] == '0'
        Log.dexter.warn { {fiberid: x.to_i64, pn: pn.to_i64, code: res[8].to_s, info: "dl_page_code_neq_0_recall"} }
        while !res[8] == '0'
          sleep 0.001
          res = get_archive_rank_by_partion(30, pn, 50)
        end
      end
      File.write "#{dir}/#{pn}.json", res
      Log.dexter.info { {fiberid: x.to_i64, pn: pn.to_i64, code: res[8].to_s, info: "dl_page_end"} }
      channel.send(pn)
      Fiber.yield
    end
    Log.dexter.info { {fiberid: x.to_i64, info: "dl_fiber_end"} }
    Fiber.yield
  end
end

crl = [] of Int64

while crl.size < target
  unless crl.size < target
    break
  end
  crl << channel.receive
  unless crl.size < target
    break
  end
  puts "#{crl.size}\t/\t#{target}\n#{((crl.size.to_f*100/target.to_f)*100).to_i/100.0}\t%#{crl.size < target}"
  Fiber.yield
end

File.write "#{dir}/info.yml", info.to_yaml

system "notify-send end"
