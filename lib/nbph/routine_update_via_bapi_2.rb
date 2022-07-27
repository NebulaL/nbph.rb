require "./lib/conf/conf"
require "./lib/bapi/bapi"
require "./lib/db/connect_db"
require "./lib/log/log"
require "oj"
require "time"

def is_aid_exist(aid)
  view = Bapi.get_video_morestat(aid)
  (view["code"]).zero? ? True : False
end

def routine_update_via_bapi(config)
  logger = get_logger("upd")
  db = connect_db(config)
  table_nbph = db[:nbph]
  tid = config["spider"]["tid"]

  logger.info "Now start routine update ,tid=#{tid}"

  if $renv["is_updating"]
    logger.warn "Lase round has not finished, stop this round"
    return
  else
    $renv["is_updating"] = true
  end

  logger.info "Now start add new video with tid #{tid}"
  last_aids = table_nbph.reverse(:create).limit(25).map(%i[aid create]).to_h
  db_latest_create = last_aids.values.first
  logger.info "Get last aids: #{last_aids}"

  page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
  page_total = (page["data"]["page"]["count"] / 50.0).ceil

  page_num = 1
  last_aid_list = []
  last_create_ts = 0
  last_create_ts_offset = 59
  new_video_count = 0
  cur_create = Time.new
  loop do
    break unless page_num <= page_total

    page = Oj.load(Bapi.get_archive_rank_by_partion(tid, page_num, 50))

    loop do
      break if page["data"]["archives"].is_a? Array

      logger.warn "pn=#{page_num}, page['data']['archives'] isn't a Array, re-call after 1s"
      sleep 1
      page = Oj.load(Bapi.get_archive_rank_by_partion(tid, page_num, 50))
    end

    video_list = []
    cur_create = db_latest_create
    page["data"]["archives"].each do |cur_video|
      cur_create = Time.parse cur_video["create"]
      cur_aid = cur_video["aid"].to_i
      if cur_create < db_latest_create
        logger.info("cur_create < db_latest_create, break/1")
        break
      end

      if table_nbph.where(aid: cur_aid).empty?
        video_list.push({ aid: cur_aid, create: cur_create, tid: tid })
        logger.info "Cache new video #{video_list.last}"
      end
    end

    table_nbph.multi_insert video_list
    logger.info "Add new videos #{video_list}"

    if cur_create < db_latest_create
      db_cur_count = table_nbph.where { create < cur_create }.count
      bapi_count = page["data"]["page"]["count"].to_i
      if db_count < bapi_count
        logger.info("db_count < bapi_count (#{db_count} #{bapi_count}), next/2")
        next
      end
      logger.info("cur_create < db_latest_create, break/2")
      break
    end

    page_total = (page["data"]["page"]["count"] / 50) + 1
    page_num += 1
  end

  logger.info "Finish routine update #{tid} tid."
ensure
  $renv["is_updating"] = false
end
