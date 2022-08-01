# frozen_string_literal: true

require './lib/conf/conf'
require './lib/bapi/bapi'
require './lib/db/connect_db'
require './lib/log/log'
require 'oj'
require 'time'
require 'async'

def aid_exist?(aid)
  Async do
    view = Bapi.get_video_morestat(aid)
  end
  view['code'].to_i.zero? ? true : false
end

def add_new_videos_via_bapi(config)
  logger = get_logger('upd#add_new_videos_via_bapi')
  db = connect_db(config)
  table_nbph = db[:nbph]
  tid = config['spider']['tid']

  logger.info "Now start add new videos with tid #{tid}"
  last_aids = table_nbph.reverse(:create).limit(25).map(%i[aid create]).to_h
  db_latest_create = last_aids.values.first
  logger.info "Get last aids: #{last_aids}"

  page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
  page_total = (page['data']['page']['count'] / 50.0).ceil

  page_num = 1
  new_video_count = 0
  cur_create = Time.new
  k = 0
  while page_num <= page_total
    page = Oj.load(Bapi.get_archive_rank_by_partion(tid, page_num, 50))

    until page['data']['archives'].is_a? Array
      logger.warn "pn=#{page_num}, page['data']['archives'] isn't a Array, re-call after 1s"
      sleep 1
      page = Oj.load(Bapi.get_archive_rank_by_partion(tid, page_num, 50))
    end

    video_list = []
    cur_create = db_latest_create
    page['data']['archives'].each do |cur_video|
      cur_create = Time.parse cur_video['create']
      cur_aid = cur_video['aid'].to_i
      if cur_create < db_latest_create
        logger.info('cur_create < db_latest_create, break/1')
        break
      end

      if table_nbph.where(aid: cur_aid).empty?
        video_list.push({ aid: cur_aid, create: cur_create, tid: tid })
        logger.info "Cache new video #{video_list.last}"
      end
    end

    table_nbph.multi_insert video_list
    logger.info "Add new videos #{video_list}"
    new_video_count += video_list.size

    if cur_create < db_latest_create
      db_count = table_nbph.count
      bapi_count = page['data']['page']['count'].to_i
      # db_cur_count = table_nbph.where { create < cur_create }.count
      # bapi_cur_count = bapi_count - (page['data']['page']['num'].to_i * 50)
      if db_count < bapi_count && k < 10
        logger.info("db_count < bapi_count (#{db_count} #{bapi_count}), next/2")
        k += 1
        next
      end
      logger.info('cur_create < db_latest_create, break/2')
      break
    end

    # page_total = (page['data']['page']['count'] / 50) + 1
    page_num += 1
  end

  logger.info "Finish add new videos via bapi #{tid} tid."
end

def delete_invalid_videos_via_bapi(config)
  logger = get_logger('upd#delete_invalid_videos_via_bapi')
  db = connect_db(config)
  table_nbph = db[:nbph]
  tid = config['spider']['tid']

  logger.info "Now start delete invalid video with tid #{tid}"

  db_count = table_nbph.count

  page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
  bapi_count = page['data']['page']['count'].to_i
  page_total = (bapi_count / 50.0).ceil

  logger.info "Get db_count=#{db_count}, bapi_count=#{bapi_count}"

  invalid_count = db_count - bapi_count
  if invalid_count.positive?
    page_num = 1
    loop do
      break if (page_num >= page_total) || !invalid_count.positive?

      pages = []
      Async do
        50.times do |i|
          Async do
            page =
              Oj.load(Bapi.get_archive_rank_by_partion(tid, page_num + i, 50))

            loop do
              break if page['data']['archives'].is_a? Array

              logger.warn "pn=#{page_num}, page['data']['archives'] isn't a Array, re-call after 1s"
              sleep 1
              page =
                Oj.load(Bapi.get_archive_rank_by_partion(tid, page_num + i, 50))
            end
            pages[i] = page
          end
        end
      end
      pages_aids = []
      pages.each do |i|
        i['data']['archives'].each { |j| pages_aids.push(j.fetch('aid')) }
      end
      # logger.info "Get pages_aids #{pages_aids}"

      create_ts_from =
        Time.parse(pages[0]['data']['archives'][0]['create']) + 59
      create_ts_to = Time.parse(pages[-1]['data']['archives'][-1]['create'])
      logger.info "Get create_ts_from = #{create_ts_from}, create_ts_to = #{create_ts_to}"

      db_aids =
        table_nbph
        .where { Sequel.&(create <= create_ts_from, create >= create_ts_to) }
        .map(:aid)

      invalid_aids = db_aids - pages_aids
      new_aids = pages_aids - db_aids
      Async do
        invalid_aids.delete_if do |i|
          aid_exist?(i)
        end
      end
      logger.info "Get invalid_aids = #{invalid_aids}"
      logger.info "Get new_aids = #{new_aids}"

      table_nbph.where(aid: invalid_aids).delete

      invalid_count -= invalid_aids.size
      logger.info "Get invalid_count = #{invalid_count}"

      page_num += pages.size
    end
  else
    logger.info 'No invalid video to delete'
  end
  logger.info "Finish delete invalid video with tid #{tid}!"
end
