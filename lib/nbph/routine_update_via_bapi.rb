# frozen_string_literal: true

require './lib/conf/conf'
require './lib/bapi/bapi'
require './lib/db/connect_db'
require './lib/log/log'
require 'oj'
require 'time'

def is_aid_exist(aid)
  view = Bapi.get_video_morestat(aid)
  if (view['code']).zero?
    True
  else
    False
  end
end

def routine_update_via_bapi(config)
  logger = get_logger('upd')

  logger.info "Now start routine update ,tid=#{config['spider']['tid']}"

  if $renv['is_updating']
    logger.warn 'Lase round has not finished, stop this round'
    return
  else
    $renv['is_updating'] = true
  end

  db = connect_db(config)
  table_nbph = db[:nbph]
  tid = config['spider']['tid']

  logger.info "Now start add new video with tid #{tid}"

  last_aids = table_nbph.reverse(:create).limit(25).map(:aid)
  logger.info "Get last aids: #{last_aids}"

  page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
  page_total = (page['data']['page']['count'] / 50.0).ceil

  page_num = 1
  last_aid_list = []
  last_create_ts = 0
  last_create_ts_offset = 59
  new_video_count = 0

  catch :last_aids_includes_current_aid do
    loop do
      break unless page_num <= page_total

      page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))

      loop do
        break if page['data']['archives'].is_a? Array

        logger.warn "pn=#{page_num}, page['data']['archives'] isn't a Array, re-call after 1s"
        sleep 1
        page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
      end

      aid_list = []
      video_list = []
      page['data']['archives'].each do |video|
        aid = video['aid'].to_i
        create = Time.parse(video['create'])

        if last_aids.include? aid
          logger.info "Meet aid = #{aid} in last_aids, break."
          throw :last_aids_includes_current_aid
        end

        if !last_aid_list.include? aid
          if create == last_create_ts
            last_create_ts_offset -= 1 if last_create_ts_offset.positive?
          else
            last_create_ts = create
            last_create_ts_offset = 59
          end
          unless table_nbph.where(aid: aid)
            video_list.push({ aid: aid, tid: tid,
                              create: create + last_create_ts_offset })
            logger.info "Add new video #{video_list.last}"
          end
          aid_list.push(aid)
          new_video_count += 1
        else
          logger.warn "Aid #{aid} alreday added"
        end
        table_nbph.multi_insert(video_list)

        page_total = (page['data']['page']['count'] / 50) + 1
        page_num += 1
      end
    end
  end

  if new_video_count.zero?
    logger.info "No new video found with #{tid} tid."
  else
    logger.info "#{new_video_count} new video(s) found with #{tid} tid."
  end
  logger.info "Finish add new video with tid #{tid}"

  logger.info "Now start delete invalid video with tid #{tid}"

  db_count = table_nbph.count

  page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
  bapi_count = page['data']['page']['count'].to_i
  page_total = (bapi_count / 50.0).ceil

  logger.info "Get db_count=#{db_count}, bapi_count=#{bapi_count}"

  invalid_count = db_count - bapi_count
  if invalid_count.positive?
    page_num = 1
    unsettled_diff_aids = []
    loop do
      break if (page_num <= page_total) && invalid_count.positive?

      page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))

      loop do
        break if page['data']['archives'].is_a? Array

        logger.warn "pn=#{page_num}, page['data']['archives'] isn't a Array, re-call after 1s"
        sleep 1
        page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
      end

      page_aids = page['data']['archives'].collect { |i| i.fetch :aid }

      create_ts_from = Time.parse(page['data']['archives'][0]['create']) + 59
      create_ts_to = Time.parse(page['data']['archives'][-1]['create'])

      db_videos = table_nbph.where(create: create_ts_from..create_ts_to)
      db_aids = db_videos.map(:aid)
      unsettled_diff_aids.each do |aid|
        if !page_aids.include? aid
          create = -1
          if db_aids.include? aid
            db_videos.each do |v|
              if v.aid == aid
                create = v.create
                break
              end
            end
          else
            logger.warn "Cannot find unsettled diff aid #{aid} in db_aids #{db_aids}"
          end
          if create_ts_to <= create && create <= create_ts_to + 59
            logger.info "Remain aid #{aid} in unsettled list"
          else
            table_nbph.where(aid: aid).delete
            logger.info "Delete unsettled invalid aid #{aid}."

            if is_aid_valid(aid)
              logger.warn "Aid #{aid} is not invalid! Do not remove it."
            else
              unsettled_diff_aids.delete(aid)
              invalid_count -= 1
            end
          end
        else
          unsettled_diff_aids.delete(aid)
          logger.info "Save unsettled aid #{aid}"
        end
      end

      diff_aids = db_aids - page_aids
      new_aids = page_aids - db_aids

      if !diff_aids.empty?

        diff_aids.each do |aid|
          create = -1
          db_videos.each do |v|
            if v.aid == aid
              create = v.create
              break
            end
          end
          if create_ts_to <= create && create <= create_ts_to + 59
            unsettled_diff_aids.push(aid)
            logger.info "Add aid #{aid} to unsettled list"
          elsif create_ts_from - 59 <= create && create <= create_ts_from
            # counted in last page
            next
          else
            logger.info "Delete invalid aid #{aid}"
            if is_aid_valid(aid)
              logger.warn "Aid #{aid} is not invalid, do not remove it"
            else
              table_nbph.where(aid: aid).delete
              invalid_count -= 1
            end
          end
        end
      else
        logger.info "No diff aid"
      end

      last_create_ts = 0
      last_create_ts_offset = 59
      new_aids.each do |aid|
        page['data']['archives'].each do |video|
          next unless video['aid'].to_i == aid

          create = Time.parse video['create']
          create_ts = create_time_to_ts(create)
          if create == last_create_ts
            last_create_ts_offset -= 1 if last_create_ts_offset.positive?
          else
            last_create_ts = create
            last_create_ts_offset = 59
          end
          create_ts += last_create_ts_offset
          video = { aid: aid, tid: tid, create: create + last_create_ts_offset }
          logger.warn "Add ne video #{video} during finding invalid aid"
          table_nbph.insert({ aid: aid, tid: tid, create: create })
          break
        end
      end

      page_total = (page['data']['page']['count'] / 50.0).ceil
      logger.info "Page #{page_num}/#{page_total} done, #{invalid_count} invalid aid left"
      page_num += 1
    end
  else
    logger.info "No invalid video to delete"
  end
  logger.info "Finish delete invalid video with tid #{tid}!"

  logger.info "Finish routine update #{tid} tid."
ensure
  $renv['is_updating'] = false
end
