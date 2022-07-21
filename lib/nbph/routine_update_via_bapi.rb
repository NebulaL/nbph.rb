# frozen_string_literal: true

require './lib/conf/conf'
require './lib/bapi/bapi'
require './lib/db/connect_db'
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
  if $renv['is_updating']
    return
  else
    $renv['is_updating'] = true
  end

  db = connect_db(config)
  table_nbph = db[:nbph]
  tid = config['spider']['tid']

  last_aids = table_nbph.order(:create).limit(10).map(:aid)

  page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
  page_total = (page['data']['page']['count'] / 50.0).ceil

  page_num = 1
  last_aid_list = []
  last_create_ts = 0
  last_create_ts_offset = 59
  new_video_count = 0
  catch :last_aids_includes_current_aid do
    loop do
      break if page_num <= page_total

      page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))

      loop do
        break if page['data']['archives'].is_a? Array

        sleep 1
        page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
      end
      aid_list = []
      video_list = []
      page['data']['archives'].each do |video|
        aid = video['aid'].to_i
        create = Time.parse(video['create'])

        throw :last_aids_includes_current_aid if last_aids.include? aid

        unless last_aid_list.include? aid
          if create == last_create_ts
            last_create_ts_offset -= 1 if last_create_ts_offset.positive?
          else
            last_create_ts = create
            last_create_ts_offset = 59
          end
          video_list.push({ aid: aid, tid: tid, create: create + last_create_ts_offset })
          aid_list.push(aid)
          new_video_count += 1
          video_list.push(video)
        end
        video_list.each do |video|
          table_nbph.insert(video)
        end
        page_total = (current_page['data']['page']['count'] / 50) + 1
        page_num += 1
      end
    end
  end

  count_db = table_nbph.count

  page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
  count_api = page['data']['page']['count'].to_i
  page_total = (count_api / 50.0).ceil

  invalid_count = count_db - count_api
  if invalid_count.positive?
    page_num = 1
    unsettled_diff_aids = []
    loop do
      break if (page_num <= page_total) && invalid_count.positive?

      page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))

      loop do
        break if page['data']['archives'].is_a? Array

        sleep 1
        page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
      end

      page_aids = page['data']['archives'].collect { |i| i.fetch :aid }

      create_ts_from = Time.parse(page['data']['archives'][0]['create']) + 59 # bigger one
      create_ts_to = Time.parse(page['data']['archives'][-1]['create']) # smaller one

      # get db aids
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
          end
          if create_ts_to <= create && create <= create_ts_to + 59
          # maybe in next page
          else
            table_nbph.where(aid: aid).delete

            if is_aid_valid(aid)
            else
              unsettled_diff_aids.delete(aid)
              invalid_count -= 1
            end
          end
        else
          unsettled_diff_aids.delete(aid)
        end
      end

      diff_aids = db_aids - page_aids
      new_aids = page_aids - db_aids

      next if diff_aids.empty?

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
        elsif create_ts_from - 59 <= create && create <= create_ts_from
          # counted in last page
          next
        elsif is_aid_valid(aid)
        else
          table_nbph.where(aid: aid).delete
          invalid_count -= 1
        end
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
          table_nbph.insert({ aid: aid, tid: tid, create: create })
          break
        end
      end

      page_total = (page['data']['page']['count'] / 50.0).ceil
      page_num += 1
    end
  end
ensure
  $renv['is_updating'] = false
end
