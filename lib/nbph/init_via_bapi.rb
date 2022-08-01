# frozen_string_literal: true

require './lib/conf/conf'
require './lib/bapi/bapi'
require './lib/db/connect_db'
require 'oj'
require 'time'
require 'async'

# TODO: logging
def init_via_bapi(config)
  db = connect_db(config)
  table_nbph = db[:nbph]
  tid = config['spider']['tid']

  page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
  page_total = (page['data']['page']['count'] / 50.0).ceil

  page_num = 1
  last_aid_list = []
  last_create_ts = 0
  last_create_ts_offset = 59

  until page_num > page_total
    current_page = Oj.load(Bapi.get_archive_rank_by_partion(tid, page_num, 50))
    aid_list = []
    video_list = []
    current_page['data']['archives'].each do |video|
      aid = video['aid'].to_i
      create = Time.parse video['create']
      next if last_aid_list.include?(aid)

      if create == last_create_ts
        last_create_ts_offset -= 1 if last_create_ts_offset.positive?
      else
        last_create_ts = create
        last_create_ts_offset = 59
      end
      video_list.push(
        { aid: aid, tid: tid, create: create + last_create_ts_offset }
      )
      aid_list.push(aid)
    end
    # video_list.each do |video|
    #   table_nbph.insert(video)
    # end
    table_nbph.multi_insert video_list
    page_total = (current_page['data']['page']['count'] / 50) + 1
    puts "Page #{page_num} / #{page_total} done."
    page_num += 1
  end
end
