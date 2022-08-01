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
    pages = []
    Async do
      30.times do |i|
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

    video_list = []
    pages.each do |i|
      i['data']['archives'].each do |video|
        aid = video['aid'].to_i
        create = Time.parse video['create']
        video_list.push({ aid: aid, tid: tid, create: create })
      end
    end

    table_nbph.multi_insert video_list
    page_total = (pages.first['data']['page']['count'] / 50) + 1
    page_num += pages.size
    puts "Page #{page_num} / #{page_total} done."
  end
end
