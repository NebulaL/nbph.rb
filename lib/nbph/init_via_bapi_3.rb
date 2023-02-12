# frozen_string_literal: true

require './lib/conf/conf'
require './lib/bapi/bapi'
require './lib/db/connect_db'
require './lib/db/create_table'
require 'oj'
require 'time'

def init_via_bapi(config)
    db = connect_db(config)
    table_nbph = db[:nbph]
    tid = config['spider']['tid']
  
    page = Oj.load(Bapi.get_archive_rank_by_partion(tid, 1, 50))
    page_total = (page['data']['page']['count'] / 50.0).ceil

    page_num = 1
    last_aid_list = []
    last_create = 0
    c_seq = 0

    until page_num >= page_total
        cur_page = Oj.load(Bapi.get_archive_rank_by_partion(tid, page_num, 50))['data']
        cur_page_vids = cur_page['archives']
        vid_list = []
        last_aid_list.clear

        cur_page_vids.each do |video|
            aid = video['aid'].to_i
            next if last_aid_list.include?(aid)
            last_aid_list.push aid
            create = Time.parse video['create']

            if create == last_create
                c_seq += 1
            else
                last_create = create
                c_seq = 0
            end
            vid_list.push(
                { aid: aid, tid: tid, create: create, c_seq: c_seq }
            )
        end

        table_nbph.multi_insert vid_list

        page_total = (cur_page['page']['count'] / 50) + 1
        puts "Page #{page_num} / #{page_total} done."
        page_num += 1
    end
end