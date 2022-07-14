# https://github.com/bunnyxt/pybiliapi/blob/master/pybiliapi/BiliApi.py

require "lib/bvid"
require 'faraday'

class Bapi
  def self.get_video_view(vid)
    _url_request('GET', "http://api.bilibili.com/x/web-interface/view?#{BVID.bvid?(vid)?"bvid=#{BVID.format(vid,:bvid)}":"aid=#{BVID.format(vid,:avid)}"}").body
  end

  def self.get_video_tags(vid)
    _url_request('GET', "http://api.bilibili.com/x/tag/archive/tags?#{BVID.bvid?(vid)?"bvid=#{BVID.format(vid,:bvid)}":"aid=#{BVID.format(vid,:avid)}"}").body
  end

  def self.get_video_pagelist(aid)
    _url_request('GET', "http://api.bilibili.com/x/player/pagelist?aid=#{aid}").body
  end

  def self.get_video_stat(aid)
    _url_request('GET', "http://api.bilibili.com/x/web-interface/archive/stat?aid=#{aid}").body
  end

  def self.get_member(mid)
    _url_request('GET', "http://api.bilibili.com/x/space/acc/info?mid=#{mid}").body
  end

  def self.get_member_relation(mid)
    _url_request('GET', "http://api.bilibili.com/x/relation/stat?vmid=#{mid}").body
  end

  def self.get_archive_rank_by_partion(tid, pn, ps)
    _url_request('GET', "http://api.bilibili.com/archive_rank/getarchiverankbypartion?jsonp=jsonp&tid=#{tid}&pn=#{pn}&ps=#{ps}").body
  end

  private

  def self._url_request(methmod, url)
    if methmod == 'GET'
        res = Faraday.get(url)
        if res.status = 200
            return res
        end
    end
  end
end
