# frozen_string_literal: true

# https://github.com/bunnyxt/pybiliapi/blob/master/pybiliapi/BiliApi.py

require './lib/bapi/bvid'
require 'faraday'

class Bapi
  def self.get_video_morestat(vid)
    type = BVID.type(vid).first
    vid = BVID.format vid
    unless type == :unknown
      _url_request('GET',
                   "http://api.bilibili.com/x/web-interface/view?#{type}=#{vid}").body
    end
  end

  def self.get_video_tags(vid)
    type = BVID.type(vid).first
    vid = BVID.format vid
    unless type == :unknown
      _url_request('GET',
                   "http://api.bilibili.com/x/tag/archive/tags?#{type}=#{vid}").body
    end
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

  def self._url_request(methmod, url)
    if methmod == 'GET'
      res = Faraday.get(url)
      res if res.status == 200
    end
  end
end
