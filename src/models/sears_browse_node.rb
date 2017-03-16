# encoding: utf-8
class SearsBrowseNode < ActiveRecord::Base
  acts_as_browse_node

  # :page
  # :path
  # :categories
  # :category
  def search opts={}
    crawl_options = opts.merge(:bn => self[:id])

    cj = CrawlJob.new(:input => crawl_options)
    cj.type = 'sears search'
    cj.save
    puts cj.inspect
  end

  def self.create_from_crawl node, path
    a_len, c_len = path.length, (node[:children] || []).length
    type = if a_len == 0 && c_len == 0
             'bud'
           elsif a_len == 0
             'root'
           elsif c_len == 0
             'leaf'
           else
             'branch'
           end
    ids = []
    names = []
    ans = path
    ids << ans.map{|a|a[:id]}.join("|")
    names << ans.map{|a|a[:name]}.join("|")
    bn = SearsBrowseNode.new :name => node[:name],
      :path_ids => ids.join("$$"),
      :path_names => names.join("$$"),
      :source_id => nil,
      :type => type
    bn.id = node[:id]
    bn.save
    bn
  rescue Exception => e
    error e.message
  end
end

