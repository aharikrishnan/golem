# encoding: utf-8
class SearsBrowseNode < ActiveRecord::Base
  def self.roots
    self.scoped(:conditions => "type = 'root'")
  end

  self.inheritance_column = :_type_disabled
  self.primary_key = :id

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

