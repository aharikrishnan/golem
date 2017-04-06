# encoding: utf-8

class CrawlDump < ActiveRecord::Base
  set_table_name 'crawls'
  belongs_to :crawl, :class_name => 'Crawl', :foreign_key => 'id'
  self.inheritance_column = :_type_disabled
end
