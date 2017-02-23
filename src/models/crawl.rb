class Crawl < ActiveRecord::Base
  serialize :fields
  self.inheritance_column = :_type_disabled
end
