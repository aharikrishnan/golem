class ActiveRecord::Base
  def self.acts_as_browse_node
    klass = self
    klass.belongs_to :source, :class_name => 'Crawl', :foreign_key => :source_id
    klass.inheritance_column = :_type_disabled
    klass.primary_key = :id

    klass.extend Product::ClassMethods
    klass.send :include, Product::InstanceMethods
  end
end

module Product
  module ClassMethods
    def roots
      self.scoped(:conditions => "type = 'root'")
    end

    def to_crawl
      self.scoped(:conditions => "status  != 'nocrawl'")
    end
  end

  module InstanceMethods
    def leaf_nodes
      AmazonBrowseNode.all(:conditions => ["type = ? AND path_ids LIKE ?",'leaf', "%#{self[:id]}%"]).select{|a|a.path_ids =~ /\b#{self[:id]}\b/}
    end

    def full_path
      self.path_names.split("$$").map{|p| "#{p}|#{self.name}"}
    end

    def full_id
      self.path_ids.split("$$").map{|p| "#{p}|#{self[:id]}"}
    end
  end
end
