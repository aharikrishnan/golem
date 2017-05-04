class ActiveRecord::Base
  def self.acts_as_product
    klass = self
    klass.belongs_to :source, :class_name => 'Crawl', :foreign_key => :source_id
    klass.belongs_to :browse_node, :class_name => klass.name, :foreign_key => :bn_id

    klass.has_many :browse_node_mapping, :class_name => 'ProductBrowseNodeMapping', :primary_key => 'id', :foreign_key => 'p_id'
    klass.has_many :browse_nodes, :through => :browse_node_mapping, :class_name => klass.name, :source => :a_browse_node

    klass.extend Product::ClassMethods
    klass.send :include, Product::InstanceMethods
  end
end

module Product
  module ClassMethods
    def add_product asin, attrs
      p = self.find(asin) rescue nil
      bn_ids = attrs.delete(:bn_ids) || []

      if p.present?
        # always keep high priority, the lesser is higher
        attrs[:priority] = attrs[:priority].present?? attrs[:priority].to_i : 999999999
        if p.priority.present? && attrs[:priority] > p.priority
          attrs[:priority] = p.priority
        end

        new_attrs = Hash[attrs.select{|k, v|v.present?}]
        new_attrs = p.attributes.merge(new_attrs)
        p.attributes = new_attrs
        if p.changed?
          p.save
        end
      else
        p = self.new attrs
        p.id = asin
        success = p.save
        if success
          bn_ids[1..-1].each do |bn|
            p.add_browse_node(bn)
          end
        end
      end
    end
  end

  module InstanceMethods
    def bn_id= browse_node_id
      if self[:bn_id].present?
        add_browse_node browse_node_id
      else
        self[:bn_id] = browse_node_id
      end
    end

    def add_browse_node browse_node_id
      existing_bn_ids = (self.browse_node_mapping.present?)? self.browse_node_mapping.map(&:bn_id) : []
      if !existing_bn_ids.include?(browse_node_id)
        self.browse_node_mapping.create :p_id => self.id, :bn_id => browse_node_id
      end
    end
  end
end
