# encoding: utf-8
class AmazonProduct < ActiveRecord::Base
  #serialize :path_ids, Array
  #serialize :path_names, Array
  belongs_to :source, :class_name => 'Crawl', :foreign_key => :source_id
  belongs_to :browse_node, :class_name => 'AmazonBrowseNode', :foreign_key => :bn_id

  has_many :browse_node_mapping, :class_name => 'ProductBrowseNodeMapping', :primary_key => 'id', :foreign_key => 'p_id'
  has_many :browse_nodes, :through => :browse_node_mapping, :class_name => 'AmazonBrowseNode', :source => :a_browse_node

  def bn_id= browse_node_id
    if self[:bn_id].present?
      add_browse_node browse_node_id
    else
      self[:bn_id] = browse_node_id
    end
  end

  def add_browse_node browse_node_id
    if self[:bn_id] != browse_node_id
      existing_bn_ids = (self.browse_node_mapping.present?)? self.browse_node_mapping.map(&:bn_id) : []
      if !existing_bn_ids.include?(browse_node_id)
        self.browse_node_mapping.create :p_id => self.id, :bn_id => browse_node_id
      end
    end
  end

  def self.create_from_crawl crawl
    doc = crawl.dump
    items = doc.css('Items > Item')
    debug "To process #{items.length}"
    items.map do |item| 
      begin
        asin = item.css('> ASIN').text.strip
        title = item.css('> ItemAttributes Title').text.strip rescue ""
        model = item.css(" > ItemAttributes Model").text.strip rescue ""
        brand = item.css("> ItemAttributes Brand").text.strip rescue ""
        bn_ids = item.css('BrowseNodes > BrowseNode').map{|bn| bn.css(">BrowseNodeId").text.to_s.strip}
        attrs = {:title => title, :model => model, :brand => brand, :source_id => crawl.id, :bn_id => bn_ids.first}
        p = self.find(asin)
        if p.present?
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
      rescue Exception => e
        error e.message, :silent => true
      end
    end
  end

end

#
# +-----------+--------------+------+-----+---------+-------+
# | Field     | Type         | Null | Key | Default | Extra |
# +-----------+--------------+------+-----+---------+-------+
# | id        | varchar(255) | NO   | PRI | NULL    |       |
# | title     | varchar(255) | YES  |     | NULL    |       |
# | model     | varchar(255) | YES  |     | NULL    |       |
# | brand     | varchar(255) | YES  |     | NULL    |       |
# | source_id | int(11)      | YES  |     | NULL    |       |
# | bn_id     | varchar(255) | YES  |     | NULL    |       |
# +-----------+--------------+------+-----+---------+-------+
#
