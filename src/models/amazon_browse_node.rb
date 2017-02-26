# encoding: utf-8
class AmazonBrowseNode < ActiveRecord::Base
  #serialize :path_ids, Array
  #serialize :path_names, Array
  belongs_to :source, :class_name => 'Crawl', :foreign_key => :source_id

  self.inheritance_column = :_type_disabled
  self.primary_key = :id

  def leaf_nodes
    AmazonBrowseNode.all(:conditions => ["type = ? AND path_ids LIKE ?",'leaf', "%#{self[:id]}%"]).select{|a|a.path_ids =~ /\b#{self[:id]}\b/} end

  # :search_index
  # :bn
  # :page
  def search opts={}
    crawl_options = opts.merge(:bn => self[:id])
    cj = CrawlJob.new(:input => crawl_options)
    cj.type = 'amazon search'
    cj.save
    puts cj.inspect
  end

  def self.path
    JSON.parse(path)
  end

  def self.path= p
    self[:path] = p.to_json
  end

  def full_path
    self.path_names.split("$$").map{|p| "#{p}|#{self.name}"}
  end

  def full_id
    self.path_ids.split("$$").map{|p| "#{p}|#{self[:id]}"}
  end

  def self.get_node_data_from_xml_doc xml_doc
    bn_id = xml_doc.xpath('BrowseNode/BrowseNodeId').text.presence||xml_doc.xpath('BrowseNodeId').text
    name = xml_doc.xpath('BrowseNode/Name').text.presence || xml_doc.xpath('Name').text
    #File.open("/tmp/vv","w"){|f|f.write(xml_doc.to_s)} if bn_id.nil? || bn_id.length < 1
    #error xml_doc.to_s if bn_id.nil? || bn_id.length < 1

    node_data = {
      :id => bn_id,
      :name => name
    }
    node_data
  end

  def self.get_browse_nodes_from_xml content
    doc = parse_xml_without_namespace content
    bn =doc.xpath('/BrowseNodeLookupResponse/BrowseNodes/BrowseNode')
    [bn].flatten
  end

  def self.get_children_from_xml_doc xml_doc
    xml_doc.xpath('BrowseNode/Children/BrowseNode')
  end
  def self.get_ancestors_from_xml_doc xml_doc
    xml_doc.xpath('BrowseNode/Ancestors/BrowseNode')
  end

end
