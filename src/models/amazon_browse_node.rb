# encoding: utf-8
class AmazonBrowseNode < ActiveRecord::Base
  #serialize :path_ids, Array
  #serialize :path_names, Array
  belongs_to :source, :class_name => 'Crawl', :foreign_key => :source_id

  def self.roots
    self.scoped(:conditions => "type = 'root'")
  end

  def self.to_crawl
    self.scoped(:conditions => "status  != 'nocrawl'")
  end

  self.inheritance_column = :_type_disabled
  self.primary_key = :id

  def leaf_nodes
    AmazonBrowseNode.all(:conditions => ["type = ? AND path_ids LIKE ?",'leaf', "%#{self[:id]}%"]).select{|a|a.path_ids =~ /\b#{self[:id]}\b/}
  end

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

  def create_from_crawl crawl
    a_len, c_len = crawl.ancestors.length, crawl.childrens.length
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
    ans = crawl.path_of_ancestors
    ans.each do |an|
      ids << an.map{|a|a[:id]}.reverse.join("|")
      names << an.map{|a|a[:name]}.reverse.join("|")
    end
    bn = AmazonBrowseNode.new :name => crawl.fields[:name],
      :path_ids => ids.join("$$"),
      :path_names => names.join("$$"),
      :source_id => crawl[:id],
      :type => type
    bn.id = crawl.fields[:id]
    bn.save
    bn
  end

end
