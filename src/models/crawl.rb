# encoding: utf-8
class Crawl < ActiveRecord::Base
  serialize :fields
  set_table_name 'crawls_mini'

  has_one :crawl_dump, :class_name => 'CrawlDump', :primary_key => 'id', :foreign_key => 'id', :autosave => true

  self.inheritance_column = :_type_disabled

  delegate :dump, :to => :crawl_dump

  accepts_nested_attributes_for :crawl_dump

  def dump_with_type
    dump_str = dump_without_type
    @dump ||= begin
                if dump_str.present?
                  case self.dump_type
                  when 'xml' then
                    Nokogiri::XML(dump_str)
                  when 'html' then
                    Nokogiri::HTML.fragment(dump_str)
                  when 'yaml' then
                    YAML.parse(dump_str) rescue {}
                  when 'json' then
                    JSON.parse(dump_str) rescue {}
                  else
                    dump_str
                  end
                else
                  nil
                end
              end
  end
  alias_method_chain :dump, :type

  def dump= dump_str
    cd = self.crawl_dump
    if cd.present?
      cd.dump = dump_str
    else
      cd = self.build_crawl_dump
      cd.id = self.id
      cd.dump = dump_str
    end
  end



  def self.crawled uid
    Crawl.find_by_uid(uid)
  end

  def populate
    case self[:type]
    when 'amazon browse node tree' then
      AmazonBrowseNode.create_from_crawl(self)
    when 'amazon search' then
      AmazonProduct.create_from_crawl(self)
    when 'amazon upc lookup' then
      AmazonProduct.create_from_upc_crawl(self)
    when 'sears search' then
      SearsProduct.create_from_crawl(self)
    else
      facepalm "#{self[:type]} not supported yet"
      nil
    end
  rescue Exception => e
    error e.message, :silent => true
  end

  # Amazon browse node related methods
  def self.create_from_bn_xml bn_xml
    bns = AmazonBrowseNode.get_browse_nodes_from_xml(bn_xml.to_s)
    crawls = []
    bns.each do |bn|
      data = AmazonBrowseNode.get_node_data_from_xml_doc(bn)
      if crawled data[:id]
        facepalm "Already crawled #{data[:id]}"
        next
      else
        crawl = Crawl.new :uid => data[:id], :fields => data, :dump => compact_str(bn.to_s), :dump_type => 'xml'
        crawl.type = 'amazon browse node tree'
        crawl.save
        crawls << crawl
      end
    end
    crawls
  end

  def childrens
    AmazonBrowseNode.get_children_from_xml_doc self.dump
  end

  def ancestors
    AmazonBrowseNode.get_ancestors_from_xml_doc self.dump
  end

  def path_of_ancestors
    self.ancestors.map do |ancestor|
      recursive_get_bn(ancestor).flatten
    end
  end

  def recursive_get_bn node, p=[]
    bns = []
    return bns if node.nil?
    bns << {:name => node.css(">Name").text, :id => node.css(">BrowseNodeId").text  }
    p << bns
    if node.css(">Ancestors").length > 0
      recursive_get_bn node.css(">Ancestors > BrowseNode"), p
    end
    p
  end


  def self.parse_xml_without_namespace xml
    doc = Nokogiri::XML(xml)
    doc.remove_namespaces!
    doc
  end

end
