class Crawl < ActiveRecord::Base
  serialize :fields
  self.inheritance_column = :_type_disabled

  def dump
    @dump ||= case self.dump_type
    when 'xml' then
      Nokogiri::XML(self[:dump])
    when 'html' then
      Nokogiri::HTML.fragment(self[:dump])
    else
      self[:dump]
    end
  end

  def self.crawled uid
    Crawl.find_by_uid('x')
  end

  def self.create_from_bn_xml bn_xml
    bns = get_browse_nodes_from_xml(bn_xml.to_s)
    crawls = []
    bns.each do |bn|
      data = get_node_data_from_xml_doc(bn)
      if crawled data[:id]
        facepalm "Already crawled #{data[:id]}"
        next
      else
        crawl = Crawl.new :uid => data[:id], :fields => data, :dump => compact_str!(bn.to_s), :dump_type => 'xml'
        crawl.type = 'amazon browse node tree'
        crawl.save
        crawls << crawl
      end
    end
    crawls
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

  def childrens
    self.class.get_children_from_xml_doc self.dump
  end

  def ancestors
    self.class.get_ancestors_from_xml_doc self.dump
  end

  def self.get_children_from_xml_doc xml_doc
    xml_doc.xpath('BrowseNode/Children/BrowseNode')
  end
  def self.get_ancestors_from_xml_doc xml_doc
    xml_doc.xpath('BrowseNode/Ancestors/BrowseNode')
  end


  def self.get_browse_nodes_from_xml content
    doc = parse_xml_without_namespace content
    bn =doc.xpath('/BrowseNodeLookupResponse/BrowseNodes/BrowseNode')
    [bn].flatten
  end


  def self.parse_xml_without_namespace xml
    doc = Nokogiri::XML(xml)
    doc.remove_namespaces!
    doc
  end

  def self.compact_str! str
    str.gsub!(/\n/, '').gsub!(/[\s]+/, ' ')
  end

end
