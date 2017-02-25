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

  def populate
    case self[:type]
    when 'amazon browse node tree' then
      a_len, c_len = self.ancestors.length, self.childrens.length
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
      ans = self.path_of_ancestors
      ans.each do |an|
        ids << an.map{|a|a[:id]}.reverse.join("|")
        names << an.map{|a|a[:name]}.reverse.join("|")
      end
      bn = AmazonBrowseNode.new :name => self.fields[:name],
        :path_ids => ids.join("$$"),
        :path_names => names.join("$$"),
        :source_id => self[:id],
        :type => type
      bn.id = self.fields[:id]
      bn.save
      bn
    else
      nil
    end
  rescue Exception => e
    error e.message, :silent => true
  end

  # Amazon browse node related methods
  def self.create_from_bn_xml bn_xml
    bns = get_browse_nodes_from_xml(bn_xml.to_s)
    crawls = []
    bns.each do |bn|
      data = AmazonBrowseNode.get_node_data_from_xml_doc(bn)
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

  def self.compact_str! str
    str.gsub!(/\n/, '').gsub!(/[\s]+/, ' ')
  end

end
