# encoding: utf-8
class AmazonBrowseNode < ActiveRecord::Base
  acts_as_browse_node

  # :search_index
  # :bn
  # :page
  def search opts={}
    search_index = opts[:search_index] || self.search_index
    crawl_options = opts.merge(:bn => self[:id], :search_index => search_index)
    cj = CrawlJob.new(:input => crawl_options)
    cj.type = 'amazon search'
    cj.save
    puts cj.inspect
  end

  def self.item_lookup_by_upc upcs
    upcs.uniq
    upcs.each_slice(10) do |upc_list|
      ids = upc_list.join(",")
      cj = CrawlJob.new(:input => {:ids => ids, :type => 'UPC', :search_index => 'All'})
      cj.type = 'amazon upc lookup'
      cj.save
    end
  end

  def self.path
    JSON.parse(path)
  end

  def self.path= p
    self[:path] = p.to_json
  end

  def root
    AmazonBrowseNode.scoped(:conditions => {:id => self.full_id.first.split("|").first, :type =>'root'})
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

  def self.create_from_crawl crawl
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

  def self.populate_dag
    t = YAML.load_file(abs_path('data/bntree-amazon'))
    forest = t.select{|k, v|v[:ancestors].nil?}
    secondary_bn_paths
    @s_path = {}
    if File.exists?(abs_path('data/bntree_paths'))
      YAML.load_file(abs_path('data/bntree_paths'))
    else
      forest.each do |k, r|
        TreeUtils.dfs(r, [], :node_picker => Proc.new{|node| t[node]}) do |node, path|
          @s_path[node[:id]] ||= {}
          spath = @s_path[node[:id]] || {}
          ids = spath[:ids] || []
          names = spath[:names] || []

          ans = path
          ids << ans.map{|a|a[:id]}.join("|")
          names << ans.map{|a|a[:name]}.join("|")

          ids.uniq!
          names.uniq!

          @s_path[node[:id]] = {:ids => ids, :names => names}
        end
      end
      File.open(abs_path('data/bntree_paths'), 'w'){|f|f.write(@s_path.to_yaml)}
    end

    AmazonBrowseNode.transaction do
      @s_path.each do |bn_id, path_info|
        puts path_info.inspect
        next if path_info.nil?
        a = AmazonBrowseNode.find(bn_id)
        next if a.nil?
        path_names = path_info[:names].join("$$").strip
        path_ids = path_info[:ids].join("$$").strip
        if path_names.length && path_ids.length && (a.path_names != path_names || a.path_ids != path_ids)
          a.path_names = path_names
          a.path_ids = path_ids
          a.save
        end
      end
    end

  end

end
