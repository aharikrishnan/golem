#@TODO Fix code to use batch browse node lookup api
#      Speed up 10x

#init_worker __FILE__.match(/\d/).first.to_i rescue 0
init_worker 0

# it returns root - like browse node and not actual root browse node,
# we have to use the browse node api to crawl*  the BrowseNode forest
# to build the whole tree
# * - crawl in both direction, parent -> child & child -> parent to explore the whole tree

def parse_root_bn_ids_from_html html
  root_bn_ids = []
  doc = Nokogiri::HTML.fragment(html)
  res = doc.css('.fsdDeptBox a[href]').map do |v, i|
    a = v.attr('href');
      y = /node=([\d]+)/.match a
      (!y.nil? && y.length > 1)? y[1] : nil
  end.compact
  res
end

def get_root_bn_ids
  url = "https://www.amazon.com/gp/site-directory/ref=nav_shopall_fullstore"
  tmpfile = 'amazon-sitemap.html'
  resp = Http.get url, tmpfile
  root_like_bns = if !resp.nil?
    parse_root_bn_ids_from_html(resp)
  else
    []
  end
  puts "Found #{root_like_bns.length} root-like nodes"
  root_like_bns
end

def amazon_bn_tree
  run_id = Time.now.to_i
  root_bn_ids = get_root_bn_ids
  root_bn_ids.each do |root_bn|
    TreeUtils.floodfill_tree(root_bn) do |bn_id|
      xml = Http.get_bn bn_id
      doc = parse_xml_without_namespace xml
      bn =doc.xpath('/BrowseNodeLookupResponse/BrowseNodes/BrowseNode')
    end
  end
end

