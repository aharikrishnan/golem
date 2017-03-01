# encoding: utf-8
#@TODO Fix code to use batch browse node lookup api
#      Speed up 10x - Done

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

def amazon_get_root_bn_ids
  # http://docs.aws.amazon.com/AWSECommerceService/latest/DG/LocaleUS.html
  url = "https://www.amazon.com/gp/site-directory/ref=nav_shopall_fullstore"
  tmpfile = 'tmp/amazon-sitemap.html'
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
  root_bn_ids = amazon_get_root_bn_ids
  # from find browsenodes
  #root_bn_ids = ["1036682","2619526011","2617942011","15690151","165797011","11055981","1000","4991426011","541966","2625374011","493964","2973079011","2255571011","16310211","3760931","1063498","16310161","3880591","358606011","284507","3238155011","599872","2350150011","624868011","301668","11965861","1084128","11846801","2619534011","502394","672124011","491286","3375301","468240","165795011","2625374011","378516011","2335753011","2407755011"].reverse
  root_bn_ids.each do |root_bn|
    TreeUtils.floodfill_tree(root_bn) do |bn_id|
      # returns array of bn docs
    end
  end
end

def sears_get_root_bn_ids
  sitemap_url = 'http://www.sears.com/en_us/sitemap.html'
  tmpfile = 'sears-sitemap.html'
  resp = Http.get sitemap_url, tmpfile
  doc = Nokogiri::HTML(resp)
  t = doc.css("section, #sitemap .has-columns ul.list-links > li")
  root_bns = t.map do |x|
    link = x.css('> h2 > a')
    c =x.css('>a')
    if x.blank? || c.blank?
      facepalm x.text
      nil
    else
      node = get_node_from_html_doc x
      node[:children] = c.attr('href').text
      node
    end
  end
  root_bns.compact!
  root_bns
end

def get_node_from_html_doc x
  a = x.css('> h2 > a').first || x.css('> a').first
  name = a.text || a.attr('name').text
  link = a.attr('href')
  id = link.match(/\/b-([0-9]+)/)[1]
  node = {:name => name, :id => id}
  children = x.css(' > ul > li').map do |child|
    get_node_from_html_doc child
  end
  children.compact!
  if children.present?
    node[:children] = children
  end
  node
end

def page_to_forest page_url
  domain = "http://www.sears.com/"
  unless page_url.starts_with?('http')
    page_url = "#{domain}#{page_url}"
  end
  resp = Http.get(page_url)
  doc = Nokogiri::HTML(resp)
  forest = doc.css('section').map do |root|
    r = get_node_from_html_doc root
  end
  forest.compact!
  forest
end

def sears_bn_tree
  root_bns = sears_get_root_bn_ids
  root_bns.map! do |tree|
    # recursive get browse node
    if tree[:children] =~ /sitemap/
      info "get recursive #{tree[:children]}"
      # expand child nodes
      tree[:children] = page_to_forest(tree[:children])
    end
    tree
  end
  root_bns
end


def populate_sears_browse_nodes
  @bn_tree = sears_bn_tree
  @bn_tree.each do |bn|
    TreeUtils.dfs(bn) do |node, path|
      SearsBrowseNode.create_from_crawl node, path
    end
  end
end
