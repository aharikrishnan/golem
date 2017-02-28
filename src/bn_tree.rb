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

def get_root_bn_ids
  # http://docs.aws.amazon.com/AWSECommerceService/latest/DG/LocaleUS.html
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
  # from find browsenodes
  root_bn_ids = ["1036682","2619526011","2617942011","15690151","165797011","11055981","1000","4991426011","541966","2625374011","493964","2973079011","2255571011","16310211","3760931","1063498","16310161","3880591","358606011","284507","3238155011","599872","2350150011","624868011","301668","11965861","1084128","11846801","2619534011","502394","672124011","491286","3375301","468240","165795011","2625374011","378516011","2335753011","2407755011"].reverse
  root_bn_ids.each do |root_bn|
    TreeUtils.floodfill_tree(root_bn) do |bn_id|
      # returns array of bn docs
    end
  end
end
