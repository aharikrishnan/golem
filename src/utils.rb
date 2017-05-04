# encoding: utf-8

Crawl.find_in_batches(:batch_size => 10000){|cs|
  Crawl.transaction{
    cs.map(&:populate);nil
  }
}
AmazonBrowseNode.find_in_batches(:batch_size => 10000){|cs|
  AmazonBrowseNode.transaction{
    cs.each{|c|
      x=c.path_names.map{|p| p.split('|').reverse.join('|')}
      y=c.path_ids.map{|p| p.split('|').reverse.join('|')}
      c.path_names = x
      c.path_ids = y
      c.save
    }
  }
}

AmazonBrowseNode.find_in_batches(:batch_size => 10000){|cs|
  AmazonBrowseNode.transaction{
    cs.each{|c|
      c.path_names = YAML.load(c.path_names).join(' $$ ')
      c.path_ids = YAML.load(c.path_ids).join(' $$ ')
      c.save
    }
  }
}
bn_ids = File.read(abs_path('input.lst')).split.uniq.compact; nil
bns = AmazonBrowseNode.find_all_by_id(bn_ids); nil

AmazonBrowseNode.roots.to_crawl.each do |root_bn|
  bns = root_bn.leaf_nodes
  CrawlJob.transaction do
    (1..10).each do |p|
      bns.each do |bn|
        #'Electronics' 'Shoes' 'Fashion' 'HomeGarden' 'Tools' 'Appliances' 'OfficeProducts' 'Wireless'
        bn.search(:page => p, :search_index => root_bn.search_index); nil
      end
    end
  end
  puts "Done #{root_bn.name}"
end

cnt_vec = File.read("/tmp/am_bn_count_vector.1.csv").split("\n").map{|r|x=r.split("\t"); [x.first, x.last.split("|")]};nil
cnt_vec = Hash[cnt_vec[1..-1]]

cnt_vec.each do |bn_id, kws|
  bn = AmazonBrowseNode.find(bn_id)
  #kws.split("|")[0..10].each do |kw|
  kws[0..15].each do |kw|
    puts kw
    (8..10).each do |p|
      bn.search(:page => p, :search_index => bn.root.first.search_index, :keywords => kw, :type => "k-#{kw}"); nil
    end
  end
end

#bns = File.read("/tmp/pass_lt_200.csv").split.compact
bns = File.read("/tmp/pass_lt_300.csv").split.compact
bns.each do |bn_id|
  bn = AmazonBrowseNode.find(bn_id)
  #kws.split("|")[0..10].each do |kw|
  kws = (cnt_vec[bn_id] || [])[0..20]
  next if kws.blank?
  kws.each do |kw|
    puts kw
    (8..10).each do |p|
      bn.search(:page => p, :search_index => bn.root.first.search_index, :keywords => kw, :type => "k-#{kw}"); nil
    end
  end
end

load '/home/hari/codebase/golem/src/init.rb'
Dir.foreach("/home/hari/amazon/flash/tmp/.").each do |file|
  if file =~/^search*/
    #bn = file.scan(/\d+/).first
    _, bn, page = file.match(/([0-9]+).([0-9]+)/).to_a
    uid ="a-s-#{bn}-#{page}"
    c = Crawl.find_by_uid(uid)
    next if c.present?
    c=Crawl.new :uid => uid, :fields => {:bn => bn,:page => page, :search_index => 'Shoes'}, :dump => File.read("/home/hari/amazon/flash/tmp/#{file}"), :dump_type => 'xml'
    c.type = 'amazon search'
    c.save
  end
end


require 'rubygems'
require 'json'
require 'pathname'

out_path = File.expand_path("/home/hari/babi/out/")
img_out_path = Pathname.new(File.expand_path("/home/hari/babi/img/"))

bn = "1020003"
Dir.foreach("#{out_path}/.").each_with_index do |file, index|
  fp = File.join(out_path, file)
  next if !File.file?(fp) || !(file =~ /^#{bn}/)
  bn_path = file.match(/[0-9_]+/)
  puts fp
  json = JSON.parse(File.read(fp)) rescue nil
  next if json.nil?
  begin
    products = json['data']['products'] || []
    puts products.length
    products.each do |p|
      sin = p["sin"]
      image = p["image"]
      next if image.nil?
      image_url = image.sub(/hei=[0-9]+&wid=[0-9]+/, 'hei=299&wid=299')
      image_file = File.expand_path(File.join(img_out_path, "#{sin}.jpg"))
      if !File.exists? image_file
      `curl -o '#{image_file}' '#{image_url}'`
      end
      if File.exists? image_file
        tar_file_name = "#{bn_path}.tar"
        tar_file = Pathname.new(File.join(img_out_path, tar_file_name))
        rel_path = Pathname.new(image_file).relative_path_from(img_out_path)
        `cd '#{File.join(img_out_path)}' && tar --append --file=#{tar_file} #{rel_path}`
        `rm #{image_file}`
      else
        puts "Failed: #{image_url}"
      end
    end
  rescue Exception => e
    puts e.message
  end
  sleep 1 if (index%100 == 0)
end


bns = ["11194456011", "13397491", "13755271", "13838451", "241127011", "289867", "289918", "289920", "289940", "3304320011", "3737631", "3737721", "3741281", "3741301", "3741611", "3741631", "404433011", "678540011", "678541011", "678542011"]

bns.each do |bn|
  uids = Crawl.all(:select => "uid", :conditions => "uid like 'a-s%-#{bn}-%'")
  puts "To process #{uids.length}"
  uids.each do |uid|
  end
end



require 'rubygems'
require 'pathname'
require 'nokogiri'

def add_img_to_tar opts={}
  image_url = opts[:url]
  image_file = File.expand_path(opts[:img])
  tar_file = File.expand_path(opts[:tar])

  tar_dir = File.dirname(tar_file)
  tar_file_name = File.basename(tar_file)

  if !File.exists? image_file
    `curl -o '#{image_file}' '#{image_url}'`
  else
    puts "From cache"
  end
  if File.exists? image_file
    rel_path = Pathname.new(image_file).relative_path_from(Pathname.new(tar_dir))
    `cd '#{File.join(tar_dir)}' && tar --append --file=#{tar_file_name} #{rel_path}`
    `rm #{image_file}`
  else
    puts "Failed: #{image_url}"
  end
end

out_path = File.expand_path("/home/hari/amazon/flash/data")
img_out_path = Pathname.new(File.expand_path("/home/hari/amazon/flash/img"))
[out_path, img_out_path].each do |p|
  `mkdir -p #{p}` if !File.directory?(p)
end
Dir.foreach("#{out_path}/.").each_with_index do |file, index|
  fp = File.expand_path(File.join(( File.join(out_path, file) )))
  puts "#{fp} --- #{File.file?(fp)}"
  next if !File.file?(fp)
  doc = Nokogiri::XML(File.read(fp))
  items = doc.css('Items> Item')
  puts "To process #{items.length}"
  items.each do |item|
    asin = item.css('>ASIN').text
    img = item.css("> LargeImage > URL").text.to_s
    puts img
    img.gsub!(/.jpg$/, '._SL299_.jpg')
    puts img
    category = item.css('> BrowseNodes > BrowseNode > BrowseNodeId').text.to_s.strip
    tar_file = File.expand_path(File.join(img_out_path, "#{category}.tar"))
    img_file = File.expand_path(File.join(img_out_path, "#{asin}.jpg"))
    add_img_to_tar :img => img_file, :tar => tar_file, :url => img
  end
  sleep 1
end




Crawl.scoped(:conditions  => 'type="amazon search"').find_in_batches(:batch_size => 1000) do |crawls|
  AmazonBrowseNode.transaction do
    ProductBrowseNodeMapping.transaction do
      crawls.each do |c|
        c.populate
      end
    end
  end
end



# populate root_bn_ids in amazon_browse_nodes
AmazonBrowseNode.transaction do
  AmazonBrowseNode.all(:conditions => "type != 'root'").each do |abn|
    root_ids = abn.path_ids.split("$$").map{|p| p.split("|").first.strip}.join("|")
    abn.root_bn_ids = root_ids
    abn.save if abn.changed?
  end
end


# load json files to DB
load '/home/hari/codebase/golem/src/init.rb'
out_path = File.expand_path('/home/hari/babi/out/.')
suffix = '.raw.json'
each_files(out_path) do |file|
  next unless file =~ /#{suffix}$/
  json_data = File.read(file)
  _, bns, page = file.match(/([0-9_]+).([0-9]+)#{suffix}/).to_a
  bn = bns
  uid ="s-s-#{bn}-#{page}"
  c = Crawl.find_by_uid(uid)
  next if c.present?
  c=Crawl.new :uid => uid, :fields => {:bn => bn,:page => page}, :dump => json_data, :dump_type => 'json'
  c.type = 'sears search'
  c.save
end


Crawl.find_in_batches(:batch_size => 1000){|cs|
  Crawl.transaction{
    SearsProduct.transaction do
      cs.map(&:populate);nil
    end
  }
}

# repopulate amazon products from search
Crawl.scoped(:conditions => "uid like 'a-s-%'").find_in_batches(:batch_size => 10000) do |cs|
  AmazonProduct.transaction do
    cs.each do |c|
      c.populate
    end
  end
end

#Crawl.scoped(:conditions => "type='amazon upc lookup' and step='crawled' and uid like 'a-ASIN-All-%'").find_in_batches(:batch_size => 10000) do |cs|
Crawl.scoped(:conditions => "step='crawled'").find_in_batches(:batch_size => 100) do |cs|
  cs.each do |c|
    c.populate
    c.update_attribute 'step', 'parsed'
  end
end

## stats collector
a = Hash[File.read("/home/ubuntu/data/csvs/bn.csv.stat").split("\n").map{|r|r.to_s.strip.split("\t")}]; nil
b = Hash[File.read("/home/ubuntu/data/csvs/bn.db.2.stat").split("\n").map{|r|r.to_s.strip.split("\t")}]; nil
c = {}
a.each do|k, v|
  if b.has_key?(k)
    c[k] = (b[k].to_f/a[k].to_f) * 100
  end
end; nil
puts c.length

d = c.select{|k, v|v< 50}


File.open("/tmp/bn_diff", 'w'){|f|f.write(c.to_a.map{|r|r.join("\t")}.join("\n"))}


## search index population
r = [ ["1020004", "Baby", "Baby"],
 ["1020005", "Automotive", "Automotive"],
 ["1020006", "Fitness & Sports", "SportingGoods"],
 ["1020009", "Gifts", "GiftCards"],
 ["1023303", "Beauty", "Beauty"],
 ["1023816", "Home Services", "Appliances"],
 ["1024539", "Health & Wellness", "HealthPersonalCare"],
 ["1029616", "Books & Magazines", "Books"],
 ["1030488", "Food & Grocery", "Grocery"],
 ["1055398", "Home & Kitchen", "HomeGarden"],
 ["1064954", "Office Products", "OfficeProducts"],
 ["10677469011", "Vehicles", "Vehicles"],
 ["11091801", "Musical Instruments", "MusicalInstruments"],
 ["11260432011", "Handmade Products", "Handmade"],
 ["1270629454", "Home Improvement", "Tools"],
 ["1325032343", "Clothing, Shoes & Jewelry", "Fashion"],
 ["133140011", "Kindle Store", "KindleStore"],
 ["1342036019", "Connected Solutions", "Electronics"],
 ["1348654256", "Home", "Fashion"],
 ["13727921011", "Alexa Skills", "Fashion"],
 ["15684181", "Automotive", "Automotive"],
 ["16310091", "Industrial & Scientific", "Industrial"],
 ["16310101", "Grocery & Gourmet Food", "Grocery"],
 ["163856011", "Digital Music", "MP3Downloads"],
 ["165793011", "Toys & Games", "Toys"],
 ["165796011", "Baby Products", "Baby"],
 ["172282", "Electronics", "Electronics"],
 ["228013", "Tools & Home Improvement", "Tools"],
 ["229534", "Software", "Software"],
 ["2334129011", "Custom Stores", "Fashion"],
 ["2334150011", "Special Features Stores", "Fashion"],
 ["2335752011", "Cell Phones & Accessories", "Wireless"],
 ["2350149011", "Apps & Games", "MobileApps"],
 ["2617941011", "Arts, Crafts & Sewing", "ArtsAndCrafts"],
 ["2619525011", "Appliances", "Appliances"],
 ["2619533011", "Pet Supplies", "PetSupplies"],
 ["2625373011", "Movies & TV", "Movies"],
 ["283155", "Books", "Books"],
 ["2972638011", "Patio, Lawn & Garden", "LawnAndGarden"],
 ["3375251", "Sports & Outdoors", "SportingGoods"],
 ["3561432011", "Credit & Payment Cards", "GiftCards"],
 ["3760901", "Health & Household", "HealthPersonalCare"],
 ["3760911", "Beauty & Personal Care", "Beauty"],
 ["468642", "Video Games", "VideoGames"],
 ["4991425011", "Collectibles & Fine Art", "Collectibles"],
 ["5174", "CDs & Vinyl", "Music"],
 ["599858", "Magazine Subscriptions", "Magazines"],
 ["7141123011", "Clothing, Shoes & Jewelry", "Fashion"],
 ["9013971011", "Video Shorts", "UnboxVideo"] ]
r.each do |id, name, search_index|
  y = AmazonBrowseNode.find_by_id(id);
  next if y.blank?
  y.search_index = search_index
  y.save
end

AmazonBrowseNode.roots.scoped(:conditions => {:id => %w(133140011 13727921011 16310101 163856011 229534 2334129011 2334150011 2350149011 2625373011  283155 3561432011 468642 5174  599858 9013971011).to_a}).update_all :status => 'nocrawl'


def update_amazon_product crawl, inplace=false
  begin
  dps = crawl.dump.css("Items > Item").map do |i| 
    asin = i.css(">ASIN").text
    seo_url = i.css(">DetailPageURL").text
    if inplace
      begin
        ap = AmazonProduct.find(asin)
        ap.seo_url = seo_url
        ap.save
      rescue Exception => e
        print "Asin not found #{asin}"
      end
    end
    [asin, seo_url]
  end
  return dps
  rescue Exception => e
    return []
  end
end

File.open("/tmp/ap.csv", "a") do |fo|
  Crawl.scoped(:conditions => {:type => "amazon search"}).find_in_batches(:batch_size => 10000){|cs|
    #Crawl.transaction{
      #cs.map(&:populate);nil
      cs.map{|c|
        dps = update_amazon_product(c)
        dps.each do |dp|
          fo.write(dp.join("\t"))
          fo.write("\n")
        end
      }
    #}
  }
end
a = AmazonProduct.all(:select => "upc").map(&:upc).uniq;
s = SearsProduct.all(:select => "upc").map(&:upc).uniq;
a.compact!;
s.compact!;
c=a&b;


aids = AmazonBrowseNode.all(:select => "id").map(&:id); nil
cids = Crawl.all(:select => "uid", :conditions => "type ='amazon browse node tree'").map(&:uid); nil
ids = cids - aids; nil
ids.each do |id|
  c = Crawl.find_by_uid(id)
  next if c.nil?
  c.populate
end

#Get all leaf nodes
l = AmazonBrowseNode.all(:conditions => "path_ids like '%#{root.id}|%' and type='leaf'")

Crawl.scoped(:conditions  => 'type="amazon search" and uid like "a-k-%"').find_in_batches(:batch_size => 100) do |cs|
    cs.map(&:populate)
end

cids = Crawl.all(:select => "id", :conditions => "type='amazon search' and uid like 'a-k-%'").map(&:id)
cids.each do |cid|
  Crawl.find(cid).populate
end

cids = Crawl.all(:select => "id", :conditions => 'type="amazon search" and uid like "a-s-%-1"').map(&:id)
cids.each do |cid|
    c = Crawl.find(cid)
    bn_id = c.fields[:bn]

    bn = AmazonBrowseNode.find(bn_id)
    #bn.total_results ||= -1
    #if bn.total_results > 0
      #debug "already there!"
      #next
    #end

    count = -1
    begin
      count = c.dump.css("ItemSearchResponse > Items > TotalResults").text.to_i
    rescue Exception => e
      error "I (#{c.id}) bombed"
    end

    bn.total_results = count
    bn.save
    info "#{bn.id} --> #{bn.total_results}"
    bn.reload
    raise "#{count} -- #{bn.total_results} #{bn.id}" unless bn.total_results == count
end

#Crawl.scoped(:conditions  => 'type="amazon search" and uid like "a-s-%-1"').find_in_batches(:batch_size => 100) do |cs|
#end


asins = File.read("/tmp/asin-to-enrich.csv").split
AmazonBrowseNode.item_lookup_by_asin(asins)



## Script to add priority to amazon products
#
# Convention = 
#   if Browse Node Crawl -> 1..10
#   if Keyword Crawl -> 999 XXX YYY, where XXX is the page number and YYY (1..10) is the position (1..10)
#   if Product/ UPC Crawl -> 888

bn_ids = [ '1197396' ]
valid_product_types = [ "amazon search", "amazon keyword search", "amazon upc lookup"]
bn_ids.each do |bn_id|
  Crawl.scoped(:conditions => [ "uid like ? and type in (?)", "%#{bn_id}%", valid_product_types]).find_in_batches(:batch_size => 100) do |cs|
      cs.map(&:populate)
  end
end

