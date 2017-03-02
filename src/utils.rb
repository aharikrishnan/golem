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
AmazonBrowseNode.transaction do
  CrawlJob.transaction do
    (1..10).each do |p|
      bns.each do |bn|
        #'Electronics' 'Shoes' 'Fashion' 'HomeGarden' 'Tools' 'Appliances'
        bn.search(:search_index => 'Appliances', :page => p); nil
      end
    end
  end
end; nil


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
