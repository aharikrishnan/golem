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
        #bn.search(:search_index => 'Electronics', :page => p)
        bn.search(:search_index => 'Shoes', :page => p)
      end
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
