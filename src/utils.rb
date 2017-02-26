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
        bn.search(:search_index => 'Electronics', :page => p)
      end
    end
  end
end
