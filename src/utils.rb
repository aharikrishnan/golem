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
