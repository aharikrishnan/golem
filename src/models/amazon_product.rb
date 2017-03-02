# encoding: utf-8
class AmazonProduct < ActiveRecord::Base
  acts_as_product

  def self.create_from_crawl crawl
    doc = crawl.dump
    items = doc.css('Items > Item')
    debug "To process #{items.length}"
    items.map do |item| 
      begin
        asin = item.css('> ASIN').text.strip
        title = item.css('> ItemAttributes Title').text.strip rescue ""
        model = item.css(" > ItemAttributes Model").text.strip rescue ""
        brand = item.css("> ItemAttributes Brand").text.strip rescue ""
        upc = item.css("> ItemAttributes UPC").text.strip rescue ""
        bn_ids = item.css('BrowseNodes > BrowseNode').map{|bn| bn.css(">BrowseNodeId").text.to_s.strip}
        attrs = {:title => title, :model => model, :brand => brand, :upc => upc, :source_id => crawl.id, :bn_id => bn_ids.first}

        add_product asin, attrs

      rescue Exception => e
        error e.message, :silent => true
      end
    end
  end

end

#
# +-----------+--------------+------+-----+---------+-------+
# | Field     | Type         | Null | Key | Default | Extra |
# +-----------+--------------+------+-----+---------+-------+
# | id        | varchar(255) | NO   | PRI | NULL    |       |
# | title     | varchar(255) | YES  |     | NULL    |       |
# | model     | varchar(255) | YES  |     | NULL    |       |
# | brand     | varchar(255) | YES  |     | NULL    |       |
# | source_id | int(11)      | YES  |     | NULL    |       |
# | bn_id     | varchar(255) | YES  |     | NULL    |       |
# +-----------+--------------+------+-----+---------+-------+
#
