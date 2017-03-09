# encoding: utf-8
class AmazonProduct < ActiveRecord::Base
  acts_as_product

  def self.create_from_upc_crawl crawl
    self.create_from_crawl crawl
  end

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
        ean = item.css("> ItemAttributes EAN").text.strip rescue ""
        bn_ids = item.css('BrowseNodes > BrowseNode').map{|bn| bn.css(">BrowseNodeId").text.to_s.strip}
        first_leaf_bn_id = item.css('BrowseNodes > BrowseNode').select{|bn| bn.css(">Children").length == 0}.first.css("> BrowseNodeId").text
        info "Leaf bn => #{first_leaf_bn_id}"
        attrs = {:title => title, :model => model, :brand => brand, :upc => upc, :ean => ean, :source_id => crawl.id, :bn_id => bn_ids.first, :bn_ids => bn_ids}

        add_product asin, attrs

      rescue Exception => e
        File.open(abs_path("error.log"), 'a'){|f|f.write("#{crawl.id} -- #{asin} -> #{e.message}\n")}
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
