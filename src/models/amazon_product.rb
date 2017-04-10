# encoding: utf-8
class AmazonProduct < ActiveRecord::Base
  acts_as_product

  def self.create_from_upc_crawl crawl
    self.create_from_crawl crawl
  end

  def self.parse_from_crawl crawl, options={}
    doc = crawl.dump
    items = doc.css('Items > Item')
    debug "To process #{items.length}"
    items.map do |item| 
      begin
        asin = item.css('> ASIN').text.strip
        seo_url = item.css('> DetailPageURL').text.strip
        title = item.css('> ItemAttributes Title').text.strip rescue ""
        model = item.css(" > ItemAttributes Model").text.strip rescue ""
        brand = item.css("> ItemAttributes Brand").text.strip rescue ""
        upc = item.css("> ItemAttributes UPC").text.strip rescue ""
        ean = item.css("> ItemAttributes EAN").text.strip rescue ""
        bn_ids = item.css('BrowseNodes > BrowseNode').map{|bn| bn.css(">BrowseNodeId").text.to_s.strip}
        first_leaf_bn_id = item.css('BrowseNodes > BrowseNode').select{|bn| bn.css(">Children").length == 0}
        first_leaf_bn_id = (first_leaf_bn_id.length > 0)?  first_leaf_bn_id.first.css("> BrowseNodeId").text : bn_ids.first
        info "Leaf bn => #{first_leaf_bn_id}"
        attrs = {:title => title, :seo_url => seo_url, :model => model, :brand => brand, :upc => upc, :ean => ean, :source_id => crawl.id, :bn_id => bn_ids.first, :bn_ids => bn_ids}
        if ( file = options[:of] ).present?
          File.open(file, 'a'){|fo| fo.write(attrs.sort.values.join("\t")); fo.write("\n") }
        end
        if options[:view_only]
          info attrs.inspect
        else
          add_product asin, attrs
        end
      rescue Exception => e
        error_log "#{crawl.id} -- #{asin} -> #{e.message}\n"
      end
    end
  end
  def self.create_from_crawl crawl
    self.parse_from_crawl crawl, :create => true
  end

end

