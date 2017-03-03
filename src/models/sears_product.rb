class SearsProduct < ActiveRecord::Base
  acts_as_product

  def self.create_from_crawl crawl
    data = crawl.dump['data']
    leaf = nil
    categories = []
    if data.present? && data['breadCrumb'].present?
      leaf = data['breadCrumb'].last["url"].match(/\/b-([0-9]+)/)[1] rescue nil
      puts leaf.inspect
    end
    items = data['products']
    if items.present?
      items.each do |item|
        sin = compact_str item['sin']
        title = compact_str item['name']
        brand = compact_str item['brandName']
        upc = compact_str item['upc']
        attrs = {:title => title, :brand => brand, :upc => upc, :source_id => crawl.id, :bn_id => leaf, :bn_ids => [leaf]}
        add_product sin, attrs
      end
    else
      facepalm "NOT processing #{crawl.uid}  Items not found...", :silent => true
    end
  end
end
