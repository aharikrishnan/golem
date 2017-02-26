# encoding: utf-8
class Golem
  def parse command
    matches = command.scan(/`([^`]+)`/)
    matches.flatten!
    debug matches.inspect
    if matches.length > 0
      site, *browse_nodes = matches
      case site
      when /amazon/i then
        get_amazon_products(browse_nodes)
      when /sears/i then
        get_sears_products(browse_nodes)
      else
        info "`#{site}` not supported yet."
      end
    else
      info "Did you say '#{command}'?"
    end
  end

  def get_amazon_products_under bn, search_index
    bns = AmazonBrowseNode.find_by_id(bn_id).presence || AmazonBrowseNode.find_by_name(bn_id)
    (1..10).each do |page|
      bns.leaf_nodes.each do |leaf|
        leaf.search :bn => bn_id, :search_index => search_index, :page => page
      end
    end
  end

  def get_amazon_products bn_id
    bns = AmazonBrowseNode.find_all_by_id(bn_id).presence || AmazonBrowseNode.find_all_by_name(bn_id)
    bns.each do |bn|
      info "Crawling `#{bn.full_path}` (#{bn[:id]}) for products"
    end
    nil
  end
  def get_amazon_products_by_keyword
  end
  def get_sears_products bn_id
    "Crawling sears browse node #{bn_id} for products"
  end
  def get_sears_products_by_keyword
  end
end


class String
  def run
    Golem.new.parse(self)
  end
end
