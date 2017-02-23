#!/usr/bin/env ruby

#@TODO Fix code to use batch browse node lookup api
#      Speed up 10x

require 'rubygems'
require 'nokogiri'
require 'net/http'
require 'set'

require 'time'
require 'uri'
require 'openssl'
require 'base64'

# Your AWS Access Key ID, as taken from the AWS Your Account page
AWS_ACCESS_KEY_ID = "AKIAJVNVAZHANY6SSDAA"
# Your AWS Secret Key corresponding to the above ID, as taken from the AWS Your Account page
AWS_SECRET_KEY = "pTP8VogJ/0n1vZfhnB3pkj8LwQtumDeO3iqutanx"
AWS_ASSOCIATE_TAG='"yoyo03f-21"'
# The region you are interested in
ENDPOINT = "webservices.amazon.com"
REQUEST_URI = "/onca/xml"

# it returns root - like browse node and not actual root browse node,
# we have to use the browse node api to crawl*  the BrowseNode forest
# to build the whole tree
# * - crawl in both direction, parent -> child & child -> parent to explore the whole tree

def parse_root_bn_ids_from_html html
  root_bn_ids = []
  doc = Nokogiri::HTML.fragment(html)

  res = doc.css('.fsdDeptBox a[href]').map do |v, i|
    a = v.attr('href');
      y = /node=([\d]+)/.match a
      (!y.nil? && y.length > 1)? y[1] : nil
  end.compact
  res
end

def http_get url, tmpfile='/tmp/blah'
  puts "[GET] #{url}"
  resp = nil
  # -s = Shutup
  `curl -o #{tmpfile} -s '#{url}' > /dev/null`
  if File.exists? tmpfile
    resp = File.read(tmpfile)
  else
    raise "Getting '#{url}' went wrong :/"
  end
  resp
end

def get_root_bn_ids
  url = "https://www.amazon.com/gp/site-directory/ref=nav_shopall_fullstore"
  tmpfile = '/tmp/amazon-sitemap.html'
  resp = http_get url, tmpfile
  root_like_bns = if !resp.nil?
    parse_root_bn_ids_from_html(resp)
  else
    []
  end
  puts "Found #{root_like_bns.length} root-like nodes"
  root_like_bns
end

def http_get_bn bn_id, timeout=0.9
  params = {
    "Service" => "AWSECommerceService",
    "Operation" => "BrowseNodeLookup",
    "AWSAccessKeyId" => AWS_ACCESS_KEY_ID,
    "AssociateTag" => AWS_ASSOCIATE_TAG,
    "BrowseNodeId" => bn_id,
    "ResponseGroup" => "BrowseNodeInfo"
  }

  # Set current timestamp if not set
  params["Timestamp"] = Time.now.gmtime.iso8601 if !params.key?("Timestamp")

  # Generate the canonical query
  canonical_query_string = params.sort.collect do |key, value|
    [URI.escape(key.to_s, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]")), URI.escape(value.to_s, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))].join('=')
  end.join('&')

  # Generate the string to be signed
  string_to_sign = "GET\n#{ENDPOINT}\n#{REQUEST_URI}\n#{canonical_query_string}"

  # Generate the signature required by the Product Advertising API
  signature = Base64.encode64(OpenSSL::HMAC.digest(OpenSSL::Digest.new('sha256'), AWS_SECRET_KEY, string_to_sign)).strip()

  # Generate the signed URL
  request_url = "http://#{ENDPOINT}#{REQUEST_URI}?#{canonical_query_string}&Signature=#{URI.escape(signature, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))}"

  tmpfile = "/tmp/bn-#{bn_id}"
  resp = http_get request_url, tmpfile
  sleep 0.9
  resp
end

def parse_xml_without_namespace xml
  doc = Nokogiri::XML(xml)
  doc.remove_namespaces!
  doc
end

def get_node_data bn_id
  @bn||={}
  @bn[bn_id]
end

def set_node_data data={}
  @bn||={}
  @bn[data[:id]] ||={}
  @bn[data[:id]].merge(data)
end

def node_visited? bn_id
  @visited_nodes ||= [].to_set
  @visited_nodes.include? bn_id
end

def visit_node bn_id
  @visited_nodes ||= [].to_set
  @visited_nodes << bn_id
end

def add_ancestor bn_id, parent_bn_id
  @bn||={}
  @bn[bn_id] ||={}
  @bn[bn_id][:ancestors] ||= []
  if !@bn[bn_id][:ancestors].include? parent_bn_id
    @bn[bn_id][:ancestors] << parent_bn_id
  else
    puts "Already an ancestor :|"
  end
  puts "#{bn_id} had #{@bn[bn_id][:ancestors].length} parent(s)"
end

def add_child bn_id, child_bn_id
  @bn||={}
  @bn[bn_id] ||={}
  @bn[bn_id][:children] ||= []
  if !@bn[bn_id][:children].include? child_bn_id
    @bn[bn_id][:children] << child_bn_id
  else
    puts "Already a child :|"
  end
  puts "#{bn_id} had #{@bn[bn_id][:children].length} child/children"
end

def get_node_data_from_xml_doc xml_doc
  bn_id = xml_doc.xpath('BrowseNodeId').text
  name = xml_doc.xpath('Name').text
  {
    :id => bn_id,
    :name => name
  }
end

def floodfill_tree bn_id
  return bn_id if node_visited?(bn_id)
  xml = http_get_bn bn_id
  doc = parse_xml_without_namespace xml
  bn =doc.xpath('/BrowseNodeLookupResponse/BrowseNodes/BrowseNode')
  set_node_data(get_node_data_from_xml_doc(bn))
  visit_node bn_id
  children = bn.xpath('Children/BrowseNode').map{|child_bn| get_node_data_from_xml_doc(child_bn)}
  children.each do |child|
    floodfill_tree child[:id]
    add_child bn_id, child[:id]
  end

  ancestors = bn.xpath('Ancestors/BrowseNode')
  ancestors.each do |parent|
    parent_data = get_node_data_from_xml_doc parent
    floodfill_tree parent_data[:id]
    add_ancestor bn_id, parent_data[:id]
    # First 2 levels of ancestors are virtual nodes
    # Add them with this request
    current_parent_bn_id = parent_data[:id]
    grandparent = parent.xpath('Ancestor/BrowseNode')
    while grandparent.length > 0
      grandparent_data = get_node_data_from_xml_doc parent
      set_node_data grandparent_data
      add_ancestor current_parent_bn_id, grandparent_data[:id]
      current_parent_bn_id = grandparent_data[:id]
      grandparent = current_parent.xpath('Ancestor/BrowseNode')
    end
  end


end

def amazon_bn_tree
  root_bn_ids = get_root_bn_ids
  root_bn_ids.each do |root_bn|
    floodfill_tree root_bn
  end
end

