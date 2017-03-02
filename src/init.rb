#!/usr/bin/env ruby
# encoding: utf-8

require 'rubygems' # ruby 1.8.7 compat

require 'active_record'
require 'base64'
require 'logger'
require 'net/http'
require 'nokogiri'
require 'openssl'
require 'pathname'
require 'set'
require 'mysql'
require 'time'
require 'uri'
require 'yaml'

PROJECT_ROOT = Pathname.new(File.expand_path(File.join(File.dirname(__FILE__), '..')))
ENDPOINT = "webservices.amazon.com"
REQUEST_URI = "/onca/xml"
MAX_WORKERS = 3

def abs_path rel_path
  File.expand_path(File.join(PROJECT_ROOT, rel_path))
end

def read_yaml rel_path
  YAML.load_file abs_path(rel_path)
end

def rel_path? path
  !path.starts_with? '/'
end

def get_path rel_or_abs_path
  full_path = rel_path?(rel_or_abs_path)? File.join(PROJECT_ROOT, rel_or_abs_path) : rel_or_abs_path
end

def each_files pat=nil, &blk
  Dir.foreach(get_path(pat)) do |file_name|
    file_path = file_name
    file_path = get_path(File.join(pat, file_name))
    puts file_path
    next if !File.file?(file_path)
    debug "> Found #{file_path}"
    blk.call(file_path)
  end
end

def require_files pat
  Dir[get_path(pat)].each do |file|
    require_file file
  end
end

def require_file file
  file.sub!(/$/, '.rb') unless file =~ /\.rb$/
  full_path = get_path file
  #require File.join(PROJECT_ROOT, file)
  load full_path
end

def worker
  @worker ||= 2
end

def init_worker id
  worker = id
  validate_worker
  puts "Worker #{worker} at your service ᕙ(⇀‸↼‶)ᕗ "
end

def validate_worker
  raise "Max worker is #{MAX_WORKERS}" unless (0..MAX_WORKERS).to_a.include?(worker)
end

def aws_access_key_id
  AWS_IDENTITIES[worker]['access_key_id']
end
def aws_associate_tag
  AWS_IDENTITIES[worker]['associate_tag']
end

def aws_secret_key
  AWS_IDENTITIES[worker]['secret_key']
end

def data_file file_name
  File.expand_path(File.join(PROJECT_ROOT, 'data', file_name))
end

def parse_xml_without_namespace xml
  Crawl.parse_xml_without_namespace xml
end

def get_browse_nodes_from_xml_file file
  AmazonBrowsNode.get_browse_nodes_from_xml File.read(file)
end

def get_node_data_from_xml_doc xml_doc
  AmazonBrowsNode.get_node_data_from_xml_doc xml_doc
end

def get_children_from_xml_doc xml_doc
  AmazonBrowsNode.get_children_from_xml_doc xml_doc
end

def get_ancestors_from_xml_doc xml_doc
  AmazonBrowsNode.get_ancestors_from_xml_doc xml_doc
end

def compact_str! str
   str.gsub!(/\n/, ' ').gsub!(/[\s\t]+/, ' ')
end

def run_id; @run_id; end
def run_id= id; @run_id = id; end

def info msg
  puts "(っ◕‿◕)っ\t #{msg}\n"
end

def debug msg
  puts "(Ͼ˳Ͽ)..!!!\t#{msg}"
end

def error msg, opts={}
  m =  "ლ(ಠ益ಠ)ლ \t#{msg}"
  if opts[:silent]
    puts m
  else
    raise m
  end
end

def facepalm msg
  puts "(>ლ) \t#{msg}\n"
end

AWS_IDENTITIES = read_yaml('identities/aws.yml')['product_advertising']
raise "err_emptyidentity" if AWS_IDENTITIES.nil? || AWS_IDENTITIES ==[]

require_files 'src/lib/*'
require_files 'src/models/*'
require_file 'src/bn_tree'
require_file 'src/golem'

def migrate
  DB.establish_connection
  require_files 'db/migrate.rb'
  require_files 'db/migrations/*'
end

def reload
  require_file __FILE__
end

