def content_cached? uid
  Crawl.crawled(uid).present?
end

def cached_content uid
  Crawl.find_by_uid(uid).dump
end

def save_bn bn_xml
  Crawl.create_from_bn_xml bn_xml
end
