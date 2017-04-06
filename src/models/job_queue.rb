# encoding: utf-8
class CrawlJob < ActiveRecord::Base
  serialize :input
  self.inheritance_column = :_type_disabled

  def pagination?
    self.input[:pagination] == true
  end

  def get_next_page
    url = case type
    when 'amazon search' then
      nil
    when 'sears search' then
      crawl = Crawl.find_by_uid(self.get_uid)
      current_page = c.dump["data"]["pagination"][0]["id"].to_i
      max_page = c.dump["data"]["pagination"][-1]["id"].to_i
      if current_page < max_page
        next_page = self.clone
        next_page.input.merge!(:page => current_page+1)
        if !next_page.already_crawled?
          next_page.status = nil
          next_page.save
          next_page
        else
          c = Crawl.find_by_uid(next_page.get_uid)
          c
        end
      end
    else
      facepalm "[#{self.tag}]: No entiendo! '#{type}'"
      nil
    end
  end

  def create_crawl resp
    begin
      uid =  self.get_uid
      Crawl.create :uid => uid,
        :type => self.type,
        :fields => self.input,
        :dump => resp,
        :dump_type => 'xml'
    rescue Exception => e
      error e.message, :silent => true
    end
  end

  def default_search_index
    "Fashion"
  end

  def get_uid
    search_index = self.input[:search_index] || default_search_index
    bn= self.input[:bn] || self.input[:ids]
    page = self.input[:page]||1
    p = self.input[:type] || 's'
    keys = ['a', p]
    keys << search_index if self.input[:search_index].present?
    keys << bn
    keys << page
    uid =  keys.join("-")
  end

  def already_crawled?
    uid = self.get_uid
    c = Crawl.find_by_uid(uid)
    c.present?
  end

  def self.pre_assign_job! worker
    status = worker.pre_assigned_status
    CrawlJob.scoped(:conditions => {:status => nil}, :limit => 1).update_all(:status => status)
    j= CrawlJob.all(:conditions => {:status => status}, :limit => 1).first
    j
  end

  def self.assign_job! worker
    j = pre_assign_job! worker
    while j.already_crawled? do
      facepalm "duplicado, ignorando #{j.id}"
      j.status = 'duplicate'
      j.save
      j = pre_assign_job! worker
    end
    j.status = 'assigned'
    j.save
    j
  end
end

class Worker
  attr_accessor :key, :tag, :secret
  attr_accessor :status, :eta, :thread
  @@all_workers=[]

  def initialize identity
    self.key = identity['access_key_id']
    self.tag = identity['associate_tag']
    self.secret = identity['secret_key']
    self.register
    if !worker_running?
      info "Worker '#{self.tag}' registered."
      update_pid(self.eta)
      self.thread = Thread.new do
        process_loop
      end
    else
      "proceso en ejecución"
    end
  end

  def worker_running?
    File.exists?(pid_file)
  end

  def pid_file
    abs_path("workers/#{self.tag}")
  end

  def update_pid time
    File.open(pid_file,'w'){|f|f.write(time)}
  end

  def register
    @@all_workers << self
  end

  def status
    @status ||= 'free'
  end

  def eta
    # 1969-12-31 18:00:00 -0600
    @eta ||= Time.at(0)
  end

  def free?
    debug "free?"
    self.status == 'free' && ((Time.now - self.eta) >= 1)
  end

  def pre_assigned_status
    "pre-assigned-to-#{self.tag}"
  end

  def job?
    CrawlJob.all(:conditions => ["status IS NULL or status = ?", self.pre_assigned_status], :limit => 1, :select => "count(*)").present?
  end

  def to_param h={}
    h.map{|k,v| [k, CGI::escape(v)].join("=")}.join("&")
  end

  def sears_search opts
    url = 'http://www.sears.com/service/search/v2/productSearch'
    page = opts[:page]||1
    path = opts[:path]||1
    category = opts[:category]||1
    categories = opts[:categories]||1
    params = {
      'pageNum' => page,
      'catgroupId' => category,
      'catgroupIdPath' => categories,
      'levels' => path,
      'primaryPath' => path
    }
    "#{url}?#{to_param(params)}"
  end

  def amazon_search opts
    search_index = opts[:search_index] || "Electronics"
    bn= opts[:bn]
    page = opts[:page]||1
    params = {
      "Service" => "AWSECommerceService",
      "Operation" => "ItemSearch",
      "AWSAccessKeyId" => self.key,
      "AssociateTag" => self.tag,
      "SearchIndex" => search_index,
      "BrowseNode" => bn,
      "ItemPage" => page,
      "ResponseGroup" => "BrowseNodes,ItemAttributes,Images,Similarities"
    }
    if opts.has_key? :keywords
      params.merge!({"Keywords" => opts[:keywords]})
    end
    # Set current timestamp if not set
    params["Timestamp"] = Time.now.gmtime.iso8601 if !params.key?("Timestamp")
    # Generate the canonical query
    debug params.inspect
    canonical_query_string = params.sort.collect do |key, value|
      [URI.escape(key.to_s, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]")), URI.escape(value.to_s, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))].join('=')
    end.join('&')
    # Generate the string to be signed
    string_to_sign = "GET\n#{ENDPOINT}\n#{REQUEST_URI}\n#{canonical_query_string}"
    # Generate the signature required by the Product Advertising API
    signature = Base64.encode64(OpenSSL::HMAC.digest(OpenSSL::Digest.new('sha256'), self.secret, string_to_sign)).strip()
    # Generate the signed URL
    request_url = "http://#{ENDPOINT}#{REQUEST_URI}?#{canonical_query_string}&Signature=#{URI.escape(signature, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))}"
    debug request_url
    request_url
  end

  def amazon_upc_lookup opts
    params = {
      "Service" => "AWSECommerceService",
      "Operation" => "ItemLookup",
      "IdType" => opts[:type],
      "ItemId" => opts[:ids],
      "AWSAccessKeyId" => self.key,
      "AssociateTag" => self.tag,
      "ResponseGroup" => "BrowseNodes,ItemAttributes,Images,Similarities"
    }

    if ['DPCI', 'SKU', 'UPC', 'EAN','ISBN'].include?(opts[:type])
      search_index = opts[:search_index] || "All"
      params["SearchIndex"] = search_index
    end
    # Set current timestamp if not set
    params["Timestamp"] = Time.now.gmtime.iso8601 if !params.key?("Timestamp")
    # Generate the canonical query
    debug params.inspect
    canonical_query_string = params.sort.collect do |key, value|
      [URI.escape(key.to_s, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]")), URI.escape(value.to_s, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))].join('=')
    end.join('&')
    # Generate the string to be signed
    string_to_sign = "GET\n#{ENDPOINT}\n#{REQUEST_URI}\n#{canonical_query_string}"
    # Generate the signature required by the Product Advertising API
    signature = Base64.encode64(OpenSSL::HMAC.digest(OpenSSL::Digest.new('sha256'), self.secret, string_to_sign)).strip()
    # Generate the signed URL
    request_url = "http://#{ENDPOINT}#{REQUEST_URI}?#{canonical_query_string}&Signature=#{URI.escape(signature, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))}"
    debug request_url
    request_url
  end

  def get_url_from crawl_job
    type = crawl_job[:type]
    url = case type
    when 'amazon search' then
      self.amazon_search(crawl_job.input)
    when 'amazon upc lookup' then
      self.amazon_upc_lookup(crawl_job.input)
    when 'sears search' then
      self.sears_search(crawl_job.input)
    else
      facepalm "[#{self.tag}]: No entiendo! '#{type}'"
      nil
    end
    url
  end

  def process_loop
    while worker_running? do
      c = free? && job?
      debug c.inspect
      if c
        debug "process.."
        start = Time.now
        process
        finish = Time.now
        diff = finish - start
        info "[#{self.tag}] Trabajo completado en #{diff} s."
        update_pid(self.eta)
        #sleep Math.max(self.eta - Time.now, 0)
        sleep 1
      else
        info "[#{self.tag}]: Dormido {zzz}°°°( -_-)>c[_]"
        sleep 1
      end
    end
    info "Adiós"
    self.thread.exit
  end

  def process
    crawl_job = CrawlJob.assign_job!(self)
    if crawl_job.present?
      request_url = self.get_url_from crawl_job
      if request_url.present?
        begin
          resp = Net::HTTP.get(URI.parse(request_url))
          crawl_job.create_crawl resp
          self.eta = Time.now
          crawl_job.status = 'complete'
          crawl_job.save
          if crawl_job.pagination?
            get_next_page
          end
        rescue Exception => e
          error e.message, :silent => true
          crawl_job.status = 'error'
          crawl_job.save
        end
      end
    else
      facepalm "Nada que procesar"
    end
  end

end

@@threads = []
def go_to_work!
  #Thread.abort_on_exception = truej
  threads = []
  AWS_IDENTITIES.each do |identity|
    Worker.new(identity)
    sleep 1 # prevent deadlock
  end
end
