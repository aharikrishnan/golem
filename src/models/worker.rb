# encoding: utf-8

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
        sleep 1 # prevent deadlock
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
    false
  end

  def job?
    false
  end

  def to_param h={}
    h.map{|k,v| [k, CGI::escape(v)].join("=")}.join("&")
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

end

class ApiWorker < Worker

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

  def get_url_from crawl_job
    type = crawl_job[:type]
    url = case type
          when 'amazon search' then
            self.amazon_search(crawl_job.input)
          when 'amazon keyword search' then
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
  private

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
end

require 'parallel'
require 'ruby-progressbar'

class ParseWorker
  attr_accessor :from_step, :to_step, :crawl_type

  def initialize opts
    self.crawl_type = opts[:type]
    self.from_step = opts[:from]
    self.to_step = opts[:to]
  end

  def start
    self.class.start self.crawl_type, self.from_step, self.to_step
  end

  def self.start crawl_type, from_step, to_step
    ActiveRecord::Base.logger = nil
    crawl_ids_to_process = Crawl.scoped(:select => "id", :conditions => [ "type = ? and step is NULL or step in (?)", crawl_type, from_step ]).map(&:id)
    grouped_crawl_ids = crawl_ids_to_process.each_slice(10)

    Parallel.each(grouped_crawl_ids, :in_processes =>  16, :progress => 'Parsing') do |grouped_crawl|
      ActiveRecord::Base.connection_pool.with_connection do
        @reconnected ||= Crawl.connection.reconnect! || true
        process grouped_crawl, to_step
      end
    end
    ActiveRecord::Base.logger = Logger.new(STDOUT)
    info "Done"
  end

  def self.process grouped_crawl, state
    Crawl.all(:conditions => {:id => grouped_crawl}).each do |c|
      c.populate
      c.step = state
      c.save
    end
  end
end


@@threads = []
def go_to_work!
  #Thread.abort_on_exception = truej
  threads = []
  AWS_IDENTITIES.each do |identity|
    ApiWorker.new(identity)
  end
end
