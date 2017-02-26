# encoding: utf-8
class CrawlJob < ActiveRecord::Base
  serialize :input
  self.inheritance_column = :_type_disabled

  def create_crawl resp
    begin
      search_index = self.input[:search_index] || "Electronics"
      bn= self.input[:bn]
      page = self.input[:page]||1
      uid =  "a-s-#{search_index}-#{bn}-#{page}"
      Crawl.create :uid => uid,
        :type => self.type,
        :fields => self.input,
        :dump => resp,
        :dump_type => 'xml'
    rescue Exception => e
      error e.message, :silent => true
    end
  end

  def self.assign_job!
    CrawlJob.transaction do
      j=CrawlJob.find_by_status(nil)
      j.status = 'assigned'
      j.save
      j
    end
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

  def job?
    CrawlJob.find_by_status(nil).present?
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
      "ResponseGroup" => "BrowseNodes,ItemAttributes,Similarities"
    }
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
    crawl_job = CrawlJob.assign_job!
    if crawl_job.present?
      request_url = self.get_url_from crawl_job
      if request_url.present?
        begin
          resp = Net::HTTP.get(URI.parse(request_url))
          crawl_job.create_crawl resp
          self.eta = Time.now
          crawl_job.status = 'complete'
          crawl_job.save
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
  end
end
