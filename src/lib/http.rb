# encoding: utf-8

# http://docs.aws.amazon.com/AWSECommerceService/latest/DG/LocaleUS.html
# JSON.stringify($('#d0e64215 tr > td:nth-child(3)').map(function(){return $(this).text()}).get())
# ["2619526011","2617942011","15690151","165797011","11055981","1000","4991426011","493964","7141124011","7147444011","7147443011","7147442011","7147441011","7147440011","2864120011","16310211","11260433011","3760931","1063498","16310161","133141011","3238155011","9479199011","599872","2350150011","2625374011","624868011","301668","11965861","1084128","541966","2619534011","409488","3375301","468240","165795011","2858778011","10677470011","11846801","2983386011","2335753011"]
class Http
  class << self

    def get url, tmpfile=nil
      tmpfile = File.basename(url) if tmpfile.nil?
      tmpfile = data_file tmpfile
      resp = nil
      # -s = Shutup
      if File.exists? tmpfile
        puts "[GET] [LOCAL] [#{tmpfile}] #{url}"
        return File.read(tmpfile)
      end
      puts "[GET] #{url}"
      `curl -o #{tmpfile} -s '#{url}' > /dev/null`
      if File.exists? tmpfile
        resp = File.read(tmpfile)
      else
        raise "Getting '#{url}' went wrong (╭ರ_•́)"
      end
      resp
    end

    def cache_obj key, obj
      instance_variable_set("@#{key}",obj)
    end
    def get_from_cache key
      instance_variable_get("@#{key}")
    end
    def in_cache? key
      !get_from_cache(key).nil?
    end
    def fetch_cached_obj key
      if in_cache? key
        get_from_cache(key)
      else
        obj = yield if block_given?
        cache_obj(key, obj)
      end
    end

    # does a binary search on the queue
    # to find optimized partition for queue
    # that reduces the number of N/W calls
    def partition q, optimal_length=10
      #debug "searching for #{optimal_length}"
      len = q.length
      index = i = j = len - 1
      m = (len/2).floor
      begin
        index = i
        #m = ((j+1)/2).floor
        to_crawl = get_uncrawled_list(q[0..i])
        #debug "#{i} #{j} #{index} to_crawl: #{to_crawl.length} optimal: #{optimal_length}"
        if to_crawl.length < optimal_length
          i = i + m
        elsif to_crawl.length > optimal_length
          j = i
          i = i - m
        else
          j = i
        end
        m = ((j-i+1)/2).floor
      end while i < j
      info "Found ya! #{index}"
      index + 1 # Array index starts from 0
    end

    def get_uncrawled_list bn_ids
      crawled = get_crawled_list
      #debug "#{crawled.length} -- #{bn_ids.length}"
      bn_ids.to_set - crawled
    end

    def get_crawled_list
      @all_crawled ||= fetch_cached_obj('crawled_set') do
        Crawl.all(:select => 'uid').map(&:uid).to_set
      end
    end

    def optimize_queue queue
      l = partition queue
      info "Optimal partition is #{l}/#{queue.length}"
      bn_ids = queue.shift(l)
      debug bn_ids.inspect
      info "#{queue.inspect}"
      crawled = Crawl.find_all_by_uid(bn_ids)
      to_crawl = bn_ids - crawled.map(&:uid)
      [to_crawl, crawled]
    end

    def get_bns queue, timeout=0.8
      bn_ids, crawled= optimize_queue queue
      #bn_id = [bn_ids].flatten.sort
      tmpfile = "bn-#{bn_ids.join('_')}"
      datafile = data_file tmpfile
      new_crawls = []
      if bn_ids.present?
        info "Fetch! #{bn_ids.inspect}"
        params = {
          "Service" => "AWSECommerceService",
          "Operation" => "BrowseNodeLookup",
          "AWSAccessKeyId" => aws_access_key_id,
          "AssociateTag" => aws_associate_tag,
          "BrowseNodeId" => bn_ids.join(","),
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
        signature = Base64.encode64(OpenSSL::HMAC.digest(OpenSSL::Digest.new('sha256'), aws_secret_key, string_to_sign)).strip()

        # Generate the signed URL
        request_url = "http://#{ENDPOINT}#{REQUEST_URI}?#{canonical_query_string}&Signature=#{URI.escape(signature, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))}"

        resp = self.get request_url, tmpfile
        new_crawls  = Crawl.create_from_bn_xml(resp)
        new_crawls.each{|c| @all_crawled << c.uid}
        sleep timeout
      end
      [crawled, new_crawls].flatten.compact.map(&:dump)
    end
  end
end
