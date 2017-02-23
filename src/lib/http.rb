class Http
  class << self

    def get url, tmpfile='blah'
      tmpfile = data_file tmpfile
      puts "[GET] #{url}"
      resp = nil
      # -s = Shutup
      `curl -o #{tmpfile} -s '#{url}' > /dev/null`
      if File.exists? tmpfile
        resp = File.read(tmpfile)
      else
        raise "Getting '#{url}' went wrong (╭ರ_•́)"
      end
      resp
    end

    def content_cached? file
      File.exists? file
    end

    def cached_content file
      File.read file
    end

    def get_bn bn_ids, timeout=0.8
      bn_id = bn_ids
      #bn_id = [bn_ids].flatten.sort
      tmpfile = "bn-#{bn_id}"
      datafile = data_file tmpfile
      return cached_content(datafile) if content_cached?(datafile)
      params = {
        "Service" => "AWSECommerceService",
        "Operation" => "BrowseNodeLookup",
        "AWSAccessKeyId" => aws_access_key_id,
        "AssociateTag" => aws_associate_tag,
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
      signature = Base64.encode64(OpenSSL::HMAC.digest(OpenSSL::Digest.new('sha256'), aws_secret_key, string_to_sign)).strip()

      # Generate the signed URL
      request_url = "http://#{ENDPOINT}#{REQUEST_URI}?#{canonical_query_string}&Signature=#{URI.escape(signature, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))}"

      resp = self.get request_url, tmpfile
      sleep timeout
      resp
    end
  end
end
