require_file 'src/lib/http.rb'
=begin
count = 10000
optimal_count = [rand(count), 10].max.to_i
CRAWLED = begin
            x = Set.new
            l = [rand(optimal_count), 0].min.to_i
            l.times{ x << rand(count) }
            debug x.length
            x
          end
class Http
  def self.get_crawled_list
    CRAWLED
  end
end
debug Http.get_count( (0..count).to_a, optimal_count)
=end
# crawled = 1,2,3,10,11,12,13,14,15
# to_crawl = 10
# result = 20
class Http
  def self.get_crawled_list
    [6,7,4,1,10,19,20,13,0]
    #[1,2,3,10,11,12,13,14,15]
  end
end

debug Http.get_count( (0..20).to_a, 10)

