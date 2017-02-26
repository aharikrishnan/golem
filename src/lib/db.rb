# encoding: utf-8
module DB
  class << self
    def establish_connection db_config_path='db/database.yml'
      ActiveRecord::Base.logger = Logger.new(STDOUT)
      self.config = read_yaml db_config_path
      ActiveRecord::Base.establish_connection(self.config['development'])
    end

    def config
      @config||={}
    end

    def config= config
      @config= config
    end

    def env
      ENV['CRAWL_ENV']||= 'development'
    end

  end
end

# gem 'mysql', '2.8.1'
begin
  DB.establish_connection
rescue Exception => e
  error "Error in bootstrapping database #{e.inspect}"
  error e.backtrace.join("\n")
  exit
end


