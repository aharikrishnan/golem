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

