class AddColumnUidToCrawls< ActiveRecord::Migration
  def self.up
    add_column :crawls, :uid, :string , :after => :id
    add_index('crawls', [:uid], :unique=>true)
  end

  def self.down
    remove_column :crawls, :uid
  end
end

