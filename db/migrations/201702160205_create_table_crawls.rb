class CreateTableCrawls < ActiveRecord::Migration
  def self.up
    create_table :crawls do |t|
      t.string :id

      t.string :type
      t.text :fields

      t.text :dump
      t.string :dump_type

      t.text :description
      t.timestamps
    end
  end

  def self.down
  end
end
