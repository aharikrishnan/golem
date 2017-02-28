class ProductBrowseNodeMapping < ActiveRecord::Base
  set_table_name 'products_browse_nodes_mapping'
  belongs_to :a_product, :class_name => 'AmazonProduct', :foreign_key => 'p_id'
  belongs_to :a_browse_node, :class_name => 'AmazonBrowseNode', :foreign_key => 'bn_id'
end

# products_browse_nodes_mapping
# +-------+--------------+------+-----+---------+-------+
# | Field | Type         | Null | Key | Default | Extra |
# +-------+--------------+------+-----+---------+-------+
# | id    | int(11)      | YES  |     | NULL    |       |
# | p_id  | varchar(255) | YES  |     | NULL    |       |
# | bn_id | varchar(255) | YES  |     | NULL    |       |
# +-------+--------------+------+-----+---------+-------+
#
