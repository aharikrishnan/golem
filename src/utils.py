%run ~/codebase/loader/load.py

root_bns_to_leaf_count = load_sql("""
  select T1.id, T1.name, count(T1.name)
  from crawls.amazon_browse_nodes as T1
  inner join crawls.amazon_browse_nodes as T2
  on T2.path_ids like CONCAT('%', T1.id, '|%') 
  where T1.type = 'root' and T2.type = 'leaf'
  group by T1.id
  order by count(T1.name) desc;
""")


#27 rows in set (5 min 4.12 sec)
root_bns_to_products_count = load_sql("""
  select T1.id, T1.name, count(T1.name)
  from crawls.amazon_browse_nodes as T1
  inner join crawls.amazon_browse_nodes as T2
  on T2.path_ids like CONCAT('%', T1.id, '|%') 
  inner join crawls.products_browse_nodes_mapping as T3
  on T3.bn_id = T2.id
  where T1.type = 'root' and T2.type = 'leaf'
  group by T1.id
  order by count(T1.name) desc;
""")

bns_to_product_count = load_sql("""
  SELECT T2.id, T2.name, count(T2.name)
  from crawls.amazon_browse_nodes as T1
  inner join crawls.amazon_browse_nodes as T2
  on T2.path_ids like CONCAT('%', T1.id, '|%') 
  inner join crawls.products_browse_nodes_mapping as T3
  on T3.bn_id = T2.id
  where T1.id= '2619525011' and T2.type = 'leaf'
  group by T2.id
  order by count(T1.name);

""")
# appliances = 2619525011
# +---------------------------+----------------+
# | name                      | count(T1.name) |
# +---------------------------+----------------+
# | Clothing, Shoes & Jewelry |         369998 |
# | Home & Kitchen            |         300745 |
# | Tools & Home Improvement  |         287042 |
# | Automotive                |         282118 |
# | Industrial & Scientific   |         264257 |
# | Sports & Outdoors         |         260235 |
# | Health & Household        |         137940 |
# | Electronics               |         116687 |
# | Office Products           |          95977 |
# | Grocery & Gourmet Food    |          78711 |
# | Toys & Games              |          74460 |
# | Patio, Lawn & Garden      |          69115 |
# | Beauty & Personal Care    |          57433 |
# | Arts, Crafts & Sewing     |          40785 |
# | Appliances                |          39073 |
# | Musical Instruments       |          35776 |
# | Baby Products             |          29789 |
# | Pet Supplies              |          23803 |
# | Movies & TV               |          18476 |
# | Video Games               |          18443 |
# | Software                  |          12121 |
# | Cell Phones & Accessories |           9484 |
# | Collectibles & Fine Art   |           9432 |
# | Books                     |           1411 |
# | CDs & Vinyl               |            504 |
# | Kindle Store              |              3 |
# | Magazine Subscriptions    |              1 |
# +---------------------------+----------------+

