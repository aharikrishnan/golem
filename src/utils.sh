cat bn_stat.csv | ruby -ne 'x=$_.to_s.strip; a,b,c = x.split("\t"); puts "#{[a,b].join("|")}\t#{c}"' > bn.db.stat

cat bn.csv.stat | ruby -ne 'x=$_.to_s.strip; a,b = x.split("\s", 2); puts "#{b}\t#{a}"' > bn.csv.stat.1

# ALTER TABLE amazon_browse_nodes CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci;

