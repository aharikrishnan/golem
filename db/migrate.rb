def load_xml_to_db
  Dir[abs_path('data/bn-*')].each do |file|
    bns = get_browse_nodes_from_xml_file(file)
    bns.each do |bn|
      begin
      data = get_node_data_from_xml_doc(bn)
      crawl = Crawl.new :uid => data[:id], :fields => data, :dump => compact_str!(bn.to_s), :dump_type => 'xml'
      crawl.type = 'amazon browse node tree'
      crawl.save
      rescue Exception => e
        error "#{e.message}", :silent => true
      end
    end
  end
end

#load_xml_to_db 'tree'
