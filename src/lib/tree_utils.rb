class TreeUtils
  class << self
    def tree
      @bn ||= {}
    end

    def visited
      @visited_nodes ||= [].to_set
    end

    # {:id => {} }
    def set_node data={}
      tree[data[:id]] ||={}
      tree[data[:id]].merge!(data)
      save
    end

    def node_visited? bn_id
      visited.include? bn_id
    end

    def visit_node bn_id
      visited << bn_id
    end

    def add_ancestor bn_id, parent_bn_id
      tree||={}
      tree[bn_id] ||={}
      tree[bn_id][:ancestors] ||= []
      if !tree[bn_id][:ancestors].include? parent_bn_id
        tree[bn_id][:ancestors] << parent_bn_id
      else
        facepalm "Already an ancestor"
      end
    end

    def add_child bn_id, child_bn_id
      tree||={}
      tree[bn_id] ||={}
      tree[bn_id][:children] ||= []
      if !tree[bn_id][:children].include? child_bn_id
        tree[bn_id][:children] << child_bn_id
      else
        facepalm "Already a child"
      end
    end

    #def get_nodes_from_xml_doc xml_doc
      #xml_doc.xpath('B')
    #end

    def floodfill_tree bn_id, &blk
      return bn_id if node_visited?(bn_id)

      bn = blk.call(bn_id)

      set_node(get_node_data_from_xml_doc(bn))

      visit_node bn_id

      children = bn.xpath('Children/BrowseNode').map{|child_bn| get_node_data_from_xml_doc(child_bn)}
      children.each do |child|
        floodfill_tree child[:id], &blk
        add_child bn_id, child[:id]
      end

      ancestors = bn.xpath('Ancestors/BrowseNode')
      ancestors.each do |parent|
        parent_data = get_node_data_from_xml_doc parent
        floodfill_tree parent_data[:id], &blk
        add_ancestor bn_id, parent_data[:id]
        # First 2 levels of ancestors are virtual nodes
        # Add them with this request
        current_parent_bn_id = parent_data[:id]
        grandparent = parent.xpath('Ancestor/BrowseNode')
        while grandparent.length > 0
          grandparent_data = get_node_data_from_xml_doc parent
          set_node grandparent_data
          add_ancestor current_parent_bn_id, grandparent_data[:id]
          current_parent_bn_id = grandparent_data[:id]
          grandparent = current_parent.xpath('Ancestor/BrowseNode')
        end
      end
    end

    def save
      bn_file = data_file "tree-#{run_id}"
      info "Tree Length #{tree.length}"
      File.open(bn_file,'w'){|f|f.write(tree.to_yaml)}
    end
  end
end
