# encoding: utf-8

class TreeUtils
  class << self
    def tree
      @bn ||= {}
      @bn
    end

    def visited
      @visited_nodes ||= [].to_set
    end

    # {:id => {} }
    def set_node data={}
      tree[data[:id]] ||={}
      tree[data[:id]].merge!(data)
      info "Set Node: #{data.inspect}"
    end

    def node_visited? bn_id
      visited.include? bn_id
    end

    def visit_node bn_id
      visited << bn_id
      info "Visited #{bn_id}"
    end

    def add_ancestor bn_id, parent_bn_id
      tree[bn_id] ||={}
      tree[bn_id][:ancestors] ||= []
      if !tree[bn_id][:ancestors].include? parent_bn_id
        tree[bn_id][:ancestors] << parent_bn_id
      else
        facepalm "Already an ancestor"
      end
      info "#{tree[bn_id]} has #{tree[bn_id][:ancestors].length} ancestors"
    end

    def add_child bn_id, child_bn_id
      tree[bn_id] ||={}
      tree[bn_id][:children] ||= []
      if !tree[bn_id][:children].include? child_bn_id
        tree[bn_id][:children] << child_bn_id
      else
        facepalm "Already a child"
      end
      info "#{tree[bn_id]} has #{tree[bn_id][:children].length} children"
    end

    #def get_nodes_from_xml_doc xml_doc
      #xml_doc.xpath('B')
    #end

    def _q
      @queue||=[]
    end

    # TODO convert to BFS - Done
    def floodfill_tree bn_id, &blk
      info "Fill #{bn_id}"
      return bn_id if node_visited?(bn_id)
      _q.push(bn_id)

      index = 0
      while _q.length > 0 do
        #bn = blk.call(bn_id)
        bns = Http.get_bns _q
        bns.each do |bn|
          bn_data = get_node_data_from_xml_doc(bn)
          set_node(bn_data)
          children = get_children_from_xml_doc(bn).map do |child_bn| 
            get_node_data_from_xml_doc(child_bn)
          end
          children.each do |child|
            #floodfill_tree child[:id], &blk
            _q.push(child[:id])
            add_child bn_data[:id], child[:id]
          end
          ancestors = get_ancestors_from_xml_doc(bn)
          ancestors.each do |parent|
            parent_data = get_node_data_from_xml_doc parent
            #floodfill_tree parent_data[:id], &blk
            _q.push(parent_data[:id])
            add_ancestor bn_data[:id], parent_data[:id]
            # First 2 levels of ancestors are virtual nodes
            # Add them with this request
            current_parent_bn_id = parent_data[:id]
            grandparent = get_ancestors_from_xml_doc(parent)
            while grandparent.length > 0
              # MAybe Bug
              grandparent_data = get_node_data_from_xml_doc parent
              set_node grandparent_data
              add_ancestor current_parent_bn_id, grandparent_data[:id]
              current_parent_bn_id = grandparent_data[:id]
              grandparent = get_ancestors_from_xml_doc(grandparent)
            end
          end
          # a node is visited when all neighbours are visited
          visit_node bn_data[:id]
        end
        info "To process #{_q.inspect}"
        _q.reject!{|e| node_visited?(e)}
        info "To process #{_q.length} node(s)"
        index += 1
        save if (index%100 == 0)
      end
      save
    end

    def save
      bn_file = data_file "bntree-amazon"
      debug "writing tree to file '#{bn_file}'. Tree Length #{tree.length}"
      File.open(bn_file,'w'){|f|f.write(tree.to_yaml)}
    end


    # Tree structure:
    #   {:id => '', :name => '', children => [..]}
    def dfs(*args, &blk)
      tree = args[0]
      path = args[1] || []
      options = args.last
      options = options.is_a?(Hash)? options: {}
      node_picker = options[:node_picker]

      return if tree.blank?
      blk.call(tree, path)

      if tree[:children].present?
        new_path = path + [{:name => tree[:name], :id => tree[:id]}]
        tree[:children].each do |node|
          if !node_picker.nil?
            node = node_picker.call(node)
          end
          dfs(*[node,new_path, {:node_picker => node_picker}], &blk)
        end
      end
    end

  end
end
