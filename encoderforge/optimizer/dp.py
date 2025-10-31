from encoderforge.base.graph import PrepGraph
from collections import defaultdict
from encoderforge.cost_model.utils import *
from encoderforge.optimizer.merge import *
from encoderforge.base import defs
import bisect

def legal_joins(graph: PrepGraph):
    op_nodes = [
        (feature, op)
        for feature, chain in graph.chains.items()
        if SQLPlanType.JOIN in graph.implements[feature]
        for op in chain.prep_operators
        if op._get_op_type() in {"CAT_C_CAT", "EXPAND"}
    ]

    op_graph = defaultdict(set)
    for i, node1 in enumerate(op_nodes):
        for node2 in op_nodes[i + 1:]:
            op_graph[node1].add(node2)
            op_graph[node2].add(node1)

    return op_nodes, op_graph

def find_connected_components(op_nodes: list, op_graph):
    # visited = set() # set place randomly 
    # components = []
                                
    # def dfs(node, component):
    #     visited.add(node)
    #     component.append(node)
    #     for neighbor in op_graph[node]:
    #         if neighbor not in visited:
    #             dfs(neighbor, component)
                                
    # for node in op_nodes:
    #     if node not in visited:
    #         component = []
    #         dfs(node, component)
    #         if len(component) >= 2:
    #             components.append(component)
    # print(components)  
    # components = [op_nodes]                 
    # return components
    return [op_nodes]

cost_cache = {}
def get_graph_cost(group, table_size, flag=False):
    nodes_info = []
    for node_group in group:
        features = []
        for feature, operator in node_group:
            features.append(feature)
        features.sort()
        key_name = ''.join(features)
        nodes_info.append(key_name)
    graph_key = str(sorted(nodes_info))
        
    if graph_key not in cost_cache or flag is True:
        fusion_groups = []
        for node_group in group:
            fusion_group = []
            for feature, operator in node_group:
                fusion_group.append((feature, operator))
            fusion_groups.append(fusion_group)
                
        merged_cost = get_group_cost(fusion_group, table_size, flag) 
        cost_cache[graph_key] = merged_cost
    return cost_cache[graph_key]

def generate_bell_partitions(nodes):
    if not nodes:
        return [[]]
    
    result = []
    first = nodes[0]

    for partition in generate_bell_partitions(nodes[1:]):
        for i in range(len(partition)):
            new_partition = partition.copy()
            new_partition[i] = new_partition[i] + [first]
            result.append(new_partition)

        result.append(partition + [[first]])

    return result

def get_case_cost(operator):
    cost = 0
    for feature_out in operator.features_out:
        cost += operator._get_op_cost(feature_out)
    return cost

def update_implement(graph, group):
    feature_name, op = group
    index = graph.chains[feature_name].prep_operators.index(op)
    graph.implements[feature_name][index] = SQLPlanType.CASE

'''way2'''
def dp_component1(component, table_size, dp_way):
    
    # print(component)
    """Use dynamic programming for a single connected component, based on the Bell number form"""
    case_update = {}
    n = len(component)
    if n < 2:
        return None, float('inf')
    # dp[state] - (min_cost, group_list)
    dp = {0: (0, [])} 
    for state in range(1, 1 << n):
        min_cost = float('inf')
        best_prev_group_list = None
        if (state & (state - 1)) == 0:
            i = state.bit_length() - 1
            single_node = component[i]
            group_cost = get_graph_cost([[single_node]], table_size)
            group_case_cost = get_case_cost(single_node[1])
            if group_case_cost < group_cost:
                case_update[single_node] = "CASE"
                group_cost = group_case_cost
            dp[state] = (group_cost, [[single_node]])
            continue
            
        subset = state
        
        # if dp_way == "disable":
        #     subset = (state - 1) & state
        #     prev_state = state & (~subset)
        #     if prev_state in dp:
        #         prev_state_cost, prev_group_list = dp[prev_state]
        #         cost_1, group_list_1 = dp[subset]
        #         total_cost = prev_state_cost + cost_1
        #         group_list = prev_group_list + group_list_1
        #         if total_cost < min_cost:
        #             min_cost = total_cost
        #             best_prev_group_list = group_list

        #     if min_cost != float("inf"):
        #         dp[state] = (min_cost, best_prev_group_list) # dict->list
        #     continue
        # elif dp_way == "base2":
        if dp_way == "base2":
            size_edge = 2
            while subset > state >> 1:
                prev_state = state & (~subset)
                if prev_state in dp:
                    prev_state_cost, prev_group_list = dp[prev_state]
                    prev_size = bin(prev_state).count("1") 
                    group = [component[i] for i in range(n) if subset & (1 << i)]

                    if prev_size == 0:
                        if size_edge == 2 and len(group) == 2:
                            group_cost = get_graph_cost([group],table_size)
                            # print(group,":",group_cost)
                            total_cost = prev_state_cost + group_cost
                            if total_cost < min_cost:
                                min_cost = total_cost
                                best_prev_group_list = prev_group_list + [group]
                    elif prev_size <= size_edge:
                        cost_1, group_list_1 = dp[subset]
                        total_cost = prev_state_cost + cost_1
                        group_list = prev_group_list + group_list_1
                        if total_cost < min_cost:
                            min_cost = total_cost
                            best_prev_group_list = group_list
                    else:  # prev_size > size_edge
                        break

                subset = (subset - 1) & state  

            if min_cost != float("inf"):
                dp[state] = (min_cost, best_prev_group_list)
        else:
            # print("origin************")
            while subset > state >> 1 : 
                prev_state = state & (~subset)
                group = [component[i] for i in range(n) if subset & (1 << i)]
                if prev_state in dp:
                    prev_state_cost, prev_group_list = dp[prev_state]

                    if subset == state:
                        group_cost = get_graph_cost([group], table_size)
                        total_cost = prev_state_cost + group_cost

                        if total_cost < min_cost:
                            min_cost = total_cost
                            best_prev_group_list = prev_group_list + [group]
                    else:
                        cost_1, group_list_1 = dp[subset]
                        # cost_1 = get_graph_cost(group_list_1, table_size ,True)
                        total_cost = prev_state_cost + cost_1
                        group_list = prev_group_list + group_list_1

                        if total_cost < min_cost:
                            min_cost = total_cost
                            best_prev_group_list = group_list

                subset = (subset - 1) & state
            if min_cost != float('inf'):
                dp[state] = (min_cost, best_prev_group_list)
                # print(dp[state])

    best_cost, best_group = dp[(1 << n) - 1]
    return best_group, best_cost, case_update           
        
def update_case(graph, components, table_size):
    case_update = {}
    for component in components:
        group_cost = get_graph_cost([[component]], table_size, True)
        group_case_cost = get_case_cost(component[1])
        # print(group_cost, group_case_cost)
        if group_case_cost < group_cost:
            case_update[component] = "CASE"
            group_cost = group_case_cost
            update_implement(graph, component)


def dp_join_fusion(graph: PrepGraph, table_size, dp_way):
    op_nodes, op_graph = legal_joins(graph)
    connected_components = find_connected_components(op_nodes, op_graph)
    # '''way3''' 
    # def dp_component2(component, table_size):
    #     n = len(component)
    #     if n < 2:
    #         return None, float('inf')

    #     cost_cache = {}

    #     def helper(sub_component, table_size):
    #         key = tuple(sorted(id(node) for node in sub_component))
    #         if key in cost_cache:
    #             return cost_cache[key]

    #         # base case
    #         if len(sub_component) == 1:
    #             group_cost = get_graph_cost([[sub_component[0]]], table_size)
    #             cost_cache[key] = (group_cost, [[sub_component[0]]])
    #             return cost_cache[key]

    #         whole_group_cost = get_graph_cost([sub_component], table_size)
    #         best_cost = whole_group_cost
    #         best_group_list = [sub_component]

    #         subset_mask = (1 << len(sub_component)) - 1
    #         subset = subset_mask
    #         while subset > 0:
    #             subset = (subset - 1) & subset_mask
    #             if subset == 0 or subset == subset_mask:
    #                 continue  

    #             left = []
    #             right = []
    #             for i in range(len(sub_component)):
    #                 if subset & (1 << i):
    #                     left.append(sub_component[i])
    #                 else:
    #                     right.append(sub_component[i])

    #             left_cost, left_group = helper(left)
    #             right_cost, right_group = helper(right)

    #             total_cost = left_cost + right_cost
    #             group_list = left_group + right_group

    #             if total_cost < best_cost:
    #                 best_cost = total_cost
    #                 best_group_list = group_list

    #         cost_cache[key] = (best_cost, best_group_list)
    #         return cost_cache[key]

    #     best_cost, best_group = helper(component, table_size)
    #     return best_group, best_cost
    #     # pass
        
    best_partition = []

    for component in connected_components:
        if dp_way == "disable":
            # print("disable*********")
            update_case(graph, component, table_size)
            final_graph = graph.copy_graph()
        elif len(component) >= 2:
            groups, cost, case_update = dp_component1(component, table_size, dp_way)
            # print(cost)
            if groups is not None:
                for group in groups:
                    if len(group) > 1:
                        best_partition.append(group)
                    elif case_update.get(group[0]):
                        update_implement(graph, group[0])
            final_graph = graph.copy_graph()
            if best_partition:
                for partition in best_partition:
                    final_graph = apply_fusion_plan(final_graph, partition)
    # cost = get_encoderforge_graph_cost_multiv(final_graph, table_size)
    # print(cost)        
                    
    return final_graph

def sort_join_fusion(graph: PrepGraph, table_size):
    
    # def update_case(graph, components):
    #     case_update = {}
    #     for component in components:
    #         group_cost = get_graph_cost([[component]], table_size, True)
    #         group_case_cost = get_case_cost(component[1])
    #         # print(group_cost, group_case_cost)
    #         if group_case_cost < group_cost:
    #             case_update[component] = "CASE"
    #             group_cost = group_case_cost
    #             update_implement(graph, component)
                           
    op_nodes, op_graph = legal_joins(graph)
    connected_components = find_connected_components(op_nodes, op_graph)
    if defs.DBMS != "monetdb":
        update_case(graph, connected_components[0],table_size)
    else:
        pass
    
    op_nodes, op_graph = legal_joins(graph)
    connected_components = find_connected_components(op_nodes, op_graph)
    
    def sort_func(component, priority):
        if priority == "r":
            return sorted(component, key=get_r)
        elif priority == "l":
            return sorted(component, key=get_l)
        elif priority == "size":
            return sorted(component, key=get_size)
        else:
            return component

    def sort_key_func(priority):
        if priority == "r":
            return get_r
        elif priority == "l":
            return get_l
        elif priority == "size":
            return get_size
        else:
            return lambda x: 0  # 不排序

    def insert_sorted(component, new_item, priority):
        """priority based place new_item into component true place"""
        key_func = sort_key_func(priority)
        key_val = key_func(new_item)
        keys = [key_func(c) for c in component]
        pos = bisect.bisect_left(keys, key_val)
        component.insert(pos, new_item)
        return component

    def sort_method(component, priority):
        """
        component: list of nodes to be sorted and merged (e.g., [A, B, C] or [[A], [B], [C]])
        priority: sorting priority
        """
        # each component is a list
        component = [[c] if not isinstance(c, list) else c for c in component]

        # init
        component = sort_func(component, priority)

        while len(component) > 1:
            cost_sep = get_graph_cost([component[0]], table_size, True) + get_graph_cost([component[1]], table_size, True)
            merged_group = component[0] + component[1]
            cost_merge = get_graph_cost([merged_group], table_size, True)

            if cost_merge < cost_sep:
                # drop first two and insert the merged group
                component = component[2:]
                component = insert_sorted(component, merged_group, priority)
            else:
                # 无法合并
                return component
        return component

    best_partition = []

    for component in connected_components:
        # priority = ["r", "l", "size"]
        priority = ["r"]
        min_cost = float('inf')
        min_cost_preprocessing_graph = None
        for pri in priority:
            print(pri,":")
            groups_ = sort_method(component, pri) 
            for group in groups_:
                if len(group) > 1:
                    best_partition.append(group)
            current_graph = graph.copy_graph()
            if best_partition:
                for partition in best_partition:
                    current_graph = apply_fusion_plan(current_graph, partition)
            # current_graph = apply_fusion_plan(graph.copy_graph(), groups_)
            cost = get_encoderforge_graph_cost_multiv(current_graph, table_size)
            if cost < min_cost:
                min_cost_preprocessing_graph = current_graph
                min_cost = cost
        if min_cost_preprocessing_graph is not None:        
            final_graph = min_cost_preprocessing_graph
        else:
            final_graph = graph

    return final_graph

def slicing_join_fusion(graph: PrepGraph, table_size):
    # graph_new = dp_join_fusion(graph, table_size, dp_way="disable") 
    
    '''case-join only + slicing'''
    op_nodes, op_graph = legal_joins(graph)
    connected_components = find_connected_components(op_nodes, op_graph)
    if defs.DBMS != "monetdb":
        update_case(graph, connected_components[0],table_size)
    else:
        pass
    op_nodes, op_graph = legal_joins(graph)
    connected_components = find_connected_components(op_nodes, op_graph)
    
    def sort_func(component, priority):
        if priority == "r":
            return sorted(component, key=get_r)
        elif priority == "l":
            return sorted(component, key=get_l)
        elif priority == "size":
            return sorted(component, key=get_size)
        else:
            return component

    def slicing_method(component, priority):
        component = [[c] if not isinstance(c, list) else c for c in component]
        
        component = sort_func(component, priority)
        group = component[0]
        number = 0
        partition = []
        table_size_ = []
        max_size = 2293760 if defs.DBMS=="postgresql" else 42000

        while number < len(component) - 1:
            next_number = number + 1
            col2, colnums2 = get_col(component[next_number][0])
            row2 = get_row(component[next_number][0])
            
            # ritht_table_size = get_size(component[number]) + get_size(component[next_number])
            if len(table_size_) == 0:
                col1, colnums1 = get_col(component[number][0])
                row1 = get_row(component[number][0])
                right_table_size = row1 * row2 * (col1 + col2)
                table_size_ = [row1 * row2, col1 + col2]
            else:
                right_table_size = table_size_[0] * row2 * (table_size_[1] + col2)
                table_size_ = [table_size_[0] * row2, table_size_[1] + col2]
            
            # right_table_row = row1 * row2
            if right_table_size < max_size:
            # if right_table_size < 43750000:
            # if right_table_row < 30000:
                group.insert(len(group),component[next_number][0])
            else:
                partition.append(group)
                group = component[next_number]
                table_size_ = []
            number = next_number
        if len(group) > 1:
            partition.append(group)
        return partition

    def slicing_method_(component, priority):
        component = [[c] if not isinstance(c, list) else c for c in component]
        
        component = sort_func(component, priority)
        group = component[0]
        number = 0
        partition = []
        table_size_ = []
        if defs.DBMS=="postgresql":
            max_size = 2293760
        elif defs.DBMS=="duckdb":
            max_size = 42000
        elif defs.DBMS=="monetdb":
            # max_size = 468730962
            max_size = 80132704700
        # max_size = 2293760 if defs.DBMS=="postgresql" else 42000
        # max_size = 2293760
        while number < len(component) - 1:
            next_number = number + 1
            # col2, colnums2 = get_col(component[next_number][0])
            # row2 = get_row(component[next_number][0])
            rightcol, rightrow = 0, 1
            for item in component[next_number]:
                col2, _ = get_col(item)
                rightcol += col2
                # rightcolnums += colnums2
                rightrow *= get_row(item)
            
            # ritht_table_size = get_size(component[number]) + get_size(component[next_number])
            if len(table_size_) == 0:
                leftcol, leftrow = 0, 1
                for item in component[number]:
                    col1, colnums1 = get_col(item)
                    leftcol += col1
                    # leftcolnums += colnums1
                    leftrow *= get_row(item)
                right_table_size = leftrow * rightrow * (leftcol + rightcol)
                table_size_ = [leftrow * rightrow, leftcol + rightcol]
            else:
                right_table_size = table_size_[0] * rightrow * (table_size_[1] + rightcol)
                table_size_ = [table_size_[0] * rightrow, table_size_[1] + rightcol]
            
            # right_table_row = row1 * row2
            if right_table_size < max_size:
            # if right_table_size < 43750000:
            # if right_table_row < 30000:
                for item in component[next_number]:
                    group.insert(len(group),item)
            else:
                partition.append(group)
                group = component[next_number]
                table_size_ = []
            number = next_number
        if len(group) > 1:
            partition.append(group)
        return partition

    
    # current_graph = graph.copy_graph()
    for component in connected_components:
        priority = ["size"]
        # min_cost = float('inf')
        # min_cost_preprocessing_graph = None
        for pri in priority: 
            best_partition = []
            print(pri,":")
            '''base2+slicing'''
            # groups, cost, case_update = dp_component1(component, table_size, dp_way="base2")
            # if groups is not None:
            #     remove_list = []
            #     for group in groups:
            #         if len(group) == 1 and case_update.get(group[0]):
            #             update_implement(graph, group[0])
            #             remove_list.append(group)
            #     groups = [group for group in groups if group not in remove_list]
            
            # final_graph = graph.copy_graph()
            # if best_partition:
            #     for partition in best_partition:
            #         final_graph = apply_fusion_plan(final_graph, partition)
            # current_graph = graph.copy_graph()       
            # groups_ = slicing_method_(groups, pri) 
            current_graph = graph.copy_graph()       
            groups_ = slicing_method_(component, pri) 
            for group in groups_:
                if len(group) > 1:
                    best_partition.append(group)
            if best_partition:
                for partition in best_partition:
                    current_graph = apply_fusion_plan(current_graph, partition)
            final_graph = current_graph
            
            # current_graph = apply_fusion_plan(graph.copy_graph(), groups_)
            # cost = get_encoderforge_graph_cost_multiv(current_graph, table_size)
            # print(cost)
        #     if cost < min_cost:
        #         min_cost_preprocessing_graph = current_graph
        #         min_cost = cost
        # if min_cost_preprocessing_graph is not None:        
        #     final_graph = min_cost_preprocessing_graph
        # else:
        #     final_graph = graph
    final_graph = current_graph

    return final_graph