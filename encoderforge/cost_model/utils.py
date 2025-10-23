import encoderforge.base.defs as defs
import encoderforge.cost_model.ml_model_space as hs
import pandas as pd
import h2o
import subprocess
import time
from autogluon.tabular import TabularDataset
# from encoderforge.transformer_manager import result
import csv
import numpy as np

MAX_INT64 = np.iinfo(np.int64).max

result = []
def tree_paths(tree, node=0, path=None):
    """
    Extract all paths from a decision tree
    """
    if path is None:
        path = []
    path.append(node)
    if tree.children_left[node] == tree.children_right[node]:  # Leaf nodes
        yield path
    else:
        yield from tree_paths(tree, tree.children_left[node], path.copy())
        yield from tree_paths(tree, tree.children_right[node], path.copy())


def calc_join_cost_by_train_data(data_rows, join_table_rows, join_table_columns):
    # pg
    if defs.DBMS == 'postgresql':
        # return data_rows * (9.08197566e-02)
        return data_rows * (4.43204396e-05 * join_table_rows + 9.08197566e-02 * join_table_columns)

    # duckdb
    elif defs.DBMS == 'duckdb':
        # return data_rows * ( 1.02417616e-05)
        return data_rows * (1.02122041e-08 * join_table_rows + 1.02417616e-05 * join_table_columns)

    # monetdb
    elif defs.DBMS == 'monetdb':
        # return data_rows * (1.67787246e-03)
        return data_rows * (4.58751378e-07 * join_table_rows + 1.67787246e-03 * join_table_columns)
    
    # clickhouse-tofix
    elif defs.DBMS == 'clickhouse':
        # 不准确，仅作为比较eb ab的参考
        return data_rows * (4.58751378e-07 * join_table_rows + 1.67787246e-03 * join_table_columns)

    # TODO: choose the accurate join cost model
    
    
def calc_ml_based_join_cost(left_table_row, left_table_col, right_table_row, right_table_col, left_col_size, right_col_size, left_table_size, right_table_size, flag=False):
    
    # Check for excessively large values
    large_value_detected = any([
        left_table_row > MAX_INT64,
        left_table_col > MAX_INT64,
        right_table_row > MAX_INT64,
        right_table_col > MAX_INT64,
        left_col_size > MAX_INT64,
        right_col_size > MAX_INT64,
        left_table_size > MAX_INT64,
        right_table_size > MAX_INT64
    ])
    
    # if large values return max num
    if large_value_detected:
        # log-
        cost_estimate = 10 * (np.log1p(float(left_table_size)) + np.log1p(float(right_table_size)))
        return max(1e20, cost_estimate)
    
    # to DataFrame，within float64
    data = {
        'left_table_row': [float(left_table_row)],
        'left_table_col': [float(left_table_col)],
        'right_table_row': [float(right_table_row)],
        'right_table_col': [float(right_table_col)],
        'left_col_size': [float(left_col_size)],
        'right_col_size': [float(right_col_size)],
        'left_table_size': [float(left_table_size)],
        'right_table_size': [float(right_table_size)]
    }
    
    # # to hf
    # data = {
    #     'left_table_row': [left_table_row],
    #     'left_table_col': [left_table_col],
    #     'right_table_row': [right_table_row],
    #     'right_table_col': [right_table_col],
    #     'left_col_size': [left_col_size],
    #     'right_col_size': [right_col_size],
    #     'left_table_size': [left_table_size],
    #     'right_table_size': [right_table_size]
    # }
    
    df = pd.DataFrame(data)
    
    # pg
    if defs.DBMS == 'postgresql':
        model_type = 'autogluon'
        if model_type == 'autogluon':
            if flag:
                prediction = hs.get_model(model_type, 'postgresql_sub').predict(TabularDataset(df))
            else:
                prediction = hs.get_model(model_type, 'postgresql').predict(TabularDataset(df))
            
        elif model_type == 'h2o':
            hf = h2o.H2OFrame(df)
            prediction = hs.get_model(model_type, 'postgresql').predict(hf)
        
        # with open('/result.csv', 'a', newline='', encoding='utf-8-sig') as f:
        #     writer = csv.writer(f)
        
        #     # write data for debug
        #     writer.writerow([data,prediction[0]])
        
        # return prediction.as_data_frame()['predict']
        return prediction[0]

    # duckdb
    elif defs.DBMS == 'duckdb':
        model_type = 'autogluon'
        if flag:
            prediction = hs.get_model(model_type ,'duckdb_sub').predict(TabularDataset(df))
        else:
            prediction = hs.get_model(model_type ,'duckdb').predict(TabularDataset(df))
        # return prediction.as_data_frame()['predict']
        return prediction[0]

    # monetdb
    elif defs.DBMS == 'monetdb':
        pass
    
    # clickhouse-tofix
    elif defs.DBMS == 'clickhouse':
        pass

    # TODO: choose the accurate join cost model



def get_pg_sql_cost(sql: str):
    # print(sql)
    try:
        output = subprocess.check_output(
            [
                "psql",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-c",
                "set max_parallel_workers_per_gather=3;" + sql,
            ],
            stderr=subprocess.STDOUT
        )
    except Exception as e:
        print(sql)
        exit
    
    execution_cost = None
    for line in output.decode().split("\n"):
        if "cost=" in line:
            execution_cost = float(line.split("..")[1].split()[0])
            break

    
    return execution_cost


def get_encoderforge_graph_cost(graph, data_rows):
    cost = 0
    timer = False
    
    if timer:
        t1 = time.time()
    
    # op cost
    for feature, chain in graph.chains.items():
        # if feature == 'nom_5':
        #     pass
        implements = graph.implements[feature]
        for idx, op in enumerate(chain.prep_operators):
            # case op cost
            if implements[idx] == defs.SQLPlanType.CASE:
                for feature_out in op.features_out:
                    cost += op._get_op_cost(feature_out)
                    
            # join op cost
            elif implements[idx] == defs.SQLPlanType.JOIN:
                for feature in op.features:
                    cost += op._get_join_cost_without_tree(feature, graph, data_rows)
    if timer:
        t2 = time.time()
        print(f'op cost time: {t2 - t1}s')
        
    if timer:
        t1 = time.time()
    
    # tree cost
    # for feature in graph.model.input_features:
    #     if graph.model.model_name in (
    #         defs.ModelName.DECISIONTREECLASSIFIER,
    #         defs.ModelName.RANDOMFORESTCLASSIFIER,
    #         defs.ModelName.DECISIONTREEREGRESSOR,
    #         defs.ModelName.RANDOMFORESTREGRESSOR,
    #     ):
    #         # if 'nom_5' in feature:
    #         #     pass
    #         tree_costs = graph.model.get_tree_costs_static(feature)
    #         total_tree_cost = sum(
    #             [tree_cost.calculate_tree_cost() for tree_cost in tree_costs]
    #         )
    #         cost += total_tree_cost
    if graph.model is None:
        pass
    
    elif graph.model.model_name in (
        defs.ModelName.DECISIONTREECLASSIFIER,
        defs.ModelName.RANDOMFORESTCLASSIFIER,
        defs.ModelName.DECISIONTREEREGRESSOR,
        defs.ModelName.RANDOMFORESTREGRESSOR,
    ):

        tree_costs = graph.model.get_tree_costs_static()
        total_tree_cost = sum(
            [tree_cost.calculate_tree_cost() for tree_cost in tree_costs]
        )
        cost += total_tree_cost
   
    if timer:
        t2 = time.time()
        print(f'tree cost time: {t2 - t1}s')
    
    # if timer:
    #     t1 = time.time()
        
    # # join op cost
    # for op in graph.join_operators:
    #     for feature in op.features:
    #         cost += op._get_join_cost_without_tree(feature, graph, data_rows)
    
    # if timer:
    #     t2 = time.time()
    #     print(f'join op cost time: {t2 - t1}s')    
    
    return cost
        
def get_encoderforge_graph_cost_multiv(graph, table_size):
    cost = 0
    timer = False
    
    if timer:
        t1 = time.time()
    flag = False
    merged_op_number = []
    
    # op cost
    for feature, chain in graph.chains.items():
        # if feature == 'nom_5':
        #     pass
        implements = graph.implements[feature]
        for idx, op in enumerate(chain.prep_operators):
            # case op cost
            if implements[idx] == defs.SQLPlanType.CASE:
                for feature_out in op.features_out:
                    cost += op._get_op_cost(feature_out)
                    
            # join op cost
            elif implements[idx] == defs.SQLPlanType.JOIN:
                if op.inter_merge == True:
                    merged_op = chain.merge_operators[idx]
                    if merged_op[0] not in merged_op_number:
                        merged_op_number.append(merged_op[0])
                        cost += merged_op[1]._get_join_cost_without_tree_new(feature, graph, table_size, flag)
                else:         
                    for feature in op.features:
                        cost += op._get_join_cost_without_tree_new(feature, graph, table_size, flag)
        flag = True
    if timer:
        t2 = time.time()
        print(f'op cost time: {t2 - t1}s')
        
    if timer:
        t1 = time.time()
        
    if graph.model is None:
        pass
    
    elif graph.model.model_name in (
        defs.ModelName.DECISIONTREECLASSIFIER,
        defs.ModelName.RANDOMFORESTCLASSIFIER,
        defs.ModelName.DECISIONTREEREGRESSOR,
        defs.ModelName.RANDOMFORESTREGRESSOR,
    ):

        tree_costs = graph.model.get_tree_costs_static()
        total_tree_cost = sum(
            [tree_cost.calculate_tree_cost() for tree_cost in tree_costs]
        )
        cost += total_tree_cost
   
    if timer:
        t2 = time.time()
        print(f'tree cost time: {t2 - t1}s')
        
    return cost
     
def get_r(op):
    row = 1
    for i in range(len(op)):
        if hasattr(op[i][1],'mapping'):
            row = row * op[i][1].mapping.shape[0]
        elif hasattr(op[i][1],'mappings'):
            row = row * len(op[i][1].mappings[0])          
    # kuan, lieshu 

    return row
                
def get_l(op):
    length = 0
    for i in range(len(op)):
        if hasattr(op[i][1],'mapping'):
            length += op[i][1].mapping.index.dtype.itemsize
            for dtype in op[i][1].mapping.dtypes:
                length += dtype.itemsize
            return length
        elif hasattr(op[i][1],'mappings'):
            length += op[i][1].mappings[0].index.dtype.itemsize 
            length += op[i][1].mappings[0].dtype.itemsize  
    return length

def get_size(op):
    r = get_r(op)
    l = get_l(op)
    return r * l

def get_row(op):
    if hasattr(op[1],'mapping'):
        return op[1].mapping.shape[0]
    elif hasattr(op[1],'mappings'):
        return len(op[1].mappings[0])
    else:
        row = 1
        for item in op:
            row = row * get_row(item)
            
    # kuan, lieshu 

    return row
                
def get_col(op):
    if hasattr(op[1],'mapping'):
        length = op[1].mapping.index.dtype.itemsize
        num = 1
        for dtype in op[1].mapping.dtypes:
            # if dtype == 'int64':
            #     length += 4
            # elif dtype == 'float':
            #     length += 8
            # else:
            #     print('error!')
            length += dtype.itemsize
            num += 1
        return length, num
    elif hasattr(op[1],'mappings'):
        return op[1].mappings[0].dtype.itemsize + op[1].mappings[0].index.dtype.itemsize, 2  #字节大小
    else:
        length = 0
        num = 0
        for item in op:
            length_,num_ = get_col(item)
            length += length_
            num += num_
            return length,num
       
  
def get_group_cost(fusion_group, table_size, flag):
    cost = 0
    right_rows = 1 
    right_columns = 0 
    right_col = 0
    for item in fusion_group:
        row = get_row(item) #
        col_size, col = get_col(item)
        # size = row * col
        # if item[1].op_type == defs.OperatorType.EXPAND:
        #     rows_, columns_ = item[1].mapping.shape
        # elif item[1].op_type == defs.OperatorType.CAT_C_CAT:
        #     rows_ = item[1].mappings[0].size
        #     columns_ = 1
        right_rows = right_rows * row
        right_columns = right_columns + col_size # columns * each length
        right_col += col
    right_size = right_rows * right_columns

# left_table_row, "left_table_col", right_table_row, "right_table_col", "left_col_size", right_col_size, "left_table_size", right_table_size
    cost = calc_ml_based_join_cost(table_size[0],table_size[1], right_rows, right_col,table_size[2], right_columns, table_size[3], right_size, flag)

    # cost = calc_join_cost_by_train_data(table_size[0], right_rows, right_col)
    # return cost[0]
    return cost