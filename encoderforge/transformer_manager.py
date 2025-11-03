import time
import copy
import pickle
import os
import re
import sys
import random
from concurrent.futures import ProcessPoolExecutor

from encoderforge.base.graph import PrepGraph
from encoderforge.base.plan import ChainCandidateFusionPlans, ChainCandidateImplementPlans
from encoderforge.optimizer.merge import *
from encoderforge.cost_model.merge import merge_by_cost_model
from encoderforge.cost_model.utils import get_pg_sql_cost, get_encoderforge_graph_cost, get_encoderforge_graph_cost_multiv, get_group_cost
from encoderforge.utility.loader import load_model
from encoderforge.utility.dbms_utils import DBMSUtils
import encoderforge.base.defs as defs
from encoderforge.preprocess.k_bins_discretizer import KBinsDiscretizerSQLOperator
from encoderforge.preprocess.one_hot_encoder import OneHotEncoderSQLOperator
from encoderforge.preprocess.binary_encoder import BinaryEncoderSQLOperator
from collections import defaultdict
from encoderforge.cost_model.ml_model_space import load_h2o_context, load_autoluon
from encoderforge.optimizer.dp import *
from encoderforge.base.func import mapping_key

def task(
    all_implements_plans,
    all_fusion_plans,
    preprocessing_graph,
    cost_model,
    data_rows,
    table_name,
    dbms,
    pre_sql,
    pipeline,
):
    min_cost = float('inf')
    plan_num = 0
    min_cost_preprocessing_graph = None
    for graph_fusion_plan in all_fusion_plans:
        for graph_implement_plan in all_implements_plans:
            preprocessing_graph_list = merge_sql_operator_by_graph_plan(preprocessing_graph, graph_implement_plan, graph_fusion_plan)
            plan_num = plan_num + len(preprocessing_graph_list)
            for graph in preprocessing_graph_list:
                if cost_model == 'encoderforge':
                    cost = get_encoderforge_graph_cost(graph, data_rows)
                elif cost_model == 'postgresql':
                    mng = TransformerManager()
                    query_str = mng.__compose_sql(graph, table_name, dbms, pre_sql, pipeline)
                    cost = get_pg_sql_cost(query_str)
                if cost < min_cost:
                    min_cost_preprocessing_graph = graph
                    min_cost = cost
    return min_cost_preprocessing_graph, min_cost, plan_num


class TransformerManager(object):

    def __extract_pipeline(self, pipeline):
        steps = pipeline.steps
        column_transformer_start_idx = 0

        fitted_imputer = None
        # if contain imputer extract it
        if 'Imputer' == steps[0][0]:
            fitted_imputer = steps[0][1]
            column_transformer_start_idx = 1

        # extract model
        model_name, trained_model = steps[-1]

        # extract preprocessors
        transforms = []
        for i in range(column_transformer_start_idx, len(steps) - 1):
            _, pipeline_transformers = steps[i]
            for idx in range(len(pipeline_transformers.transformers)):
                a, b, c = pipeline_transformers.transformers_[idx]
                a = a.split('_')[0]
                transforms.append({
                    'transform_name': a,
                    'fitted_transform': b,
                    'transform_features': c,
                })

        return {
            'imputer': {
                'filled_values': fitted_imputer.statistics_ if fitted_imputer else [],
                'missing_cols': fitted_imputer.missing_cols if fitted_imputer else [],
                'missing_col_indexs': fitted_imputer.missing_col_indexs if fitted_imputer else []
            },
            'transforms': transforms,
            'model': {
                'model_name': model_name,
                'trained_model': trained_model
            }
        }

    def generate_query(
        self,
        model_file,
        table_name,
        dbms,
        *,
        just_push_flag=False,
        masq=False,
        pre_sql=None,
        order_when=False,
        group='prune',
        cost_model='encoderforge',
        max_process_num=1,
        dp_way="origin",
        assigned_rule=None,
        single_sql_gen=False,
        pipeline_no=None,
        join_method=None
    ):
        assert group in ('case_base','separate', 'pos', 'uncertain', 'enum', 'prune', 'blue_elephants','inter', 'intra','composite','dp','sort','slicing','enumerate'), "group must in ('case_base','separate', 'pos', 'uncertain', 'enum', 'prune', 'blue_elephants','inter', 'intra','composite','dp','sort','slicing','enumerate')"
        assert cost_model in ('encoderforge', 'postgresql'), "cost_model must in ('encoderforge', 'postgresql')"

        # init h2o,autogluon context
        load_h2o_context()
        load_autoluon()

        # some load and extract tasks
        defs.DBMS = dbms
        defs.set_JUST_PUSH_FLAG(just_push_flag)
        defs.ORDER_WHEN = order_when
        defs.GROUP = group
        defs.MASQ = masq
        defs.TABLE_NAME = table_name

        model = load_model(model_file)
        pipeline_features_in = model.feature_names_in_.tolist()
        defs.PIPELINE_FEATURES_IN = pipeline_features_in
        pipeline = self.__extract_pipeline(model)
        data_rows = model.data_rows
        model_name = pipeline['model']['model_name']
        # build the graph of the preprocessing operators
        preprocessing_graph = PrepGraph(pipeline_features_in, pipeline)

        # for every implement plan, use different fusion plan, get the execute plan
        # calculate the cost of every execture plan, get the min cost plan
        min_cost = float("inf")
        
        left_rows = 100000000
        left_cols = len(pipeline_features_in)
        left_cols_size = 0
        
        def get_index_size(index):
            sizes = []
            for i in index:
                if isinstance(i, str):
                    size = len(i.encode('utf-8')) * 2
                    if size % 5 != 0:
                        size = ((size + 4) // 5) * 5
                    sizes.append(size)
                elif isinstance(i, int):
                    sizes.append((i.bit_length() + 7) // 8)
                else:
                    sizes.append(None)
            return max(sizes)
        
        for op in preprocessing_graph.chains.items():
            op_detail = op[1].prep_operators[0]
            if op_detail._get_op_type in ('CAT_C_CAT', 'EXPAND'):
                if hasattr(op_detail, 'mapping'):
                    df = op_detail.mapping
                    left_cols_size += get_index_size(df.index.to_series())
                else:
                    left_cols_size += op_detail.mappings[0].dtypes.itemsize
            else:
                left_cols_size += 1
            
        left_size = left_rows * left_cols_size
        table_size = [left_rows, left_cols, left_cols_size, left_size]
        
        defs.use_cut = True
        if preprocessing_graph.model is not None:
            filter_features = self.extract_columns_from_sql(preprocessing_graph.model.query("", dbms), preprocessing_graph)   # analysis used columns in model    
            preprocessing_graph = preprocessing_graph.graph_feature_used(filter_features)
        else:
            preprocessing_graph = preprocessing_graph.implemente_mergeop()
        
        
        # all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
        # for feature, chain in preprocessing_graph.chains.items():
        #     all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain))
        t0 = time.time()

        if group == 'case_base':
            # reuse_flag = False
            # if reuse_flag and os.path.exists(f'{table_name}_{model_name}_{dbms}_org.pkl'):
            #     with open(f'{table_name}_{model_name}_{dbms}_org.pkl', 'rb') as f:
            #         min_cost_preprocessing_graph = pickle.load(f)

            # else:
            # enumerate the chain implement plans
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            for graph_implement_plan in enumerate_graph_implement_plans():
                preprocessing_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                cost = 0
                if cost_model == 'encoderforge':
                    cost = get_encoderforge_graph_cost(preprocessing_graph, data_rows)
                elif cost_model == 'postgresql':
                    query_str = self.__compose_sql(preprocessing_graph, table_name, dbms, pre_sql, pipeline)
                    cost = get_pg_sql_cost(query_str)
                if cost < min_cost:
                    min_cost_preprocessing_graph = preprocessing_graph
                    min_cost = cost
            searching_time_end = time.time()
            print(f'plan searching time: {(searching_time_end - t0):.4f} s', flush = True)    

            # with open(f'{table_name}_{model_name}_{dbms}_org.pkl', 'wb') as f:
            #     pickle.dump(min_cost_preprocessing_graph, f)

            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            print(f'case_base sql generate time: {(t2 - t1):.4f} s', flush=True)    

        if group == 'separate':
            # reuse_flag = False
            # if reuse_flag and os.path.exists(f'{table_name}_{model_name}_{dbms}_org.pkl'):
            #     with open(f'{table_name}_{model_name}_{dbms}_org.pkl', 'rb') as f:
            #         min_cost_preprocessing_graph = pickle.load(f)

            # else:
            # enumerate the chain implement plans
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain, "join"))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            for graph_implement_plan in enumerate_graph_implement_plans():
                preprocessing_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                cost = 0
                if cost_model == 'encoderforge':
                    cost = get_encoderforge_graph_cost(preprocessing_graph, data_rows)
                elif cost_model == 'postgresql':
                    query_str = self.__compose_sql(preprocessing_graph, table_name, dbms, pre_sql, pipeline)
                    cost = get_pg_sql_cost(query_str)
                if cost < min_cost:
                    min_cost_preprocessing_graph = preprocessing_graph
                    min_cost = cost
            
            searching_time_end = time.time()
            print(f'plan searching time: {(searching_time_end - t0):.4f} s', flush = True)    

            # with open(f'{table_name}_{model_name}_{dbms}_org.pkl', 'wb') as f:
            #     pickle.dump(min_cost_preprocessing_graph, f)

            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            print(f'separate sql generate time: {(t2 - t1):.4f} s', flush=True)    

        if group == 'pos':
            # if os.path.exists(f'{table_name}_{model_name}_{dbms}_org.pkl'):
            #     with open(f'{table_name}_{model_name}_{dbms}_org.pkl', 'rb') as f:
            #         min_cost_preprocessing_graph = pickle.load(f)
            # else:
            # enumerate the chain implement plans
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            for graph_implement_plan in enumerate_graph_implement_plans():
                preprocessing_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                if cost_model == 'encoderforge':
                    cost = get_encoderforge_graph_cost(preprocessing_graph, data_rows)
                elif cost_model == 'postgresql':
                    query_str = self.__compose_sql(preprocessing_graph, table_name, dbms, pre_sql, pipeline)
                    cost = get_pg_sql_cost(query_str)
                if cost < min_cost:
                    min_cost_preprocessing_graph = preprocessing_graph
                    min_cost = cost
            min_cost_preprocessing_graph = merge_sql_operator_by_benifit_rules(min_cost_preprocessing_graph)
            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            print(f'pos sql generate time: {(t2 - t1):.4f} s', flush=True)
            # with open(f'{table_name}_{model_name}_{dbms}_pos.pkl', 'wb') as f:
            #     pickle.dump(min_cost_preprocessing_graph, f)

        if group == 'uncertain':
            # if os.path.exists(f'{table_name}_{model_name}_{dbms}_pos.pkl'):
            #     with open(f'{table_name}_{model_name}_{dbms}_pos.pkl', 'rb') as f:
            #         min_cost_preprocessing_graph = pickle.load(f)
            # elif os.path.exists(f'{table_name}_{model_name}_{dbms}_org.pkl'):
            #     with open(f'{table_name}_{model_name}_{dbms}_org.pkl', 'rb') as f:
            #         min_cost_preprocessing_graph = pickle.load(f)
            #     min_cost_preprocessing_graph = merge_sql_operator_by_benifit_rules(min_cost_preprocessing_graph)
            # else:
            # enumerate the chain implement plans
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            for graph_implement_plan in enumerate_graph_implement_plans():
                preprocessing_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                if cost_model == 'encoderforge':
                    cost = get_encoderforge_graph_cost(preprocessing_graph, data_rows)
                elif cost_model == 'postgresql':
                    query_str = self.__compose_sql(preprocessing_graph, table_name, dbms, pre_sql, pipeline)
                    cost = get_pg_sql_cost(query_str)
                if cost < min_cost:
                    min_cost_preprocessing_graph = preprocessing_graph
                    min_cost = cost
            min_cost_preprocessing_graph = merge_sql_operator_by_benifit_rules(min_cost_preprocessing_graph)

            min_cost_preprocessing_graph = merge_sql_operator_by_uncertain_rules(min_cost_preprocessing_graph)
            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            print(f'uncertain sql generate time: {(t2 - t1):.4f} s', flush=True)

        if group == 'enum':
            concurrent_flag = True

            # enumerate the chain implement plans
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            # enumerate the chain fusion plans
            all_chain_candidate_fusion_plans: list[ChainCandidateFusionPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_fusion_plans.append(ChainCandidateFusionPlans(feature, chain))

            def enumerate_graph_fusion_plans(index=0, current_graph_fusion_plan:list = []):
                if index == len(all_chain_candidate_fusion_plans):
                    yield copy.deepcopy(current_graph_fusion_plan)
                    return
                for chain_fusion_plan in all_chain_candidate_fusion_plans[index].candidate_fusion_plans:
                    current_graph_fusion_plan.append(chain_fusion_plan)
                    yield from enumerate_graph_fusion_plans(index + 1, current_graph_fusion_plan)
                    current_graph_fusion_plan.pop()

            total_plan_num = 0

            if max_process_num == 1:
                for graph_implement_plan in enumerate_graph_implement_plans():
                    for graph_fusion_plan in enumerate_graph_fusion_plans():
                        preprocessing_graph_list = merge_sql_operator_by_graph_plan(preprocessing_graph, graph_implement_plan, graph_fusion_plan)
                        total_plan_num = total_plan_num + len(preprocessing_graph_list)
                        for graph in preprocessing_graph_list:
                            if cost_model == 'encoderforge':
                                cost = get_encoderforge_graph_cost(graph, data_rows)
                            elif cost_model == 'postgresql':
                                query_str = self.__compose_sql(graph, table_name, dbms, pre_sql, pipeline)
                                cost = get_pg_sql_cost(query_str)
                            if cost < min_cost:
                                min_cost_preprocessing_graph = graph
                                min_cost = cost

            else:
                all_graph_implements_plans = []
                for graph_implement_plan in enumerate_graph_implement_plans():
                    all_graph_implements_plans.append(graph_implement_plan)

                all_graph_fusion_plans = []
                for graph_fusion_plan in enumerate_graph_fusion_plans():
                    all_graph_fusion_plans.append(graph_fusion_plan)

                max_workers_num = max_process_num
                graph_fusion_plan_num = len(all_graph_fusion_plans)
                with ProcessPoolExecutor(max_workers=max_workers_num) as executor:
                    futures = []
                    if graph_fusion_plan_num >= max_workers_num:
                        per_worker_plan_num = graph_fusion_plan_num // max_workers_num
                        remain_plan_num = graph_fusion_plan_num % max_workers_num
                        begin_plan_idx = 0
                        for i in range(remain_plan_num):
                            futures.append(
                                executor.submit(
                                    task,
                                    all_graph_implements_plans,
                                    all_graph_fusion_plans[begin_plan_idx: begin_plan_idx + per_worker_plan_num + 1],
                                    preprocessing_graph.copy_graph(),
                                    cost_model,
                                    data_rows,
                                    table_name,
                                    dbms,
                                    pre_sql,
                                    pipeline,
                                )
                            )
                            begin_plan_idx = begin_plan_idx + per_worker_plan_num + 1
                        if per_worker_plan_num > 0:
                            for i in range(max_workers_num - remain_plan_num):
                                futures.append(
                                    executor.submit(
                                        task,
                                        all_graph_implements_plans,
                                        all_graph_fusion_plans[begin_plan_idx: begin_plan_idx + per_worker_plan_num],
                                        preprocessing_graph.copy_graph(),
                                        cost_model,
                                        data_rows,
                                        table_name,
                                        dbms,
                                        pre_sql,
                                        pipeline,
                                    )
                                )
                                begin_plan_idx = begin_plan_idx + per_worker_plan_num
                    else:
                        per_plan_worker_num = max_workers_num // graph_fusion_plan_num
                        remain_plan_num = max_workers_num % graph_fusion_plan_num
                        plan_worker_nums = [per_plan_worker_num] * (graph_fusion_plan_num - remain_plan_num) + [per_plan_worker_num + 1] * remain_plan_num
                        graph_implement_plan_num = len(all_graph_implements_plans)
                        for i, woker_num in enumerate(plan_worker_nums):
                            per_worker_implement_plan_num =  graph_implement_plan_num // woker_num
                            remain_implement_plan_num = graph_implement_plan_num % woker_num
                            begin_implement_plan_idx = 0
                            for j in range(remain_implement_plan_num):
                                futures.append(
                                    executor.submit(
                                        task,
                                        all_graph_implements_plans[begin_implement_plan_idx: begin_implement_plan_idx + per_worker_implement_plan_num + 1],
                                        all_graph_fusion_plans[i: i + 1],
                                        preprocessing_graph.copy_graph(),
                                        cost_model,
                                        data_rows,
                                        table_name,
                                        dbms,
                                        pre_sql,
                                        pipeline,
                                    )
                                )
                                begin_implement_plan_idx = begin_implement_plan_idx + per_worker_implement_plan_num + 1
                            if per_worker_implement_plan_num > 0:
                                for j in range(woker_num - remain_implement_plan_num):
                                    futures.append(
                                        executor.submit(
                                                task,
                                                all_graph_implements_plans[begin_implement_plan_idx: begin_implement_plan_idx + per_worker_implement_plan_num],
                                                all_graph_fusion_plans[i: i + 1],
                                                preprocessing_graph.copy_graph(),
                                                cost_model,
                                                data_rows,
                                                table_name,
                                                dbms,
                                                pre_sql,
                                                pipeline,
                                            )
                                    )
                                    begin_implement_plan_idx = begin_implement_plan_idx + per_worker_implement_plan_num    
                        
                    for future in futures:
                        try:
                            concurrent_min_cost_preprocessing_graph, concurrent_min_cost, process_plan_num = future.result()
                        except Exception as e:
                            print(f"Future raised an exception: {e}")
                        
                        total_plan_num += process_plan_num
                        if concurrent_min_cost < min_cost:
                            min_cost_preprocessing_graph = concurrent_min_cost_preprocessing_graph
                            min_cost = concurrent_min_cost  
            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            print(f'enum plan num: {total_plan_num}', flush=True)
            print(f'enum sql generate time: {(t2 - t1):.4f} s', flush=True)

        if group == 'prune':
            # enumerate the chain implement plans
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            # enumerate the chain fusion plans
            all_chain_candidate_fusion_plans: list[ChainCandidateFusionPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_fusion_plans.append(ChainCandidateFusionPlans(feature, chain))

            def enumerate_graph_fusion_plans(index=0, current_graph_fusion_plan:list = []):
                if index == len(all_chain_candidate_fusion_plans):
                    yield copy.deepcopy(current_graph_fusion_plan)
                    return
                for chain_fusion_plan in all_chain_candidate_fusion_plans[index].candidate_fusion_plans:
                    current_graph_fusion_plan.append(chain_fusion_plan)
                    yield from enumerate_graph_fusion_plans(index + 1, current_graph_fusion_plan)
                    current_graph_fusion_plan.pop()

            total_plan_num = 0        
            last_chain_min_cost_preprocessing_graph = preprocessing_graph.copy_graph()
            for chain_candidate_implement_plans, chain_candidate_fusion_plans, feature in zip(
                all_chain_candidate_implement_plans,
                all_chain_candidate_fusion_plans,
                preprocessing_graph.chains.keys(),
            ):
                for chain_implement_plan in chain_candidate_implement_plans.candidate_implement_plans:
                    for chain_fusion_plan in chain_candidate_fusion_plans.candidate_fusion_plans:
                        if feature == 'hours-per-week':
                            pass
                        preprocessing_graph_list = merge_sql_operator_by_chain_plan(last_chain_min_cost_preprocessing_graph, chain_implement_plan, chain_fusion_plan, feature, assigned_rule)
                        
                        total_plan_num += len(preprocessing_graph_list)
                        # print(f'current plan num: {total_plan_num}')
                        # t1 = time.time()
                        for graph in preprocessing_graph_list:
                            if defs.MASQ:
                                min_cost_preprocessing_graph = graph
                            else:
                                if cost_model == 'encoderforge':
                                    cost = get_encoderforge_graph_cost(graph, data_rows)
                                elif cost_model == 'postgresql':
                                    query_str = self.__compose_sql(graph, table_name, dbms, pre_sql, pipeline)
                                    cost = get_pg_sql_cost(query_str)
                                if cost < min_cost:
                                    min_cost_preprocessing_graph = graph
                                    min_cost = cost
                        # t2 = time.time()
                        # print(f'{len(preprocessing_graph_list)} plans\' cost evaluate time: {(t2 - t1):.4f} s', flush=True)
                last_chain_min_cost_preprocessing_graph = min_cost_preprocessing_graph.copy_graph()

            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            print(f'prune plan num: {total_plan_num}', flush=True)
            print(f'prune sql generate time: {(t2 - t1):.4f} s', flush=True)

        if group == 'blue_elephants':
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain, "join"))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            for graph_implement_plan in enumerate_graph_implement_plans():
                preprocessing_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                cost = 0
                if cost_model == 'encoderforge':
                    cost = get_encoderforge_graph_cost(preprocessing_graph, data_rows)
                elif cost_model == 'postgresql':
                    query_str = self.__compose_sql(preprocessing_graph, table_name, dbms, pre_sql, pipeline)
                    cost = get_pg_sql_cost(query_str)
                if cost < min_cost:
                    min_cost_preprocessing_graph = preprocessing_graph
                    min_cost = cost
            
            searching_time_end = time.time()
            print(f'plan searching time: {(searching_time_end - t0):.4f} s', flush = True)    

            # with open(f'{table_name}_{model_name}_{dbms}_org.pkl', 'wb') as f:
            #     pickle.dump(min_cost_preprocessing_graph, f)

            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline, True)
            t2 = time.time()
            print(f'blue_elephants sql generate time: {(t2 - t1):.4f} s', flush=True)    

        if group == 'inter':
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain,"join"))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            for graph_implement_plan in enumerate_graph_implement_plans():
                preprocessing_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                if cost_model == 'encoderforge':
                    cost = get_encoderforge_graph_cost(preprocessing_graph, data_rows)
                elif cost_model == 'postgresql':
                    query_str = self.__compose_sql(preprocessing_graph, table_name, dbms, pre_sql, pipeline)
                    cost = get_pg_sql_cost(query_str)
                if cost < min_cost:
                    min_cost_preprocessing_graph = preprocessing_graph
                    min_cost = cost
                   
            # min_cost_preprocessing_graph = merge_inter_operator(min_cost_preprocessing_graph, dbms)
            min_cost_preprocessing_graph = merge_inter_operator(min_cost_preprocessing_graph)
                
            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            print(f'pos sql generate time: {(t2 - t1):.4f} s', flush=True)
            # with open(f'{table_name}_{model_name}_{dbms}_pos.pkl', 'wb') as f:
            #     pickle.dump(min_cost_preprocessing_graph, f)
        
        if group == 'intra':
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain,"join"))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            min_cost = float('inf')
            min_cost_preprocessing_graph = None
            total_plan_num = 0
                        
            #  implement  fusion 
            for graph_implement_plan in enumerate_graph_implement_plans():
                #  implement
                current_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                current_graph = fs_fusion(current_graph) # all_merge
                cost = get_encoderforge_graph_cost(current_graph, data_rows)
                if cost < min_cost:
                    min_cost_preprocessing_graph = current_graph
                    min_cost = cost
                
            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline) #！！
            t2 = time.time()
            print(f'intra sql generate time: {(t2 - t1):.4f} s', flush=True)

        if group == 'composite':
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain,"join"))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            min_cost = float('inf')
            min_cost_preprocessing_graph = None
            total_plan_num = 0
                        
            #  implement  fusion 
            for graph_implement_plan in enumerate_graph_implement_plans():
                #  implement
                current_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                # current_graph = fs_fusion(current_graph) # intra_merge
                # inter_merge_all
                current_graph = ss_fusion(current_graph)
                
                cost = get_encoderforge_graph_cost(current_graph, data_rows)
                if cost < min_cost:
                    min_cost_preprocessing_graph = current_graph
                    min_cost = cost
            
            searching_time_end = time.time()
            print(f'plan searching time: {(searching_time_end - t0):.4f} s', flush = True)    
                
            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            print(f'intra sql generate time: {(t2 - t1):.4f} s', flush=True)

        if group == 'dp':
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain,"join"))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            min_cost = float('inf')
            min_cost_preprocessing_graph = None
            total_plan_num = 0
            for graph_implement_plan in enumerate_graph_implement_plans():
                current_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                min_cost_preprocessing_graph = dp_join_fusion(current_graph, table_size, dp_way)
                
            
            searching_time_end = time.time()
            print(f'plan searching time: {(searching_time_end - t0):.4f} s', flush = True)    
            
            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            # print(f'twostate plan num: {total_plan_num if not use_dp else "DP"}', flush=True)
            print(f'{dp_way} sql generate time: {(t2 - t1):.4f} s', flush=True)

        if group == 'enumerate':
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain, "join_case"))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            min_cost = float('inf')
            min_cost_preprocessing_graph = None
            total_plan_num = 0
            
            #  implement  fusion 
            for graph_implement_plan in enumerate_graph_implement_plans():
                #  implement
                current_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                
                # current_graph = fs_fusion(current_graph) # all_merge
                
                '''stage2 enumerate'''
                # 
                def enumerate_join_fusion_plans(graph: PrepGraph):
                    """ join ，"""
                    op_nodes, op_graph = legal_joins(graph)
                    connected_components = find_connected_components(op_nodes, op_graph)
                        
                    for component in connected_components:
                        # 1
                        if len(component) >= 2:
                            all_partitions = []
                            for partition in generate_bell_partitions(component):
                                valid_groups = [group for group in partition if len(group) >= 2]
                                if valid_groups:
                                    all_partitions.append(valid_groups)
                        
                            all_partitions.sort(key=lambda x: max(len(group) for group in x))
                            # yield
                            for partition in all_partitions:
                                yield partition
                    
                # 
                merged_graph = current_graph
                total_plan_num += 1
                if cost_model == 'encoderforge':
                    cost = get_encoderforge_graph_cost_multiv(merged_graph, table_size)
                elif cost_model == 'postgresql':
                    query_str = self.__compose_sql(merged_graph, table_name, dbms, pre_sql, pipeline)
                    cost = get_pg_sql_cost(query_str)
                        
                if cost < min_cost:
                    min_cost_preprocessing_graph = merged_graph
                    min_cost = cost
                    
                for fusion_plan in enumerate_join_fusion_plans(current_graph):
                    merged_graph = apply_fusion_plan(current_graph, fusion_plan)
                    total_plan_num += 1
                        
                    #  tofix 
                    if cost_model == 'encoderforge':
                        # if len(fusion_plan) == 1:
                        #     pass
                        cost = get_encoderforge_graph_cost_multiv(merged_graph, table_size)
                        # print(total_plan_num,"merged_best",cost)
                    elif cost_model == 'postgresql':
                        query_str = self.__compose_sql(merged_graph, table_name, dbms, pre_sql, pipeline)
                        cost = get_pg_sql_cost(query_str)
                        
                    if cost < min_cost:
                        min_cost_preprocessing_graph = merged_graph
                        min_cost = cost
                        
            searching_time_end = time.time()
            print(f'plan searching time: {(searching_time_end - t0):.4f} s', flush = True)    
            
            
            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            # print(f'twostate plan num: {total_plan_num if not use_dp else "DP"}', flush=True)
            print(f'enumerate sql generate time: {(t2 - t1):.4f} s', flush=True)

        if group == 'sort':
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain,"join"))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            min_cost = float('inf')
            min_cost_preprocessing_graph = None
            total_plan_num = 0
            
            #  implement  fusion 
            for graph_implement_plan in enumerate_graph_implement_plans():
                #  implement
                current_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                
                # current_graph = fs_fusion(current_graph) # all_merge
                
                min_cost_preprocessing_graph = sort_join_fusion(current_graph, table_size)
            
            searching_time_end = time.time()
            print(f'plan searching time: {(searching_time_end - t0):.4f} s', flush = True)    
            
            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            # print(f'slicing plan num: {total_plan_num if not  else "DP"}', flush=True)
            print(f'sort sql generate time: {(t2 - t1):.4f} s', flush=True)

        if group == 'slicing':
            all_chain_candidate_implement_plans: list[ChainCandidateImplementPlans] = []
            for feature, chain in preprocessing_graph.chains.items():
                all_chain_candidate_implement_plans.append(ChainCandidateImplementPlans(feature, chain,"join"))

            def enumerate_graph_implement_plans(index=0, current_graph_implement_plan:list = []):
                if index == len(all_chain_candidate_implement_plans):
                    yield copy.deepcopy(current_graph_implement_plan) 
                    return
                for chain_implement_plan in all_chain_candidate_implement_plans[index].candidate_implement_plans:
                    current_graph_implement_plan.append(chain_implement_plan)
                    yield from enumerate_graph_implement_plans(index + 1, current_graph_implement_plan)
                    current_graph_implement_plan.pop()

            min_cost = float('inf')
            min_cost_preprocessing_graph = None
            total_plan_num = 0
            
            #  implement  fusion 
            for graph_implement_plan in enumerate_graph_implement_plans():
                #  implement
                current_graph = implement_operator_by_plan(preprocessing_graph, graph_implement_plan)
                
                # current_graph = fs_fusion(current_graph) # all_merge
                
                min_cost_preprocessing_graph = slicing_join_fusion(current_graph, table_size)
            
            searching_time_end = time.time()
            print(f'plan searching time: {(searching_time_end - t0):.4f} s', flush = True)    

            t1 = time.time()
            min_cost_query_str = self.__compose_sql(min_cost_preprocessing_graph, table_name, dbms, pre_sql, pipeline)
            t2 = time.time()
            # print(f'slicing plan num: {total_plan_num if not  else "DP"}', flush=True)
            print(f'slicing sql generate time: {(t2 - t1):.4f} s', flush=True)

        if single_sql_gen:
            for feature, _ in min_cost_preprocessing_graph.chains.items():
                if 'KBinsDiscretizer' == min_cost_preprocessing_graph.chains[feature].prep_operators[0].op_name.value or min_cost_preprocessing_graph.chains[feature].prep_operators[0].is_encoder == False:
                    continue
                single_feature_graph = min_cost_preprocessing_graph.get_empty_chains_graph()
                single_feature_graph.chains[feature].prep_operators.append(min_cost_preprocessing_graph.chains[feature].prep_operators[0])
                single_feature_graph.implements[feature].append(min_cost_preprocessing_graph.implements[feature][0])
                single_feature_sql = self.__compose_sql(single_feature_graph, table_name, dbms, pre_sql, pipeline, single_sql_gen=single_sql_gen, join_method=join_method)
                generated_file_path = os.path.join(defs.single_op_sql_parent_dir, f"{pipeline_no}%{group}%{feature}%{join_method}.sql")
                with open('/home/jqy/single_op_desc.csv', "a") as csv_file:
                    if hasattr(min_cost_preprocessing_graph.chains[feature].prep_operators[0], 'mapping'):
                        unique_count = len(min_cost_preprocessing_graph.chains[feature].prep_operators[0].mapping)
                        csv_file.write(f'{pipeline_no},{feature},{min_cost_preprocessing_graph.chains[feature].prep_operators[0].op_name.value},{unique_count},"{min_cost_preprocessing_graph.chains[feature].prep_operators[0].mapping.index[random.randint(0, unique_count-1)]}"\n')
                    elif hasattr(min_cost_preprocessing_graph.chains[feature].prep_operators[0], 'mappings'):
                        unique_count = len(min_cost_preprocessing_graph.chains[feature].prep_operators[0].mappings[0])
                        csv_file.write(f'{pipeline_no},{feature},{min_cost_preprocessing_graph.chains[feature].prep_operators[0].op_name.value},{unique_count},"{min_cost_preprocessing_graph.chains[feature].prep_operators[0].mappings[0].index[random.randint(0, unique_count-1)]}"\n')
                with open(generated_file_path, "w") as sql_file:
                    sql_file.write(pre_sql+single_feature_sql)
        else:
            return min_cost_query_str
        

    """extract used columns from tree/lr SQL"""
    def extract_columns_from_sql(self, sql, graph):
        used_columns = set()
        graph = join_the_operators(graph)
        if (defs.use_cut):
        # if (SQLPlanType.JOIN in graph.implements):
            # WHEN pattern
            when_pattern = r'WHEN\s+"([^"]+)"'
            when_matches = re.findall(when_pattern, sql)
            for col in when_matches:
                used_columns.add(col)
                
            # weight * "column" pattern
            lr_pattern = r'"([^"]+)"\s*\*\s*[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?'
            lr_matches = re.findall(lr_pattern, sql)
            for col, _ in lr_matches:
                used_columns.add(col)
        else:
            if(graph.model.model_name == ModelName.DECISIONTREEREGRESSOR or graph.model.model_name == ModelName.DECISIONTREECLASSIFIER):
                used_columns = set(graph.model.tree_node_mappings.keys())
            else:
                used_columns = set(graph.model.input_features)

        # new_set prefix
        # additional_columns = set()
        column_dict = dict()

        for col in used_columns:
            parts = col.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                prefix = '_'.join(parts[:-1])
                column_dict.setdefault(prefix, []).append(col)   
            column_dict.setdefault(col, []).append(col)
        
        # return used_columns 
        return column_dict   
        # if(graph.model.model_name == ModelName.DECISIONTREEREGRESSOR or graph.model.model_name == ModelName.DECISIONTREECLASSIFIER):
        #     used_columns = set(graph.model.tree_node_mappings.keys())
        # else:
        #     used_columns = set(graph.model.input_features)
        
        
        # Find all required columns including intermediate transformations
        # required_columns = set()
        # required_columns.update(used_columns)
        
        # # Track back through all transformations to find required input columns
        # for transform in pipeline['transforms']:
        #     transform_outputs = transform['transform_features']
        #     # If any output column is used, we need all input columns for this transformation
        #     if any(col in used_columns for col in transform_outputs):
        #         for col in transform_outputs:
        #             if col in used_columns:
        #                 # Add original input columns needed for this transformation
        #                 required_columns.update(transform['transform_features'])
    
        # Update used_columns to include all required columns
        # used_columns = required_columns
    
    def implements_case(graph_implements):
        for graph_implement in graph_implements:
            if graph_implement == SQLPlanType.JOIN:
                return False
        return True
    
    def __compose_sql(self, graph: PrepGraph, table_name: str, dbms: str, pre_sql: str, pipeline, array_ = False, single_sql_gen=False, join_method=None) -> str:
        input_table = table_name
        expend_features = {}
        used_array = set()
        array_feature = set()
        # used_columns = self.extract_columns_from_sql(graph.model.query("", dbms), graph)   # analysis used columns in model     
        ohe_array = array_
        lr_down = set()
        fill_sqls = {}
        imputer_sql = '(SELECT '
        
        if pipeline['imputer']['missing_cols']:
            filled_values = pipeline['imputer']['filled_values']
            missing_cols = pipeline['imputer']['missing_cols']
            missing_col_indexs = pipeline['imputer']['missing_col_indexs']
            imputer_feature_sqls = []
            for idx, feature in enumerate(missing_cols):
                # if feature in used_columns:
                fill_sqls[feature] = filled_values[missing_col_indexs[idx]]

        # compose join sqls
        join_feature_sqls = {}
        join_feature_list = {}
        first_join_op_feature = []
        # left_join_ops = []
        merge_table = []
        merged_list = [] 
        
        for index, item in enumerate(graph.chains.items()):
            feature, chain = item 
            if single_sql_gen and len(chain.prep_operators) == 0:
                continue
            op = chain.prep_operators[0]
            if graph.implements[feature][0] == SQLPlanType.JOIN:
                if op.features[0] not in first_join_op_feature and op.features[0] not in merge_table:
                    first_join_op_feature.append(op.features[0])
                    if op.inter_merge:
                        merge_op = chain.merge_operators[0]
                        if merge_op[0] != index and index not in merged_list:
                            merged_list.append(merge_op[0])
                                
                            input_table, feature_sql, join_features, merge_feature = merge_op[1].get_join_sql(dbms, input_table, table_name, pipeline, fill_sqls)
                            merge_table.extend(merge_feature)
                            join_feature_sqls[op.features[0]] = feature_sql
                            join_feature_list[op.features[0]] = join_features
                            
                    else:
                        if((isinstance(op, OneHotEncoderSQLOperator) or isinstance(op, BinaryEncoderSQLOperator)) and ohe_array):
                            if graph.model is not None and graph.model.model_name == ModelName.LINEARREGRESSION and graph.model.array == True:
                                input_table, feature_sql, join_features = op.get_join_sql_array(dbms, input_table, table_name, pipeline, True)
                                lr_down |= set(join_features)   
                                # input_table, feature_sql, join_features = op.get_join_sql_array(dbms, input_table, table_name, pipeline)
                            else:
                                input_table, feature_sql, join_features = op.get_join_sql_array(dbms, input_table, table_name, pipeline)
                            # used_array |= set(join_features)
                            used_array |= {f.strip('"') for f in join_features}
                            array_feature.add(op.features[0])
                        else:
                            input_table, feature_sql, join_features = op.get_join_sql(dbms, input_table, table_name, pipeline, fill_sqls)
                        
                        join_feature_sqls[op.features[0]] = feature_sql
                        join_feature_list[op.features[0]] = join_features
                # else:
                #     left_join_ops.append(op)
        # graph.join_operators = left_join_ops     
 
        # compose imputer sql if exists missing cols
        # or compose first level join sql
        if fill_sqls or join_feature_sqls:
            imputer_feature_sqls = []
            for feature, chain in graph.chains.items():
                delimited_feature = DBMSUtils.get_delimited_col(dbms, feature)
                if join_feature_sqls.get(feature):
                    imputer_feature_sqls.append(join_feature_sqls[feature])
                    join_feature_sqls.pop(feature)
                    if len(join_feature_list.get(feature)) >= 1: 
                        if(feature not in array_feature):
                            expend_features[feature] = join_feature_list.get(feature)
                        else:
                            expend_features[feature] = [delimited_feature]
                    # elif feature in first_join_op_feature:
                    #     continue
                elif feature in fill_sqls:
                    if type(fill_sqls[feature]) == str:
                        imputer_feature_sqls.append(f'COALESCE({delimited_feature}, \'{fill_sqls[feature]}\') AS {delimited_feature}')
                    else:
                        imputer_feature_sqls.append(f'COALESCE({delimited_feature}, {fill_sqls[feature]}) AS {delimited_feature}')
                # elif feature in used_columns:
                elif feature not in merge_table: # expand_tofix
                    imputer_feature_sqls.append(f'{delimited_feature}')
            imputer_sql += ','.join(imputer_feature_sqls)
            imputer_sql += f' FROM {input_table}) AS data'
            input_table = imputer_sql       
        # compose preprocessing sqls
        max_level = 0
        # prep_sqls = []
        prep_level = 0
        ab_list = []
        for _, chain in graph.chains.items():
            max_level = max(max_level, len(chain.prep_operators) - 1)
        while prep_level <= max_level or len(join_feature_sqls) > 0:
            perp_level_sqls = []
            for feature, chain in graph.chains.items():
                if prep_level > 0 and len(graph.implements[feature])> prep_level and graph.implements[feature][prep_level] == SQLPlanType.JOIN and feature not in merge_table:
                    if chain.prep_operators[prep_level].inter_merge:
                        op = chain.merge_operators[prep_level][1]
                        input_table, feature_sql, join_features,merge_feature = op.get_join_sql(dbms, input_table, "data", pipeline, fill_sqls)
                        merge_table.extend(merge_feature)
                    else:
                        op = chain.prep_operators[prep_level]
                        if((isinstance(op, OneHotEncoderSQLOperator) or isinstance(op, BinaryEncoderSQLOperator)) and ohe_array):
                            if graph.model is not None and graph.model.model_name == ModelName.LINEARREGRESSION and graph.model.array == True:
                                input_table, feature_sql, join_features = op.get_join_sql_array(dbms, input_table, "data", pipeline, True)
                                lr_down |= set(join_features)                                        
                            else:
                                input_table, feature_sql, join_features = op.get_join_sql_array(dbms, input_table, "data", pipeline)
                            # used_array |= set(join_features)
                            used_array |= {f.strip('"') for f in join_features}
                            array_feature.add(op.features[0])
                        else:
                            input_table, feature_sql, join_features = op.get_join_sql(dbms, input_table, "data", pipeline, fill_sqls)
                            
                    join_feature_sqls[op.features[0]] = feature_sql
                    join_feature_list[op.features[0]] = join_features
                if len(chain.prep_operators) > prep_level and graph.implements[feature][prep_level] == SQLPlanType.CASE:
                    if feature in merge_table:
                        continue
                    op = chain.prep_operators[prep_level]
                    perp_level_sqls.append(op.get_sql(dbms))
                    if op.features != op.features_out:
                        expend_features[feature] = [DBMSUtils.get_delimited_col(dbms, c) for c in op.features_out]
                
                else:
                    if(feature in ab_list):
                        perp_level_sqls.append(DBMSUtils.get_delimited_col(dbms, feature))
                    elif join_feature_sqls.get(feature):
                        perp_level_sqls.append(join_feature_sqls[feature])
                        join_feature_sqls.pop(feature)
                        if len(join_feature_list.get(feature)) > 1:
                            expend_features[feature] = join_feature_list.get(feature)   
                    elif expend_features.get(feature):
                        perp_level_sqls.append(','.join(expend_features[feature])) 
                    elif feature not in merge_table:
                        perp_level_sqls.append(DBMSUtils.get_delimited_col(dbms, feature))
            # prep_sqls.append(','.join(perp_level_sqls))
            prep_sql_ = ','.join(perp_level_sqls)
            if single_sql_gen:
                if join_method != 'case':
                    res = input_table[1:]
                    res = res[:-9]
                    return res
                else:
                    return 'SELECT ' + prep_sql_ + f' FROM {input_table}'
            input_table = "({}) AS data".format('SELECT ' + prep_sql_ + f' FROM {input_table}')
            prep_level += 1

        # for prep_sql in prep_sqls:
        #     # the input table for the possible next transformer is the output of the current transformer
        #     input_table = "({}) AS data".format('SELECT ' + prep_sql + f' FROM {input_table}')
        
        # process lr_down dict
        lr_down_dict = self.mapping_name(dbms, lr_down)
        # rename
        used_array_dict = mapping_key(dbms, used_array)
        if ohe_array and graph.model is not None:
            # compose model sql 
            predication_query = graph.model.query(input_table, dbms, used_array_dict, lr_down_dict, True)
        elif graph.model is not None:
            # close_array
            predication_query = graph.model.query(input_table, dbms, float = True)
        elif ohe_array:
            predication_query = graph.select_all(dbms, input_table)
        else:
            predication_query = self.extract_outer_sql(input_table) 


        return pre_sql + predication_query
    
    def extract_outer_sql(self, sql: str) -> str:
        sql = sql.strip()
        if not sql.startswith('('):
            raise ValueError("Input must start with '('")
        
        depth = 0
        start = None
        for i, ch in enumerate(sql):
            if ch == '(':
                if depth == 0:
                    start = i + 1
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return sql[start:i].strip()
        
        raise ValueError("Unmatched parentheses")

    def mapping_name(self, dbms,columns):
        grouped_data = defaultdict(list)  
        final_mapping = {}

        for col in columns:
            match = re.match(r'(.+)_(\d+)$', col)
            if match:
                prefix, y = match.groups()
                grouped_data[prefix].append((int(y), col))  
            else:
                final_mapping[col] = col 
                
        for key in sorted(grouped_data.keys()): 
            grouped_data[key].sort() 
            for i, (_, original) in enumerate(grouped_data[key]):
                final_mapping[original] = f'{key}'
                
        return final_mapping
    
    def join_merge(self,query,input_table):
        
        merged_table = f"{input_table}_enc_merged"
       
        from_clause = re.search(re.escape(f"FROM {input_table}") + r'(.*?\))',query)
        
        if from_clause:
            from_clause_str = from_clause.group() 
            matches = re.findall(r'=(\S+?)(?=\s|\))', from_clause_str) 
        else:
            matches = [] 
        
            
        table_and_cols = []
        for item in matches:
            parts = item.split('.')
            if len(parts) == 2:
                table_and_cols.append(parts)
        
        new_from_clause = f"{input_table} JOIN {merged_table} ON "
        merged_SQL = f"DROP VIEW IF EXISTS {merged_table}; CREATE VIEW {merged_table} AS SELECT * FROM "
        
        for index, item in enumerate(table_and_cols):
            join_col = item[1]
            enc_table = item[0]
            if index != len(table_and_cols)-1:
                new_from_clause += f"{input_table}.{join_col} = {merged_table}.{join_col} AND "
                merged_SQL += f"{enc_table} CROSS JOIN "
            else:
                new_from_clause += f"{input_table}.{join_col} = {merged_table}.{join_col}"
                merged_SQL += f"{enc_table};"
                
        # print(new_from_clause)
        new_from_clause = "FROM "  + new_from_clause

        new_query = re.sub(re.escape(f"FROM {input_table}") + r'.*?\)', f"{new_from_clause})", query, count=1)
    
        return new_query,merged_SQL