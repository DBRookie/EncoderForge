from __future__ import annotations
from typing import Type
import re
import importlib
import copy

from encoderforge.base.chains import PrepChain
from encoderforge.model.base_model import SQLModel
from encoderforge.base.defs import MODEL_PACKAGE_PATH, SQLPlanType
from encoderforge.base.operator import Operator
from encoderforge.utility.dbms_utils import DBMSUtils
from encoderforge.base.func import mapping_key

class PrepGraph(object):

    def __init__(self, input_features: list[str] = None, pipeline: dict = None) -> None:
        self.model: Type[SQLModel]
        self.chains: dict[str, PrepChain] = {}
        self.implements: dict[str, list] = {}
        self.join_operators: list[Operator] = []
        # self.operator_chains: dict[str, list[tuple[str,int]]] = {}
        # self.merge_cp: list[list[str]] = []
        if pipeline is not None:
            self.__build_graph(input_features, pipeline)

    def __camel_to_snake(self, name: str):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    
    def __build_graph(self, input_features: list[str], pipeline: dict) -> None:
        model_info = pipeline['model']
        model_name = model_info['model_name']  
        
        for feature in input_features:
            self.chains[feature] = PrepChain(feature, pipeline)
            self.implements[feature] = [SQLPlanType.CASE] * len(self.chains[feature].prep_operators)
        
        # if FunctionTransformer or dummy skip
        if model_name == "dummy_estimator" or model_info['trained_model'] is None:
            self.model = None  # or some DummySQLModel()
            print("Skipping model: FunctionTransformer (no SQL translation needed)")
            return

        try:
            module_name = self.__camel_to_snake(model_name)
            transform_module = importlib.import_module(MODEL_PACKAGE_PATH + module_name)
            model_class = getattr(transform_module, model_name + 'SQLModel')
            self.model = model_class(model_info['trained_model'])
        except ModuleNotFoundError as e:
            raise ImportError(f"SQLModel for {model_name} not found: {e}") from e
        except AttributeError as e:
            raise ImportError(f"{model_name}SQLModel class not defined in {module_name}.py") from e
        
        # transform_module = importlib.import_module(MODEL_PACKAGE_PATH + module_name)
        # model_class = getattr(transform_module, model_name + 'SQLModel')
        # self.model = model_class(pipeline['model']['trained_model']) 
               
        # transforms = pipeline['transforms']
        # index_ = 0
        # index_features = dict()
        # for transform in transforms:
        #     features_in_transform = '-'.join(transform['transform_features'])
        #     transform_name = transform['transform_name']
        #     if features_in_transform not in self.operator_chains:
        #         index_features[features_in_transform] = index_
        #         index_ += 1
        #         self.operator_chains[features_in_transform] = [(transform_name,index_features[features_in_transform])]
        #     else:
        #         self.operator_chains[features_in_transform].append((transform_name,index_features[features_in_transform]))
        
        
    def get_empty_chains_graph(self) -> PrepGraph:
        """construct a new graph with empty featrues preprocessing chains

        Returns:
            PrepGraph: new graph
        """
        
        new_graph = PrepGraph()
        new_graph.model = copy.deepcopy(self.model)
        new_graph.join_operators = copy.deepcopy(self.join_operators)
        for feature, _ in self.chains.items():
            new_graph.chains[feature] = PrepChain(feature)
            new_graph.implements[feature] = []
            
        #  operator_chains
        # for features, ops in self.operator_chains.items():
        #     new_graph.operator_chains[features] = []

        return new_graph
    
    def copy_graph(self) -> PrepGraph:
        """construct a new graph by clone

        Returns:
            PrepGraph: new graph
        """
        
        new_graph = PrepGraph()
        new_graph.model = copy.deepcopy(self.model)
        new_graph.join_operators = copy.deepcopy(self.join_operators)
        for feature, _ in self.chains.items():
            new_graph.chains[feature] = self.chains[feature].copy()
            new_graph.implements[feature] = copy.deepcopy(self.implements[feature])
            
        #  operator_chains
        # for features, ops in self.operator_chains.items():
        #     new_graph.operator_chains[features] = ops

        return new_graph
    
    def copy_prune_graph(self, cur_feature) -> PrepGraph:
        """construct a new graph by clone

        Returns:
            PrepGraph: new graph
        """
        
        new_graph = PrepGraph()
        
        # new_graph.model = copy.deepcopy(self.model)
        new_graph.model = self.model
        
        
        # new_graph.join_operators = copy.deepcopy(self.join_operators)
        new_graph.join_operators = self.join_operators
        
        for feature, _ in self.chains.items():
            if feature == cur_feature:
                new_graph.chains[feature] = self.chains[feature].copy_prune()
                new_graph.implements[feature] = copy.deepcopy(self.implements[feature])
            else:
                new_graph.chains[feature] = self.chains[feature]
                new_graph.implements[feature] = self.implements[feature]
        

        return new_graph
    
    def add_join_operator(self, op):
        self.join_operators.append(op)
        
    def graph_feature_used(self, used_columns) -> PrepGraph:
        new_graph = PrepGraph()
        new_graph.model = copy.deepcopy(self.model)
        new_graph.join_operators = copy.deepcopy(self.join_operators)
        id = 0
        for feature, _ in self.chains.items():
            if feature in used_columns.keys():  # update chains, filter unused features: next step - filter expand unused feature
                new_graph.chains[feature] = self.chains[feature].chain_feature_used(used_columns[feature], id) 
                new_graph.implements[feature] = copy.deepcopy(self.implements[feature])
                id += 1

        return new_graph
    
    def implemente_mergeop(self) -> PrepGraph:
        new_graph = PrepGraph()
        new_graph.model = copy.deepcopy(self.model)
        new_graph.join_operators = copy.deepcopy(self.join_operators)
        id = 0
        for feature, _ in self.chains.items():
            new_graph.chains[feature] = self.chains[feature].implemente_merge_op(id)
            new_graph.implements[feature] = copy.deepcopy(self.implements[feature])
            id += 1

        return new_graph
    
    def select_all(self, dbms, input_table) -> str:
        output_sql = "select "
        array = set()
        origin = set()
        for feature, _ in self.chains.items():
            if feature == "ADATE_3":
                pass
            op = _.prep_operators[-1]
            if len(op.features_out) == 1 and feature == op.features_out[0]:
                origin |= set([DBMSUtils.get_delimited_col(dbms, feature)])
            else:
                array |= set(op.features_out)
        array_dict = mapping_key(dbms, array)
        output_sql += ",".join(origin)
        if origin and array_dict:
            output_sql += ","
        output_sql += ",".join(array_dict.values())
        return output_sql + " from " + input_table
    