import re
import importlib
import copy
from encoderforge.base.operator import Operator,EXPAND
from encoderforge.base.defs import PREPROCESS_PACKAGE_PATH, OperatorName

class PrepChain(object):

    def __init__(self, feature: str, pipeline: dict = None) -> None:
        self.feature: str = feature
        self.prep_operators: list[Operator] = []
        self.merge_operators: list[list[int,Operator]] = []  # int->str compare feature directly
        if pipeline is not None:
            self.__build_chain(pipeline)

    def __camel_to_snake(self, name: str):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def __build_chain(self, pipeline: dict) -> None:
        transforms = pipeline['transforms']
        for transform in transforms:
            transform_features = transform['transform_features']
            after_expand_features = [
                f
                for f in transform_features
                if "_" in f 
                and f.split("_")[-1].isdigit()
                and "_".join(f.split("_")[:-1]) == self.feature
            ]
            if self.feature in transform_features or after_expand_features:

                transform_name = transform['transform_name']
                fitted_transform = transform['fitted_transform']

                module_name = self.__camel_to_snake(transform_name)
                transform_module = importlib.import_module(PREPROCESS_PACKAGE_PATH + module_name)
                operator_class = getattr(transform_module, transform_name + 'SQLOperator')

                if self.feature in transform_features:
                    operator = operator_class([self.feature], fitted_transform)
                else:
                    operator = operator_class(after_expand_features, fitted_transform)
                    
                self.prep_operators.append(operator)
                # self.merge_operators.append([id_number,operator])
            
                
    def copy(self):
        new_chain = PrepChain(self.feature)
        new_chain.prep_operators = copy.deepcopy(self.prep_operators)
        new_chain.merge_operators = copy.deepcopy(self.merge_operators)
        return new_chain
    
    def copy_prune(self):
        new_chain = PrepChain(self.feature)
        for op in self.prep_operators:
            new_chain.prep_operators.append(op)
        return new_chain
    
    def chain_feature_used(self, used_columns, id_number):
        # generate merge_operators and index
        new_chain = PrepChain(self.feature)
        for op in self.prep_operators:
            # expand is special need: fix mapping and features_out
            if isinstance(op, EXPAND):
                op.cut_features(used_columns) 
            new_chain.prep_operators.append(op)
            new_chain.merge_operators.append([id_number, op])
        
        return new_chain
        # init_v
        
    def implemente_merge_op(self, id_number):
        new_chain = PrepChain(self.feature)
        for op in self.prep_operators:
            new_chain.prep_operators.append(op)
            new_chain.merge_operators.append([id_number, op])
                    
        return new_chain