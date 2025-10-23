import numpy as np
from pandas import DataFrame, Series
from sympy import And, Symbol
from numpy import array

from encoderforge.base.operator import CON_C_CAT
from encoderforge.base.defs import OperatorName
import encoderforge.base.defs as defs

class KBinsDiscretizerSQLOperator(CON_C_CAT):
    '''
    todo:
    
    case-when => join
    belike:
    CASE WHEN "ord_4" >= 122.0 AND "ord_4" < 4680.0 THEN 0
         WHEN "ord_4" >= 4680.0 AND "ord_4" < 6771.0 THEN 1
         WHEN "ord_4" >= 6771.0 AND "ord_4" < 7344.0 THEN 2
         WHEN "ord_4" >= 7344.0 AND "ord_4" < 9329.0 THEN 3
         ELSE 4 
    => 
    "ord_4"
    
    create table ord_4_mapping;
    ord_4_low     ord_4_up      ord_4_value
    122     4680    0
    4680    6771    1
    6771    7344    2
    7344    9329    3
    9329    NULL    4

    left join ord_4_mapping on data."ord_4" >= ord_4_mapping.ord_4_low
              AND (
                data."ord_4" < ord_4_mapping.ord_4_up
                OR ord_4_mapping.ord_4_up IS NULL
              )
              
    "ord_4_cat_c_cat_col" AS "ord_4"
    =>
    "ord_3_value" AS "ord_4"
    
    
    '''

    def __init__(self, featrues: list[str], fitted_transform):
        super().__init__(OperatorName.KBINSDISCRETIZER)
        self.features = featrues
        self._extract(fitted_transform)


    def _extract(self, fitted_transform) -> None:  
        self.strategy = fitted_transform.strategy
        self.bin_distribution = fitted_transform.bin_distribution
        for feature in self.features:
            self.features_out.append(feature)
            feature_idx = fitted_transform.feature_names_in_.tolist().index(feature)
            self.bin_edges.append(fitted_transform.bin_edges_[feature_idx])
            self.n_bins.append(fitted_transform.n_bins_[feature_idx])
            self.categories.append(array(list(range(fitted_transform.n_bins_[feature_idx]))))
            intervals = [
                (self.bin_edges[0][i], self.bin_edges[0][i + 1])
                for i in range(self.n_bins[0])
            ]
            
            if defs.AUTO_RULE_GEN:
                self.inequations[feature] = []
                self.inequations_mappings[feature] = []
                x = Symbol('x')
                for idx, interval in enumerate(intervals):
                    self.inequations[feature].append(And(x > interval[0], x < interval[1]))
                    self.inequations_mappings[feature].append(idx)
                
            self.mappings.append(
                Series(
                    self.categories[-1],
                    index=intervals
                )
            )
        

    @staticmethod
    def trans_feature_names_in(input_data: DataFrame):
        return input_data.columns
