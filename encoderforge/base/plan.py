from encoderforge.base.chains import PrepChain
from encoderforge.base.defs import SQLPlanType, OperatorType
import encoderforge.base.defs as defs

class ChainFusionPlan(object):
    
    def __init__(self, begin_op_index, fusion_directions) -> None:
        self.begin_op_index = begin_op_index
        self.fusion_directions: list = fusion_directions

class ChainCandidateFusionPlans(object):
    
    def __init__(self, feature: str, chain: PrepChain) -> None:
        ops_num = len(chain.prep_operators)
        self.feature = feature
        self.candidate_fusion_plans = []
        self.exist_begin_pairs = set()
        if ops_num == 0:
            self.candidate_fusion_plans.append(ChainFusionPlan(-1, []))
            
        for begin_idx in range(ops_num):
            left_remain_ops_num = begin_idx
            right_remain_ops_num = ops_num - begin_idx
            for fusion_directions in self.generate_sequences(begin_idx, left_remain_ops_num, right_remain_ops_num):
                self.candidate_fusion_plans.append(ChainFusionPlan(begin_idx, fusion_directions))
                
                
    def generate_sequences(self, begin_idx, remaining_zeros, remaining_ones, current_seq:list = []):
        if len(current_seq) == 1:
            if (current_seq[0] + begin_idx) in self.exist_begin_pairs:
                return
            else:
                self.exist_begin_pairs.add(current_seq[0] + begin_idx)
            
        if remaining_ones == 0 and remaining_zeros == 0:
            yield current_seq.copy()
            return
        if remaining_zeros > 0:
            current_seq.append(0)
            yield from self.generate_sequences(begin_idx, remaining_zeros - 1, remaining_ones, current_seq)
            current_seq.pop()
        if remaining_ones > 0:
            current_seq.append(1)
            yield from self.generate_sequences(begin_idx, remaining_zeros, remaining_ones - 1, current_seq)
            current_seq.pop()


class ChainImplementPlan(object):
    
    def __init__(self, chain_implement_plan: list[SQLPlanType]) -> None:
        self.chain_implement_plan = chain_implement_plan


class ChainCandidateImplementPlans(object):

    def __init__(self, feature: str, chain: PrepChain, strategy: str = "case") -> None:
        self.feature = feature
        self.candidate_implement_plans = []
        self.strategy = strategy
        for candidate_implement_plan in self.enumerate_implementations(chain):
            self.candidate_implement_plans.append(ChainImplementPlan(candidate_implement_plan))

    def enumerate_implementations(self, chain: PrepChain, index: int = 0, current_combination: list[SQLPlanType] = []):
        # if finished return current op
        if index == len(chain.prep_operators):
            yield current_combination.copy()
            return
        
        if self.strategy == "join":
            if chain.prep_operators[index].op_type in [OperatorType.CAT_C_CAT, OperatorType.EXPAND]:
                '''
                
                todo: "kbins——join con_c_cat"
                
                '''
                # if defs.GROUP in ('org', 'pos', 'uncertain'):
                #     # Heuristic
                #     if (
                #         chain.prep_operators[index].op_type == OperatorType.CAT_C_CAT
                #         and len(chain.prep_operators[index].mappings[0]) > 100
                #         or chain.prep_operators[index].op_type == OperatorType.EXPAND
                #         and len(chain.prep_operators[index].mapping) > 100
                #     ):
                #         implementations = [SQLPlanType.JOIN]
                #     else:
                #         implementations = [SQLPlanType.CASE, SQLPlanType.JOIN]
                # else:
                # implementations = [SQLPlanType.CASE, SQLPlanType.JOIN]
                implementations = [SQLPlanType.JOIN] # join only 
                defs.use_cut = True
            else:
                implementations = [SQLPlanType.CASE]          
        elif self.strategy == "join_case" or self.strategy == "case_join":
            if chain.prep_operators[index].op_type in [OperatorType.CAT_C_CAT, OperatorType.EXPAND]:
                implementations = [SQLPlanType.CASE, SQLPlanType.JOIN]
                defs.use_cut = True
            else:
                implementations = [SQLPlanType.CASE]    
        else:
            implementations = [SQLPlanType.CASE]

        # Traverse all implementations of the current operator
        for i in range(len(implementations)):
            # add current way
            current_combination.append(implementations[i])
            # Recursively process the next operator
            yield from self.enumerate_implementations(chain, index + 1, current_combination)
            # Backtrack and remove the current implementation
            current_combination.pop()
