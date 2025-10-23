from sklearn.linear_model import LogisticRegression

from encoderforge.model.base_model import LinearModel
from encoderforge.base.defs import ModelName,quote_if_str,DBMS
from encoderforge.utility.dbms_utils import DBMSUtils
from encoderforge.base.operator import Operator


class LogisticRegressionSQLModel(LinearModel):
    
    def __init__(self, trained_model: LogisticRegression) -> None:
        super().__init__()
        self.model_name = ModelName.LOGISTICREGRESSION
        self.trained_model = trained_model
        self.input_features = trained_model.feature_names_in_
        
        self.weights = trained_model.coef_
        self.bias = trained_model.intercept_
        self.classes = trained_model.classes_
        self.array = False
        self.modify_ = False

    def query(self, input_table: str, dbms: str, used_columns: dict = None, lr_down: list = None, float: bool = False) -> str:
        if DBMS == 'monetdb': 
            float = False
        if len(self.classes) == 2:
            # with push down 
            query = "SELECT CASE WHEN "
            plus_items = []
            for i in range(len(self.input_features)):
                col = DBMSUtils.get_delimited_col(dbms, self.input_features[i])
                # plus_item = " {}::float * {}::float ".format(col, f"{self.weights[0][i]:.6f}", col)
                feature_name = col.split('"')[1]
                    
                if used_columns is None or feature_name not in used_columns.keys():
                    col_source = col
                elif feature_name in used_columns:
                    col_source = used_columns[feature_name]
                else:
                    raise ValueError(f"Unexpected feature: {feature_name}")

                if self.modify_:
                    plus_item = f"{col_source}::float" if float else f"{col_source}"
                else:
                    plus_item = f"{col_source}::float * {self.weights[0][i]:.6f}::float" if float else f"{col_source} * {self.weights[0][i]:.6f}"

                plus_items.append(plus_item)
                
                # if ((used_columns is None) or (feature_name not in used_columns.keys())):
                #     if float:
                #         if self.modify_:
                #             plus_item = " {}::float".format(col)
                #         else:
                #             plus_item = " {}::float * {}::float ".format(col, f"{self.weights[0][i]:.6f}", col)
                #     else:
                #         #origin
                #         if self.modify_:
                #             plus_item = " {}".format(col)
                #         else:
                #             plus_item = " {} * {} ".format(col, f"{self.weights[0][i]:.6f}", col)
                # elif feature_name in used_columns:
                #     # array
                #     if self.modify_:
                #         plus_item = " {}::float".format(used_columns[feature_name])
                #     else:
                #         plus_item = " {}::float * {}::float ".format(used_columns[feature_name], f"{self.weights[0][i]:.6f}", col)
                #     # plus_item = " {} * {} ".format(used_columns[feature_name], self.weights[i][j], col)
                # else:
                #     print("Error")

            # sum_sql = '(' + '+'.join(plus_items) + f'+ {self.bias[0]}::float' + ')'   
            sum_sql = '(' + '+'.join(plus_items) + f'+ {self.bias[0]}' + ')'   
            query += f"1 / (1 + EXP(-{sum_sql})) <= 0.5 THEN {quote_if_str(self.classes[0])} ELSE {quote_if_str(self.classes[1])} END "
            query += " FROM {}".format(input_table)
            return query

        else:
            # sum each class's logit
            query = '(SELECT '
            for i in range(len(self.classes)):
                plus_items = []
                for j in range(len(self.input_features)):
                    col = DBMSUtils.get_delimited_col(dbms, self.input_features[j])
                    # plus_item = " {}::float * {}::float ".format(col, self.weights[i][j], col)
                    feature_name = col.split('"')[1]
                    
                    if ((used_columns is None) or (feature_name not in used_columns.keys())):
                        if float:
                            plus_item = " {}::float * {}::float ".format(col, self.weights[i][j], col)
                        else:
                            plus_item = " {} * {} ".format(col, self.weights[i][j], col)
                    elif feature_name in used_columns:
                        plus_item = " {}::float * {}::float ".format(used_columns[feature_name], self.weights[i][j], col)
                    else:
                        print("Error")
            
                    plus_items.append(plus_item)
                # sum_sql = '+'.join(plus_items) + f'+ {self.bias[i]}::float' 
                sum_sql = '+'.join(plus_items) + f'+ {self.bias[i]}' 
                query += f'{sum_sql} AS sum_{i},'
            query = query[:-1]  # remove last comma
            query += f' FROM {input_table}) as data'

            input_table = query

            # softmax denominator
            sum_exp_sql = "(" + "+".join([f"EXP(sum_{i})" for i in range(len(self.classes))]) + ")"

            # compute class probabilities
            query = '(SELECT '
            for i in range(len(self.classes)):
                query += f"(EXP(sum_{i}) / {sum_exp_sql}) AS class_{i},"
            query = query[:-1]  # remove last comma
            query += f"\n FROM {input_table}) as data"

            # argmax via CASE
            case_stm = "CASE"
            for i in range(len(self.classes)):
                case_stm += " WHEN "
                for j in range(len(self.classes)):
                    if j == i:
                        continue
                    case_stm += f"class_{i} >= class_{j} AND "
                case_stm = case_stm[:-5]  # remove last ' AND '
                case_stm += f" THEN {quote_if_str(self.classes[i])}\n"
            case_stm += "END AS Score"

            final_query = f"SELECT {case_stm} FROM {query}"
            return final_query
        
    def modify_model(self, feature: str, sql_operator: Operator):
        if len(self.classes) == 2:
            for i in range(len(self.input_features)):
                if feature == self.input_features[i]:
                    sql_operator.modify_lr(feature, self.weights[0][i])
                    self.modify_ = True