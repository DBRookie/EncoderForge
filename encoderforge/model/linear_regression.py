from sklearn.linear_model import LinearRegression

from encoderforge.model.base_model import LinearModel
from encoderforge.base.defs import ModelName
from encoderforge.utility.dbms_utils import DBMSUtils

class LinearRegressionSQLModel(LinearModel):
    
    def __init__(self, trained_model: LinearRegression) -> None:
        super().__init__()
        self.model_name = ModelName.LINEARREGRESSION
        self.trained_model = trained_model
        self.input_features = trained_model.feature_names_in_
        self.weights = trained_model.coef_.ravel()
        self.bias = trained_model.intercept_
        self.array = False
    def query(self, input_table: str, dbms: str, used_columns: dict = None, lr_down: list = None, float: bool = False) -> str:
        query = "SELECT "
        plus_items = []
        for i in range(len(self.input_features)):      
            # ab
            # col = DBMSUtils.get_delimited_col(dbms, self.input_features[i])
            # name = col.strip('"')
            # last_underscore_pos = name.rfind('_')
            # # if is no_expand operator
            # if last_underscore_pos == -1:
            #     plus_item = " {} * {} ".format(col, f"{self.weights[i]:.6f}", col)
            #     plus_items.append(plus_item)
            # else:
            #     prefix = name[:last_underscore_pos]
            #     suffix = name[last_underscore_pos + 1:]
            #     new_suffix = f"[{int(suffix) + 1}]"
            #     new_name = prefix + new_suffix
            #     plus_item = " {} * {} ".format(new_name, f"{self.weights[i]:.6f}", col)
            #     plus_items.append(plus_item)
            
            # eb
            col = DBMSUtils.get_delimited_col(dbms, self.input_features[i])
            feature_name = col.split('"')[1]
            
            if ((used_columns is None) or (feature_name not in used_columns.keys())):
                plus_item = " {}::float * {}::float ".format(col, f"{self.weights[i]:.6f}") if float else " {} * {} ".format(col, f"{self.weights[i]:.6f}")
            elif (self.array and feature_name in lr_down):
                col_name = '"' + lr_down[feature_name] + '"'
                if col_name in plus_items:
                    continue
                plus_item = "{}::float".format(col_name) if float else "{}".format(col_name)
            else:
                plus_item = " {}::float * {}::float ".format(used_columns[feature_name], f"{self.weights[i]:.6f}") if float else " {} * {} ".format(used_columns[feature_name], f"{self.weights[i]:.6f}")
            plus_items.append(plus_item)
            
        query += ('+'.join(plus_items) + f'+ {self.bias}')
        query += " FROM {}".format(input_table)
        
        return query