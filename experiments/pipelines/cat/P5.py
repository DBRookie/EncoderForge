from sympy import numer
import os, sys
CUR_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CUR_DIR, "../../../"))
sys.path.insert(0, PROJECT_ROOT)

import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.preprocessing import FunctionTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import category_encoders as ce
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from encoderforge.utility.loader import save_model
from encoderforge.utility.training_helper import *
from sklearn.preprocessing import StandardScaler
from encoderforge.base.defs import OperatorName, ModelName
from encoderforge.base.defs import *

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
train_path = os.path.join(BASE_DIR, "dataset/5Cat/cat_train.csv")
test_path = os.path.join(BASE_DIR, "dataset/5Cat/cat_test.csv")
pipeline_save_path = os.path.join(BASE_DIR, "EncoderForge/experiments/pipelines/cat/cat.joblib")


# load dataset
data = pd.read_csv(train_path)
y = data["target"]
X = data.drop(["target"], axis=1)

columns = X.columns.tolist()


# drop col
scaler_cols = ["bin_1","bin_2","ord_0","day","month"]  #bin_0

tens_cols = ["bin_3","bin_4","nom_0","nom_1","nom_2","nom_3","nom_4","ord_1","ord_2", "ord_3","ord_4"] #<=100

hundreds_cols = ["nom_5","nom_6","ord_5"] # > 100

thousands_cols = ["nom_7","nom_8","nom_9"] #>= 1000

all_cols =  scaler_cols + tens_cols + hundreds_cols + thousands_cols
X = X[all_cols]


#############################  define pipline  ##############################

# define transfomer
minmax_scalar = MinMaxScaler()
standard_scaler = StandardScaler()
onehot_encoder = EncoderForgeOneHotEncoder()
binary_encoder = EncoderForgeBinaryEncoder()
label_encoder = EncoderForgeLabelEncoder()
k_bins_discretizer = EncoderForgeKBinsDiscretizer(encode="ordinal",n_bins=20)

# define model
rf = RandomForestClassifier(max_depth=5,n_estimators=5,random_state=24)

# define steps
X_copy = X.copy()
# transformers = EncoderForgeColumnTransformer(
#     remainder="passthrough",
#     transformers=[
#         (
#             OperatorName.MINMAXSCALER.value,
#             minmax_scalar,
#             scaler_cols,
#         ),
#         (
#              OperatorName.LABELENCODER.value,
#              label_encoder,
#              hundreds_encoder_cols,
#         ),
#         (
#              OperatorName.BINARYENCODER.value,
#              binary_encoder,
#              thousands_encoder_cols,
#         ),
#     ],
#     input_data=X_copy
# )
# step1_column_transform = ("ColumnTransformer_step1", transformers)
# # transformers_copy = clone(transformers)
# X_copy = transformers.fit_transform(X_copy)

imputer = EncoderForgeSimpleImputer(strategy="most_frequent")
step_imputer = ('Imputer', imputer)
X_copy = imputer.fit_transform(X_copy)

# transformer list
transformers_list = [
    (
        OperatorName.MINMAXSCALER.value,
        minmax_scalar,
        scaler_cols,
    ),
    (
        OperatorName.ONEHOTENCODER.value,
        onehot_encoder,
        tens_cols,
    )
]

# EncoderForgeLabelEncoder
big_cols = hundreds_cols + thousands_cols

for idx, col in enumerate(big_cols):
    transformers_list.append((
        f"{OperatorName.LABELENCODER.value}_{idx}",
        label_encoder,
        [col],
    ))

# ColumnTransformer
transformers = EncoderForgeColumnTransformer(
    remainder="passthrough",
    transformers=transformers_list,
    input_data=X_copy
)

step1_column_transform = ("ColumnTransformer_step1", transformers)
X_copy = transformers.fit_transform(X_copy)


step2_pipeline_estimator = (ModelName.RANDOMFORESTCLASSIFIER.value, rf)

# define pipline
pipeline = Pipeline(
    steps=[
        step_imputer,
        step1_column_transform,       
        step2_pipeline_estimator        
    ]
)
############################### end ##########################################
pipeline.data_rows = len(X)
print("before fit")
# train model
pipeline.fit(X, y)
print("fit done!")


# insert join tables to database
defs.DBMS = 'postgresql'
insert_encoders_table_to_db(pipeline)

# save model to the file
save_model(pipeline, pipeline_save_path)

# # test model
# data_test = pd.read_csv(test_data_path)
# y_test = data_test["target"]
# X_test = data_test.drop("target", axis=1)
# X_test = X_test[all_cols]

# # evaluate the test result
# y_predict = pipeline.predict(X_test)

# def detect_unseen_categories(X_train, X_test):
#     print("check no appearance...\n")
#     for col in X_train.columns:
#         train_vals = set(X_train[col].dropna().unique())
#         test_vals = set(X_test[col].dropna().unique())
#         unseen_vals = test_vals - train_vals
#         if unseen_vals:
#             print(f"⚠️ 特征 '{col}' 中测试集存在未见过的类别: {sorted(unseen_vals)}")
            
# detect_unseen_categories(X,X_test)

# # predict
# y_pred = pipeline.predict(X_test)

# # evaluate
# print("=== Evaluation Metrics on Test Set ===")
# print(f"Accuracy : {accuracy_score(y_test, y_pred):.4f}")
# print(f"Precision: {precision_score(y_test, y_pred, average='macro'):.4f}")
# print(f"Recall   : {recall_score(y_test, y_pred, average='macro'):.4f}")
# print(f"F1 Score : {f1_score(y_test, y_pred, average='macro'):.4f}")
# print("=========================================\n")


# # step_models = list(pipeline.named_steps.values())
# # for i in range(3):
# #     X = step_models[i].transform(X)
# # for i in range(3):
# #     X_test = step_models[i].transform(X_test)

# # print("train: \n")
# # print_distribute(model,X)


# # print("\n test: \n")
# # print_distribute(model,X_test)