from sympy import numer
import os, sys
CUR_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CUR_DIR, "../../../"))
sys.path.insert(0, PROJECT_ROOT)

import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.preprocessing import FunctionTransformer
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from encoderforge.utility.loader import save_model
from encoderforge.utility.training_helper import *
from sklearn.preprocessing import StandardScaler
from encoderforge.base.defs import OperatorName, ModelName
from encoderforge.base.defs import *

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
train_path = os.path.join(BASE_DIR, "dataset/4Bank/bank_train.csv")
test_path = os.path.join(BASE_DIR, "dataset/4Bank/bank_test.csv")
pipeline_save_path = os.path.join(BASE_DIR, "EncoderForge/experiments/pipelines/bank/bank.joblib")

data = pd.read_csv(train_path, encoding='utf-8')

# # drop high-cardinality or PII columns
# drop_cols = [
#     "Customer_ID", "Customer_Name", "Transaction_ID", "Merchant_ID", "Customer_Contact", "Customer_Email", "Transaction_Currency"
# ]
# data.drop(columns=drop_cols, inplace=True)

# target
y = data["Is_Fraud"]
X = data.drop(["Is_Fraud"], axis=1)

# time series
# X["Transaction_Second"] = pd.to_timedelta(X["Transaction_Time"]).dt.total_seconds()
# X["Transaction_Day"] = pd.to_datetime(X["Transaction_Date"], dayfirst=True, errors="coerce").dt.day

X.drop(columns=["Transaction_Date", "Transaction_Time"], inplace=True)

# all_columns = ["Gender", "Age", "State", "City", "Bank_Branch", "Account_Type", "Transaction_Amount", "Transaction_Type", "Merchant_Category", 
#  "Account_Balance", "Transaction_Device", "Transaction_Location", "Device_Type", "Is_Fraud", "Transaction_Description"]

# columns group
numeric_features = ["Age", "Transaction_Amount", "Account_Balance"] #Transaction_Type Transaction_Location
low_unique = ["Gender", "Account_Type", "Device_Type","Transaction_Type"]
mid_unique = ["State", "City", "Bank_Branch", "Transaction_Device", "Transaction_Description", "Merchant_Category","Transaction_Location"]

label_features = low_unique + mid_unique

all_cols = numeric_features + label_features
X = X[all_cols]

# transformers
imputer = EncoderForgeSimpleImputer(strategy="most_frequent")
standard_scaler = StandardScaler()
onehot_encoder = EncoderForgeOneHotEncoder(handle_unknown="ignore")
ordinal_encoder = EncoderForgeOrdinalEncoder()
count_encoder = EncoderForgeCountEncoder()
label_encoder = EncoderForgeLabelEncoder()

X_copy = X.copy()
step_imputer = ("Imputer", imputer)
X_copy = imputer.fit_transform(X_copy)

# pipeline steps
# transformer_1 = EncoderForgeColumnTransformer(
#     remainder="passthrough",
#     transformers=[
#         (
#             OperatorName.STANDARDSCALER.value, 
#             standard_scaler, 
#             numeric_features
#             ),
#         (
#             OperatorName.LABELENCODER.value, 
#             label_encoder, 
#             label_features
#             ),
#         #     OperatorName.ONEHOTENCODER.value, 
#         #     onehot_encoder, 
#         #     low_unique
#         #     ),
#         # (
#         #     OperatorName.ORDINALENCODER.value, 
#         #     ordinal_encoder, 
#         #     mid_unique
#         #     ), # CountEncoder/OrdinalEncoder
#     ],
#     input_data=X_copy
# )
# step_1_encoder = ("ColumnTransformer_step1", transformer_1)
# X_copy = transformer_1.fit_transform(X_copy)

transformers = [
    (
        OperatorName.STANDARDSCALER.value,
        standard_scaler,
        numeric_features,
    )
]

# LabelEncoder
for idx, col in enumerate(label_features):
    transformers.append((
        f"{OperatorName.LABELENCODER.value}_{idx}",
        EncoderForgeLabelEncoder(),
        [col],
    ))

transformer_1 = EncoderForgeColumnTransformer(
    remainder="passthrough",
    transformers=transformers,
    input_data=X_copy
)
step_1_encoder = ("ColumnTransformer_step1", transformer_1)
X_copy = transformer_1.fit_transform(X_copy)


# model
lr = LogisticRegression(max_iter=1000, solver="lbfgs")
step_estimator = (ModelName.LOGISTICREGRESSION.value, lr)

pipeline = Pipeline([
    step_imputer,
    step_1_encoder,
    step_estimator
])

pipeline.data_rows = len(X)
print("before fit")
pipeline.fit(X, y)
print("fit done!")

save_model(pipeline, pipeline_save_path)

# # insert join tables to database
defs.DBMS = 'postgresql'
insert_encoders_table_to_db(pipeline)

# data_test = pd.read_csv(test_data_path)
# y_test = data_test["Is_Fraud"]
# X_test = data_test.drop("Is_Fraud", axis=1)

# X_test["Transaction_Second"] = pd.to_timedelta(X_test["Transaction_Time"]).dt.total_seconds()
# X_test["Transaction_Day"] = pd.to_datetime(X_test["Transaction_Date"], dayfirst=True, errors="coerce").dt.day
# X_test.drop(columns=["Transaction_Date", "Transaction_Time"], inplace=True)

# X_test = X_test[all_cols]

# 检测未知类别
# def detect_unseen_categories(X_train, X_test):
#     print("检查测试集中是否存在训练集未观测类别...\n")
#     for col in X_train.columns:
#         train_vals = set(X_train[col].dropna().unique())
#         test_vals = set(X_test[col].dropna().unique())
#         unseen_vals = test_vals - train_vals
#         if unseen_vals:
#             print(f"特征 '{col}' 中测试集存在未观測类别: {sorted(unseen_vals)}")

# detect_unseen_categories(X, X_test)

# # predict
# y_pred = pipeline.predict(X_test)

# # evalueate
# print("=== Evaluation Metrics on Test Set ===")
# print(f"Accuracy : {accuracy_score(y_test, y_pred):.4f}")
# print(f"Precision: {precision_score(y_test, y_pred, average='macro'):.4f}")
# print(f"Recall   : {recall_score(y_test, y_pred, average='macro'):.4f}")
# print(f"F1 Score : {f1_score(y_test, y_pred, average='macro'):.4f}")
# print("=========================================\n")

# print(data["Is_Fraud"].value_counts(normalize=True))