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
from category_encoders import CountEncoder, BinaryEncoder, TargetEncoder
from encoderforge.base.defs import *

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
train_path = os.path.join(BASE_DIR, "dataset/7Mushrooms/mushrooms_train.csv")
test_path = os.path.join(BASE_DIR, "dataset/7Mushrooms/mushrooms_test.csv")
pipeline_save_path = os.path.join(BASE_DIR, "EncoderForge/experiments/pipelines/mushrooms/mushrooms.joblib")

# load data
data = pd.read_csv(train_path)
y = data["class"]
X = data.drop(["class"], axis=1)

# count_features = ["cap_shape", "cap_surface", "cap_color",  "gill_color", "stalk_root", "stalk_surface_above_ring", "stalk_surface_below_ring", "stalk_color_above_ring", "stalk_color_below_ring", "veil_color",  "ring_type", "spore_print_color", "odor", "population", "habitat"]
# label_features = ["gill_attachment", "gill_spacing", "gill_size" ,"bruises", "stalk_shape","ring_number"]
count_features =[]
label_features = ["gill_attachment", "gill_spacing", "gill_size" ,"bruises", "stalk_shape","ring_number","cap_shape", "cap_surface", "cap_color",  "gill_color", "stalk_root", "stalk_surface_above_ring", "stalk_surface_below_ring", "stalk_color_above_ring", "stalk_color_below_ring", "veil_color",  "ring_type", "spore_print_color", "odor", "population", "habitat"]


all_cols = count_features + label_features
# all_cols = label_features
X = X[all_cols]

imputer = EncoderForgeSimpleImputer(strategy="most_frequent")
onehot_encoder = EncoderForgeOneHotEncoder(handle_unknown="ignore")
ordinal_encoder = EncoderForgeOrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
count_encoder = EncoderForgeCountEncoder()

X_copy = X.copy()
step_imputer = ("Imputer", imputer)
X_copy = imputer.fit_transform(X_copy)

transformers = EncoderForgeColumnTransformer(
    remainder="passthrough",
    transformers=[
         (
             OperatorName.ONEHOTENCODER.value,
             onehot_encoder,
             all_cols,
        )
    ],
    input_data=X_copy
)
step1_column_transform = ("ColumnTransformer_step1", transformers)

X_copy = transformers.fit_transform(X_copy)

# # second Expand encoder
# transformer_2 = EncoderForgeColumnTransformer(
#     remainder="passthrough",
#     transformers=[
#         (
#             OperatorName.ONEHOTENCODER.value, 
#             onehot_encoder, 
#             all_cols),
#     ],
#     input_data=X_copy
# )
# step_2_encoder = ("ColumnTransformer_Expand", transformer_2)
# X_copy = transformer_2.fit_transform(X_copy)

lr = LogisticRegression(max_iter=500, solver="lbfgs", multi_class="auto")
step_estimator = (ModelName.LOGISTICREGRESSION.value, lr)


pipeline = Pipeline(steps=[
    step_imputer,
    step1_column_transform,
    # step_2_encoder,
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
# y_test = data_test["class"]
# X_test = data_test.drop("class", axis=1)
# X_test = X_test[all_cols]

# # unkown
# def detect_unseen_categories(X_train, X_test):
#     print("check no appearance...\n")
#     for col in X_train.columns:
#         train_vals = set(X_train[col].dropna().unique())
#         test_vals = set(X_test[col].dropna().unique())
#         unseen_vals = test_vals - train_vals
#         if unseen_vals:
#             print(f"feature '{col}' unseen: {sorted(unseen_vals)}")

# detect_unseen_categories(X, X_test)

# # predict
# y_pred = pipeline.predict(X_test)

# # evaluate
# print("=== Evaluation Metrics on Test Set ===")
# print(f"Accuracy : {accuracy_score(y_test, y_pred):.4f}")
# print(f"Precision: {precision_score(y_test, y_pred, average='macro'):.4f}")
# print(f"Recall   : {recall_score(y_test, y_pred, average='macro'):.4f}")
# print(f"F1 Score : {f1_score(y_test, y_pred, average='macro'):.4f}")
# print("=========================================\n")
