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
train_path = os.path.join(BASE_DIR, "dataset/6criteo/criteo_train.csv")
test_path = os.path.join(BASE_DIR, "dataset/6criteo/criteo_test.csv")
pipeline_save_path = os.path.join(BASE_DIR, "EncoderForge/experiments/pipelines/criteo/criteo.joblib")

# load data
data = pd.read_csv(train_path, nrows=2000000)
y = data["target"]
X = data.drop("target", axis=1)

numeric_col =  ["n3","n5","n6","n7","n9","n11"]
low_cardinality = ["c6", "c9", "c14", "c17", "c20", "c22", "c23"]  # <100
# medium_cardinality = []  # <20k

medium_cardinality = ["c2","c5","c8","c25"]  # <1k
high_cardinality = ["c1", "c7", "c10", "c11", "c13", "c15", "c18", "c19","c26"]  # >1k

all_columns = low_cardinality + high_cardinality

X = X[all_columns]

# imputer = EncoderForgeSimpleImputer(strategy="most_frequent")
onehot_encoder = EncoderForgeOneHotEncoder(handle_unknown="ignore")
kbins = EncoderForgeKBinsDiscretizer(encode="ordinal",n_bins=15,strategy='uniform')
binary_encoder = EncoderForgeBinaryEncoder()
ordinal_encoder = EncoderForgeOrdinalEncoder()

X_copy = X.copy()

# pipeline
transformer1 = EncoderForgeColumnTransformer(
    remainder="passthrough",
    transformers=[
        (
            OperatorName.ONEHOTENCODER.value,
            onehot_encoder,
            low_cardinality,
        ),
        (
            OperatorName.BINARYENCODER.value,
            binary_encoder,
            high_cardinality,
        )
    ],
    input_data=X_copy
)
step1_column_transform = ("ColumnTransformer_step1", transformer1)
X_copy = transformer1.fit_transform(X_copy)

pipeline = Pipeline([
    # ('Imputer', imputer),
    step1_column_transform,
    ("dummy_estimator",FunctionTransformer())  
])

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