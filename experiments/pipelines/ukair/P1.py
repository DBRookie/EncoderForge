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
train_path = os.path.join(BASE_DIR, "dataset/ukair/ukair2017_train.csv")
test_path = os.path.join(BASE_DIR, "dataset/ukair/ukair2017_test.csv")
pipeline_save_path = os.path.join(BASE_DIR, "EncoderForge/experiments/pipelines/ukair/ukair.joblib")

# load data
df = pd.read_csv(train_path)

# feature and label
y = df["PM_25"]
X = df.drop(columns=["PM_25"])

low = ["Environment_Type"]
high = ["Site_Name", "Zone"]
categoric_feature = high + low

numeric_feature = ["Hour", "Month", "DayofWeek",  "Altitude_m", "PM_10"]


categorical_features = numeric_feature + categoric_feature
X = X[categorical_features]

X_copy = X.copy()

# load encoder
imputer = EncoderForgeSimpleImputer(strategy="most_frequent")
label_encoder = EncoderForgeLabelEncoder()
standard_scaler = StandardScaler()
onehot_encoder = EncoderForgeOneHotEncoder(handle_unknown="ignore")

# Imputer step
step_imputer = ("Imputer", imputer)
X_copy = imputer.fit_transform(X_copy)

transformers = [
    (
        OperatorName.STANDARDSCALER.value,
        standard_scaler,
        numeric_feature,
    ),
    (
        OperatorName.ONEHOTENCODER.value,
        onehot_encoder,
        low,
    )
]

# labelencoder
for idx, col in enumerate(high):
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
lr = LinearRegression()
step_estimator = (ModelName.LINEARREGRESSION.value, lr)

# pipeline structure
pipeline = Pipeline([
    step_imputer,
    step_1_encoder,
    step_estimator
])
pipeline.data_rows = len(X)

# train
print("before fit")
pipeline.fit(X, y)
print("fit done!")

# save model
save_model(pipeline, pipeline_save_path)

defs.DBMS = 'postgresql'
insert_encoders_table_to_db(pipeline)
