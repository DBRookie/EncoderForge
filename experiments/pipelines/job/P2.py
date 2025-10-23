from sympy import numer
import os, sys
CUR_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CUR_DIR, "../../../"))
sys.path.insert(0, PROJECT_ROOT)

import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from encoderforge.utility.loader import save_model
from encoderforge.utility.training_helper import *
from sklearn.preprocessing import StandardScaler
from encoderforge.base.defs import *

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
train_path = os.path.join(BASE_DIR, "dataset/new/Job/ai_job_market_insights_train.csv")
test_path = os.path.join(BASE_DIR, "dataset/new/Job/ai_job_market_insights_test.csv")
pipeline_save_path = os.path.join(BASE_DIR, "EncoderForge/experiments/pipelines/job/job.joblib")

# load data
df = pd.read_csv(train_path)

# feature and label
y = df["Remote_Friendly"]
X = df.drop(columns=["Remote_Friendly"])

categorical_features = ["Job_Title","Industry","Company_Size","Location","AI_Adoption_Level","Automation_Risk", "Required_Skills","Job_Growth_Projection"]

numeric_feature = ["Salary_USD"]

X_copy = X.copy()

# EncoderForge 
imputer = EncoderForgeSimpleImputer(strategy="most_frequent")
onehot_encoder = EncoderForgeOneHotEncoder(handle_unknown="ignore")
kbins = EncoderForgeKBinsDiscretizer(encode="ordinal",n_bins=20,strategy='uniform')
standard_scaler = StandardScaler()

# Imputer step
step_imputer = ("Imputer", imputer)
X_copy = imputer.fit_transform(X_copy)

# transform step
transformer_1 = EncoderForgeColumnTransformer(
    remainder="passthrough",
    transformers=[
        (
            OperatorName.ONEHOTENCODER.value,
            onehot_encoder,
            categorical_features,
        ),
        (
            OperatorName.KBINSDISCRETIZER.value,
            kbins,
            numeric_feature,
        ) #kbins 20 + onehot
    ],
    input_data=X_copy
)
step_encoder_1 = ("ColumnTransformer_step1", transformer_1)
X_copy = transformer_1.fit_transform(X_copy)

transformer_2 = EncoderForgeColumnTransformer(
    remainder="passthrough",
    transformers=[
        (
            OperatorName.ONEHOTENCODER.value,
            onehot_encoder,
            numeric_feature,
        )
    ],
    input_data=X_copy
)
step_encoder_2 = ("ColumnTransformer_step2", transformer_2)
X_copy = transformer_2.fit_transform(X_copy)

# model
lr = LogisticRegression(max_iter=500, solver="lbfgs", multi_class="auto")
step_estimator = (ModelName.LOGISTICREGRESSION.value, lr)

# pipeline structure
pipeline = Pipeline([
    step_imputer,
    step_encoder_1,
    step_encoder_2,
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
