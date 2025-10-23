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
train_path = os.path.join(BASE_DIR, "dataset/3Adult/adult_train.csv")
test_path = os.path.join(BASE_DIR, "dataset/3Adult/adult_test.csv")
pipeline_save_path = os.path.join(BASE_DIR, "EncoderForge/experiments/pipelines/adult/adult.joblib")


# load dataset
data = pd.read_csv(train_path)
y = data["target"]
X = data.drop(["target"], axis=1)

cat_cols = ['workclass', 'education', 'marital_status', 'occupation', 'relationship', 'race', 'sex', 'native_country']
num_cols = ['age', 'fnlwgt','education_num', 'capital_gain', 'capital_loss', 'hours_per_week']

# drop target col
all_cols = cat_cols + num_cols
X = X[all_cols]

kbins = EncoderForgeKBinsDiscretizer(encode="ordinal",n_bins=14,strategy='uniform')
onehot_encoder = EncoderForgeOneHotEncoder()

#############################  define pipline  ##############################
# define steps
X_copy = X.copy()
transformer1 = EncoderForgeColumnTransformer(
    remainder="passthrough",
    transformers=[
        (
            OperatorName.KBINSDISCRETIZER.value,
            kbins,
            num_cols,
        ),
        (
            OperatorName.ONEHOTENCODER.value,
            onehot_encoder,
            cat_cols,
        )
    ],
    input_data=X_copy
)
step1_column_transform = ("ColumnTransformer_step1", transformer1)
X_copy = transformer1.fit_transform(X_copy, y)

transformer2 = EncoderForgeColumnTransformer(
    remainder="passthrough",
    transformers=[
         (
            OperatorName.ONEHOTENCODER.value,
            onehot_encoder,
            num_cols,
        )
    ],
    input_data=X_copy
)
step2_column_transform = ("ColumnTransformer_step2", transformer2)
X_copy = transformer2.fit_transform(X_copy)

# Compose steps to build a pipeline
# pipeline = Pipeline(
#     steps=[
#         ("ColumnTransformer_step1", transformer1), 
#         ("ColumnTransformer_step2", transformer2) 
#     ]
# )

pipeline = Pipeline([
    step1_column_transform, 
    step2_column_transform,
    ("dummy_estimator",FunctionTransformer())  
    # None
])
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