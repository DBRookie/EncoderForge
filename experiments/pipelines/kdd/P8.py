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
train_path = os.path.join(BASE_DIR, "dataset/new/kdd/cup98LRN_train.csv")
test_path = os.path.join(BASE_DIR, "dataset/new/kdd/cup98LRN_test.csv")
pipeline_save_path = os.path.join(BASE_DIR, "EncoderForge/experiments/pipelines/kdd/kdd.joblib")


# load data
data = pd.read_csv(train_path)
y = data["TARGET_B"]
X = data.drop(["TARGET_B"], axis=1)

# count_features = ["cap_shape", "cap_surface", "cap_color",  "gill_color", "stalk_root", "stalk_surface_above_ring", "stalk_surface_below_ring", "stalk_color_above_ring", "stalk_color_below_ring", "veil_color",  "ring_type", "spore_print_color", "odor", "population", "habitat"]
most_high_features = ['OSOURCE','ZIP','RFA_6','RFA_7','RFA_8', 'RFA_9', 'RFA_11', 'RFA_12', 'RFA_16', 'RFA_17', 'RFA_18','RFA_19', 'RFA_21', 'RFA_22']

cate_features_1 = ['STATE', 'MAILCODE', 'PVASTATE', 'NOEXCH','CHILD18', 'GENDER', 'DATASRCE', 'SOLP3', 'SOLIH', 'RFA_20'] #73 #10
cate_features_2 = ['RECINHSE', 'RECP3', 'RECPGVG', 'RECSWEEP', 'MDMAUD', 'DOMAIN','MAJOR', 'GEOCODE', 'COLLECT1', 'VETERANS', 'BIBLE', 'CATLG','RFA_23', 'RFA_24', 'RFA_2R']#15
cate_features_3 = ['CLUSTER', 'AGEFLAG', 'HOMEOWNR', 'CHILD03', 'CHILD07', 'CHILD12', 'HOMEE', 'PETS', 'CDPLAY', 'STEREO', 'PCOWNERS', 'PHOTO','MDMAUD_R', 'MDMAUD_F']#14
cate_features_4 = ['CRAFTS', 'FISHER', 'GARDENIN', 'BOATS', 'WALKER', 'KIDSTUFF', 'CARDS', 'PLATES', 'LIFESRC', 'PEPSTRFL', 'RFA_2', 'RFA_3', 'RFA_4', 'RFA_2A']#14
cate_features_5 = ['RFA_5', 'RFA_10', 'RFA_13', 'RFA_14', 'RFA_15',  'MDMAUD_A', 'GEOCODE2'] #7
cate_features = cate_features_1 + cate_features_2 + cate_features_3 + cate_features_4 + cate_features_5
for col in cate_features:
    X[col] = X[col].astype(str)
bin_features = ['AGE', 'NUMCHLD', 'INCOME', 'WEALTH1', 'MBCRAFT', 'MBGARDEN', 'MBBOOKS', 'MBCOLECT', 'MAGFAML', 'MAGFEM', 'MAGMALE', 'PUBGARDN', 'PUBCULIN', 'PUBHLTH', 'PUBDOITY', 'PUBNEWFN', 'PUBPHOTO', 'PUBOPP', 'WEALTH2', 'MSA', 'ADI', 'DMA', 'ADATE_3', 'ADATE_4', 'ADATE_6', 'ADATE_7', 'ADATE_8', 'ADATE_9', 'ADATE_10', 'ADATE_11', 'ADATE_12', 'ADATE_13', 'ADATE_14', 'ADATE_16', 'ADATE_17', 'ADATE_18', 'ADATE_19', 'ADATE_20', 'ADATE_21', 'ADATE_22', 'ADATE_23', 'ADATE_24', 'RDATE_3', 'RDATE_4', 'RDATE_5', 'RDATE_6', 'RDATE_7']
numeric_featues = ['RDATE_8', 'RDATE_9', 'RDATE_10', 'RDATE_11', 'RDATE_12', 'RDATE_13', 'RDATE_14', 'RDATE_15', 'RDATE_16', 'RDATE_17', 'RDATE_18', 'RDATE_19', 'RDATE_20', 'RDATE_21', 'RDATE_22', 'RDATE_23', 'RDATE_24', 'RAMNT_3', 'RAMNT_4', 'RAMNT_5', 'RAMNT_6', 'RAMNT_7', 'RAMNT_8', 'RAMNT_9', 'RAMNT_10', 'RAMNT_11', 'RAMNT_12', 'RAMNT_13', 'RAMNT_14', 'RAMNT_15', 'RAMNT_16', 'RAMNT_17', 'RAMNT_18', 'RAMNT_19', 'RAMNT_20', 'RAMNT_21', 'RAMNT_22', 'RAMNT_23', 'RAMNT_24', 'RAMNTALL', 'MINRAMNT', 'MAXRAMNT', 'LASTGIFT', 'NEXTDATE', 'TIMELAG', 'AVGGIFT', 'CLUSTER2']

all_cols = most_high_features + cate_features + bin_features + numeric_featues

# all_cols = label_features
X = X[all_cols]
imputer = EncoderForgeSimpleImputer(strategy="most_frequent")
# imputer = EncoderForgeSimpleImputer(strategy="most_frequent")
onehot_encoder = EncoderForgeOneHotEncoder(handle_unknown="ignore")
# ordinal_encoder = EncoderForgeOrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
# count_encoder = EncoderForgeCountEncoder()
kbins = EncoderForgeKBinsDiscretizer(encode="ordinal",n_bins=5,strategy='uniform')
standard_scaler = StandardScaler()
label_encoder = EncoderForgeLabelEncoder()

X_copy = X.copy()
step_imputer = ("Imputer", imputer)
X_copy = imputer.fit_transform(X_copy)

transformers=[
        (
            OperatorName.KBINSDISCRETIZER.value,
            kbins,
            bin_features,
        ),
        (
            OperatorName.ONEHOTENCODER.value,
            onehot_encoder,
            cate_features,
        ),
        (
            OperatorName.STANDARDSCALER.value,
            standard_scaler,
            numeric_featues,
        )
]

for idx, col in enumerate(most_high_features):
    transformers.append((
        f"{OperatorName.LABELENCODER.value}_{idx}",
        EncoderForgeLabelEncoder(),
        [col],
    ))
    

transformers_ = EncoderForgeColumnTransformer(
    remainder="passthrough",
    transformers=transformers,
    input_data=X_copy
)

step1_column_transform = ("ColumnTransformer_step1", transformers_)
X_copy = transformers_.fit_transform(X_copy)

# transformer_2 = EncoderForgeColumnTransformer(
#     remainder="passthrough",
#     transformers=[
#         (
#             OperatorName.ONEHOTENCODER.value,
#             onehot_encoder,
#             count_features,
#         )
#     ],
#     input_data=X_copy
# )
# step2_column_transform = ("ColumnTransformer_step2", transformer_2)
# X_copy = transformer_2.fit_transform(X_copy)


# lr = LogisticRegression(max_iter=500, solver="lbfgs", multi_class="auto")
# step_estimator = (ModelName.LOGISTICREGRESSION.value, lr)


pipeline = Pipeline(steps=[
    step_imputer,
    step1_column_transform,
    # step2_column_transform,
    ("dummy_estimator",FunctionTransformer())  
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
# y_test = data_test["TARGET_B"]
# X_test = data_test.drop("TARGET_B", axis=1)
# X_test = X_test[all_cols]

# # none-appearance
# def detect_unseen_categories(X_train, X_test):
#     for col in X_train.columns:
#         train_vals = set(X_train[col].dropna().unique())
#         test_vals = set(X_test[col].dropna().unique())
#         unseen_vals = test_vals - train_vals
#         if unseen_vals:
#             print(f"feature '{col}' category: {sorted(unseen_vals)}")

# detect_unseen_categories(X, X_test)

# y_pred = pipeline.predict(X_test)


# print("=== Evaluation Metrics on Test Set ===")
# print(f"Accuracy : {accuracy_score(y_test, y_pred):.4f}")
# print(f"Precision: {precision_score(y_test, y_pred, average='macro'):.4f}")
# print(f"Recall   : {recall_score(y_test, y_pred, average='macro'):.4f}")
# print(f"F1 Score : {f1_score(y_test, y_pred, average='macro'):.4f}")
# print("=========================================\n")
