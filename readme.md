# EncoderForge: Generating Efficient SQL for Encoders in Machine Learning Inference Pipelines
## Project Introduction

EncoderForge is a framework that transalte trained machine learning (ML) pipelines into efficient pure SQL queries, which enables the trained pipelines to be executed natively in target databases, such as PostgreSQL, DuckDB and MonetDB. 

## Quick Start

We use the UK Air Quality dataset to demonstrate the end-to-end process of training a machine learning pipeline, translating it into an optimized SQL query, and executing it natively within PostgreSQL.

### Step 1: Prepare the dataset in the database

```sql
-- Example for PostgreSQL
-- create table
CREATE TABLE ukair (
    "datetime" VARCHAR(40),
    "Hour" INTEGER,
    "Month" INTEGER,
    "DayofWeek" INTEGER,
    "Site_Name" VARCHAR(60),
    "Environment_Type" VARCHAR(35),
    "Zone" VARCHAR(65),
    "Altitude_m" FLOAT,
    "PM_25" FLOAT,
    "PM_10" FLOAT
);

-- insert data
COPY ukair ("datetime", "Hour", "Month", "DayofWeek", "Site_Name", "Environment_Type", "Zone", "Altitude_m", "PM_25", "PM_10")
FROM '/home/jqy/dataset/ukair/ukair2017_test.csv'
WITH (
    FORMAT CSV,
    HEADER TRUE,
    DELIMITER ',',
    NULL ''
);
```

### Step 2: Build and train a pipeline using scikit-learn-like API

```python
# specify the preprocessors
imputer = EncoderForgeSimpleImputer(strategy="most_frequent")
label_encoder = EncoderForgeLabelEncoder()
standard_scaler = StandardScaler()
onehot_encoder = EncoderForgeOneHotEncoder(handle_unknown="ignore")

# Imputing null value
step_imputer = ("Imputer", imputer)
X_copy = imputer.fit_transform(X_copy)


# scaling and encoding
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

# ML model
lr = LinearRegression()
step_estimator = (ModelName.LINEARREGRESSION.value, lr)

# combine preprocessors like encoders, and the ML model into an ML pipeline
pipeline = Pipeline([
    step_imputer,
    step_1_encoder,
    step_estimator
])

# train the ML pipeline
pipeline.fit(X, y)

# insert encoding mapping tables (EM tables) of encoders into the target database
defs.DBMS = 'postgresql'
insert_encoders_table_to_db(pipeline)

# save model
save_model(pipeline, pipeline_save_path)
```


### Step 3: Generate an efficient pure SQL query for the trained ML pipeline

```python
query = manager.generate_query(
    pipeline_file,
    table_name,
    dbms,
    pre_sql=pre_sql,
    order_when=False,
    group=group,
    cost_model='encoderforge',
    max_process_num=1,
    dp_way=dp_way
)
```

The part of generated SQL is as follows, see ./exmaple/generated_query.sql for the complete SQL.

```sql         
SET enable_mergejoin = off;                 
SET enable_nestloop = off;                 
EXPLAIN ANALYZE
SELECT
    "Hour" :: float * -0.094827 :: float + "Month" :: float * -0.538514 :: float + "DayofWeek" :: float * -0.137126 :: float + "Altitude_m" :: float * 0.463857 :: float + "PM_10" :: float * 7.986671 :: float + "Environment_Type_0" :: float * -0.930519 :: float + "Environment_Type_1" :: float * 0.724066 :: float + "Environment_Type_2" :: float * 0.117994 :: float + "Environment_Type_3" :: float * 0.088459 :: float + "Site_Name" :: float * 0.017326 :: float + "Zone" :: float * -0.011424 :: float + 9.059806404687478 :: float
FROM
    (
        SELECT
            0.144148410030892 :: float * "Hour" :: float - 1.65757944722357 :: float AS "Hour",
            0.289018566041717 :: float * "Month" :: float - 1.89321412906852 :: float AS "Month",
            0.497459505282944 :: float * "DayofWeek" :: float - 1.98911100629868 :: float AS "DayofWeek",
            0.023370767290347 :: float * "Altitude_m" :: float - 1.05903940830843 :: float AS "Altitude_m",
            0.0853502010931776 :: float * "PM_10" :: float - 1.27192567278553 :: float AS "PM_10",
            "Site_Name",
            "Zone",
            "Environment_Type_0",
            "Environment_Type_1",
            "Environment_Type_2",
            "Environment_Type_3"
        FROM
            (
                SELECT
                    "Hour",
                    "Month",
                    "DayofWeek",
                    "Altitude_m",
                    "PM_10",
                    "site_name_cat_c_cat_col" AS "Site_Name",
                    "zone_cat_c_cat_col" AS "Zone",
                    "environment_type_0" AS "Environment_Type_0",
                    "environment_type_1" AS "Environment_Type_1",
                    "environment_type_2" AS "Environment_Type_2",
                    "environment_type_3" AS "Environment_Type_3"
                FROM
                    ukair
                    left join merged_site_name_zone_environment_type_table on ukair."Site_Name" = merged_site_name_zone_environment_type_table."site_name"
                    AND ukair."Zone" = merged_site_name_zone_environment_type_table."zone"
                    AND ukair."Environment_Type" = merged_site_name_zone_environment_type_table."environment_type"
            ) AS data
    ) AS data          
```

### Step 4: Execute the generated pure SQL query in PostgreSQL

```shell
psql db_name postgres
\i path/to/sql-file.sql  # i.e., ./exmaple/generated_query.sql
```

## Datasets Links

|  Dataset  |                             Link                             |
| :-------: | :----------------------------------------------------------: |
|   UKAir   | https://www.openml.org/search?type=data&id=42207&status=active&sort=runs |
|    Job    | https://www.kaggle.com/datasets/uom190346a/ai-powered-job-market-insights |
|   Adult   | https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data |
|   Bank    | https://www.kaggle.com/datasets/marusagar/bank-transaction-fraud-detection |
|    Cat    |         https://www.kaggle.com/c/cat-in-the-dat/data         |
|  Criteo   |             https://ailab.criteo.com/ressources/             |
| Mushrooms |  https://www.kaggle.com/datasets/joebeachcapital/mushrooms   |
|    Kdd    | https://www.kaggle.com/datasets/towhidultonmoy/kddcup98-dataset |

## Code Structure

- `encoderforge/base/` directory contains the definitions of the main data structures.
- `encoderforge/cost_model/` directory contains data structures related to the cost model.
- `encoderforge/model/` directory contains the definitions of supported machine learning models.
- `encoderforge/preprocess/` directory contains the definitions of supported preprocessing operators.
- `encoderforge/optimizer/` directory contains the logic for translation plan selection strategies.
- `encoderforge/utility/` directory contains utility functions and the definition of preprocessing operators wrapped by encoderforge.
- `encoderforge/transformer_manager.py` file contains the entry function `TransformerManager.generate_query()`, which implements the logic of generating the complete SQL query according to the selected translation plan.
