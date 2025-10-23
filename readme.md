# EncoderForge: High-Efficiency SQL Generation for ML Pipelines

## Project Introduction

EncoderForge is a tool to generate highly efficient SQL queries from trained ML pipelines. It allows pipelines trained in Python to be executed natively in databases with high efficiency, supporting different DBMS like DuckDB, PostgreSQL.

## Quick Start

We use the UKAir Quality dataset as an example to illustrate how to build a pipeline, generate SQL, and execute it in a database.

### Step 1: Prepare dataset in database

```sql
-- Example for postgresql
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

### Step 2: Build and train a pipeline using Python

```python
# imputer step
step_imputer = ("Imputer", imputer)

# encoder step
transformer_step1 = EncoderForgeColumnTransformer(
    remainder="passthrough",
    transformers=[
        ...
        (
            OperatorName.ONEHOTENCODER.value,
            onehot_encoder,
            low_feature,
        )
    ],
    input_data=X_copy
)
encoder_step1 = ("ColumnTransformer_step1", transformer_step1)

# model step
step_estimator = (ModelName.LINEARREGRESSION.value, LinearRegression())

# pipeline structure
pipeline = Pipeline([
    step_imputer,
    encoder_step1,
    step_estimator
])
```


### Step 3: Generate SQL from trained pipeline

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

```sql
SET max_parallel_workers_per_gather = 3;                 
SET statement_timeout = 3600000;                 
SET enable_mergejoin = off;                 
SET enable_nestloop = off;                 
EXPLAIN ANALYZE 
SELECT
		"Hour"::float * -0.094827::float + "Month"::float * -0.538514::float + ... + "Zone"::float * -0.011424::float + 9.059806404687478::float 
    FROM 
        (
          SELECT 
              0.144148410030892::float*"Hour"::float - 1.65757944722357::float AS "Hour",
              0.289018566041717::float*"Month"::float - 1.89321412906852::float AS "Month",
              ...
              "Zone",
              "Site_Name",
              CASE
                  WHEN "Environment_Type" = 'Background Rural' THEN 1
                  ELSE 0
              END AS "Environment_Type_0",
              ...
              FROM
                  (
                      SELECT
                          "Hour",
                          "Month",
                          "DayofWeek",
                          "Altitude_m",
                          "PM_10",
                          "zone_cat_c_cat_col" AS "Zone",
                          "site_name_cat_c_cat_col" AS "Site_Name",
                          "Environment_Type"
                      FROM
                          ukair
                          left join merged_zone_site_name_table on ukair."Zone" = merged_zone_site_name_table."zone"
                          AND ukair."Site_Name" = merged_zone_site_name_table."site_name"
                  ) AS data
        ) AS data
```

### Step 4: Execute SQL in database

```shell
psql db_name postgres
\i path/to/sql-file.sql
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
- `encoderforge/rule_based_optimize/` directory contains the logic for encoder table merge
- `encoderforge/utility/` directory contains utility functions and the definition of preprocessing operators wrapped by encoderforge.
- `encoderforge/transformer_manager.py` file contains the entry function `TransformerManager.generate_query()`, which includes the SQL assembly logic.