import re
import importlib

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import LabelEncoder, KBinsDiscretizer, OrdinalEncoder, OneHotEncoder
from scipy import sparse
from category_encoders import TargetEncoder, CountEncoder, LeaveOneOutEncoder, BinaryEncoder, CatBoostEncoder, BaseNEncoder, HashingEncoder

from encoderforge.base.defs import *
import encoderforge.base.defs as defs
from encoderforge.base.graph import PrepGraph
from encoderforge.base.operator import EncoderOperator, CAT_C_CAT, EXPAND
from encoderforge.utility.join_utils import insert_db, df_type2db_type
from encoderforge.preprocess.k_bins_discretizer import KBinsDiscretizerSQLOperator
from encoderforge.preprocess.one_hot_encoder import OneHotEncoderSQLOperator
from encoderforge.preprocess.binary_encoder import BinaryEncoderSQLOperator


class EncoderForgeColumnTransformer(ColumnTransformer):

    def __init__(
        self,
        transformers,
        input_data,
        *,
        remainder="drop",
        sparse_threshold=0.3,
        n_jobs=None,
        transformer_weights=None,
        verbose=False,
        verbose_feature_names_out=True,
    ):
        super().__init__(
            transformers,
            remainder=remainder,
            sparse_threshold=sparse_threshold,
            n_jobs=n_jobs,
            transformer_weights=transformer_weights,
            verbose=verbose,
            verbose_feature_names_out=verbose_feature_names_out,
        )
        self.feature_names_in_: list[str]
        self.feature_names_out_: list[str]
        self.input_data = None

        self.__init_transform_data_columns(input_data)

    def __camel_to_snake(self, name: str):
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def __init_transform_data_columns(self, input_data: pd.DataFrame):
        self.feature_names_in_ = input_data.columns.tolist()
        self.feature_names_out_ = []
        features_trans = []
        for idx, (trans_name, fitted_trans, trans_features) in enumerate(
            self.transformers
        ):
            module_name = self.__camel_to_snake(trans_name.split("_")[0])
            transform_module = importlib.import_module(
                PREPROCESS_PACKAGE_PATH + module_name
            )
            operator_class = getattr(
                transform_module, trans_name.split("_")[0] + "SQLOperator"
            )

            # replace the trans_features for the last expand operator
            new_trans_features = []
            for feature in trans_features:
                if feature in self.feature_names_in_:
                    new_trans_features.append(feature)
                else:
                    after_expand_features = [
                        f_in for f_in in self.feature_names_in_ if feature in f_in
                    ]
                    if not after_expand_features:
                        after_expand_features = [
                            f_in for f_in in self.feature_names_in_ if defs.HASHING_ENCODER_FEATURE in f_in and f_in not in new_trans_features
                        ]
                    new_trans_features.extend(after_expand_features)

            features_trans.extend(new_trans_features)
            self.transformers[idx] = (trans_name, fitted_trans, new_trans_features)
            self.feature_names_out_.extend(
                operator_class.trans_feature_names_in(input_data[new_trans_features])
            )

        features_remain = [
            feature
            for feature in self.feature_names_in_
            if feature not in features_trans
        ]
        self.feature_names_out_.extend(features_remain)

    def fit(self, X, y=None):
        X = pd.DataFrame(X, columns=self.feature_names_in_)
        return super().fit(X, y)

    def transform(self, X):
        X = pd.DataFrame(X, columns=self.feature_names_in_)
        trans_data = super().transform(X)
        if isinstance(trans_data, sparse.spmatrix):
            return pd.DataFrame(trans_data.toarray(), columns=self.feature_names_out_)
        else:
            return pd.DataFrame(trans_data, columns=self.feature_names_out_)

    def fit_transform(self, X, y=None):
        X = pd.DataFrame(X, columns=self.feature_names_in_)
        trans_data = super().fit_transform(X, y)
        if isinstance(trans_data, sparse.spmatrix):
            return pd.DataFrame(trans_data.toarray(), columns=self.feature_names_out_)
        else:
            return pd.DataFrame(trans_data, columns=self.feature_names_out_)


class EncoderForgeSimpleImputer(SimpleImputer):
    def __init__(
        self,
        *,
        missing_values=np.nan,
        strategy="mean",
        fill_value=None,
        copy=True,
        add_indicator=False,
        keep_empty_features=False,
    ):
        super().__init__(
            missing_values=missing_values,
            strategy=strategy,
            fill_value=fill_value,
            copy=copy,
            add_indicator=add_indicator,
            keep_empty_features=keep_empty_features,
        )
        self.cols: list[str]
        self.missing_cols: list[str]
        self.missing_col_indexs: list[int]

    def fit(self, X, y=None, **fit_params):
        self.cols = X.columns.tolist()
        # indentify the columns which have the missing values
        missing_values = X.isnull().any()
        missing_columns = missing_values[missing_values].index.to_list()
        self.missing_cols = missing_columns
        self.missing_col_indexs = [self.cols.index(col) for col in self.missing_cols]
        super().fit(X, y)
        return self

    def transform(self, X, y=None, **fit_params):
        imputed_X = super().transform(X)
        return pd.DataFrame(imputed_X, columns=self.cols)

    def fit_transform(self, X, y=None, **fit_params):
        self.cols = X.columns.tolist()
        # indentify the columns which have the missing values
        missing_values = X.isnull().any()
        missing_columns = missing_values[missing_values].index.to_list()
        self.missing_cols = missing_columns
        self.missing_col_indexs = [self.cols.index(col) for col in self.missing_cols]
        imputed_X = super().fit_transform(X, y)
        return pd.DataFrame(imputed_X, columns=self.cols)


class EncoderForgeLabelEncoder(LabelEncoder):

    def __init__(self) -> None:
        super().__init__()

    def fit(self, X, y=None):
        if not hasattr(self, 'value_counts'):
            self.value_counts = {X.columns[0]: X.value_counts()}
        super().fit(X)

    def transform(self, X, y=None):
        trans_data = super().transform(X).reshape(-1, 1)
        return pd.DataFrame(trans_data)

    def fit_transform(self, X, y=None, **fit_params):
        if not hasattr(self, 'value_counts'):
            self.value_counts = {X.columns[0]: X.value_counts()}
        trans_data = super().fit_transform(X).reshape(-1, 1)
        return pd.DataFrame(trans_data)


class EncoderForgeTargetEncoder(TargetEncoder):

    def __init__(
        self, verbose=0, cols=None, drop_invariant=False, return_df=True, handle_missing='value',
                 handle_unknown='value', min_samples_leaf=20, smoothing=10, hierarchy=None
    ):
        super().__init__(
            verbose=verbose,
            cols=cols,
            drop_invariant=drop_invariant,
            return_df=return_df,
            handle_missing=handle_missing,
            handle_unknown=handle_unknown,
            min_samples_leaf=min_samples_leaf,
            smoothing=smoothing,
            hierarchy=hierarchy,
        )

    def fit(self, X, y=None, **kwargs):
        X = pd.DataFrame(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in X.columns:
                self.value_counts[col] = X[col].value_counts()
        non_string_columns = X.select_dtypes(exclude=['object']).columns
        X[non_string_columns] = X[non_string_columns].astype(str)
        super().fit(X, y, **kwargs)
        return self

    def transform(self, X, y=None, override_return_df=False):
        X = pd.DataFrame(X)
        trans_data = super().transform(X, y, override_return_df=override_return_df)
        return pd.DataFrame(trans_data)

    def fit_transform(self, X, y=None, **fit_params):
        X = pd.DataFrame(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in X.columns:
                self.value_counts[col] = X[col].value_counts()
        non_string_columns = X.select_dtypes(exclude=['object']).columns
        column_types = X.dtypes
        self.non_string_columns = {col: column_types[col] for col in non_string_columns} 
        X[non_string_columns] = X[non_string_columns].astype(str)
        trans_data = super().fit_transform(X, y, **fit_params)
        return pd.DataFrame(trans_data)

class EncoderForgeCatBoostEncoder(CatBoostEncoder):

    def __init__(
        self, verbose=0, cols=None, drop_invariant=False, return_df=True, handle_missing='value',
                 handle_unknown='value', min_samples_leaf=20, smoothing=10, hierarchy=None
    ):
        super().__init__(
            verbose=verbose,
            cols=cols,
            drop_invariant=drop_invariant,
            return_df=return_df,
            handle_missing=handle_missing,
            handle_unknown=handle_unknown,
            min_samples_leaf=min_samples_leaf,
            smoothing=smoothing,
            hierarchy=hierarchy,
        )

    def fit(self, X, y=None, **kwargs):
        X = pd.DataFrame(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in X.columns:
                self.value_counts[col] = X[col].value_counts()
        non_string_columns = X.select_dtypes(exclude=['object']).columns
        X[non_string_columns] = X[non_string_columns].astype(str)
        super().fit(X, y, **kwargs)
        return self

    def transform(self, X, y=None, override_return_df=False):
        X = pd.DataFrame(X)
        trans_data = super().transform(X, y, override_return_df=override_return_df)
        return pd.DataFrame(trans_data)

    def fit_transform(self, X, y=None, **fit_params):
        X = pd.DataFrame(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in X.columns:
                self.value_counts[col] = X[col].value_counts()
        non_string_columns = X.select_dtypes(exclude=['object']).columns
        column_types = X.dtypes
        self.non_string_columns = {col: column_types[col] for col in non_string_columns} 
        X[non_string_columns] = X[non_string_columns].astype(str)
        trans_data = super().fit_transform(X, y, **fit_params)
        return pd.DataFrame(trans_data)

class EncoderForgeKBinsDiscretizer(KBinsDiscretizer):

    def __init__(
        self,
        n_bins=5,
        *,
        encode="onehot",
        strategy="quantile",
        dtype=None,
        subsample="warn",
        random_state=None,
    ):
        super().__init__(
            n_bins=n_bins,
            encode=encode,
            strategy=strategy,
            dtype=dtype,
            subsample=subsample,
            random_state=random_state,
        )
    
    def fit(self, X, y=None, sample_weight=None):
        X = pd.DataFrame(X)
        super().fit(X, y, sample_weight)
        return self

    def transform(self, X):
        X = pd.DataFrame(X)
        trans_data = super().transform(X)
        return pd.DataFrame(trans_data, columns=X.columns)

    def fit_transform(self, X, y=None, **fit_params):
        X = pd.DataFrame(X)
        trans_data = super().fit_transform(X, y, **fit_params)
        self.bin_distribution = {}
        for column in trans_data.columns:
            trans_data[column] = trans_data[column].astype(np.int64)
            col_data = trans_data[column].values
            col_data = col_data[np.argsort(col_data)]
            self.bin_distribution[column] = np.bincount(col_data)
            
        return trans_data


class EncoderForgeOrdinalEncoder(OrdinalEncoder):

    def __init__(
        self,
        *,
        categories="auto",
        dtype=np.float64,
        handle_unknown="error",
        unknown_value=None,
        encoded_missing_value=np.nan,
        min_frequency=None,
        max_categories=None,
    ):
        super().__init__(
            categories=categories,
            dtype=dtype,
            handle_unknown=handle_unknown,
            unknown_value=unknown_value,
            encoded_missing_value=encoded_missing_value,
            min_frequency=min_frequency,
            max_categories=max_categories,
        )
        
    def fit(self, X, y=None):
        X = pd.DataFrame(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in X.columns:
                self.value_counts[col] = X[col].value_counts()
        super().fit(X, y)
        return self

    def transform(self, X):
        X = pd.DataFrame(X)
        trans_data = super().transform(X)
        return pd.DataFrame(trans_data, columns=X.columns)

    def fit_transform(self, X, y=None, **fit_params):
        X = pd.DataFrame(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in X.columns:
                self.value_counts[col] = X[col].value_counts()
        trans_data = super().fit_transform(X, y, **fit_params)
            
        return trans_data
    
    
class EncoderForgeCountEncoder(CountEncoder):

    def __init__(self, verbose=0, cols=None, drop_invariant=False,
                 return_df=True, handle_unknown='value',
                 handle_missing='value',
                 min_group_size=None, combine_min_nan_groups=None,
                 min_group_name=None, normalize=False):
        
        super().__init__(verbose=verbose, cols=cols, drop_invariant=drop_invariant, return_df=return_df,
                         handle_unknown=handle_unknown, handle_missing=handle_missing,
                         min_group_size=min_group_size, combine_min_nan_groups=combine_min_nan_groups,
                         min_group_name=min_group_name, normalize=normalize)
        
    def fit(self, X, y=None, **kwargs):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in X.columns:
                self.value_counts[col] = X[col].value_counts()
        super().fit(X, y, **kwargs)
        return self

    def transform(self, X, override_return_df=False):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        trans_data = super().transform(X, override_return_df=override_return_df)
        return pd.DataFrame(trans_data, columns=X.columns)

    def fit_transform(self, X, y=None, **fit_params):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in X.columns:
                self.value_counts[col] = X[col].value_counts()
        trans_data = super().fit_transform(X, y, **fit_params)
        return trans_data
    
    
class EncoderForgeLeaveOneOutEncoder(LeaveOneOutEncoder):

    def __init__(self, verbose=0, cols=None, drop_invariant=False, return_df=True,
                 handle_unknown='value', handle_missing='value', random_state=None, sigma=None):
        super().__init__(verbose=verbose, cols=cols, drop_invariant=drop_invariant, return_df=return_df,
                         handle_unknown=handle_unknown, handle_missing=handle_missing,
                         random_state=random_state, sigma=sigma)
        
    def fit(self, X, y=None, **kwargs):
        X = pd.DataFrame(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in X.columns:
                self.value_counts[col] = X[col].value_counts()
        super().fit(X, y, **kwargs)
        return self

    def transform(self, X, y=None, override_return_df=False):
        X = pd.DataFrame(X)
        trans_data = super().transform(X, override_return_df=override_return_df)
        return pd.DataFrame(trans_data, columns=X.columns)

    def fit_transform(self, X, y=None, **fit_params):
        X = pd.DataFrame(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in X.columns:
                self.value_counts[col] = X[col].value_counts()
        trans_data = super().fit_transform(X, y, **fit_params)
        return trans_data


class EncoderForgeOneHotEncoder(OneHotEncoder):
    def __init__(
        self,
        *,
        categories="auto",
        drop=None,
        sparse="deprecated",
        sparse_output=True,
        dtype=np.float64,
        handle_unknown="error",
        min_frequency=None,
        max_categories=None,
        feature_name_combiner="concat",
    ):
        super().__init__(categories=categories, drop=drop, sparse=sparse, 
                         sparse_output=sparse_output, dtype=dtype, handle_unknown=handle_unknown,
                         min_frequency=min_frequency, max_categories=max_categories,
                         feature_name_combiner=feature_name_combiner)
        
    def fit(self, X, y=None):
        X = pd.DataFrame(X)
        super().fit(X, y)
        trans_data = self.transform(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in trans_data.columns:
                self.value_counts[col] = trans_data[col].value_counts()
        return self
    
    def transform(self, X):
        X = pd.DataFrame(X)
        trans_data = super().transform(X).toarray()
        features_out = []
        for idx, feature in enumerate(self.feature_names_in_):
            features_out.extend(feature + f'_{i}' for i in range(len(self.categories_[idx])))
        return pd.DataFrame(trans_data, columns=features_out)
    
    
    def fit_transform(self, X, y=None, **fit_params):
        X = pd.DataFrame(X)
        trans_data = super().fit_transform(X, y, **fit_params)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in trans_data.columns:
                self.value_counts[col] = trans_data[col].value_counts()
        
        return trans_data
    
    
class EncoderForgeBinaryEncoder(BinaryEncoder):
    
    def __init__(self, verbose=0, cols=None, mapping=None, drop_invariant=False, return_df=True, base=2,
                 handle_unknown='value', handle_missing='value'):
        super().__init__(verbose=verbose, cols=cols, mapping=mapping, drop_invariant=drop_invariant, return_df=return_df,
                         base=base, handle_unknown=handle_unknown, handle_missing=handle_missing)
        
    def fit(self, X, y=None, **kwargs):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        super().fit(X, y, **kwargs)
        trans_data = self.transform(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in trans_data.columns:
                self.value_counts[col] = trans_data[col].value_counts()
        return self

    def transform(self, X, override_return_df=False):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        trans_data = super().transform(X, override_return_df=override_return_df)
        return pd.DataFrame(trans_data)

    def fit_transform(self, X, y=None, **fit_params):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        trans_data = super().fit_transform(X, y, **fit_params)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in trans_data.columns:
                self.value_counts[col] = trans_data[col].value_counts()
        return trans_data

class EncoderForgeBaseNEncoder(BaseNEncoder):
    
    def __init__(self, verbose=0, cols=None, mapping=None, drop_invariant=False, return_df=True, base=2,
                 handle_unknown='value', handle_missing='value'):
        super().__init__(verbose=verbose, cols=cols, mapping=mapping, drop_invariant=drop_invariant, return_df=return_df,
                         base=base, handle_unknown=handle_unknown, handle_missing=handle_missing)
        
    def fit(self, X, y=None, **kwargs):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        super().fit(X, y, **kwargs)
        trans_data = self.transform(X)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in trans_data.columns:
                self.value_counts[col] = trans_data[col].value_counts()
        return self

    def transform(self, X, override_return_df=False):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        trans_data = super().transform(X, override_return_df=override_return_df)
        return pd.DataFrame(trans_data)

    def fit_transform(self, X, y=None, **fit_params):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        trans_data = super().fit_transform(X, y, **fit_params)
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in trans_data.columns:
                self.value_counts[col] = trans_data[col].value_counts()
        return trans_data  

class EncoderForgeHashingEncoder(HashingEncoder):
    
    def __init__(self, max_process=0, max_sample=0, verbose=0, n_components=8, cols=None, drop_invariant=False,
                 return_df=True, hash_method='md5'):
        super().__init__(max_process=max_process, max_sample=max_sample, verbose=verbose, n_components=n_components, cols=cols, drop_invariant=drop_invariant,
                 return_df=return_df, hash_method=hash_method)
        
    def fit(self, X, y=None, **kwargs):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        super().fit(X, y, **kwargs)
        trans_data = self.transform(X)
        X_unique = X.drop_duplicates()
        self.x_unique = X.unique()
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in trans_data.columns:
                self.value_counts[col] = trans_data[col].value_counts()
        return self

    def transform(self, X, override_return_df=False):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        trans_data = super().transform(X, override_return_df=override_return_df)
        return pd.DataFrame(trans_data)

    def fit_transform(self, X, y=None, **fit_params):
        X = pd.DataFrame(X)
        for col in X.columns:
            X[col] = X[col].astype(str)
        trans_data = super().fit_transform(X, y, **fit_params)
        self.x_unique = X.unique()
        if not hasattr(self, 'value_counts'):
            self.value_counts = {}
            for col in trans_data.columns:
                self.value_counts[col] = trans_data[col].value_counts()
        return trans_data  
    
def insert_encoders_table_to_db(pipeline):
    steps = pipeline.steps
    column_transformer_start_idx = 0
    pipeline_features_in = pipeline.feature_names_in_.tolist()
    fitted_imputer = None
    # if contain imputer extract it
    if 'Imputer' == steps[0][0]:
        fitted_imputer = steps[0][1]
        column_transformer_start_idx = 1

    # extract model
    model_name, trained_model = steps[-1]

    # extract preprocessors
    transforms = []
    for i in range(column_transformer_start_idx, len(steps) - 1):
        _, pipeline_transformers = steps[i]
        for idx in range(len(pipeline_transformers.transformers)):
            a, b, c = pipeline_transformers.transformers_[idx]
            a = a.split('_')[0]
            transforms.append({
                'transform_name': a,
                'fitted_transform': b,
                'transform_features': c,
            })

    pipeline =  {
        'imputer': {
            'filled_values': fitted_imputer.statistics_ if fitted_imputer else [],
            'missing_cols': fitted_imputer.missing_cols if fitted_imputer else [],
            'missing_col_indexs': fitted_imputer.missing_col_indexs if fitted_imputer else []
        },
        'transforms': transforms,
        'model': {
            'model_name': model_name,
            'trained_model': trained_model
        }
    }
    
    # insert_db LR_on
    # LR_down = not (model_name == "LinearRegression")
    # LR_down = (model_name == "LinearRegression")
    LR_down = False
    
    # build the graph of the preprocessing operators
    preprocessing_graph = PrepGraph(pipeline_features_in, pipeline)
    if preprocessing_graph.model is not None:
        tree_sql = preprocessing_graph.model.query("", defs.DBMS)
        used_columns,dict = extract_columns_from_sql(tree_sql,LR_down)
        
        for feature, chain in preprocessing_graph.chains.items():
            for op in chain.prep_operators:
            # if(isinstance(op, KBinsDiscretizerSQLOperator) and feature in used_columns):
            #     join_table_name = feature + CON_C_CAT_JOIN_POSTNAME
            #     join_table_name = join_table_name.lower()
            #     cols = {feature.lower()+'_low': DBDataType.FLOAT.value,
            #             feature.lower()+'_up': DBDataType.FLOAT.value,
            #             feature.lower()+'_value': DBDataType.INT.value
            #                 }
            #     data = []
            #     if(op.n_bins[0] > 1):
            #         for i in range(op.n_bins[0]):
            #             if(i==op.n_bins[0]-1):
            #                 data.append((op.bin_edges[0][i],op.bin_edges[0][i+1]+1,int(np.int64(op.categories[0][i]))))
            #                 break
            #             data.append((op.bin_edges[0][i],op.bin_edges[0][i+1],int(np.int64(op.categories[0][i]))))
            #     else:
            #         pass
                
            #     insert_db(defs.DBMS, join_table_name, cols, data)
               
                if isinstance(op, EncoderOperator):
                    if isinstance(op, CAT_C_CAT):
                        feature = op.features[0]
                        if feature in used_columns:
                            mapping = op.mappings[0]
                            join_table_name = feature + CAT_C_CAT_JOIN_POSTNAME
                            join_table_name = join_table_name.lower()
                            cols = {feature.lower(): DBDataType.VARCHAR.value if (defs.DBMS != 'monetdb' and defs.DBMS != 'tidb') else DBDataType.VARCHAR512.value}
                            col_name = feature + CAT_C_CAT_JOIN_COL_POSTNAME
                            col_name = col_name.lower()
                            cols[col_name] = df_type2db_type(mapping.dtype, defs.DBMS)
                            data = [
                                (idx, mapping.tolist()[mapping.index.get_loc(idx)]) for idx in mapping.index
                            ]
                            insert_db(defs.DBMS, join_table_name, cols, data)
                        
                    elif isinstance(op, EXPAND):
                        if(isinstance(op, OneHotEncoderSQLOperator) or isinstance(op, BinaryEncoderSQLOperator)) and (defs.DBMS != 'monetdb' and defs.DBMS != 'tidb'):
                            # todo: POSTNAME_form
                            join_table_name = feature + ARRAY_JOIN_POSTNAME
                            join_table_name = join_table_name.lower()
                            cols = {feature.lower(): df_type2db_type(op.mapping.index.dtype,defs.DBMS)}
                            # print(feature, df_type2db_type(op.mapping.index.dtype,defs.DBMS),DBDataType.VARCHAR.value)
                            # cols = {feature.lower(): DBDataType.VARCHAR.value if (defs.DBMS != 'monetdb' and defs.DBMS != 'tidb') else DBDataType.VARCHAR512.value}
                            col_name = feature + ARRAY_JOIN_COL_POSTNAME
                            col_name = col_name.lower()
                            cols[col_name] = f"{df_type2db_type(op.mapping[op.mapping.columns[0]].dtype, defs.DBMS)}[]" 
                            if(LR_down):
                                col_name = feature + ARRAY_JOIN_POSTNAME + "_value"
                                col_name = col_name.lower()
                                cols[col_name] = DBDataType.FLOAT.value
                            data = []
                            # filter used_columns
                            used_cols = [col for col in op.mapping.columns if col in used_columns]
                            for idx in op.mapping.index:
                                array_series = op.mapping.loc[idx, used_cols]
                                data_tuple = array_series.tolist()
                                value = None
                                if(LR_down):
                                    value = (array_series * array_series.index.map(dict)).sum()
                                if 1 in data_tuple:
                                    if value is None:
                                        data.append((idx, data_tuple))
                                    else:
                                        data.append((idx, data_tuple, value))
                            if(len(data) > 0):
                                insert_db(defs.DBMS, join_table_name, cols, data)
                        feature = op.features[0]
                        join_table_name = feature + EXPAND_JOIN_POSTNAME
                        join_table_name = join_table_name.lower()
                        if (isinstance(chain.prep_operators[chain.prep_operators.index(op) - 1], KBinsDiscretizerSQLOperator)):
                            cols = {feature.lower():DBDataType.INT.value} 
                        elif defs.DBMS == 'monetdb' or defs.DBMS == 'tidb':
                            cols = {feature.lower():DBDataType.VARCHAR512.value} 
                        else:
                            cols = {feature.lower(): DBDataType.VARCHAR.value}
                        # cols = {feature.lower(): DBDataType.VARCHAR.value if defs.DBMS != 'monetdb' else DBDataType.VARCHAR512.value}
                        for col in op.mapping.columns:
                            if col in used_columns:
                                cols[col.lower()] = df_type2db_type(op.mapping[col].dtype, defs.DBMS)
                        if len(cols) <= 1:  # feature only
                            continue
                        data = []
                        used_cols = [col for col in op.mapping.columns if col in used_columns]
                        for idx in op.mapping.index:
                            data_tuple = tuple(op.mapping.loc[idx, used_cols])
                            # if 1 in data_tuple:
                            data.append((idx,) + data_tuple)
                        insert_db(defs.DBMS, join_table_name, cols, data)
               
    else:
        for feature, chain in preprocessing_graph.chains.items():
            for op in chain.prep_operators:
                if isinstance(op, EncoderOperator):
                    if isinstance(op, CAT_C_CAT):
                        feature = op.features[0]
                        mapping = op.mappings[0]
                        join_table_name = feature + CAT_C_CAT_JOIN_POSTNAME
                        join_table_name = join_table_name.lower()
                        cols = {feature.lower(): DBDataType.VARCHAR.value if (defs.DBMS != 'monetdb' and defs.DBMS != 'tidb')  else DBDataType.VARCHAR512.value}
                        col_name = feature + CAT_C_CAT_JOIN_COL_POSTNAME
                        col_name = col_name.lower()
                        cols[col_name] = df_type2db_type(mapping.dtype, defs.DBMS)
                        data = [
                            (idx, mapping.tolist()[mapping.index.get_loc(idx)]) for idx in mapping.index
                        ]
                        insert_db(defs.DBMS, join_table_name, cols, data)
                        
                    elif isinstance(op, EXPAND):
                        if(isinstance(op, OneHotEncoderSQLOperator) or isinstance(op, BinaryEncoderSQLOperator)) and (defs.DBMS != 'monetdb' and defs.DBMS != 'tidb'):
                            # todo: POSTNAME_form
                            join_table_name = feature + ARRAY_JOIN_POSTNAME
                            join_table_name = join_table_name.lower()
                            cols = {feature.lower(): df_type2db_type(op.mapping.index.dtype,defs.DBMS)}
                            col_name = feature + ARRAY_JOIN_COL_POSTNAME
                            col_name = col_name.lower()
                            cols[col_name] = f"{df_type2db_type(op.mapping[op.mapping.columns[0]].dtype, defs.DBMS)}[]" 
                            # if(LR_down):
                            #     col_name = feature + ARRAY_JOIN_POSTNAME + "_value"
                            #     col_name = col_name.lower()
                            #     cols[col_name] = DBDataType.FLOAT.value
                            data = []
                            # filter used_columns
                            used_cols = [col for col in op.mapping.columns]
                            for idx in op.mapping.index:
                                array_series = op.mapping.loc[idx, used_cols]
                                data_tuple = array_series.tolist()
                                value = None
                                # if(LR_down):
                                #     value = (array_series * array_series.index.map(dict)).sum()
                                if 1 in data_tuple:
                                    if value is None:
                                        data.append((idx, data_tuple))
                                    else:
                                        data.append((idx, data_tuple, value))
                            if(len(data) > 0):
                                insert_db(defs.DBMS, join_table_name, cols, data)
                        feature = op.features[0]
                        join_table_name = feature + EXPAND_JOIN_POSTNAME
                        join_table_name = join_table_name.lower()
                        # cols = {feature.lower(): DBDataType.VARCHAR.value if defs.DBMS != 'monetdb' else DBDataType.VARCHAR512.value}
                        cols = {feature.lower(): df_type2db_type(op.mapping.index.dtype,defs.DBMS) if defs.DBMS!='tidb' else DBDataType.VARCHAR512.value}
                        for col in op.mapping.columns:
                            cols[col.lower()] = df_type2db_type(op.mapping[col].dtype, defs.DBMS)
                        if len(cols) <= 1:  # feature
                            continue
                        data = []
                        used_cols = [col for col in op.mapping.columns]
                        for idx in op.mapping.index:
                            data_tuple = tuple(op.mapping.loc[idx, used_cols])
                            # if 1 in data_tuple:
                            data.append((idx,) + data_tuple)
                        insert_db(defs.DBMS, join_table_name, cols, data)

def extract_columns_from_sql(sql, LR_down):
    used_columns = set()
    dict = {}
    
    # WHEN pattern for tree models
    when_pattern = r'WHEN\s+"([^"]+)"'
    when_matches = re.findall(when_pattern, sql)
    for col in when_matches:
        used_columns.add(col)
        
    # weight * "column" pattern for linear models
    lr_pattern = r'"(.*?)"\s*\*\s*([-]?\d+\.\d+|\d+)'
    lr_matches = re.findall(lr_pattern, sql)
    for col,coeff in lr_matches:
        if LR_down:
            dict[col] = float(coeff)
        used_columns.add(col)
        
    additional_columns = set()
    for col in used_columns:
        parts = col.split('_')
        if len(parts) >= 2 and parts[-1].isdigit():
            prefix = '_'.join(parts[:-1])
            additional_columns.add(prefix)
    used_columns.update(additional_columns)
                
    return used_columns, dict
