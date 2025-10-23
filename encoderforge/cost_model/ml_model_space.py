import h2o
import os
from autogluon.tabular import TabularPredictor

h2o_models = {}
h2o_models_config = {
    'postgresql': 'pg_automl_model_h2o_moremoredata',
    # 'postgresql': 'pg_automl_model_h2o_moremoredata_sub',
}

autogluon_models = {}
autogluon_models_config = {
    'duckdb': 'EncoderForge/encoderforge/cost_model/AutogluonModels/duckdb_automl_autogluon/',
    'duckdb_sub': 'EncoderForge/encoderforge/cost_model/AutogluonModels/duckdb_automl_autogluon_sub/',
    'postgresql': 'EncoderForge/encoderforge/cost_model/AutogluonModels/pg_automl_autogulon/',
    'postgresql_sub': 'EncoderForge/encoderforge/cost_model/AutogluonModels/pg_automl_autogulon_sub/',
    
}


def load_h2o_context():
    h2o.init()
    for db_name in h2o_models_config:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        h2o_model_path = os.path.join(BASE_DIR, "h2o_models", h2o_models_config[db_name])
        db_model = h2o.import_mojo(h2o_model_path)
        # db_model = h2o.import_mojo(h2o_models_config[db_name])
        h2o_models[db_name] = db_model

def load_autoluon():
     for db_name in autogluon_models_config:
        db_model = TabularPredictor.load(autogluon_models_config[db_name])
        autogluon_models[db_name] = db_model
        
    
def get_model(model_type:str, db_name:str):
    if model_type == 'autogluon':
        return autogluon_models[db_name]
    elif model_type == 'h2o':
        return h2o_models[db_name]
    # if db_name == 'duckdb':
    #     return autogluon_models[db_name]
    # elif db_name == 'postgresql':
    #     # return h2o_models[db_name]
    #     return h2o_models[db_name]