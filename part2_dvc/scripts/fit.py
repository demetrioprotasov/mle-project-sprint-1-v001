import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from catboost import CatBoostRegressor
import yaml
import os
import joblib

# обучение модели
def fit_model():
    # Прочитайте файл с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    # загрузите результат предыдущего шага: inital_data.csv
    data = pd.read_csv('data/initial_data.csv')
    
    # реализуйте основную логику шага с использованием гиперпараметров
    target_col = params['target_col']
    num_features = params['num_features']
    cat_features = params['cat_features']
    cat_bin_features = params['cat_bin_features']
    preprocessor = ColumnTransformer(
        [
            ('binary', OneHotEncoder(drop=params['binary_one_hot_drop'], sparse_output=False), cat_bin_features),
            ('cat', OneHotEncoder(drop=params['cat_one_hot_drop'], sparse_output=False), cat_features),
            ('num', StandardScaler(), num_features)
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )
    model = CatBoostRegressor(random_state=params['random_state'], verbose=0)
    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )

    # Обучаем пайплайн, передавая целевую переменную через гиперпараметр
    pipeline.fit(data, data[target_col])
    
    # сохраните обученную модель в models/fitted_model.pkl
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd)

if __name__ == '__main__':
    fit_model()
