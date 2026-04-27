import pandas as pd
from sklearn.model_selection import KFold, cross_validate
import joblib
import json
import yaml
import os

# оценка качества модели
def evaluate_model():
    # прочитайте файл с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    # загрузите результат прошлого шага: fitted_model.pkl
    with open('models/fitted_model.pkl', 'rb') as fd:
        pipeline = joblib.load(fd)
    data = pd.read_csv('data/initial_data.csv')

    # реализуйте основную логику шага с использованием прочтённых гиперпараметров
    cv_strategy = KFold(n_splits=params['n_splits'], shuffle=True, random_state=params['random_state'])
    cv_res = cross_validate(
        pipeline,
        data,
        data[params['target_col']],
        cv=cv_strategy,
        n_jobs=params['n_jobs'],
        scoring=params['metrics'],
        error_score='raise'
    )
    for key, value in cv_res.items():
        cv_res[key] = round(float(value.mean()), 3) 

    # сохраните результата кросс-валидации в cv_res.json
    os.makedirs('cv_results', exist_ok=True)
    with open('cv_results/cv_res.json', 'w') as fd:
        json.dump(cv_res, fd)

if __name__ == '__main__':
    evaluate_model()
