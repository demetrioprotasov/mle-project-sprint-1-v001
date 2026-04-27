# Проект 1 спринта
Решение проекта 1 спринта курса «ML-инженер с опытом»: ETL-пайплайн на Apache Airflow для подготовки данных и DVC-пайплайн для обучения базовой модели предсказания стоимости квартир Яндекс Недвижимости.

## Имя бакета S3
s3-student-mle-20260317-efc01cb482-freetrack

## Структура проекта

### Этап 1. Сбор данных (выполнено)
- DAG: `part1_airflow/dags/prepare_real_estate_dataset.py`
- Функция DAG: `prepare_dataset`
- Плагин уведомлений: `part1_airflow/plugins/telegram_notifier.py`
- Результирующая таблица в `destination_db`: `real_estate_raw`

### Этап 2. Очистка данных
- DAG: `part1_airflow/dags/clean_real_estate_dataset.py`
- Функция DAG: `clean_dataset`
- Ноутбук с EDA и функциями очистки: `part1_airflow/notebooks/eda_and_cleaning.ipynb`
- Результирующая таблица в `destination_db`: `real_estate_clean`

### Этап 3. DVC-пайплайн обучения модели
Базовая модель — `CatBoostRegressor` (выбран по результатам сравнения пяти алгоритмов на отложенной выборке: LinearRegression, RandomForest, LightGBM, XGBoost, CatBoost). Пайплайн целиком (препроцессор + модель) сохраняется в `models/fitted_model.pkl`.

#### Файлы пайплайна
- Описание пайплайна: `part2_dvc/dvc.yaml`
- Гиперпараметры: `part2_dvc/params.yaml`
- Снимок состояния артефактов: `part2_dvc/dvc.lock`

#### Шаги пайплайна
- `get_data` — `part2_dvc/scripts/data.py`. Читает таблицу `real_estate_clean` из `destination_db`, сохраняет в `data/initial_data.csv`.
- `fit_model` — `part2_dvc/scripts/fit.py`. Обучает `Pipeline` (препроцессор `ColumnTransformer` + `CatBoostRegressor`), сохраняет в `models/fitted_model.pkl`.
- `evaluate_model` — `part2_dvc/scripts/evaluate.py`. Кросс-валидация (`KFold`, 5 фолдов, `shuffle=True`), сохраняет метрики (`r2`, `neg_mean_absolute_error`, `neg_root_mean_squared_error`) в `cv_results/cv_res.json`.

#### Ноутбук с базовым экспериментом
- `part2_dvc/notebooks/model_training.ipynb` — сравнение моделей и обоснование выбора CatBoost.

#### Запуск пайплайна
```bash
cd part2_dvc
dvc repro
dvc metrics show
dvc push
```
