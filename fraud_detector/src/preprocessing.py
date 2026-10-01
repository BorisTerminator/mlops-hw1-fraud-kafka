"""Препроцессинг транзакций: сырая строка формата test.csv -> признаки модели.

Логика повторяет препроцессинг из соревнования (код семинара):
  * категории ранжируются по частоте в train, топ-50 получают свой код,
    остальные — 'cat_50+', пропуски и неизвестные значения — 'cat_NAN';
  * из transaction_time берутся час, год, месяц, день месяца и день недели;
  * для категорий и временных признаков считается mean target encoding;
  * расстояние клиент–мерчант по большому кругу;
  * пропуски в непрерывных признаках заполняются средним по train, затем log(x + 1).

Все статистики по train посчитаны заранее скриптом tools/build_artifacts.py
и лежат в artifacts/preprocessing.json, поэтому сервису не нужен train.csv.
"""
import json
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

CATEGORICAL_COLS = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs']
TIME_COLS = ['hour', 'year', 'month', 'day_of_month', 'day_of_week']
CONTINUOUS_COLS = ['amount', 'population_city', 'distance']
GEO_COLS = ['lat', 'lon', 'merchant_lat', 'merchant_lon']

# Ключ, под которым в JSON хранится категория для пропуска (NaN нельзя сделать ключом)
NAN_KEY = '__nan__'
UNKNOWN_CAT = 'cat_NAN'
EARTH_RADIUS_KM = 6371.009  # тот же радиус, что у geopy.distance.great_circle


def add_time_features(df):
    ts = pd.to_datetime(df['transaction_time'])
    df['hour'] = ts.dt.hour
    df['year'] = ts.dt.year
    df['month'] = ts.dt.month
    df['day_of_month'] = ts.dt.day
    df['day_of_week'] = ts.dt.dayofweek
    return df.drop(columns='transaction_time')


def great_circle_km(lat1, lon1, lat2, lon2):
    """Векторная версия geopy.distance.great_circle: та же формула, но без цикла по строкам."""
    lat1, lon1, lat2, lon2 = (np.radians(np.asarray(x, dtype=float)) for x in (lat1, lon1, lat2, lon2))
    delta_lon = lon2 - lon1
    y = np.sqrt(
        (np.cos(lat2) * np.sin(delta_lon)) ** 2
        + (np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(delta_lon)) ** 2
    )
    x = np.sin(lat1) * np.sin(lat2) + np.cos(lat1) * np.cos(lat2) * np.cos(delta_lon)
    return EARTH_RADIUS_KM * np.arctan2(y, x)


def add_distance_feature(df):
    df['distance'] = great_circle_km(df['lat'], df['lon'], df['merchant_lat'], df['merchant_lon'])
    return df.drop(columns=GEO_COLS)


def encode_key(value):
    """Значение признака -> строковый ключ JSON-таблицы."""
    return NAN_KEY if pd.isna(value) else str(value)


class Preprocessor:
    def __init__(self, artifacts_path):
        with open(artifacts_path, encoding='utf-8') as f:
            artifacts = json.load(f)
        self.cat_maps = artifacts['cat_maps']
        self.mean_enc = artifacts['mean_enc']
        self.impute_means = artifacts['impute_means']
        logger.info('Preprocessing artifacts loaded from %s', artifacts_path)

    def transform(self, raw_df):
        required = CATEGORICAL_COLS + GEO_COLS + ['transaction_time', 'amount', 'population_city']
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            raise KeyError(f'Input has no columns: {missing}')

        # Лишние колонки (name_1, street, post_code и т.п.) модели не нужны
        df = raw_df[required].copy()

        # Категории -> коды по частоте в train; неизвестное значение -> cat_NAN
        for col in CATEGORICAL_COLS:
            mapping = self.cat_maps[col]
            df[col + '_cat'] = df[col].map(lambda v: mapping.get(encode_key(v), UNKNOWN_CAT))
        df = df.drop(columns=CATEGORICAL_COLS)

        df = add_time_features(df)

        # Mean target encoding; категории, которых не было в train, получают NaN
        for col in [c + '_cat' for c in CATEGORICAL_COLS] + TIME_COLS:
            table = self.mean_enc[col]
            df[col + '_mean_enc'] = df[col].map(lambda v: table.get(encode_key(v), np.nan)).astype(float)

        df = add_distance_feature(df)

        for col in CONTINUOUS_COLS:
            values = pd.to_numeric(df[col], errors='coerce').fillna(self.impute_means[col])
            df[col + '_log'] = np.log(values + 1)
            df = df.drop(columns=col)

        return df.reset_index(drop=True)
