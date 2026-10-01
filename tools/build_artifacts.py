"""Считает по train.csv таблицы для препроцессинга и сохраняет их в JSON.

Запускается один раз вне Docker. Результат (fraud_detector/artifacts/preprocessing.json)
коммитится в репозиторий, поэтому сервису не нужен train.csv на 138 МБ.

    python tools/build_artifacts.py --train data/train.csv
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'fraud_detector' / 'src'))
from preprocessing import (  # noqa: E402
    CATEGORICAL_COLS, CONTINUOUS_COLS, NAN_KEY, TIME_COLS,
    add_distance_feature, add_time_features, encode_key,
)

TARGET = 'target'
N_CATS = 50  # столько самых частых значений получают собственный код


def category_codes(train, col):
    """Значение -> 'cat_<ранг по частоте>' для топ-N_CATS, иначе 'cat_50+'; пропуск -> 'cat_NAN'.

    Ранжирование повторяет код семинара один в один (включая место NaN в рейтинге
    и порядок при равных частотах) — на этих кодах обучена модель.
    """
    counts = train.groupby(col, dropna=False)[[TARGET]].count() \
        .sort_values(TARGET, ascending=False).reset_index()
    codes = {}
    for rank, value in enumerate(counts[col]):
        if pd.isna(value):
            continue
        codes[str(value)] = f'cat_{rank}' if rank < N_CATS else f'cat_{N_CATS}+'
    codes[NAN_KEY] = 'cat_NAN'
    return codes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', default=str(ROOT / 'data' / 'train.csv'))
    parser.add_argument('--out', default=str(ROOT / 'fraud_detector' / 'artifacts' / 'preprocessing.json'))
    args = parser.parse_args()

    train = pd.read_csv(args.train)
    print('train:', train.shape)

    cat_maps = {}
    for col in CATEGORICAL_COLS:
        cat_maps[col] = category_codes(train, col)
        train[col + '_cat'] = train[col].map(lambda v, m=cat_maps[col]: m[encode_key(v)])

    train = add_time_features(train)
    train = add_distance_feature(train)

    mean_enc = {}
    for col in [c + '_cat' for c in CATEGORICAL_COLS] + TIME_COLS:
        means = train.groupby(col)[TARGET].mean()
        mean_enc[col] = {str(k): float(v) for k, v in means.items()}

    impute_means = {col: float(np.nanmean(train[col])) for col in CONTINUOUS_COLS}

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        'cat_maps': cat_maps,
        'mean_enc': mean_enc,
        'impute_means': impute_means,
    }, ensure_ascii=False, indent=1), encoding='utf-8')
    print('saved:', out, f'({out.stat().st_size / 1024:.0f} KB)')


if __name__ == '__main__':
    main()
