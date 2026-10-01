"""Скоринг: признаки после препроцессинга -> вероятность фрода и флаг."""
import logging

from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)


class Scorer:
    def __init__(self, model_path, threshold):
        self.model = CatBoostClassifier()
        self.model.load_model(model_path)
        self.threshold = threshold
        self.features = self.model.feature_names_
        self.cat_features = [self.features[i] for i in self.model.get_cat_feature_indices()]
        logger.info('Model loaded from %s: %d features, threshold %.2f',
                    model_path, len(self.features), threshold)

    def predict(self, features_df):
        # Порядок колонок как при обучении, категориальные признаки — строками
        df = features_df[self.features].copy()
        for col in self.cat_features:
            df[col] = df[col].astype(str)

        scores = self.model.predict_proba(df)[:, 1]
        flags = (scores > self.threshold).astype(int)
        return scores, flags
