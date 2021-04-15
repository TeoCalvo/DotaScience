# %%
import os
import pandas as pd
from sklearn import tree
from sklearn import model_selection
from sklearn import metrics
from sklearn import preprocessing
from sklearn import ensemble
from sklearn import pipeline
from sklearn import decomposition
from sklearn.model_selection import RandomizedSearchCV

import random

import xgboost as xgb

from feature_engine.imputation import ArbitraryNumberImputer
from feature_engine.discretisation import DecisionTreeDiscretiser


# %%
#ORACLE_DIR = os.path.dirname(os.path.abspath(__file__))
ORACLE_DIR = os.path.abspath(".")
SRC_DIR = os.path.dirname(ORACLE_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, "data")

DATA_TRAIN_DIR = os.path.join(DATA_DIR, "train")
TABLE_PATH = os.path.join(DATA_TRAIN_DIR, "tb_abt_oracle")

file_path = [i for i in os.listdir(TABLE_PATH) if i.endswith(".csv")][-1]
file_path = os.path.join(TABLE_PATH, file_path)

# %%

# Importando o dataset
df_full = pd.read_csv(file_path)

columns = df_full.columns.tolist()

features =list(set(columns) - set(['match_id', 'dt_start_time', 'radiant_win']))
target = 'radiant_win'

df_full[features] = df_full[features].dropna(how="all")


# %%
# Separa entre treinamento e teste
X_train, X_test, y_train, y_test = model_selection.train_test_split( df_full[features],
                                                                     df_full[target],
                                                                     random_state=42,
                                                                     test_size=0.1 )


# %%

arbitrary_imputer = ArbitraryNumberImputer(arbitrary_number=-999,
                                           variables=features)

disc = DecisionTreeDiscretiser(cv=3,
                               scoring='roc_auc',
                               variables=features,
                               regression=False,
                               random_state=42)

pca = decomposition.PCA(n_components=120,
                        random_state=42)

params = {
    "n_estimators":[100,500,600,700,800],
    "max_depth":[4,5,6,7,10],
    "subsample":[0.1,0.2,0,3, 0.7, 0.75,0.8,0.9, 0.99],
    "learning_rate":[0.1, 0.2, 0.5, 0.7, 0.8, 0.9, 0.99],
}

clf_xgb = xgb.XGBClassifier(nthread=8,
                            eval_metric='auc',
                            random_state=42)

random_search = RandomizedSearchCV(clf_xgb,
                                   params,
                                   scoring="roc_auc",
                                   n_jobs=1,
                                   random_state=42,
                                   n_iter=20,
                                   cv=4)

my_pipe = pipeline.Pipeline( [("imputer", arbitrary_imputer), 
                              ("discretizator", disc),
                              ("pca", pca),
                              ("model", random_search)])

my_pipe.fit(X_train, y_train)

# %%
 
y_test_pred = my_pipe.predict(X_test)
y_test_prob = my_pipe.predict_proba(X_test)

# %%
metrics.roc_auc_score( y_test, y_test_prob[:,1] )
# %%
pd.DataFrame(random_search.cv_results_)
# %%
random_search.best_params_
# %%
bets = {'subsample': 0.7, 'n_estimators': 100, 'max_depth': 5, 'learning_rate': 0.2}
