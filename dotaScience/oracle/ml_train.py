import os
import pandas as pd
from sklearn import tree
from sklearn import model_selection
from sklearn import metrics

ORACLE_DIR = os.path.dirname(os.path.abspath(__file__))
ORACLE_DIR = os.path.join( os.path.abspath("."), "dotaScience", "oracle")
SRC_DIR = os.path.dirname(ORACLE_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, "data")

DATA_TRAIN_DIR = os.path.join(DATA_DIR, "train")
TABLE_PATH = os.path.join(DATA_TRAIN_DIR, "tb_abt_oracle")

file_path = [i for i in os.listdir(TABLE_PATH) if i.endswith(".csv")][-1]
file_path = os.path.join(TABLE_PATH, file_path)

df_full = pd.read_csv(file_path)

columns = df_full.columns.tolist()

features = set(columns) - set(['match_id', 'dt_start_time', 'radiant_win'])
target = 'radiant_win'

X_train, X_test, y_train, y_test = model_selection.train_test_split( df_full[features],
                                                                     df_full[target],
                                                                     random_state=42,
                                                                     test_size=0.1 )

X_train, X_test, y_train, y_test = X_train.reset_index(drop=True), X_test.reset_index(drop=True), y_train.reset_index(drop=True), y_test.reset_index(drop=True)

X_train = X_train.fillna(-100)

clf = tree.DecisionTreeClassifier(min_samples_leaf=100)

clf.fit( X_train, y_train )

y_train_score, y_train_lbl = clf.predict_proba(X_train), clf.predict(X_train)

print("Acurácia Treino:", metrics.accuracy_score(y_train, y_train_lbl) )
print("AUC Treino:",metrics.roc_auc_score(y_train, y_train_score[:,1]) )

X_test = X_test.fillna(-100)
y_test_score, y_test_lbl = clf.predict_proba(X_test), clf.predict(X_test)
print("Acurácia Teste:", metrics.accuracy_score(y_test, y_test_lbl) )
print("AUC Teste:",metrics.roc_auc_score(y_test, y_test_score[:,1]) )

importante = pd.Series(clf.feature_importances_, index = features)

print(importante.sort_values(ascending=False).head(20))
