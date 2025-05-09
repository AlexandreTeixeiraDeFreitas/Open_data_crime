# ia/train.py
import os
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import accuracy_score, classification_report
from xgboost import XGBClassifier
from utils import fetch_all, preprocess, save_pickle

MODEL_F = 'xgb_top5_model.pkl'
LE_F    = 'xgb_le_top5.pkl'
OE_F    = 'xgb_oe_top5.pkl'
COLS_F  = 'xgb_top5_cols.pkl'
PKLS    = [MODEL_F, LE_F, OE_F, COLS_F]

def train():
    if all(os.path.exists(f) for f in PKLS):
        print("Les .pkl existent déjà, rien à faire.")
        return

    df = fetch_all()
    X, y, le, oe = preprocess(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    base = XGBClassifier(
        objective='multi:softprob',
        num_class=len(le.classes_),
        eval_metric='mlogloss',
        tree_method='hist',
        random_state=42,
        n_jobs=-1
    )
    param_dist = {
        'n_estimators':[100,200,300,500],
        'max_depth':  [4,6,8,10],
        'learning_rate':[0.01,0.05,0.1],
        'subsample':  [0.6,0.8,1.0],
        'colsample_bytree':[0.6,0.8,1.0]
    }

    rs = RandomizedSearchCV(
        base, param_dist, n_iter=12, cv=3, verbose=2, n_jobs=-1, random_state=42
    )
    rs.fit(X_train, y_train)
    best = rs.best_estimator_

    y_pred = best.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {acc:.4f}")
    print(classification_report(y_test, y_pred, target_names=le.classes_, zero_division=0))

    save_pickle(best, MODEL_F)
    save_pickle(le,    LE_F)
    save_pickle(oe,    OE_F)
    save_pickle(X.columns.tolist(), COLS_F)

if __name__ == "__main__":
    train()
