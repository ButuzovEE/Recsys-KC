import os
import pickle
from catboost import CatBoostClassifier
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
import pandas as pd

def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

def load_models():
    model_path = get_model_path("model")
    model = CatBoostClassifier()
    model.load_model(model_path)
    return model


DATABASE_URL="postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"
FEATURES_DF_NAME="evgeneey89_features_lesson_22"
CHUNKSIZE="200000"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
def load_features() -> pd.DataFrame:
    engine = create_engine(DATABASE_URL)
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    try:
        print(("features - loading"))
        for chunk_dataframe in pd.read_sql(FEATURES_DF_NAME,
                                           conn, chunksize=int(CHUNKSIZE)):
            chunks.append(chunk_dataframe)

        print(("features -  successfully"))

    except Exception as e:

        raise RuntimeError(f"Loading error: {e}")

    finally:
        conn.close()

    return pd.concat(chunks, ignore_index=True)