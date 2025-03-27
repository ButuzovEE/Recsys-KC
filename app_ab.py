
import os
from loguru import logger

from catboost import CatBoostClassifier
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import os
from typing import List
from fastapi import FastAPI
from schema import PostGet
from schema import Response
from datetime import datetime
import hashlib

from pydantic import BaseModel
class PostGet(BaseModel):
    id: int
    text: str
    topic: str
    
    class Config:
        orm_mode = True

class Response(BaseModel):
    exp_group: str
    recommendations: List[PostGet] 


app = FastAPI()
# функция разделения моделей на контрольную и тестовую
##########
def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":
        if path == 'CONTROL_MODEL_PATH':
            path = '/workdir/user_input/model_control'
        elif path == 'TEST_MODEL_PATH':
            path = '/workdir/user_input/model_test'
    return path
SALT = "Butuzov_final"
def get_exp_group(id: int) -> str:
    value_str = str(id) + SALT
    value_num = int(hashlib.md5(value_str.encode()).hexdigest(), 16) % 2
    if value_num == 1:
        return "control"
    elif value_num == 0:
        return "test"
    return "unknown"
#########
# словарь для обработки текста
topic_dict = {
    'business': 0,
    'covid': 1,
    'entertainment': 2, 
    'movie': 3,
    'politics': 4,
    'sport': 5,
    'tech': 6, 
}
######
def load_models():
    model_control = CatBoostClassifier()
    model_test= CatBoostClassifier()
    model_control.load_model(get_model_path("CONTROL_MODEL_PATH"))
    model_test.load_model(get_model_path("TEST_MODEL_PATH"))
    return model_control, model_test

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
        for chunk_dataframe in pd.read_sql(FEATURES_DF_NAME,
                                           conn, chunksize=int(CHUNKSIZE)):
            chunks.append(chunk_dataframe)
    except Exception as e:
        raise RuntimeError(f"Loading error: {e}")
    finally:
        conn.close()
    return pd.concat(chunks, ignore_index=True)


feed_df = pd.read_sql("""SELECT distinct post_id, user_id, timestamp FROM public.feed_data
                     where 1=1 and action = 'like' limit 2000000;""", engine)
user_df = load_features()
user_df = user_df.drop('index', axis=1)
post_df = pd.read_sql("SELECT * FROM public.post_text_df;", engine)

# обработка датафрейма для ML-модели
post_ml = pd.read_sql("evgeneey89_PCA_features_lesson_22", engine)
post_ml = pd.merge(post_df, post_ml, on='post_id', how='left')
post_ml = post_ml.drop('text', axis=1)
post_ml = post_ml.drop('index', axis=1)
post_ml['topic'] = post_ml['topic'].map(topic_dict)


# обработка датафрейма для DL-модели
post_dl = pd.read_sql("evgeneey89_emb_rob_features_lesson_22", engine)
post_dl = pd.merge(post_df, post_dl, on='post_id', how='left')
post_dl = post_dl.drop('text', axis=1)
post_dl = post_dl.drop('index', axis=1)
post_dl['topic'] = post_dl['topic'].map(topic_dict)

# Загрузка двух моделей
model_control, model_test = load_models()
def add_todf(users, posts):
    for col in users:
        posts[col] = users[col].values.squeeze()
    return posts
# функции рукомендаций
def rec_ml(id:int, time:datetime = None, limit:int = 5)-> List[PostGet]:
    timestamp = pd.to_datetime(time)
    user_data = user_df[user_df.user_id == id] 
    user_data = add_todf(user_data, post_ml)
    liked_post = feed_df[(feed_df['timestamp'] > timestamp) & (feed_df['user_id'] == id)]['post_id']
    user_data = user_data[~user_data['post_id'].isin(liked_post)]
    user_data['probability'] = model_control.predict_proba(user_data)[:,1]
    rec_id_post = list(user_data.sort_values('probability', ascending=False).head(limit).reset_index().post_id)
    posts = [PostGet(**{
            'id': i,
            'text': post_df[post_df['post_id'] == i]['text'].values[0],
            'topic': post_df[post_df['post_id'] == i]['topic'].values[0]
        }) for i in rec_id_post]
    return posts
def rec_dl(id:int, time:datetime = None, limit:int = 5) -> List[PostGet]:
    timestamp = pd.to_datetime(time)
    user_data = user_df[user_df.user_id == id] 
    user_data = add_todf(user_data, post_dl)
    liked_post = feed_df[(feed_df['timestamp'] > timestamp) & (feed_df['user_id'] == id)]['post_id']
    user_data = user_data[~user_data['post_id'].isin(liked_post)]
    user_data['probability'] = model_test.predict_proba(user_data)[:,1]
    rec_id_post = list(user_data.sort_values('probability', ascending=False).head(limit).reset_index().post_id)
    posts = [PostGet(**{
            'id': i,
            'text': post_df[post_df['post_id'] == i]['text'].values[0],
            'topic': post_df[post_df['post_id'] == i]['topic'].values[0]
        }) for i in rec_id_post]
    return posts
@app.get("/post/recommendations/", response_model=Response)
def recommended_posts(  id: int , 
		                time: datetime , 
		                limit: int = 5) -> Response:
    exp_group = get_exp_group(id)
    if  exp_group== 'control':
        return Response(**{
            'exp_group': exp_group,
            'recommendations': rec_ml(id, time, limit)}   )
        
    elif exp_group == 'test':
        return Response(**{
            'exp_group': exp_group,
            'recommendations': rec_dl(id, time, limit)}   )
    else: 
        raise ValueError('unknown group')