import datetime
from typing import Optional
from pydantic import BaseModel, Field
from table_feed import User

class UserGet(BaseModel):
    age: int
    city: str = ""
    country: str = ""
    exp_group: int
    gender: int
    id: int
    os:str = ""
    source: str = ""
    class Config:
        orm_mode = True

class PostGet(BaseModel):
    id: int
    text: str
    topic: str
    
    class Config:
        orm_mode = True

class Response(BaseModel):
    exp_group: str
    recommendations: List[PostGet]

class FeedGet(BaseModel):
    action: str = ""
    post_id: int
    time: datetime.datetime
    user_id:int
    user: UserGet
    post: PostGet
    class Config:
        orm_mode = True
