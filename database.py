from sqlalchemy import create_engine 
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy.orm import sessionmaker 

SQLALCHEMY_DATABASE_URL = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"
# данные для подключения

engine = create_engine(SQLALCHEMY_DATABASE_URL)
# инициализируем движок

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# подготавливаемся к ессиям

Base = declarative_base()
