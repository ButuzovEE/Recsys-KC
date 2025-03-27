from database import Base, SessionLocal, engine
from sqlalchemy import Integer, String, Column, desc,func

class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True, nullable=False)
    age = Column(Integer)
    city = Column(String)
    country = Column(String, nullable=False)
    exp_group = Column(Integer)
    gender = Column(Integer)
    os = Column(String, nullable=False)
    source = Column(String)

if __name__ == "__main__":
    session = SessionLocal()
    results = (
          session.query(
                
                User.country,
                User.os, 
                func.count("*").label('count')
                        )
        .filter(User.exp_group == 3)
        .group_by(User.country, User.os)
        .having(func.count("*") > 100)
        .order_by(desc(func.count("*")))
        .all()
    )
    print(results)
    