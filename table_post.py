from database import Base, SessionLocal, engine
from sqlalchemy import Integer, String, Column, desc

class Post(Base):
    __tablename__ = "post"
    __table_args__ = {"schema": "public"}
    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)
if __name__ == "__main__":
    session = SessionLocal()
    results = (
        session.query(Post)
        .filter(Post.topic == "business")
        .order_by(desc(Post.id))
        .limit(10)
        .all()
    )
    res = []
    for i in results:
        res.append(i.id)
    print(res)

