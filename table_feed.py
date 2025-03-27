from database import Base, SessionLocal, engine
from table_user import User
from table_post import Post
from sqlalchemy import Integer, String, Column, desc, TIMESTAMP, ForeignKey
from sqlalchemy.orm import relationship

class Feed(Base):
    __tablename__ = "feed_action"
    action = Column(String)
    post_id = Column(Integer,  ForeignKey(Post.id), primary_key=True,  name = "post_id")
    time = Column(TIMESTAMP)
    user_id = Column(Integer, ForeignKey(User.id), name = "user_id")
    user = relationship(User)
    post = relationship(Post)
    
