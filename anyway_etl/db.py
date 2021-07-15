import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .config import SQLALCHEMY_URL


engine = create_engine(SQLALCHEMY_URL)
Session = sessionmaker(bind=engine)


def session_decorator(func):

    def _func(*args, **kwargs):
        session = Session()
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()

    return _func
