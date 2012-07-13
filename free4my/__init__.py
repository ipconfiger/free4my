#coding=utf8
import threading
import db
from session import Session

__version__="0.10"

g=threading.local()

class DbContext(object):
    def __init__(self,**kwargs):
        self.params=dict(**kwargs)
        self.conn_pool=threading.local()
        self.conn_pool.current=db.Connection(**kwargs)

    def get_connection(self):
        try:
            conn=self.conn_pool.current
        except:
            self.conn_pool.current=db.Connection(**self.params)
        return self.conn_pool.current

def session_maker(context,**kwargs):
    def session_wrapper():
        g.current_session=Session(context,**kwargs)
        return g.current_session
    return session_wrapper


def current_session():
    def wrapper():
        return g.current_session
    return wrapper