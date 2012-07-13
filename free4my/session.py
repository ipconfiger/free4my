#coding:utf8

import db

class dict_store(object):
    def __init__(self):
        self.store={}

    def get(self,key):
        return self.store.get(key,None)

    def check(self,key):
        return True if key in self.store else False

    def set(self,key,value):
        return self.store.update({key:value})

    def delete(self,key):
        if self.check(key):
            del self.store[key]
        return

class Session(object):
    def __init__(self,context,global_cache=None):
        if global_cache:
            self.cache=global_cache
        else:
            self.cache=dict_store()
        self.conn=context.get_connection()
        self.conn.commit()
    
    @property
    def connection(self):
        return self.conn

    def get_obj(self,key):
        return self.cache.get(key)

    def check_obj(self,key):
        return self.cache.check(key)

    def set_obj(self,key,value):
        return self.cache.set(key,value)

    def del_obj(self,key):
        return self.cache.delete(key)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        pass