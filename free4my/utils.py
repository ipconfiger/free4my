#coding=utf8

class Row(dict):
    """A dict that allows for object-like property access syntax"""
    def __getattr__(self, item):
        try:
            return self[item]
        except  KeyError:
            raise AttributeError(item)
    def __setattr__(self, name, value):
        try:
            self[name]=value
        except KeyError:
            raise AttributeError(name)

def tou(data, enc='utf8'):
    if isinstance(data, unicode):return data
    return data.decode(enc)

def tob(data, enc='utf8'):
    if isinstance(data, unicode):return data.encode(enc)
    return data