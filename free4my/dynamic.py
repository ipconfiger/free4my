# -*- coding: utf-8 -*-
__author__ = 'Alexander Li'

import time
import uuid
try:
    from zlib import compress, decompress
except:
    compress=lambda s:s
    decompress=lambda s:s
from cPickle import dumps, loads
from logging import log, WARNING, ERROR,INFO
from utils import tou, tob
import session
import datetime
from free4my import current_session
myid = lambda :str(uuid.uuid4())

class Oper(object):
    def __init__(self, value):
        self.value = value

class Lt(Oper):
    atom = "<"

class Lte(Oper):
    atom = "<="

class Gt(Oper):
    atom = ">"

class Gte(Oper):
    atom = ">="

class NotEq(Oper):
    atom = "<>"

def _get_cmp_operator(value):
    classes = [Lt, Lte, Gt, Gte, NotEq]
    for cls in classes:
        if isinstance(value, cls):
            return cls.atom, value.value
    return "=", value

def _local_type_to_db_column(column):
    exchange = {
        int:"INT",
        unicode:"VARCHAR",
        str:"VARCHAR",
        float:"DOUBLE(20,6)",
        datetime.datetime:"DATETIME"
    }
    tb_column = exchange[column.allowed_type]
    if column.max_length:
        tb_column += "(%s)" % column.max_length
    tb_column += " NOT NULL"
    return tb_column

def _tb_exists(conn, table_name):
    try:
        conn.query("SELECT `id` FROM `%s` LIMIT 1" % table_name)
        log(ERROR,"table %s exists"%table_name)
        return True
    except:
        log(ERROR,"table %s not exists"%table_name)
        return False

class Column(object):
    def __init__(self, allowed_type, max_length=0, map2class=None, db_index=False):
        self.allowed_type = allowed_type
        self.max_length = max_length
        self.map_class = map2class
        self.db_index = db_index

class FkColumn(object):
    def __init__(self, related_type):
        self.related_type = related_type

class Empty:
    pass

class ModelBase(type):
    def __new__(cls, name, bases, attrs):
        super_new = super(ModelBase, cls).__new__
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return super_new(cls, name, bases, attrs)
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})
        columns = {}
        ExIndex = {}
        Funcs = {}
        ext_meta = None
        def func_type():
            pass
        for attr_name, attr in attrs.iteritems():
            if isinstance(attr, Column) or isinstance(attr, FkColumn):
                if isinstance(attr, Column):
                    columns[attr_name] = attr
                    if attr.db_index:
                        ExIndex[attr_name] = Index(attr_name)
                else:
                    columns[attr_name] = Column(unicode, max_length=36, map2class=attr.related_type)
                    ExIndex[attr_name] = Index(attr_name)
                    getter = lambda obj:obj[attr_name]
                    setter = lambda obj, value:obj.update({attr_name:value})
                    setattr(new_class, "get_" + attr_name, getter)
                    setattr(new_class, "set_" + attr_name, setter)
            else:
                if attr_name == "Meta":
                    ext_meta = attr
                if isinstance(attr, type(func_type)):
                    Funcs[attr_name] = attr
        if 'id' not in columns:
            columns['id'] = Column(unicode, max_length=36)
        if not hasattr(ext_meta, "table_name"):
            table_name = name
        else:
            table_name = ext_meta.table_name
        meta_attrs = dir(ext_meta)
        indexes = {}
        for attr_name in meta_attrs:
            meta_attr = getattr(ext_meta, attr_name)
            if isinstance(meta_attr, Index):
                indexes[attr_name] = meta_attr
        indexes.update(ExIndex)
        manager = Manager(ext_meta.table_name, new_class, name, columns, **indexes)
        setattr(new_class, "_columns", columns)
        setattr(new_class, "_table_name", table_name)
        setattr(new_class, "objects", manager)
        setattr(new_class, "_edited_columns", [])
        for name, func in Funcs.iteritems():
            setattr(new_class, name, func)
        return new_class


class DynamicBase(dict):
    __metaclass__ = ModelBase
    def __init__(self, **kwargs):
        if "id" in kwargs:
            self.update(id=kwargs['id'])
        if "_updated" in kwargs:
            self.update({"_updated":kwargs['_updated']})
        if "_auto_id" in kwargs:
            self.update({"_auto_id":kwargs['_auto_id']})
        for key in self._columns:
            if key in kwargs:
                if self._columns[key].map_class:
                    if isinstance(kwargs[key], self._columns[key].map_class):
                        self.update({key:kwargs[key].id})
                    else:
                        self.update({key:kwargs[key]})
                else:
                    self.update({key:kwargs[key]})
            else:
                if self._columns[key].allowed_type == datetime.datetime:
                    self.update({key:datetime.datetime.now()})
                else:
                    self.update({key:self._columns[key].allowed_type()})

    def __getattr__(self, attr_name):
        return self[attr_name]

    def __setattr__(self, attr_name, value):
        if attr_name == "_updated" or attr_name=="_auto_id":
            return self.update({attr_name:value})
        if attr_name in self._columns:
            column = self._columns[attr_name]
            if not self._columns[attr_name].map_class:
                if isinstance(value, column.allowed_type):
                    if column.max_length:
                        if column.max_length < len(value):
                            raise AttributeError, "%s length longer than %s,length=%s" % (attr_name, column.max_length, len(value))
                    self._edited_columns.append(attr_name)
                    return self.update({attr_name:value})
            else:
                if isinstance(value, column.allowed_type):
                    self._edited_columns.append(attr_name)
                    return self.update({attr_name:value})
                if isinstance(value, column.map_class):
                    self._edited_columns.append(attr_name)
                    return self.update({attr_name:value["id"]})
            raise AttributeError, "invalid type at attribute:%s %s allowed but %s got" % (
                attr_name,
                str(column),
                str(type(value))
               )
        raise AttributeError, "no attribute %s" % attr_name

    def __getitem__(self, attr_name):
        if attr_name == u"_updated" or attr_name=="_auto_id":
            return dict.__getitem__(self, attr_name)
        if attr_name[:-3] in self._columns and attr_name[-3:] == '_id':
            if self._columns[attr_name[:-3]].map_class:
                return dict.__getitem__(self, attr_name[:-3])

        if attr_name in self._columns:
            if not attr_name in self:
                if self._columns[attr_name] == datetime.datetime:
                    self.update({attr_name:datetime.datetime.now()})
                else:
                    self.update({attr_name:self._columns[attr_name].allowed_type()})

            if self._columns[attr_name].map_class:
                return self._columns[attr_name].map_class.objects.get(dict.__getitem__(self, attr_name))
            else:
                return dict.__getitem__(self, attr_name)

        raise AttributeError, "no attribute %s" % attr_name

    def save(self):
        return self.objects.update(self)

    def delete(self):
        return self.objects.delete(self)

    def is_authenticated(self):
        return True


class Manager(object):
    def __init__(self, table_name, obj_type, class_name, columns, **kwargs):
        self.session=current_session()
        self.table_name = table_name
        self.columns = columns
        self.index_list = kwargs
        self.data_type = obj_type
        self.obj_acc = ObjectAcc(self.session, self.table_name)
        self.idx_accs = {}
        for idx in self.index_list:
            idxobj = self.index_list[idx]
            setattr(self, idx, idxobj)
            idxobj.config(self.get, self.session, class_name, self.columns)
            self.idx_accs[idx] = idxobj

    def create(self, **kwargs):
        data = self.data_type(**kwargs)
        auto_id=0
        if 'id' in kwargs:
            id,auto_id = self.obj_acc.set_object(id=kwargs['id'], object=data)
        else:
            id,auto_id = self.obj_acc.set_object(object=data)
        data.update({"id":id,"_auto_id":auto_id})
        for index_name, index_acc in self.idx_accs.iteritems():
            index_acc.push_index(data)
        return data

    def get(self, id,use_cache=True):
        data = self.obj_acc.get_object(id=id,use_cache=use_cache)
        if data:
            return self.data_type(**data)

    def all(self):
        sql="SELECT `id` FROM %s"%self.table_name
        try:
            session=self.session()
            results = session.connection.query(sql)
            for idx_item in results:
                yield self.obj_acc.get_object(id=idx_item.id)
        except Exception, e:
            log(ERROR, "\nsql:%s" % sql)
            raise

    def update(self, data):
        self.obj_acc.update_object(object=data)
        for index, idx_acc in self.idx_accs.iteritems():
            for attr_name in data._edited_columns:
                if idx_acc.check_update(attr_name):
                    idx_acc.update_index(data)
                    break
        del data._edited_columns[:]

    def delete(self, data):
        self.obj_acc.delete_object(object=data)
        for index in self.idx_accs:
            self.idx_accs[index].delete_index(data["id"])

    def truncate(self):
        sql = "truncate table `%s`" % self.table_name
        self.conn.execute(sql, [])
        for index, idx_acc in self.idx_accs.iteritems():
            idx_acc.truncate()

    def create_table(self):
        conn=self.session().connection
        if not _tb_exists(conn, self.table_name):
            sql = """CREATE TABLE `%(table_name)s` (
`auto_id` int(11) NOT NULL AUTO_INCREMENT,
`id` VARCHAR(36) NOT NULL,
`object` varbinary(20000) NOT NULL,
`updated` DATETIME NOT NULL,
PRIMARY KEY (`auto_id`),
UNIQUE KEY (`id`),
INDEX (`updated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """ % dict(table_name=self.table_name)
            conn.execute(sql)
            for index, idx_acc in self.idx_accs.iteritems():
                idx_acc.create_table()



    def sync_table(self):
        session=self.session()
        if not _tb_exists(session.connection, self.table_name):
            self.create_table()
            return
        else:
            missing_idx = []
            for idx_name, idx_obj in self.index_list.iteritems():
                if not _tb_exists(session.connection, idx_obj.table_name):
                    missing_idx.append(idx_obj)
                    idx_obj.create_table()
            #flush_idx
            data = session.connection.query("SELECT `id` FROM `%s` ORDER BY `auto_id` ASC" % self.table_name)
            total=len(data)
            idx=1
            error_arr=[]
            if missing_idx:
                for item in data:
                    data_item = self.get(item.id,use_cache=False)
                    print "%s/%s complete %s errors"%(idx,total,len(error_arr))
                    for idx_obj in missing_idx:
                        if not idx_obj.push_index(data_item):
                            error_arr.append(item.id)
                    idx+=1
                if error_arr:
                    print "error feeds:\n"
                    for i in error_arr:
                        print i
            session.connection.execute("COMMIT;")

class Index(object):
    def __init__(self, *columns, **kwargs):
        self.columns = list(columns)
        self.table_name = kwargs['table_name'] if "table_name" in kwargs else ""
        self.params = []
        if "orders" in kwargs:
            self.orders = kwargs['orders']
        else:
            self.orders = []
        self.store_sql = ""
        self.unique = kwargs['unique'] if "unique" in kwargs else False
        self.limit = None

    def config(self, getone, session, class_name, columns):
        self.get_one = getone
        self.session=session
        self.column_objects = columns
        if not self.table_name:
            self.table_name = "%s_idx_%s" % (class_name.lower(), "_".join(self.columns))

    def check_update(self, col_name):
        if col_name in self.columns:
            return True
        return False

    def push_index(self, obj):
        session=self.session()
        entity_id = obj.id
        sql = "INSERT INTO `" + self.table_name + "` ( `entity_id`,"
        vsql = ""
        values = [entity_id]
        for field in self.columns:
            if field in obj:
                sql += "`" + field + "`,"
                vsql += "%s,"
                if self.column_objects[field].map_class:
                    try:
                        values.append(obj[field].id)
                    except Exception,e:
                        return False
                else:
                    values.append(obj[field])

        sql = sql[:-1]
        vsql += "%s"
        sql += ") VALUES (" + vsql + ")"
        session.connection.execute(sql, *values)
        return True

    def update_index(self, obj):
        session=self.session()
        sql = "UPDATE `" + self.table_name + "` SET "
        values = []
        for field in self.columns:
            if field in obj:
                sql += "`" + field + "`=%s,"
                if self.column_objects[field].map_class:
                    values.append(obj[field].id)
                else:
                    values.append(obj[field])
        sql = sql[:-1]
        sql += " WHERE `entity_id`=%s"
        values.append(obj["id"])
        session.connection.execute(sql, *values)

    def delete_index(self, id):
        session=self.session()
        sql = "DELETE FROM `" + self.table_name + "` WHERE `entity_id`=%s"
        session.connection.execute(sql, id)

    def truncate(self):
        session=self.session()
        sql = "truncate table `%s`" % self.table_name
        session.connection.execute(sql)

    def create_table(self):
        """
        `email` varchar(50) NOT NULL,
        """
        session=self.session()
        fields_list = []
        for col_name, col_obj in self.column_objects.iteritems():

            if col_name in self.columns:
                fields_list.append("`%s` %s," % (col_name, _local_type_to_db_column(col_obj)))
        field_str = "\n".join(fields_list)
        index_type = "UNIQUE KEY" if self.unique else "INDEX"
        index_fileds = ",".join("`" + f + "`" for f in self.columns)

        sql = """CREATE TABLE `%(table_name)s` (
`id` int(10) unsigned NOT NULL AUTO_INCREMENT,
`entity_id` VARCHAR(36) NOT NULL,
%(fields)s
PRIMARY KEY (`id`),
UNIQUE KEY (`entity_id`),
%(idx_type)s `idx_%(idx_name)s` (%(idx_fields)s) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """ % dict(table_name=self.table_name, fields=field_str, idx_type=index_type, idx_name=self.table_name, idx_fields=index_fileds)

        session.connection.execute(sql)

    def raw_query(self, sql, *argv):
        session=self.session()
        raw_sql = sql.replace('[object]', '`%s`' % self.table_name)
        results = session.connection.query(raw_sql, *argv)
        return [self.get_one(item.entity_id) for item in results]

    def _prepare_sql(self, **kwargs):
        del self.params[:]
        sql = "SELECT `entity_id` FROM `" + self.table_name + "` WHERE "
        for k, v in kwargs.iteritems():
            oper, value = _get_cmp_operator(v)
            sql += "`" + k + "`" + oper + "%s AND "
            self.params.append(value)
        sql = sql[:-4]
        return sql

    def get(self, **kwargs):
        session=self.session()
        sql = self._prepare_sql(**kwargs)
        sql += " LIMIT 1"
        results = session.connection.query(sql, *self.params)
        if results:
            return self.get_one(results[0].entity_id)

    def group_count(self,key):
        session=self.session()
        sql="SELECT `%(key)s`, count(1) as `ct` from `%(table)s` GROUP BY `%(key)s` ORDER BY `ct` DESC"%dict(key=key,table=self.table_name)
        results=session.connection.query(sql,[])
        if results:
            return results
        return []

    def exists(self,**kwargs):
        session=self.session()
        sql = self._prepare_sql(**kwargs)
        sql = sql.replace("`entity_id`","count(1) as `ct`")
        results = session.connection.query(sql, *self.params)
        if results:
            if results[0].ct:
                return True
        return False


    def count(self, **kwargs):
        session=self.session()
        sql = self.store_sql
        sql = sql.replace('`entity_id`', 'count(*) as c')
        results = session.connection.query(sql, *self.params)
        if results:
            return results[0].c

    def __getslice__(self, start, length):
        self.limit = " LIMIT %s,%s" % (start, length)
        return self

    def query(self, **kwargs):
        self.store_sql = self._prepare_sql(**kwargs)
        return self

    def all(self):
        self.store_sql = "SELECT `entity_id` FROM %s" % self.table_name
        return self

    def order(self, *fields):
        self.orders = fields
        return self

    def __iter__(self):
        orders = []
        for item in self.orders:
            if item.startswith('-'):
                orders.append("`" + item[1:] + "`" + " DESC")
            else:
                orders.append("`" + item + "`" + " ASC")
        if orders:
            order_sql = " ORDER BY " + ",".join(orders)
        else:
            order_sql = ""
        sql = self.store_sql
        sql += order_sql
        if self.limit:
            sql += " " + self.limit
        try:
            session=self.session()
            log(ERROR,sql)
            results = session.connection.query(sql, *self.params)
            for idx_item in results:
                yield self.get_one(idx_item.entity_id)
        except Exception, e:
            log(ERROR, "\nsql:%s\nparam:%s" % (sql, self.params))
            raise

class ObjectAcc(object):
    def __init__(self, session, table):
        self.table = table
        self.session=session

    def get_object(self, id=None,use_cache=True):
        session=self.session()
        raw_id = "o-%s" % tob(id)
        if use_cache:
            try:
                if session.check_obj(raw_id):
                    data = session.get_obj(raw_id)
                    if "_auto_id" in data:
                        return loads(data)
            except:
                log(ERROR,"CACHE read error")
        sql = "SELECT `auto_id`,`id`,`object`,`updated` FROM `"+self.table+"` WHERE `id`=%s"
        rows = session.connection.query(sql, tou(id))
        if rows:
            data = rows[0].object
            objstr = decompress(data)
            obj = loads(objstr)
            obj.update(dict(id=id))
            obj["_updated"] = rows[0].updated
            obj["_auto_id"] =rows[0].auto_id
            if use_cache:
                try:
                    session.set_obj(raw_id,dumps(obj))
                except:
                    log(ERROR,"CACHE write error")
            return obj

    def set_object(self, id=None, object=None):
        session=self.session()
        r_data=dumps(object)
        datas = compress(r_data)
        if not id:
            entity_id = myid()
        else:
            entity_id = id
        timestamp=datetime.datetime.now()
        sql = "INSERT INTO `" + self.table + "` (`id`,`object`,`updated`) VALUES (%s,%s,%s)"
        ret=session.connection.execute(sql, entity_id, datas,timestamp)
        raw_id = "o-%s" % tob(entity_id)
        return entity_id,ret

    def update_object(self,object=None):
        session=self.session()
        raw_id = "o-%s" %object['id']
        datas = compress(dumps(object))
        key_id=object["_auto_id"]
        timestamp=datetime.datetime.now()
        sql = "UPDATE `" + self.table + "` SET `object`=%s,`updated`=%s WHERE `auto_id`=%s"
        session.connection.execute(sql, datas,timestamp,key_id)
        object['_updated'] = timestamp
        try:
            session.set_obj(raw_id,dumps(object))
        except:
            log(ERROR,"CACHE update error")

    def delete_object(self,object=None):
        session=self.session()
        raw_id = "o-%s" % object['id']
        key_id=object["_auto_id"]
        sql = "DELETE FROM `" + self.table + "` WHERE `auto_id`=%s"
        session.connection.execute(sql, key_id)
        try:
            session.del_obj(raw_id)
        except:
            log(ERROR,"CACHE delete error")
