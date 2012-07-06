import sys
sys.path.append("..")
import datetime
import free4my
from free4my import DbContext,session_maker
from free4my.dynamic import DynamicBase,Column,FkColumn,Lt,Lte,Gt,Gte,NotEq,Index

ctx=DbContext(host="221.237.177.83",user="alex",password="1qasw2",database="test_db")

class Auther(DynamicBase):
    name=Column(unicode,max_length=20)
    password=Column(unicode,max_length=46)
    age=Column(int)
    regist_date=Column(datetime.datetime)
    class Meta:
        table_name = "auther"
        by_regist = Index('regist_date',table_name="reg_time_idx_auther")
        by_age = Index('age',table_name="age_idx_auther")

Session=session_maker(ctx)

session=Session()

Auther.objects.sync_table()


#au1=Auther.objects.create(name=u"alexabder",password=u"123456",age=14,regist_date=datetime.datetime.now())

#au2=Auther.objects.create(name=u"susan",password=u"abc",age=20,regist_date=datetime.datetime.now())

#session.commit()

for user in Auther.objects.by_regist.all().order('-regist_date'):
    print user.name
for user in Auther.objects.by_regist.all().order('-regist_date'):
    print user.name

session.commit()

