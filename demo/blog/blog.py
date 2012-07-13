#coding=utf8
import sys
sys.path.append("../..")

import time
import datetime
import hmac
import random
import uuid
import urllib
import json
import free4my
from functools import wraps
from free4my.dynamic import DynamicBase,Column,FkColumn,Lt,Lte,Gt,Gte,NotEq,Index
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash, get_flashed_messages
from logging import log,INFO,ERROR,DEBUG,WARNING


DATABASE = 'blog'
DEBUG = True
HOST = '127.0.0.1'
SECRET_KEY = '1r4$df(8'
USERNAME = 'root'
PASSWORD = '123456'
PAGE_SIZE=20

app = Flask(__name__)
app.config.from_object(__name__)

db_ctx=free4my.DbContext(host=HOST,user=USERNAME,password=PASSWORD,database=DATABASE)
Session=free4my.session_maker(db_ctx)

def touni(data, enc='utf8'):
    if isinstance(data, unicode):return data
    return data.decode(enc)

def tob(data, enc='utf8'):
    if isinstance(data, unicode):return data.encode(enc)
    return data

def _to_date_str(key):
    time_s=str(key)
    time_tuple=(time_s[:4],time_s[4:6],time_s[6:9])
    return "-".join(time_tuple)

def hash_passwd(raw_password):
    salt="".join(random.sample('abcdefghijklmnopqrstuvwxyz1234567890ABSDXFJOHDXFH',4))
    return "$".join([salt,hmac.new(salt,tob(raw_password)).hexdigest()])

def check_passwd(raw_password,enc_password):
    salt,hmac_password=tob(enc_password).split('$')
    if hmac.new(salt,tob(raw_password)).hexdigest()==hmac_password:
        return True

def one(item):
    if isinstance(item,list):
        if item:
            return item[0]
    return item

@app.template_filter('summary')
def reverse_filter(s):
    return s[:300]

class SiteConfig(DynamicBase):
    user_name=Column(unicode,max_length=20)
    password=Column(unicode,max_length=46)
    site_name=Column(unicode,max_length=50)
    fav_icon=Column(unicode,max_length=50)
    css=Column(unicode,max_length=2000)
    js=Column(unicode,max_length=2000)
    copyright=Column(unicode,max_length=100)
    files=Column(dict)
    class Meta:
        table_name = "site_config"

    def set_password(self,new_password):
        self.password=touni(hash_passwd(new_password))

    def check_password(self,input_password):
        return check_passwd(input_password,self.password)

class Blog(DynamicBase):
    title=Column(unicode,max_length=50)
    short_key=Column(unicode,max_length=36)
    content=Column(unicode,max_length=10000)
    create_date=Column(int,db_index=True)
    create_time=Column(datetime.datetime)
    view_count=Column(int)
    class Meta:
        table_name = "blog"
        by_short = Index('short_key',table_name="sort_key_idx_blog",unique=True)
        by_date = Index('create_time',table_name="create_time_idx_blog")

def init_db():
    session=Session()
    Blog.objects.sync_table()
    SiteConfig.objects.sync_table()
    if not list(SiteConfig.objects.all()):
        SiteConfig.objects.create(user_name=u'admin',password=touni(hash_passwd('123456')),site_name=u'DEMO Blog')
    session.commit()

if DEBUG:
    init_db()

def view(loc):
    data=dict(**loc)
    data.update(dict(config=g.config),is_login=check_authenticate())
    date_files=[]
    for k,v in g.config.files.iteritems():
        date_files.append(dict(sort=k,date=_to_date_str(k),count=v))
    date_files.sort(lambda a,b:cmp(b['sort'],a['sort']))
    data.update(dict(date_files=date_files))
    return data


def pages(item_count,page_id,page_size,base_url):
    def make_url(base,pid):
        base=tob(base)
        if not pid:
            return ""
        url_slice=base.split('?')
        if len(url_slice)<2:
            return base+"?p=%s"%pid
        else:
            params=dict([(lambda i:tuple(i) if len(i)<3 else (i[0],"=".join(i[1:])))(item.split("=")) for item in url_slice[1].split('&')])
            params["p"]=pid
            return "%s?%s"%(url_slice[0],urllib.urlencode(params))
    page_count=item_count/page_size+1 if item_count%page_size else item_count/page_size
    if page_count<10:
        return [(i+1,make_url(base_url,i+1)) for i in range(page_count)]
    else:
        if page_id<5:
            return [(p,make_url(base_url,p)) for p in [1,2,3,4,5,0,page_count]]
        if page_id>(page_count-4):
            return [(p,make_url(base_url,p)) for p in [1,0,page_count-4,page_count-3,page_count-2,page_count-1,page_count]]
        return [(p,make_url(base_url,p)) for p in [1,0,page_id-2,page_id-1,page_id,page_id+1,page_id+2,0,page_count]]


@app.before_request
def before_request():
    g.db=Session()
    if not hasattr(g,"config"):
        g.config=(lambda tp:tp[0] if tp else None)(list(SiteConfig.objects.all()))

@app.teardown_request
def teardown_request(exception):
    g.db.rollback()
    if exception:
        log(ERROR,exception.message)

@app.after_request
def after_request(response):
    try:
        g.db.commit()
    except Exception,e:
        g.db.rollback()
        log(ERROR,e.message)
    return response


def check_authenticate():
    if "login_flag" in session:
        if session["login_flag"]:
            return True

def authoriz(user_name,password):
    if g.config.user_name==user_name and g.config.check_password(password):
        session['login_flag']=True
        return True

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not check_authenticate():
            url="/"+"/".join(request.url.split("/")[3:])
            url=urllib.quote(url)
            return redirect(url_for('login',next=url))
        return f(*args, **kwargs)
    return decorated

@app.route("/login",methods=['GET','POST'])
def login():
    if request.method=="GET":
        error=one(get_flashed_messages())
        log(ERROR,error)
        return render_template('login.html', **view(locals()))
    login_id=request.form.get("login_id")
    password=request.form.get("password")
    next=request.args.get("next","/")
    if authoriz(login_id,password):
        return redirect(next)
    flash("invalid user or password")
    return redirect(url_for('login',next=next))


@app.route("/")
def index():
    p=int(request.args.get('p','1'))
    query=Blog.objects.by_date.all().order('-create_time')
    total=query.count()
    page_links=pages(total,p,PAGE_SIZE,"/")
    start_idx=(p-1)*PAGE_SIZE
    blogs=query[start_idx:PAGE_SIZE]
    dated={}
    for b in blogs:
        if b.create_date not in dated:
            dated[b.create_date]=[b,]
        else:
            dated[b.create_date].append(b)
    date_list=[]
    for k,v in dated.iteritems():
        date_list.append(dict(dt=k,blogs=v,date=_to_date_str(k)))
    date_list.sort(lambda a,b:cmp(b['dt'],a['dt']))
    return render_template("index.html",**view(locals()))

@app.route("/<date>")
def date_blog(date):
    p=int(request.args.get('p','1'))
    query_date=int(date.replace("-",""))
    query=Blog.objects.create_date.query(create_date=query_date).order('-id')
    total=query.count()
    page_links=pages(total,p,PAGE_SIZE,"/%s"%date)
    start_idx=(p-1)*PAGE_SIZE
    data=dict(blogs=query[start_idx:PAGE_SIZE])
    return render_template("date_blogs.html",**view(locals()))

@app.route("/logout")
def logout():
    session.pop("login_flag")
    return redirect(url_for('index'))


@app.route("/blog/<bid>",methods=["POST","GET","DELETE"])
def blog(bid):
    blog=Blog.objects.by_short.get(short_key=bid)
    if request.method=="GET":
        error=one(get_flashed_messages())
        if blog:
            blog.view_count+=1
            blog.save()
        return render_template("blog.html",**view(locals()))
    if request.method=="DELETE":
        dt=blog.create_date
        if dt in g.config.files:
            ct=g.config.files.get(dt)
            if ct:
                g.config.files.update({dt:ct-1})
            else:
                del g.config.files[dt]
        g.config.save()
        blog.delete()
        return json.dumps(dict(r=True))
    if request.method=="POST":
        while True:
            title=touni(request.form['title'])
            content=touni(request.form['content'])
            if not title:
                flash("must input the blog title")
                break
            if not content:
                flash("must input the blog content")
                break
            blog.title=title
            blog.content=content
            blog.save()
            flash("update successful")
            break
        return redirect(url_for('blog',bid=blog.short_key))
    return "woops"

@app.route("/edit/blog/<bid>")
def show_edit_post(bid):
    error=one(get_flashed_messages())
    blog=Blog.objects.by_short.get(short_key=bid)
    return render_template("blog_edit.html",**view(locals()))

@app.route("/admin/<action>",methods=['GET','POST'])
@requires_auth
def show_admin(action):
    if request.method=="GET":
        error=one(get_flashed_messages())
        if action=="post":
            return render_template("post.html",**view(locals()))
        if action=="conf":
            return render_template("conf.html",**view(locals()))
        return "unsupport choice"
    if action=="conf":
        while True:
            g.config.user_name=touni(request.form['login_id'])
            password=request.form['password']
            cpassword=request.form['cpassword']
            if password:
                if password!=cpassword:
                    flash("confirm input not match the new password")
                    break
                g.config.set_password(touni(password))
            g.config.site_name=touni(request.form['site_name'])
            g.config.fav_icon=touni(request.form['fav_icon'])
            g.config.css=touni(request.form['css'])
            g.config.js=touni(request.form['jscript'])
            g.config.copyright=touni(request.form['copyright'])
            g.config.save()
            flash("configure updated!")
            break
        return redirect(url_for('show_admin',action=action))
    if action=="post":
        while True:
            short_name=touni(request.form['short_name'].strip())
            title=touni(request.form['title'])
            content=touni(request.form['content'])
            if not title:
                flash("must input the blog title")
                break
            if not content:
                flash("must input the blog content")
                break
            if Blog.objects.by_short.get(short_key=short_name):
                flash("duplicate key for short name")
                break
            n=datetime.datetime.now()
            dt=int("%s%02d%02d"%(n.year,n.month,n.day))
            blog_params=dict(
                title=title,
                content=content,
                create_time=n,
                create_date=dt,
                view_count=0,
            )
            if not short_name:
                man_id=str(uuid.uuid4())
                blog_params.update(
                    dict(
                        id=man_id,
                        short_key=man_id,
                    )
                )
            else:
                blog_params.update(dict(short_key=short_name))
            blog=Blog.objects.create(**blog_params)
            posts=g.config.files.get(dt,0)
            g.config.files.update({dt:posts+1})
            g.config.save()
            flash("post successful")
            break
        return redirect(url_for('blog',bid=blog.short_key))
    return "woops"

@app.route("/admin/resetpass")
def reset_password():
    g.config.set_password("123456")
    g.config.save()
    return "ok"



if __name__ == "__main__":
    app.run()