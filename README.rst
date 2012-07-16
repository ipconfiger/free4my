Schema-free For MySQL
=====================

This is a DAL tool for `MySQL database`_ users to create applications using schema-free data structure.

.. _MySQL database: http://www.mysql.com/


Installing
----------

#. Install from the pypi index. ``$ sudo pip install free4my``

#.  Clone source code from github with the following command:

        $ git clone https://github.com/ipconfiger/free4my.git
        $ sudo python setup.py install


Usage
-----

#. Import necessary functions and classes::

    from free4my import DbContext, session_maker
    from free4my.Dynamic import DynamicBase, Column, Index, Lt, Gt, Lte, Gte, NotEq

#. Create a db context instance binding to a database connection::

    context = DbContext(
            host='127.0.0.1',
            user='db_user',
            password='db_password',
            database='db_name',
            )

#. Create a session factory instance binding to a db context::

    Session = session_maker(context)

#. Define the model class::

    class Author(DynamicBase):
        name = Column(unicode,max_length=20)
        password = Column(unicode,max_length=46)
        age = Column(int)
        regist_date = Column(datetime.datetime)

        class Meta:
            table_name = "author"
            by_regist = Index('regist_date', table_name="reg_time_idx_author")
            by_age = Index('age', table_name="age_idx_author")

#. Initialize database. This code will create tables nedded for structure ``Author``::

    Author.objects.sync_table()

#. Open a session and play with it::

    db = Session()
    user = Author.objects.create(
            name=u'Alexander',
            password=u'pass',
            age=31,
            regist_date=datetime.datetime.now(),
            )

    user_list = Author.objects.by_regist.query(
            regist_date=Lte(datetime.datetime.now())
            ).order("-regist_date")

    for user in user_list:
        print user.name

    db.commit()


API
---

RTFC


.. vim:ai:et:ts=4:sw=4:sts=4:
