#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Micheal Liao'
__learner__ = 'Monster Xiao'

'''
Database operation module.
'''

import time, uuid, functools, threading, logging

# Dict object:
class Dict(dict):
	'''
	Simple dict but support asscess as x.y style.

	>>> d1 = Dict()
	>>> d1['x'] = 100
	>>> d1.x
	100
	>>> d1.y = 200
	>>> d1['y']
	200
	>>> d2 = Dict(a=1, b=2, c='3')
	>>> d2.c
	'3'
	>>> d2['empty']
	Traceback (most recent call last):
	    ...
	KeyError: 'empty'
	>>> d2.empty
    Traceback (most recent call last):
        ...
    AttributeError: 'Dict' object has no attribute 'empty'
    >>> d3 = Dict(('a', 'b', 'c'), (1, 2, 3))
    >>> d3.a
    1
    >>> d3.b
    2
    >>> d3.c
    3
    '''
    def __init__(self, names=(), values=(), **kw):
    	super(Dict, self).__init__(**kw)
    	for k, v in zip(names, values):
    		self[k] = v

    def __getattr__(self, key):
    	try:
    		return self[key]
    	except KeyError:
    		raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
    	self[key] = value

def next_id(t=None):
	'''
	Return next id as 50-char string.

	Args:
	    t: unix timestamp, default to None and using time.time().
    '''
    if t is None:
    	t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)
    # (15(unix timestamp)+32(uuid4)+3('000')=50)

# record the status of sql, privte value, internal use only
def _profiling(start, sql=''):
	t = time.time() - start
	if t > 0.1:
		logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
	else:
		logging.info('[PROFILING] [DB] %s: %s' % (t, sql))

class DBError(Exception):
	pass

class MultiColumnsError(DBError):
    pass

# global engine object: to save the mysql db connection 
engine = None 

class _Engine(object):

	def __init__(self, connect):
		self._connect = connect

	def connect(self):
		return self._connect() 
	# call the func connect(), then return the results

# initialize the database connection
def create_engine(user=user, password=password, database, host='127.0.0.1', port=3306, **kw):
	import mysql.connector # import mysql driver
	global engine
	# check whether Engine is initialized or not
	if engine is not None:
		raise DBError('Engine is already initialized.')
	# bind DB info
	params = dict(user=user, password=password, database=database, host=host, port=port)
	# defaults settings
	defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
	for k, v in defaults.iteritems(): 
		#dict.iteritems() return  an iteration
		params[k] = kw.pop(k, v)
		# dict.pop():if k in kw,bind its value,or bind v
		# params are increased because of defaults & kw
	# add other params from kw
	params.update(kw) 
	params['buffered'] = True # flag
	engine = _Engine(lambda: mysql.connector.connect(**params))
	# lambda: anonymization, then 
	# return a func as param that is submitted to _Engine()
	# test connection...
	logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))


# now we can get a none-repetive link by global engine.


# DB connection and basic operations
class _LasyConnection(object):

	def __init__(self):
		self.connection = None

	def cursor(self):
		if self.connection is None:
			connection = engine.connect()
			logging.info('Open connection <%s>...' % hex(id(connection)))
			self.connection = connection
		return self.connection.cursor()

	def commit(self):
		return self.connection.commit()

	def rollback(self):
		# an operation to recall
		self.connection.rollback()

	def cleanup(self):
		# initialized and break the connnection
		if self.connection:
			connection = self.connection
			self.connection = None
			logging.info('close connection <%s>...' % hex(id(connection)))
			connection.close()

# thread local object, keep connection independent,private
class _DbCtx(threading.local):
	'''
	Thread local object that holds connection info.
	'''
	def __init__(self):
		self.connection = None
		self.transactions = 0

	def is_init(self):
		# False: uninitialized
		# True: initialized
		return not self.connection is None

	def init(self):
		logging.info('open lazy connection...')
		self.connection = _LasyConnection()
		self.transactions = 0 

	def cleanup(self):
		self.connection.cleanup()
		self.transactions = 0

	def cursor(self):
		'''
		return cursor
		'''
		return self.connection.cursor()

# thread-local db context, private instance:
_db_ctx = _DbCtx()


# now we can open or close a link through _db_ctx which is a thread-local object


# Design a context manager for connections
class _ConnectionCtx(object):
	'''
	_ConnectionCtx object that can open and close connection context. 
	_ConnectionCtx object can be nested and only the most outer connection has effect.

	with connection():
		pass
		with connection():
			pass
	'''
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		# check whether initialized or not
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup = True
		return self

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

# Decorator for using 'with' statement
def connection():
	'''
	return _ConnectionCtx object that can be used by 'with' statement:

	with connection():
		pass
	'''
	return _ConnectionCtx()

def with_connection(func):
	'''
	Decorator for reuse connection.

	@with_connection
	def foo(*args, **kw):
		f1()
		f2()
		f3()
	'''
	@functools.wraps(func)
	def _wrapper(*args, **kw):
		with connection(): # run DB connection
			return func(*args, **kw) # do some operation()
	return _wrapper

# Design a context manager for transactions(more complex)
class _TransactionCtx(object):
	'''
	_TransactionCtx object that can handle transactions.

	with _TransactionCtx():
		pass
	'''

	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			# needs open a connection first
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions = _db_ctx.transactions + 1
		logging.info('begin transaction...' if _db_ctx.transactions==1 else 'join current transaction...')
		return self

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions - 1
		try:
			if _db_ctx.transactions==0:
				# 0 means no transaction to be to deal with
				# check whether had exception or not
				if exctype is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):
		global _db_ctx
		logging.info('commit transaction...')
		try:
			_db_ctx.connection.commit()
			logging.info('commit ok.')
		except:
			logging.warning('commit failed. try rollback...')
			_db_ctx.connection.rollback()
			logging.warning('rollback ok.')
			raise

	def rollback(self):
		global _db_ctx
		logging.warning('rollback transaction...')
		_db_ctx.connection.rollback()
		logging.info('rollback ok.')

def transaction():
	'''
	create a transaction object so can use with statement:

	with transaction():
		pass

	>>> def update_profile(id, name, rollback):
    ...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
    ...     insert('user', **u)
    ...     r = update('update user set passwd=? where id=?', name.upper(), id)
    ...     if rollback:
    ...         raise StandardError('will cause rollback...')
    >>> with transaction():
    ...     update_profile(900301, 'Python', False)
    >>> select_one('select * from user where id=?', 900301).name
    u'Python'
    >>> with transaction():
    ...     update_profile(900302, 'Ruby', True)
    Traceback (most recent call last):
      ...
    StandardError: will cause rollback...
    >>> select('select * from user where id=?', 900302)
    []
    '''
    return _TransactionCtx()

def with_transaction(func):
	'''
	A decorator that makes function around transaction.

	>>> @with_transaction
    ... def update_profile(id, name, rollback):
    ...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
    ...     insert('user', **u)
    ...     r = update('update user set passwd=? where id=?', name.upper(), id)
    ...     if rollback:
    ...         raise StandardError('will cause rollback...')
    >>> update_profile(8080, 'Julia', False)
    >>> select_one('select * from user where id=?', 8080).passwd
    u'JULIA'
    >>> update_profile(9090, 'Robert', True)
    Traceback (most recent call last):
      ...
    StandardError: will cause rollback...
    >>> select('select * from user where id=?', 9090)
    []
    '''
    @functools.wraps(func)
    def _wrapper(*args, **kw):
    	_start = time.time()
    	with transaction():
    		return func(*args, **kw)
    	_profiling(_start)
    return _wrapper


# now transaction has been built.


# Building functions: select, update, 
def _select(sql, first, *args):
	'execute select SQL and return unique result or results.'
	global _db_ctx
	cursor = None
	# avoid SQL injection, we use '?' as placeholder
	# use '&s' to replace '?' when needs operation
	sql = sql.replace('?', '%s')
	logging.info('SQL: %s, ARGS: %s' % (sql, args))
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql, args) # return some info
		if cursor.description:
			names = [x[0] for x in cursor.description]
			# return description of the result set
			# x[0] called column name of each result set
		if first:
			values = cursor.fetchone()
			# fetch one/first result as values
			if not values:
				return None
				# break
			return Dict(names, values)
			# or return a dict that just has one/first result, break
		return [Dict(names, x) for x in cursor.fetchall()]
		# when 'first' is False, fecth all the results as a dict, and return,break
	finally:
		if cursor:
			cursor.close()

@with_connection
def select_one(sql, *args):
	'''
	Execute select SQL and expected one result. 
    If no result found, return None.
    If multiple results found, the first one returned.
    >>> u1 = dict(id=100, name='Alice', email='alice@test.org', passwd='ABC-12345', last_modified=time.time())
    >>> u2 = dict(id=101, name='Sarah', email='sarah@test.org', passwd='ABC-12345', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> u = select_one('select * from user where id=?', 100)
    >>> u.name
    u'Alice'
    >>> select_one('select * from user where email=?', 'abc@email.com')
    >>> u2 = select_one('select * from user where passwd=? order by email', 'ABC-12345')
    >>> u2.name
    u'Alice'
    '''
    return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
	'''
	Execute select SQL and expected one int and only int result.

	>>> n = update('delete from user')
    >>> u1 = dict(id=96900, name='Ada', email='ada@test.org', passwd='A-12345', last_modified=time.time())
    >>> u2 = dict(id=96901, name='Adam', email='adam@test.org', passwd='A-12345', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> select_int('select count(*) from user')
    2
    >>> select_int('select count(*) from user where email=?', 'ada@test.org')
    1
    >>> select_int('select count(*) from user where email=?', 'notexist@test.org')
    0
    >>> select_int('select id from user where email=?', 'ada@test.org')
    96900
    >>> select_int('select id, name from user where email=?', 'ada@test.org')
    Traceback (most recent call last):
        ...
    MultiColumnsError: Expect only one column.
    '''
    d = _select(sql, True, *args)
    # if u wanna know whether aresult had more than 1 column
    # use len() to check
    if len(d)!=1:
    	raise MultiColumnsError('Expect only one column.')
    return d.values()[0]

@with_connection
def select(sql, *args):
	'''
	Execute select SQL and return list or empty list if no result.
    
    >>> u1 = dict(id=200, name='Wall.E', email='wall.e@test.org', passwd='back-to-earth', last_modified=time.time())
    >>> u2 = dict(id=201, name='Eva', email='eva@test.org', passwd='back-to-earth', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> L = select('select * from user where id=?', 900900900)
    >>> L
    []
    >>> L = select('select * from user where id=?', 200)
    >>> L[0].email
    u'wall.e@test.org'
    >>> L = select('select * from user where passwd=? order by id desc', 'back-to-earth')
    >>> L[0].name
    u'Eva'
    >>> L[1].name
    u'Wall.E'
    '''
    return _select(sql, False, *args)

@with_connection
def _update(sql, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
    	cursor = _db_ctx.connection.cursor()
    	cursor.execute(sql, args)
    	r = cursor.rowcount
    	if _db_ctx.transactions==0:
    		# no transaction enviroment:
    		logging.info('auto commit')
    		_db_ctx.connection.commit()
    	return r
    finally:
    	if cursor:
    		cursor.close()

def insert(table, **kw):
	'''
	Execute insert SQL.

	>>> u1 = dict(id=2000, name='Bob', email='bob@test.org', passwd='bobobob', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> u2 = select_one('select * from user where id=?', 2000)
    >>> u2.name
    u'Bob'
    >>> insert('user', **u2)
    Traceback (most recent call last):
      ...
    IntegrityError: 1062 (23000): Duplicate entry '2000' for key 'PRIMARY'
    '''
    cols, args = zip(*kw.iteritems()) # data classification
    sql = 'insert into '%s' (%s) values (%s)' % (table, ','.join([''%s'' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    # build a table named param 'table'

def update(sql, *args):
    r'''
    Execute update SQL.
    >>> u1 = dict(id=1000, name='Michael', email='michael@test.org', passwd='123456', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> u2 = select_one('select * from user where id=?', 1000)
    >>> u2.email
    u'michael@test.org'
    >>> u2.passwd
    u'123456'
    >>> update('update user set email=?, passwd=? where id=?', 'michael@example.org', '654321', 1000)
    1
    >>> u3 = select_one('select * from user where id=?', 1000)
    >>> u3.email
    u'michael@example.org'
    >>> u3.passwd
    u'654321'
    >>> update('update user set passwd=? where id=?', '***', '123\' or id=\'456')
    0
    '''
    return _update(sql, *args)

if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG)
	create_engine('www-data', 'www-data', 'test')
	update('drop table if exists user')
	update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    import doctest
    doctest.testmod()