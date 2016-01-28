#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Micheal Liao'
__learner__ = 'Monster Xiao'

'''
Database operation module.
'''

import time, uuid, functools, threading, logging

#Dict object:

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
    #(15(unix timestamp)+32(uuid4)+3('000')=50)

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

class _LasyConnection(object):

	def __init__(self):
		self.connection = None

	def cursor(self):
		if self.connection is None:
			connection = engine.connect()
			