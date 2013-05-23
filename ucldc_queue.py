#! /usr/bin/env python

'''A wrapper for any queue implementation we use. For now, the queue for 
harvesting is just a csv file with id nums??
may use dirq, rabbitmq, etc.
Eventually will be networked queue manager which code from various languages can interact with.

The idea behind this is to decouple the code and creating distinct phases
to the processing.
The collection registry client will drive all this and have *knowledge* of the collections. The client will be the controller for processing, smartly pulling registry data as needed.
'''
import os
import sqlite3
import re

DIR_QUEUES = "./queues"
SQLITE_DB = os.path.join(DIR_QUEUES, 'queue.sqlite3')
valid_table_regex = re.compile(r'^[A-Za-z]\w*$')

class UCLDC_Queue(object):
    '''A job queue for the UCLDC project.
    Use sorta RESTful fn names'''

    def __init__(self, name=None):
        '''Initialize a new queue with given name

        >>> q = UCLDC_Queue(name='test-q')
        Traceback (most recent call last):
            ...
        Exception: Invalid queue name:test-q. Queue names must start with an alpha char are limited to alphanumeric and _ underscores.
        >>> q = UCLDC_Queue(name="_bad_again")
        Traceback (most recent call last):
            ...
        Exception: Invalid queue name:_bad_again. Queue names must start with an alpha char are limited to alphanumeric and _ underscores.
        >>> q = UCLDC_Queue(name="OK_this_7839q10985_name")
        >>> q.destroy_queue()
        '''
        super(UCLDC_Queue, self).__init__()
        #may need a queue name eventually and URL?
        #init queue (read from file or create new)
        if not valid_table_regex.match(name):
            raise Exception("Invalid queue name:" + name + ". Queue names must start with an alpha char are limited to alphanumeric and _ underscores.")
        self._table_name = name
        self._dbconn = sqlite3.connect(SQLITE_DB)
        try:
            self._dbconn.execute("create table " + self._table_name + " (msgid integer primary key, status text, message text)")
        except sqlite3.OperationalError, e: #need more granularity
            #print e
            pass

    def destroy_queue(self):
        '''Destroy the given queue, removing all entries'''
        self._dbconn.execute("drop table " + self._table_name)

    def __len__(self):
        '''How many in queue db?'''
        return self._dbconn.execute('select COUNT(*) from ' + self._table_name).fetchone()[0]

    def put(self, message):
        '''Add a message to queue
        
        >>> q = UCLDC_Queue(name='test_q')
        >>> msgid = q.put("HI FROM A TEST")
        >>> q.destroy_queue()
        '''
        cursor = self._dbconn.execute("insert into " + self._table_name + " values (null, 'READY', ?)", (message, ))
        msgid = cursor.lastrowid
        self._dbconn.commit()
        return msgid

    def pop(self):
        '''Get the first "READY" message

        >>> q = UCLDC_Queue(name='test_q')
        >>> msgid = q.put("HI FROM A TEST")
        >>> msgid = q.put("HI FROM A TEST 2")
        >>> msgid = q.put("HI FROM A TEST 3")
        >>> msg = q.pop()
        >>> print msg
        (1, u'READY', u'HI FROM A TEST')
        >>> q.destroy_queue()
        '''
        c = self._dbconn.execute("select * from " + self._table_name + " where status=\"READY\"")
        return c.fetchone()

    def get(self, msgid):
        '''Get them message for given id

        >>> q = UCLDC_Queue(name='test_q')
        >>> msgid = q.put("HI FROM A TEST")
        >>> msgid = q.put("HI FROM A TEST 2")
        >>> msgid = q.put("HI FROM A TEST 3")
        >>> msg = q.get(msgid)
        >>> print msg
        (3, u'READY', u'HI FROM A TEST 3')
        >>> q.destroy_queue()
        '''
        c = self._dbconn.execute("select * from " + self._table_name + " where msgid=?" , (msgid,))
        return c.fetchone()

    def update(self, msgid, status_msg):
        '''Update a particular message's status'''
        pass

    def delete(self, msgid):
        '''Delete given message. This is used once a subprocess successfully
        completes it's tasks.

        >>> q = UCLDC_Queue(name='test_q')
        >>> msgid = q.put("HI FROM A TEST")
        >>> msgid = q.put("HI FROM A TEST 2")
        >>> msgid = q.put("HI FROM A TEST 3")
        >>> print len(q)
        3
        >>> msg = q.delete(msgid)
        >>> print len(q)
        2
        >>> q.destroy_queue()
        '''
        c = self._dbconn.execute("delete from " + self._table_name + " where msgid=?" , (msgid,))


if __name__=='__main__':
    import doctest;doctest.testmod()
