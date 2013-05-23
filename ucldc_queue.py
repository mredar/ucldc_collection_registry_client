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

DIR_QUEUES = "./queues"
SQLITE_DB = os.path.join(DIR_QUEUES, 'queue.sqlite3')

class UCLDC_Queue(object):
    '''A job queue for the UCLDC project.
    Use sorta RESTful fn names'''

    def __init__(self, name=None):
        super(UCLDC_Queue, self).__init__()
        #may need a queue name eventually and URL?
        #init queue (read from file or create new)
        assert(name)
        self._table_name = name
        self._dbconn = sqlite3.connect(SQLITE_DB)
        try:
            self._dbconn.execute("create table " + self._table_name + " (msgid integer primary key, status text, message text)")
        except sqlite3.OperationalError, e: #need more granularity
            print e
            pass

    def put(self, message):
        '''Add a message to queue'''
        self._dbconn.execute("insert into " + self._table_name + " values (null, 'READY', ?)", (message, ))
        self._dbconn.commit()

    def pop(self):
        '''Get the first "READY" message'''
        c = self._dbconn.execute("select * from " + self._table_name + " where status=\"READY\"")
        return c.fetchone()

    def get(self, msgid):
        '''Get them message for given id'''
        c = self._dbconn.execute("select * from " + self._table_name + " where msgid=?" , (msgid,))
        return c.fetchone()

    def update(self, id_msg, status_msg):
        '''Update a particular message's status'''
        pass

if __name__=='__main__':
    q = UCLDC_Queue('marks')
    q.put('HI')
    r = q.pop()
    print r
    t=q.get(2)
    print t

