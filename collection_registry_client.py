#! /usr/bin/env python
import os
import json
import tastypie_client
import boto.sqs

QUEUE_OAI_HARVEST = os.environ.get('QUEUE_OAI_HARVEST', 'OAI_harvest')

url_root = "http://vorol-dev.cdlib.org/"
url_root = "http://127.0.0.1:8000/"
path_collection_registry = "api/v1"
url_api = url_root+path_collection_registry
collection_obj_name = "collection"

tp = tastypie_client.Api(url_api)
collection = None
for c in tp.collections:
    try:
        c.url.index(collection_obj_name) #this throws if name not found
        collection = c
    except:
        pass

print collection.url 

#queue for OAI
conn=boto.sqs.connect_to_region('us-east-1')
q_oai = conn.get_queue(QUEUE_OAI_HARVEST)

obj_list = []
for obj in collection:
    #Currently we only know how to harvest OAI type collections
    #right now, we key off of which fields are found in the object
    #add a message to appropriate queue
    if obj.fields['url_oai'] != '':
        #TODO: Use json to serialize dict
        msg_dict = { 'url': obj.fields['url_oai'],
                'set_spec': obj.fields['oai_set_spec'],
                'campus': obj.fields['campus'],
                'repository': obj.fields['repository'],
                'collection_name': obj.fields['name']
                }
        msg = json.dumps(msg_dict)
        print "PUTTING MSG ON OAI Q:", msg
        q_msg = boto.sqs.message.Message()
        q_msg.set_body(msg)
        status = q_oai.write(q_msg)
