import json

import requests
import sys, os
sys.path.insert(0,  os.path.abspath('./python-tastypie-client'))

import tastypie_client

url_root = "http://vorol-dev.cdlib.org/"
path_collection_registry = "collection_registry/api/v1"
url_api = url_root+path_collection_registry

entrypoint_entrypoint_key = "list_entrypoint"
entrypoint_schema_key = "schema"
collection_name = "provenancialcollection"

####lass Tastypieentrypoint(object):
####   '''Tastypie list_entrypoint object'''
####   pass
####lass TastypieClient(object):
####   '''Tastypie (http://tastypieapi.org/) HATEOAS API client'''
####   def __init__(self, url_api):
####       self._url_api = url_api
####       self._entrypoints = None

####   @property
####   def entrypoints(self):
####       '''Returns a dict of entrypoint entry points'''
####       if not self._entrypoints:
####           self._entrypoints = json.loads(requests.get(url_root+path_collection_registry).text)
####       return self._entrypoints

####   def find(self, name_id):
####       '''find the resource entry point for the given resource name?'''
####       return self.entrypoints.get(name_id, None)

####p = TastypieClient(url_api)
####c = tp.find(collection_name) #need to return object for entrypoint, study tastypie_client code
####print pc
#####'''
#####url_collection_entrypoint = url_collection_schema = None
#####for c, d in hateoas.items():
#####    if c == collection_name:
#####        url_collection_entrypoint = d[entrypoint_entrypoint_key]
#####        url_collection_schema = d[entrypoint_schema_key]
#####entrypoint = json.loads(requests.get(url_root+url_collection_entrypoint).text)
#####print entrypoint['meta']
######TODO: wrap entrypoint in iterable object, uses meta & next to keep getting more
###### when currently downloaded exhausted. Perfect use of generator.
#####'''


tp = tastypie_client.Api(url_api)
provenancialcollection = None
for c in tp.collections:
    #print c, dir(c), c.url
    try:
        c.url.index(collection_name)
        provenancialcollection = c
    except:
        pass

print provenancialcollection.url 
#print type(provenancialcollection)
#print dir(provenancialcollection)
import time;time.sleep(5)

obj_list = []
for obj in provenancialcollection:#.next():
    #print "OBJ?", obj.fields
    if obj.fields['url_local']:
        print obj.fields['resource_uri'], obj.fields['url_local']
    obj_list.append(obj)
print "LENGTH:::", len(obj_list)
print "COLLECTION:"#, dir(provenancialcollection)
print provenancialcollection.meta
print obj.fields
#import code;code.interact(local=locals())
