import sys, os
sys.path.insert(0,  os.path.abspath('./python-tastypie-client'))

import tastypie_client

url_root = "http://vorol-dev.cdlib.org/"
path_collection_registry = "collection_registry/api/v1"
url_api = url_root+path_collection_registry

entrypoint_entrypoint_key = "list_entrypoint"
entrypoint_schema_key = "schema"
collection_name = "provenancialcollection"

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
