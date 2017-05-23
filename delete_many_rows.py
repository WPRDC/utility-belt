import csv, json, time, sys
#from collections import OrderedDict
from push_to_CKAN_resource import push_data_to_ckan, DEFAULT_CKAN_INSTANCE
from gadgets import get_site, get_resource_name, get_number_of_rows, delete_row_from_resource, query_yes_no, fire_grappling_hook

server = "Live"
resource_id = "4a984000-6ddb-4c77-a628-7e979ce3c6d3" # HyperB
resource_id = "48437601-7682-4b62-b754-5757a0fa3170"  # The Tessercat was Here

site, API_key, _ = fire_grappling_hook('ckan_settings.json',server)

resource_name = get_resource_name(site,resource_id,API_key)
initial_count = get_number_of_rows(site,resource_id,API_key)

_id_end = 1599043-1+1
_id_start = 1619396 

_id_end = 9 # _id_end should be less than _id_start
_id_start = 10
print("Preparing to delete rows {} to {} of the {} rows from {} ({}) on {}".format(_id_end, _id_start, initial_count, resource_name, resource_id, site))
response = query_yes_no("Are you ready to delete some rows?", "no")

if response:
    for _id in range(_id_start,_id_end-1,-1): # It's necessary to subtract 1 from _id_end here
    # to go from an open range like
    #     [_id_start, _id_start-1, _id_start-2, ... _id_end+1]
    # to one that includes _id_end:
    #     [_id_start, _id_start-1, _id_start-2, ... _id_end+1, _id_end].

        # This all fails terribly and silently if the _id values exceed the size of the resource.
        #print('{} - '.format(_id),end='')
        #sys.stdout.write('{} - '.format(_id))
        print _id
        deleted = delete_row_from_resource(site,resource_id,_id,API_key)
        if not deleted:
            print("Failed to deleted row with _id = {}. Halting...".format(_id))
            break
        time.sleep(0.1)

    print(get_number_of_rows(site,resource_id,API_key))
