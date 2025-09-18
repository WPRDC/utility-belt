import time
#from collections import OrderedDict
from gadgets import get_resource_name, get_number_of_rows, delete_row_from_resource, query_yes_no

resource_id = "4a984000-6ddb-4c77-a628-7e979ce3c6d3" # HyperB
resource_id = "48437601-7682-4b62-b754-5757a0fa3170"  # The Tessercat was Here

resource_id = "7e203d63-35a1-4ed9-90a5-6de70e406784" # Sample Parking Transactions  - Ad hoc zones

resource_id = "d54200cc-3127-4dba-9080-3dcb7f66708c" # BigBurgh Events archive
resource_id = "a540145a-0d1c-409c-80c7-c3707c2da0ff" # BigBurgh Services archive
resource_id = "d836ce20-0c97-4976-bde3-d63fe1af6b81" # BigBurgh Safe Places archive

from credentials import site, API_key

resource_name = get_resource_name(site, resource_id, API_key)
initial_count = get_number_of_rows(site, resource_id, API_key)

_id_end = 1599043-1+1
_id_start = 1619396 

_id_start = 1970
_id_end = 698 # The last one to delete # _id_end should be less than _id_start
assert _id_end < _id_start

print("Preparing to delete some rows (with _id values from {} to {}) of the {} rows from {} ({}) on {}".format(_id_end, _id_start, initial_count, resource_name, resource_id, site))
response = query_yes_no("Are you ready to delete some rows?", "no")

if response:
    for _id in range(_id_start, _id_end-1, -1): # It's necessary to subtract 1 from _id_end here
    # to go from an open range like
    #     [_id_start, _id_start-1, _id_start-2, ... _id_end+1]
    # to one that includes _id_end:
    #     [_id_start, _id_start-1, _id_start-2, ... _id_end+1, _id_end].

        # This all fails terribly and silently if the _id values exceed the size of the resource.
        #print('{} - '.format(_id),end='')
        #sys.stdout.write('{} - '.format(_id))
        print(f'Deleting row with _id == {_id}...')
        deleted = delete_row_from_resource(site, resource_id, _id, API_key)
        if not deleted:
            print(f"Failed to delete row with _id = {_id}. Halting...")
            break
        time.sleep(0.1)

    print(f'Now the table has {get_number_of_rows(site, resource_id, API_key)} rows.')
