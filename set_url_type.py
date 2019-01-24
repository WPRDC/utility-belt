import sys, csv, json, pprint
#from collections import OrderedDict
from push_to_CKAN_resource import push_data_to_ckan, DEFAULT_CKAN_INSTANCE
from gadgets import get_schema, get_fields, get_site, disable_downloading, get_resource_parameter, elicit_primary_key, get_resource_name, set_resource_parameters_to_values

#resource_id = "40776043-ad00-40f5-9dc8-1fde865ff571" # 311 data
#resource_id = "5d6e9a34-b000-4afc-9583-a65f18d83c51" #Fish Fry Locations
server = "Live"

#resource_id = "c0122191-c733-4fdb-af5d-b6578842e7ab" # Subways. Integers. Moof./Moofer - on stage.
#server = "Staging"
####################
#resource_id = "2c13021f-74a9-4289-a1e5-fe0472c89881" # Cumulative Crash Data
#server = "Live"

#sys.exit(1)
#

if len(sys.argv) < 3:
    print("Specify a resource ID to change the download URL of and the value to set that download URL to.")
else:
    resource_id = sys.argv[1]
    url_type = sys.argv[2]
    with open('ckan_settings.json') as f:
        settings = json.load(f)
        API_key = settings["API Keys"][server]
        site = get_site(settings,server)

    s = get_schema(site, resource_id, API_key)
    pprint.pprint(s)

    current_url_type = get_resource_parameter(site,resource_id,'url_type',API_key)
    print("current url_type = {}".format(current_url_type))
    #keys = elicit_primary_key(site,resource_id,API_key)
    #print("keys = {}".format(keys))
    name = get_resource_name(site,resource_id,API_key)
    print("resource name = {}".format(name))
    #print("The primary keys of {} ({}) are {}".format(name,resource_id,keys))
    print("\nSetting url_type to {}...".format(url_type))
#    set_resource_parameters_to_values(site,resource_id,['url_type'],[url_type],API_key)
