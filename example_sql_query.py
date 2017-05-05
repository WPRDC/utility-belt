import csv, json
from util import get_fields, get_resource_name, get_site, query_resource, get_resource_data
import pprint


which = "Live"
with open('ckan_settings.json') as f:
    settings = json.load(f)
    API_key = settings["API Keys"][which]
    site = get_site(settings,which)
print(site)
#fs = get_fields(site, resource_id, API_key)
#print(fs)
#r, success = get_resource_data(site, resource_id, API_key, 3)
#pprint.pprint(r)
#query_constructor(selection = '*', from_part=resource_id, leftovers = '')

# Get the first three fish frys listed as being at churches:
#resource_id = "5d6e9a34-b000-4afc-9583-a65f18d83c51"
#r, success = query_resource(site,  'SELECT * FROM "{}" WHERE venue_type = \'Church\' LIMIT 3'.format(resource_id), API_key)
#pprint.pprint(r)

# Get the first three rows from the Tax Liens Summary table:
resource_id = "d1e80180-5b2e-4dab-8ec3-be621628649e"
r, success = query_resource(site,  'SELECT * FROM "{}" LIMIT 3'.format(resource_id), API_key)
pprint.pprint(r)
# SQL queries constraining a date field would have a part that looks like this:
#WHERE p.release_date > '2014-09-30';

