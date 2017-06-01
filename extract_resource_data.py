#!/usr/bin/env python
import sys, os
import pprint
import json

from gadgets import write_to_csv, get_all_records, get_site, get_fields, get_metadata

def obtain_resource(site,r_id,API_key,filename=None):
    # This function pulls information from a particular resource on a
    # CKAN instance and outputs it to a CSV file.

    # Tests suggest that there are some small differences between this
    # file and the one downloaded directly through the CKAN web
    # interface: 1) the row order is different (this version is ordered
    # by _id (logically) while the downloaded version has the same weird
    # order as that shown in the Data Explorer (which can have an _id
    # sequence like 1,2,3,4,5,6,7,45,8,9,10... for no obvious reason
    # (it may be that the data is being sorted by the order they appear
    # in the database, which may be by their time of last update...))
    # and 2) weird non-ASCII characters are not being handled optimally,
    # so probably some work on character encoding is in order in the
    # Python script.

    if filename is None:
        filename = "{}.csv".format(r_id)

    try:
        list_of_dicts = get_all_records(site, r_id, API_key, chunk_size=5000)
        fields = get_fields(site,r_id,API_key)
        metadata = get_metadata(site,r_id,API_key)
    except:
        print("Something went wrong and the resource/fields/metadata was not obtained.")
        return False

    #Eliminate _id field
    fields.remove("_id")
    print("The resource has the following fields: {}".format(fields))
    write_to_csv(filename,list_of_dicts,fields)
    metaname = filename + '-metadata.json'
    with open(metaname, 'w') as outfile:
        json.dump(metadata, outfile, indent=4, sort_keys=True)

    return True

def main():
    path = os.path.dirname(os.path.realpath(__file__))
    resource_id = sys.argv[1]
    filename = None
    if len(sys.argv) > 2:
        filename = sys.argv[2]
    server = "Live"

    settings_file = '/ckan_settings.json'
    if len(sys.argv) > 3:
        settings_file = sys.argv[3]

    with open(path+settings_file) as f:
        settings = json.load(f)
        API_key = settings["API Keys"][server]
        site = get_site(settings,server)

    success = obtain_resource(site,resource_id,API_key,filename)

############

if __name__ == '__main__':
  main()
