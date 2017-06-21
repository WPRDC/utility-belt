#!/usr/bin/env python
import sys, os
import pprint
import json

from gadgets import clone_resource, query_yes_no, dealias

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

def investigate_id(site,source_id,API_key):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    try: # if its_a_resource(source_id):
        metadata = ckan.action.resource_show(id=source_id)
        source_type = 'resource'
        source_name = metadata['name']
    except ckanapi.errors.NotFound:
        resource_alias = dealias(site,source_id,API_key)
        if resoure_alias is not None:
            source_type = 'resource alias'
            source_id = resource_alias
            source_name = ckan.action.resource_show(id=resource_alias)['name']
        else:
            try:
                metadata = ckan.action.package_show(id=source_id)
                source_type = 'package'
                source_name = metadata['title']
            except:
                return None, None, None
    return source_type, source_id, source_name

def main():
    path = os.path.dirname(os.path.realpath(__file__))
    source_id = sys.argv[1]

    site, API_key, settings = fire_grappling_hook(filepath='ckan_settings.json',server='Live'):

    # Is it a resource or a package or a resource alias?
    source_type, source_id, source_name = investigate_id(site,source_id,API_key)

    destination_id = None
    if len(sys.argv) > 2:
        destination_id = sys.argv[2]

    question = "Are you sure you want to clone {} {} ({})".format(source_type, source_id, source_name)
    if source_type != 'package':
        if destination_id is None:
            question += " to a completely new package?"
        else:
            question += " to destination ID {} ({})?".format(destination_id, desintation_name)
    response = query_yes_no(question, default="yes")
    if response:
        if destination_id is None:
            # Create a new package 

            destination_id = ckan.action.package_create
            destination_type = 'package'
        # Do the cloning

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
