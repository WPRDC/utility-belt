# -*- coding: utf-8 -*-
import os, sys
import re
import csv
from collections import OrderedDict
from json import loads, dumps
import json
import operator
import time
import urllib
try:
    from urlparse import urlparse # Python 2
except:
    from urllib.parse import urlparse # Python 3 renamed urlparse.

import pprint
from icecream import ic

try:
    import datapusher
except:
    from . import datapusher # Python 3/prime_ckan workaround

import ckanapi
#from ckanapi import ValidationError

import traceback

def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    # obtained from https://code.activestate.com/recipes/577058/

    # Then modified to work under both Python 2 and 3.
    # (Python 3 renamed "raw_input()" to "input()".)
    global input

    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    try: input = raw_input
    except NameError: pass

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

def fire_grappling_hook(filepath='ckan_settings.json',server='Stage'):
    # Get parameters to communicate with a CKAN instance
    # from the specified JSON file.
    with open(filepath) as f:
        settings = json.load(f)
        API_key = settings["API Keys"][server]
        site = get_site(settings,server)

    return site, API_key, settings


## FUNCTIONS RELATED TO DATASTORE ALIASES ##
def get_resource_aliases(site,resource_id):
    # If a resource ID is an alias for the real resource ID, this function will
    # convert the pseudonym into the real resource ID and return it.
    ckan = ckanapi.RemoteCKAN(site)
    results = ckan.action.datastore_search(id='_table_metadata',filters={'alias_of':resource_id})['records']
    known_aliases = [r['name'] for r in results]
    return known_aliases

def dealias(site,pseudonym):
    # If a resource ID is an alias for the real resource ID, this function will
    # convert the pseudonym into the real resource ID and return it.
    ckan = ckanapi.RemoteCKAN(site)
    alias_response = ckan.action.datastore_search(id='_table_metadata',filters={'name': pseudonym})
    aliases = alias_response['records']
    if len(aliases) > 0:
        resource_id = aliases[0]['alias_of']
        return resource_id
    else:
        return None

def add_aliases_to_resource(site,resource_id,API_key,aliases=[],overwrite=False):
    # Add one or more datastore aliases to an existing CKAN resource.
    # Set the "overwrite" flag to True to just replace the resource's existing
    # aliases with the ones passed in the aliases argument.
    if not overwrite:
        # Get existing aliases.
        known_aliases = get_resource_aliases(site,resource_id)
        # Add new aliases to existing aliases.
        if type(aliases) == list:
            aliases = known_aliases + aliases
        elif type(aliases) == str:
            aliases = known_aliases + aliases.split(',')
    # Push list of aliases back to the datastore.
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    outcome = ckan.action.datastore_create(resource_id=resource_id,aliases=aliases,force=True)
    return outcome

## END OF FUNCTIONS RELATED TO DATASTORE ALIASES ##

def resource_show(ckan,resource_id):
    # A wrapper around resource_show (which could be expanded to any resource endpoint)
    # that tries the action, and if it fails, tries to dealias the resource ID and tries
    # the action again.
    try:
        metadata = ckan.action.resource_show(id=resource_id)
    except ckanapi.errors.NotFound:
        # Maybe the resource_id is an alias for the real one.
        real_id = dealias(site,resource_id)
        metadata = ckan.action.resource_show(id=real_id)
    except:
        msg = "{} was not found on that CKAN instance".format(resource_id)
        print(msg)
        raise ckanapi.errors.NotFound(msg)

    return metadata

def initialize_datastore(resource_id, ordered_fields, keys=None, settings_file='ckan_settings.json', server='Live'):
    # For a CKAN resource that already exists (identified by resource_id)
    # on a CKAN instance specified by the settings in the JSON
    # settings_file and the specified server, reset the datastore
    # (deleting all stored data) and create a new datastore with the
    # field given by ordered_fields (giving the order, names, and types
    # of the fields). The primary key or keys are given in the keys
    # argument.
    with open(settings_file) as f:
        settings = json.load(f)
    dp = datapusher.Datapusher(settings, server=server)
    dp.delete_datastore(resource_id)
    # Example of ordered_fields and keys:
    #ordered_fields = [{"id": "Zone", "type": "text"}]
    #ordered_fields.append({"id": "Start", "type": "timestamp"})
    #ordered_fields.append({"id": "End", "type": "timestamp"})
    #keys = ["Zone", "UTC Start"]

    call_result = dp.create_datastore(resource_id, ordered_fields, keys=keys)
    print("Datastore creation result: {}".format(call_result))
    return call_result

def get_site(settings,server):
    # From the dictionary obtained from ckan_settings.json,
    # extract the URL for a particular CKAN server and return it.
    url = settings["URLs"][server]["CKAN"]
    scheme = urlparse(url).scheme
    hostname = urlparse(url).hostname
    return "{}://{}".format(scheme,hostname)

def get_number_of_rows(site,resource_id,API_key=None):
    """Returns the number of rows in a datastore. Note that even when there is a limit
    placed on the number of results a CKAN API call can return, this function will
    still give the true number of rows."""
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    try:
        results_dict = ckan.action.datastore_info(id = resource_id)
        return results_dict['meta']['count']
    except:
        return None

def get_fields(site,resource_id,API_key=None):
    # In principle, it should be possible to do this using the datastore_info
    # endpoint instead and taking the 'schema' part of the result.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
        fields = [d['id'] for d in schema]
    except:
        return None

    return fields

def schema_dict(schema):
    # A schema looks like this:
    #[{'id': '_id', 'type': 'int4'},
    # {'id': 'pin', 'type': 'text'},
    # {'id': 'block_lot', 'type': 'text'},
    # {'id': 'filing_date', 'type': 'date'},
    # {'id': 'tax_year', 'type': 'int4'},
    # {'id': 'dtd', 'type': 'text'},
    # {'id': 'lien_description', 'type': 'text'},
    # {'id': 'municipality', 'type': 'text'},
    # {'id': 'ward', 'type': 'text'},
    # {'id': 'last_docket_entry', 'type': 'text'},
    # {'id': 'amount', 'type': 'float8'},
    # {'id': 'assignee', 'type': 'text'}]
    # This function converts it into a more useful form:
    # {'_id': 'int4', 'pin': 'text',...}
    d = {}
    for s in schema:
        d[s['id']] = s['type']
    return d

def get_schema(site,resource_id,API_key=None):
    # In principle, it should be possible to do this using the datastore_info
    # endpoint instead and taking the 'schema' part of the result.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
    except:
        return None

    return schema

def get_metadata(site,resource_id,API_key=None):
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = resource_show(ckan,resource_id)
    except:
        return None

    return metadata

def get_package_parameter(site,package_id,parameter=None,API_key=None):
    """Gets a CKAN package parameter. If no parameter is specified, all metadata
    for that package is returned."""
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = ckan.action.package_show(id=package_id)
    if parameter is None:
        return metadata
    else:
        if parameter in metadata:
            return metadata[parameter]
        else:
            return None

def delete_resource(site, resource_id, API_key=None):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    results_dict = ckan.action.resource_delete(id=resource_id)
    return results_dict

def get_resource_parameter(site,resource_id,parameter=None,API_key=None):
    # Some resource parameters you can fetch with this function are
    # 'cache_last_updated', 'package_id', 'webstore_last_updated',
    # 'datastore_active', 'id', 'size', 'state', 'hash',
    # 'description', 'format', 'last_modified', 'url_type',
    # 'mimetype', 'cache_url', 'name', 'created', 'url',
    # 'webstore_url', 'mimetype_inner', 'position',
    # 'revision_id', 'resource_type'
    # Note that 'size' does not seem to be defined for tabular
    # data on WPRDC.org. (It's not the number of rows in the resource.)
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = ckan.action.resource_show(id=resource_id)
    if parameter is None:
        return metadata
    else:
        return metadata[parameter]

def get_resource_parameter_aliases(site,resource_id,parameter=None,API_key=None):
    # Some resource parameters you can fetch with this function are
    # 'cache_last_updated', 'package_id', 'webstore_last_updated',
    # 'datastore_active', 'id', 'size', 'state', 'hash',
    # 'description', 'format', 'last_modified', 'url_type',
    # 'mimetype', 'cache_url', 'name', 'created', 'url',
    # 'webstore_url', 'mimetype_inner', 'position',
    # 'revision_id', 'resource_type'
    # Note that 'size' does not seem to be defined for tabular
    # data on WPRDC.org. (It's not the number of rows in the resource.)
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = resource_show(ckan,resource_id)
    if parameter is None:
        return metadata
    else:
        return metadata[parameter]

def get_resource_name(site,resource_id,API_key=None):
    return get_resource_parameter(site,resource_id,'name',API_key)

def get_package_name_from_resource_id(site,resource_id,API_key=None):
    p_id = get_resource_parameter(site,resource_id,'package_id',API_key)
    return get_package_parameter(site,p_id,'title',API_key)

def find_resource_id(site,package_id,resource_name,API_key=None):
#def get_resource_id_by_resource_name():
    # Get the resource ID given the package ID and resource name.
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

def query_resource(site,query,API_key=None):
    # Use the datastore_search_sql API endpoint to query a CKAN resource.


    # Note that this doesn't work for private datasets.
    # The relevant CKAN GitHub issue has been closed.
    # https://github.com/ckan/ckan/issues/1954
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search_sql(sql=query)
    # A typical response is a dictionary like this
    #{u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'_full_text', u'type': u'tsvector'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'records': [{u'_full_text': u"'0001b00010000000':1 '11':2 '13585.47':3",
    #               u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_full_text': u"'0001c00058000000':3 '2':2 '7827.64':1",
    #               u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_full_text': u"'0001c01661006700':3 '1':1 '3233.59':2",
    #               u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}]
    # u'sql': u'SELECT * FROM "d1e80180-5b2e-4dab-8ec3-be621628649e" LIMIT 3'}
    data = response['records']

    # Note that if a CKAN table field name is a Postgres reserverd word, you
    # get a not-very-useful error
    #      (e.g., 'query': ['(ProgrammingError) syntax error at or near
    #     "on"\nLINE 1: SELECT * FROM (SELECT load, on FROM)
    # and you need to escape the reserved field name with double quotes.

    # These seem to be reserved Postgres words:
    # ALL, ANALYSE, ANALYZE, AND, ANY, ARRAY, AS, ASC, ASYMMETRIC, AUTHORIZATION, BETWEEN, BINARY, BOTH, CASE, CAST, CHECK, COLLATE, COLUMN, CONSTRAINT, CREATE, CROSS, CURRENT_DATE, CURRENT_ROLE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_USER, DEFAULT, DEFERRABLE, DESC, DISTINCT, DO, ELSE, END, EXCEPT, FALSE, FOR, FOREIGN, FREEZE, FROM, FULL, GRANT, GROUP, HAVING, ILIKE, IN, INITIALLY, INNER, INTERSECT, INTO, IS, ISNULL, JOIN, LEADING, LEFT, LIKE, LIMIT, LOCALTIME, LOCALTIMESTAMP, NATURAL, NEW, NOT, NOTNULL, NULL, OFF, OFFSET, OLD, ON, ONLY, OR, ORDER, OUTER, OVERLAPS, PLACING, PRIMARY, REFERENCES, RIGHT, SELECT, SESSION_USER, SIMILAR, SOME, SYMMETRIC, TABLE, THEN, TO, TRAILING, TRUE, UNION, UNIQUE, USER, USING, VERBOSE, WHEN, WHERE

    return data

def query_any_resource(site,query,resource_id,API_key=None):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    # From resource ID determine package ID.
    package_id = ckan.action.resource_show(id=resource_id)['package_id']
    # From package ID determine if the package is private.
    private = ckan.action.package_show(id=package_id)['private']
    if private:
        print("As of February 2018, CKAN still doesn't allow you to run a datastore_search_sql query on a private dataset. Sorry. See this GitHub issue if you want to know a little more: https://github.com/ckan/ckan/issues/1954")
        raise ValueError("CKAN can't query private resources (like {}) yet.".format(resource_id))
    else:
        return query_resource(site,query,API_key)

def get_resource_data(site,resource_id,API_key=None,count=50,offset=0,fields=None):
    # Use the datastore_search API endpoint to get <count> records from
    # a CKAN resource starting at the given offset and only returning the
    # specified fields in the given order (defaults to all fields in the
    # default datastore order).
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    if fields is None:
        response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset)
    else:
        response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset, fields=fields)
    # A typical response is a dictionary like this
    #{u'_links': {u'next': u'/api/action/datastore_search?offset=3',
    #             u'start': u'/api/action/datastore_search'},
    # u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'limit': 3,
    # u'records': [{u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}],
    # u'resource_id': u'd1e80180-5b2e-4dab-8ec3-be621628649e',
    # u'total': 88232}
    data = response['records']
    return data

def get_all_records(site,resource_id,API_key=None,chunk_size=5000):
    all_records = []
    failures = 0
    k = 0
    offset = 0 # offset is almost k*chunk_size (but not quite)
    row_count = get_number_of_rows(site,resource_id,API_key)
    if row_count == 0: # or if the datastore is not active
       print("No data found in the datastore.")
       success = False
    while len(all_records) < row_count and failures < 5:
        time.sleep(0.01)
        try:
            records = get_resource_data(site,resource_id,API_key,chunk_size,offset)
            if records is not None:
                all_records += records
            failures = 0
            offset += chunk_size
        except:
            failures += 1

        # If the number of rows is a moving target, incorporate
        # this step:
        #row_count = get_number_of_rows(site,resource_id,API_key)
        k += 1
        print("{} iterations, {} failures, {} records, {} total records".format(k,failures,len(records) if records is not None else 0,len(all_records)))

        # Another option for iterating through the records of a resource would be to
        # just iterate through using the _links results in the API response:
        #    "_links": {
        #  "start": "/api/action/datastore_search?limit=5&resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1",
        #  "next": "/api/action/datastore_search?offset=5&limit=5&resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"
        # Like this:
            #if r.status_code != 200:
            #    failures += 1
            #else:
            #    URL = site + result["_links"]["next"]

        # Information about better ways to handle requests exceptions:
        #http://stackoverflow.com/questions/16511337/correct-way-to-try-except-using-python-requests-module/16511493#16511493

    return all_records


# A function to upsert data would look like this:
def push_new_data(site,resource_id,API_key,list_of_dicts,method='upsert'):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    outcome = ckan.action.datastore_upsert(resource_id=resource_id,records=list_of_dicts,method=method,force=True)
#   outcome = ckan.action.datastore_upsert(resource_id='48437601-7682-4b62-b754-5757a0fa3170',records=[{'Number':23,'Another Number':798,'Subway':'FALSE','Foodtongue':'Ice cream sandwich'}],method='upsert',force=True)
    return outcome

def copy_all_records(site,source_resource_id,destination_resource_id,API_key=None,chunk_size=5000):
    # This is based on get_all_records. It might be interesting to pass
    # a generalized version of these functions something that would let it
    # either copy the records to a second repository (and return a Boolean)
    # or just return all the records. The best way of structuring such
    # a generalized iteration over the records is not clear (though passing
    # a function that either uploads data or does nothing might be a nice
    # approach.
    total_records = 0
    failures = 0
    failure_limit = 5
    k = 0
    offset = 0 # offset is almost k*chunk_size (but not quite)
    row_count = get_number_of_rows(site,source_resource_id,API_key)
    if row_count == 0: # or if the datastore is not active
       print("No data found in the datastore.")
       return False

    # Filter out the annoying _id column by getting the field names,
    # deleting "_id", and passing the resulting list to
    # get_resource_data.
    fields = get_fields(site,source_resource_id,API_key)
    fields.remove('_id')

    while total_records < row_count and failures < failure_limit:
        time.sleep(0.02)
        try:
            records = get_resource_data(site,source_resource_id,API_key,chunk_size,offset,fields)
            failures = 0
            offset += chunk_size
            outcome = push_new_data(site,destination_resource_id,API_key,records,'upsert')
            total_records += len(records)
        except:
            failures += 1

        # If the number of rows is a moving target, incorporate
        # this step:
        row_count = get_number_of_rows(site,source_resource_id,API_key)

        k += 1
        print("{} iterations, {} failures, {} records, {} total records".format(k,failures,len(records),total_records))

    return (failures < failure_limit)


## PRIMARY KEY FUNCTIONS ##
def elicit_primary_key(site,resource_id,API_key):
    # This function uses a workaround to determine the primary keys of a resource
    # from a CKAN API call.

    # Note that it has not been tested on primary-key-less resources and this represents
    # kind of a problem because, if used on such a resource, it will succeed in adding
    # the duplicate row to the table.
    primary_keys = None
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        # Get the very last row of the resource.
        row_count = get_number_of_rows(site,resource_id,API_key)
        records = get_resource_data(site,resource_id,API_key,count=1)
        first_row = records[0]
        # Try to insert it into the database
        del first_row["_id"]
        results = ckan.action.datastore_upsert(resource_id=resource_id, method='insert',
            records=[first_row], force=True)
        pprint.pprint(results)
    except ckanapi.ValidationError as exception:
        orig = exception.error_dict['info']['orig']
        print(orig)
        details = orig.split('\n')[1]

        string_of_keys = re.sub(r'\)=\(.*', '', re.sub(r'DETAIL:  Key \(','',details))
        primary_keys = string_of_keys.split(', ')

        # The above works if the keys are lowercased and have no spaces.
        # Otherwise, it seems that they are returned like this:
        # [u'"Key Number 1"', u'"Another Key that is Primary"']
        # so some extra processing is required.
        revised_primary_keys = []
        for pk in primary_keys:
            if pk[0] == u'"' and pk[-1] == u'"':
                pk = pk[1:-1]
            revised_primary_keys.append(pk)
        primary_keys = revised_primary_keys
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Error: {}".format(exc_type))
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('!!! ' + line for line in lines))
    else:
        new_row_count = get_number_of_rows(site,resource_id,API_key)
        records = get_resource_data(site,resource_id,API_key,count=1,offset=new_row_count-1)
        last_row = records[0]
        value_of_id = int(last_row['_id'])
        msg = "This function was run on a resource that has no primary key"
        msg += " and therefore added a duplicate row that was never intended to be added."
        msg += " The correct thing to do here is to delete"
        msg += " row with _id = {}".format(value_of_id)
        print(msg)

        if new_row_count == row_count+1:
            # Delete the last row (if it matches the one that was just added):
            del last_row['_id']
            if last_row == first_row:
                print("Deleting the last row...")
                deleted = delete_row_from_resource(site,resource_id,value_of_id,API_key)
            else:
                print("The last row doesn't match the added row, even though the number of rows",
                    "has increased")

        primary_keys = []

    return primary_keys

def set_primary_keys(site,resource_id,API_key,keys):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    outcome = ckan.action.datastore_create(resource_id=resource_id,primary_key=keys,force=True)
    return outcome
## END OF PRIMARY KEY FUNCTIONS ##
def create_resource_parameter(site,resource_id,parameter,value,API_key):
    """Creates one parameter with the given value for the specified
    resource."""
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    payload = {}
    payload['id'] = resource_id
    payload[parameter] = value
    #For example,
    #   results = ckan.action.resource_patch(id=resource_id, url='#', url_type='')
    results = ckan.action.resource_patch(**payload)
    print(results)
    print("Created the parameter {} with value {} for resource {}".format(parameter, value, resource_id))
    success = True

def set_resource_parameters_to_values(site,resource_id,parameters,new_values,API_key):
    """Sets the given resource parameters to the given values for the specified
    resource.

    This fails if the parameter does not currently exist. (In this case, use
    create_resource_parameter().)"""
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    original_values = [get_resource_parameter(site,resource_id,p,API_key) for p in parameters]
    payload = {}
    payload['id'] = resource_id
    for parameter,new_value in zip(parameters,new_values):
        payload[parameter] = new_value
    #For example,
    #   results = ckan.action.resource_patch(id=resource_id, url='#', url_type='')
    results = ckan.action.resource_patch(**payload)
    print(results)
    print("Changed the parameters {} from {} to {} on resource {}".format(parameters, original_values, new_values, resource_id))

# Comment out this function since it's not working as intended yet.
#def recast_field(site,resource_id,field,new_type,API_key):
#    # Experiments suggest that this function can be used to convert an integer field
#    # to a string field (text), but that if you try to convert back, the string
#    # values in that field do not get converted back to integers (though the field
#    # itself does appear to have type numeric.
#
#    # Perhaps a proper recasting function would need to iterate through the data
#    # and fix the types (or possibly download everything, reset the datastore, and
#    # then upload it all with the proper types).
#
#    schema = get_schema(site,resource_id,API_key)
#    if schema[0]['id'] == '_id':
#        new_schema = schema[1:]
#        for d in new_schema:
#            if d['id'] == field:
#                d['type'] == new_type
#        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
#        outcome = ckan.action.datastore_create(resource_id=resource_id,fields=new_schema, force=True)
#        print("Verifying that the schema has changed...")
#        final_schema = get_schema(site,resource_id,API_key)
#        return final_schema
#    else:
#        print("Unable to eliminate the _id field from this schema")
#        return schema

def delete_row_from_resource(site,resource_id,_id,API_key):
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        response = ckan.action.datastore_delete(id=resource_id, filters={"_id":_id}, force=True)
        success = True
    except:
        success = False
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Error: {}".format(exc_type))
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('!!! ' + line for line in lines))
    return success


def disable_downloading(site,resource_id,API_key=None):
    # Under CKAN, if the user tries to download a huge table,
    # CKAN tries to generate a CSV (storing all the data in
    # memory), exhausts the server's memory supplies, and
    # causes the server to crash.

    # As part of a temporary workaround, we use this function
    # to change the URL parameter from a link that triggers a
    # dump from the datastore to a "#" symbol.
    return set_resource_parameters_to_values(site,resource_id,['url','url_type'],['#',''],API_key)

##### Resource-scale operations #####
def clone_resource(site,source_resource_id,API_key,destination_package_id=None):
    """Clone a resource.

    Makes a copy of the resource specified by the given resource ID and puts
    it in the package specified by the supplied package ID.

    If no destination_package_id is given, the resource is cloned
    to the source package (dataset).

    Things to be cloned include Filestore files; Datastore data, schema, and
    primary keys; metadata; and resource views."""


    # To do: Generalize dealias function and use it here to allow
    # an alias to be used instead when specifying source_resource_id.
    print("This function is currently in beta and only does a subset of cloning operations.")

    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)

    # Get metadata

    metadata = get_metadata(site,source_resource_id,API_key)
    # Returns stuff like this:
    # {u'cache_last_updated': None,
    # u'cache_url': None,
    # u'created': u'2017-04-25T15:20:28.470100',
    # u'datastore_active': True, <<<<<<<<<<<<<<
    # u'description': u'', <<<<<<<<<<<<
    # u'format': u'CSV', <<<<<<<<<<<<<<<<
    # u'hash': u'',
    # u'id': u'683e38ea-338c-474f-a56f-1aa553a55443',
    # u'last_modified': u'2017-05-17T16:59:17.116500',
    # u'mimetype': None,
    # u'mimetype_inner': None,
    # u'name': u'Parking Data', <<<<<<<<<<<<<<<
    # u'package_id': u'530f334b-4d7c-40c5-bf50-ba55645bb8b3',
    # u'position': 3,
    # u'resource_type': None,
    # u'revision_id': u'9e086e5f-f171-451e-a827-38cb24f5482b', <<<<<<<<<<<<< Interesting.
    # u'size': None,
    # u'state': u'active', <?????????????
    # u'url': u'https://data.wprdc.org/datastore/dump/683e38ea-338c-474f-a56f-1aa553a55443', << The url
    #  << parameter is required when creating a resource (in our outdated CKAN version), but I think
    #  << we can just put an octothorpe there initially.
    # u'url_type': u'datapusher',
    # u'webstore_last_updated': None,
    # u'webstore_url': None}
    # This is just the results of resource_show.
    name = metadata['name']
    position_in_list = metadata['position']
    source_package_id = metadata['package_id']
    if destination_package_id is None:
        destination_package_id = source_package_id
        name = 'Clone of ' + metadata['name']

    datastore_active = metadata['datastore_active']
    r_format = metadata['format'] # Consider checking whether this format is an oddball format
                                  # like "CSV" vs. ".csv" vs. "csv".
    # How does creating the resource fail if the package ID does not map to an existing package?
    cloned_resource_as_dict = ckan.action.resource_create(package_id=destination_package_id,url='#',format=r_format,name=name)

    pprint.pprint(cloned_resource_as_dict)
    clone_resource_id = cloned_resource_as_dict['id']
    # Get datastore data (if any)
    if datastore_active:
        #ckan.action.resource_patch(id=clone_resource_id,url_type='datastore',url=site)
        # It seems like patching the resource to have a url_type of 'datastore' did avoid needing to force
        # the creation of the datastore below, but always resulted in a URL that was just
        #   "/datastore/dump/<clone_resource_id>"
        # and which was never hyperlinked (and could seemingly never be changed to be hyperlinked thereafter).
        schema = get_schema(site,source_resource_id,API_key)
        if schema[0] == {u'type': u'int4', u'id': u'_id'}:
            del schema[0]
        pprint.pprint(schema)
        primary_keys = elicit_primary_key(site,source_resource_id,API_key)

        call_result = ckan.action.datastore_create(resource_id=clone_resource_id,fields=schema,primary_key=primary_keys,force=True)
        last_part = get_resource_parameter(site,clone_resource_id,'url',API_key)
        outcome = ckan.action.resource_patch(id=clone_resource_id,url='https://data.wprdc.org/datastore/dump/'+clone_resource_id,force=True)
        #pprint.pprint(outcome)

        print(get_resource_parameter(site,clone_resource_id,'url',API_key))
        # Loop through rows of data in chunks, getting data and putting it into the new resource
        copy_success = copy_all_records(site,source_resource_id,clone_resource_id,API_key,chunk_size=5000)
        success = copy_success
    else:
        success = True

    #success = False
    #try:
    #    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    #    response = ckan.action.datastore_delete(id=resource_id, filters={"_id":_id}, force=True)
    #    success = True
    #except:
    #    success = False
    #    exc_type, exc_value, exc_traceback = sys.exc_info()
    #    print("Error: {}".format(exc_type))
    #    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    #    print(''.join('!!! ' + line for line in lines))
    #return success

    return success

##### End of resource-scale operations #####

##### (Some) dataset-scale operations #####
# get_package_parameter is defined above. #

def set_package_parameters_to_values(site,package_id,parameters,new_values,API_key):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    original_values = [get_package_parameter(site,package_id,p,API_key) for p in parameters]
    payload = {}
    payload['id'] = package_id
    for parameter,new_value in zip(parameters,new_values):
        payload[parameter] = new_value
    results = ckan.action.package_patch(**payload)
    print(results)
    print("Changed the parameters {} from {} to {} on package {}".format(parameters, original_values, new_values, package_id))

##### End of dataset-scale operations #####

##### Beginning of data-dictionary operations #####
def get_data_dictionary(site, resource_id, API_key=None):
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results = ckan.action.datastore_search(resource_id=resource_id)
        return results['fields']
    except ckanapi.errors.NotFound: # Either the resource doesn't exist, or it doesn't have a datastore.
        return None

def set_data_dictionary(site, resource_id, fields, API_key=None):
    # Here "fields" needs to be in the same format as the data dictionary
    # returned by get_data_dictionary: a list of type dicts and info dicts.
    # Though the '_id" field needs to be removed for this to work.
    if fields[0]['id'] == '_id':
        fields = fields[1:]

    # Some validation here to ensure that the fields in fields match up
    # with the existing ones in the datastore would be nice.

    # Note that a subset can be sent, and they will update part of
    # the integrated data dictionary.
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    results = ckan.action.datastore_create(resource_id=resource_id, fields=fields, force=True)
    # The response without force=True is
    # ckanapi.errors.ValidationError: {'__type': 'Validation Error', 'read-only': ['Cannot edit read-only resource. Either pass"force=True" or change url-type to "datastore"']}
    # With force=True, it works.

    return results

##### End of data-dictionary operations #####

def to_dict(input_ordered_dict):
    return loads(dumps(input_ordered_dict))

def value_or_blank(key,d,subfields=[]):
    if key in d:
        if d[key] is None:
            return ''
        elif len(subfields) == 0:
            return d[key]
        else:
            return value_or_blank(subfields[0],d[key],subfields[1:])
    else:
        return ''

def write_or_append_to_csv(filename,list_of_dicts,keys):
    if not os.path.isfile(filename):
        with open(filename, 'wb') as g: # This is still written for Python 2.
            g.write(','.join(keys)+'\n')
    with open(filename, 'ab') as output_file: # This is still written for Python 2.
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        #dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)


def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def unique_values(xs,field):
    return { x[field] if field in x else None for x in to_dict(xs) }

def char_delimit(xs,ch):
    return(ch.join(xs))

def sort_dict(d):
    return sorted(d.items(), key=operator.itemgetter(1))
