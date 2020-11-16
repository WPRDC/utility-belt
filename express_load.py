from credentials import site, ckan_api_key as API_key
from pprint import pprint
import ckanapi

def get_package_parameter(site, package_id, parameter=None, API_key=None):
    """Get a CKAN package parameter. If no parameter is specified, all metadata
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

def find_resource_id(site, package_id, resource_name, API_key=None):
    """Get the resource ID given the package ID and resource name."""
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

def query_resource(site,query,API_key=None):
    """Use the datastore_search_sql API endpoint to query a CKAN resource."""
    success = False
    try:
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
        success = True
    except:
        return None, False
    return data, success

def get_resource_data(site,resource_id,API_key=None,count=50):
    # Use the datastore_search API endpoint to get <count> records from 
    # a CKAN resource
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        response = ckan.action.datastore_search(id=resource_id, limit=count)
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
        success = True
    except:
        return None, False
    return data, success


# How to use the Express Loader through the API:
    # "xloader looks at the resource.url, downloads it, parses it and then loads the rows into the datastore database. For the last step xloader makes use of datastore_create to create the db table, and then directly talks to the db to load the rows." - David Read, creator of the extension

    #mysite.action.resource_create(
    #    package_id='my-dataset-with-files',
    #    url='dummy-value',  # ignored but required by CKAN<2.6
    #    upload=open('/path/to/file/to/upload.csv', 'rb'))

    #https://github.com/ckan/ckanext-xloader/issues/66#issuecomment-4679912943
name = "Allegheny County COVID-19 Tests and Cases"
TEST_PACKAGE_ID = "812527ad-befc-4214-a4d3-e621d8230563" # Test package on data.wprdc.org
destination_package_id = TEST_PACKAGE_ID
resource_id = find_resource_id(site, destination_package_id, name, API_key)
csv_file_path = '/Users/drw/WPRDC/etl/rocket-etl/output_files/ac_hd/covid_19_tests_1000.csv'

ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
#metadata = get_metadata(site, resource_id, API_key)
#resource_as_dict = ckan.action.resource_create(package_id=destination_package_id, url='#', format=r_format, name=name)
print(f"Uploading CSV file {csv_file_path} to resource with name '{name}' in package with ID {destination_package_id}.")
# If the resource doesn't exist already, use resource_create, otherwise use resource_update (or resource_patch).
resource_as_dict = ckan.action.resource_patch(id = resource_id,
    upload=open(csv_file_path, 'r'))
# Running this once sets the file to the correct file and triggers some datastore action and
# the Express Loader, but for some reason, it seems to be processing the old file.

# So let's run it twice.

resource_as_dict = ckan.action.resource_update(id = resource_id,
    upload=open(csv_file_path, 'r'))

pprint(resource_as_dict)
