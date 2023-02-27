import argparse
import sys, time, ckanapi
from icecream import ic


# Somehow apply a gadget function to multiple entities. 
# For instance, we need to update the links on 29 identically named resources (which are just hyperlinks).
# We want to invoke
# set_resource_parameters_to_values,
# iterating over it for different resource_id values that have resources that match a certain search term.

def act_on_parameter(entity, mode, parameter, parameter_value):
    if mode == 'get':
        return entity[parameter]
    else:
        print(f"(This is where the value of {parameter} should be set to {parameter_value}.")


def multiplex_with_functional_selection(mode, parameter, parameter_value, dataset_filter, resource_filter):
    # Filter by dataset and resource with the passed filter functions.
    # Then based on the mode value, either select the corresponding parameter or set it to parameter_value.
    pass

def is_uuid(s):
    if s is None:
        return False
    import re
    return re.search('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', s) is not None

def construct_function(pattern):
    if is_uuid(pattern):
        return lambda x: True if x['id'] == pattern else None
    elif pattern == 'all':
        return lambda x: True
    else:
        import re
        return lambda x: True if re.search(pattern, x['title']) is not None else None
    # In principle, we might want to select on other metadata values.
        
def multi(mode, parameter, parameter_value, dataset_selector, resource_selector):
    if resource_selector is None: # It's a dataset metadata field.
        assert parameter in [ 'id', 'title', 'name', 'geographic_unit', 'owner_org', 'maintainer',
            'data_steward_email', 'relationships_as_object', 'access_level_comment',
            'frequency_publishing', 'maintainer_email', 'num_tags',
            'metadata_created', 'group', 'metadata_modified', 'author',
            'author_email', 'state', 'version', 'department', 'license_id',
            'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
            'frequency_data_change', 'private', 'groups',
            'creator_user_id', 'relationships_as_subject', 'data_notes',
            'isopen', 'url', 'notes', 'license_title',
            'temporal_coverage', 'related_documents', 'license_url',
            'organization', 'revision_id']
    else:
        assert parameter in ['id', 'cache_last_updated', 'package_id', 'webstore_last_updated',
            'datastore_active', 'size', 'state', 'hash',
            'description', 'format', 'last_modified', 'url_type',
            'mimetype', 'cache_url', 'name', 'created', 'url',
            'webstore_url', 'mimetype_inner', 'position',
            'revision_id', 'resource_type']

    # [ ] Which fields have non-string values (and would need to be cast)?

    dataset_filter = construct_function(dataset_selector)
    resource_filter = construct_function(resource_selector)

    site = "https://data.wprdc.org"
    ckan = ckanapi.RemoteCKAN(site) # Without specifying the apikey field value,
# the next line will only return non-private packages.
    try:
        packages = ckan.action.current_package_list_with_resources(limit=999999)
    except:
        time.sleep(0.01)
        packages = ckan.action.current_package_list_with_resources(limit=999999)

    for dataset in packages:
        if resource_selector is None:
        # Operate on the dataset level
            if dataset_filter(dataset):
                act_on_parameter(dataset, mode, parameter, parameter_value)
        else:
        # Find all matching resources
            for resource in dataset['resources']:
                if resource_filter(resource):
                    act_on_parameter(dataset, mode, parameter, parameter_value)
                    print(resource['name'])
                    

    # Some package parameters you can fetch from the WPRDC with
    # this function are:


#for resource in resources:
#    ic(resource['url'])
    #resource['url'] = 

# A full command-line specification would be like
# > multiplex.py change resources "DASH Data Guide" url <new url>
# > multichange.py resources "DASH Data Guide" url <new url>

# It would also be nice to be able to change all the resources (or resources matching a regex) in one dataset.

# > multiplex.py set dataset all resource "DASH Data Guide" url <new url>

# > multiplex.py set dataset <package_id> resource all url <new url>

# > multiplex.py set dataset (all|regex|package_id) resource (all|regex|resource_id) <parameter> <parameter value>


# > multiplex.py (set|get) <parameter> <parameter value> --dataset (all|regex|package_id) --resource (all|regex|resource_id)
parser = argparse.ArgumentParser(description='Select dataset packages/resources to set/get parameters on')
parser.add_argument('mode', default='get', choices=['set', 'get'], help='Either "set" or "get"')
parser.add_argument('--parameter', dest='parameter', default='title', required=False, help='The parameter of interest (resource-level if the --resource parameter is given, else dataset-level)')
parser.add_argument('--value', dest='parameter_value', required=False, help='The parameter value to set the parameter to (resource-level if the --resource parameter is given, else dataset-level)')
parser.add_argument('--dataset', dest='dataset_selector', default=None, required=False, help='(all|<search term to match>|<package ID>)')
parser.add_argument('--resource', dest='resource_selector', default=None, required=False, help='(all|<search term to match>|<resource ID>)')

args = parser.parse_args()
multi(args.mode, args.parameter, args.parameter_value, args.dataset_selector, args.resource_selector)
