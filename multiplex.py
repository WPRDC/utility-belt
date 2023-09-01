import argparse
import sys, time, ckanapi, re
from icecream import ic
from pprint import pprint
from gadgets import (set_resource_parameters_to_values, set_package_parameters_to_values,
        get_value_from_extras, set_package_extras_parameter_to_value)
from credentials import site, API_key

# Somehow apply a gadget function to multiple entities.
# For instance, we need to update the links on 29 identically named resources (which are just hyperlinks).
# We want to invoke
# set_resource_parameters_to_values,
# iterating over it for different resource_id values that have resources that match a certain search term.
def guess_parameter_type(parameter, value):
    # We need to change types from the default (string) to the correct type (like Boolean).
    if value in ['None', 'null']:
        print("Coercing parameter to a null value.")
        return None
    if parameter in ['relationships_as_object', 'relationships_as_subject', 'groups']: # There are other
        # lists that could be added here, like 'tags' and 'extras'.

        # Note that relationships_as_object and relationships_as_subject should not need to be manually set.
        # The correct way of setting a relationship is through the separate relationships CKAN API endpoints.
        # I noticed that deleting one of those relationships did not result in immediate deletion of the
        # relevant package-level metadata, but within a day they automatically updated, suggesting that there
        # is some infrequent background process that eventually updates the package-level metadata.
        import json
        return json.loads(value) # We need to convert '[]' to [] (a proper empty list)
    if parameter in ['datastore_active', 'private', 'isopen']:
        return bool(value)
    if parameter in ['position']: # We shouldn't be messing with these without at least some more effort: 'num_resources', 'num_tags'
        return int(value)
    return value

def act_on_parameter(entity, entity_type, mode, parameter, parameter_value):
    if mode == 'get':
        if ':' not in parameter:
            return entity[parameter]
        else: # Handle sub-parameters (for "extras")
            params = parameter.split(':')
            if len(params) == 2:
                first = entity[params[0]]
                if params[0] == "extras":
                    return get_value_from_extras(extras=first, key=params[1], default=None)
                else:
                    raise ValueError(f'act_on_parameter is not yet designed to handle parameters like {parameter}')
    elif mode == 'set':
        print(f"(This is where the value of {parameter} should be set to {parameter_value}.)")
        if entity_type == 'resource':
            set_resource_parameters_to_values(site, entity['id'], [parameter], [parameter_value], API_key)
        elif entity_type == 'dataset':
            if ':' in parameter: # Handle sub-parameters (for "extras")
                params = parameter.split(':')
                if len(params) == 2:
                    first = entity.get(params[0], {}) # The assumption here is that
                    # the first-level parameter should be a dictionary (if it's not there
                    # at all. This is true for the "extras" field, but should be reconsidered
                    # for others.
                    assert params[0] == "extras"
                    if params[0] == "extras":
                        package = set_package_extras_parameter_to_value(site, entity['id'], params[1], parameter_value, API_key)
                        new_value = get_value_from_extras(extras = package['extras'], key=params[1], default=None)
                        return new_value
                    else:
                        raise ValueError(f'act_on_parameter is not yet designed to handle parameters like {parameter}')
                else:
                    raise ValueError(f'act_on_parameter is not yet designed to handle {len(params)} parameters like in {parameter}')
        else:
            raise ValueError(f'Unknown entity_type == {entity_type}')

        return parameter_value # Why are we returning this? Does it get changed somewhere?

    elif mode == 'delete':
        if ':' not in parameter or parameter.split(':')[0] != 'extras':
            raise ValueError(f'Not programmed to handle deleting parameters like "{parameter}".')
        params = parameter.split(':')
        key = params[1]
        assert entity_type == 'dataset' # Since resources don't have extras metadata.
        assert len(params) == 2
        assert params[0] == "extras"
        extras_list = entity.get(params[0], {}) # The assumption here is that
        # the first-level parameter should be a dictionary (if it's not there
        # at all. This is true for the "extras" field, but should be reconsidered
        # for others.
        new_extras_list = [d for d in extras_list if d['key'] != key]
        package = set_package_parameters_to_values(site, entity['id'], ['extras'], [new_extras_list], API_key)
    else:
        raise ValueError(f'Unknown mode == {mode}')

def multiplex_with_functional_selection(mode, entity_type, parameter, parameter_value, dataset_filter, resource_filter):
    # Filter by dataset and resource with the passed filter functions.
    # Then based on the mode value, either select the corresponding parameter or set it to parameter_value.
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    try:
        packages = ckan.action.current_package_list_with_resources(limit=999999)
    except:
        time.sleep(0.01)
        packages = ckan.action.current_package_list_with_resources(limit=999999)

    collected = []
    for dataset in packages:
        if entity_type == 'dataset':
        # Operate on the dataset level
            if dataset_filter(dataset):
                if parameter is None:
                    pprint(dataset)
                    after_param = dataset
                else:
                    after_param = act_on_parameter(dataset, entity_type, mode, parameter, parameter_value)
                collected.append({'parameter': after_param, 'dataset': dataset, 'name': dataset['title'], 'id': dataset['id']})

        elif entity_type == 'resource':
        # Find all matching resources
            for resource in dataset['resources']:
                if resource_filter(resource):
                    if parameter is None:
                        pprint(resource)
                        after_param = resource
                    else:
                        after_param = act_on_parameter(resource, entity_type, mode, parameter, parameter_value)
                    collected.append({'parameter': after_param, 'resource': resource, 'name': resource['name'], 'id': resource['id']})
        else:
            assert entity_type in ['dataset', 'resource']


    for c in sorted(collected, key=lambda d: d['name']):
        print(f"{c['name']} ({c['id']}){'' if parameter is None else '[' + parameter + ']'}: {c['parameter']}")

    print(f"{'Set' if mode == 'set' else 'Got'} parameters for {len(collected)} {entity_type}{'s' if len(collected) != 1 else ''}.")
    return collected

def is_uuid(s):
    if s is None:
        return False
    import re
    return re.search('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', s) is not None

def construct_function(pattern, entity_type):
    if is_uuid(pattern):
        return lambda x: True if x['id'] == pattern else None
    elif pattern in ['all', None]:
        return lambda x: True
    else:
        import re
        # This needs to be generalized so it can handle cases where either the title or the name is given for datasets.


        return lambda x: True if re.search(pattern, x['title'] if entity_type == 'dataset' else x['name']) is not None else None
    # In principle, we might want to select on other metadata values.

def multi(mode, parameter, parameter_value, dataset_selector, resource_selector, tag_selector):
    if resource_selector is None: # It's a dataset metadata field.
        assert (parameter in [ 'id', 'title', 'name', 'geographic_unit', 'owner_org', 'maintainer',
            'tags', 'relationships_as_object', 'access_level_comment',
            'frequency_publishing', 'maintainer_email', 'num_tags',
            'metadata_created', 'group', 'metadata_modified', 'author',
            'author_email', 'state', 'version', 'department', 'license_id',
            'type', 'resources', 'num_resources', 'data_steward_name', 'data_steward_email',
            'frequency_data_change', 'private', 'groups',
            'creator_user_id', 'relationships_as_subject', 'data_notes',
            'isopen', 'url', 'notes', 'license_title',
            'temporal_coverage', 'related_documents', 'license_url',
            'organization', 'revision_id', 'extras', None]) or re.match('extras:', parameter)
    else:
        assert parameter in ['id', 'cache_last_updated', 'package_id', 'webstore_last_updated',
            'datastore_active', 'size', 'state', 'hash',
            'description', 'format', 'last_modified', 'url_type',
            'mimetype', 'cache_url', 'name', 'created', 'url',
            'webstore_url', 'mimetype_inner', 'position',
            'revision_id', 'resource_type', None]

    # [ ] Which fields have non-string values (and would need to be cast)?

    dataset_filter = construct_function(dataset_selector, 'dataset')
    resource_filter = construct_function(resource_selector, 'resource')

    if tag_selector == 'all':
        tag_selector = None
    if resource_selector is None and tag_selector is None:
        entity_type = 'dataset'
    elif resource_selector is None and tag_selector is not None:
        # Make a dataset_filter based on the tag.
        dataset_filter = lambda x: tag_selector in [t['name'] for t in x['tags']]
        entity_type = 'dataset'
    elif resource_selector is not None and tag_selector is None:
        entity_type = 'resource'

    if parameter is not None:
        parameter_value = guess_parameter_type(parameter, parameter_value)

    multiplex_with_functional_selection(mode, entity_type, parameter, parameter_value, dataset_filter, resource_filter)

    # Some package parameters you can fetch from the WPRDC with
    # this function are:

# A full command-line specification would be like
# > multiplex.py change resources "DASH Data Guide" url <new url>
# > multichange.py resources "DASH Data Guide" url <new url>

# It would also be nice to be able to change all the resources (or resources matching a regex) in one dataset.

# > multiplex.py set dataset all resource "DASH Data Guide" url <new url>

# > multiplex.py set dataset <package_id> resource all url <new url>

# > multiplex.py set dataset (all|regex|package_id) resource (all|regex|resource_id) <parameter> <parameter value>


# > multiplex.py (set|get) <parameter> <parameter value> --dataset (all|regex|package_id) --resource (all|regex|resource_id)
parser = argparse.ArgumentParser(description='Select dataset packages/resources to set/get parameters on')
parser.add_argument('mode', default='get', choices=['set', 'get', 'delete'], help='Either "set" or "get" (or "delete" for extras keys).')
parser.add_argument('--parameter', dest='parameter', default=None, required=False, help='The parameter of interest (resource-level if the --resource parameter is given, else dataset-level)')
parser.add_argument('--value', dest='parameter_value', required=False, help='The parameter value to set the parameter to (resource-level if the --resource parameter is given, else dataset-level)')
parser.add_argument('--dataset', dest='dataset_selector', default=None, required=False, help='(all|<search term to match>|<package ID or name>)')
parser.add_argument('--resource', dest='resource_selector', default=None, required=False, help='(all|<search term to match>|<resource ID>)')
parser.add_argument('--tag', dest='tag_selector', default=None, required=False, help='(all|<search term to match>)') # We could add support for tag IDs.

args = parser.parse_args()
multi(args.mode, args.parameter, args.parameter_value, args.dataset_selector, args.resource_selector, args.tag_selector)
