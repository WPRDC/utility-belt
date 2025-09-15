import sys, time, ckanapi
from credentials import site, API_key # Use sample-credentials.py as a model for creating this file.
from pprint import pprint
from icecream import ic

def get_resource_parameter(site, resource_id, parameter=None, API_key=None):
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

def get_package_parameter(site, package_id, parameter=None, API_key=None):
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

def get_resource_name(site, resource_id, API_key=None):
    return get_resource_parameter(site, resource_id, 'name', API_key)

def get_package_name_from_resource_id(site, resource_id, API_key=None):
    p_id = get_resource_parameter(site, resource_id, 'package_id', API_key)
    return get_package_parameter(site, p_id, 'title', API_key)

def strip_fields(record, fields_to_strip):
    new_record = {}
    for field, value in record.items():
        if field not in fields_to_strip:
            new_record[field] = value
    return new_record
        
def strip_fields_all_rows(records, fields_to_strip):
    return [strip_fields(x, fields_to_strip) for x in records]

def compare_resources(site, resource_id_1, resource_id_2, fields_to_ignore, API_key):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    from gadgets import get_number_of_rows, get_resource_data, get_all_records
    row_counts = [get_number_of_rows(site, r_id, API_key=API_key) for r_id in [resource_id_1, resource_id_2]]
    if row_counts[0] != row_counts[1]:
        print(f"The resources differ in number of rows ({row_counts[0]} vs. {row_counts[1]}).")
        return False
    
    import json
    lines = [json.dumps(strip_fields(get_resource_data(site, r_id, API_key, count=1)[0], fields_to_ignore)) for r_id in [resource_id_1, resource_id_2]]
    if lines[0] != lines[1]:
        print(f"The resources differ in the first row ({lines[0]} vs. {lines[1]}).")
        return False

    lines = [json.dumps(strip_fields_all_rows(get_all_records(site, r_id, API_key), fields_to_ignore)) for r_id in [resource_id_1, resource_id_2]]
    if lines[0] != lines[1]:
        print(f"The resources differ somewhere in the data.")
        return False
    else:
        return True

    # Are they big? If so, maybe just compare the first records. Or the lengths. Or the hashes (more computationally intensive).
    # Could also compare the schemas, but the form that CKAN returns them in are objects that are not immediately comparable.

    get_all_records(site,resource_id,API_key=None,chunk_size=5000)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please specify the resource IDs for the two resources to be compared and (optionally) any fields to ignore in the comparison (like added timestamps).")
    source_resource_id, destination_resource_id = sys.argv[1], sys.argv[2]

    if len(sys.argv) > 3:
        fields_to_ignore = sys.argv[3:]
    else:
        fields_to_ignore = []

    source_name = get_resource_name(site, source_resource_id, API_key)
    source_package_name = get_package_name_from_resource_id(site, source_resource_id, API_key)
    destination_name = get_resource_name(site, destination_resource_id, API_key)
    destination_package_name = get_package_name_from_resource_id(site, destination_resource_id, API_key)

    print(f'Comparing {source_name} ({source_package_name}) to {destination_name} ({destination_package_name}), but ignoring {fields_to_ignore}...')

    match = compare_resources(site, source_resource_id, destination_resource_id, fields_to_ignore, API_key)
    if match:
        print("The resources match.")
    else:
        print("The resources do not match.")
