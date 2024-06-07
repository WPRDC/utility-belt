# This script lets the user specify a CKAN tag and (optionally) a CKAN group,
# and it will then find all the datasets with that tag and add them to the associated group 
# (defaulting to a group with the same name as the tag).
# So
# > python group_by_tag.py health
# will find all datasets with the tag "health" and try to add them to the "health" group.
# > python group_by_tag.py transit transportation
# will find all datasets with the tag "transit" and try to add them to the "transportation" group.

import argparse
import sys, time, ckanapi, re
from icecream import ic
from pprint import pprint
from gadgets import (set_resource_parameters_to_values, set_package_parameters_to_values,
        get_value_from_extras, set_package_extras_parameter_to_value,
        get_package_parameter, get_group_list,
        clear_package_groups, assign_package_to_group)
from credentials import site, API_key

ckan = ckanapi.RemoteCKAN(site, apikey=API_key)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        tag_name = group_name = sys.argv[1]
        group_names, groups = get_group_list(site)
        assert group_name in group_names

        MAX_ROWS = 1000
        response = ckan.action.package_search(fq=f'tags:{tag_name}', rows=MAX_ROWS)
        packages = response['results']
        if len(packages) == MAX_ROWS:
            raise ValueError("package_search results are being limited to {MAX_ROWS}. Check for more!")
        print(f'Found {len(packages)} datasets with tag {tag_name}')
        for package in packages:
            print(f"{'[private] ' if package['private'] else ''}{package['title']:<50.50} {[g['name'] for g in package['groups']]}")
        do_it = input(f"Assign these {len(packages)} datasets to the {group_name} group? (y/n) ")
        if do_it.lower() == 'y':
            for package in packages:
                assign_package_to_group(site, package, package['id'], group_name, API_key)
        print('Done.')

