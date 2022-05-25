import sys
from credentials import site, API_key
from gadgets import set_package_parameters_to_values

if len(sys.argv) != 2:
    print("USAGE: > python undelete_dataset.py <package ID>")
else:
    package_id = sys.argv[1]
    print(package_id)
    parameters = ['state']
    new_values = ['active']
    set_package_parameters_to_values(site, package_id, parameters, new_values, API_key)
