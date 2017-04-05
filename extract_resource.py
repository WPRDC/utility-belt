#!/usr/bin/env python
import sys
import requests
import pprint

from collections import OrderedDict, defaultdict
import re
from util import write_to_csv, get_resource

DEFAULT_CKAN_INSTANCE = 'https://data.wprdc.org'

def obtain_resource(id,filename=None):
    # This function pulls information from a particular resource on a
    # CKAN instance and outputs it to a CSV file.

    # Tests suggest that there are some small differences between this
    # file and the one downloaded directly through the CKAN web
    # interface: 1) the row order is different (this version is ordered
    # by _id (logically) while the downloaded version has the same weird
    # order as that shown in the Data Explorer (which can have an _id
    # sequence like 1,2,3,4,5,6,7,45,8,9,10... for no obvious reason)
    # and 2) weird non-ASCII characters are not being handled optimally,
    # so probably some work on character encoding is in order in the
    # Python script.

    if filename is None:
        filename = "{}.csv".format(id)

    list_of_dicts, fields, success = get_resource(DEFAULT_CKAN_INSTANCE,id,chunk_size=1000)

    if not success:
        print("Something went wrong and the resource was not obtained.")
    else:

        #Eliminate _id field
        fields.remove("_id")
        print("The resource has the following fields: {}".format(fields))
        write_to_csv(filename,list_of_dicts,fields)

def main():
    resource_id = sys.argv[1]
    filename = None
    if len(sys.argv) > 2:
        filename = sys.argv[2]
    obtain_resource(resource_id,filename)

############

if __name__ == '__main__':
  main()
