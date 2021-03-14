import ckanapi, os, sys, csv, json, requests, time, re
from credentials import site, API_key, debug_package_id as DEFAULT_TESTBED_ID
from gadgets import get_package_parameter, get_resource_parameter, set_resource_parameters_to_values
from icecream import ic
from pprint import pprint

from collections import defaultdict

BASE_URL = 'https://data.wprdc.org/api/3/action/'

def review_package_resources(package_id=DEFAULT_TESTBED_ID):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key) # Without specifying the apikey field value,
    # the next line will only return non-private packages.

    # This is a list of all the packages with all the resources nested inside and all the current information.
    metadata = get_package_parameter(site, package_id, parameter=None, API_key=API_key)

    resources = metadata['resources']
    public_or_private = 'private' if metadata['private'] else 'public'
    ic(resources)

    deleted = 0
    all_resources = 0
    tabular_resources = 0
    for r in resources[::-1]:
        #{'cache_last_updated': None,
        # 'cache_url': None,
        # 'created': '2020-12-18T19:30:34.889331',
        # 'datastore_active': True,
        # 'description': '',
        # 'format': 'CSV',
        # 'hash': '',
        # 'id': '0fcacebc-2a02-4faa-9f6f-af72311095d8',
        # 'last_modified': '2020-12-18T14:31:03.292333',
        # 'mimetype': None,
        # 'mimetype_inner': None,
        # 'name': 'Stores',
        # 'package_id': '812527ad-befc-4214-a4d3-e621d8230563',
        # 'position': 100,
        # 'resource_type': None,
        # 'revision_id': 'ad165a8f-f177-48fb-9e27-38fddc4eb334',
        # 'size': None,
        # 'state': 'active',
        # 'url': 'https://data.wprdc.org/datastore/dump/0fcacebc-2a02-4faa-9f6f-af72311095d8',
        # 'url_type': 'datapusher'
        all_resources += 1
        print(f"{r['name']}: format = {r['format']}, resource_type = {r['resource_type']}, url_type = {r['url_type']}")
        resource_id = r['id']
        resource_format = r['format']
        if r['format'].lower() in ['csv', '.csv', 'xlsx', 'xls']:
            tabular_resources += 1
            if not r['datastore_active']:
                print(f"   A datastore would be expected for {r['name']} ({resource_id}) but none can be found! [url_type = {r['url_type']}")
        answer = ' '
        while answer.lower() not in ['y', 'n', '']:
            answer = input(f"      Delete {r['name']} ({r['id']}) from {metadata['title']} ({public_or_private})? (y/n) ")
        if answer.lower() == 'y':
            try:
                results_dict = delete_resource(site, r['id'], API_key)
                ic(results_dict)
                deleted += 1
            except ckanapi.errors.CKANAPIError:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                endpoint, status_code, html_response = exc_value.extra_msg[1:-1].split(', ')
                print(f"      [CKAN returned another {status_code} error.]")



    print(f"Deleted {deleted} resources out of {all_resources} resources.")

def set_mime_type(site, resource_id, new_mime_type, API_key):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key) # Without specifying the apikey field value,
    # the next line will only return non-private packages.

    # This is a list of all the packages with all the resources nested inside and all the current information.
    #metadata = get_package_parameter(site, package_id, parameter=None, API_key=API_key)

    #resources = metadata['resources']
    try:
        set_resource_parameters_to_values(site, resource_id, ['mimetype'], [new_mime_type], API_key)
    except ckanapi.errors.CKANAPIError:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        if hasattr(exc_value, 'extra_msg'):
            endpoint, status_code, html_response = exc_value.extra_msg[1:-1].split(', ')
            print(f"      [CKAN returned another {status_code} error.]")
        else:
            print(f"      [CKAN error of type {exc_type} with value {exc_value}]")

def autocorrect_mime_type(r, existing_format, correct_mimetype_by_format, changes, site, API_key):
    new_mimetype = correct_mimetype_by_format[existing_format]
    print(f"{r['name']} with format {existing_format} will be changed to have MIME type {new_mimetype}.")
    changes += 1
    set_mime_type(site, r['id'], new_mimetype, API_key)
    time.sleep(5)
    return changes

#review_package_resources()
#resource_id = "308aa63f-2d7f-4f20-b379-92af0ac73052"
#new_mime_type = "text/html" html
#resource_id = "8eff881d-4d28-4064-83f1-30cc991cfec7"
#new_mime_type = "text/csv" CSV
#resource_id = "1949e396-83f2-4d36-845b-97826c39dc9f"
#new_mime_type = "image/png" PNG
#resource_id = "b4cfc9b5-259a-4f9a-9975-5fae16c8e3b1"
#new_mime_type = "text/xml" #XML
#resource_id = "df05da91-d15d-45b4-b137-2c4fe31a3e73"
#new_mime_type = "image/gif" # GIF
#set_mime_type(site, resource_id, new_mime_type, API_key)
#resource_id = "df05da91-d15d-45b4-b137-2c4fe31a3e73"
#new_mime_type = "image/gif" # GIF
#resource_id = "9b5b9e7d-75b8-40e1-a24b-5ad9df0e45c7"
#new_mime_type = "text/csv"
#resource_id = "48ca3dce-8d4b-43ed-8f7f-a4dbfdaf42af"
#new_mime_type = "application/vnd.google-apps.map" # Google My Maps
#resource_id = "515114ad-624a-4a08-951f-56a3ea68f384"
#new_mime_type = "text/html" #"application/http" # Possibly a reasonable MIME type for web apps
#resource_id = "43325c96-d709-45d6-8bbd-7a7b5b62ba95"
#new_mime_type = "application/zip"
#resource_id = "313b4a63-d292-4f45-939c-26b328ac6a74"
#new_mime_type = "application/octet-stream"
#resource_id = "cb957b16-9945-4730-9ed9-325e230946d3"
#resource_id = "8231e3f9-050c-4805-8ded-4ba2b442ac5c"
#new_mime_type = "text/html"
#set_mime_type(site, resource_id, new_mime_type, API_key)

ckan = ckanapi.RemoteCKAN(site) # Without specifying the apikey field value,
# the next line will only return non-private packages.
try:
    packages = ckan.action.current_package_list_with_resources(limit=999999)
except:
    time.sleep(5)
    packages = ckan.action.current_package_list_with_resources(limit=999999)

mimetypes_by_format = defaultdict(list)
none_count = 0
resource_count = 0
for package in packages:
    resources = package['resources']
    resource_count += len(resources)
    for r in resources:
        existing_mimetype = r.get('mimetype')
        if existing_mimetype is None:
            none_count += 1
        existing_mimetype_inner = r.get('mimetype_inner')
        if existing_mimetype_inner is not None:
            print(f"Found mimetype_inner = {existing_mimetype_inner} for resource_id = {r['id']} and mimetype = {existing_mimetype}")
        existing_format = r.get('format')
        if existing_mimetype not in mimetypes_by_format[existing_format]:
            mimetypes_by_format[existing_format].append(existing_mimetype)

print("The current situation (before setting mimetypes).")
pprint(mimetypes_by_format)
print(f"Found {resource_count} resources. {none_count}/{resource_count} have no MIME type defined.")

correct_mimetype_by_format = {'CSV': 'text/csv',
        'HTML': 'text/html',
        'ZIP': 'application/zip',
        'GeoJSON': 'application/zip',
        'XLSX': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'XLS': 'application/vnd.ms-excel',
        'Esri REST': 'text/html',
        'JSON': 'application/json',
        'KML': 'application/vnd.google-earth.kml+xml',
        'PDF': 'application/pdf',
        'JPEG': 'image/jpeg',
        'DOCX': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'TXT': 'text/plain',
        'API': 'application/json',
        'OGC WMS': 'application/vnd.ogc.wms_xml',
        'OGC WFS': 'application/vnd.ogc.wfs_xml',
        'RTF': 'application/rtf',
        }

expected_extension_by_format = {'CSV': 'csv',
        'HTML': 'html',
        'ZIP': 'zip',
        'GeoJSON': 'geojson',
        'XLSX': 'xlsx',
        'Esri REST': None,
        'XLS': 'xls',
        'JSON': 'json',
        'KML': 'kml',
        'PDF': 'pdf',
        'JPEG': 'jpg',
        'DOCX': 'docx',
        'TXT': 'txt',
        'API': None,
        'OGC WMS': None,
        'OGC WFS': None,
        'RTF': 'rtf',
        }

assert len(correct_mimetype_by_format) == len(expected_extension_by_format)
new_none_count = 0
changes = 0
for package in packages:
    resources = package['resources']
    for r in resources:
        existing_mimetype = r.get('mimetype')
        existing_format = r.get('format')
        url = r.get('url')
        if re.match('https://docs.google.com/', url) is not None:
            new_mimetype = 'application/vnd.google-apps.document'
            if existing_mimetype != new_mimetype:
                print(f"{r['name']} is a Google Doc, so its MIME type will be changed to application/vnd.google-apps.document.")
                set_mime_type(site, r['id'], new_mimetype, API_key)
                changes += 1
        elif existing_mimetype is None and existing_format in correct_mimetype_by_format:
            extension = url.split('/')[-1].split('.')[-1]
            if len(extension) > 3:
                extension = extension.split('?')[0]
            if expected_extension_by_format[existing_format] is not None and (extension == expected_extension_by_format[existing_format] or r['datastore_active']):
                # Previously the block below was not originally propertly logic-gated, so some resources had their MIME types
                # set even though their extension was not the expected one.
                changes = autocorrect_mime_type(r, existing_format, correct_mimetype_by_format, changes, site, API_key)
            else:
            #if expected_extension_by_format[existing_format] is None or extension != expected_extension_by_format[existing_format]:
                if expected_extension_by_format[existing_format] is None and existing_format == 'Esri REST':
                    changes = autocorrect_mime_type(r, existing_format, correct_mimetype_by_format, changes, site, API_key)
                    pass
                if existing_format == 'HTML':
                    if r['name'] == 'ArcGIS Hub Dataset':
                        changes = autocorrect_mime_type(r, existing_format, correct_mimetype_by_format, changes, site, API_key)
                    pass
                elif existing_format == 'CSV' and len(extension) == len('cb0a4d8b-2893-4d20-ad1c-47d5fdb7e8d5'):
                    pass
                elif existing_format == 'CSV' and extension == '#':
                    pass
                else:
                    print(f"{r['name']} with format {existing_format} is not expected to have extension {extension}.")

        #if existing_mimetype is None:
        #    new_none_count += 1
        #existing_mimetype_inner = r.get('mimetype_inner')
        #if existing_mimetype_inner is not None:
        #    print(f"Found mimetype_inner = {existing_mimetype_inner} for resource_id = {r['id']} and mimetype = {existing_mimetype}")
        #if existing_mimetype not in mimetypes_by_format[existing_format]:
        #    mimetypes_by_format[existing_format].append(existing_mimetype)

pprint(mimetypes_by_format)
print(f"Changed {changes} resource MIME types.")
print(f"Found {resource_count} resources. {none_count}/{resource_count} have no MIME type defined. After changes, {none_count - changes}/{resource_count} have no MIME type.")
#[ ] Find API resources and change their types to "API". Maybe for Esri REST API as well?
# application/json for an API that is actually the API endpoint (if it returns JSON)
# but maybe text/html for a web site that documents that API.
