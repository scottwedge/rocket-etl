import csv, json, requests, sys, traceback
import time
import re
import datetime

from dateutil import parser

from marshmallow import fields, pre_load, pre_dump
from engine.wprdc_etl import pipeline as pl
from engine.parameters.local_parameters import SETTINGS_FILE, SOURCE_DIR
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.etl_util import find_resource_id, post_process

from icecream import ic

OVERRIDE_GEOCODING = True

def correct_address(address_str):
    translations = {}
    # Street-name corrections
    translations['ROBINSON CENTER'] = 'ROBINSON CENTRE'
    translations['Robinson Center'] = 'Robinson Centre'
    translations['Davision'] = 'Division'
    translations['Third Ave'] = '3rd Ave'

    # City-name corrections:
    translations['Mc Keesport'] = 'McKeesport' # While Mc Keesport is apparently the standard version, the property assessments file has 55 instances of McKeesport.

    proposed_corrections = []
    maximally_translated = str(address_str)
    for before,after in translations.items():
        if before in address_str:
            proposed_corrections.append(re.sub(before,after,address_str))
            maximally_translated = re.sub(before,after,maximally_translated)

    ic(proposed_corrections)
    ic(maximally_translated)

    if maximally_translated != address_str and maximally_translated not in proposed_corrections:
        proposed_corrections = [maximally_translated] + proposed_corrections

    return list(set(proposed_corrections))

def geocode_address_string(address):
    url = "https://tools.wprdc.org/geo/geocode?addr={}".format(address)
    r = requests.get(url)
    result = r.json()
    time.sleep(0.1)
    if result['data']['status'] == "OK":
        longitude, latitude = result['data']['geom']['coordinates']
        return longitude, latitude
    print("Unable to geocode {}, failing with status code {}.".format(address,result['data']['status']))
    return None, None

#def geocode_address_by_parts():
#     number = request.GET['number']
#    directional = request.GET.get('directional', None)
#    street_name = request.GET['street_name']
#    street_type = request.GET['street_type']
#    city = request.GET['city']
#    state = request.GET.get('state', None)
#    zip_code = request.GET.get('zip_code', None)

class RestaurantsSchema(pl.BaseSchema):
    id = fields.String()
    #storeid = fields.String(load_from='StoreID', dump_to='store_id')
    facility_name = fields.String()
    num = fields.String(allow_none=True) # Non-integer values include "8011-B".
    street = fields.String()
    city = fields.String()
    state = fields.String()
    zip = fields.String()
    municipal = fields.String()
    category_cd = fields.String()
    description = fields.String()
    p_code = fields.String()
    fdo = fields.Date()
    bus_st_date = fields.Date(allow_none=True)
    noseat = fields.Integer() #dump_to="seat_count")
    noroom = fields.Integer(allow_none=True)
    sqfeet = fields.Integer() #dump_to="sq_feet")
    status = fields.String()
    placard_st = fields.String()
    x = fields.Float(allow_none=True)
    y = fields.Float(allow_none=True)
    address = fields.String()

    class Meta:
        ordered = True

#    @pre_load
#    def geocode(self,data):
#        if 'num' in data and 'street' in data and 'city' in data:
#            num = data['num']
#            street = data['street']
#            city = data['city']
#            state = None
#            zip_code = None
#            directional = None
#            if 'state' in data:
#                state = data['state']
#            if 'zip' in data:
#                zip_code = data['zip']
#            longitude, latitude = geocode_address_by_parts(num, directional, street, city, state, zip_code)
#
#            if longitude is None:
#                streets = correct_address(street)
#                if len(streets) > 0:
#                    longitude, latitude = geocode_address_by_parts(num, directional, street, city, state, zip_code)
#            data['x'] = longitude
#            data['y'] = latitude

    @pre_load
    def geocode(self,data):
        if 'address' in data and not OVERRIDE_GEOCODING:
            address_string = data['address']
            longitude, latitude = geocode_address_string(address_string)
            if longitude is None:
                corrected_addresses = correct_address(address_string)
                if len(corrected_addresses) > 0:
                    # For now just try the first of the proposed corrections:
                    longitude, latitude = geocode_address_string(corrected_addresses[0])
            data['x'] = longitude
            data['y'] = latitude

    @pre_load
    def convert_dates(self,data):
        date_fields = ['fdo', 'bus_st_date']
        for field in date_fields:
            if data[field] is not None:
                data[field] = parser.parse(data[field]).date().isoformat()


restaurants_package_id = "8744b4f6-5525-49be-9054-401a2c4c2fac" # Restaurants package, production

job_dicts = [
    {
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': 'locations-for-geocode.csv',
        'encoding': 'latin-1',
        'connector_config_string': 'sftp.county_sftp',
        'schema': RestaurantsSchema,
        'primary_key_fields': ['id'],
        'upload_method': 'upsert',
        'destinations': ['ckan'],
        'package': restaurants_package_id,
        'resource_name': 'Geocoded Food Facilities',
    },
]

def process_job(**kwparameters):
    job = kwparameters['job']
    use_local_files = kwparameters['use_local_files']
    clear_first = kwparameters['clear_first']
    test_mode = kwparameters['test_mode']
    job.default_setup(use_local_files)
    # [ ] Check whether this process_job function can be put into standard form.
    job.loader_config_string = 'production'
    if OVERRIDE_GEOCODING: # This part may not convert well to the Job class approach.
        job.target = '/Users/drw/WPRDC/etl/rocket-etl/archives/previously-geocoded-restaurants.csv'
        job.source_connector = pl.FileConnector
        job.source_type = 'local'
        job.connector_config_string = ''
        print("Using local archive file: {}".format(target))
    elif use_local_files:
        job.target = SOURCE_DIR + job.source_file
    else:
        job.target = job.source_dir + "/" + job.source_file

    package_id = job.package if not test_mode else TEST_PACKAGE_ID
    print("==============\n {} in package {}".format(job.resource_name,package_id))

    if clear_first:
        print("Clearing the datastore for {}".format(job.resource_name))
    # Upload data to datastore
    print('Uploading tabular data...')
    curr_pipeline = pl.Pipeline(job.resource_name + ' pipeline', job.resource_name + ' Pipeline', log_status=False, chunk_size=1000, settings_file=SETTINGS_FILE) \
        .connect(job.source_connector, job.target, config_string=job.connector_config_string, encoding=job.encoding) \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(job.schema) \
        .load(pl.CKANDatastoreLoader, job.loader_config_string,
              #fields=schema().serialize_to_ckan_fields(),
              fields=job.schema().serialize_to_ckan_fields(capitalize=False),
              key_fields=job.primary_key_fields,
              package_id=package_id,
              resource_name=job.resource_name,
              clear_first=clear_first,
              method=job.upload_method).run()

    resource_id = find_resource_id(package_id, resource_name)
    locators_by_destination = {destinations[0]: resource_id}
    assert len(destinations) == 1
    return locators_by_destination

def main(use_local_files=False,clear_first=False,test_mode=False):
    for job in jobs:
        locators_by_destination = process_job(job,use_local_files,clear_first,test_mode)
        for destination, resource_id in locators_by_destination.items():
            if destination == 'ckan':
                post_process(resource_id)

if __name__ == '__main__':
    mute_alerts = False
    use_local_files = False
    clear_first = False
    test_mode = False
    try:
        if len(sys.argv) > 1:
            if 'mute' in sys.argv[1:]:
                mute_alerts = True
            if 'local' in sys.argv[1:]:
                use_local_files = True
            if 'test' in sys.argv[1:]:
                test_mode = True
            if 'clear_first' in sys.argv[1:]:
                clear_first = True
        main(use_local_files,clear_first,test_mode)
    except:
        e = sys.exc_info()[0]
        msg = "Error: {} : \n".format(e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        msg = ''.join('!! ' + line for line in lines)
        print(msg) # Log it or whatever here
        if not mute_alerts:
            send_to_slack(msg,username='food-facilities-geocoded-ac ETL assistant',channel='@david',icon=':illuminati:')
