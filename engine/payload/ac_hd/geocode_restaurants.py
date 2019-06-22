import csv, json, requests, sys, traceback
import time
import re
from dateutil import parser

from marshmallow import fields, pre_load, pre_dump
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import post_process, default_job_setup, push_to_datastore
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

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
#                zip_code = data['zip'] # This line has been corrected since the last unsuccessful attempt.
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
        if 'address' in data:
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

jobs = [
    {
        'package': restaurants_package_id,
        'source_dir': 'Health Department',
        'source_file': 'locations-for-geocode.csv',
        'resource_name': 'Geocoded Food Facilities',
        'schema': RestaurantsSchema
    },
]

def process_job(job,use_local_files,clear_first,test_mode):
    target, local_directory, destination = default_job_setup(job)

    ## BEGIN CUSTOMIZABLE SECTION ##
    file_connector = pl.FileConnector
    config_string = ''
    encoding = 'latin-1'
    if not use_local_files:
        file_connector = pl.SFTPConnector
        config_string = 'sftp.county_sftp' # This is just used to look up parameters in the settings.json file.
    primary_key_fields = ['id']
    upload_method = 'upsert'
    ## END CUSTOMIZABLE SECTION ##

    resource_id = push_to_datastore(job, file_connector, target, config_string, encoding, destination, primary_key_fields, test_mode, clear_first, upload_method)
    return [resource_id] # Return a complete list of resource IDs affected by this call to process_job.
