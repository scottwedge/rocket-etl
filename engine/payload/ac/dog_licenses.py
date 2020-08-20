import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa


class DogLicensesSchema(pl.BaseSchema):
    license_type = fields.String(load_from="licensetype", dump_to='LicenseType')
    breed = fields.String(allow_none=True, dump_to='Breed')
    color = fields.String(allow_none=True, dump_to='Color')
    dog_name = fields.String(load_from='dogname', dump_to='DogName')
    owner_zip = fields.String(load_from='ownerzip', dump_to='OwnerZip')
    exp_year = fields.Integer(load_from='expyear', dump_to='ExpYear')
    valid_date = fields.DateTime(load_from='validdate', dump_to='ValidDate')

    class Meta:
        ordered = True

    #@post_load
    #def combine_date_and_time(self, in_data):
    #    death_date, death_time = in_data['death_date'], in_data['death_time']
    #    today = datetime.datetime.today()
    #    if not death_time:
    #        death_time = datetime.time(0, 0, 0)
    #    try:
    #        in_data['death_date_and_time'] = datetime.datetime(
    #            death_date.year, death_date.month, death_date.day,
    #            death_time.hour, death_time.minute, death_time.second
    #        )
    #    except:
    #        in_data['death_date_and_time'] = None

    #    return

    @pre_load
    def fix_nas_and_strip(self, data):
        for k, v in data.items():
            if k in ['breed', 'color']:
                if v in ['.', '', 'NA', 'N/A']:
                    data[k] = None
                else:
                    data[k] = data[k].strip()

            if k in ['dogname']:
                data[k] = data[k].strip()

    @pre_load
    def fix_zip(self, data):
        f = 'ownerzip'
        if f in data and data[f] not in [None, '.', '', 'NA', 'N/A']:
            if len(data[f]) > 5 or '-' in data[f]:
                print(f"{data[f]} is not a valid 5-digit ZIP code.")
                data[f] = data[f][:5]
                print(f"... coerced to {data[f]}.")

dog_licenses_package_id = 'ad5bd3d6-1b53-4ed0-8cd9-157a985bd0bd' # Production version of Dog Licenses

current_year = datetime.now().year

#old_year = 2019 # Uncommend this and the 'historical' job
# below to process old data.

job_dicts = [
#    {
#        'job_code': 'historical',
#        'source_type': 'sftp',
#        'source_dir': 'Dog_Licenses',
#        'source_file': f'{old_year}_dog_licenses.csv',
#        'connector_config_string': 'sftp.county_sftp',
#        'encoding': 'utf-8-sig',
#        'schema': DogLicensesSchema,
#        #'primary_key_fields': [],
#        'always_wipe_data': True,
#        'upload_method': 'insert',
#        #'destinations': ['file'],
#        #'destination_file': f'{old_year}_dog_licenses.csv',
#        'package': dog_licenses_package_id,
#        'resource_name': f'{old_year} Dog Licenses'
#    },
    {
        'job_code': 'dog_licenses_this_year',
        'source_type': 'sftp',
        'source_dir': 'Dog_Licenses',
        'source_file': f'DL_gvData_{current_year}.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': DogLicensesSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'{current_year}_dog_licenses.csv', # purposes.
        'package': dog_licenses_package_id,
        'resource_name': f'{current_year} Dog Licenses'
    },
    {
        'job_code': 'dog_licenses_last_year',
        'source_type': 'sftp',
        'source_dir': 'Dog_Licenses',
        'source_file': f'DL_gvData_{current_year - 1}.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': DogLicensesSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'ignore_if_source_is_missing': True, # Maybe this should be bundled up with always_wipe_data into a nicer structure.
        'upload_method': 'insert',
        'package': dog_licenses_package_id,
        'resource_name': f'{current_year - 1} Dog Licenses'
    },
    {
        'job_code': 'lifetime_dog_licenses',
        'source_type': 'sftp',
        'source_dir': 'Dog_Licenses',
        'source_file': f'DL_gvData_2099.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': DogLicensesSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': dog_licenses_package_id,
        'resource_name': f'Lifetime Dog Licenses'
    },
]
