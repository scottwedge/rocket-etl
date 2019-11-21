import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class AverageRidershipSchema(pl.BaseSchema):
    route = fields.String(allow_none=False)
    ridership_route_code = fields.String(allow_none=False)
    route_full_name = fields.String(allow_none=False)
    current_garage = fields.String(allow_none=False)
    mode = fields.String(allow_none=False)
    month_start = fields.Date(allow_none=False)
    year_month = fields.String(load_from="date_key", allow_none=False) # You must
    # lowercase the field name you are loading from (using "Date_Key" will fail silently).
    day_type = fields.String(allow_none=False)
    avg_riders = fields.Integer(allow_none=False)
    day_count = fields.Integer(allow_none=False)

    class Meta:
        ordered = True

    @pre_load
    def fix_dates(self, data):
        for k, v in data.items():
            if k in ['month_start']:
                if v:
                    try:
                        data[k] = parser.parse(v).date().isoformat()
                    except:
                        data[k] = None

class OnTimePerformanceSchema(pl.BaseSchema):
    route = fields.String(allow_none=False)
    ridership_route_code = fields.String(allow_none=False)
    route_full_name = fields.String(allow_none=False)
    current_garage = fields.String(allow_none=False)
    mode = fields.String(allow_none=False)
    month_start = fields.Date(allow_none=False)
    year_month = fields.String(load_from="datekey", allow_none=False) # You must
    # lowercase the field name you are loading from (using "dateKey" will fail silently).
    day_type = fields.String(allow_none=False)
    on_time_percent = fields.Float(load_from="otp_pct", allow_none=True) # You must
    # lowercase the field name you are loading from (using "dateKey" will fail silently).
    data_source = fields.String(allow_none=False)

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k in ['otp_pct']:
                if v in ['NA']:
                    data[k] = None

    def fix_dates(self, data):
        for k, v in data.items():
            if k in ['month_start']:
                if v:
                    try:
                        data[k] = parser.parse(v).date().isoformat()
                    except:
                        data[k] = None

average_ridership_package_id = "e6c089da-43d1-439b-92fc-e500d6fb5e73" # Production version of Average Ridership package
otp_package_id = "b8b5fee7-2281-4426-a68e-2e05c6dec365" # Production version of Average Monthly OTP package

job_dicts = [
        {
        'source_type': 'http',
        'source_url_path': 'https://www.portauthority.org/external_data_sharing', # This is a stand-in for source_dir, so
        # it maintains the convention of not having a trailing slash and allows source_file to still be parsed
        # and easily used for whatever it was previously used for (specifying the file format in run_pipeline).
        'source_file': 'ridershipMonthAvg.csv',
        'encoding': 'utf-8-sig',
        'schema': AverageRidershipSchema,
        'primary_key_fields': ['route', 'month_start', 'day_type'],
        'upload_method': 'upsert',
        #'destinations': ['ckan_filestore'],
        'package': average_ridership_package_id,
        'resource_name': 'Monthly Average Ridership by Route & Weekday',
    },
    {
        'source_type': 'http',
        'source_url_path': 'https://www.portauthority.org/external_data_sharing', # This is a stand-in for source_dir, so
        # it maintains the convention of not having a trailing slash and allows source_file to still be parsed
        # and easily used for whatever it was previously used for (specifying the file format in run_pipeline).
        'source_file': 'routeMonthlyOTP.csv',
        'encoding': 'utf-8-sig',
        'schema': OnTimePerformanceSchema,
        'primary_key_fields': ['route', 'month_start', 'day_type'],
        'upload_method': 'upsert',
        #'destinations': ['ckan_filestore'],
        'package': otp_package_id,
        'resource_name': 'Monthly OTP by Route',
    },
]

def process_job(**kwparameters):
    job = kwparameters['job']
    use_local_files = kwparameters['use_local_files']
    clear_first = kwparameters['clear_first']
    test_mode = kwparameters['test_mode']
    job.default_setup(use_local_files)
    locators_by_destination = job.run_pipeline(test_mode, clear_first, file_format='csv')
    # [ ] What is file_format used for? Should it be hard-coded?

    return locators_by_destination # Return a dict allowing look up of final destinations of data (filepaths for local files and resource IDs for data sent to a CKAN instance).
