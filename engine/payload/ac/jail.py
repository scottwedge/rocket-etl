import csv, json, requests, sys, traceback
from datetime import date, timedelta
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class JailCensusSchema(pl.BaseSchema):
    record_id = fields.Integer(load_from="RecordID".lower(), dump_to='record_id', allow_none=False)
    census_date = fields.Date(allow_none=False, load_from='CENSUS_DATE'.lower(), dump_to='census_date')
    gender = fields.String(allow_none=True)
    race = fields.String(allow_none=True)
    age_at_booking = fields.Integer(load_from='AGE_AT_BOOKING_YRS'.lower(), dump_to='age_at_booking', allow_none=True)
    age_at_census = fields.Integer(load_from='AGE_AT_CENSUS_YRS'.lower(), dump_to='age_at_census', allow_none=True)

    class Meta:
        ordered = True

    @pre_load()
    def fix_nas_and_strip(self, data):
        fields_to_fix = ['gender', 'race']
        for field in fields_to_fix:
            if field in data and data[field] in ['NA', '']:
                data[field] = None
            elif data[field] is not None:
                data[field] = data[field].strip()

    @pre_load()
    def fix_still_more_nas(self, data):
        fields_to_fix = ['AGE_AT_BOOKING_YRS'.lower(), 'AGE_AT_CENSUS_YRS'.lower()]
        for field in fields_to_fix:
            if field in data and data[field] in ['NA', '']:
                data[field] = None

    @post_load()
    def check_for_unlikely_ages(self, data):
        fields = ['age_at_booking', 'age_at_census']
        for field in fields:
            if field in data and data[field] not in ['NA', '', None]:
                if data[field] < 12 or data[field] > 100:
                    msg = f"Anomalous age found in Jail Census: {data[field]} years for id = {data['record_id']}."
                    print(msg)


package_id = 'd15ca172-66df-4508-8562-5ec54498cfd4' # Production version of Jail Census package

job_dicts = [
    {
        'source_type': 'sftp',
        'source_dir': 'jail_census_data',
        'source_file': 'ACJ_Census.csv',
        #'source_file': 'acj-census-cy2015-2019-rev.csv',
        'connector_config_string': 'sftp.county_sftp', # This is just used to look up parameters in the settings.json file.
        'upload_method': 'upsert',
        'primary_key_fields': ['record_id', 'census_date'],
        'schema': JailCensusSchema,
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'acj-output.csv', # purposes.
        'package': package_id,
        'pipeline_name': 'ac_jail_census_pipeline', # Not yet used.
        'resource_name': 'ACJ Daily Census Data (Combined)'
    },

]
