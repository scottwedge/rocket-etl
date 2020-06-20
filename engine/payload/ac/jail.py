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
    record_id = fields.Integer(load_from="RecordID", allow_none=False)
    census_date = fields.Date(allow_none=False)
    gender = fields.String(allow_none=True)
    race = fields.String(allow_none=True)
    age_at_booking = fields.Integer(load_from='AGE_AT_BOOKING_YRS', allow_none=True)
    age_at_census = fields.Integer(load_from='AGE_AT_CENSUS_YRS', allow_none=True)

    class Meta:
        ordered = True

    #@pre_load()
    #def format_date(self, data):
    #    data['date'] = date(
    #        int(data['date'][0:4]),
    #        int(data['date'][4:6]),
    #        int(data['date'][6:])).isoformat()
    @pre_load()
    def fix_nas(self, data):
        fields_to_fix = ['gender', 'race']
        for field in fields_to_fix:
            if field in data and data[field] in ['NA', '']:
                data[field] = None

package_id = 'd15ca172-66df-4508-8562-5ec54498cfd4' # Production version of Jail Census package

job_dicts = [
    {
        'source_type': 'sftp',
        'source_dir': 'jail_census_data',
        'source_file': 'ACJ_Census.csv',
        'connector_config_string': 'sftp.county_sftp', # This is just used to look up parameters in the settings.json file.
        'upload_method': 'upsert',
        'primary_key_fields': ['record_id', 'census_date'],
        'schema': JailCensusSchema,
        'package': package_id,
        'pipeline_name': 'ac_jail_census_pipeline', # Not yet used.
        'resource_name': 'ACJ Daily Census Data (Combined)'
    },
]
