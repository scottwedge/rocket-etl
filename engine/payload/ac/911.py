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

class CallSchema(pl.BaseSchema):
    call_id_hash = fields.String(allow_none=False)
    service = fields.String(allow_none=True)
    priority = fields.String(allow_none=True)
    priority_desc = fields.String(allow_none=True)
    call_quarter = fields.String(allow_none=False)
    call_year = fields.Integer(allow_none=False)
    description_short = fields.String(allow_none=True)
    city_code = fields.String(allow_none=True)
    city_name = fields.String(allow_none=True)
    geoid = fields.String(allow_none=True)
    censusblockgroupcenter_x = fields.Float(dump_to='census_block_group_center__x', allow_none=True)
    censusblockgroupcenter_y = fields.Float(dump_to='census_block_group_center__y', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k in ['priority', 'priority_desc', 'agency', 'description_short', 'city_code', 'city_name', 'geoid', 'censusblockgroupcenter_x', 'censusblockgroupcenter_y']:
                if v in ['.', '', 'NA', 'N/A']:
                    data[k] = None

    #@pre_load
    #def check_blacklist(self, data):
    #    pass

nine_one_one_package_id = 'abba9671-a026-4270-9c83-003a1414d628' # Production version of 911 Calls

job_dicts = [
    {
        'job_code': '911_ems',
        'source_type': 'sftp',
        'source_dir': '911-CAD',
        'source_file': '911-EMS-dispatches.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': CallSchema,
        'primary_key_fields': ['call_id_hash', 'call_year', 'call_quarter'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'{current_year}_dog_licenses.csv', # purposes.
        'package': nine_one_one_package_id,
        'resource_name': '911 EMS Dispatches'
    },
    {
        'job_code': '911_fire',
        'source_type': 'sftp',
        'source_dir': '911-CAD',
        'source_file': '911-fire-dispatches.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': CallSchema,
        'primary_key_fields': ['call_id_hash', 'call_year', 'call_quarter'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'{current_year}_dog_licenses.csv', # purposes.
        'package': nine_one_one_package_id,
        'resource_name': '911 Fire Dispatches'
    },
]
