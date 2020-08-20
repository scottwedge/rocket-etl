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


class MWDBEBusinesseschema(pl.BaseSchema):
    certification_type = fields.String()
    firm_name = fields.String()
    owners = fields.String()
    work_decsription = fields.String(dump_to="work_description", allow_none=True)
    naics_codes = fields.String()
    physical_address = fields.String()
    mailing_address = fields.String()
    phone_number = fields.String()
    fax_number = fields.String(allow_none=True)
    email_address = fields.String()
    website = fields.String(allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        for field in data.keys():
            if field in ['phone_number', 'fax_number']:
                if data[field] in ['NA', '', ' ', '___-___-____']:
                    data[field] = None
            elif field in ['work_decsription', 'email_address', 'website']:
                if data[field] in ['NA', '', ' ']:
                    data[field] = None

mwdbe_package_id = 'f2deaf21-1a73-4bf7-a6a2-d2e8ea1f2b18'

job_dicts = [
        {
        'source_type': 'sftp',
        'source_dir': 'Equity_and_Inclusion',
        'source_file': 'mwdbe_certified.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': MWDBEBusinesseschema,
        #'primary_key_fields': [], # firm_name seems unique but probably not guaranteed to be so.
        'always_wipe_data': True,
        'upload_method': 'insert',
        #'destinations': ['ckan_filestore'],
        'package': mwdbe_package_id,
        'resource_name': 'Allegheny County Certified MWDBE Businesses'
    },
]
