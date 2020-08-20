import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from unidecode import unidecode

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa


DATE_FORMAT = "%Y-%m-%d"# %H:%M:%S"

class AsbestosPermitSchema(pl.BaseSchema):
    permit_number = fields.String()
    s_name = fields.String(allow_none=True)
    s_address = fields.String(allow_none=True)
    city = fields.String(allow_none=True)
    z_code = fields.String(allow_none=True)
    p_fee = fields.Float(allow_none=True)
    c_number = fields.String(allow_none=True)
    contractor = fields.String(allow_none=True)
    permit_specifications = fields.String(allow_none=True)
    square_feet = fields.Integer(allow_none=True)
    is_the_building_occupied = fields.Boolean(allow_none=True)
    i_date = fields.Date(format='%m/%d/%Y', allow_none=True)
    e_date = fields.Date(format='%m/%d/%Y', allow_none=True)
    achd_inspector = fields.String(allow_none=True)
    job_complete = fields.Boolean(allow_none=True)
    extension_date = fields.Boolean(allow_none=True, dump_to="is_permit_date_extended")
    permit_o_e_date = fields.Date(format='%m/%d/%Y', allow_none=True)
    project_type = fields.String(allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)

    class Meta:
        ordered = True

    @pre_load()
    def fix_booleans(self, in_data):
        boolean_fields = ['job_complete', 'is_the_building_occupied', 'extension_date']
        for field in boolean_fields:
            if field in in_data:
                if in_data[field] == 'TRUE':
                    in_data[field] = True
                elif in_data[field] == 'FALSE':
                    in_data[field] = False
                else:
                    in_data[field] = None

    @pre_load()
    def fix_fee(self, in_data):
        try:
            in_data['p_fee'] = in_data['p_fee'].lstrip('$')
        except:
            in_data['p_fee'] = None

    @pre_load()
    def clean_dates(self, in_data):
        try:
            in_data['e_date'] = datetime.strptime(in_data['e_date'], DATE_FORMAT).date().isoformat()
        except ValueError:
            in_data['e_date'] = None
        try:
            in_data['i_date'] = datetime.strptime(in_data['i_date'], DATE_FORMAT).date().isoformat()
        except ValueError:
            in_data['i_date'] = None
        try:
            in_data['permit_o_e_date'] = datetime.strptime(in_data['permit_o_e_date'], DATE_FORMAT).date().isoformat()
        except ValueError:
            in_data['permit_o_e_date'] = None

    @pre_load
    def fix_geocoordinates(self, data):
        for k, v in data.items():
            if k in ['latitude', 'longitude']:
                if v in ['', '0', '0.0', 'NA']:
                    data[k] = None


asbestos_permits_package_id = '7c0175b7-ff33-43ea-b353-bc2c5199a7ff'

job_dicts = [
    {
        'job_code': 'asbestos_permits',
        'source_type': 'sftp',
        'source_dir': 'Asbestos Permits', 
        'source_file': 'asbestos_sites.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': AsbestosPermitSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': asbestos_permits_package_id,
        'resource_name': f'Asbestos Permits Data'
    }
]
