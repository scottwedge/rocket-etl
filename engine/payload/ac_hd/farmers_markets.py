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

class FarmersMarketsSchema(pl.BaseSchema):
    market_id = fields.String(load_from='FarmMarketID'.lower(), dump_to='market_id')
    market_type = fields.String(load_from='MarketType'.lower())
    market_name = fields.String(load_from='MarketName'.lower())
    address1 = fields.String()
    #address2 = fields.String(allow_none=True) # Left out because it's always either
    # the same as Address 1 or empty.
    city = fields.String()
    state = fields.String(load_from='StateCode'.lower(), dump_to='state')
    zip_code = fields.String(load_from='zip', dump_to='zip_code')
    zip_plus = fields.String(allow_none=True, load_only=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    market_phone = fields.String(allow_none=True, load_from='MarketPhone'.lower(), dump_to='phone')
    market_phone_ext = fields.String(allow_none=True, load_from='MarketPhoneExt'.lower(), dump_to='phone_ext')
    county = fields.String(allow_none=True, load_from='FarmMarketCounty'.lower(), dump_to='county')

    class Meta:
        ordered = True

    @pre_load
    def fix_zip(self, data):
        if 'zip_plus' in data:
            if data['zip_plus'] != '' and len(data['zip'] > 0):
                assert len(data['zip']) == 5
                data['zip_code'] = f"{data['zip_code']}-{data['zip_plus']}"
        return data

    @pre_load
    def fix_geocoordinates(self, data):
        for k, v in data.items():
            if k in ['latitude', 'longitude']:
                if v in ['', '0', '0.0', 'NA']:
                    data[k] = None

class CumulativeFarmersMarketsSchema(FarmersMarketsSchema):
    year = fields.Integer(dump_only=True, default=datetime.now().year)

    class Meta:
        fields = ["year"] + list(FarmersMarketsSchema().fields.keys())

class VendorsSchema(pl.BaseSchema):
    market_id = fields.String(load_from='FarmMarketID'.lower(), dump_to='market_id')
    vendor_id = fields.String(load_from='vendorID'.lower(), dump_to='vendor_id')
    vendor_name = fields.String(load_from='vendorName'.lower(), dump_to='vendor_name')
    vendor_phone = fields.String(allow_none=True, load_from='VendorPhone'.lower(), dump_to='vendor_phone')
    vendor_schedule = fields.String(allow_none=True, load_from='VendorSchedule'.lower(), dump_to='vendor_schedule')
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_geocoordinates(self, data):
        for k, v in data.items():
            if k in ['latitude', 'longitude']:
                if v in ['', '0', '0.0', 'NA']:
                    data[k] = None

    @pre_load
    def fix_schedule(self, data):
        if 'vendorschedule' in data and data['vendorschedule'] not in [None]:
            parts = data['vendorschedule'].split('\n')
            if len(parts) > 1:
                data['vendorschedule'] = '; '.join([part.strip() for part in parts])

class CumulativeVendorsSchema(VendorsSchema):
    year = fields.Integer(dump_only=True, default=datetime.now().year)

    class Meta:
        fields = ["year"] + list(VendorsSchema().fields.keys())

farmers_markets_package_id = '5706fa23-87f6-4757-be8a-bcfc0b677e01' # Production version of Farmers Markets

current_year = datetime.now().year

job_dicts = [
    {
        'job_code': 'current_farmer_markets',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'alco_farmers_markets_{current_year}.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': FarmersMarketsSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'{current_year}_farmers_markets.csv', # purposes.
        'package': farmers_markets_package_id,
        'resource_name': f'Current Farmers Markets'
    },
    {
        'job_code': 'cumulative_farmer_markets',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'alco_farmers_markets_{current_year}.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': CumulativeFarmersMarketsSchema,
        'primary_key_fields': ['market_id', 'year'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'package': farmers_markets_package_id,
        'resource_name': f'Historical Farmers Markets'
    },
    {
        'job_code': 'current_vendors',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'alco_market_vendors_{current_year}.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': VendorsSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': farmers_markets_package_id,
        'resource_name': f'Current Vendors'
    },
    {
        'job_code': 'cumulative_vendors',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'alco_market_vendors_{current_year}.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': CumulativeVendorsSchema,
        'primary_key_fields': ['vendor_id', 'year'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'package': farmers_markets_package_id,
        'resource_name': f'Historical Vendors'
    },
]
