import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint
from scourgify import normalize_address_record
from collections import OrderedDict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import fetch_city_file
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def simpler_time(time_str):
    t = parser.parse(time_str).time()
    if t.second != 0:
        return t.strftime("%H:%M:%S")
    if t.minute != 0:
        return t.strftime("%H:%M")
    return t.strftime("%H")

class FarmersMarketsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='farmers_markets')
    name = fields.String()
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String()
    city = fields.String()
    state = fields.String()
    zip_code = fields.String(load_from='zip')
    latitude = fields.Float()
    longitude = fields.Float()
    #additional_directions = fields.String(allow_none=True)
    hours_of_operation = fields.String(load_from='day_time')
    season = fields.String(load_only=True, allow_none=True)
    child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #affiliations = fields.String(allow_none=True)
    computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.String(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets' 
    #data_source_url = 

    class Meta:
        ordered = True

    @pre_load
    def strip_strings(self, data): # Eventually, make this some kind of built-in easily callable preprocessor, along with fix_encoding_errors.
        #fields_to_recode = ['facility_name', 'description']
        #for field in fields_to_recode:
        #    data[field] = fix_encoding_errors(data[field].strip())
        fields_to_strip = ['name']
        for field in fields_to_strip:
            if type(data[field]) == str:
                data[field] = data[field].strip()

    @pre_load
    def fix_hours(self, data):
        if data['season'] not in [None, '']:
            data['day_time'] = f"{data['season'].strip()}, {data['day_time'].strip()}"

class FishFriesSchema(pl.BaseSchema):
#venue_type = fields.String() # Identifies churches!
#venue_notes = fields.String(allow_none=True)
    validated = fields.Boolean(load_only=True) # Probably 2020 data should be used and non-validated locations should be dumped.
    asset_type = fields.String(dump_only=True, default='fish_fries')
    name = fields.String(load_from='venue_name')
    localizability = fields.String(dump_only=True, default='fixed')
    venue_address = fields.String(load_only=True) # Can we split this into street_address, city, state, and zip_code? Try using usaddresses.
    street_address = fields.String(allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    url = fields.String(load_from='website', allow_none=True)
    email = fields.String(allow_none=True)
    phone = fields.String(allow_none=True)
    hours_of_operation = fields.String(load_from='events', allow_none=True)
    child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    accessibility = fields.Boolean(load_from='handicap', allow_none=True) # [ ] Verify that accessibility meaning is consistent with handicap meaning.
    computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.String(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets' 
    #data_source_url = 

    class Meta:
        ordered = True

    #@pre_load
    #def strip_strings(self, data): # Eventually, make this some kind of built-in easily callable preprocessor, along with fix_encoding_errors.
    #    #fields_to_recode = ['facility_name', 'description']
    #    #for field in fields_to_recode:
    #    #    data[field] = fix_encoding_errors(data[field].strip())
    #    fields_to_strip = ['name']
    #    for field in fields_to_strip:
    #        if type(data[field]) == str:
    #            data[field] = data[field].strip()
    #@pre_load
    #def eliminate_the_unvalidated(self, data):
    #    if data['validated'] != 'True':
    #        ic(self)
    @pre_load
    def fix_booleans(self, data):
        booleans = ['handicap']
        for boolean in booleans:
            if data[boolean] == 'True':
                data[boolean] = True
            elif data[boolean] == 'False':
                data[boolean] = False

    @pre_load
    def parse_address(self, data):
        # First hard code some addresses that break the parsing scheme.
        #if data['venue_address'] == '14335 U.S. 30, Irwin, PA 15642':
        #    data['street_address'] = '14335 U.S. 30'
        #    data['city'] = 'Irwin'
        #    data['state'] = 'PA'
        #    data['zip_code'] = '15642'
        #elif data['venue_address'] == '1520 State Rte 837, Elrama, Pennsylvania 15038, United States':
        #    data['street_address'] = '1520 State Rte 837'
        #    data['city'] = 'Elrama'
        #    data['state'] = 'PA'
        #    data['zip_code'] = '15038'
        #elif data['venue_address'] == '131 Bigham, Pittsburgh, PA':
        #    data['street_address'] = '131 Bigham St'
        #    data['city'] = 'Pittsburgh'
        #    data['state'] = 'PA'
        #    data['zip_code'] = '15211'
        #elif data['venue_address'] == '11264 Route 97 North, Waterford PA':
        #    data['street_address'] = '11264 Route 97 North'
        #    data['city'] = 'Waterford'
        #    data['state'] = 'PA'
        #elif data['venue_address'] == '100 Adams Shoppes, Cranberry, PA':
        #    data['street_address'], data['city'], data['state'], data['zip_code'] = '100 Adams Shoppes', 'Mars', 'PA', '16046'
        #elif data['venue_address'] == '11919 U.S. 422 Penn Run, PA 15765':
        #    data['street_address'] = '11919 U.S. 422'
        #    data['city'], data['state'] = 'Penn Run', 'PA'
        #    data['zip_code'] = '15765'
        #else:
        #    try:
        #        parsed, detected_type = usaddress.tag(data['venue_address'])
        #        assert detected_type == 'Street Address'
        #        house_number = parsed['AddressNumber']
        #        street_pre_type = parsed.get('StreetNamePreType', '')
        #        street_name = parsed['StreetName']
        #        street_post_type = parsed.get('StreetNamePostType', '')
        #        data['street_address'] = f"{ house_number } { street_name } { street_post_type }"
        #        data['city'] = parsed['PlaceName']
        #        data['state'] = parsed.get('StateName', None)
        #        data['zip_code'] = parsed.get('ZipCode', None)
        #    except KeyError:
        #        ic(parsed)
        #        raise
        if data['venue_address'] == 'Holy Trinity Church Social Hall, 529 Grant Avenue Ext. West Mifflin, PA 15122-3830':
            data['street_address'] = '529 Grant Avenue Ext.'
            data['city'] = 'West Mifflin'
            data['state'] = 'PA'
            data['zip_code'] = '15122-3830'
        elif data['venue_address'] == 'Cherry Ridge Ln, Lowber, Pennsylvania 15660, United States':
            data['street_address'] = 'Cherry Ridge Ln'
            data['city'] = 'Lowber'
            data['state'] = 'PA'
            data['zip_code'] = '15660'
        elif data['venue_address'] == 'McCann Hall, 1915 Broadway Avenue, Pittsburgh, Pennsylvania 15216, United States':
            data['street_address'] = '1915 Broadway Avenue'
            data['city'] = 'Pittsburgh'
            data['state'] = 'PA'
            data['zip_code'] = '15216'
        elif data['venue_address'] == '536 State Route 130 Level Green, Pennsylvania':
            data['street_address'] = '536 State Route 130'
            data['city'] = 'Trafford'
            data['state'] = 'PA'
            data['zip_code'] = '15085'
        elif data['venue_address'] == 'Marian Hall (lower level of church), 624 Washington Ave, Charleroi, Pennsylvania 15022, United States':
            data['street_address'] = '624 Washington Ave'
            data['city'] = 'Charleroi'
            data['state'] = 'PA'
            data['zip_code'] = '15022'
        else:
            try:
                parsed = normalize_address_record(data['venue_address'])
                #assert detected_type == 'Street Address'
                data['street_address'] = parsed['address_line_1']
                if 'address_line_2' in parsed and parsed['address_line_2'] not in [None, '']:
                    data['street_address'] += ', ' + parsed['address_line_2']
                data['city'] = parsed.get('city', None)
                data['state'] = parsed.get('state', None)
                data['zip_code'] = parsed.get('postal_code', None)
            except KeyError:
                ic(parsed)
                raise


class LibrariesSchema(pl.BaseSchema):
#clpid = fields.String()
#sq_ft = fields.Integer()
    asset_type = fields.String(dump_only=True, default='libraries')
    name = fields.String()
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address')
    city = fields.String()
    state = fields.String()
    zip_code = fields.String(load_from='zip4')
    latitude = fields.Float(load_from='lat', allow_none=True)
    longitude = fields.Float(load_from='lon', allow_none=True)
    phone = fields.String()
    #additional_directions = fields.String(allow_none=True)
    moopen = fields.String(load_only=True, allow_none=True)
    molose = fields.String(load_only=True, allow_none=True)
    tuopen = fields.String(load_only=True, allow_none=True)
    tuclose = fields.String(load_only=True, allow_none=True)
    weopen = fields.String(load_only=True, allow_none=True)
    weclose = fields.String(load_only=True, allow_none=True)
    thopen = fields.String(load_only=True, allow_none=True)
    thclose = fields.String(load_only=True, allow_none=True)
    fropen = fields.String(load_only=True, allow_none=True)
    frclose = fields.String(load_only=True, allow_none=True)
    saopen = fields.String(load_only=True, allow_none=True)
    saclose = fields.String(load_only=True, allow_none=True)
    suopen = fields.String(load_only=True, allow_none=True)
    suclose = fields.String(load_only=True, allow_none=True)
    hours_of_operation = fields.String(allow_none=True)
    child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #affiliations = fields.String(allow_none=True)
    computers_available = fields.String(dump_only=True, allow_none=True, default=True)

    sensitive = fields.String(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets' 
    #data_source_url = 

    class Meta:
        ordered = True

    @pre_load
    def strip_strings(self, data): # Eventually, make this some kind of built-in easily callable preprocessor, along with fix_encoding_errors.
        #fields_to_recode = ['facility_name', 'description']
        #for field in fields_to_recode:
        #    data[field] = fix_encoding_errors(data[field].strip())
        fields_to_strip = ['name']
        for field in fields_to_strip:
            if type(data[field]) == str:
                data[field] = data[field].strip()

    @pre_load
    def fix_hours(self, data):
        field_prefix_by_day = OrderedDict([('M', 'mo'), ('Tu', 'tu'),
            ('W', 'we'), ('Th', 'th'), ('F', 'fr'), ('Sat', 'sa'), ('Sun', 'su')])
        hours_entries = []
        for day, prefix in field_prefix_by_day.items():
            open_field = prefix + "open"
            close_field = prefix + "close"
            if open_field in data and data[open_field] not in [None, '']:
                hours_entries.append(f"{day} {simpler_time(data[open_field])}-{simpler_time(data[close_field])}")
        data['hours_of_operation'] = ', '.join(hours_entries)

class FaithBasedFacilitiesSchema(pl.BaseSchema):
    # Unused field: Denomination
    # Calvin Memorial Church has no maiing address!
    asset_type = fields.String(dump_only=True, default='faith-based_facilities')
    name = fields.String(load_from='account_name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='mailing_address_line_1', allow_none=True)
    address2 = fields.String(load_only=True, load_from='mailing_address_line_2', allow_none=True)
    city = fields.String(load_from='mailing_city', allow_none=True)
    state = fields.String(load_from='mailing_state/province', allow_none=True)
    zip_code = fields.String(load_from='mailing_zip/postal_code', allow_none=True)
    url = fields.String(load_from='website', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.String(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets' 
    #data_source_url = 

    class Meta:
        ordered = True

    @pre_load
    def join_address(self, data):
        if 'address2' in data and data['address2'] not in [None, '']:
            data['street_address'] += ', ' + data['address2']


#def conditionally_get_city_files(job, **kwparameters):
#    if not kwparameters['use_local_files']:
#        fetch_city_file(job)

asset_map_source_dir = '/Users/drw/WPRDC/asset-map/AssetMapDownloads1/'
processed_dir = ''

job_dicts = [
    {
        'source_type': 'local',
        #'source_dir_absolute': '/Users/drw/WPRDC/asset-map/AssetMapDownloads1',
        'source_file': '/Users/drw/WPRDC/asset-map/AssetMapDownloads1/2019_farmers-markets.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FarmersMarketsSchema,
        'always_clear_first': True,
        'destinations': ['file'],
        'destination_file': '/Users/drw/WPRDC/asset-map/processed/2019_farmers-markets.csv',
        'resource_name': 'farmers_markets',
    },
    {
        'source_type': 'local',
        #'source_dir_absolute': '/Users/drw/WPRDC/asset-map/AssetMapDownloads1',
        'source_file': '/Users/drw/WPRDC/asset-map/AssetMapDownloads1/2019_pittsburgh_fish_fry_locations_validated.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FishFriesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': '/Users/drw/WPRDC/asset-map/processed/2019_pittsburgh_fish_fry_locations_validated.csv',
        'resource_name': 'fish_fries',
    },
    {
        'source_type': 'local',
        'source_file': '/Users/drw/WPRDC/asset-map/AssetMapDownloads1/CLP_Library_Locations.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': LibrariesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['clpid'],
        'destinations': ['file'],
        'destination_file': '/Users/drw/WPRDC/asset-map/processed/CLP_Library_Locations.csv',
        'resource_name': 'libraries',
    },
    {
        'source_type': 'local',
        'source_file': asset_map_source_dir + 'AlleghenyCountyChurches.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FaithBasedFacilitiesSchema,
        'always_clear_first': True,
        'primary_key_fields': [''],
        'destinations': ['file'],
        'destination_file': '/Users/drw/WPRDC/asset-map/processed/AlleghenyCountyChurches.csv',
        'resource_name': 'faith-based_facilities',
    },
]
# [ ] Fix fish-fries validation by googling for how to delete rows in marshmallow schemas (or else pre-process the rows somehow... load the whole thing into memory and filter).
