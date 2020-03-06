import csv, json, requests, sys, traceback, re
from datetime import datetime
from dateutil import parser
from pprint import pprint
from scourgify import normalize_address_record
from collections import OrderedDict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import fetch_city_file
from engine.notify import send_to_slack
from engine.parameters.local_parameters import ASSET_MAP_SOURCE_DIR, ASSET_MAP_PROCESSED_DIR

csv.field_size_limit(sys.maxsize) # Workaround to address
# this error:
#       _csv.Error: field larger than field limit (131072)
# which is probably because of those giant geometry fields
# in a converted shapefile of city parks.

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def form_key(job_code, file_key):
    """Concatenate the job code with the file's primary key
    to form a compound primary key."""
    return f'{job_code}::{file_key}'

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

    sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
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
    asset_type = fields.String(dump_only=True, default='community_nonprofit_orgs')
    name = fields.String(load_from='venue_name')
    localizability = fields.String(dump_only=True, default='fixed')
    periodicity = fields.String(dump_only=True, default='seasonal') # Could have values like 'permanent', 'annual', 'seasonal', 'monthly', 'one-time'
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
    child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    accessibility = fields.Boolean(load_from='handicap', allow_none=True) # [ ] Verify that accessibility meaning is consistent with handicap meaning.
    computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url
    tags = fields.String(dump_only=True, default='fish fry')
    notes = fields.String(load_from='events', allow_none=True)

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
    def fix_notes(self, data):
        # Since the fish fry events are being squeezed into the "community/non-profit organizations"
        # category, the hours of operation will be put into the notes field like so:
        if 'events' in data and data['events'] not in [None, '']:
            data['notes'] = f"Fish Fry events: {data['events']}"

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
    job_code = 'clp_libraries'
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

    sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =
    primary_key_from_rocket = fields.String(load_from='clpid', allow_none=False)

    class Meta:
        ordered = True

    @post_load
    def fix_key(self, data):
        assert hasattr(self, 'job_code')
        data['primary_key_from_rocket'] = form_key(self.job_code, data['primary_key_from_rocket'])

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

    sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
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

class FamilySupportCentersSchema(pl.BaseSchema):
    # Unused field: Denomination
    # Calvin Memorial Church has no maiing address!
    asset_type = fields.String(dump_only=True, default='family_support_centers')
    name = fields.String(load_from='center')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='facility_address_line', allow_none=True)
    city = fields.String(load_from='facility_city', allow_none=True)
    state = fields.String(load_from='facility_state', allow_none=True)
    zip_code = fields.String(load_from='facility_zip_code', allow_none=True)
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    organization_name = fields.String(load_from='lead_agency', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =
    primary_key_from_rocket = fields.String(load_from='objectid', allow_none=True) # Possibly Unreliable

    class Meta:
        ordered = True

class SeniorCentersSchema(pl.BaseSchema):
    # Unused field: Denomination
    # Calvin Memorial Church has no mailing address!
    asset_type = fields.String(dump_only=True, default='senior_centers')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    #popupinfo = fields.String(load_only=True, allow_none=True)
    #snippet = fields.String(load_only=True, allow_none=True) # One PopupInfo string was accidentally put in this field.
    street_address = fields.String(allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(allow_none=True)
    geometry = fields.String(load_only=True)
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    #organization_name = fields.String(load_from='lead_agency', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =
    primary_key_from_rocket = fields.String(load_from='id', allow_none=True) # Possibly Unreliable

    class Meta:
        ordered = True

    @pre_load
    def extract_coordinates(self, data):
        if data['geometry'] is not None:
            geometry = json.loads(data['geometry'])
            coordinates = geometry['coordinates']
            data['latitude'] = coordinates[1]
            data['longitude'] = coordinates[0]

class PollingPlacesSchema(pl.BaseSchema):
    # This data source has an "Accessible" field, but there
    # are no values in that field.
    asset_type = fields.String(dump_only=True, default='polling_places')
    name = fields.String(load_from='locname')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='newaddress', allow_none=True) # Note that
    # there is also an "OrigAddress" field that is sometimes different.
    city = fields.String(load_from='city', allow_none=True)
    state = fields.String(dump_only=True, default='PA')
    zip_code = fields.String(load_from='zip', allow_none=True)
    parcel_id = fields.String(load_from='parcel', allow_none=True)
    #url = fields.String(load_from='website', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)
    latitude = fields.Float(load_from='y', allow_none=True)
    longitude = fields.Float(load_from='x', allow_none=True)

    sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =
    primary_key_from_rocket = fields.String(load_from='objectid_1', allow_none=True) # Possibly Unreliable

    class Meta:
        ordered = True

class ACHACommunitySitesSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='acha_community_sites')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(allow_none=True)
    #city = fields.String(allow_none=True)
    #state = fields.String(allow_none=True)
    zip_code = fields.String(allow_none=True)
    geometry = fields.String(load_only=True)
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    #organization_name = fields.String(load_from='lead_agency', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =
    primary_key_from_rocket = fields.String(load_from='objectid', allow_none=True) # Possibly Unreliable

    class Meta:
        ordered = True

    @pre_load
    def extract_coordinates(self, data):
        if data['geometry'] is not None:
            geometry = json.loads(data['geometry'])
            coordinates = geometry['coordinates']
            data['latitude'] = coordinates[1]
            data['longitude'] = coordinates[0]

class ClinicsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='achd_clinics')
    name = fields.String(load_from='type_1')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='staddr', allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(load_from='zip', allow_none=True)
    #latitude = fields.Float(load_from='latitude', allow_none=True)
    #longitude = fields.Float(load_from='longitude', allow_none=True)
    #organization_name = fields.String(load_from='lead_agency', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =
    primary_key_from_rocket = fields.String(load_from='objectid_1', allow_none=True) # Possibly Unreliable

    class Meta:
        ordered = True

    @pre_load
    def fix_name(self, data):
        f = 'type_1'
        data['name'] = 'ACHD Clinic'
        if data[f] not in [None, '']:
            data['name'] = f'ACHD {data[f]} Clinic'

    @pre_load
    def fix_address(self, data):
        f = 'subaddtype'
        if data[f] not in [None, '']:
            if data[f].strip() != '':
                data['staddr'] += f" ({data[f]} {data['subaddunit']})"

class AffordableHousingSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='affordable_housing')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(load_from='city_1', allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(load_from='zip', allow_none=True)
    latitude = fields.Float(load_from='y', allow_none=True)
    longitude = fields.Float(load_from='x', allow_none=True)
    phone = fields.String(load_from='telephone', allow_none=True)
    organization_name = fields.String(load_from='owner', allow_none=True)
    organization_phone = fields.String(load_from='owner_phon', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def fix_phone(self, data):
        f = 'telephone'
        if data[f] not in [None, '']:
            data['phone'] = data[f]

class WICVendorsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='wic_vendors')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='arc_street', allow_none=True)
    city = fields.String(load_from='arc_city', allow_none=True)
    #state = fields.String(allow_none=True) # The 'ARC_State' field in the WIC data is empty.
    zip_code = fields.String(load_from='arc_zip', allow_none=True)
    #latitude = fields.Float(load_from='y', allow_none=True)
    #longitude = fields.Float(load_from='x', allow_none=True)
    #phone = fields.String(load_from='telephone', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =
    primary_key_from_rocket = fields.String(load_from='objectid', allow_none=True) # Possibly Unreliable

    class Meta:
        ordered = True

class BigBurghServicesSchema(pl.BaseSchema):
    name = fields.String(load_from='service_name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    #city = fields.String(load_from='address', allow_none=True)
    state = fields.String(load_from='address', allow_none=True)
    zip_code = fields.String(load_from='address', allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    phone = fields.String(allow_none=True)
    organization_name = fields.String(load_from='organization', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    hours_of_operation = fields.String(load_from='schedule', allow_none=True)
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.Boolean(allow_none=True)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def fix_address_fields(self, data):
        f = 'address'
        if data[f] in [None, '']:
            data['street_address'] = None
            data['state'] = None
            data['zip_code'] = None
            data['sensitive'] = True
        else:
            try:
                parsed = normalize_address_record(data[f])
                #assert detected_type == 'Street Address'
                parts = data[f].split(', ')
                data['street_address'] = parts[0]
                data['state'] = parsed.get('state', None)
                data['zip_code'] = parsed.get('postal_code', None)
                data['sensitive'] = False
            except KeyError:
                ic(parsed)
                raise

class HomelessSheltersSchema(BigBurghServicesSchema):
    asset_type = fields.String(dump_only=True, default='homeless_shelters')

class BigBurghRecCentersSchema(BigBurghServicesSchema):
    asset_type = fields.String(dump_only=True, default='rec_centers')

class BusStopsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='bus_stops')
    name = fields.String(load_from='median_stop_name')
    localizability = fields.String(dump_only=True, default='fixed')
    #street_address = fields.String(load_from='address', allow_none=True)
    #city = fields.String(load_from='address', allow_none=True)
    #state = fields.String(load_from='address', allow_none=True)
    #zip_code = fields.String(load_from='address', allow_none=True)
    latitude = fields.Float(load_from='concat_latitude', allow_none=True)
    longitude = fields.Float(load_from='concat_longitude', allow_none=True)
    #phone = fields.String(allow_none=True)
    #organization_name = fields.String(load_from='organization', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='schedule', allow_none=True)
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(allow_none=True)
    notes = fields.String(load_from='concat_route', allow_none=True)

    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =
    primary_key_from_rocket = fields.String(load_from='stop_id', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fill_out_notes(self, data):
        # fix NAs
        fs = ['concat_latitude', 'concat_longitude', 'concat_stop_type', 'concat_shelter']

        for f in fs:
            if f in data and data[f] == "#N/A":
                data[f] = None

        data['notes'] = f"Routes: {data['concat_route']}, stop type: {data['concat_stop_type']}, shelter: {data['concat_shelter']}"

class CatholicSchema(pl.BaseSchema):
    # No addresses present in this file.
    asset_type = fields.String(dump_only=True, default='faith-based_facilities')
    name = fields.String()
    localizability = fields.String(dump_only=True, default='fixed')
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    url = fields.String(load_from='website', allow_none=True)
    additional_directions = fields.String(load_from='directions', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)
    phone = fields.String(load_from='phone_number', allow_none=True)
    email = fields.String(allow_none=True)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    last_updated = fields.DateTime(load_from='last_update', allow_none=True)
    data_source_name = fields.String(default='Catholic Churches in Allegheny County')
    data_source_url = fields.String(default='http://alcogis.maps.arcgis.com/home/item.html?id=a691de8fc3254f6d8b23d97610f67920')
    primary_key_from_rocket = fields.String(load_from='id', allow_none=True) # This key
    # looks like it could be good, but without knowing more about the data source,
    # it's hard to be sure.

    class Meta:
        ordered = True

    @pre_load
    def fix_datetime(self, data):
        # Since the fish fry events are being squeezed into the "community/non-profit organizations"
        # category, the hours of operation will be put into the notes field like so:
        if 'last_update' in data and data['last_update'] not in [None, '']:
            data['last_updated'] = parser.parse(data['last_update']).isoformat()

class MoreLibrariesSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='libraries')
    name = fields.String(load_from='library')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(load_from='city_1', allow_none=True)
    state = fields.String(load_from='state_1', allow_none=True)
    child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url
    primary_key_from_rocket = fields.String(load_from='objectid_12_13') # Possibly Unreliable

    class Meta:
        ordered = True

class MuseumsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='museums')
    name = fields.String(load_from='descr')
    localizability = fields.String(dump_only=True, default='fixed')
    latitude = fields.Float(load_from='y', allow_none=True)
    longitude = fields.Float(load_from='x', allow_none=True)
    county1 = fields.String(load_from='county1', dump_to='county', allow_none=True) # This one is not currently used in the asset-map schema.

    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url
    #primary_key_from_rocket = fields.String(load_from='fid') # This is just the row number
    # so is considered unreliable and fragile.

    class Meta:
        ordered = True

class WICOfficesSchema(pl.BaseSchema):
    # There are X and Y values in the source file, but they
    # are not longitude and latitude values.
    asset_type = fields.String(dump_only=True, default='wic_offices')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(load_from='arc_city', allow_none=True)
    state = fields.String(load_from='state_1', allow_none=True)
    zip_code = fields.String(load_from='zipcode', allow_none=True)
    phone = fields.String(load_from='phone_1', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    hours_of_operation = fields.String(load_from='officehours')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
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
            data['address'] += ', ' + data['address2']

    @pre_load
    def fix_degenerate_fields(self, data):
        if 'phone_1' in data:
            data['phone'] = data['phone_1']
        if 'city_1' in data:
            data['city'] = data['city_1']

class RecCentersSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='rec_centers')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address_number', allow_none=True)
    city = fields.String(default='Pittsburgh')
    state = fields.String(default='PA')
    zip_code = fields.String(load_from='zip', allow_none=True)
    parcel_id = fields.String(allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    organization_name = fields.String(load_from='primary_user', allow_none=True)
    #phone = fields.String(load_from='phone_1', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='officehours')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =
    primary_key_from_rocket = fields.String(load_from='id')

    class Meta:
        ordered = True

    @pre_load
    def join_address(self, data):
        if 'street' in data and data['street'] not in [None, '']:
            data['address_number'] += ' ' + data['street']

class FedQualHealthCentersSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='health_centers')
    name = fields.String(load_from='sitename')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='arc_addres', allow_none=True)
    city = fields.String(load_from='arc_city', default=None)
    state = fields.String(load_from='arc_state', default=None)
    zip_code = fields.String(allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    #organization_name = fields.String(load_from='primary_user', allow_none=True)
    phone = fields.String(allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='officehours')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =
    primary_key_from_rocket = fields.String(load_from='objectid')

    class Meta:
        ordered = True

    @pre_load
    def fix_coordinates(self, data):
        if 'geometry' in data and data['geometry'] not in [None, '']:
            geometry = json.loads(data['geometry'])
            coordinates = geometry['coordinates']
            data['latitude'] = coordinates[1]
            data['longitude'] = coordinates[0]

class PlacesOfWorshipSchema(pl.BaseSchema):
    # Unused: Denomination/Religion and a few Attendance/Membership values
    asset_type = fields.String(dump_only=True, default='faith-based_facilities')
    name = fields.String()
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(load_from='city', default=None)
    state = fields.String(load_from='state', default=None)
    zip_code = fields.String(load_from='zip', allow_none=True)
    latitude = fields.Float(load_from='y', allow_none=True)
    longitude = fields.Float(load_from='x', allow_none=True)
    #organization_name = fields.String(load_from='primary_user', allow_none=True)
    phone = fields.String(load_from='telephone', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='officehours')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def join_address(self, data):
        f2 = 'address2'
        if f2 in data and data[f2] not in [None, '', 'NOT AVAILABLE']:
            data['address'] += ' ' + data[f2]

class ParkAndRidesSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='park_and_rides')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='street_address', allow_none=True)
    city = fields.String(default=None)
    state = fields.String(default=None)
    #zip_code = fields.String(allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    #organization_name = fields.String(load_from='primary_user', allow_none=True)
    #phone = fields.String(allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='officehours')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def fix_name(self, data):
        f = 'name'
        if f in data and data[f] not in [None, '']:
            data[f] += ' PARK & RIDE'

class PreschoolSchema(pl.BaseSchema):
    # Some of the X/Y values are legitimate geocoordinates, but
    # most are those weird ones that need to be converted.
    asset_type = fields.String(dump_only=True, default='schools')
    name = fields.String(load_from='name1')
    parent_location = fields.String(load_from='name2', allow_none=True)
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='street', allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    phone = fields.String(load_from='phone', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def obtain_city_state_zip(self, data):
        f = 'town'
        if f in data and data[f] not in [None, '']:
            if 'PA' in data[f]:
                data['state'] = 'PA'
                city, zip_code = data[f].split(' PA ')
                city = re.sub(',', '', city.strip())
                data['city'] = city
                data['zip_code'] = zip_code.strip()

    @pre_load
    def fix_phones(self, data):
        f = 'cell'
        if f in data and data[f] not in [None, '', ' ']:
            if data['phone'] in [None, '', ' ']:
                data['phone'] = data[f]
            else:
                data['phone'] += ' | ' + data[f]

    @pre_load
    def fix_coords(self, data):
        f = 'x'
        if f in data and data[f] not in [None, '']:
            if float(data[f]) < 0:
                data['latitude'] = float(data['y'])
                data['longitude'] = float(data[f])

class SeniorCommunityCentersSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='senior_centers')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(allow_none=True)
    phone = fields.String(load_from='phone_1', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def obtain_city_state_zip(self, data):
        f = 'address3'
        if f in data and data[f] not in [None, '']:
            if 'PA' in data[f]:
                data['state'] = 'PA'
                city, zip_code = data[f].split(' PA ')
                city = re.sub(',', '', city.strip())
                data['city'] = city
                data['zip_code'] = zip_code.strip()

    @pre_load
    def join_address(self, data):
        if 'address2' in data and data['address2'] not in [None, '', ' ']:
            data['address'] += ', ' + data['address2']

    @pre_load
    def fix_degenerate_fields(self, data):
        if 'phone_1' in data:
            data['phone'] = data['phone_1']

class PublicBuildingsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='public_buildings')
    name = fields.String(load_from='facility')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(allow_none=True)
    #state = fields.String(allow_none=True)
    zip_code = fields.String(load_from='zipcode', allow_none=True)
    #phone = fields.String(load_from='phone_1', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def join_name(self, data):
        f = 'facility_c'
        if f in data and data[f] not in [None, '']:
            data['facility'] += f' ({data[f]})'

class VAFacilitiesSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='va_facilities')
    name = fields.String()
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(load_from='zip', allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    phone = fields.String(load_from='phone', allow_none=True)
    additional_directions = fields.String(load_from='directions', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def join_address(self, data):
        if 'address2' in data and data['address2'] not in [None, '', ' ', 'NOT AVAILABLE']:
            data['address'] += ', ' + data['address2']

    @pre_load
    def join_name(self, data):
        f = 'facility_c'
        if f in data and data[f] not in [None, '']:
            data['facility'] += f' ({data[f]})'

class VetSocialOrgsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='veterans_social_orgs')
    name = fields.String(load_from='title')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(load_from='address', allow_none=True)
    state = fields.String(load_from='address', allow_none=True)
    zip_code = fields.String(load_from='address', allow_none=True)
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def fix_address_fields(self, data):
        f = 'fixed_address'
        if data[f] in [None, '']:
            data['street_address'] = None
            data['city'] = None
            data['state'] = None
            data['zip_code'] = None
        else:
            try:
                address = data[f]
                parsed = normalize_address_record(address)
                data['street_address'] = parsed.get('address_line_1', None)
                data['city'] = parsed.get('city', None)
                data['state'] = parsed.get('state', None)
                data['zip_code'] = parsed.get('postal_code', None)
            except KeyError:
                ic(parsed)
                raise

class NursingHomesSchema(pl.BaseSchema):
    # In addition to contact phone number, the source file lists
    # the name of a contact person.
    asset_type = fields.String(dump_only=True, default='nursing_homes')
    name = fields.String(load_from='facility_n')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='street', allow_none=True)
    city = fields.String(load_from='city_or_bo', allow_none=True)
    #state = fields.String(allow_none=True)
    zip_code = fields.String(load_from='zip_code', allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    phone = fields.String(load_from='contact_nu', allow_none=True)
    email = fields.String(load_from='contact_em', allow_none=True)
    #additional_directions = fields.String(load_from='directions', allow_none=True)
    url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True



class LicenseSchema(pl.BaseSchema):
    name = fields.String(load_from='fullname')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='addressline1', allow_none=True)
    city = fields.String(load_from='city', allow_none=True)
    state = fields.String(load_from='statename', allow_none=True)
    zip_code = fields.String(load_from='zipcode', allow_none=True)
    #latitude = fields.Float(allow_none=True)
    #longitude = fields.Float(allow_none=True)
    #phone = fields.String(load_from='contact_nu', allow_none=True)
    #email = fields.String(load_from='contact_em', allow_none=True)
    #additional_directions = fields.String(load_from='directions', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def join_address(self, data):
        f1 = 'addressline1'
        data[f1] = data[f1].strip()
        f2 = 'addressline2'
        if f2 in data and data[f2] not in [None, '', ' ', 'NOT AVAILABLE', 'NULL']:
            data[f1] += ', ' + data[f2]
            f3 = 'addressline3'
            if f3 in data and data[f3] not in [None, '', ' ', 'NOT AVAILABLE', 'NULL']:
                data[f1] += ', ' + data[f3]

    @pre_load
    def fix_zip(self, data):
        f = 'zipcode'
        if f in data and data[f] not in [None, '', ' ', 'NOT AVAILABLE', 'NULL'] and len(data[f]) > 5:
            if data[f][6] != '-' and len(data[f]) == 9:
                data[f] = data[f][:5] + '-' + data[f][5:]

class DentistsSchema(LicenseSchema):
    asset_type = fields.String(dump_only=True, default='dentists')

class BarbersSchema(LicenseSchema):
    asset_type = fields.String(dump_only=True, default='barbers')

class NailSalonsSchema(LicenseSchema):
    asset_type = fields.String(dump_only=True, default='nail_salons')

class HairSalonsSchema(LicenseSchema):
    asset_type = fields.String(dump_only=True, default='hair_salons')

class PharmaciesSchema(LicenseSchema):
    asset_type = fields.String(dump_only=True, default='pharmacies')

class WMDSchema(pl.BaseSchema):
    name = fields.String(load_from='store_name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(load_from='mailing_city', allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(load_from='zip', allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    phone = fields.String(load_from='business_phone', allow_none=True)
    #email = fields.String(load_from='contact_em', allow_none=True)
    additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def fix_phone(self, data):
        f = 'business_phone'
        if f in data and data[f] in ['', ' ', 'NOT AVAILABLE', 'NULL', 'NO PHONE #']:
            data[f] = None

class LaundromatsSchema(WMDSchema):
    asset_type = fields.String(dump_only=True, default='laundromats')

class GasVendorsSchema(WMDSchema):
    asset_type = fields.String(dump_only=True, default='gas_stations')

class ChildCareCentersSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='child_care_centers')
    name = fields.String(load_from='facility_name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='facility_address', allow_none=True)
    city = fields.String(load_from='facility_city', allow_none=True)
    state = fields.String(load_from='facility_state', allow_none=True)
    zip_code = fields.String(load_from='facility_zip_code', allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    phone = fields.String(load_from='facility_phone', allow_none=True)
    email = fields.String(load_from='facility_email', allow_none=True)
    organization_name = fields.String(load_from='legal_entity_name', allow_none=True)
    residence = fields.Boolean(allow_none=True) # This is not officially part of the asset-map schema yet.
    notes = fields.String(load_from='provider_type', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def join_address(self, data):
        f = 'facility_address'
        f2 = 'facility_address_(continued)'
        if f2 in data and data[f2] not in [None, '', ' ']:
            if f in data and data[f] not in [None, '', ' ']:
                data[f] += ', ' + data[f2]
            else:
                data[f] = data[f2]

    @pre_load
    def fix_residence(self, data):
        f = 'provider_type'
        if f in data and data[f] not in ['', ' ', 'NOT AVAILABLE', 'NULL', 'NO PHONE #']:
            if data[f] == 'Child Care Center':
                data['residence'] = False
            elif data[f] in ['Family Child Care Home', 'Group Child Care Home']:
                data['residence'] = True

    @pre_load
    def fix_coords(self, data):
        f = 'facility_latitude_&_longitude'
        if f in data and data[f] not in [None, '', ' ']:
            point_string = data[f]
            point_string = re.sub('POINT \(', '', re.sub('\)$', '', point_string))
            lon_string, lat_string = point_string.split(' ')
            data['latitude'] = float(lat_string)
            data['longitude'] = float(lon_string)

class PropertyAssessmentsSchema(pl.BaseSchema):
    name = fields.String(load_from='usedesc', allow_none=True)
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='propertyhousenum', allow_none=True)
    city = fields.String(load_from='propertycity', allow_none=True)
    state = fields.String(load_from='propertystate', allow_none=True)
    zip_code = fields.String(load_from='propertyzip', allow_none=True)
    parcel_id = fields.String(load_from='parid', allow_none=True)
    #latitude = fields.Float(allow_none=True)
    #longitude = fields.Float(allow_none=True)
    #phone = fields.String(load_from='facility_phone', allow_none=True)
    #email = fields.String(load_from='facility_email', allow_none=True)
    #organization_name = fields.String(load_from='legal_entity_name', allow_none=True)
    #residence = fields.Boolean(allow_none=True) # This is not officially part of the asset-map schema yet.
    #notes = fields.String(load_from='provider_type', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def join_address(self, data):
        f0 = 'propertyhousenum'
        fs = ['propertyfraction', 'propertyaddress']
        for f in fs:
            if f in data and data[f] not in [None, '', ' ']:
                if f0 in data and data[f0] not in [None, '', ' ']:
                    if f == 'propertyfraction':
                        if re.match('\d', data[f][0]) is not None:
                            data[f0] += '-' + data[f]
                        else:
                            data[f0] += data[f]
                    else:
                        data[f0] += ' ' + data[f]
                else:
                    data[f0] = data[f]
    @post_load
    def extend_name(self, data):
        base = 'name'
        f = 'street_address'
        if f in data and data[f] not in [None, '', ' ']:
            if base in data:
                data[base] += ' @ ' + data[f]
            else:
                data[base] = data[f]

class ApartmentsSchema(PropertyAssessmentsSchema):
    asset_type = fields.String(dump_only=True, default='apartment_buildings')
    notes = fields.String(load_from='legal1', allow_none=True)

    @pre_load
    def join_notes(self, data):
        f0 = 'legal1'
        fs = ['legal2', 'legal3']
        for f in fs:
            if f in data and data[f] not in [None, '', ' ']:
                if f0 in data and data[f0] not in [None, '', ' ']:
                    data[f0] = data[f0].strip() + ', ' + data[f].strip()
                else:
                    data[f0] = data[f].strip()

class UniversitiesSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='universities')
    name = fields.String(load_from='descr')
    localizability = fields.String(dump_only=True, default='fixed')
    latitude = fields.Float(load_from='y', allow_none=True)
    longitude = fields.Float(load_from='x', allow_none=True)
    #organization_name = fields.String(load_from='legal_entity_name', allow_none=True)
    notes = fields.String(load_from='label', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

class SchoolsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='schools')
    name = fields.String(load_from='school')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address_line_1', allow_none=True)
    city = fields.String(load_from='city', allow_none=True)
    state = fields.String(load_from='state', allow_none=True)
    zip_code = fields.String(load_from='zip_code', allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    organization_name = fields.String(load_from='local_education_agency_(lea)', allow_none=True)
    #notes = fields.String(load_from='provider_type', allow_none=True)
    tags = fields.String(load_from='lea_type', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def join_address(self, data):
        f = 'address_line_1'
        f2 = 'address_line_2'
        if f2 in data and data[f2] not in [None, '', ' ']:
            if f in data and data[f] not in [None, '', ' ']:
                data[f] += ', ' + data[f2]
            else:
                data[f] = data[f2]

    @pre_load
    def fix_tags(self, data):
        f = 'lea_type'
        if f in data and data[f] not in ['', ' ', 'NOT AVAILABLE', 'NULL']:
            if data[f] == 'Other Private, Non -Licensed Entity':
                data[f] = 'Other Private, Non-Licensed Entity'
            data[f] = "LEA type: " + data[f]

    @pre_load
    def fix_coords(self, data):
        f = 'location_1'
        if f in data and data[f] not in [None, '', ' ']:
            parts = data[f].split('\n')
            point_string = re.sub('\(', '', re.sub('\)$', '', parts[-1]))
            lat_string, lon_string = point_string.split(', ')
            try:
                data['latitude'] = float(lat_string)
                data['longitude'] = float(lon_string)
            except ValueError: # It's not really a pair of floats.
                f = 'location_2'
                if f in data and data[f] not in [None, '', ' ']:
                    parts = data[f].split('\n')
                    point_string = re.sub('\(', '', re.sub('\)$', '', parts[-1]))
                    lat_string, lon_string = point_string.split(', ')
                    try:
                        data['latitude'] = float(lat_string)
                        data['longitude'] = float(lon_string)
                    except ValueError: # It's not really a pair of floats either.
                        data['latitude'] = None
                        data['longitude'] = None

class ParkFacilitiesSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='parks_and_facilities')
    name = fields.String(load_from='name', allow_none=False)
    parent_location = fields.String(load_from='park', allow_none=True)
    latitude = fields.Float(load_from='y', allow_none=True)
    longitude = fields.Float(load_from='x', allow_none=True)
    organization_name = fields.String(default='Allegheny County Parks Department')
    #notes = fields.String(load_from='label', allow_none=True)
    tags = fields.String(load_from='type', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    localizability = fields.String(dump_only=True, default='fixed')
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    data_source_name = fields.String(default='Allegheny County Park Facilities')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/allegheny-county-park-facilities')

    class Meta:
        ordered = True

    @pre_load
    def fix_nones(self, data):
        fs = ['name', 'park', 'type']
        for f in fs:
            if f in data and data[f] in [None, '', ' ']:
                if f == 'name':
                    data[f] = '(Unnamed location)'
                else: # park and type (which maps to parent_location and tags)
                    data[f] = None

class CityParksSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='parks_and_facilities')
    name = fields.String(load_from='updatepknm', allow_none=False)
    #parent_location = fields.String(load_from='park', allow_none=True)
    #latitude = fields.Float(load_from='y', allow_none=True)
    #longitude = fields.Float(load_from='x', allow_none=True)
    #organization_name = fields.String(default='Allegheny County Parks Department')
    notes = fields.String(load_from='maintenanc', allow_none=True)
    tags = fields.String(load_from='final_cat', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    geom = fields.String(load_from='geometry')
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    localizability = fields.String(dump_only=True, default='fixed')
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    last_updated = fields.DateTime(load_from='last_edi_1', allow_none=True)
    data_source_name = fields.String(default='Pittsburgh Parks')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/pittsburgh-parks')

    class Meta:
        ordered = True

    @pre_load
    def fix_notes(self, data):
        fs = ['maintenanc']
        for f in fs:
            if f in data and data[f] not in [None, '', ' ']:
                data[f] = 'Maintenance: ' + data[f]

class CityPlaygroundsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='parks_and_facilities')
    name = fields.String(load_from='name', allow_none=False)
    #parent_location = fields.String(load_from='park', allow_none=True)
    street_address = fields.String(load_from='median_street_number', allow_none=True)
    city = fields.String(dump_only=True, default='Pittsburgh')
    state = fields.String(dump_only=True, default='PA')
    #zip_code = fields.String(load_from='zip_code', allow_none=True)
    latitude = fields.Float(load_from='avg_latitude', allow_none=True)
    longitude = fields.Float(load_from='avg_longitude', allow_none=True)
    #organization_name = fields.String(default='Allegheny County Parks Department')
    #tags = fields.String(load_from='final_cat', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    child_friendly = fields.String(dump_only=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    notes = fields.String(dump_only=True, default='This is derived from an aggregated version of the WPRDC Playground Equipment dataset.')
    #geometry = fields.String()
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    localizability = fields.String(dump_only=True, default='fixed')
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = fields.DateTime(load_from='last_edi_1', allow_none=True)
    data_source_name = fields.String(default='Playground Equipment')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/playground-equipment')

    class Meta:
        ordered = True

    @pre_load
    def fix_address(self, data):
        f0 = 'median_street_number'
        f = 'median_street'
        if f0 not in data or data[f0] in [None, '', ' ']:
            data[f0] = data[f]
        elif f in data and data[f] not in [None, '', ' ']:
            data[f0] += ' ' + data[f]

class CityPlaygroundEquipmentSchema(pl.BaseSchema):
    equipment_type = fields.String(load_only=True, allow_none=True)

    asset_type = fields.String(dump_only=True, default='parks_and_facilities')
    name = fields.String(load_from='name', allow_none=False)
    parent_location = fields.String(load_from='name', allow_none=True)
    street_address = fields.String(load_from='street_number', allow_none=True)
    city = fields.String(dump_only=True, default='Pittsburgh')
    state = fields.String(dump_only=True, default='PA')
    #zip_code = fields.String(load_from='zip_code', allow_none=True)
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    #organization_name = fields.String(default='Allegheny County Parks Department')
    #tags = fields.String(load_from='final_cat', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    child_friendly = fields.String(dump_only=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    notes = fields.String(dump_only=True, default='This is derived from an aggregated version of the WPRDC Playground Equipment dataset.')
    #geometry = fields.String()
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    localizability = fields.String(dump_only=True, default='fixed')
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = fields.DateTime(load_from='last_edi_1', allow_none=True)
    data_source_name = fields.String(default='Playground Equipment')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/playground-equipment')
    primary_key_from_rocket = fields.String(load_from='id')

    class Meta:
        ordered = True

    @post_load
    def join_name(self, data):
        f2 = 'equipment_type'
        f = 'name'
        if f2 in data and data[f2] not in [None, '', ' ']:
            if f in data and data[f] not in [None, '', ' ']:
                data[f] += f' ({data[f2]})'
            else:
                data[f] = f' ({data[f2]})'

    @pre_load
    def fix_address(self, data):
        f0 = 'street_number'
        f = 'street'
        if f0 not in data or data[f0] in [None, '', ' ']:
            data[f0] = data[f]
        elif f in data and data[f] not in [None, '', ' ']:
            data[f0] += ' ' + data[f]

class GeocodedFoodFacilitiesSchema(pl.BaseSchema):
    name = fields.String(load_from='facility_name', allow_none=False)
    #parent_location = fields.String(load_from='name', allow_none=True)
    street_address = fields.String(load_from='num', allow_none=True)
    city = fields.String()
    state = fields.String()
    #zip_code = fields.String(load_from='zip_code', allow_none=True)
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    #organization_name = fields.String(default='Allegheny County Parks Department')
    #tags = fields.String(load_from='final_cat', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    notes = fields.String(dump_only=True, default='This is derived from an aggregated version of the WPRDC Playground Equipment dataset.')
    #geometry = fields.String()
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    localizability = fields.String(dump_only=True, default='fixed')
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = fields.DateTime(load_from='last_edi_1', allow_none=True)
    data_source_name = fields.String(default='Geocoded Food Facilities')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/allegheny-county-restaurant-food-facility-inspection-violations/resource/112a3821-334d-4f3f-ab40-4de1220b1a0a')
    primary_key_from_rocket = fields.String(load_from='id')

    class Meta:
        ordered = True

    @pre_load
    def fix_address(self, data):
        f0 = 'num'
        f = 'street'
        if f0 not in data or data[f0] in [None, '', ' ']:
            data[f0] = data[f]
        elif f in data and data[f] not in [None, '', ' ']:
            data[f0] += ' ' + data[f]

class GeocodedRestaurantsSchema(GeocodedFoodFacilitiesSchema):
    asset_type = fields.String(dump_only=True, default='restaurants')

class GeocodedSupermarketsSchema(GeocodedFoodFacilitiesSchema):
    asset_type = fields.String(dump_only=True, default='supermarkets')

class GeocodedFoodBanksSchema(GeocodedFoodFacilitiesSchema):
    asset_type = fields.String(dump_only=True, default='food_banks')

#def conditionally_get_city_files(job, **kwparameters):
#    if not kwparameters['use_local_files']:
#        fetch_city_file(job)

# dfg

job_dicts = [
    {
        'job_code': 'farmers_markets',
        'source_type': 'local',
        #'source_dir_absolute': '/Users/drw/WPRDC/asset-map/AssetMapDownloads1',
        'source_file': ASSET_MAP_SOURCE_DIR + '2019_farmers-markets.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FarmersMarketsSchema,
        #'primary_key_fields': Nothing solid.
        'always_clear_first': True,
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + '2019_farmers-markets.csv',
    },
    {
        'job_code': 'fish_fries',
        'source_type': 'local',
        #'source_dir_absolute': '/Users/drw/WPRDC/asset-map/AssetMapDownloads1',
        'source_file': ASSET_MAP_SOURCE_DIR + '2020_pittsburgh_fish_fry_locations-validated.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FishFriesSchema,
        'always_clear_first': True,
        #'primary_key_fields': Nothing solid. The 'id' field only has values for the fries with publish = False.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + '2020_pittsburgh_fish_fry_locations-validated.csv',
    },
    {
        'job_code': LibrariesSchema().job_code, #'clp_libraries',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'CLP_Library_Locations.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': LibrariesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['clpid'], # A solid primary key.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'CLP_Library_Locations.csv',
    },
    {
        'job_code': 'county_churches',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'AlleghenyCountyChurches.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FaithBasedFacilitiesSchema,
        'always_clear_first': True,
        #'primary_key_fields': Nothing solid.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'AlleghenyCountyChurches.csv',
    },
    {
        'job_code': 'family_support_centers',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'FamilySupportCtrs.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FamilySupportCentersSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # It's not clear whether these will be fixed under updates
        # since it's just a sequence of integers. I'll call it a Possibly Unreliable Key.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'FamilySupportCtrs.csv',
    },
    {
        'job_code': 'senior_centers',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'FamilySeniorServices-fixed.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': SeniorCentersSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'], # Possibly Unreliable Key
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'FamilySeniorServices-fixed.csv',
    },
    {
        'job_code': 'polling_places',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Allegheny_County_Polling_Places_May2019.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': PollingPlacesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid_1'], # Possibly Unreliable Key
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Allegheny_County_Polling_Places_May2019.csv',
    },
    {
        'job_code': 'acha_community_sites',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'ACHA_CommunitySitesMap-fixed.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ACHACommunitySitesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'],  # Possibly Unreliable Key # 'id' would also be another (Possibly Unreliable)  option.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'ACHA_CommunitySitesMap-fixed.csv',
    },
    {
        'job_code': 'achd_clinics',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'ACHD_Clinic.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ClinicsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid_1'], # Possibly Unreliable Key
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'ACHD_Clinic.csv',
    },
    {
        'job_code': 'affordable_housing',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Affordable_Housing.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': AffordableHousingSchema,
        'always_clear_first': True,
        #'primary_key_fields': Nothing solid.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Affordable_Housing.csv',
    },
    {
        'job_code': 'wic_vendors',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Allegheny_County_WIC_Vendor_Locations-nonempty-rows.csv', # One empty row was manually removed.
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': WICVendorsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # Possibly Unreliable Key
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Allegheny_County_WIC_Vendor_Locations-nonempty-rows.csv',
    },
    {
        'job_code': 'homeless_shelters',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'BigBurghServices-shelters.csv',
        'encoding': 'utf-8-sig',
        #'source_transformation': "SELECT * FROM source WHERE category LIKE '%roof-overnight%'",
        # The ETL framework is not ready to handle such source transformations.
        # Options: 1) Manually modify the file.
        # 2) Build a custom_processing function to do the same thing (somehow).
        # 3) Modify the extractor to send SQL requests to the CKAN handler and use the
        # resulting data for the rest of the ETL job.
        #'custom_processing': conditionally_get_city_files,
        'schema': HomelessSheltersSchema,
        'always_clear_first': True,
        #'primary_key_fields': [''], No solid key.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'BigBurghServices-shelters.csv',
    },
    # To get homeless shelters from BigBurghServices, filter out just the six rows containing the string 'roof-overnight'.
    # SELECT * FROM <source_file_converted_to_in_memory_sqlite_file> WHERE 'roof-overnight' <is a sting within the field> category;
    {
        'job_code': 'bus_stops', # Optional job code to allow just this job to be selected from the command line.
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'bussStopUsageByRoute_STOP_ID_CURRENT_STOP_not_No.csv',
        'encoding': 'utf-8-sig',
        #'source_transformation': "SELECT * FROM source WHERE category LIKE '%roof-overnight%'",
        # The ETL framework is not ready to handle such source transformations.
        # Options: 1) Manually modify the file.
        # 2) Build a custom_processing function to do the same thing (somehow).
        # 3) Modify the extractor to send SQL requests to the CKAN handler and use the
        # resulting data for the rest of the ETL job.
        #'custom_processing': conditionally_get_city_files,
        'schema': BusStopsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['stop_id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'bussStopUsageByRoute_STOP_ID_CURRENT_STOP_not_No.csv',
    },
    # Filter bussStopUseageByRoute.csv, eliminating CURRENT_STOP != 'No', aggregate so STOP_NAME is unique
    {
        'job_code': 'catholic', # Optional job code to allow just this job to be selected from the command line.
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Catholic.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': CatholicSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Catholic.csv',
    },
    {
        'job_code': 'librariesall',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'LibrariesAll.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': MoreLibrariesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid_12_13'], # Possibly Unreliable Key
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'LibrariesAll.csv',
    },
    {
        'job_code': 'museums',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Museums.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': MuseumsSchema,
        'always_clear_first': True,
        #'primary_key_fields': ['fid'], # Nothing solid. fid is just the row number.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Museums.csv',
    },
    {
        'job_code': 'wic_offices',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'WIC_Offices.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': WICOfficesSchema,
        'always_clear_first': True,
        #'primary_key_fields': ['objectid'], 'objectid' is just the row number. ZIP would be more reliable.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'WIC_Offices.csv',
    },
    {
        'job_code': 'rec_centers',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'City_of_Pgh_Facilities_just_rec_centers.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': RecCentersSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'], # This is a good primary key. parcel_id could also work.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'City_of_Pgh_Facilities_just_rec_centers.csv',
    },
    {
        'job_code': 'fed_qual',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'FederallyQualifiedHealthCtr.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FedQualHealthCentersSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # Possibly Unreliable
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'FederallyQualifiedHealthCtr.csv',
    },
    {
        'job_code': 'places_of_worship',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'PlacesOfWorship.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': PlacesOfWorshipSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'PlacesOfWorship.csv',
    },
    {
        'job_code': 'park_and_rides',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'ParkandRides_1909-w-manual-addresses.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ParkAndRidesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'ParkandRides_1909-w-manual-addresses.csv',
    },
    {
        'job_code': 'preschools',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'PreschoolACLA.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': PreschoolSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'PreschoolACLA.csv',
    },
    {
        'job_code': 'senior_community_centers',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'SeniorCommunityCenter.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': SeniorCommunityCentersSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'SeniorCommunityCenter.csv',
    },
    {
        'job_code': 'public_buildings',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'PublicBuildings.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': PublicBuildingsSchema,
        'always_clear_first': True,
        #'primary_key_fields': ['fid'], #'fid' is just row number and is not solid.
        # About 40% of rows have a BUILDING_C (code) value, but the rest don't.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'PublicBuildings.csv',
    },
    {
        'job_code': 'va_facilities',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'VA_FacilitiesPA.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': VAFacilitiesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid_1'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'VA_FacilitiesPA.csv',
    },
    {
        'job_code': 'veterans_social_orgs',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'VeteransSocialOrg-fixed.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': VetSocialOrgsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'VeteransSocialOrg-fixed.csv',
    },
    {
        'job_code': 'nursing_homes',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'DOH_NursingHome201806.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': NursingHomesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['facility_i'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'DOH_NursingHome201806.csv',
    },
    {
        'job_code': 'dentists',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'LicenseData_Active_Allegheny_Dentists.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': DentistsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['licensenumber'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'LicenseData_Active_Allegheny_Dentists.csv',
    },
    {
        'job_code': 'barbers',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'LicenseData_Active_Allegheny_Barbers.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': BarbersSchema,
        'always_clear_first': True,
        'primary_key_fields': ['licensenumber'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'LicenseData_Active_Allegheny_Barbers.csv',
    },
    {
        'job_code': 'nail_salons',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'LicenseData_Active_Allegheny_Nail_Salons.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': NailSalonsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['licensenumber'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'LicenseData_Active_Allegheny_Nail_Salons.csv',
    },
    {
        'job_code': 'hair_salons',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'LicenseData_Active_Allegheny_Hair_Salons.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': HairSalonsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['licensenumber'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'LicenseData_Active_Allegheny_Hair_Salons.csv',
    },
    {
        'job_code': 'pharmacies',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'LicenseData_Active_Allegheny_Pharmacies.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': PharmaciesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['licensenumber'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'LicenseData_Active_Allegheny_Pharmacies.csv',
    },
    {
        'job_code': 'laundromats',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'wmd-laundromats.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': LaundromatsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['store_id'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'wmd-laundromats.csv',
    },
    {
        'job_code': 'gas_vendors',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'wmd-gas-vendors.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': GasVendorsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['store_id'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'wmd-gas-vendors.csv',
    },
    {
        'job_code': 'child_care_providers',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Child_Care_Providers_Listing_Child_Care_Center_Allegheny.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ChildCareCentersSchema,
        'always_clear_first': True,
        'primary_key_fields': [], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Child_Care_Providers_Listing_Child_Care_Center_Allegheny.csv',
    },
    {
        'job_code': 'apartments',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'apartments20plus20200117.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ApartmentsSchema,
        'always_clear_first': True,
        'primary_key_fields': [], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'apartments20plus20200117.csv',
    },
    {
        'job_code': 'universities',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Universities.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': UniversitiesSchema,
        'always_clear_first': True,
        'primary_key_fields': [], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Universities.csv'
    },
    {
        'job_code': 'schools',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Public_and_Private_Education_Institutions_2017_Education_Allegheny-minus_PO_Boxes.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': SchoolsSchema,
        'always_clear_first': True,
        'primary_key_fields': [], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Public_and_Private_Education_Institutions_2017_Education_Allegheny-minus_PO_Boxes.csv'
    },
    {
        'job_code': 'county_park_facilities',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Allegheny_County_Park_Facilities.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ParkFacilitiesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Allegheny_County_Park_Facilities.csv',
    },
    {
        'job_code': 'city_parks',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Pittsburgh_Parks.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': CityParksSchema,
        'always_clear_first': True,
        'primary_key_fields': ['globalid_2'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Pittsburgh_Parks.csv'
    },
#    {
#        'job_code': 'city_playgrounds',
#        'source_type': 'local',
#        'source_file': ASSET_MAP_SOURCE_DIR + 'playgroundequipment_averaged.csv',
#        'encoding': 'utf-8-sig',
#        #'custom_processing': conditionally_get_city_files,
#        'schema': CityPlaygroundsSchema,
#        'always_clear_first': True,
#        'primary_key_fields': ['name'], # This is a so-so primary key field as the name
#           or spelling or other features of the name can change, so I'm switching to
#           using playground equipment as the assets because of their actual primary keys.
#        'destinations': ['file'],
#        'destination_file': ASSET_MAP_PROCESSED_DIR + 'playgroundequipment_averaged.csv'
#    },
    {
        'job_code': 'city_playground_equipment',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'playgroundequipment.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': CityPlaygroundEquipmentSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'playgroundequipment.csv'
    },
    {
        'job_code': 'bb_rec_centers',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'BigBurghServices-rec_centers.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': BigBurghRecCentersSchema,
        'always_clear_first': True,
        #'primary_key_fields': No solid key.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'BigBurghServices-rec_centers.csv'
    },
    {
        'job_code': 'restaurants',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'FoodFacilitiesGeocoded-restaurants.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': GeocodedRestaurantsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'FoodFacilitiesGeocoded-restaurants.csv',
    },
    {
        'job_code': 'supermarkets',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'FoodFacilitiesGeocoded-supermarkets.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': GeocodedSupermarketsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'FoodFacilitiesGeocoded-supermarkets.csv',
    },
    {
        'job_code': 'geocoded_food_banks',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'FoodFacilitiesGeocoded-food-banks.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': GeocodedFoodBanksSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'FoodFacilitiesGeocoded-food-banks.csv',
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.

# [ ] Fix fish-fries validation by googling for how to delete rows in marshmallow schemas (or else pre-process the rows somehow... load the whole thing into memory and filter).
