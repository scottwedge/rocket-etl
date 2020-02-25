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
from engine.parameters.local_parameters import ASSET_MAP_SOURCE_DIR, ASSET_MAP_PROCESSED_DIR

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

    class Meta:
        ordered = True

class SeniorCentersSchema(pl.BaseSchema):
    # Unused field: Denomination
    # Calvin Memorial Church has no maiing address!
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
    # Unused field: parcel
    parcel_id = fields.String(load_from='parcel', allow_none=True)
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
    def fix_address(self, data):
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

    class Meta:
        ordered = True

class HomelessSheltersSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='homeless_shelters')
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
        ic(data)
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

class BusStopsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='bus_stops')
    name = fields.String(load_from='stop_name')
    localizability = fields.String(dump_only=True, default='fixed')
    #street_address = fields.String(load_from='address', allow_none=True)
    #city = fields.String(load_from='address', allow_none=True)
    #state = fields.String(load_from='address', allow_none=True)
    #zip_code = fields.String(load_from='address', allow_none=True)
    latitude = fields.Float(load_from='median_latitude', allow_none=True)
    longitude = fields.Float(load_from='median_longitude', allow_none=True)
    #phone = fields.String(allow_none=True)
    #organization_name = fields.String(load_from='organization', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='schedule', allow_none=True)
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(allow_none=True)
    notes = fields.String(load_from='median_all_routes', allow_none=True)

    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

    class Meta:
        ordered = True

    @pre_load
    def fill_out_notes(self, data):
        data['notes'] = f"Routes: {data['median_all_routes']}, stop type: {data['median_stop_type']}, shelter: {data['median_shelter']}"

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
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url =

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

    class Meta:
        ordered = True

class MuseumsSchema(pl.BaseSchema):
    asset_type = fields.String(dump_only=True, default='museums')
    name = fields.String(load_from='descr')
    localizability = fields.String(dump_only=True, default='fixed')
    latitude = fields.Float(load_from='y', allow_none=True)
    longitude = fields.Float(load_from='x', allow_none=True)
    county = fields.String(load_from='county1', allow_none=True) # This one is not currently used in the asset-map schema.

    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = 'WPRDC Dataset: 2019 Farmer's Markets'
    #data_source_url

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
    def fix_things(self, data):
        ic(data)
        if 'phone_1' in data:
            data['phone'] = data['phone_1']
        if 'city_1' in data:
            data['city'] = data['city_1']

#def conditionally_get_city_files(job, **kwparameters):
#    if not kwparameters['use_local_files']:
#        fetch_city_file(job)

# dfg

job_dicts = [
    {
        'source_type': 'local',
        #'source_dir_absolute': '/Users/drw/WPRDC/asset-map/AssetMapDownloads1',
        'source_file': ASSET_MAP_SOURCE_DIR + '2019_farmers-markets.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FarmersMarketsSchema,
        'always_clear_first': True,
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + '2019_farmers-markets.csv',
        'resource_name': 'farmers_markets',
    },
    {
        'source_type': 'local',
        #'source_dir_absolute': '/Users/drw/WPRDC/asset-map/AssetMapDownloads1',
        'source_file': ASSET_MAP_SOURCE_DIR + '2019_pittsburgh_fish_fry_locations_validated.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FishFriesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + '2019_pittsburgh_fish_fry_locations_validated.csv',
        'resource_name': 'fish_fries',
    },
    {
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'CLP_Library_Locations.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': LibrariesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['clpid'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'CLP_Library_Locations.csv',
        'resource_name': 'libraries',
    },
    {
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'AlleghenyCountyChurches.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FaithBasedFacilitiesSchema,
        'always_clear_first': True,
        'primary_key_fields': [''],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'AlleghenyCountyChurches.csv',
        'resource_name': 'faith-based_facilities',
    },
    {
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'FamilySupportCtrs.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FamilySupportCentersSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'FamilySupportCtrs.csv',
        'resource_name': 'family_support_centers',
    },
    {
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'FamilySeniorServices-fixed.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': SeniorCentersSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'FamilySeniorServices-fixed.csv',
        'resource_name': 'senior_centers',
    },
    {
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Allegheny_County_Polling_Places_May2019.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': PollingPlacesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid_1'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Allegheny_County_Polling_Places_May2019.csv',
        'resource_name': 'polling_places',
    },
    {
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'ACHA_CommunitySitesMap-fixed.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ACHACommunitySitesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'ACHA_CommunitySitesMap-fixed.csv',
        'resource_name': 'acha_community_sites'
    },
    {
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'ACHD_Clinic.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ClinicsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'ACHD_Clinic.csv',
        'resource_name': 'achd_clinics'
    },
    {
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Affordable_Housing.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': AffordableHousingSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Affordable_Housing.csv',
        'resource_name': 'affordable_housing'
    },
    {
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Allegheny_County_WIC_Vendor_Locations-nonempty-rows.csv', # One empty row was manually removed.
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': WICVendorsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Allegheny_County_WIC_Vendor_Locations-nonempty-rows.csv',
        'resource_name': 'wic_vendors'
    },
    {
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
        'primary_key_fields': [''],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'BigBurghServices-shelters.csv',
        'resource_name': 'homeless_shelters'
    },
    # To get homeless shelters from BigBurgServices, filter out just the six rows containing the string 'roof-overnight'.
    # SELECT * FROM <source_file_converted_to_in_memory_sqlite_file> WHERE 'roof-overnight' <is a sting within the field> category;
    {
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'bussStopUsageByRoute_selectedref_STOP_NAME_freq.csv',
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
        'primary_key_fields': ['stop_name'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'bussStopUsageByRoute_selectedref_STOP_NAME_freq.csv',
        'resource_name': 'bus_stops'
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
        'resource_name': 'faith-based_facilities'
    },
    {
        'job_code': 'librariesall',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'LibrariesAll.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': MoreLibrariesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid_12_13'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'LibrariesAll.csv',
        'resource_name': 'libraries'
    },
    {
        'job_code': 'museums',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Museums.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': MuseumsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['fid'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Museums.csv',
        'resource_name': 'museums'
    },
    {
        'job_code': 'wic_offices',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'WIC_Offices.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': WICOfficesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'WIC_Offices.csv',
        'resource_name': 'wic_offices'
    },
]
# [ ] Fix fish-fries validation by googling for how to delete rows in marshmallow schemas (or else pre-process the rows somehow... load the whole thing into memory and filter).
